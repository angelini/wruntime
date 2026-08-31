mod engine;
mod registry;
mod server;

use wr_engine::config::{self, EnvValue};

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_retry::strategy::{ExponentialBackoff, FixedInterval};
use tokio_retry::Retry;
use tracing::{info, warn};
use uuid::Uuid;

use wr_common::lifecycle_service::{notify_supervisor, AdmissionGate};
use wr_common::process_lifecycle::{
    ProcessLifecycleCoordinator, ProcessState, ServiceKind, TransitionReason,
};
use wr_common::signal::{apply_shutdown_request, shutdown_signal_request};
use wr_common::task_group::{TaskExit, TaskGroup};
use wr_common::wruntime::lifecycle_service_client::LifecycleServiceClient;
use wr_common::wruntime::{
    node_service_client::NodeServiceClient, BeginEngineDrainRequest, DeregisterEngineRequest,
    EngineRegistration, GetLifecycleStatusRequest, HeartbeatRequest, HeartbeatResponse,
    ModuleDescriptor, ProcessLifecycleState, RegisterEngineRequest, SecretRequest,
};

const SHUTDOWN_BUDGET: Duration = Duration::from_secs(30);

fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .on_thread_start(wasmtime::Engine::tls_eager_initialize)
        .build()?;
    runtime.block_on(async_main())
}

async fn async_main() -> Result<()> {
    let arguments: Vec<String> = std::env::args().collect();
    if arguments
        .get(1)
        .is_some_and(|argument| argument == "--lifecycle-probe")
    {
        return lifecycle_probe(
            arguments
                .get(2)
                .map(String::as_str)
                .unwrap_or("engine.toml"),
        )
        .await;
    }

    let mut telemetry = wr_common::telemetry::init("wr-engine")?;
    let result = run_service(
        arguments
            .get(1)
            .map(String::as_str)
            .unwrap_or("engine.toml"),
    )
    .await;
    let finalized = telemetry.finalize();
    if !finalized.is_success() && result.is_ok() {
        anyhow::bail!("telemetry finalization failed: {:?}", finalized.failures);
    }
    result
}

async fn lifecycle_probe(config_path: &str) -> Result<()> {
    let config = config::EngineConfig::load(config_path)?;
    let address = config.listen_address.trim_start_matches("http://");
    let status = LifecycleServiceClient::connect(format!("http://{address}"))
        .await?
        .get_status(GetLifecycleStatusRequest {})
        .await?
        .into_inner()
        .status
        .context("lifecycle response omitted status")?;
    anyhow::ensure!(
        status.state == ProcessLifecycleState::Ready as i32,
        "engine lifecycle state is not READY"
    );
    Ok(())
}

async fn collect_healthy_module_descriptors(
    registry: &registry::ModuleRegistry,
    module_configs: &[config::ModuleConfig],
) -> Vec<ModuleDescriptor> {
    let mut healthy = Vec::new();
    let mut checked = std::collections::HashSet::new();
    for module in module_configs {
        if !checked.insert((&module.namespace, &module.name, &module.version)) {
            continue;
        }
        let Ok(module_id) =
            wr_common::identity::ModuleId::parse(&module.namespace, &module.name, &module.version)
        else {
            continue;
        };
        if let Some(sender) = registry.next_sender(&module_id).await {
            if engine::check_module_health(&sender).await {
                healthy.push(ModuleDescriptor {
                    name: module.name.clone(),
                    namespace: module.namespace.clone(),
                    version: module.version.clone(),
                    proto_schema: Vec::new(),
                });
            }
        }
    }
    healthy
}

async fn send_heartbeat_with_retry(
    client: &mut NodeServiceClient<tonic::transport::Channel>,
    engine_id: &str,
    healthy_modules: Vec<ModuleDescriptor>,
) -> std::result::Result<HeartbeatResponse, tonic::Status> {
    let request = HeartbeatRequest {
        engine_id: engine_id.to_string(),
        healthy_modules,
    };
    Retry::start(FixedInterval::from_millis(50).take(2), || {
        let mut client = client.clone();
        let request = request.clone();
        async move { client.heartbeat(request).await }
    })
    .await
    .map(tonic::Response::into_inner)
}

async fn connect_proxy(address: &str) -> Result<NodeServiceClient<tonic::transport::Channel>> {
    let strategy = ExponentialBackoff::from_millis(200)
        .max_delay(Duration::from_secs(5))
        .take(10);
    Retry::start(strategy, || {
        let address = address.to_string();
        async move {
            NodeServiceClient::connect(address)
                .await
                .map_err(anyhow::Error::from)
        }
    })
    .await
    .with_context(|| format!("failed to connect to proxy at {address} after retries"))
}

async fn run_service(config_path: &str) -> Result<()> {
    let config = config::EngineConfig::load(config_path)?;
    let engine_id = Uuid::new_v4().to_string();
    let lifecycle = ProcessLifecycleCoordinator::new(
        ServiceKind::Engine,
        wr_common::process_lifecycle::resolve_process_instance_id(engine_id.clone()),
    );
    let http_admission = AdmissionGate::closed();
    let worker_admission = AdmissionGate::closed();

    let listen_address = config.listen_address.trim_start_matches("http://");
    let socket_address: std::net::SocketAddr = listen_address
        .parse()
        .context("invalid engine listen_address")?;
    anyhow::ensure!(
        socket_address.ip().is_loopback(),
        "engine lifecycle and workload listener must bind to loopback"
    );
    let listener = TcpListener::bind(socket_address)
        .await
        .context("failed to bind engine listener")?;

    let advertise_address = format!("http://{socket_address}");
    let peer_address = config.node.peer_address()?;
    let deployment =
        config
            .deployment
            .clone()
            .map(|metadata| wr_common::wruntime::DeploymentMetadata {
                node_id: metadata.node_id,
                revision: metadata.revision,
                bundle_digest: metadata.bundle_digest,
                engine_slot: metadata.engine_slot,
            });

    let registry = registry::ModuleRegistry::new();
    let mut runner = engine::EngineRunner::new(config.clone())?;
    let mut tasks = TaskGroup::new();
    runner.spawn_epoch_ticker(&mut tasks);
    {
        let registry = registry.clone();
        let database = runner.admin_pool();
        let defaults = Arc::new(server::WorkerDefaults::from_modules(&config.modules)?);
        let admission = http_admission.clone();
        let workers = worker_admission.clone();
        let lifecycle = lifecycle.clone();
        tasks.spawn("engine-http-listener", move |cancellation| {
            server::serve(
                listener,
                registry,
                database,
                defaults,
                server::EngineAdmission {
                    workload: admission,
                    worker: workers,
                },
                lifecycle,
                cancellation,
            )
        });
    }

    let mut node_client: Option<NodeServiceClient<tonic::transport::Channel>> = None;
    let startup: Result<()> = async {
        node_client = Some(connect_proxy(&config.node.control_address).await?);
        let client = node_client
            .as_mut()
            .context("proxy client missing after connection")?;

        let mut module_descriptors = Vec::new();
        let mut schema_sent = std::collections::HashSet::new();
        for module in &config.modules {
            let first = schema_sent.insert((&module.namespace, &module.name, &module.version));
            let proto_schema = if first {
                let schema_path = module
                    .schema_path
                    .as_deref()
                    .filter(|path| !path.trim().is_empty())
                    .with_context(|| {
                        format!(
                            "schema_path is required for '{}.{}@{}'",
                            module.namespace, module.name, module.version
                        )
                    })?;
                std::fs::read(schema_path).with_context(|| {
                    format!(
                        "failed to read schema for '{}.{}@{}' from {schema_path}",
                        module.namespace, module.name, module.version
                    )
                })?
            } else {
                Vec::new()
            };
            module_descriptors.push(ModuleDescriptor {
                name: module.name.clone(),
                namespace: module.namespace.clone(),
                version: module.version.clone(),
                proto_schema,
            });
        }

        let mut secret_requests = Vec::new();
        let mut seen_secrets = std::collections::HashSet::new();
        for module in &config.modules {
            for (key, value) in &module.env {
                if matches!(value, EnvValue::Secret(_))
                    && seen_secrets.insert((&module.namespace, key))
                {
                    secret_requests.push(SecretRequest {
                        namespace: module.namespace.clone(),
                        key: key.clone(),
                    });
                }
            }
        }
        let db_namespaces = config
            .modules
            .iter()
            .filter(|module| module.database)
            .map(|module| module.namespace.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        let registration = RegisterEngineRequest {
            registration: Some(EngineRegistration {
                engine_id: engine_id.clone(),
                address: advertise_address,
                proxy_address: config.node.proxy_address.clone(),
                peer_address,
                modules: module_descriptors,
                secrets: secret_requests,
                db_namespaces,
                deployment,
            }),
        };
        let registration_response = Retry::start(
            ExponentialBackoff::from_millis(500)
                .max_delay(Duration::from_secs(5))
                .take(10),
            || {
                let mut client = client.clone();
                let request = registration.clone();
                async move { client.register_engine(request).await }
            },
        )
        .await
        .context("engine registration failed after retries")?
        .into_inner();

        runner
            .provision_schemas(&registration_response.db_credentials)
            .await?;
        runner.run_job_migrations().await?;
        runner.run_migrations().await?;
        runner.build_namespace_pools(&registration_response.db_credentials)?;
        runner.start_recovery_coordinator(&mut tasks)?;

        let mut secrets = HashMap::new();
        for namespace in &registration_response.secrets {
            for (key, value) in &namespace.secrets {
                secrets.insert((namespace.namespace.as_str(), key.as_str()), value.as_str());
            }
        }
        let mut resolved_envs = HashMap::new();
        for module in &config.modules {
            let mut environment = HashMap::new();
            for (key, value) in &module.env {
                let value = match value {
                    EnvValue::Plain(value) => value.clone(),
                    EnvValue::Secret(_) => secrets
                        .get(&(module.namespace.as_str(), key.as_str()))
                        .with_context(|| {
                            format!(
                                "secret '{key}' not found for namespace '{}'",
                                module.namespace
                            )
                        })?
                        .to_string(),
                };
                environment.insert(key.clone(), value);
            }
            resolved_envs.insert((module.namespace.clone(), module.name.clone()), environment);
        }

        runner
            .load_modules(
                &registry,
                &resolved_envs,
                &engine_id,
                &mut tasks,
                worker_admission.clone(),
            )
            .await?;
        let healthy = collect_healthy_module_descriptors(&registry, &config.modules).await;
        let expected = config
            .modules
            .iter()
            .map(|module| (&module.namespace, &module.name, &module.version))
            .collect::<std::collections::HashSet<_>>()
            .len();
        anyhow::ensure!(
            healthy.len() == expected,
            "only {}/{} configured modules passed startup health checks",
            healthy.len(),
            expected
        );
        let readiness = tokio::time::timeout(
            SHUTDOWN_BUDGET,
            send_heartbeat_with_retry(client, &engine_id, healthy),
        )
        .await
        .context("engine readiness publication timed out")??;
        anyhow::ensure!(
            readiness.proxy_routing_table_version >= readiness.manager_routing_table_version,
            "engine readiness lacked manager/proxy routing convergence evidence"
        );
        if let Some(outcome) = tasks.try_next_completion() {
            anyhow::bail!(
                "required task {} exited during engine startup: {:?}",
                outcome.name,
                outcome.kind
            );
        }

        http_admission.open();
        worker_admission.open();
        lifecycle.mark_ready("modules healthy and routing convergence acknowledged")?;
        notify_supervisor("READY=1")?;
        info!(
            engine_id,
            manager_version = readiness.manager_routing_table_version,
            proxy_version = readiness.proxy_routing_table_version,
            "engine ready"
        );
        Ok(())
    }
    .await;

    if let Err(error) = startup {
        let shutdown_deadline = tokio::time::Instant::now() + SHUTDOWN_BUDGET;
        http_admission.close();
        worker_admission.close();
        let _ = lifecycle.request_stop(TransitionReason::TaskFailure, "engine startup failed");
        let _ = notify_supervisor("STOPPING=1");
        if let Some(client) = node_client.as_mut() {
            let _ = tokio::time::timeout_at(
                shutdown_deadline,
                client.deregister_engine(DeregisterEngineRequest {
                    engine_id: engine_id.clone(),
                }),
            )
            .await;
        }
        let report = tasks.shutdown(shutdown_deadline).await;
        return Err(error).context(format!("engine startup failed; task shutdown: {report:?}"));
    }

    let mut client = node_client.context("proxy client missing after startup")?;
    {
        let mut heartbeat_client = client.clone();
        let heartbeat_engine_id = engine_id.clone();
        let heartbeat_registry = registry.clone();
        let heartbeat_modules = config.modules.clone();
        let lifecycle_handle = lifecycle.handle();
        tasks.spawn("engine-heartbeat", move |mut cancellation| async move {
            let mut interval = tokio::time::interval(Duration::from_secs(3));
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => return Ok(TaskExit::Cancelled),
                    _ = interval.tick() => {}
                }
                if lifecycle_handle.current().state >= ProcessState::Draining {
                    continue;
                }
                let healthy =
                    collect_healthy_module_descriptors(&heartbeat_registry, &heartbeat_modules)
                        .await;
                if let Err(error) =
                    send_heartbeat_with_retry(&mut heartbeat_client, &heartbeat_engine_id, healthy)
                        .await
                {
                    warn!(%error, "engine heartbeat failed after retries");
                }
            }
        });
    }

    let mut updates = lifecycle.handle().subscribe();
    let mut failure: Option<anyhow::Error> = None;
    loop {
        tokio::select! {
            request = shutdown_signal_request() => {
                if let Err(error) = apply_shutdown_request(&lifecycle, request) {
                    failure = Some(error.into());
                    let _ = lifecycle.request_stop(
                        TransitionReason::TaskFailure,
                        "engine shutdown transition failed",
                    );
                }
                break;
            }
            changed = updates.changed() => {
                if changed.is_err() {
                    failure = Some(anyhow::anyhow!("lifecycle coordinator closed unexpectedly"));
                    let _ = lifecycle.request_stop(
                        TransitionReason::TaskFailure,
                        "engine lifecycle coordinator closed",
                    );
                    break;
                }
                if updates.borrow().state >= ProcessState::Draining {
                    break;
                }
            }
            outcome = tasks.next_completion() => {
                if let Some(outcome) = outcome {
                    failure = Some(anyhow::anyhow!("required task {} exited: {:?}", outcome.name, outcome.kind));
                    let _ = lifecycle.request_stop(TransitionReason::TaskFailure, outcome.name);
                } else {
                    failure = Some(anyhow::anyhow!("all required engine tasks exited"));
                    let _ = lifecycle.request_stop(
                        TransitionReason::TaskFailure,
                        "all required engine tasks exited",
                    );
                }
                break;
            }
        }
    }

    let mut drain_deadline = tokio::time::Instant::now() + SHUTDOWN_BUDGET;
    worker_admission.close();
    let withdrawal = tokio::time::timeout_at(
        drain_deadline,
        client.begin_engine_drain(BeginEngineDrainRequest {
            engine_id: engine_id.clone(),
        }),
    )
    .await;
    match withdrawal {
        Ok(Ok(response)) => {
            let response = response.into_inner();
            if response.proxy_routing_table_version < response.manager_routing_table_version {
                failure = Some(anyhow::anyhow!(
                    "proxy acknowledged an unconverged engine withdrawal"
                ));
            }
        }
        Ok(Err(error)) => {
            failure = Some(anyhow::anyhow!("engine route withdrawal failed: {error}"));
        }
        Err(_) => {
            failure = Some(anyhow::anyhow!("engine route withdrawal timed out"));
        }
    }

    http_admission.close();
    if let Err(remaining) = http_admission.wait_for_idle(drain_deadline).await {
        failure.get_or_insert_with(|| {
            anyhow::anyhow!("engine HTTP drain timed out with {remaining} requests in flight")
        });
    }
    if let Err(remaining) = worker_admission.wait_for_idle(drain_deadline).await {
        failure.get_or_insert_with(|| {
            anyhow::anyhow!("engine worker drain timed out with {remaining} jobs in flight")
        });
    }
    match tokio::time::timeout_at(
        drain_deadline,
        client.deregister_engine(DeregisterEngineRequest {
            engine_id: engine_id.clone(),
        }),
    )
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => {
            failure.get_or_insert_with(|| anyhow::anyhow!("engine deregistration failed: {error}"));
        }
        Err(_) => {
            failure.get_or_insert_with(|| anyhow::anyhow!("engine deregistration timed out"));
        }
    }

    if failure.is_none() && lifecycle.current().state == ProcessState::Draining {
        loop {
            tokio::select! {
                request = shutdown_signal_request() => {
                    if let Err(error) = apply_shutdown_request(&lifecycle, request) {
                        failure = Some(error.into());
                        let _ = lifecycle.request_stop(
                            TransitionReason::TaskFailure,
                            "engine stop transition failed",
                        );
                    }
                    break;
                }
                changed = updates.changed() => {
                    if changed.is_err() || updates.borrow().state == ProcessState::Stopping {
                        break;
                    }
                }
                outcome = tasks.next_completion() => {
                    if let Some(outcome) = outcome {
                        failure = Some(anyhow::anyhow!("required task {} exited while engine was drained: {:?}", outcome.name, outcome.kind));
                        let _ = lifecycle.request_stop(TransitionReason::TaskFailure, outcome.name);
                    } else {
                        failure = Some(anyhow::anyhow!("all required engine tasks exited while drained"));
                        let _ = lifecycle.request_stop(
                            TransitionReason::TaskFailure,
                            "all required engine tasks exited while drained",
                        );
                    }
                    break;
                }
            }
        }
        // Drain remains observable without implying exit. A subsequent Stop
        // request begins the separate final control-listener shutdown budget.
        drain_deadline = tokio::time::Instant::now() + SHUTDOWN_BUDGET;
    }

    if lifecycle.current().state != ProcessState::Stopping {
        if let Err(error) = lifecycle.request_stop(
            if failure.is_some() {
                TransitionReason::TaskFailure
            } else {
                TransitionReason::ShutdownOrchestration
            },
            "engine drain complete",
        ) {
            failure.get_or_insert_with(|| error.into());
        }
    }
    if let Err(error) = notify_supervisor("STOPPING=1") {
        failure.get_or_insert_with(|| {
            anyhow::Error::new(error).context("failed to notify supervisor that engine is stopping")
        });
    }
    let report = tasks.shutdown(drain_deadline).await;
    if !report.is_clean() {
        failure.get_or_insert_with(|| {
            anyhow::anyhow!("engine task shutdown was not clean: {report:?}")
        });
    }

    info!(engine_id, "engine stopped");
    if let Some(error) = failure {
        Err(error)
    } else {
        Ok(())
    }
}
