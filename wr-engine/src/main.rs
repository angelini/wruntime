mod engine;
mod registry;
mod server;

use wr_engine::config::{self, EnvValue};

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_retry::strategy::{ExponentialBackoff, FixedInterval};
use tokio_retry::Retry;
use tracing::{info, warn};
use uuid::Uuid;

use wr_common::lifecycle_service::{notify_supervisor, AdmissionGate};
use wr_common::process_lifecycle::{LifecycleDriver, ProcessState, ServiceKind, TransitionReason};
use wr_common::signal::{shutdown_signal_request, wait_for_shutdown_trigger, ShutdownCause};
use wr_common::task_group::{TaskExit, TaskGroup};
use wr_common::wruntime::lifecycle_service_client::LifecycleServiceClient;
use wr_common::wruntime::{
    node_service_client::NodeServiceClient, BeginEngineDrainRequest, DeregisterEngineRequest,
    EngineRegistration, GetLifecycleStatusRequest, HeartbeatRequest, HeartbeatResponse,
    ModuleDescriptor, ProcessLifecycleState, RegisterEngineRequest, SecretRequest,
};

const SHUTDOWN_BUDGET: Duration = Duration::from_secs(30);

type ShutdownOperation<'a> = Pin<Box<dyn Future<Output = Result<()>> + 'a>>;

trait EngineShutdownOperations {
    fn fence_claims(&mut self, deadline: tokio::time::Instant) -> ShutdownOperation<'_>;
    fn withdraw_routes(&mut self, deadline: tokio::time::Instant) -> ShutdownOperation<'_>;
    fn drain_http(&mut self, deadline: tokio::time::Instant) -> ShutdownOperation<'_>;
    fn drain_workers(&mut self, deadline: tokio::time::Instant) -> ShutdownOperation<'_>;
    fn deregister(&mut self, deadline: tokio::time::Instant) -> ShutdownOperation<'_>;
    fn join_tasks(&mut self, deadline: tokio::time::Instant) -> ShutdownOperation<'_>;
}

async fn run_engine_shutdown(
    operations: &mut impl EngineShutdownOperations,
    deadline: tokio::time::Instant,
) -> Result<()> {
    let mut failures = Vec::new();
    macro_rules! execute {
        ($name:literal, $operation:expr) => {
            if let Err(error) = $operation.await {
                failures.push(format!("{} failed: {error:#}", $name));
            }
        };
    }
    execute!("fence claims", operations.fence_claims(deadline));
    execute!("withdraw routes", operations.withdraw_routes(deadline));
    execute!("drain HTTP", operations.drain_http(deadline));
    execute!("drain workers", operations.drain_workers(deadline));
    execute!("deregister", operations.deregister(deadline));
    execute!("join tasks", operations.join_tasks(deadline));
    if failures.is_empty() {
        Ok(())
    } else {
        anyhow::bail!(failures.join("; "))
    }
}

async fn drain_http_admission(
    admission: &AdmissionGate,
    deadline: tokio::time::Instant,
) -> Result<()> {
    admission.close();
    admission
        .wait_for_idle(deadline)
        .await
        .map_err(|remaining| {
            anyhow::anyhow!("engine HTTP drain timed out with {remaining} requests in flight")
        })
}

async fn shutdown_engine_tasks(
    tasks: &mut TaskGroup,
    deadline: tokio::time::Instant,
    supervisor_notification: std::io::Result<()>,
) -> Result<()> {
    let notification_failure = supervisor_notification
        .context("failed to notify supervisor that engine is stopping")
        .err();
    let report = tasks.shutdown(deadline).await;
    match (notification_failure, report.is_clean()) {
        (None, true) => Ok(()),
        (Some(error), true) => Err(error),
        (None, false) => anyhow::bail!("engine task shutdown was not clean: {report:?}"),
        (Some(error), false) => {
            anyhow::bail!("{error:#}; engine task shutdown was not clean: {report:?}")
        }
    }
}

struct ProductionEngineShutdown<'a> {
    worker_admission: &'a AdmissionGate,
    http_admission: &'a AdmissionGate,
    client: &'a mut NodeServiceClient<tonic::transport::Channel>,
    engine_id: &'a str,
    tasks: &'a mut TaskGroup,
}

impl EngineShutdownOperations for ProductionEngineShutdown<'_> {
    fn fence_claims(&mut self, _deadline: tokio::time::Instant) -> ShutdownOperation<'_> {
        Box::pin(async move {
            self.worker_admission.close();
            Ok(())
        })
    }

    fn withdraw_routes(&mut self, deadline: tokio::time::Instant) -> ShutdownOperation<'_> {
        Box::pin(async move {
            let response = tokio::time::timeout_at(
                deadline,
                self.client.begin_engine_drain(BeginEngineDrainRequest {
                    engine_id: self.engine_id.to_string(),
                }),
            )
            .await
            .context("engine route withdrawal timed out")?
            .context("engine route withdrawal failed")?
            .into_inner();
            anyhow::ensure!(
                response.proxy_routing_table_version >= response.manager_routing_table_version,
                "proxy acknowledged an unconverged engine withdrawal"
            );
            Ok(())
        })
    }

    fn drain_http(&mut self, deadline: tokio::time::Instant) -> ShutdownOperation<'_> {
        Box::pin(drain_http_admission(self.http_admission, deadline))
    }

    fn drain_workers(&mut self, deadline: tokio::time::Instant) -> ShutdownOperation<'_> {
        Box::pin(async move {
            self.worker_admission
                .wait_for_idle(deadline)
                .await
                .map_err(|remaining| {
                    anyhow::anyhow!("engine worker drain timed out with {remaining} jobs in flight")
                })
        })
    }

    fn deregister(&mut self, deadline: tokio::time::Instant) -> ShutdownOperation<'_> {
        Box::pin(async move {
            tokio::time::timeout_at(
                deadline,
                self.client.deregister_engine(DeregisterEngineRequest {
                    engine_id: self.engine_id.to_string(),
                }),
            )
            .await
            .context("engine deregistration timed out")?
            .context("engine deregistration failed")?;
            Ok(())
        })
    }

    fn join_tasks(&mut self, deadline: tokio::time::Instant) -> ShutdownOperation<'_> {
        Box::pin(shutdown_engine_tasks(
            self.tasks,
            deadline,
            notify_supervisor("STOPPING=1"),
        ))
    }
}

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
    let (lifecycle_driver, lifecycle) = LifecycleDriver::new(
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
    tasks.spawn("lifecycle-driver", move |cancellation| {
        lifecycle_driver.run(cancellation)
    });
    runner.spawn_epoch_ticker(&mut tasks);
    {
        let registry = registry.clone();
        let database = runner.admin_pool();
        let defaults = Arc::new(server::WorkerDefaults::from_modules(&config.modules)?);
        let admission = http_admission.clone();
        let lifecycle = lifecycle.clone();
        tasks.spawn("engine-http-listener", move |cancellation| {
            server::serve(
                listener,
                registry,
                database,
                defaults,
                server::EngineAdmission {
                    workload: admission,
                },
                lifecycle.snapshot(),
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
        lifecycle
            .mark_ready("modules healthy and routing convergence acknowledged")
            .await?;
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
        let _ = lifecycle
            .request_stop(TransitionReason::TaskFailure, "engine startup failed")
            .await;
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
        let lifecycle_handle = lifecycle.snapshot();
        tasks.spawn("engine-heartbeat", move |mut cancellation| async move {
            let mut interval = tokio::time::interval(Duration::from_secs(3));
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => return Ok(TaskExit::Cancelled),
                    _ = interval.tick() => {}
                }
                if lifecycle_handle.current().state == ProcessState::Stopping {
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

    let mut failure: Option<anyhow::Error> = None;
    match wait_for_shutdown_trigger(&lifecycle, &mut tasks, shutdown_signal_request()).await {
        Ok(ShutdownCause::Signal) => {}
        Ok(ShutdownCause::RequiredTask(Some(outcome))) => {
            failure = Some(anyhow::anyhow!(
                "required task {} exited: {:?}",
                outcome.name,
                outcome.kind
            ));
        }
        Ok(ShutdownCause::RequiredTask(None)) => {
            failure = Some(anyhow::anyhow!("all required engine tasks exited"));
        }
        Err(error) => failure = Some(error.into()),
    }

    if lifecycle.current().state != ProcessState::Stopping {
        if let Err(error) = lifecycle
            .request_stop(
                TransitionReason::ShutdownOrchestration,
                "engine shutdown started",
            )
            .await
        {
            failure.get_or_insert_with(|| error.into());
        }
    }
    let drain_deadline = tokio::time::Instant::now() + SHUTDOWN_BUDGET;
    let mut shutdown = ProductionEngineShutdown {
        worker_admission: &worker_admission,
        http_admission: &http_admission,
        client: &mut client,
        engine_id: &engine_id,
        tasks: &mut tasks,
    };
    if let Err(shutdown_error) = run_engine_shutdown(&mut shutdown, drain_deadline).await {
        failure = Some(match failure.take() {
            Some(primary) => anyhow::anyhow!(
                "{primary:#}; engine shutdown operations also failed: {shutdown_error:#}"
            ),
            None => shutdown_error,
        });
    }

    info!(engine_id, "engine stopped");
    if let Some(error) = failure {
        Err(error)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use wr_common::signal::ShutdownRequest;

    #[derive(Default)]
    struct RecordingShutdown {
        calls: Vec<(&'static str, tokio::time::Instant)>,
        fail_at: Option<&'static str>,
    }

    impl RecordingShutdown {
        fn record(
            &mut self,
            operation: &'static str,
            deadline: tokio::time::Instant,
        ) -> ShutdownOperation<'_> {
            self.calls.push((operation, deadline));
            let result = if self.fail_at == Some(operation) {
                Err(anyhow::anyhow!("injected {operation} failure"))
            } else {
                Ok(())
            };
            Box::pin(async move { result })
        }
    }

    impl EngineShutdownOperations for RecordingShutdown {
        fn fence_claims(&mut self, deadline: tokio::time::Instant) -> ShutdownOperation<'_> {
            self.record("fence_claims", deadline)
        }

        fn withdraw_routes(&mut self, deadline: tokio::time::Instant) -> ShutdownOperation<'_> {
            self.record("withdraw_routes", deadline)
        }

        fn drain_http(&mut self, deadline: tokio::time::Instant) -> ShutdownOperation<'_> {
            self.record("drain_http", deadline)
        }

        fn drain_workers(&mut self, deadline: tokio::time::Instant) -> ShutdownOperation<'_> {
            self.record("drain_workers", deadline)
        }

        fn deregister(&mut self, deadline: tokio::time::Instant) -> ShutdownOperation<'_> {
            self.record("deregister", deadline)
        }

        fn join_tasks(&mut self, deadline: tokio::time::Instant) -> ShutdownOperation<'_> {
            self.record("join_tasks", deadline)
        }
    }

    struct DeadlineConsumingShutdown {
        calls: Vec<(&'static str, tokio::time::Instant)>,
        http_admission: AdmissionGate,
        tasks: TaskGroup,
    }

    impl DeadlineConsumingShutdown {
        fn record(&mut self, operation: &'static str, deadline: tokio::time::Instant) {
            self.calls.push((operation, deadline));
        }
    }

    impl EngineShutdownOperations for DeadlineConsumingShutdown {
        fn fence_claims(&mut self, deadline: tokio::time::Instant) -> ShutdownOperation<'_> {
            self.record("fence_claims", deadline);
            Box::pin(async { Ok(()) })
        }

        fn withdraw_routes(&mut self, deadline: tokio::time::Instant) -> ShutdownOperation<'_> {
            self.record("withdraw_routes", deadline);
            Box::pin(async move {
                tokio::time::sleep_until(deadline).await;
                anyhow::bail!("shared deadline consumed by route withdrawal")
            })
        }

        fn drain_http(&mut self, deadline: tokio::time::Instant) -> ShutdownOperation<'_> {
            self.record("drain_http", deadline);
            Box::pin(drain_http_admission(&self.http_admission, deadline))
        }

        fn drain_workers(&mut self, deadline: tokio::time::Instant) -> ShutdownOperation<'_> {
            self.record("drain_workers", deadline);
            Box::pin(async { Ok(()) })
        }

        fn deregister(&mut self, deadline: tokio::time::Instant) -> ShutdownOperation<'_> {
            self.record("deregister", deadline);
            Box::pin(async { anyhow::bail!("deregistration deadline expired") })
        }

        fn join_tasks(&mut self, deadline: tokio::time::Instant) -> ShutdownOperation<'_> {
            self.record("join_tasks", deadline);
            Box::pin(shutdown_engine_tasks(
                &mut self.tasks,
                deadline,
                Err(std::io::Error::other(
                    "synthetic STOPPING notification failure",
                )),
            ))
        }
    }

    #[tokio::test]
    async fn signal_owned_engine_shutdown_executes_real_sequence_under_one_deadline() -> Result<()>
    {
        let (driver, lifecycle) = LifecycleDriver::new(ServiceKind::Engine, "engine-test");
        let mut tasks = TaskGroup::new();
        tasks.spawn("lifecycle-driver", move |cancellation| {
            driver.run(cancellation)
        });
        tasks.spawn("engine-required", |mut cancellation| async move {
            cancellation.cancelled().await;
            Ok(TaskExit::Cancelled)
        });

        let cause = wait_for_shutdown_trigger(&lifecycle, &mut tasks, async {
            ShutdownRequest::stop(TransitionReason::SignalInterrupt, "SIGINT fixture")
        })
        .await?;
        assert_eq!(cause, ShutdownCause::Signal);
        assert_eq!(lifecycle.current().state, ProcessState::Stopping);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let expected = [
            "fence_claims",
            "withdraw_routes",
            "drain_http",
            "drain_workers",
            "deregister",
            "join_tasks",
        ];
        for failed_operation in ["withdraw_routes", "drain_http", "deregister", "join_tasks"] {
            let mut shutdown = RecordingShutdown {
                fail_at: Some(failed_operation),
                ..RecordingShutdown::default()
            };
            let error = run_engine_shutdown(&mut shutdown, deadline)
                .await
                .expect_err("injected operation failure unexpectedly reported success");
            assert!(error.to_string().contains(failed_operation));
            assert_eq!(
                shutdown
                    .calls
                    .iter()
                    .map(|(operation, _)| *operation)
                    .collect::<Vec<_>>(),
                expected
            );
            assert!(
                shutdown
                    .calls
                    .iter()
                    .all(|(_, observed)| *observed == deadline),
                "every real shutdown operation must receive the same absolute deadline"
            );
        }

        let expired_deadline = tokio::time::Instant::now();
        let mut expired = RecordingShutdown::default();
        run_engine_shutdown(&mut expired, expired_deadline).await?;
        assert_eq!(
            expired
                .calls
                .iter()
                .map(|(operation, _)| *operation)
                .collect::<Vec<_>>(),
            expected,
            "deadline exhaustion must not skip mandatory cleanup operations"
        );
        assert!(expired
            .calls
            .iter()
            .all(|(_, observed)| *observed == expired_deadline));

        let report = tasks.shutdown(deadline).await;
        assert!(report.is_clean(), "{report:?}");
        Ok(())
    }

    #[tokio::test]
    async fn deadline_consumed_mid_sequence_still_runs_local_cleanup_and_task_shutdown() {
        let mut owned_tasks = TaskGroup::new();
        owned_tasks.spawn("non-terminating-owned-task", |_| async {
            std::future::pending::<()>().await;
            Ok(TaskExit::Completed)
        });
        let http_admission = AdmissionGate::closed();
        http_admission.open();
        let in_flight = http_admission
            .try_enter()
            .expect("fixture HTTP request was not admitted");
        let mut shutdown = DeadlineConsumingShutdown {
            calls: Vec::new(),
            http_admission,
            tasks: owned_tasks,
        };
        let deadline = tokio::time::Instant::now() + Duration::from_millis(20);
        let started = tokio::time::Instant::now();
        let error = run_engine_shutdown(&mut shutdown, deadline)
            .await
            .expect_err("injected deadline exhaustion unexpectedly reported success");

        assert_eq!(
            shutdown
                .calls
                .iter()
                .map(|(operation, _)| *operation)
                .collect::<Vec<_>>(),
            [
                "fence_claims",
                "withdraw_routes",
                "drain_http",
                "drain_workers",
                "deregister",
                "join_tasks",
            ]
        );
        assert!(shutdown
            .calls
            .iter()
            .all(|(_, observed)| *observed == deadline));
        assert!(!shutdown.http_admission.is_open());
        assert_eq!(shutdown.http_admission.in_flight(), 1);
        assert!(shutdown.tasks.is_empty());
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "expired shutdown introduced a replacement grace period"
        );
        let evidence = error.to_string();
        assert!(evidence.contains("shared deadline consumed by route withdrawal"));
        assert!(evidence.contains("engine HTTP drain timed out with 1 requests in flight"));
        assert!(evidence.contains("deregistration deadline expired"));
        assert!(evidence.contains("synthetic STOPPING notification failure"));
        assert!(evidence.contains("engine task shutdown was not clean"));
        drop(in_flight);
    }
}
