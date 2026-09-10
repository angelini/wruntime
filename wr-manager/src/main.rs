pub mod auth;
pub mod config;
pub mod crypto;
pub mod db;
pub mod job_admin;
pub mod migrate;
pub mod operations;
pub mod pool;
pub mod scheduler;
pub mod service;
pub mod state;
pub mod status;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tonic::transport::server::TcpIncoming;
use tonic::transport::Server;
use tracing::info;
use uuid::Uuid;
use wr_common::lifecycle_service::{
    notify_supervisor, AdmissionGate, LifecycleServiceAdapter, ManagerLifecycleState,
};
use wr_common::process_lifecycle::{LifecycleOwner, ProcessState, ServiceKind, TransitionReason};
use wr_common::signal::{shutdown_signal_request, wait_for_shutdown_trigger, ShutdownCause};
use wr_common::task_group::{TaskExit, TaskGroup};
use wr_common::wruntime::cluster_service_server::ClusterServiceServer;
use wr_common::wruntime::infrastructure_service_server::InfrastructureServiceServer;
use wr_common::wruntime::job_service_server::JobServiceServer;
use wr_common::wruntime::lifecycle_service_server::LifecycleServiceServer;
use wr_common::wruntime::node_service_server::NodeServiceServer;
use wr_common::wruntime::policy_service_server::PolicyServiceServer;

const SHUTDOWN_BUDGET: Duration = Duration::from_secs(30);
const STALE_MANAGER_REAP_INTERVAL: Duration = Duration::from_secs(60);

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    let arguments: Vec<String> = std::env::args().collect();
    if arguments
        .get(1)
        .is_some_and(|arg| arg == "--lifecycle-probe")
    {
        let config_path = arguments
            .get(2)
            .map(String::as_str)
            .unwrap_or("manager.toml");
        return lifecycle_probe(config_path).await;
    }

    let mut telemetry = wr_common::telemetry::init("wr-manager")?;
    let result = run_service(
        arguments
            .get(1)
            .map(String::as_str)
            .unwrap_or("manager.toml"),
    )
    .await;
    telemetry.finalize_preserving(result)
}

async fn lifecycle_probe(config_path: &str) -> Result<()> {
    let config = config::ManagerConfig::load(config_path)?;
    let endpoint = config
        .cluster
        .advertise_grpc_address
        .clone()
        .unwrap_or_else(|| {
            format!(
                "https://{}",
                config.listen_address.replace("0.0.0.0", "127.0.0.1")
            )
        });
    let tls = wr_common::tls::build_tonic_client_tls(&config.client_tls)?;
    let uri: http::Uri = endpoint.parse()?;
    let server_name = uri
        .host()
        .ok_or_else(|| anyhow::anyhow!("manager lifecycle endpoint requires a host"))?
        .to_owned();
    let provider = wr_common::manager_client::ManagerEpochProvider::new(
        vec![wr_common::manager_client::ManagerCandidate {
            endpoint,
            server_name,
        }],
        tls,
        config.client_tls.server_ca_cert_path.clone(),
        config.client_tls.cert_path.clone(),
    )?;
    let epoch = provider
        .pin(wr_common::manager_client::RetryClass::ReadOnly)
        .await?;
    epoch
        .observation()
        .require_identity(&wr_common::manager_client::EpochIdentity {
            manager_id: config.manager_id.to_string(),
            policy_generation: config.authorization_policy.generation,
            policy_digest: config.authorization_policy.digest.clone(),
        })?;
    anyhow::ensure!(
        epoch.observation().process_ready,
        "manager lifecycle endpoint is not ready"
    );
    Ok(())
}

async fn run_service(config_path: &str) -> Result<()> {
    let config = config::ManagerConfig::load(config_path)?;
    let manager_id = config.manager_id.to_string();
    let mut lifecycle = LifecycleOwner::new(
        ServiceKind::Manager,
        wr_common::process_lifecycle::resolve_process_instance_id(Uuid::new_v4().to_string()),
    );
    let admission = AdmissionGate::closed();
    let addr = config.listen_address.parse()?;

    let database_url = wr_common::pool::redact_database_url(&config.database.url);
    {
        let bootstrap =
            wr_common::pool::build_pool(&config.database.url, 1).with_context(|| {
                format!("failed to create manager bootstrap database pool for {database_url}")
            })?;
        let client = bootstrap.get().await.with_context(|| {
            format!(
                "failed to connect to manager database {database_url} while bootstrapping wr_system schema"
            )
        })?;
        client
            .batch_execute("CREATE SCHEMA IF NOT EXISTS wr_system")
            .await
            .with_context(|| format!("failed to create wr_system schema in {database_url}"))?;
    }
    let db_pool = pool::build_pool(&config.database.url, config.database.max_connections)
        .with_context(|| format!("failed to create manager database pool for {database_url}"))?;
    let mut client = db_pool
        .get()
        .await
        .with_context(|| format!("failed to connect to manager database {database_url}"))?;
    migrate::run_migrations(&mut client)
        .await
        .with_context(|| format!("failed to run manager database migrations in {database_url}"))?;
    drop(client);

    let grpc_address = config
        .cluster
        .advertise_grpc_address
        .clone()
        .unwrap_or_else(|| {
            format!(
                "https://{}",
                config.listen_address.replace("0.0.0.0", "127.0.0.1")
            )
        });
    let crypto = Arc::new(crypto::SecretCrypto::from_env()?);
    let incoming = TcpIncoming::bind(addr).context("failed to bind manager gRPC listener")?;
    let tls = wr_common::tls::build_tonic_server_tls(&config.tls)
        .map_err(|error| anyhow::anyhow!("failed to build TLS config: {error}"))?;
    wr_common::tls::build_tonic_client_tls(&config.client_tls)
        .context("failed to build manager workload TLS configuration")?;
    let mut server = Server::builder()
        .tls_config(tls)
        .context("failed to apply TLS config")?;

    let manager = service::Manager::with_admission(
        db_pool.clone(),
        crypto,
        config.cluster.manager_liveness_threshold_secs,
        admission.clone(),
    )
    .with_proxy_inventory_owner(manager_id.clone(), config.proxy_tombstone_retention_secs)
    .with_proxy_status_thresholds(
        config.proxy_heartbeat_timeout_secs,
        config.proxy_routing_freshness_secs,
    )
    .with_workload_policy(config.authorization_policy.clone());
    let principal_policy = auth::PrincipalPolicy::new(config.authorization_policy.clone());
    let operator_service = service::OperatorApi::with_admission(
        db_pool.clone(),
        principal_policy.clone(),
        admission.clone(),
        config.cluster.manager_liveness_threshold_secs as f64,
        config.engine_heartbeat_timeout_secs as f64,
        config.module_heartbeat_timeout_secs.get() as f64,
    )
    .with_proxy_status_thresholds(
        config.proxy_heartbeat_timeout_secs,
        config.proxy_routing_freshness_secs,
    );
    let job_admin_service = job_admin::JobAdminApi::new(
        db_pool.clone(),
        config.engine_heartbeat_timeout_secs,
        config.client_tls.clone(),
        admission.clone(),
    );
    let agent_service = service::NodeAgentApi::with_admission(
        db_pool.clone(),
        principal_policy.clone(),
        admission.clone(),
    );
    let manager_lifecycle = ManagerLifecycleState::default();
    manager_lifecycle.update(
        wr_common::wruntime::PrivilegedAdmissionState::ClosedStartup,
        "",
        wr_common::wruntime::ManagerRolloutPhase::Unspecified as i32,
        "",
        0,
    );
    let policy_service = service::PolicyApi::new(
        principal_policy.clone(),
        admission.clone(),
        manager_lifecycle.clone(),
    );
    let infrastructure_service = service::InfrastructureApi::new(manager.clone(), operator_service);
    let node_service = service::ManagerNodeApi::new(manager.clone(), agent_service);
    let lifecycle_service = service::AuthenticatedLifecycleApi::new(
        LifecycleServiceAdapter::new_manager(
            lifecycle.snapshot(),
            manager_id.clone(),
            config.authorization_policy.generation,
            config.authorization_policy.digest.clone(),
            manager_lifecycle.clone(),
        ),
        principal_policy.clone(),
    );
    let authorizer = Arc::new(auth::ManagerAuthorizer::new(
        principal_policy,
        admission.clone(),
    ));
    let router = server
        .add_service(ClusterServiceServer::new(
            service::AuthorizedClusterService::new(manager, authorizer.clone()),
        ))
        .add_service(InfrastructureServiceServer::new(
            service::AuthorizedInfrastructureService::new(
                infrastructure_service,
                authorizer.clone(),
            ),
        ))
        .add_service(NodeServiceServer::new(service::AuthorizedNodeService::new(
            node_service,
            authorizer.clone(),
        )))
        .add_service(
            JobServiceServer::new(job_admin::AuthorizedJobService::new(
                job_admin_service,
                authorizer.clone(),
            ))
            .max_encoding_message_size(wr_common::lifecycle::MAX_JOB_ADMIN_MESSAGE_BYTES),
        )
        .add_service(PolicyServiceServer::new(
            service::AuthorizedPolicyService::new(policy_service, authorizer.clone()),
        ))
        .add_service(LifecycleServiceServer::new(
            service::AuthorizedLifecycleService::new(lifecycle_service, authorizer),
        ));

    db::register_manager(&db_pool, &manager_id, &grpc_address)
        .await
        .map_err(|error| anyhow::anyhow!("failed to register manager: {error}"))?;
    db::bootstrap_initial_manager_policy_state(
        &db_pool,
        config.authorization_policy.generation,
        &config.authorization_policy.digest,
    )
    .await
    .map_err(|error| anyhow::anyhow!("failed to bootstrap initial manager policy: {error}"))?;
    let open_at_startup = db::initialize_manager_policy_state(
        &db_pool,
        &manager_id,
        config.authorization_policy.generation,
        &config.authorization_policy.digest,
    )
    .await
    .map_err(|error| anyhow::anyhow!("failed to install manager policy state: {error}"))?;
    if open_at_startup {
        manager_lifecycle.update(
            wr_common::wruntime::PrivilegedAdmissionState::Open,
            "",
            wr_common::wruntime::ManagerRolloutPhase::Unspecified as i32,
            "",
            0,
        );
    }

    let mut tasks = TaskGroup::new();
    tasks.spawn("manager-grpc", move |cancellation| async move {
        let mut shutdown = cancellation.clone();
        router
            .serve_with_incoming_shutdown(incoming, async move {
                shutdown.cancelled().await;
            })
            .await?;
        Ok(if cancellation.is_cancelled() {
            TaskExit::Cancelled
        } else {
            TaskExit::Completed
        })
    });
    {
        let pool = db_pool.clone();
        let id = manager_id.clone();
        let admission = admission.clone();
        let interval = Duration::from_secs(config.cluster.manager_heartbeat_interval_secs);
        tasks.spawn("manager-self-heartbeat", move |cancellation| {
            db::run_manager_heartbeat_owned(pool, id, interval, admission, cancellation)
        });
    }

    {
        let pool = db_pool.clone();
        let id = manager_id.clone();
        let generation = config.authorization_policy.generation;
        let digest = config.authorization_policy.digest.clone();
        let rollout_admission = admission.clone();
        let rollout_lifecycle = manager_lifecycle.clone();
        let rollout_policy = config.authorization_policy.clone();
        tasks.spawn("manager-rollout-observer", move |cancellation| {
            db::run_manager_rollout_observer_owned(
                pool,
                id,
                generation,
                digest,
                rollout_admission,
                rollout_lifecycle,
                rollout_policy,
                cancellation,
            )
        });
    }

    {
        let pool = db_pool.clone();
        let stale_threshold_secs = config.cluster.manager_stale_row_reap_threshold_secs;
        tasks.spawn("manager-stale-row-reaper", move |cancellation| {
            db::run_stale_manager_reaper_owned(
                pool,
                stale_threshold_secs,
                STALE_MANAGER_REAP_INTERVAL,
                cancellation,
            )
        });
    }

    {
        let pool = db_pool.clone();
        let id = manager_id.clone();
        let interval = Duration::from_secs(config.release_cleanup_interval_secs);
        tasks.spawn("manager-release-cleanup", move |cancellation| {
            state::reconcile_release_cleanup_owned(pool, id, interval, cancellation)
        });
    }

    {
        let pool = db_pool.clone();
        let engine_timeout = config.engine_heartbeat_timeout_secs;
        let module_timeout = config.module_heartbeat_timeout_secs.get();
        tasks.spawn("manager-route-monitor", move |cancellation| {
            state::monitor_heartbeats_owned(
                pool,
                engine_timeout,
                module_timeout,
                Duration::from_secs(5),
                cancellation,
            )
        });
    }

    {
        let pool = db_pool.clone();
        let id = manager_id.clone();
        let scheduler_admission = admission.clone();
        let local_proxy = config.local_proxy_address.clone();
        let lease = config.scheduler_lease_secs as f64;
        let retry_base = config.scheduler_retry_base_secs as f64;
        let retry_cap = config.scheduler_retry_cap_secs as f64;
        tasks.spawn("manager-scheduler", move |cancellation| {
            scheduler::run_scheduler(
                pool,
                id,
                Duration::from_secs(10),
                lease,
                retry_base,
                retry_cap,
                local_proxy,
                scheduler_admission,
                cancellation,
            )
        });
    }

    let mut failure: Option<anyhow::Error> = None;
    if let Some(outcome) = tasks.try_next_completion() {
        failure = Some(anyhow::anyhow!(
            "required task {} exited during manager startup: {:?}",
            outcome.name,
            outcome.kind
        ));
        let _ = lifecycle.request_stop(TransitionReason::TaskFailure, "manager startup failed");
    }
    if failure.is_none() {
        if open_at_startup {
            admission.open();
        }
        if let Err(error) =
            lifecycle.mark_ready("database, policy, scheduler, monitor, and gRPC services ready")
        {
            failure = Some(error.into());
            let _ =
                lifecycle.request_stop(TransitionReason::TaskFailure, "manager readiness failed");
        }
    }
    if failure.is_none() {
        if let Err(error) = notify_supervisor("READY=1") {
            failure = Some(
                anyhow::Error::new(error)
                    .context("failed to notify supervisor that manager is ready"),
            );
            let _ = lifecycle.request_stop(
                TransitionReason::TaskFailure,
                "manager supervisor readiness notification failed",
            );
        }
    }
    if failure.is_none() {
        info!(address = %addr, manager_id, "manager ready");
    }

    if failure.is_none() {
        match wait_for_shutdown_trigger(&mut lifecycle, &mut tasks, shutdown_signal_request()).await
        {
            Ok(ShutdownCause::Signal) => {}
            Ok(ShutdownCause::RequiredTask(Some(outcome))) => {
                failure = Some(anyhow::anyhow!(
                    "required task {} exited: {:?}",
                    outcome.name,
                    outcome.kind
                ));
            }
            Ok(ShutdownCause::RequiredTask(None)) => {
                failure = Some(anyhow::anyhow!("all required manager tasks exited"));
            }
            Err(error) => failure = Some(error.into()),
        }
    }

    if lifecycle.current().state != ProcessState::Stopping {
        if let Err(error) = lifecycle.request_stop(
            TransitionReason::ShutdownOrchestration,
            "manager shutdown started",
        ) {
            failure.get_or_insert_with(|| error.into());
        }
    }
    let shutdown_deadline = tokio::time::Instant::now() + SHUTDOWN_BUDGET;
    admission.close();
    if let Err(remaining) = admission.wait_for_idle(shutdown_deadline).await {
        failure.get_or_insert_with(|| {
            anyhow::anyhow!("manager drain timed out with {remaining} mutations in flight")
        });
    }
    if let Err(error) = notify_supervisor("STOPPING=1") {
        failure.get_or_insert_with(|| {
            anyhow::Error::new(error)
                .context("failed to notify supervisor that manager is stopping")
        });
    }

    match tokio::time::timeout_at(
        shutdown_deadline,
        db::deregister_manager(&db_pool, &manager_id),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            failure.get_or_insert_with(|| anyhow::anyhow!("failed to deregister manager: {error}"));
        }
        Err(_) => {
            failure.get_or_insert_with(|| anyhow::anyhow!("manager deregistration timed out"));
        }
    }

    let report = tasks.shutdown(shutdown_deadline).await;
    if !report.is_clean() {
        failure.get_or_insert_with(|| {
            anyhow::anyhow!("manager task shutdown was not clean: {report:?}")
        });
    }

    info!(manager_id, "manager stopped");
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

    #[tokio::test]
    async fn first_signal_starts_one_quiet_manager_shutdown() -> Result<()> {
        let mut lifecycle = LifecycleOwner::new(ServiceKind::Manager, "manager-test");
        let mut tasks = TaskGroup::new();
        tasks.spawn("manager-required", |mut cancellation| async move {
            cancellation.cancelled().await;
            Ok(TaskExit::Cancelled)
        });

        let cause = wait_for_shutdown_trigger(&mut lifecycle, &mut tasks, async {
            ShutdownRequest::stop(TransitionReason::SignalTerminate, "SIGTERM fixture")
        })
        .await?;
        assert_eq!(cause, ShutdownCause::Signal);
        assert_eq!(lifecycle.current().state, ProcessState::Stopping);

        let report = tasks
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(1))
            .await;
        assert!(report.is_clean(), "{report:?}");
        Ok(())
    }
}
