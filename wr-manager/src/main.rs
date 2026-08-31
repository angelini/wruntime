pub mod cluster;
pub mod config;
pub mod crypto;
pub mod db;
pub mod migrate;
pub mod pool;
pub mod scheduler;
pub mod service;
pub mod state;
pub mod status;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Endpoint, Server};
use tracing::{info, warn};
use uuid::Uuid;
use wr_common::lifecycle_service::{notify_supervisor, AdmissionGate, LifecycleServiceAdapter};
use wr_common::process_lifecycle::{
    ProcessLifecycleCoordinator, ProcessState, ServiceKind, TransitionReason,
};
use wr_common::signal::{apply_shutdown_request, shutdown_signal_request};
use wr_common::task_group::{TaskExit, TaskGroup};
use wr_common::wruntime::lifecycle_service_client::LifecycleServiceClient;
use wr_common::wruntime::lifecycle_service_server::LifecycleServiceServer;
use wr_common::wruntime::manager_service_server::ManagerServiceServer;
use wr_common::wruntime::{GetLifecycleStatusRequest, ProcessLifecycleState};

const SHUTDOWN_BUDGET: Duration = Duration::from_secs(30);

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
    let finalized = telemetry.finalize();
    if !finalized.is_success() && result.is_ok() {
        anyhow::bail!("telemetry finalization failed: {:?}", finalized.failures);
    }
    result
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
    let tls = wr_common::tls::build_tonic_client_tls(&config.tls)?;
    let channel = Endpoint::from_shared(endpoint)?
        .tls_config(tls)?
        .connect()
        .await?;
    let status = LifecycleServiceClient::new(channel)
        .get_status(GetLifecycleStatusRequest {})
        .await?
        .into_inner()
        .status
        .context("lifecycle response omitted status")?;
    anyhow::ensure!(
        status.state == ProcessLifecycleState::Ready as i32,
        "manager lifecycle state is not READY"
    );
    Ok(())
}

async fn run_service(config_path: &str) -> Result<()> {
    let manager_id = Uuid::new_v4().to_string();
    let lifecycle = ProcessLifecycleCoordinator::new(
        ServiceKind::Manager,
        wr_common::process_lifecycle::resolve_process_instance_id(manager_id.clone()),
    );
    let admission = AdmissionGate::closed();
    let config = config::ManagerConfig::load(config_path)?;
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
    let gossip_address = config.cluster.gossip_listen_address.clone();
    let peers = db::list_managers(&db_pool)
        .await
        .map_err(|error| anyhow::anyhow!("failed to list managers: {error}"))?;
    let seed_addrs = peers
        .iter()
        .filter(|peer| peer.manager_id != manager_id)
        .map(|peer| peer.gossip_address.clone())
        .collect();
    let gossip_listen = config.cluster.gossip_listen_address.parse()?;
    let crypto = Arc::new(crypto::SecretCrypto::from_env()?);
    let incoming = TcpIncoming::bind(addr).context("failed to bind manager gRPC listener")?;
    let tls = wr_common::tls::build_tonic_server_tls(&config.tls)
        .map_err(|error| anyhow::anyhow!("failed to build TLS config: {error}"))?;
    let mut server = Server::builder()
        .tls_config(tls)
        .context("failed to apply TLS config")?;

    // Start gossip only after all other fallible boot prerequisites have
    // completed. From this point onward, every failure path explicitly
    // terminates gossip and removes the manager registration.
    let cluster = Arc::new(
        cluster::ClusterHandle::new(
            &manager_id,
            &config.cluster.cluster_id,
            gossip_listen,
            seed_addrs,
            Duration::from_millis(config.cluster.gossip_interval_ms),
            chitchat::FailureDetectorConfig::default(),
        )
        .await?,
    );
    cluster
        .publish_metadata(&grpc_address, &gossip_address)
        .await;

    let manager = service::Manager::with_admission(
        db_pool.clone(),
        crypto,
        Arc::clone(&cluster),
        admission.clone(),
    );
    let lifecycle_service =
        LifecycleServiceAdapter::new(lifecycle.clone(), Some(admission.clone()));
    let router = server
        .add_service(ManagerServiceServer::new(manager))
        .add_service(LifecycleServiceServer::new(lifecycle_service));

    if let Err(error) =
        db::register_manager(&db_pool, &manager_id, &grpc_address, &gossip_address).await
    {
        if let Err(shutdown_error) = cluster.initiate_shutdown() {
            warn!(%shutdown_error, "failed to stop gossip after manager registration failure");
        }
        let _ = tokio::time::timeout(SHUTDOWN_BUDGET, cluster.wait_for_termination()).await;
        return Err(anyhow::anyhow!("failed to register manager: {error}"));
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
        tasks.spawn(
            "manager-self-heartbeat",
            move |mut cancellation| async move {
                let mut interval = tokio::time::interval(Duration::from_secs(15));
                loop {
                    tokio::select! {
                        _ = cancellation.cancelled() => return Ok(TaskExit::Cancelled),
                        _ = interval.tick() => {}
                    }
                    if !admission.is_open() {
                        continue;
                    }
                    db::heartbeat_manager(&pool, &id)
                        .await
                        .map_err(|error| anyhow::anyhow!("manager heartbeat failed: {error}"))?;
                    db::cleanup_stale_managers(&pool, 60)
                        .await
                        .map_err(|error| {
                            anyhow::anyhow!("stale manager cleanup failed: {error}")
                        })?;
                }
            },
        );
    }

    {
        let cluster = Arc::clone(&cluster);
        tasks.spawn(
            "manager-membership-watcher",
            move |mut cancellation| async move {
                let mut watcher = cluster.live_nodes_watcher().await;
                let mut known: HashSet<String> = watcher
                    .borrow_and_update()
                    .keys()
                    .map(|id| id.node_id.to_string())
                    .collect();
                loop {
                    tokio::select! {
                        _ = cancellation.cancelled() => return Ok(TaskExit::Cancelled),
                        changed = watcher.changed() => {
                            if changed.is_err() {
                                anyhow::bail!("membership watcher closed unexpectedly");
                            }
                        }
                    }
                    let current: HashSet<String> = watcher
                        .borrow()
                        .keys()
                        .map(|id| id.node_id.to_string())
                        .collect();
                    for id in current.difference(&known) {
                        info!(manager_id = %id, "manager joined cluster");
                    }
                    for id in known.difference(&current) {
                        info!(manager_id = %id, "manager left cluster");
                    }
                    known = current;
                }
            },
        );
    }

    {
        let cluster = Arc::clone(&cluster);
        tasks.spawn("manager-gossip", move |mut cancellation| async move {
            tokio::select! {
                result = cluster.wait_for_termination() => {
                    result?;
                    Ok(TaskExit::Completed)
                }
                _ = cancellation.cancelled() => {
                    cluster.initiate_shutdown()?;
                    cluster.wait_for_termination().await?;
                    Ok(TaskExit::Cancelled)
                }
            }
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
    admission.open();
    if let Err(error) =
        lifecycle.mark_ready("database, gossip, scheduler, monitor, and gRPC listener ready")
    {
        failure = Some(error.into());
        let _ = lifecycle.request_stop(TransitionReason::TaskFailure, "manager readiness failed");
    } else if let Err(error) = notify_supervisor("READY=1") {
        failure = Some(
            anyhow::Error::new(error).context("failed to notify supervisor that manager is ready"),
        );
        let _ = lifecycle.request_stop(
            TransitionReason::TaskFailure,
            "manager supervisor readiness notification failed",
        );
    } else {
        info!(address = %addr, manager_id, "manager ready");
    }

    let mut updates = lifecycle.handle().subscribe();
    if failure.is_none() {
        loop {
            tokio::select! {
                request = shutdown_signal_request() => {
                    if let Err(error) = apply_shutdown_request(&lifecycle, request) {
                        failure = Some(error.into());
                        let _ = lifecycle.request_stop(
                            TransitionReason::TaskFailure,
                            "manager shutdown transition failed",
                        );
                    }
                    break;
                }
                changed = updates.changed() => {
                    if changed.is_err() {
                        anyhow::bail!("lifecycle coordinator closed unexpectedly");
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
                        failure = Some(anyhow::anyhow!("all required manager tasks exited"));
                        let _ = lifecycle.request_stop(
                            TransitionReason::TaskFailure,
                            "all required manager tasks exited",
                        );
                    }
                    break;
                }
            }
        }
    }

    let mut shutdown_deadline = tokio::time::Instant::now() + SHUTDOWN_BUDGET;
    admission.close();
    if let Err(remaining) = admission.wait_for_idle(shutdown_deadline).await {
        failure.get_or_insert_with(|| {
            anyhow::anyhow!("manager drain timed out with {remaining} mutations in flight")
        });
    }

    if failure.is_none() && lifecycle.current().state == ProcessState::Draining {
        loop {
            tokio::select! {
                request = shutdown_signal_request() => {
                    if let Err(error) = apply_shutdown_request(&lifecycle, request) {
                        failure = Some(error.into());
                        let _ = lifecycle.request_stop(
                            TransitionReason::TaskFailure,
                            "manager stop transition failed",
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
                        failure = Some(anyhow::anyhow!("required task {} exited while manager was drained: {:?}", outcome.name, outcome.kind));
                        let _ = lifecycle.request_stop(TransitionReason::TaskFailure, outcome.name);
                    } else {
                        failure = Some(anyhow::anyhow!("all required manager tasks exited while drained"));
                        let _ = lifecycle.request_stop(
                            TransitionReason::TaskFailure,
                            "all required manager tasks exited while drained",
                        );
                    }
                    break;
                }
            }
        }
        // A later Stop request starts its own final-stop budget; the completed
        // drain operation did not imply process exit.
        shutdown_deadline = tokio::time::Instant::now() + SHUTDOWN_BUDGET;
    }

    if lifecycle.current().state != ProcessState::Stopping {
        if let Err(error) = lifecycle.request_stop(
            TransitionReason::ShutdownOrchestration,
            "manager drain complete",
        ) {
            failure.get_or_insert_with(|| error.into());
        }
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
