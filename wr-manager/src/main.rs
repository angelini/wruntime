pub mod cluster;
pub mod config;
pub mod crypto;
pub mod db;
pub mod migrate;
pub mod pool;
pub mod scheduler;
pub mod service;
pub mod state;

use std::sync::Arc;

use anyhow::Result;
use tonic::transport::Server;
use tracing::{info, warn};
use uuid::Uuid;
use wr_common::wruntime::manager_service_server::ManagerServiceServer;

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    let _telemetry = wr_common::telemetry::init("wr-manager")?;

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "manager.toml".to_string());

    let config = config::ManagerConfig::load(&config_path)?;
    let addr = config.listen_address.parse()?;

    // Database
    let db_pool = pool::build_pool(&config.database.url, config.database.max_connections)?;
    let client = db_pool.get().await?;
    migrate::run_migrations(&client).await?;
    drop(client);

    // ── Cluster self-registration ────────────────────────────────────────
    let manager_id = Uuid::new_v4().to_string();
    let grpc_address = config
        .cluster
        .advertise_grpc_address
        .clone()
        .unwrap_or_else(|| {
            // Replace 0.0.0.0 with 127.0.0.1 — 0.0.0.0 is a bind address, not connectable
            let addr = config.listen_address.replace("0.0.0.0", "127.0.0.1");
            format!("https://{addr}")
        });
    let gossip_address = config.cluster.gossip_listen_address.clone();

    db::register_manager(&db_pool, &manager_id, &grpc_address, &gossip_address)
        .await
        .map_err(|e| anyhow::anyhow!("failed to register manager: {e}"))?;
    info!(
        manager_id,
        grpc_address, gossip_address, "manager registered in cluster"
    );

    // Background: heartbeat self + cleanup stale managers
    {
        let pool = db_pool.clone();
        let mid = manager_id.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(15));
            loop {
                interval.tick().await;
                if let Err(e) = db::heartbeat_manager(&pool, &mid).await {
                    warn!(error = %e, "manager heartbeat failed");
                }
                if let Err(e) = db::cleanup_stale_managers(&pool, 60).await {
                    warn!(error = %e, "stale manager cleanup failed");
                }
            }
        });
    }

    // ── Bootstrap chitchat ─────────────────────────────────────────────
    let peers = db::list_managers(&db_pool)
        .await
        .map_err(|e| anyhow::anyhow!("failed to list managers: {e}"))?;
    let seed_addrs: Vec<String> = peers
        .iter()
        .filter(|p| p.manager_id != manager_id)
        .map(|p| p.gossip_address.clone())
        .collect();

    let gossip_listen: std::net::SocketAddr = config.cluster.gossip_listen_address.parse()?;
    let cluster_handle = Arc::new(
        cluster::ClusterHandle::new(
            &manager_id,
            &config.cluster.cluster_id,
            gossip_listen,
            seed_addrs,
            std::time::Duration::from_millis(config.cluster.gossip_interval_ms),
        )
        .await?,
    );
    info!(
        "chitchat gossip started on {}",
        config.cluster.gossip_listen_address
    );

    let crypto = Arc::new(crypto::SecretCrypto::from_env()?);
    let manager = service::Manager::new(db_pool.clone(), crypto);

    // Monitor for engines that miss their heartbeat deadline
    let monitor_handle = tokio::spawn(state::monitor_heartbeats(
        db_pool.clone(),
        config.engine_heartbeat_timeout_secs,
        std::time::Duration::from_secs(5),
    ));

    // Evaluate scheduled jobs
    let scheduler_handle = tokio::spawn(scheduler::run_scheduler(
        db_pool.clone(),
        std::time::Duration::from_secs(10),
    ));

    info!(address = %addr, "manager listening");

    let shutdown = wr_common::signal::shutdown_signal();

    let mut server = Server::builder();
    let tls_config = wr_common::tls::build_tonic_server_tls(&config.tls)
        .map_err(|e| anyhow::anyhow!("failed to build TLS config: {e}"))?;
    server = server
        .tls_config(tls_config)
        .map_err(|e| anyhow::anyhow!("failed to apply TLS config: {e}"))?;

    server
        .add_service(ManagerServiceServer::new(manager))
        .serve_with_shutdown(addr, shutdown)
        .await?;

    // ── Graceful shutdown ─────────────────────────────────────────────────
    monitor_handle.abort();
    scheduler_handle.abort();

    if let Err(e) = db::deregister_manager(&db_pool, &manager_id).await {
        warn!(error = %e, "failed to deregister manager");
    } else {
        info!(manager_id, "manager deregistered from cluster");
    }

    // Shut down chitchat after aborting tasks that hold references
    match Arc::try_unwrap(cluster_handle) {
        Ok(cluster) => cluster.shutdown().await,
        Err(_) => warn!("skipping chitchat shutdown — references still held"),
    }

    info!("manager shutting down");
    Ok(())
}
