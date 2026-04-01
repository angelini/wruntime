pub mod config;
pub mod db;
pub mod migrate;
pub mod pool;
pub mod service;
pub mod state;

use anyhow::Result;
use tonic::transport::Server;
use tracing::info;
use wr_common::wruntime::manager_service_server::ManagerServiceServer;

#[tokio::main]
async fn main() -> Result<()> {
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

    let shared = state::new_state();
    let manager = service::Manager::new(shared.clone(), db_pool.clone());

    // Monitor for engines that miss their heartbeat deadline
    tokio::spawn(state::monitor_heartbeats(
        shared,
        db_pool,
        config.engine_heartbeat_timeout_secs,
    ));

    info!(address = %addr, "manager listening");

    use tokio::signal::unix::{signal, SignalKind};
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;
    let shutdown = async move {
        tokio::select! {
            _ = sigint.recv()  => {},
            _ = sigterm.recv() => {},
        }
    };

    Server::builder()
        .add_service(ManagerServiceServer::new(manager))
        .serve_with_shutdown(addr, shutdown)
        .await?;

    info!("manager shutting down");
    Ok(())
}
