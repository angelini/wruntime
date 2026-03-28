mod config;
mod service;
mod state;

use anyhow::Result;
use tonic::transport::Server;
use tracing::info;
use wr_common::wruntime::manager_service_server::ManagerServiceServer;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "manager.toml".to_string());

    let config = config::ManagerConfig::load(&config_path)?;
    let addr = config.listen_address.parse()?;

    let shared = state::new_state();
    let manager = service::Manager::new(shared.clone());

    // Monitor for engines that miss their heartbeat deadline
    tokio::spawn(state::monitor_heartbeats(
        shared,
        config.engine_heartbeat_timeout_secs,
    ));

    info!(address = %addr, "manager listening");

    Server::builder()
        .add_service(ManagerServiceServer::new(manager))
        .serve(addr)
        .await?;

    Ok(())
}
