use anyhow::{Context, Result};
use tonic::transport::Channel;
use wr_common::wruntime::manager_service_client::ManagerServiceClient;

/// Connect to a specific manager address.
pub async fn connect(addr: &str) -> Result<ManagerServiceClient<Channel>> {
    let client = ManagerServiceClient::connect(addr.to_string()).await?;
    Ok(client)
}

/// Discover a manager gRPC address from the `wr_managers` Postgres table.
pub async fn discover_manager(database_url: &str) -> Result<String> {
    let pg_config = deadpool_postgres::Config {
        url: Some(database_url.to_string()),
        ..Default::default()
    };
    let pool = pg_config
        .create_pool(
            Some(deadpool_postgres::Runtime::Tokio1),
            tokio_postgres::NoTls,
        )
        .context("failed to create discovery pool")?;

    let client = pool.get().await.context("failed to connect to Postgres")?;
    let row = client
        .query_opt(
            "SELECT grpc_address FROM wr_managers WHERE last_heartbeat > NOW() - INTERVAL '60 seconds' ORDER BY random() LIMIT 1",
            &[],
        )
        .await
        .context("failed to query wr_managers")?
        .ok_or_else(|| anyhow::anyhow!("no active managers found in wr_managers table"))?;

    Ok(row.get::<_, String>(0))
}
