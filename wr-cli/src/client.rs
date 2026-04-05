use std::time::Duration;

use anyhow::{Context, Result};
use tonic::transport::{Channel, Endpoint};
use wr_common::wruntime::manager_service_client::ManagerServiceClient;
use wr_common::wruntime::ListManagersRequest;

/// Connect to a specific manager address with a 5-second timeout.
pub async fn connect(addr: &str) -> Result<ManagerServiceClient<Channel>> {
    let channel = Endpoint::from_shared(addr.to_string())?
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10))
        .connect()
        .await
        .context("failed to connect to manager")?;
    Ok(ManagerServiceClient::new(channel))
}

/// List all active managers in the cluster via a seed manager.
pub async fn list_managers(addr: &str) -> Result<Vec<(String, String)>> {
    let mut client = connect(addr).await?;
    let resp = client
        .list_managers(ListManagersRequest {})
        .await?
        .into_inner();
    Ok(resp
        .managers
        .into_iter()
        .map(|m| (m.manager_id, m.grpc_address))
        .collect())
}
