use anyhow::Result;
use tonic::transport::Channel;
use wr_common::wruntime::manager_service_client::ManagerServiceClient;
use wr_common::wruntime::ListManagersRequest;

/// Connect to a specific manager address.
pub async fn connect(addr: &str) -> Result<ManagerServiceClient<Channel>> {
    let client = ManagerServiceClient::connect(addr.to_string()).await?;
    Ok(client)
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
