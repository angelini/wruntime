use anyhow::Result;
use tonic::transport::Channel;
use wr_common::wruntime::manager_service_client::ManagerServiceClient;

pub async fn connect(addr: &str) -> Result<ManagerServiceClient<Channel>> {
    let client = ManagerServiceClient::connect(addr.to_string()).await?;
    Ok(client)
}
