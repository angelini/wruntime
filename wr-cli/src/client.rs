use anyhow::Result;
use wr_common::wruntime::manager_service_client::ManagerServiceClient;
use tonic::transport::Channel;

pub async fn connect(addr: &str) -> Result<ManagerServiceClient<Channel>> {
    let client = ManagerServiceClient::connect(addr.to_string()).await?;
    Ok(client)
}
