use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use wr_common::wruntime::{
    manager_service_client::ManagerServiceClient, GetRoutingTableRequest, RoutingTable,
};

pub type CachedRoutingTable = Arc<RwLock<RoutingTable>>;

pub fn new_routing_table() -> CachedRoutingTable {
    Arc::new(RwLock::new(RoutingTable { rules: vec![], version: 0 }))
}

/// Background task: polls wr-manager for the routing table and updates the
/// local cache whenever the version number increments.
pub async fn sync_routing_table(
    mut client: ManagerServiceClient<tonic::transport::Channel>,
    table: CachedRoutingTable,
    ttl_secs: u64,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(ttl_secs));
    loop {
        interval.tick().await;
        match client.get_routing_table(GetRoutingTableRequest {}).await {
            Ok(resp) => {
                if let Some(incoming) = resp.into_inner().table {
                    let current_version = table.read().await.version;
                    if incoming.version > current_version {
                        *table.write().await = incoming;
                        println!("[proxy] routing table updated");
                    }
                }
            }
            Err(e) => eprintln!("[proxy] routing table sync failed: {e}"),
        }
    }
}
