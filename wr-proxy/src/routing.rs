use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use tracing::{info, warn};
use wr_common::discovery::ManagerDiscovery;
use wr_common::wruntime::{
    manager_service_client::ManagerServiceClient, GetRoutingTableRequest, RoutingTable,
};

use crate::circuit_breaker::CircuitBreakerRegistry;

pub type CachedRoutingTable = Arc<RwLock<RoutingTable>>;

pub fn new_routing_table() -> CachedRoutingTable {
    Arc::new(RwLock::new(RoutingTable {
        rules: vec![],
        version: 0,
    }))
}

/// Perform a single routing-table sync from wr-manager.
pub async fn sync_once(
    client: &mut ManagerServiceClient<tonic::transport::Channel>,
    table: &CachedRoutingTable,
    cb_registry: &CircuitBreakerRegistry,
) -> Result<(), tonic::Status> {
    let resp = client.get_routing_table(GetRoutingTableRequest {}).await?;
    if let Some(incoming) = resp.into_inner().table {
        let current_version = table.read().await.version;
        if incoming.version > current_version {
            let version = incoming.version;
            let active: HashSet<&str> = incoming
                .rules
                .iter()
                .map(|r| r.engine_address.as_str())
                .collect();
            cb_registry.evict_missing(&active);
            *table.write().await = incoming;
            info!(version, "routing table updated");
        }
    }
    Ok(())
}

/// Background task: polls a random manager for the routing table via discovery.
pub async fn sync_routing_table(
    discovery: Arc<ManagerDiscovery>,
    table: CachedRoutingTable,
    ttl_secs: u64,
    cb_registry: Arc<CircuitBreakerRegistry>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(ttl_secs));
    loop {
        interval.tick().await;
        match discovery.get_client().await {
            Ok(mut client) => {
                if let Err(e) = sync_once(&mut client, &table, &cb_registry).await {
                    warn!(error = %e, "routing table sync failed");
                }
            }
            Err(e) => {
                warn!(error = %e, "routing table sync: all managers unreachable");
            }
        }
    }
}
