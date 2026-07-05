use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use tracing::{info, warn};
use wr_common::discovery::ManagerDiscovery;
use wr_common::wruntime::{manager_service_client::ManagerServiceClient, GetRoutingTableRequest};

use crate::circuit_breaker::CircuitBreakerRegistry;
use crate::indexed_routing::IndexedRoutingTable;

pub type CachedRoutingTable = Arc<RwLock<IndexedRoutingTable>>;

pub fn new_routing_table() -> CachedRoutingTable {
    Arc::new(RwLock::new(IndexedRoutingTable::empty()))
}

/// Perform a single routing-table sync from wr-manager.
pub async fn sync_once(
    client: &mut ManagerServiceClient<tonic::transport::Channel>,
    table: &CachedRoutingTable,
    cb_registry: &CircuitBreakerRegistry,
    self_peer_address: &str,
) -> Result<(), tonic::Status> {
    let known_version = table.read().await.version;
    let resp = client
        .get_routing_table(GetRoutingTableRequest { known_version })
        .await?;
    if let Some(incoming) = resp.into_inner().table {
        let version = incoming.version;
        let mut guard = table.write().await;
        let indexed = IndexedRoutingTable::from_proto(&incoming, Some(&*guard));
        {
            let active = indexed.active_forward_addrs(self_peer_address);
            cb_registry.evict_missing(&active);
        }
        *guard = indexed;
        drop(guard);
        info!(version, "routing table updated");
    }
    Ok(())
}

/// Background task: polls a random manager for the routing table via discovery.
pub async fn sync_routing_table(
    discovery: Arc<ManagerDiscovery>,
    table: CachedRoutingTable,
    ttl_secs: u64,
    cb_registry: Arc<CircuitBreakerRegistry>,
    self_peer_address: String,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(ttl_secs));
    loop {
        interval.tick().await;
        match discovery.get_client().await {
            Ok(mut client) => {
                if let Err(e) =
                    sync_once(&mut client, &table, &cb_registry, &self_peer_address).await
                {
                    warn!(error = %e, "routing table sync failed");
                }
            }
            Err(e) => {
                warn!(error = %e, "routing table sync: all managers unreachable");
            }
        }
    }
}
