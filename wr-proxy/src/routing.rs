use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, RwLock, RwLockReadGuard};
use tokio::time::Instant;
use tracing::{info, warn};
use wr_common::discovery::ManagerDiscovery;
use wr_common::task_group::{TaskCancellation, TaskExit};
use wr_common::wruntime::{
    manager_service_client::ManagerServiceClient, GetRoutingTableRequest, RoutingTable,
};

use crate::circuit_breaker::CircuitBreakerRegistry;
use crate::config::CircuitBreakerConfig;
use crate::indexed_routing::IndexedRoutingTable;

/// Coupled routing snapshot, refresh serialization, and breaker ownership.
#[derive(Clone)]
pub struct CachedRoutingTable {
    table: Arc<RwLock<IndexedRoutingTable>>,
    refresh: Arc<Mutex<()>>,
    registry: Arc<CircuitBreakerRegistry>,
    self_peer_address: Arc<str>,
}

impl CachedRoutingTable {
    pub fn new(config: CircuitBreakerConfig, self_peer_address: impl Into<Arc<str>>) -> Self {
        Self {
            table: Arc::new(RwLock::new(IndexedRoutingTable::empty())),
            refresh: Arc::new(Mutex::new(())),
            registry: Arc::new(CircuitBreakerRegistry::new(config)),
            self_peer_address: self_peer_address.into(),
        }
    }

    pub async fn read(&self) -> RwLockReadGuard<'_, IndexedRoutingTable> {
        self.table.read().await
    }

    pub async fn version(&self) -> u64 {
        self.table.read().await.version
    }

    pub fn open_duration_secs(&self) -> u64 {
        self.registry.open_duration_secs()
    }

    /// Publish a newer manager table and evict breaker membership afterward.
    pub async fn replace(&self, incoming: &RoutingTable) -> bool {
        let _refresh = self.refresh.lock().await;
        if incoming.version <= self.version().await {
            return false;
        }

        let mut indexed = IndexedRoutingTable::from_proto(
            incoming,
            None,
            &self.registry,
            &self.self_peer_address,
        );
        let active = indexed.active_forward_addrs().clone();

        let mut current = self.table.write().await;
        if incoming.version <= current.version {
            return false;
        }
        indexed.seed_counters_from(&current);
        *current = indexed;
        drop(current);

        self.registry.evict_missing(&active);
        true
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn resolver_calls(&self) -> usize {
        self.registry.resolver_calls()
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn reset_resolver_calls(&self) {
        self.registry.reset_resolver_calls();
    }
}

pub fn new_routing_table(
    config: CircuitBreakerConfig,
    self_peer_address: impl Into<Arc<str>>,
) -> CachedRoutingTable {
    CachedRoutingTable::new(config, self_peer_address)
}

/// Perform a single routing-table sync from wr-manager.
pub async fn sync_once(
    client: &mut ManagerServiceClient<tonic::transport::Channel>,
    table: &CachedRoutingTable,
) -> Result<(), tonic::Status> {
    let known_version = table.version().await;
    let response = client
        .get_routing_table(GetRoutingTableRequest { known_version })
        .await?;
    if let Some(incoming) = response.into_inner().table {
        let version = incoming.version;
        if table.replace(&incoming).await {
            info!(version, "routing table updated");
        }
    }
    Ok(())
}

/// Synchronize until the local snapshot contains at least `target_version`.
pub async fn converge_to_version(
    discovery: &ManagerDiscovery,
    table: &CachedRoutingTable,
    target_version: u64,
    deadline: Instant,
) -> Result<u64, tonic::Status> {
    loop {
        let local_version = table.version().await;
        if local_version >= target_version {
            return Ok(local_version);
        }
        if Instant::now() >= deadline {
            return Err(tonic::Status::deadline_exceeded(format!(
                "routing convergence timed out: manager version {target_version}, local version {local_version}"
            )));
        }

        let mut client = discovery.get_client().await?;
        sync_once(&mut client, table).await?;
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
            _ = tokio::time::sleep_until(deadline) => {
                let local_version = table.version().await;
                return Err(tonic::Status::deadline_exceeded(format!(
                    "routing convergence timed out: manager version {target_version}, local version {local_version}"
                )));
            }
        }
    }
}

/// Background task: polls a manager for the routing table until cancellation.
pub async fn sync_routing_table(
    discovery: Arc<ManagerDiscovery>,
    table: CachedRoutingTable,
    ttl_secs: u64,
    mut cancellation: TaskCancellation,
) -> anyhow::Result<TaskExit> {
    let mut interval = tokio::time::interval(Duration::from_secs(ttl_secs));
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return Ok(TaskExit::Cancelled),
            _ = interval.tick() => {}
        }
        match discovery.get_client().await {
            Ok(mut client) => {
                if let Err(error) = sync_once(&mut client, &table).await {
                    warn!(%error, "routing table sync failed");
                }
            }
            Err(error) => {
                warn!(%error, "routing table sync: all managers unreachable");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wr_common::wruntime::RoutingRule;

    fn rule(address: &str) -> RoutingRule {
        RoutingRule {
            rule_id: "rule".into(),
            source_module: String::new(),
            destination_module: "svc".into(),
            engine_id: "engine".into(),
            engine_address: address.into(),
            destination_version: "1.0.0".into(),
            healthy: true,
            source_namespace: String::new(),
            destination_namespace: "ns".into(),
            peer_address: "https://self:9443".into(),
        }
    }

    #[tokio::test]
    async fn refresh_preserves_active_breaker_and_eviction_is_reset_boundary() {
        let cache = CachedRoutingTable::new(
            CircuitBreakerConfig {
                failure_threshold: 1,
                open_duration_secs: 30,
            },
            "https://self:9443",
        );
        let initial = RoutingTable {
            rules: vec![rule("http://engine")],
            version: 1,
        };
        assert!(cache.replace(&initial).await);
        let old = cache.read().await.get("ns", "svc").unwrap().candidates[0]
            .breaker
            .clone();
        old.on_error();

        let retained = RoutingTable {
            rules: vec![rule("http://engine")],
            version: 2,
        };
        assert!(cache.replace(&retained).await);
        let retained_handle = cache.read().await.get("ns", "svc").unwrap().candidates[0]
            .breaker
            .clone();
        assert!(!retained_handle.is_call_permitted());

        assert!(
            cache
                .replace(&RoutingTable {
                    rules: Vec::new(),
                    version: 3,
                })
                .await
        );
        assert!(
            cache
                .replace(&RoutingTable {
                    rules: vec![rule("http://engine")],
                    version: 4,
                })
                .await
        );
        let fresh = cache.read().await.get("ns", "svc").unwrap().candidates[0]
            .breaker
            .clone();
        assert!(fresh.is_call_permitted());
        assert!(!old.is_call_permitted());
    }

    #[tokio::test]
    async fn replace_rejects_stale_versions_and_resolves_only_on_publish_attempt() {
        let cache = CachedRoutingTable::new(CircuitBreakerConfig::default(), "https://self:9443");
        let current = RoutingTable {
            rules: vec![rule("http://engine")],
            version: 2,
        };
        assert!(cache.replace(&current).await);
        cache.reset_resolver_calls();

        let stale = RoutingTable {
            rules: vec![rule("http://other")],
            version: 1,
        };
        assert!(!cache.replace(&stale).await);
        assert_eq!(cache.version().await, 2);
        assert_eq!(cache.resolver_calls(), 0);
    }
}
