use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use deadpool_postgres::Pool;
use rand::seq::SliceRandom;
use tokio::sync::RwLock;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tracing::warn;

use crate::wruntime::manager_service_client::ManagerServiceClient;
use crate::wruntime::ListManagersRequest;

struct AffinityState {
    client: ManagerServiceClient<Channel>,
    established_at: Instant,
}

/// Discovers managers via a reachable manager's ListManagers RPC
/// (chitchat-reconciled), bootstrapping/falling back to the wr_managers
/// Postgres table when no manager is reachable.
pub struct ManagerDiscovery {
    pool: Pool,
    managers: RwLock<Vec<String>>,
    affinity: RwLock<Option<AffinityState>>,
    tls_config: Option<ClientTlsConfig>,
    rpc_failures: AtomicU32,
}

impl ManagerDiscovery {
    const AFFINITY_DURATION: Duration = Duration::from_secs(120);
    const MAX_QUIET_FAILURES: u32 = 3;

    pub fn new(pool: Pool, tls_config: Option<ClientTlsConfig>) -> Self {
        Self {
            pool,
            managers: RwLock::new(Vec::new()),
            affinity: RwLock::new(None),
            tls_config,
            rpc_failures: AtomicU32::new(0),
        }
    }

    /// Refresh the cached manager list. Bootstrap from the `wr_managers` table on
    /// cold start, then prefer the chitchat-reconciled `ListManagers` view via a
    /// reachable manager. Fall back to a direct DB query ONLY when no manager RPC
    /// is reachable (or it returns empty). Keeps the previous cache on error.
    pub async fn refresh(&self) {
        // Cold start: seed from DB so `get_client` has a target to connect to.
        if self.managers.read().await.is_empty() {
            self.refresh_from_db_fallback().await;
        }

        // Steady state: trust the manager-side reconciliation.
        if self.refresh_from_managers().await {
            self.rpc_failures.store(0, Ordering::Relaxed);
            return;
        }

        // Fallback trigger = no manager client reachable / ListManagers failed or
        // empty (NOT chitchat live-node counts). Warn only on repeated failures.
        let failures = self.rpc_failures.fetch_add(1, Ordering::Relaxed) + 1;
        if failures >= Self::MAX_QUIET_FAILURES {
            warn!(
                consecutive_failures = failures,
                "manager discovery could not reach any manager RPC; using direct-DB fallback",
            );
        }
        self.refresh_from_db_fallback().await;
    }

    /// Refresh the cache from a reachable manager's `ListManagers`. Returns true iff
    /// the cache was replaced with a non-empty reconciled result.
    async fn refresh_from_managers(&self) -> bool {
        let mut client = match self.get_client().await {
            Ok(c) => c,
            Err(_) => return false,
        };
        let resp = match client.list_managers(ListManagersRequest {}).await {
            Ok(r) => r,
            Err(_) => {
                // Cached affinity target is unhealthy — drop it so the next attempt
                // reconnects to a different manager.
                self.clear_affinity().await;
                return false;
            }
        };
        let addrs: Vec<String> = resp
            .into_inner()
            .managers
            .into_iter()
            .map(|m| m.grpc_address)
            .filter(|a| !a.is_empty())
            .collect();
        if addrs.is_empty() {
            return false;
        }
        *self.managers.write().await = addrs;
        true
    }

    /// Direct `wr_managers` query — bootstrap/fallback path only. Keeps the previous
    /// cache when the query errors or returns empty.
    async fn refresh_from_db_fallback(&self) {
        match self.query_managers().await {
            Ok(addrs) if !addrs.is_empty() => {
                *self.managers.write().await = addrs;
            }
            Ok(_) => {}
            Err(e) => {
                warn!(error = %e, "manager discovery DB fallback query failed");
            }
        }
    }

    /// Get a connected gRPC client, reusing the sticky affinity if still valid.
    /// On connection failure, tries all managers in shuffled order.
    /// Returns error only if ALL managers are unreachable.
    pub async fn get_client(&self) -> Result<ManagerServiceClient<Channel>, tonic::Status> {
        // Check existing affinity
        {
            let affinity = self.affinity.read().await;
            if let Some(state) = affinity.as_ref() {
                if state.established_at.elapsed() < Self::AFFINITY_DURATION {
                    return Ok(state.client.clone());
                }
            }
        }

        // Affinity expired or absent — establish new connection
        let client = self.connect_new().await?;

        *self.affinity.write().await = Some(AffinityState {
            client: client.clone(),
            established_at: Instant::now(),
        });

        Ok(client)
    }

    /// Clear the sticky affinity so the next `get_client` call picks a fresh manager.
    pub async fn clear_affinity(&self) {
        *self.affinity.write().await = None;
    }

    /// Spawn a background task that refreshes the manager list every 30 seconds.
    pub fn spawn_refresh_task(self: &Arc<Self>) {
        let discovery = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                interval.tick().await;
                discovery.refresh().await;
            }
        });
    }

    async fn connect_new(&self) -> Result<ManagerServiceClient<Channel>, tonic::Status> {
        let managers = self.managers.read().await;
        if managers.is_empty() {
            return Err(tonic::Status::unavailable(
                "no managers discovered — is wr_managers table populated?",
            ));
        }

        // Shuffle a copy for round-robin with jitter
        let mut shuffled = managers.clone();
        drop(managers);
        shuffled.shuffle(&mut rand::rng());

        let mut last_err: Option<String> = None;
        for addr in &shuffled {
            let result = match &self.tls_config {
                Some(tls) => {
                    let ep = Endpoint::from_shared(addr.clone())
                        .and_then(|ep| ep.tls_config(tls.clone()));
                    match ep {
                        Ok(ep) => ep.connect().await.map(ManagerServiceClient::new),
                        Err(e) => Err(e),
                    }
                }
                None => ManagerServiceClient::connect(addr.clone()).await,
            };
            match result {
                Ok(client) => return Ok(client),
                Err(e) => {
                    warn!(address = %addr, error = %e, "manager connection failed, trying next");
                    last_err = Some(e.to_string());
                }
            }
        }

        Err(tonic::Status::unavailable(format!(
            "all {} managers unreachable: {}",
            shuffled.len(),
            last_err.unwrap_or_else(|| "unknown".into()),
        )))
    }

    async fn query_managers(
        &self,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "SELECT grpc_address FROM wr_managers WHERE last_heartbeat > NOW() - INTERVAL '60 seconds'",
                &[],
            )
            .await
            .map_err(|e| {
                // Walk the source chain — tokio_postgres prints "db error"
                // but the real message is in the cause.
                use std::error::Error;
                let mut msg = e.to_string();
                let mut source = e.source();
                while let Some(cause) = source {
                    msg.push_str(": ");
                    msg.push_str(&cause.to_string());
                    source = cause.source();
                }
                msg
            })?;
        Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
    }
}
