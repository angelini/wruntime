use std::sync::atomic::{AtomicU32, Ordering};
#[cfg(feature = "signal")]
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
    endpoint: String,
    client: ManagerServiceClient<Channel>,
    established_at: Instant,
}

/// Discovers managers via a reachable manager's `ListManagers` RPC,
/// bootstrapping/falling back to the `wr_managers` PostgreSQL lease table when
/// no manager is reachable.
pub struct ManagerDiscovery {
    pool: Pool,
    managers: RwLock<Vec<String>>,
    affinity: RwLock<Option<AffinityState>>,
    tls_config: Option<ClientTlsConfig>,
    manager_liveness_threshold_secs: u64,
    rpc_failures: AtomicU32,
}

impl ManagerDiscovery {
    const AFFINITY_DURATION: Duration = Duration::from_secs(120);
    const MAX_QUIET_FAILURES: u32 = 3;

    pub fn new(
        pool: Pool,
        tls_config: Option<ClientTlsConfig>,
        manager_liveness_threshold_secs: u64,
    ) -> Self {
        Self {
            pool,
            managers: RwLock::new(Vec::new()),
            affinity: RwLock::new(None),
            tls_config,
            manager_liveness_threshold_secs,
            rpc_failures: AtomicU32::new(0),
        }
    }

    /// Refresh the cached manager list. Bootstrap from the `wr_managers` table on
    /// cold start, then prefer the lease-filtered `ListManagers` view via a
    /// reachable manager. Fall back to a direct DB query only when no manager RPC
    /// is reachable. Successful empty results authoritatively clear cached state;
    /// transport/query errors preserve it.
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

        // Fallback trigger = no manager client reachable / ListManagers failed.
        // Warn only on repeated failures.
        let failures = self.rpc_failures.fetch_add(1, Ordering::Relaxed) + 1;
        if failures >= Self::MAX_QUIET_FAILURES {
            warn!(
                consecutive_failures = failures,
                "manager discovery could not reach any manager RPC; using direct-DB fallback",
            );
        }
        self.refresh_from_db_fallback().await;
    }

    /// Refresh the cache from a reachable manager's `ListManagers`. Returns true
    /// when the RPC succeeded, including an authoritative empty live set.
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
        self.replace_managers(addrs).await;
        true
    }

    /// Direct `wr_managers` query — bootstrap/fallback path only. A successful
    /// query replaces the cache even when no lease is fresh; errors preserve it.
    async fn refresh_from_db_fallback(&self) {
        match self.query_managers().await {
            Ok(addrs) => self.replace_managers(addrs).await,
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
        let (endpoint, client) = self.connect_new().await?;

        *self.affinity.write().await = Some(AffinityState {
            endpoint,
            client: client.clone(),
            established_at: Instant::now(),
        });

        Ok(client)
    }

    /// Clear the sticky affinity so the next `get_client` call picks a fresh manager.
    pub async fn clear_affinity(&self) {
        *self.affinity.write().await = None;
    }

    async fn replace_managers(&self, managers: Vec<String>) {
        let mut affinity = self.affinity.write().await;
        if affinity
            .as_ref()
            .is_some_and(|state| !managers.contains(&state.endpoint))
        {
            *affinity = None;
        }
        *self.managers.write().await = managers;
    }

    /// Run the refresh loop as an owned, cancellation-aware service task.
    #[cfg(feature = "signal")]
    pub async fn run_refresh_loop(
        self: Arc<Self>,
        mut cancellation: crate::task_group::TaskCancellation,
    ) -> anyhow::Result<crate::task_group::TaskExit> {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => {
                    return Ok(crate::task_group::TaskExit::Cancelled);
                }
                _ = interval.tick() => self.refresh().await,
            }
        }
    }

    async fn connect_new(&self) -> Result<(String, ManagerServiceClient<Channel>), tonic::Status> {
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
                Ok(client) => return Ok((addr.clone(), client)),
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
        let threshold_secs = self.manager_liveness_threshold_secs as f64;
        let rows = client
            .query(
                "SELECT grpc_address FROM wr_managers
                 WHERE last_heartbeat > NOW() - make_interval(secs => $1::double precision)",
                &[&threshold_secs],
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
