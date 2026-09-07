use std::sync::atomic::{AtomicU32, Ordering};
#[cfg(feature = "signal")]
use std::sync::Arc;
#[cfg(feature = "signal")]
use std::time::Duration;

use deadpool_postgres::Pool;
use tokio::sync::{Mutex, RwLock};
use tonic::transport::ClientTlsConfig;
use tracing::warn;

use crate::manager_client::{ManagerCandidate, ManagerEpoch, ManagerEpochProvider, RetryClass};
use crate::wruntime::ListManagersRequest;

/// Discovers managers via a reachable manager's `ListManagers` RPC,
/// bootstrapping/falling back to the `wr_managers` PostgreSQL lease table when
/// no manager is reachable. Every returned client is a typed, observed epoch.
pub struct ManagerDiscovery {
    pool: Pool,
    managers: RwLock<Vec<String>>,
    refresh_epoch: Mutex<Option<ManagerEpoch>>,
    tls_config: ClientTlsConfig,
    trust_roots_path: String,
    client_identity_path: String,
    manager_liveness_threshold_secs: u64,
    rpc_failures: AtomicU32,
}

impl ManagerDiscovery {
    const MAX_QUIET_FAILURES: u32 = 3;

    pub fn new(
        pool: Pool,
        tls_config: ClientTlsConfig,
        trust_roots_path: impl Into<String>,
        client_identity_path: impl Into<String>,
        manager_liveness_threshold_secs: u64,
    ) -> Result<Self, tonic::Status> {
        let trust_roots_path = trust_roots_path.into();
        let client_identity_path = client_identity_path.into();
        if trust_roots_path.is_empty() || client_identity_path.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "manager discovery requires explicit trust-root and client-identity metadata",
            ));
        }
        Ok(Self {
            pool,
            managers: RwLock::new(Vec::new()),
            refresh_epoch: Mutex::new(None),
            tls_config,
            trust_roots_path,
            client_identity_path,
            manager_liveness_threshold_secs,
            rpc_failures: AtomicU32::new(0),
        })
    }

    /// Refresh the cached manager list. Bootstrap from DB on cold start, then
    /// prefer the lease-filtered manager view. Errors preserve the old set.
    pub async fn refresh(&self) {
        if self.managers.read().await.is_empty() {
            self.refresh_from_db_fallback().await;
        }
        if self.refresh_from_managers().await {
            self.rpc_failures.store(0, Ordering::Relaxed);
            return;
        }
        let failures = self.rpc_failures.fetch_add(1, Ordering::Relaxed) + 1;
        if failures >= Self::MAX_QUIET_FAILURES {
            warn!(
                consecutive_failures = failures,
                "manager discovery could not reach any manager RPC; using direct-DB fallback",
            );
        }
        self.refresh_from_db_fallback().await;
    }

    async fn refresh_from_managers(&self) -> bool {
        let mut retained = self.refresh_epoch.lock().await;
        if retained.is_none() {
            *retained = self.pin(RetryClass::ReadOnly).await.ok();
        }
        let Some(epoch) = retained.as_mut() else {
            return false;
        };
        let response = match epoch.list_managers(ListManagersRequest {}).await {
            Ok(response) => response,
            Err(_) => {
                let replacement = self.repin(epoch).await.ok();
                *retained = replacement;
                return false;
            }
        };
        let addresses = response
            .into_inner()
            .managers
            .into_iter()
            .map(|manager| manager.grpc_address)
            .filter(|address| !address.is_empty())
            .collect();
        self.replace_managers(addresses).await;
        true
    }

    async fn refresh_from_db_fallback(&self) {
        match self.query_managers().await {
            Ok(addresses) => self.replace_managers(addresses).await,
            Err(error) => warn!(%error, "manager discovery DB fallback query failed"),
        }
    }

    /// Acquire one immutable manager epoch in stable candidate order.
    pub async fn pin(&self, retry_class: RetryClass) -> Result<ManagerEpoch, tonic::Status> {
        let addresses = self.managers.read().await.clone();
        if addresses.is_empty() {
            return Err(tonic::Status::unavailable(
                "no managers discovered — is wr_managers table populated?",
            ));
        }
        let mut candidates = Vec::with_capacity(addresses.len());
        for endpoint in addresses {
            let uri: http::Uri = endpoint.parse().map_err(|_| {
                tonic::Status::failed_precondition("discovered manager endpoint is malformed")
            })?;
            let server_name = uri.host().ok_or_else(|| {
                tonic::Status::failed_precondition("discovered manager endpoint has no host")
            })?;
            candidates.push(ManagerCandidate {
                endpoint,
                server_name: server_name.to_owned(),
            });
        }
        ManagerEpochProvider::new(
            candidates,
            self.tls_config.clone(),
            self.trust_roots_path.clone(),
            self.client_identity_path.clone(),
        )?
        .pin(retry_class)
        .await
    }

    /// Repin only at the workflow's declared retry boundary. The replacement
    /// uses the previous epoch's immutable candidate set and rotates after its
    /// pinned endpoint rather than restarting discovery at candidate zero.
    pub async fn repin(&self, previous: &ManagerEpoch) -> Result<ManagerEpoch, tonic::Status> {
        previous.repin().await
    }

    async fn replace_managers(&self, mut managers: Vec<String>) {
        managers.sort();
        managers.dedup();
        *self.managers.write().await = managers;
    }

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
            .map_err(|error| {
                use std::error::Error;
                let mut message = error.to_string();
                let mut source = error.source();
                while let Some(cause) = source {
                    message.push_str(": ");
                    message.push_str(&cause.to_string());
                    source = cause.source();
                }
                message
            })?;
        Ok(rows.iter().map(|row| row.get::<_, String>(0)).collect())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn discovery_has_no_direct_channel_or_randomized_origin() {
        let source = include_str!("discovery.rs");
        assert!(!source.contains(concat!("Endpoint", "::from")));
        assert!(!source.contains(concat!("ManagerClient", "::connect")));
        assert!(!source.contains(concat!("shuf", "fle(")));
        assert!(source.contains(concat!("ManagerEpochProvider", "::new")));
    }
}
