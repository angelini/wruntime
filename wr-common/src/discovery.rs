use std::sync::Arc;
use std::time::{Duration, Instant};

use deadpool_postgres::Pool;
use rand::seq::SliceRandom;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tracing::warn;

use crate::wruntime::manager_service_client::ManagerServiceClient;

struct AffinityState {
    client: ManagerServiceClient<Channel>,
    established_at: Instant,
}

/// Discovers managers via the `wr_managers` Postgres table.
/// Caches addresses locally and shuffles requests across them.
/// Maintains sticky affinity to one manager for `AFFINITY_DURATION`
/// to reduce heartbeat key scatter across chitchat nodes.
pub struct ManagerDiscovery {
    pool: Pool,
    managers: RwLock<Vec<String>>,
    affinity: RwLock<Option<AffinityState>>,
}

impl ManagerDiscovery {
    const AFFINITY_DURATION: Duration = Duration::from_secs(120);

    pub fn new(pool: Pool) -> Self {
        Self {
            pool,
            managers: RwLock::new(Vec::new()),
            affinity: RwLock::new(None),
        }
    }

    /// Query `wr_managers` for active manager gRPC addresses.
    /// On error, keeps the previous cached list.
    pub async fn refresh(&self) {
        match self.query_managers().await {
            Ok(addrs) if !addrs.is_empty() => {
                *self.managers.write().await = addrs;
            }
            Ok(_) => {
                warn!("no active managers found in wr_managers");
            }
            Err(e) => {
                warn!(error = %e, "failed to refresh manager list from Postgres");
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

        let mut last_err = None;
        for addr in &shuffled {
            match ManagerServiceClient::connect(addr.clone()).await {
                Ok(client) => return Ok(client),
                Err(e) => {
                    warn!(address = %addr, error = %e, "manager connection failed, trying next");
                    last_err = Some(e);
                }
            }
        }

        Err(tonic::Status::unavailable(format!(
            "all {} managers unreachable: {}",
            shuffled.len(),
            last_err
                .map(|e| e.to_string())
                .unwrap_or_else(|| "unknown".into()),
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
            .await?;
        Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
    }
}
