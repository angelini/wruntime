use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use chitchat::transport::UdpTransport;
use chitchat::{ChitchatConfig, ChitchatHandle, ChitchatId, FailureDetectorConfig, NodeState};
use tracing::warn;

/// Re-exported so callers can construct/override the failure detector without a
/// direct chitchat dependency.
pub use chitchat::FailureDetectorConfig as ClusterFailureDetectorConfig;

/// Bootstrap convergence window: DB-fresh managers never yet seen in gossip are
/// included by `reconcile_managers` only until this elapses after startup, then
/// chitchat becomes authoritative. ~2× the default gossip interval.
pub const GOSSIP_CONVERGENCE_WINDOW: Duration = Duration::from_secs(10);

/// A manager chitchat considers live, with the metadata it published into gossip.
#[derive(Clone, Debug)]
pub struct ManagerLiveness {
    pub manager_id: String,
    pub grpc_address: String,
    pub gossip_address: String,
}

/// Wraps a chitchat instance for manager-to-manager liveness detection.
///
/// Engine heartbeats are stored in Postgres — chitchat carries the manager's own
/// `grpc_address`/`gossip_address` and exists as the primary manager liveness
/// signal via the built-in phi-accrual failure detector.
pub struct ClusterHandle {
    handle: ChitchatHandle,
    started_at: Instant,
}

impl ClusterHandle {
    /// Bootstrap chitchat with peers discovered from `wr_managers`.
    pub async fn new(
        manager_id: &str,
        cluster_id: &str,
        gossip_listen_addr: SocketAddr,
        seed_gossip_addrs: Vec<String>,
        gossip_interval: Duration,
        failure_detector_config: FailureDetectorConfig,
    ) -> anyhow::Result<Self> {
        let chitchat_id = ChitchatId {
            node_id: manager_id.to_string().into(),
            generation_id: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            gossip_advertise_addr: gossip_listen_addr,
        };

        let config = ChitchatConfig {
            chitchat_id,
            cluster_id: cluster_id.to_string(),
            gossip_interval,
            listen_addr: gossip_listen_addr,
            seed_nodes: seed_gossip_addrs,
            failure_detector_config,
            marked_for_deletion_grace_period: Duration::from_secs(120),
            catchup_callback: None,
            extra_liveness_predicate: None,
        };

        let handle = chitchat::spawn_chitchat(config, vec![], &UdpTransport).await?;

        Ok(Self {
            handle,
            started_at: Instant::now(),
        })
    }

    /// This manager's own id (chitchat `node_id`).
    pub fn self_id(&self) -> String {
        self.handle.chitchat_id().node_id.to_string()
    }

    /// True while still inside the bootstrap convergence window after startup.
    pub fn within_convergence_window(&self) -> bool {
        self.started_at.elapsed() < GOSSIP_CONVERGENCE_WINDOW
    }

    /// Publish this manager's addresses into its own gossip node state. First and
    /// only application data written into chitchat; consumed by reconciliation.
    pub async fn publish_metadata(&self, grpc_address: &str, gossip_address: &str) {
        let grpc = grpc_address.to_string();
        let gossip = gossip_address.to_string();
        self.handle
            .with_chitchat(move |c| {
                let state = c.self_node_state();
                state.set("grpc_address", &grpc);
                state.set("gossip_address", &gossip);
            })
            .await;
    }

    /// Managers chitchat considers live, keyed on `node_id`. Includes self.
    /// A live node without `grpc_address` is omitted from live gossip output;
    /// reconciliation only uses the DB record through the unknown-gossip bootstrap path.
    pub async fn live_managers(&self) -> Vec<ManagerLiveness> {
        self.handle
            .with_chitchat(|c| {
                let live_ids: Vec<ChitchatId> = c.live_nodes().cloned().collect();
                let mut out = Vec::with_capacity(live_ids.len());
                for id in &live_ids {
                    let Some(state) = c.node_state(id) else {
                        continue;
                    };
                    let Some(grpc) = state.get("grpc_address") else {
                        continue;
                    };
                    let gossip = state.get("gossip_address").unwrap_or("");
                    out.push(ManagerLiveness {
                        manager_id: id.node_id.to_string(),
                        grpc_address: grpc.to_string(),
                        gossip_address: gossip.to_string(),
                    });
                }
                out
            })
            .await
    }

    /// Manager ids chitchat has affirmatively marked dead or scheduled for
    /// deletion, keyed on `node_id`.
    pub async fn dead_manager_ids(&self) -> HashSet<String> {
        self.handle
            .with_chitchat(|c| {
                let mut ids = HashSet::new();
                for id in c.dead_nodes() {
                    ids.insert(id.node_id.to_string());
                }
                for id in c.scheduled_for_deletion_nodes() {
                    ids.insert(id.node_id.to_string());
                }
                ids
            })
            .await
    }

    /// Watcher over the live-node set, for membership join/leave logging.
    pub async fn live_nodes_watcher(
        &self,
    ) -> tokio::sync::watch::Receiver<std::collections::BTreeMap<ChitchatId, NodeState>> {
        self.handle.with_chitchat(|c| c.live_nodes_watcher()).await
    }

    /// Stop gossiping without consuming the handle (test hook to simulate death).
    pub fn initiate_shutdown(&self) -> anyhow::Result<()> {
        self.handle.initiate_shutdown()
    }

    /// Gracefully shut down the chitchat server.
    pub async fn shutdown(self) {
        if let Err(e) = self.handle.shutdown().await {
            warn!(error = %e, "chitchat shutdown error");
        }
    }
}
