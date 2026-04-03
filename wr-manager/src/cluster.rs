use std::net::SocketAddr;
use std::time::Duration;

use chitchat::transport::UdpTransport;
use chitchat::{ChitchatConfig, ChitchatHandle, ChitchatId, FailureDetectorConfig};
use tracing::warn;

/// Wraps a chitchat instance for manager-to-manager liveness detection.
///
/// Engine heartbeats are stored in Postgres — chitchat carries zero application
/// keys and exists solely for the built-in phi-accrual failure detector.
pub struct ClusterHandle {
    handle: ChitchatHandle,
}

impl ClusterHandle {
    /// Bootstrap chitchat with peers discovered from `wr_managers`.
    pub async fn new(
        manager_id: &str,
        cluster_id: &str,
        gossip_listen_addr: SocketAddr,
        seed_gossip_addrs: Vec<String>,
        gossip_interval: Duration,
    ) -> anyhow::Result<Self> {
        let chitchat_id = ChitchatId {
            node_id: manager_id.to_string(),
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
            failure_detector_config: FailureDetectorConfig::default(),
            marked_for_deletion_grace_period: Duration::from_secs(120),
            catchup_callback: None,
            extra_liveness_predicate: None,
        };

        let handle = chitchat::spawn_chitchat(config, vec![], &UdpTransport).await?;

        Ok(Self { handle })
    }

    /// Gracefully shut down the chitchat server.
    pub async fn shutdown(self) {
        if let Err(e) = self.handle.shutdown().await {
            warn!(error = %e, "chitchat shutdown error");
        }
    }
}
