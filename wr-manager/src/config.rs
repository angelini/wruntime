use anyhow::Result;
use serde::Deserialize;

#[derive(Deserialize, Clone)]
pub struct ManagerConfig {
    /// gRPC listen address, e.g. "0.0.0.0:9000"
    pub listen_address: String,
    /// How long (seconds) without a heartbeat before an engine is considered unhealthy
    #[serde(default = "default_heartbeat_timeout")]
    pub engine_heartbeat_timeout_secs: u64,
    /// PostgreSQL connection pool configuration.
    pub database: DatabaseConfig,
    /// Cluster configuration for multi-manager HA.
    pub cluster: ClusterConfig,
}

#[derive(Deserialize, Clone)]
pub struct ClusterConfig {
    /// Unique cluster identifier. All managers in the same cluster must match.
    pub cluster_id: String,
    /// UDP address for chitchat gossip, e.g. "0.0.0.0:9010"
    pub gossip_listen_address: String,
    /// This manager's gRPC address as reachable by proxies.
    /// Defaults to listen_address if not set.
    #[serde(default)]
    pub advertise_grpc_address: Option<String>,
    /// Gossip interval in milliseconds. Defaults to 500.
    #[serde(default = "default_gossip_interval_ms")]
    pub gossip_interval_ms: u64,
}

fn default_gossip_interval_ms() -> u64 {
    500
}

#[derive(Deserialize, Clone)]
pub struct DatabaseConfig {
    /// `postgres://user:pass@host:port/dbname` connection string.
    pub url: String,
    /// Maximum number of pooled connections. Defaults to 10.
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
}

fn default_heartbeat_timeout() -> u64 {
    10
}

fn default_max_connections() -> usize {
    10
}

impl wr_common::config::Validatable for ManagerConfig {
    fn validate(&self) -> Result<()> {
        self.validate_inner()
    }
}

impl ManagerConfig {
    pub fn load(path: &str) -> Result<Self> {
        wr_common::config::load(path)
    }

    fn validate_inner(&self) -> Result<()> {
        use wr_common::config::Validator;
        let mut v = Validator::new();

        v.check(!self.listen_address.is_empty(), "listen_address is required");
        v.check(self.engine_heartbeat_timeout_secs > 0, "engine_heartbeat_timeout_secs must be > 0");
        v.check(!self.database.url.is_empty(), "database.url is required");
        v.check(self.database.max_connections > 0, "database.max_connections must be > 0");
        v.check(!self.cluster.cluster_id.is_empty(), "cluster.cluster_id is required");
        v.check(!self.cluster.gossip_listen_address.is_empty(), "cluster.gossip_listen_address is required");

        v.finish()
    }
}
