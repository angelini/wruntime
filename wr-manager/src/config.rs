use std::num::NonZeroU64;

use anyhow::Result;
use serde::Deserialize;
use wr_common::node::TlsConfig;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HeartbeatTimeoutSecs(NonZeroU64);

impl HeartbeatTimeoutSecs {
    pub fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Clone)]
pub struct ManagerConfig {
    /// gRPC listen address, e.g. "0.0.0.0:9000"
    pub listen_address: String,
    /// How long (seconds) without a heartbeat before an engine is considered unhealthy
    pub engine_heartbeat_timeout_secs: u64,
    /// How long (seconds) without a per-module heartbeat before that module's
    /// routes are marked unhealthy.
    pub module_heartbeat_timeout_secs: HeartbeatTimeoutSecs,
    /// Loopback proxy address the scheduler POSTs jobs to, e.g.
    /// "http://127.0.0.1:9001". REQUIRED — startup fails if unset or empty.
    pub local_proxy_address: String,
    /// How long (seconds) a claimed schedule lease is held before another manager
    /// may reclaim it. Must exceed worst-case per-tick submission time.
    pub scheduler_lease_secs: u64,
    /// Base backoff (seconds) for a failed submission; doubles per consecutive failure.
    pub scheduler_retry_base_secs: u64,
    /// Maximum backoff (seconds) cap for consecutive failures.
    pub scheduler_retry_cap_secs: u64,
    /// PostgreSQL connection pool configuration.
    pub database: DatabaseConfig,
    /// Cluster configuration for multi-manager HA.
    pub cluster: ClusterConfig,
    /// TLS certificate configuration for the gRPC listener.
    pub tls: TlsConfig,
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

fn default_scheduler_lease_secs() -> u64 {
    30
}
fn default_scheduler_retry_base_secs() -> u64 {
    5
}
fn default_scheduler_retry_cap_secs() -> u64 {
    300
}

fn default_max_connections() -> usize {
    10
}

#[derive(Deserialize, Clone)]
pub struct RawManagerConfig {
    pub listen_address: String,
    #[serde(default = "default_heartbeat_timeout")]
    pub engine_heartbeat_timeout_secs: u64,
    #[serde(default)]
    pub module_heartbeat_timeout_secs: Option<u64>,
    pub local_proxy_address: String,
    #[serde(default = "default_scheduler_lease_secs")]
    pub scheduler_lease_secs: u64,
    #[serde(default = "default_scheduler_retry_base_secs")]
    pub scheduler_retry_base_secs: u64,
    #[serde(default = "default_scheduler_retry_cap_secs")]
    pub scheduler_retry_cap_secs: u64,
    pub database: DatabaseConfig,
    pub cluster: ClusterConfig,
    pub tls: TlsConfig,
}

impl wr_common::config::Validatable for RawManagerConfig {
    fn validate(&self) -> Result<()> {
        self.validate_inner()
    }
}

impl RawManagerConfig {
    fn validate_inner(&self) -> Result<()> {
        use wr_common::config::Validator;
        let mut v = Validator::new();

        v.check(
            !self.listen_address.is_empty(),
            "listen_address is required",
        );
        v.check(
            self.engine_heartbeat_timeout_secs > 0,
            "engine_heartbeat_timeout_secs must be > 0",
        );
        if let Some(t) = self.module_heartbeat_timeout_secs {
            v.check(t > 0, "module_heartbeat_timeout_secs must be > 0");
        }
        v.check(
            !self.local_proxy_address.is_empty(),
            "local_proxy_address is required",
        );
        v.check(
            self.scheduler_lease_secs > 0,
            "scheduler_lease_secs must be > 0",
        );
        v.check(
            self.scheduler_retry_base_secs > 0,
            "scheduler_retry_base_secs must be > 0",
        );
        v.check(
            self.scheduler_retry_cap_secs >= self.scheduler_retry_base_secs,
            "scheduler_retry_cap_secs must be >= scheduler_retry_base_secs",
        );
        v.check(!self.database.url.is_empty(), "database.url is required");
        v.check(
            self.database.max_connections > 0,
            "database.max_connections must be > 0",
        );
        v.check(
            !self.cluster.cluster_id.is_empty(),
            "cluster.cluster_id is required",
        );
        v.check(
            !self.cluster.gossip_listen_address.is_empty(),
            "cluster.gossip_listen_address is required",
        );
        v.check(!self.tls.cert_path.is_empty(), "tls.cert_path is required");
        v.check(!self.tls.key_path.is_empty(), "tls.key_path is required");
        v.check(
            !self.tls.ca_cert_path.is_empty(),
            "tls.ca_cert_path is required",
        );

        v.finish()
    }
}

impl TryFrom<RawManagerConfig> for ManagerConfig {
    type Error = anyhow::Error;

    fn try_from(raw: RawManagerConfig) -> Result<Self> {
        let module_timeout = raw
            .module_heartbeat_timeout_secs
            .unwrap_or(raw.engine_heartbeat_timeout_secs);
        let module_heartbeat_timeout_secs = NonZeroU64::new(module_timeout)
            .map(HeartbeatTimeoutSecs)
            .ok_or_else(|| anyhow::anyhow!("module_heartbeat_timeout_secs must be > 0"))?;

        Ok(Self {
            listen_address: raw.listen_address,
            engine_heartbeat_timeout_secs: raw.engine_heartbeat_timeout_secs,
            module_heartbeat_timeout_secs,
            local_proxy_address: raw.local_proxy_address,
            scheduler_lease_secs: raw.scheduler_lease_secs,
            scheduler_retry_base_secs: raw.scheduler_retry_base_secs,
            scheduler_retry_cap_secs: raw.scheduler_retry_cap_secs,
            database: raw.database,
            cluster: raw.cluster,
            tls: raw.tls,
        })
    }
}

impl ManagerConfig {
    pub fn load(path: &str) -> Result<Self> {
        let raw: RawManagerConfig = wr_common::config::load(path)?;
        raw.try_into()
    }
}
