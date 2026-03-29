use anyhow::{Context, Result};
use serde::Deserialize;
use wr_common::node::NodeConfig;

#[derive(Deserialize, Clone)]
pub struct ProxyConfig {
    /// TCP address to listen on for inbound HTTP, e.g. "0.0.0.0:9001"
    pub listen_address: String,
    /// gRPC address of wr-manager, e.g. "http://127.0.0.1:9000"
    pub manager_address: String,
    /// Node configuration — this proxy's own address as reachable by peer proxies.
    pub node: NodeConfig,
    #[serde(default)]
    pub cache: CacheConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
}

#[derive(Deserialize, Clone)]
pub struct CacheConfig {
    /// How often (seconds) to poll wr-manager for routing table updates
    pub routing_table_ttl_secs: u64,
    /// How often (seconds) to re-fetch module schemas from wr-manager
    pub schema_ttl_secs: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            routing_table_ttl_secs: 5,
            schema_ttl_secs: 60,
        }
    }
}

#[derive(Deserialize, Clone)]
pub struct MetricsConfig {
    /// How often (seconds) to flush buffered metrics to wr-manager
    pub flush_interval_secs: u64,
    /// Capacity of the in-process metrics channel
    pub queue_depth: usize,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            flush_interval_secs: 10,
            queue_depth: 1000,
        }
    }
}

impl ProxyConfig {
    pub fn load(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config: {path}"))?;
        let config: ProxyConfig =
            toml::from_str(&content).context("failed to parse proxy config")?;
        config.validate().context("invalid proxy config")?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            !self.listen_address.is_empty(),
            "listen_address is required"
        );
        anyhow::ensure!(
            !self.manager_address.is_empty(),
            "manager_address is required"
        );
        anyhow::ensure!(
            !self.node.proxy_address.is_empty(),
            "node.proxy_address is required"
        );
        anyhow::ensure!(
            self.cache.routing_table_ttl_secs > 0,
            "cache.routing_table_ttl_secs must be > 0"
        );
        anyhow::ensure!(
            self.cache.schema_ttl_secs > 0,
            "cache.schema_ttl_secs must be > 0"
        );
        anyhow::ensure!(
            self.metrics.flush_interval_secs > 0,
            "metrics.flush_interval_secs must be > 0"
        );
        anyhow::ensure!(
            self.metrics.queue_depth > 0,
            "metrics.queue_depth must be > 0"
        );
        Ok(())
    }
}
