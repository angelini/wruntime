use anyhow::{Context, Result};
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
    30
}

fn default_max_connections() -> usize {
    10
}

impl ManagerConfig {
    pub fn load(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config: {path}"))?;
        let config: ManagerConfig =
            toml::from_str(&content).context("failed to parse manager config")?;
        config.validate().context("invalid manager config")?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            !self.listen_address.is_empty(),
            "listen_address is required"
        );
        anyhow::ensure!(
            self.engine_heartbeat_timeout_secs > 0,
            "engine_heartbeat_timeout_secs must be > 0"
        );
        anyhow::ensure!(!self.database.url.is_empty(), "database.url is required");
        anyhow::ensure!(
            self.database.max_connections > 0,
            "database.max_connections must be > 0"
        );
        Ok(())
    }
}
