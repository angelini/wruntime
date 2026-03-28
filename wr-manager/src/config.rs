use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Deserialize, Clone)]
pub struct ManagerConfig {
    /// gRPC listen address, e.g. "0.0.0.0:9000"
    pub listen_address: String,
    /// How long (seconds) without a heartbeat before an engine is considered unhealthy
    #[serde(default = "default_heartbeat_timeout")]
    pub engine_heartbeat_timeout_secs: u64,
}

fn default_heartbeat_timeout() -> u64 {
    30
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
        Ok(())
    }
}
