use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Deserialize, Clone)]
pub struct EngineConfig {
    /// gRPC address of wr-manager, e.g. "http://127.0.0.1:9000"
    pub manager_address: String,
    /// Plain HTTP address of wr-proxy, e.g. "http://127.0.0.1:9001"
    pub proxy_address: String,
    /// Address this engine listens on for inbound requests from the proxy
    pub listen_address: String,
    #[serde(rename = "module", default)]
    pub modules: Vec<ModuleConfig>,
    /// Optional PostgreSQL connection pool shared across DB-enabled modules.
    pub database: Option<DatabaseConfig>,
}

#[derive(Deserialize, Clone)]
pub struct DatabaseConfig {
    /// `postgres://user:pass@host:port/dbname` connection string.
    pub url: String,
    /// Maximum number of pooled connections. Defaults to 8.
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
}

fn default_max_connections() -> usize {
    8
}

#[derive(Deserialize, Clone)]
pub struct ModuleConfig {
    pub name: String,
    pub version: String,
    pub wasm_path: String,
    /// Path to a compiled `FileDescriptorSet` binary for this module's API.
    /// Optional — if absent the module is registered without a schema and
    /// schema validation for it is skipped by the proxy.
    pub schema_path: Option<String>,
    /// Whether this module has access to the shared database pool.
    /// Requires a `[database]` section in the engine config.
    #[serde(default)]
    pub database: bool,
}

impl EngineConfig {
    pub fn load(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config file: {path}"))?;
        let config: EngineConfig =
            toml::from_str(&content).context("failed to parse engine config")?;
        config.validate().context("invalid engine config")?;
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
        anyhow::ensure!(!self.proxy_address.is_empty(), "proxy_address is required");

        for module in &self.modules {
            anyhow::ensure!(!module.name.is_empty(), "module.name is required");
            anyhow::ensure!(!module.version.is_empty(), "module.version is required");
            anyhow::ensure!(
                std::path::Path::new(&module.wasm_path).exists(),
                "wasm_path not found for module '{}': {}",
                module.name,
                module.wasm_path,
            );
            if let Some(schema) = &module.schema_path {
                anyhow::ensure!(
                    std::path::Path::new(schema).exists(),
                    "schema_path not found for module '{}': {}",
                    module.name,
                    schema,
                );
            }
            anyhow::ensure!(
                !module.database || self.database.is_some(),
                "module '{}' has database = true but no [database] section is configured",
                module.name,
            );
        }

        Ok(())
    }
}
