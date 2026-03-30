use anyhow::{Context, Result};
use serde::Deserialize;
use wr_common::node::NodeConfig;

#[derive(Deserialize, Clone)]
pub struct EngineConfig {
    /// gRPC address of wr-manager, e.g. "http://127.0.0.1:9000"
    pub manager_address: String,
    /// Address this engine listens on for inbound requests from the proxy
    pub listen_address: String,
    /// Node configuration — identifies the local proxy for this engine.
    pub node: NodeConfig,
    #[serde(rename = "module", default)]
    pub modules: Vec<ModuleConfig>,
    /// Optional PostgreSQL connection pool shared across DB-enabled modules.
    pub database: Option<DatabaseConfig>,
    /// Optional S3-compatible blobstore shared across blobstore-enabled modules.
    pub blobstore: Option<BlobstoreConfig>,
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
    20
}

#[derive(Deserialize, Clone)]
pub struct BlobstoreConfig {
    /// S3-compatible endpoint URL, e.g. "http://127.0.0.1:8900"
    pub endpoint: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    /// S3 region. Defaults to "us-east-1".
    #[serde(default = "default_bs_region")]
    pub region: String,
}

fn default_bs_region() -> String {
    "us-east-1".into()
}

/// Filesystem access mode for a module.
#[derive(Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum FsMode {
    /// Mount an ephemeral temp directory at `/`. Deleted when the store is dropped.
    Tempdir,
}

#[derive(Deserialize, Clone)]
pub struct ModuleConfig {
    pub name: String,
    pub namespace: String,
    pub version: String,
    pub wasm_path: String,
    /// Path to a compiled `FileDescriptorSet` binary for this module's API.
    /// Required — every module must declare a schema so the proxy can validate
    /// request bodies.
    pub schema_path: String,
    /// Whether this module has access to the shared database pool.
    /// Requires a `[database]` section in the engine config.
    #[serde(default)]
    pub database: bool,
    /// Overrides `[database].max_connections` for this module's pool.
    /// Falls back to the global value when absent.
    #[serde(default)]
    pub db_max_connections: Option<usize>,
    /// Whether this module has access to the shared blobstore client.
    /// Requires a `[blobstore]` section in the engine config.
    #[serde(default)]
    pub blobstore: bool,
    /// Optional filesystem access. Set `fs = "tempdir"` to mount an ephemeral
    /// writable directory at `/` for the duration of each store's lifetime.
    #[serde(default)]
    pub fs: Option<FsMode>,
    /// Per-request timeout in seconds. Requests that exceed this are cancelled
    /// and the caller receives a 504. Defaults to 30.
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,
    /// Inbound request channel depth. Requests that arrive when the channel is
    /// full receive a 429. Defaults to 128.
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,
}

fn default_request_timeout_secs() -> u64 {
    30
}

fn default_channel_capacity() -> usize {
    128
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
        anyhow::ensure!(
            !self.node.proxy_address.is_empty(),
            "node.proxy_address is required"
        );

        for module in &self.modules {
            anyhow::ensure!(!module.name.is_empty(), "module.name is required");
            anyhow::ensure!(!module.namespace.is_empty(), "module.namespace is required");
            anyhow::ensure!(!module.version.is_empty(), "module.version is required");
            anyhow::ensure!(
                std::path::Path::new(&module.wasm_path).exists(),
                "wasm_path not found for module '{}': {}",
                module.name,
                module.wasm_path,
            );
            anyhow::ensure!(
                !module.schema_path.is_empty(),
                "schema_path is required for module '{}'",
                module.name,
            );
            anyhow::ensure!(
                std::path::Path::new(&module.schema_path).exists(),
                "schema_path not found for module '{}': {}",
                module.name,
                module.schema_path,
            );
            anyhow::ensure!(
                !module.database || self.database.is_some(),
                "module '{}' has database = true but no [database] section is configured",
                module.name,
            );
            anyhow::ensure!(
                !module.blobstore || self.blobstore.is_some(),
                "module '{}' has blobstore = true but no [blobstore] section is configured",
                module.name,
            );
        }

        Ok(())
    }
}
