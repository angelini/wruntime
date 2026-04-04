//! Shared TOML config structs for engine and proxy configurations.
//!
//! Used by both `dev` (local development) and `node` (remote deployment) commands
//! to parse and generate config files via serde, avoiding manual TOML string building.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Engine config
// ---------------------------------------------------------------------------

#[derive(Deserialize, Serialize, Clone)]
pub struct EngineConfig {
    pub listen_address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<NodeConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<DatabaseConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<toml::Value>,
    #[serde(rename = "module", default, skip_serializing_if = "Vec::is_empty")]
    pub modules: Vec<ModuleConfig>,
}

#[derive(Deserialize, Serialize, Clone, Default)]
pub struct NodeConfig {
    #[serde(default)]
    pub proxy_address: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub control_address: String,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct DatabaseConfig {
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guest_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_connections: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub statement_timeout_secs: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_in_transaction_timeout_secs: Option<u32>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct ModuleConfig {
    pub name: String,
    pub namespace: String,
    pub version: String,
    pub wasm_path: String,
    /// Path to a pre-compiled native artifact (`.cwasm`).
    /// When present, the engine deserializes this instead of JIT-compiling the `.wasm`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwasm_path: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub schema_path: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub database: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub db_max_connections: Option<usize>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub blobstore: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub llm: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub migrations_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel_capacity: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_timeout_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
}

fn is_false(v: &bool) -> bool {
    !v
}

impl EngineConfig {
    /// Parse an engine config from a TOML file.
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config: {path}"))?;
        toml::from_str(&content).with_context(|| format!("failed to parse config: {path}"))
    }

    /// Serialize to TOML string.
    pub fn to_toml(&self) -> anyhow::Result<String> {
        toml::to_string_pretty(self).map_err(Into::into)
    }

    /// Create a bundle-ready copy: rewrite module paths to bundle-relative
    /// directories and insert `{host}`, `{db_url}`, `{guest_db_url}` template
    /// placeholders for values that vary per deployment target.
    pub fn to_bundle_config(&self) -> Self {
        let mut config = self.clone();

        // Rewrite module paths
        for module in &mut config.modules {
            module.cwasm_path = Some(format!("modules/{}.cwasm", module.name));
            module.wasm_path = format!("modules/{}.wasm", module.name);
            if !module.schema_path.is_empty() {
                module.schema_path = format!("schemas/{}.binpb", module.name);
            }
            if module.migrations_path.is_some() {
                module.migrations_path = Some(format!("migrations/{}", module.name));
            }
        }

        // Template host-dependent node addresses (preserve ports from source)
        if let Some(ref node) = self.node {
            let proxy_port = super::helpers::extract_port(&node.proxy_address);
            let control_port = super::helpers::extract_port(&node.control_address);
            config.node = Some(NodeConfig {
                proxy_address: format!("http://{{host}}:{proxy_port}"),
                control_address: format!("http://{{host}}:{control_port}"),
            });
        }

        // Template database URLs
        if let Some(ref mut db) = config.database {
            db.url = "{db_url}".to_string();
            if db.guest_url.is_some() {
                db.guest_url = Some("{guest_db_url}".to_string());
            }
        }

        config
    }
}

use anyhow::Context;

// ---------------------------------------------------------------------------
// Proxy config
// ---------------------------------------------------------------------------

#[derive(Deserialize, Serialize, Clone)]
pub struct ProxyConfig {
    pub listen_address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_address: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<ProxyNodeConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<ProxyDatabaseConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache: Option<ProxyCacheConfig>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct ProxyNodeConfig {
    pub proxy_address: String,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct ProxyDatabaseConfig {
    pub url: String,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct ProxyCacheConfig {
    pub routing_table_ttl_secs: u32,
}

// ---------------------------------------------------------------------------
// Manager config
// ---------------------------------------------------------------------------

#[derive(Deserialize, Serialize, Clone)]
pub struct ManagerConfig {
    pub listen_address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine_heartbeat_timeout_secs: Option<u32>,
    pub database: ManagerDatabaseConfig,
    pub cluster: ClusterConfig,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct ManagerDatabaseConfig {
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_connections: Option<usize>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct ClusterConfig {
    pub cluster_id: String,
    pub gossip_listen_address: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub seed_nodes: Vec<String>,
}

impl ManagerConfig {
    /// Parse a manager config from a TOML file.
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config: {path}"))?;
        toml::from_str(&content).with_context(|| format!("failed to parse config: {path}"))
    }

    /// Serialize to TOML string.
    pub fn to_toml(&self) -> anyhow::Result<String> {
        toml::to_string_pretty(self).map_err(Into::into)
    }

    /// Create a bundle-ready copy with `{db_url}` template placeholder.
    pub fn to_bundle_config(&self) -> Self {
        let mut config = self.clone();
        config.database.url = "{db_url}".to_string();
        config.cluster.seed_nodes.clear();
        config
    }
}

impl ProxyConfig {
    /// Serialize to TOML string.
    pub fn to_toml(&self) -> anyhow::Result<String> {
        toml::to_string_pretty(self).map_err(Into::into)
    }
}
