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

    /// Create a bundle-relative copy: rewrite wasm_path, schema_path, and
    /// migrations_path to point at the standard bundle directories.
    pub fn to_bundle_config(&self) -> Self {
        let mut config = self.clone();
        for module in &mut config.modules {
            module.wasm_path = format!("modules/{}.wasm", module.name);
            if !module.schema_path.is_empty() {
                module.schema_path = format!("schemas/{}.binpb", module.name);
            }
            if module.migrations_path.is_some() {
                module.migrations_path = Some(format!("migrations/{}", module.name));
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

impl ProxyConfig {
    /// Parse a proxy config from a TOML file.
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config: {path}"))?;
        toml::from_str(&content).with_context(|| format!("failed to parse config: {path}"))
    }

    /// Serialize to TOML string.
    pub fn to_toml(&self) -> anyhow::Result<String> {
        toml::to_string_pretty(self).map_err(Into::into)
    }
}
