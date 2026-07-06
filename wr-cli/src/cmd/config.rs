//! Shared TOML config structs for engine, proxy, and manager configurations.
//!
//! Used by development and deployment commands to parse and generate config files
//! via serde, avoiding manual TOML string building.

use anyhow::Context;
use serde::{Deserialize, Serialize};
use toml::map::Map;

pub type ExtraFields = Map<String, toml::Value>;

pub fn empty_extra_fields() -> ExtraFields {
    ExtraFields::new()
}

fn option_string_is_empty(value: &Option<String>) -> bool {
    value.as_deref().unwrap_or("").is_empty()
}

fn option_string_has_value(value: &Option<String>) -> bool {
    !option_string_is_empty(value)
}

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
    #[serde(flatten)]
    pub extra: ExtraFields,
}

#[derive(Deserialize, Serialize, Clone, Default)]
pub struct NodeConfig {
    #[serde(default)]
    pub proxy_address: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub control_address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<CliTlsConfig>,
    #[serde(flatten)]
    pub extra: ExtraFields,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct CliTlsConfig {
    pub cert_path: String,
    pub key_path: String,
    pub ca_cert_path: String,
    #[serde(flatten)]
    pub extra: ExtraFields,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct DatabaseConfig {
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_connections: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub statement_timeout_secs: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_in_transaction_timeout_secs: Option<u32>,
    #[serde(flatten)]
    pub extra: ExtraFields,
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
    #[serde(default, skip_serializing_if = "option_string_is_empty")]
    pub schema_path: Option<String>,
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
    #[serde(flatten)]
    pub extra: ExtraFields,
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
    /// directories and insert `{host}`, `{db_url}` template placeholders
    /// for values that vary per deployment target.
    pub fn to_bundle_config(&self) -> Self {
        let mut config = self.clone();

        // Rewrite module paths
        for module in &mut config.modules {
            module.cwasm_path = Some(format!("modules/{}.cwasm", module.name));
            module.wasm_path = format!("modules/{}.wasm", module.name);
            if option_string_has_value(&module.schema_path) {
                module.schema_path = Some(format!("schemas/{}.binpb", module.name));
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
                peer_port: node.peer_port,
                tls: node.tls.clone(),
                extra: node.extra.clone(),
            });
        }

        // Template database URL
        if let Some(ref mut db) = config.database {
            db.url = "{db_url}".to_string();
        }

        config
    }
}

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
    #[serde(flatten)]
    pub extra: ExtraFields,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct ProxyNodeConfig {
    pub proxy_address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<CliTlsConfig>,
    #[serde(flatten)]
    pub extra: ExtraFields,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct ProxyDatabaseConfig {
    pub url: String,
    #[serde(flatten)]
    pub extra: ExtraFields,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct ProxyCacheConfig {
    pub routing_table_ttl_secs: u32,
    #[serde(flatten)]
    pub extra: ExtraFields,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<CliTlsConfig>,
    #[serde(flatten)]
    pub extra: ExtraFields,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct ManagerDatabaseConfig {
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_connections: Option<usize>,
    #[serde(flatten)]
    pub extra: ExtraFields,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct ClusterConfig {
    pub cluster_id: String,
    pub gossip_listen_address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub advertise_grpc_address: Option<String>,
    #[serde(default, skip_serializing)]
    pub seed_nodes: Vec<String>,
    #[serde(flatten)]
    pub extra: ExtraFields,
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

    /// Create a bundle-ready copy with `{db_url}` and `{advertise_address}` template placeholders.
    pub fn to_bundle_config(&self) -> Self {
        let mut config = self.clone();
        config.database.url = "{db_url}".to_string();
        config.cluster.advertise_grpc_address = Some("{advertise_address}".to_string());
        config.cluster.seed_nodes.clear();
        let mut tls = config.tls.take().unwrap_or(CliTlsConfig {
            cert_path: String::new(),
            key_path: String::new(),
            ca_cert_path: String::new(),
            extra: empty_extra_fields(),
        });
        tls.cert_path = "certs/manager.crt".to_string();
        tls.key_path = "certs/manager.key".to_string();
        tls.ca_cert_path = "certs/ca.crt".to_string();
        config.tls = Some(tls);
        config
    }
}

impl ProxyConfig {
    /// Parse a proxy config from a TOML file.
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config: {path}"))?;
        toml::from_str(&content).with_context(|| format!("failed to parse config: {path}"))
    }

    /// Create a bundle-ready copy with `{db_url}` and `{host}` template placeholders.
    pub fn to_bundle_config(&self) -> Self {
        let mut config = self.clone();
        if let Some(ref mut db) = config.database {
            db.url = "{db_url}".to_string();
        }
        if let (Some(source_node), Some(config_node)) = (&self.node, &mut config.node) {
            let proxy_port = super::helpers::extract_port(&source_node.proxy_address);
            config_node.proxy_address = format!("http://{{host}}:{proxy_port}");
            let mut tls = config_node.tls.take().unwrap_or(CliTlsConfig {
                cert_path: String::new(),
                key_path: String::new(),
                ca_cert_path: String::new(),
                extra: empty_extra_fields(),
            });
            tls.cert_path = "certs/node.crt".to_string();
            tls.key_path = "certs/node.key".to_string();
            tls.ca_cert_path = "certs/ca.crt".to_string();
            config_node.tls = Some(tls);
        }
        config
    }

    /// Serialize to TOML string.
    pub fn to_toml(&self) -> anyhow::Result<String> {
        toml::to_string_pretty(self).map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};
    use wr_engine::config::EngineConfig as RuntimeEngineConfig;
    use wr_manager::config::ManagerConfig as RuntimeManagerConfig;
    use wr_proxy::config::ProxyConfig as RuntimeProxyConfig;

    fn runtime_unique_temp_path(name: &str, ext: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "wr-cli-runtime-config-{name}-{}-{nanos}.{ext}",
            std::process::id()
        ))
    }

    fn runtime_unique_temp_dir(name: &str) -> PathBuf {
        let dir = runtime_unique_temp_path(name, "dir");
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn runtime_config_from_toml<T>(
        name: &str,
        content: &str,
        load: impl FnOnce(&str) -> anyhow::Result<T>,
    ) -> T {
        let path = runtime_unique_temp_path(name, "toml");
        fs::write(&path, content).unwrap();
        let cfg = load(path.to_str().unwrap()).unwrap();
        let _ = fs::remove_file(path);
        cfg
    }

    fn resolve_generated_toml(content: &str, vars: &[(&str, &str)]) -> String {
        let vars = vars.iter().copied().collect();
        crate::cmd::helpers::resolve_template(content, &vars).unwrap()
    }

    fn engine_bundle_toml_with_runtime_artifacts(content: &str, root: &Path) -> String {
        let mut value: toml::Value = toml::from_str(content).unwrap();
        let modules_dir = root.join("modules");
        let schemas_dir = root.join("schemas");
        let migrations_dir = root.join("migrations");
        fs::create_dir_all(&modules_dir).unwrap();
        fs::create_dir_all(&schemas_dir).unwrap();
        fs::create_dir_all(&migrations_dir).unwrap();

        let modules = value
            .get_mut("module")
            .and_then(toml::Value::as_array_mut)
            .expect("engine bundle TOML must contain module tables");

        for module in modules {
            let table = module.as_table_mut().unwrap();
            let name = table
                .get("name")
                .and_then(toml::Value::as_str)
                .unwrap()
                .to_string();

            let wasm_path = modules_dir.join(format!("{name}.wasm"));
            fs::write(&wasm_path, b"wasm").unwrap();
            table.insert(
                "wasm_path".to_string(),
                toml::Value::String(wasm_path.to_string_lossy().into_owned()),
            );

            if table.contains_key("schema_path") {
                let schema_path = schemas_dir.join(format!("{name}.binpb"));
                fs::write(&schema_path, b"schema").unwrap();
                table.insert(
                    "schema_path".to_string(),
                    toml::Value::String(schema_path.to_string_lossy().into_owned()),
                );
            }

            if table.contains_key("migrations_path") {
                let migrations_path = migrations_dir.join(&name);
                fs::create_dir_all(&migrations_path).unwrap();
                table.insert(
                    "migrations_path".to_string(),
                    toml::Value::String(migrations_path.to_string_lossy().into_owned()),
                );
            }
        }

        toml::to_string_pretty(&value).unwrap()
    }

    fn parse_engine_bundle_toml(config: &EngineConfig) -> toml::Value {
        let toml = config.to_bundle_config().to_toml().unwrap();
        toml::from_str(&toml).unwrap()
    }

    fn parse_manager_bundle_toml(config: &ManagerConfig) -> toml::Value {
        let toml = config.to_bundle_config().to_toml().unwrap();
        toml::from_str(&toml).unwrap()
    }

    fn parse_proxy_bundle_toml(config: &ProxyConfig) -> toml::Value {
        let toml = config.to_bundle_config().to_toml().unwrap();
        toml::from_str(&toml).unwrap()
    }

    #[test]
    fn engine_bundle_transform_preserves_runtime_owned_fields() {
        let source = r#"
listen_address = "127.0.0.1:9100"
allow_non_loopback_internal = true
max_outbound_body_bytes = 1048576

[database]
url = "postgres://localhost/source"
max_connections = 10

[pool]
max_memory = "1GiB"

[node]
proxy_address = "http://127.0.0.1:9001"
control_address = "http://127.0.0.1:9002"
peer_port = 9443
custom_node_key = "kept"

[node.tls]
cert_path = "certs/source.crt"
key_path = "certs/source.key"
ca_cert_path = "certs/source-ca.crt"
verify_name = "node.local"

[blobstore]
endpoint = "http://127.0.0.1:9000"
bucket = "objects"

[llm]
provider = "openai"
model = "gpt-4"

[limits]
max_component_size_bytes = 4096

[[module]]
name = "inventory"
namespace = "ecommerce"
version = "1.0.0"
wasm_path = "target/wasm32-wasip2/debug/inventory.wasm"
cwasm_path = "target/wasm32-wasip2/debug/inventory.cwasm"
schema_path = "schemas/inventory.binpb"
database = true
migrations_path = "migrations/inventory"
mode = "worker"
worker_concurrency = 4
worker_poll_interval_secs = 2
worker_job_timeout_secs = 30
worker_max_attempts = 5
blobstore = true
llm = true
fs = { "/data" = "./data" }

[module.env]
RUST_LOG = "debug"

[[module]]
name = "client"
namespace = "ecommerce"
version = "1.0.0"
wasm_path = "target/wasm32-wasip2/debug/client.wasm"
"#;

        let config: EngineConfig = toml::from_str(source).unwrap();
        let bundle = parse_engine_bundle_toml(&config);

        assert_eq!(bundle["database"]["url"].as_str(), Some("{db_url}"));
        assert_eq!(
            bundle["node"]["proxy_address"].as_str(),
            Some("http://{host}:9001")
        );
        assert_eq!(
            bundle["node"]["control_address"].as_str(),
            Some("http://{host}:9002")
        );
        assert_eq!(bundle["node"]["custom_node_key"].as_str(), Some("kept"));
        assert_eq!(
            bundle["node"]["tls"]["verify_name"].as_str(),
            Some("node.local")
        );
        assert_eq!(bundle["allow_non_loopback_internal"].as_bool(), Some(true));
        assert_eq!(
            bundle["max_outbound_body_bytes"].as_integer(),
            Some(1048576)
        );
        assert_eq!(bundle["blobstore"]["bucket"].as_str(), Some("objects"));
        assert_eq!(bundle["llm"]["provider"].as_str(), Some("openai"));
        assert_eq!(
            bundle["limits"]["max_component_size_bytes"].as_integer(),
            Some(4096)
        );

        let modules = bundle["module"].as_array().unwrap();
        let inventory = &modules[0];
        assert_eq!(
            inventory["wasm_path"].as_str(),
            Some("modules/inventory.wasm")
        );
        assert_eq!(
            inventory["cwasm_path"].as_str(),
            Some("modules/inventory.cwasm")
        );
        assert_eq!(
            inventory["schema_path"].as_str(),
            Some("schemas/inventory.binpb")
        );
        assert_eq!(
            inventory["migrations_path"].as_str(),
            Some("migrations/inventory")
        );
        assert_eq!(inventory["fs"]["/data"].as_str(), Some("./data"));
        assert_eq!(inventory["env"]["RUST_LOG"].as_str(), Some("debug"));
        assert_eq!(inventory["mode"].as_str(), Some("worker"));
        assert_eq!(inventory["worker_concurrency"].as_integer(), Some(4));
        assert_eq!(inventory["worker_poll_interval_secs"].as_integer(), Some(2));
        assert_eq!(inventory["worker_job_timeout_secs"].as_integer(), Some(30));
        assert_eq!(inventory["worker_max_attempts"].as_integer(), Some(5));
        assert_eq!(inventory["blobstore"].as_bool(), Some(true));
        assert_eq!(inventory["llm"].as_bool(), Some(true));

        let client = &modules[1];
        assert_eq!(client["wasm_path"].as_str(), Some("modules/client.wasm"));
        assert!(client.get("schema_path").is_none());
    }

    #[test]
    fn manager_bundle_transform_preserves_runtime_fields_and_drops_seed_nodes() {
        let source = r#"
listen_address = "127.0.0.1:9000"
local_proxy_address = "http://127.0.0.1:9001"
engine_heartbeat_timeout_secs = 20
module_heartbeat_timeout_secs = 30
scheduler_tick_secs = 2
scheduler_retry_tick_secs = 3
scheduler_lease_secs = 60

[database]
url = "postgres://localhost/source"
max_connections = 20
statement_timeout_secs = 5

[cluster]
cluster_id = "local"
gossip_listen_address = "127.0.0.1:9010"
advertise_grpc_address = "https://127.0.0.1:9000"
gossip_interval_ms = 500
seed_nodes = ["10.0.0.2:9010"]

[tls]
cert_path = "certs/source.crt"
key_path = "certs/source.key"
ca_cert_path = "certs/source-ca.crt"
server_name = "manager.local"
"#;

        let config: ManagerConfig = toml::from_str(source).unwrap();
        let bundle = parse_manager_bundle_toml(&config);

        assert_eq!(bundle["database"]["url"].as_str(), Some("{db_url}"));
        assert_eq!(
            bundle["cluster"]["advertise_grpc_address"].as_str(),
            Some("{advertise_address}")
        );
        assert_eq!(
            bundle["tls"]["cert_path"].as_str(),
            Some("certs/manager.crt")
        );
        assert_eq!(
            bundle["tls"]["key_path"].as_str(),
            Some("certs/manager.key")
        );
        assert_eq!(bundle["tls"]["ca_cert_path"].as_str(), Some("certs/ca.crt"));
        assert_eq!(bundle["tls"]["server_name"].as_str(), Some("manager.local"));
        assert_eq!(
            bundle["local_proxy_address"].as_str(),
            Some("http://127.0.0.1:9001")
        );
        assert_eq!(
            bundle["module_heartbeat_timeout_secs"].as_integer(),
            Some(30)
        );
        assert_eq!(bundle["scheduler_tick_secs"].as_integer(), Some(2));
        assert_eq!(bundle["scheduler_retry_tick_secs"].as_integer(), Some(3));
        assert_eq!(bundle["scheduler_lease_secs"].as_integer(), Some(60));
        assert_eq!(
            bundle["cluster"]["gossip_interval_ms"].as_integer(),
            Some(500)
        );
        assert!(bundle["cluster"].get("seed_nodes").is_none());
    }

    #[test]
    fn proxy_bundle_transform_preserves_runtime_sections() {
        let source = r#"
listen_address = "127.0.0.1:9001"
control_address = "127.0.0.1:9002"

[database]
url = "postgres://localhost/source"
max_connections = 12

[node]
proxy_address = "http://127.0.0.1:9443"
peer_port = 9443

[node.tls]
cert_path = "certs/source.crt"
key_path = "certs/source.key"
ca_cert_path = "certs/source-ca.crt"
server_name = "node.local"

[circuit_breaker]
failure_threshold = 7
reset_timeout_secs = 15

[external]
default_timeout_secs = 8

[[external.routes]]
prefix = "https://api.example.com"
allow = true

[egress]
allowed_hosts = ["api.example.com"]
"#;

        let config: ProxyConfig = toml::from_str(source).unwrap();
        let bundle = parse_proxy_bundle_toml(&config);

        assert_eq!(bundle["database"]["url"].as_str(), Some("{db_url}"));
        assert_eq!(bundle["database"]["max_connections"].as_integer(), Some(12));
        assert_eq!(
            bundle["node"]["proxy_address"].as_str(),
            Some("http://{host}:9443")
        );
        assert_eq!(bundle["node"]["peer_port"].as_integer(), Some(9443));
        assert_eq!(
            bundle["node"]["tls"]["cert_path"].as_str(),
            Some("certs/node.crt")
        );
        assert_eq!(
            bundle["node"]["tls"]["key_path"].as_str(),
            Some("certs/node.key")
        );
        assert_eq!(
            bundle["node"]["tls"]["ca_cert_path"].as_str(),
            Some("certs/ca.crt")
        );
        assert_eq!(
            bundle["node"]["tls"]["server_name"].as_str(),
            Some("node.local")
        );
        assert_eq!(
            bundle["circuit_breaker"]["failure_threshold"].as_integer(),
            Some(7)
        );
        assert_eq!(
            bundle["external"]["default_timeout_secs"].as_integer(),
            Some(8)
        );
        assert_eq!(
            bundle["external"]["routes"].as_array().unwrap()[0]["prefix"].as_str(),
            Some("https://api.example.com")
        );
        assert_eq!(
            bundle["egress"]["allowed_hosts"].as_array().unwrap()[0].as_str(),
            Some("api.example.com")
        );

        let minimal = r#"
listen_address = "127.0.0.1:9001"
control_address = "127.0.0.1:9002"

[database]
url = "postgres://localhost/source"

[node]
proxy_address = "http://127.0.0.1:9443"
"#;
        let minimal_config: ProxyConfig = toml::from_str(minimal).unwrap();
        let minimal_bundle = parse_proxy_bundle_toml(&minimal_config);
        assert!(minimal_bundle.get("external").is_none());
        assert!(minimal_bundle.get("egress").is_none());
    }

    #[test]
    fn generated_manager_bundle_toml_validates_with_runtime_config() {
        let source = r#"
            listen_address = "0.0.0.0:9000"
            engine_heartbeat_timeout_secs = 45
            local_proxy_address = "http://127.0.0.1:9001"
            scheduler_lease_secs = 60
            scheduler_retry_base_secs = 7
            scheduler_retry_cap_secs = 70

            [tls]
            cert_path = "certs/source-manager.crt"
            key_path = "certs/source-manager.key"
            ca_cert_path = "certs/source-ca.crt"

            [database]
            url = "postgres://postgres@localhost/source"
            max_connections = 12

            [cluster]
            cluster_id = "prod"
            gossip_listen_address = "0.0.0.0:9010"
            advertise_grpc_address = "http://127.0.0.1:9000"
            gossip_interval_ms = 750
            seed_nodes = ["10.0.0.2:9010"]
        "#;

        let bundle_toml = toml::from_str::<ManagerConfig>(source)
            .unwrap()
            .to_bundle_config()
            .to_toml()
            .unwrap();
        let resolved = resolve_generated_toml(
            &bundle_toml,
            &[
                ("db_url", "postgres://postgres@db/wruntime"),
                ("advertise_address", "https://manager.example:9000"),
            ],
        );
        assert!(!resolved.contains("seed_nodes"));

        let cfg =
            runtime_config_from_toml("generated-manager", &resolved, RuntimeManagerConfig::load);
        assert_eq!(cfg.local_proxy_address, "http://127.0.0.1:9001");
        assert_eq!(cfg.module_heartbeat_timeout_secs, Some(45));
        assert_eq!(cfg.scheduler_lease_secs, 60);
        assert_eq!(cfg.scheduler_retry_base_secs, 7);
        assert_eq!(cfg.scheduler_retry_cap_secs, 70);
        assert_eq!(cfg.cluster.gossip_interval_ms, 750);
    }

    #[test]
    fn generated_proxy_bundle_toml_validates_with_runtime_config() {
        let source = r#"
            listen_address = "127.0.0.1:9001"
            control_address = "127.0.0.1:9002"

            [node]
            proxy_address = "http://10.0.0.5:9001"
            peer_port = 9443

            [node.tls]
            cert_path = "certs/source-node.crt"
            key_path = "certs/source-node.key"
            ca_cert_path = "certs/source-ca.crt"

            [database]
            url = "postgres://postgres@localhost/source"
            max_connections = 4

            [cache]
            routing_table_ttl_secs = 3

            [circuit_breaker]
            failure_threshold = 7
            open_duration_secs = 45

            [egress]
            allowed_domains = ["api.github.com", "*.docs.rs"]

            [external]
            listen_address = "0.0.0.0:8080"

            [[external.route]]
            path = "/tasks"
            methods = ["POST"]
            module = "coordinator"
            namespace = "codegen"
        "#;

        let bundle_toml = toml::from_str::<ProxyConfig>(source)
            .unwrap()
            .to_bundle_config()
            .to_toml()
            .unwrap();
        let resolved = resolve_generated_toml(
            &bundle_toml,
            &[
                ("host", "127.0.0.1"),
                ("db_url", "postgres://postgres@db/wruntime"),
            ],
        );

        let cfg = runtime_config_from_toml("generated-proxy", &resolved, RuntimeProxyConfig::load);
        assert_eq!(cfg.listen_address, "127.0.0.1:9001");
        assert_eq!(cfg.control_address, "127.0.0.1:9002");
        assert_eq!(cfg.database.max_connections, 4);
        assert_eq!(cfg.cache.routing_table_ttl_secs, 3);
        assert_eq!(cfg.circuit_breaker.failure_threshold, 7);
        assert_eq!(cfg.circuit_breaker.open_duration_secs, 45);
        assert_eq!(
            cfg.egress.as_ref().unwrap().allowed_domains,
            vec!["api.github.com".to_string(), "*.docs.rs".to_string()]
        );
        assert_eq!(cfg.external.as_ref().unwrap().routes.len(), 1);
    }

    #[test]
    fn generated_engine_bundle_toml_validates_codegen_and_stockmarket_edges() {
        let codegen_source = include_str!("../../../examples/codegen/engine.toml");
        let codegen_bundle_toml = toml::from_str::<EngineConfig>(codegen_source)
            .unwrap()
            .to_bundle_config()
            .to_toml()
            .unwrap();
        assert!(codegen_bundle_toml.contains("[blobstore]"));
        assert!(codegen_bundle_toml.contains("[llm]"));
        assert!(codegen_bundle_toml.contains("fs = \"tempdir\""));
        assert!(codegen_bundle_toml.contains("worker_concurrency = 1"));
        assert!(codegen_bundle_toml.contains("worker_job_timeout_secs = 900"));
        assert!(codegen_bundle_toml.contains("migrations/agent"));

        let codegen_resolved = resolve_generated_toml(
            &codegen_bundle_toml,
            &[
                ("host", "127.0.0.1"),
                ("db_url", "postgres://postgres@db/codegen"),
            ],
        );
        let codegen_root = runtime_unique_temp_dir("codegen-engine");
        let codegen_runtime_toml =
            engine_bundle_toml_with_runtime_artifacts(&codegen_resolved, &codegen_root);
        let codegen = runtime_config_from_toml(
            "generated-codegen-engine",
            &codegen_runtime_toml,
            RuntimeEngineConfig::load,
        );
        assert!(codegen.blobstore.is_some());
        assert!(codegen.llm.is_some());
        assert!(codegen.modules.iter().any(|m| {
            m.name == "worker"
                && m.database
                && m.worker_concurrency == 1
                && m.worker_job_timeout_secs == 900
        }));
        let _ = fs::remove_dir_all(codegen_root);

        let ledger_source = include_str!("../../../examples/stockmarket/engine-ledger.toml");
        let ledger_bundle_toml = toml::from_str::<EngineConfig>(ledger_source)
            .unwrap()
            .to_bundle_config()
            .to_toml()
            .unwrap();
        assert!(ledger_bundle_toml.contains("[blobstore]"));
        assert!(ledger_bundle_toml.contains("blobstore = true"));

        let ledger_resolved = resolve_generated_toml(
            &ledger_bundle_toml,
            &[
                ("host", "127.0.0.1"),
                ("db_url", "postgres://postgres@db/stockmarket"),
            ],
        );
        let ledger_root = runtime_unique_temp_dir("ledger-engine");
        let ledger_runtime_toml =
            engine_bundle_toml_with_runtime_artifacts(&ledger_resolved, &ledger_root);
        let ledger = runtime_config_from_toml(
            "generated-ledger-engine",
            &ledger_runtime_toml,
            RuntimeEngineConfig::load,
        );
        assert!(ledger.blobstore.is_some());
        assert!(ledger
            .modules
            .iter()
            .any(|m| m.name == "ledger" && m.database && m.blobstore));
        let _ = fs::remove_dir_all(ledger_root);
    }
}
