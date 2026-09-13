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

pub(super) fn deployed_job_admin_address(address: &str, host: &str) -> anyhow::Result<String> {
    let port = super::helpers::extract_port(address)?;
    let authority_host = if host.parse::<std::net::Ipv6Addr>().is_ok() {
        format!("[{host}]")
    } else {
        host.to_string()
    };
    Ok(format!("https://{authority_host}:{}/", port.get()))
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
    pub deployment: Option<DeploymentConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<DatabaseConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_admin: Option<JobAdminConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<toml::Value>,
    #[serde(rename = "module", default, skip_serializing_if = "Vec::is_empty")]
    pub modules: Vec<ModuleConfig>,
    #[serde(flatten)]
    pub extra: ExtraFields,
}

#[derive(Deserialize, Serialize, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    #[serde(default)]
    pub proxy_address: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub control_address: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub peer_address: String,
}

#[derive(Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct CliServerTlsConfig {
    pub cert_path: String,
    pub key_path: String,
    pub client_ca_cert_path: String,
}

#[derive(Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct CliClientTlsConfig {
    pub cert_path: String,
    pub key_path: String,
    pub server_ca_cert_path: String,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct DeploymentConfig {
    pub node_id: String,
    pub revision: String,
    pub bundle_digest: String,
    pub engine_slot: String,
    pub operation_id: String,
    pub revision_digest: String,
    #[serde(flatten)]
    pub extra: ExtraFields,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct JobAdminConfig {
    pub listen_address: String,
    pub advertise_address: String,
    pub queue_id: String,
    pub tls: CliServerTlsConfig,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct DatabaseConfig {
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<TenantDatabaseConfig>,
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
#[serde(deny_unknown_fields)]
pub struct TenantDatabaseConfig {
    pub server_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_addr: Option<String>,
    pub port: u16,
    pub trust_root_path: String,
    pub client_cert_path: String,
    pub client_key_path: String,
    pub connect_timeout_secs: u64,
    #[serde(default)]
    pub expected_namespaces: Vec<NamespaceDatabaseExpectation>,
}

#[derive(Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct NamespaceDatabaseExpectation {
    pub namespace: String,
    pub generation: u64,
    pub deployment_digest: String,
    pub bundle_digest: String,
    #[serde(default)]
    pub migrations: Vec<ExpectedMigration>,
}

#[derive(Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ExpectedMigration {
    pub module: String,
    pub version: u64,
    pub filename: String,
    pub content_hash: String,
    pub byte_length: u64,
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
    /// Populate the authenticated namespace migration inventory shared by a
    /// generated local topology. SQL staging may use an earlier receipt digest;
    /// successful ledger rows are authenticated by immutable file identity,
    /// while engines bind this generated inventory to the manager revision.
    pub fn populate_local_tenant_expectations(configs: &mut [Self]) -> anyhow::Result<()> {
        let deployment_digest = configs
            .iter()
            .filter_map(|config| config.deployment.as_ref())
            .map(|deployment| deployment.revision_digest.as_str())
            .next()
            .context("managed deployment metadata is required")?
            .to_string();
        anyhow::ensure!(
            configs
                .iter()
                .filter_map(|config| config.deployment.as_ref())
                .all(|deployment| deployment.revision_digest == deployment_digest),
            "generated engine configs disagree on deployment digest"
        );

        let mut sources = std::collections::BTreeMap::new();
        for module in configs
            .iter()
            .flat_map(|config| &config.modules)
            .filter(|module| module.database)
        {
            if let Some(path) = module.migrations_path.as_deref() {
                let canonical = std::fs::canonicalize(path).with_context(|| {
                    format!(
                        "failed to canonicalize migrations_path for module '{}.{}'",
                        module.namespace, module.name
                    )
                })?;
                let key = (module.namespace.clone(), module.name.clone());
                if let Some(previous) = sources.insert(key.clone(), canonical.clone()) {
                    anyhow::ensure!(
                        previous == canonical,
                        "conflicting migration sources for module '{}.{}'",
                        key.0,
                        key.1
                    );
                }
            }
        }
        let source_list = sources
            .into_iter()
            .map(|((namespace, module), path)| (namespace, module, path))
            .collect::<Vec<_>>();
        let bundle = if source_list.is_empty() {
            None
        } else {
            Some(
                wr_common::migration_bundle::MigrationBundleManifest::capture_sources(
                    deployment_digest.clone(),
                    wr_common::migration_bundle::MigrationLimits {
                        max_migrations_per_namespace: 1_024,
                        max_file_bytes: 2 * 1024 * 1024,
                        max_startup_bytes: 64 * 1024 * 1024,
                        file_deadline_ms: 30_000,
                        cancellation_grace_ms: 5_000,
                    },
                    &source_list,
                )?,
            )
        };
        let bundle_digest = bundle
            .as_ref()
            .map(|bundle| bundle.manifest.bundle_digest.clone())
            .unwrap_or_else(|| format!("sha256:{}", "0".repeat(64)));

        let migrations = bundle
            .as_ref()
            .map(|bundle| {
                bundle
                    .files
                    .iter()
                    .map(|file| wr_common::migration_bundle::MigrationFileManifest {
                        namespace: file.manifest.namespace.clone(),
                        module: file.manifest.module.clone(),
                        version: file.manifest.version,
                        filename: file.manifest.filename.clone(),
                        content_hash: file.manifest.content_hash.clone(),
                        byte_length: file.manifest.byte_length,
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        Self::populate_receipt_tenant_expectations(
            configs,
            &deployment_digest,
            &bundle_digest,
            None,
            &migrations,
        )
    }

    /// Populate a deployment from already authenticated reservation/receipt data.
    /// Unlike local source capture, this never resolves migration paths on the
    /// operator host after a bundle has been created.
    pub fn populate_receipt_tenant_expectations(
        configs: &mut [Self],
        deployment_digest: &str,
        bundle_digest: &str,
        generation: Option<u64>,
        migrations: &[wr_common::migration_bundle::MigrationFileManifest],
    ) -> anyhow::Result<()> {
        for config in configs {
            let required = config
                .modules
                .iter()
                .filter(|module| module.database)
                .map(|module| module.namespace.as_str())
                .collect::<std::collections::BTreeSet<_>>();
            if required.is_empty() {
                continue;
            }
            let tenant = config
                .database
                .as_mut()
                .and_then(|database| database.tenant.as_mut())
                .context("database-enabled engine lacks node-local tenant configuration")?;
            anyhow::ensure!(
                tenant.expected_namespaces.len() == required.len()
                    && tenant
                        .expected_namespaces
                        .iter()
                        .all(|expected| required.contains(expected.namespace.as_str())),
                "tenant expected namespace inventory differs from database-enabled modules"
            );
            for expectation in &mut tenant.expected_namespaces {
                if let Some(generation) = generation {
                    expectation.generation = generation;
                }
                expectation.deployment_digest = deployment_digest.to_string();
                expectation.bundle_digest = bundle_digest.to_string();
                expectation.migrations = migrations
                    .iter()
                    .filter(|file| file.namespace == expectation.namespace)
                    .map(|file| ExpectedMigration {
                        module: file.module.clone(),
                        version: file.version,
                        filename: file.filename.clone(),
                        content_hash: file.content_hash.clone(),
                        byte_length: file.byte_length,
                    })
                    .collect();
            }
        }
        Ok(())
    }

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
    pub fn to_bundle_config(&self) -> anyhow::Result<Self> {
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

        // Engine-to-proxy HTTP and control traffic is host-local. Preserve the
        // explicit loopback addresses from the source config; only the proxy's
        // peer listener is target-advertised by the generated proxy config.
        if self.node.is_none() {
            anyhow::bail!("engine config requires [node]");
        }
        if let Some(ref node) = self.node {
            if !node.proxy_address.starts_with("http://")
                || !wr_common::node::is_loopback_addr(&node.proxy_address)
                || !node.control_address.starts_with("http://")
                || !wr_common::node::is_loopback_addr(&node.control_address)
            {
                anyhow::bail!(
                    "engine node proxy/control addresses must be absolute loopback HTTP URLs"
                );
            }
            super::helpers::extract_port(&node.proxy_address)?;
            super::helpers::extract_port(&node.control_address)?;
            super::helpers::extract_port(&node.peer_address)?;
            let mut bundled_node = node.clone();
            bundled_node.peer_address = "https://{host}:{peer_port}".to_string();
            config.node = Some(bundled_node);
        }

        // Template database URL and the manager-routable engine admin address.
        if let Some(ref mut db) = config.database {
            db.url = "{db_url}".to_string();
        }
        if let Some(ref mut job_admin) = config.job_admin {
            job_admin.advertise_address =
                deployed_job_admin_address(&job_admin.advertise_address, "{host}")?;
            job_admin.tls.cert_path =
                "/etc/wruntime/pki/engine-admin-endpoint/sets/v1/leaf.pem".to_string();
            job_admin.tls.key_path =
                "/etc/wruntime/pki/engine-admin-endpoint/sets/v1/key.pem".to_string();
            job_admin.tls.client_ca_cert_path = "/etc/wruntime/pki/roots/client/ca.crt".to_string();
        }

        Ok(config)
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
    pub endpoint_tls: CliServerTlsConfig,
    pub client_tls: CliClientTlsConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<ProxyDatabaseConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache: Option<ProxyCacheConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<ProxyStatusConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deployment: Option<ProxyDeploymentConfig>,
    #[serde(flatten)]
    pub extra: ExtraFields,
}

#[derive(Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ProxyNodeConfig {
    pub proxy_address: String,
    pub control_address: String,
    pub peer_address: String,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct ProxyDatabaseConfig {
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manager_liveness_threshold_secs: Option<u64>,
    #[serde(flatten)]
    pub extra: ExtraFields,
}

#[derive(Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ProxyStatusConfig {
    pub report_interval_secs: u64,
}

#[derive(Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ProxyDeploymentConfig {
    pub node_id: String,
    pub revision: String,
    pub bundle_digest: String,
    pub operation_id: String,
    pub revision_digest: String,
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
    pub manager_id: String,
    pub listen_address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine_heartbeat_timeout_secs: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_heartbeat_timeout_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_routing_freshness_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_tombstone_retention_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release_cleanup_interval_secs: Option<u64>,
    pub database: ManagerDatabaseConfig,
    pub cluster: ClusterConfig,
    pub tls: CliServerTlsConfig,
    pub client_tls: CliClientTlsConfig,
    pub authorization: ManagerAuthorizationConfig,
    #[serde(flatten)]
    pub extra: ExtraFields,
}

#[derive(Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ManagerAuthorizationConfig {
    pub policy_file: String,
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
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub advertise_grpc_address: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manager_heartbeat_interval_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manager_liveness_threshold_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manager_stale_row_reap_threshold_secs: Option<u64>,
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

    /// Create a bundle-ready copy with deploy-varying database and manager addresses.
    pub fn to_bundle_config(&self, _workdir: &str) -> Self {
        let mut config = self.clone();
        config.authorization.policy_file = format!(
            "/var/lib/wruntime/manager-config/{}/authorization.toml",
            self.manager_id
        );
        config.database.url = "{db_url}".to_string();
        config.cluster.advertise_grpc_address = Some("{advertise_address}".to_string());
        config.tls.cert_path = "/etc/wruntime/pki/manager-endpoint/sets/v1/leaf.pem".to_string();
        config.tls.key_path = "/etc/wruntime/pki/manager-endpoint/sets/v1/key.pem".to_string();
        config.tls.client_ca_cert_path = "/etc/wruntime/pki/roots/client/ca.crt".to_string();
        config.client_tls.cert_path =
            "/etc/wruntime/pki/manager-client/sets/v1/leaf.pem".to_string();
        config.client_tls.key_path = "/etc/wruntime/pki/manager-client/sets/v1/key.pem".to_string();
        config.client_tls.server_ca_cert_path = "/etc/wruntime/pki/roots/server/ca.crt".to_string();
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
    pub fn to_bundle_config(&self) -> anyhow::Result<Self> {
        let mut config = self.clone();
        if let Some(ref mut db) = config.database {
            db.url = "{db_url}".to_string();
        }
        if let (Some(source_node), Some(config_node)) = (&self.node, &mut config.node) {
            if !wr_common::node::is_loopback_addr(&self.listen_address)
                || self
                    .control_address
                    .as_deref()
                    .is_none_or(|address| !wr_common::node::is_loopback_addr(address))
                || !source_node.proxy_address.starts_with("http://")
                || !wr_common::node::is_loopback_addr(&source_node.proxy_address)
                || !source_node.control_address.starts_with("http://")
                || !wr_common::node::is_loopback_addr(&source_node.control_address)
            {
                anyhow::bail!(
                    "proxy listen/control and node proxy/control addresses must use loopback"
                );
            }
            super::helpers::extract_port(&source_node.proxy_address)?;
            super::helpers::extract_port(&source_node.control_address)?;
            super::helpers::extract_port(&source_node.peer_address)?;
            config_node.proxy_address = source_node.proxy_address.clone();
            config_node.control_address = source_node.control_address.clone();
            config_node.peer_address = "https://{host}:{peer_port}".to_string();
            config.endpoint_tls.cert_path =
                "/etc/wruntime/pki/proxy-endpoint/sets/v1/leaf.pem".to_string();
            config.endpoint_tls.key_path =
                "/etc/wruntime/pki/proxy-endpoint/sets/v1/key.pem".to_string();
            config.endpoint_tls.client_ca_cert_path =
                "/etc/wruntime/pki/roots/client/ca.crt".to_string();
            config.client_tls.cert_path =
                "/etc/wruntime/pki/proxy-client/sets/v1/leaf.pem".to_string();
            config.client_tls.key_path =
                "/etc/wruntime/pki/proxy-client/sets/v1/key.pem".to_string();
            config.client_tls.server_ca_cert_path =
                "/etc/wruntime/pki/roots/server/ca.crt".to_string();
        }
        Ok(config)
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
    use wr_manager::config::RawManagerConfig as RuntimeManagerConfig;
    use wr_proxy::config::ProxyConfig as RuntimeProxyConfig;

    #[test]
    fn deployed_job_admin_address_is_canonical_and_ipv6_safe() {
        assert_eq!(
            deployed_job_admin_address("https://127.0.0.1:9150", "192.0.2.10").unwrap(),
            "https://192.0.2.10:9150/"
        );
        assert_eq!(
            deployed_job_admin_address("https://127.0.0.1:9150/", "2001:db8::10").unwrap(),
            "https://[2001:db8::10]:9150/"
        );
        assert_eq!(
            deployed_job_admin_address("https://127.0.0.1:9150/", "{host}").unwrap(),
            "https://{host}:9150/"
        );
    }

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
        let toml = config.to_bundle_config().unwrap().to_toml().unwrap();
        toml::from_str(&toml).unwrap()
    }

    fn parse_manager_bundle_toml(config: &ManagerConfig) -> toml::Value {
        let toml = config.to_bundle_config("/opt/wruntime").to_toml().unwrap();
        toml::from_str(&toml).unwrap()
    }

    fn parse_proxy_bundle_toml(config: &ProxyConfig) -> toml::Value {
        let toml = config.to_bundle_config().unwrap().to_toml().unwrap();
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
peer_address = "https://127.0.0.1:9443"

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
            Some("http://127.0.0.1:9001")
        );
        assert_eq!(
            bundle["node"]["control_address"].as_str(),
            Some("http://127.0.0.1:9002")
        );
        assert_eq!(
            bundle["node"]["peer_address"].as_str(),
            Some("https://{host}:{peer_port}")
        );
        assert!(bundle["node"].get("tls").is_none());
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
    fn manager_bundle_transform_preserves_runtime_fields() {
        let source = r#"
manager_id = "manager-a"
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
advertise_grpc_address = "https://127.0.0.1:9000"
manager_heartbeat_interval_secs = 1
manager_liveness_threshold_secs = 5
manager_stale_row_reap_threshold_secs = 300

[tls]
cert_path = "certs/source-endpoint.crt"
key_path = "certs/source-endpoint.key"
client_ca_cert_path = "certs/source-client-root.crt"

[client_tls]
cert_path = "certs/source-manager-client.crt"
key_path = "certs/source-manager-client.key"
server_ca_cert_path = "certs/source-server-root.crt"

[authorization]
policy_file = "policy/source.toml"
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
            Some("/etc/wruntime/pki/manager-endpoint/sets/v1/leaf.pem")
        );
        assert_eq!(
            bundle["tls"]["key_path"].as_str(),
            Some("/etc/wruntime/pki/manager-endpoint/sets/v1/key.pem")
        );
        assert_eq!(
            bundle["tls"]["client_ca_cert_path"].as_str(),
            Some("/etc/wruntime/pki/roots/client/ca.crt")
        );
        assert_eq!(
            bundle["client_tls"]["cert_path"].as_str(),
            Some("/etc/wruntime/pki/manager-client/sets/v1/leaf.pem")
        );
        assert!(bundle.get("job_admin").is_none());
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
            bundle["cluster"]["manager_heartbeat_interval_secs"].as_integer(),
            Some(1)
        );
        assert_eq!(
            bundle["cluster"]["manager_liveness_threshold_secs"].as_integer(),
            Some(5)
        );
        assert_eq!(
            bundle["cluster"]["manager_stale_row_reap_threshold_secs"].as_integer(),
            Some(300)
        );
    }

    #[test]
    fn proxy_bundle_transform_preserves_runtime_sections() {
        let source = r#"
listen_address = "127.0.0.1:9001"
control_address = "127.0.0.1:9002"

[database]
url = "postgres://localhost/source"
max_connections = 12
manager_liveness_threshold_secs = 7

[node]
proxy_address = "http://127.0.0.1:9001"
control_address = "http://127.0.0.1:9002"
peer_address = "https://127.0.0.1:9443"

[endpoint_tls]
cert_path = "certs/source-endpoint.crt"
key_path = "certs/source-endpoint.key"
client_ca_cert_path = "certs/source-client-root.crt"

[client_tls]
cert_path = "certs/source-client.crt"
key_path = "certs/source-client.key"
server_ca_cert_path = "certs/source-server-root.crt"

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
            bundle["database"]["manager_liveness_threshold_secs"].as_integer(),
            Some(7)
        );
        assert_eq!(
            bundle["node"]["proxy_address"].as_str(),
            Some("http://127.0.0.1:9001")
        );
        assert_eq!(
            bundle["node"]["peer_address"].as_str(),
            Some("https://{host}:{peer_port}")
        );
        assert_eq!(
            bundle["endpoint_tls"]["cert_path"].as_str(),
            Some("/etc/wruntime/pki/proxy-endpoint/sets/v1/leaf.pem")
        );
        assert_eq!(
            bundle["client_tls"]["cert_path"].as_str(),
            Some("/etc/wruntime/pki/proxy-client/sets/v1/leaf.pem")
        );
        assert!(bundle["node"].get("tls").is_none());
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
proxy_address = "http://127.0.0.1:9001"
control_address = "http://127.0.0.1:9002"
peer_address = "https://127.0.0.1:9443"

[endpoint_tls]
cert_path = "endpoint.crt"
key_path = "endpoint.key"
client_ca_cert_path = "client-root.crt"

[client_tls]
cert_path = "client.crt"
key_path = "client.key"
server_ca_cert_path = "server-root.crt"
"#;
        let minimal_config: ProxyConfig = toml::from_str(minimal).unwrap();
        let minimal_bundle = parse_proxy_bundle_toml(&minimal_config);
        assert!(minimal_bundle.get("external").is_none());
        assert!(minimal_bundle.get("egress").is_none());
    }

    #[test]
    fn generated_manager_bundle_toml_validates_with_runtime_config() {
        let source = r#"
            manager_id = "manager-a"
            listen_address = "0.0.0.0:9000"
            engine_heartbeat_timeout_secs = 45
            local_proxy_address = "http://127.0.0.1:9001"
            scheduler_lease_secs = 60
            scheduler_retry_base_secs = 7
            scheduler_retry_cap_secs = 70

            [tls]
            cert_path = "certs/source-manager.crt"
            key_path = "certs/source-manager.key"
            client_ca_cert_path = "certs/source-client-root.crt"

            [client_tls]
            cert_path = "certs/source-manager-client.crt"
            key_path = "certs/source-manager-client.key"
            server_ca_cert_path = "certs/source-server-root.crt"

            [authorization]
            policy_file = "policy/source.toml"

            [database]
            url = "postgres://postgres@localhost/source"
            max_connections = 12

            [cluster]
            advertise_grpc_address = "https://127.0.0.1:9000"
            manager_heartbeat_interval_secs = 2
            manager_liveness_threshold_secs = 8
            manager_stale_row_reap_threshold_secs = 120
        "#;

        let bundle_toml = toml::from_str::<ManagerConfig>(source)
            .unwrap()
            .to_bundle_config("/opt/wruntime")
            .to_toml()
            .unwrap();
        let resolved = resolve_generated_toml(
            &bundle_toml,
            &[
                ("db_url", "postgres://postgres@db/wruntime"),
                ("advertise_address", "https://manager.example:9000"),
            ],
        );

        let cfg: RuntimeManagerConfig = toml::from_str(&resolved).unwrap();
        assert_eq!(cfg.local_proxy_address, "http://127.0.0.1:9001");
        assert_eq!(cfg.module_heartbeat_timeout_secs, None);
        assert_eq!(cfg.scheduler_lease_secs, 60);
        assert_eq!(cfg.scheduler_retry_base_secs, 7);
        assert_eq!(cfg.scheduler_retry_cap_secs, 70);
        assert_eq!(cfg.cluster.manager_heartbeat_interval_secs, 2);
        assert_eq!(cfg.cluster.manager_liveness_threshold_secs, 8);
        assert_eq!(cfg.cluster.manager_stale_row_reap_threshold_secs, 120);
    }

    #[test]
    fn generated_proxy_bundle_toml_validates_with_runtime_config() {
        let source = r#"
            listen_address = "127.0.0.1:9001"
            control_address = "127.0.0.1:9002"

            [node]
            proxy_address = "http://127.0.0.1:9001"
            control_address = "http://127.0.0.1:9002"
            peer_address = "https://10.0.0.5:9443"

            [endpoint_tls]
            cert_path = "certs/source-endpoint.crt"
            key_path = "certs/source-endpoint.key"
            client_ca_cert_path = "certs/source-client-root.crt"

            [client_tls]
            cert_path = "certs/source-client.crt"
            key_path = "certs/source-client.key"
            server_ca_cert_path = "certs/source-server-root.crt"

            [database]
            url = "postgres://postgres@localhost/source"
            max_connections = 4
            manager_liveness_threshold_secs = 8

            [cache]
            routing_table_ttl_secs = 3

            [circuit_breaker]
            failure_threshold = 7
            open_duration_secs = 45

            [egress]
            allowed_domains = ["api.github.com", "*.docs.rs"]

            [external]
            listen_address = "0.0.0.0:8080"

            [[external.routes]]
            path = "/tasks"
            rpc_path = "/codegen.CoordinatorService/RunTask"
            methods = ["POST"]
            module = "coordinator"
            namespace = "codegen"
        "#;

        let bundle_toml = toml::from_str::<ProxyConfig>(source)
            .unwrap()
            .to_bundle_config()
            .unwrap()
            .to_toml()
            .unwrap();
        let resolved = resolve_generated_toml(
            &bundle_toml,
            &[
                ("host", "127.0.0.1"),
                ("peer_port", "9443"),
                ("db_url", "postgres://postgres@db/wruntime"),
            ],
        );

        let cfg = runtime_config_from_toml("generated-proxy", &resolved, RuntimeProxyConfig::load);
        assert_eq!(cfg.listen_address, "127.0.0.1:9001");
        assert_eq!(cfg.control_address, "127.0.0.1:9002");
        assert_eq!(cfg.database.max_connections, 4);
        assert_eq!(cfg.database.manager_liveness_threshold_secs, 8);
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
    fn local_tenant_generation_carries_complete_cross_engine_migration_inventory() {
        let root = runtime_unique_temp_dir("tenant-topology");
        let first = root.join("first");
        let second = root.join("second");
        fs::create_dir_all(&first).unwrap();
        fs::create_dir_all(&second).unwrap();
        fs::write(first.join("V1__first.sql"), "SELECT 1;").unwrap();
        fs::write(second.join("V1__second.sql"), "SELECT 2;").unwrap();
        let source = |name: &str, migrations: &Path| {
            toml::from_str::<EngineConfig>(&format!(
                r#"
listen_address = "127.0.0.1:9100"
[node]
proxy_address = "http://127.0.0.1:9001"
control_address = "http://127.0.0.1:9002"
peer_address = "https://127.0.0.1:9443"
[deployment]
node_id = "node-a"
revision = "1"
bundle_digest = "sha256:{zeros}"
engine_slot = "engine-{name}"
operation_id = "operation"
revision_digest = "sha256:{revision}"
[database]
url = "postgres://postgres@localhost/jobs"
[database.tenant]
server_name = "localhost"
host_addr = "127.0.0.1"
port = 5432
trust_root_path = "/tmp/ca.pem"
client_cert_path = "/tmp/client.pem"
client_key_path = "/tmp/client.key"
connect_timeout_secs = 1
[[database.tenant.expected_namespaces]]
namespace = "shop"
generation = 1
deployment_digest = "sha256:{zeros}"
bundle_digest = "sha256:{zeros}"
[[module]]
name = "{name}"
namespace = "shop"
version = "1.0.0"
wasm_path = "{name}.wasm"
database = true
migrations_path = "{}"
"#,
                migrations.display(),
                zeros = "0".repeat(64),
                revision = "a".repeat(64),
            ))
            .unwrap()
        };
        let mut configs = vec![source("first", &first), source("second", &second)];
        EngineConfig::populate_local_tenant_expectations(&mut configs).unwrap();
        for config in &configs {
            let expectation = &config
                .database
                .as_ref()
                .unwrap()
                .tenant
                .as_ref()
                .unwrap()
                .expected_namespaces[0];
            assert_eq!(
                expectation.deployment_digest,
                format!("sha256:{}", "a".repeat(64))
            );
            assert_eq!(expectation.migrations.len(), 2);
            assert_eq!(expectation.migrations[0].module, "first");
            assert_eq!(expectation.migrations[1].module, "second");
        }
        let rendered = configs[0].to_toml().unwrap();
        assert!(!rendered.contains("password"));
        assert!(!rendered.contains("admin_url"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn generated_engine_bundle_toml_validates_codegen_and_stockmarket_edges() {
        let codegen_source = include_str!("../../../examples/codegen/engine.toml");
        let codegen_bundle_toml = toml::from_str::<EngineConfig>(codegen_source)
            .unwrap()
            .to_bundle_config()
            .unwrap()
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
                ("peer_port", "9443"),
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
            .unwrap()
            .to_toml()
            .unwrap();
        assert!(ledger_bundle_toml.contains("[blobstore]"));
        assert!(ledger_bundle_toml.contains("blobstore = true"));

        let ledger_resolved = resolve_generated_toml(
            &ledger_bundle_toml,
            &[
                ("host", "127.0.0.1"),
                ("peer_port", "9443"),
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
