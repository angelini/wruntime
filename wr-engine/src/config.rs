use std::collections::HashMap;
use std::fmt;

use anyhow::Result;
use serde::de::{value::MapAccessDeserializer, Error as _, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use wr_common::identity::ModuleId;
use wr_common::node::{is_loopback_addr, NodeConfig};

#[derive(Deserialize, Clone)]
pub struct EngineConfig {
    /// Address this engine listens on for inbound requests from the proxy
    pub listen_address: String,
    /// Escape hatch: permit `listen_address` to bind a non-loopback interface.
    /// Defaults to `false` — engine listeners should be loopback (the local
    /// proxy reaches them directly; cross-node traffic uses the mTLS peer
    /// listener). Operators enabling this own reachability of the advertised
    /// address (the `0.0.0.0`→`127.0.0.1` rewrite in main.rs stays same-host only).
    #[serde(default)]
    pub allow_non_loopback_internal: bool,
    /// Node configuration — identifies the local proxy for this engine.
    pub node: NodeConfig,
    /// Immutable deployment identity injected into a staged release by wr-cli.
    /// Local development configurations may omit this until they are bundled.
    #[serde(default)]
    pub deployment: Option<DeploymentMetadata>,
    #[serde(rename = "module", default)]
    pub modules: Vec<ModuleConfig>,
    /// Optional PostgreSQL settings for per-namespace guest connection pools.
    pub database: Option<DatabaseConfig>,
    /// Optional S3-compatible blobstore shared across blobstore-enabled modules.
    pub blobstore: Option<BlobstoreConfig>,
    /// Optional LLM provider for inference-enabled modules.
    pub llm: Option<LlmConfig>,
    /// WASM instance pooling allocator configuration.
    /// Wasmtime pre-allocates a pool of instance slots to avoid per-request
    /// memory mapping overhead. All fields have sensible defaults so an empty
    /// `[pool]` section (or omitting it entirely) enables pooling with defaults.
    #[serde(default)]
    pub pool: PoolConfig,
    /// Ceilings on guest-created host resources (spans, DB tx/cursors, LLM streams).
    /// Omitting the `[limits]` section uses the defaults.
    #[serde(default)]
    pub limits: ResourceLimits,
    /// Maximum outbound HTTP request body size in bytes that a guest may send.
    /// Bodies are buffered up to this bound and rejected beyond it with
    /// `HttpRequestBodySize`. Defaults to 16 MiB.
    #[serde(default = "default_max_outbound_body_bytes")]
    pub max_outbound_body_bytes: usize,
}

#[derive(Deserialize, Clone)]
#[serde(default)]
pub struct PoolConfig {
    /// Maximum number of concurrent component instances across all modules.
    /// Defaults to 1000.
    pub total_component_instances: u32,
    /// Maximum linear memory size in bytes per instance. Defaults to 10 MiB.
    pub max_memory_size: usize,
    /// Epoch tick interval in milliseconds. A background task increments the
    /// wasmtime epoch at this rate, enabling preemption of CPU-bound WASM
    /// modules that never yield to the host. Defaults to 10.
    pub epoch_tick_interval_ms: u64,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            total_component_instances: 1000,
            max_memory_size: 10 * 1024 * 1024, // 10 MiB
            epoch_tick_interval_ms: 10,
        }
    }
}

/// Per-store ceilings on guest-created host resources. Enforced live (one
/// running count per kind), so a guest cannot exhaust the wasmtime
/// `ResourceTable` and crash the engine. Applies globally across modules.
#[derive(Deserialize, Clone, Debug)]
#[serde(default)]
pub struct ResourceLimits {
    /// Max concurrent guest-created tracing spans per request. Defaults to 1024.
    pub max_spans: u32,
    /// Max concurrent open DB transactions per request. Defaults to 64.
    pub max_db_transactions: u32,
    /// Max concurrent open DB row cursors per request. Defaults to 256.
    pub max_db_cursors: u32,
    /// Max concurrent open LLM completion streams per request. Defaults to 32.
    pub max_llm_streams: u32,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_spans: 1024,
            max_db_transactions: 64,
            max_db_cursors: 256,
            max_llm_streams: 32,
        }
    }
}

#[derive(Deserialize, Clone)]
pub struct DeploymentMetadata {
    pub node_id: String,
    pub revision: u64,
    pub bundle_digest: String,
    pub engine_slot: String,
}

#[derive(Deserialize, Clone)]
pub struct DatabaseConfig {
    /// `postgres://user:pass@host:port/dbname` connection string.
    /// Used for admin operations (schema provisioning, migrations).
    pub url: String,
    /// Maximum number of pooled connections. Defaults to 20.
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    /// Per-statement timeout in seconds applied to every guest connection.
    /// Prevents runaway queries from consuming CPU/IO indefinitely.
    /// Defaults to 30.
    #[serde(default = "default_db_statement_timeout_secs")]
    pub statement_timeout_secs: u32,
    /// Timeout in seconds for idle-in-transaction sessions.
    /// Kills connections that hold a transaction open without activity.
    /// Defaults to 60.
    #[serde(default = "default_db_idle_in_transaction_timeout_secs")]
    pub idle_in_transaction_timeout_secs: u32,
    /// Database span disclosure controls. Query text is omitted by default.
    #[serde(default)]
    pub telemetry: DatabaseTelemetryConfig,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct DatabaseTelemetryConfig {
    pub include_query_text: bool,
}

fn default_max_connections() -> usize {
    20
}

fn default_db_statement_timeout_secs() -> u32 {
    30
}

fn default_db_idle_in_transaction_timeout_secs() -> u32 {
    60
}

#[derive(Deserialize, Clone)]
pub struct BlobstoreConfig {
    /// S3-compatible endpoint URL, e.g. "http://127.0.0.1:8900"
    pub endpoint: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    /// Buckets guests may access. Required and must contain at least one bucket.
    pub allowed_buckets: Vec<String>,
    /// S3 region. Defaults to "us-east-1".
    #[serde(default = "default_bs_region")]
    pub region: String,
    /// Max object size in bytes, enforced on both upload and download.
    /// Defaults to 16 MiB.
    #[serde(default = "default_max_object_size")]
    pub max_object_size: usize,
    /// Max objects returned by a single list-objects call. Defaults to 1000.
    #[serde(default = "default_max_list_objects")]
    pub max_list_objects: usize,
}

fn default_bs_region() -> String {
    "us-east-1".into()
}

fn default_max_object_size() -> usize {
    16 * 1024 * 1024
}

fn default_max_list_objects() -> usize {
    1000
}

fn default_max_outbound_body_bytes() -> usize {
    16 * 1024 * 1024
}

/// Host-enforced blobstore size/count ceilings. Global (the blobstore client is
/// shared across modules). Carried to enforcement via `ModuleServices` →
/// `BlobstoreCapability`, mirroring how `blob_prefix` flows.
#[derive(Clone, Copy, Debug)]
pub struct BlobstoreLimits {
    /// Upload + download byte ceiling. Checked before `put` and during streaming `get`.
    pub max_object_size: usize,
    /// Per-call listing cap; `list_objects` returns `too-large` beyond this.
    pub max_list_objects: usize,
}

impl Default for BlobstoreLimits {
    fn default() -> Self {
        Self {
            max_object_size: default_max_object_size(),
            max_list_objects: default_max_list_objects(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LlmProvider {
    Anthropic,
}

#[derive(Deserialize, Clone)]
pub struct LlmConfig {
    /// LLM provider. Currently only "anthropic" is supported.
    pub provider: LlmProvider,
    /// Environment variable name that holds the API key.
    /// Resolved at engine startup, never passed to guests.
    pub api_key_env: String,
    /// Base URL for the API. Defaults to "https://api.anthropic.com".
    #[serde(default = "default_llm_base_url")]
    pub base_url: String,
    /// Host-enforced ceiling on max_tokens per request.
    #[serde(default = "default_max_tokens_limit")]
    pub max_tokens_limit: u32,
}

fn default_llm_base_url() -> String {
    "https://api.anthropic.com".into()
}

fn default_max_tokens_limit() -> u32 {
    8192
}

/// Filesystem access mode for a module.
#[derive(Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum FsMode {
    /// Mount an ephemeral temp directory at `/`. Deleted when the store is dropped.
    Tempdir,
}

/// Module execution mode.
#[derive(Debug, Deserialize, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ModuleMode {
    /// HTTP request handler with per-request instantiation (exports `wasi:http/incoming-handler`).
    #[default]
    Service,
    /// Service guest driven by an engine-managed job queue instead of external HTTP traffic.
    Worker,
}

/// A validated secret environment reference. The TOML marker must be
/// `{ secret = true }`; `false` is not a meaningful runtime state.
#[derive(Clone, Debug)]
pub struct SecretEnvRef;

#[derive(Deserialize)]
struct RawSecretEnvRef {
    secret: bool,
}

impl<'de> Deserialize<'de> for SecretEnvRef {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawSecretEnvRef::deserialize(deserializer)?;
        if raw.secret {
            Ok(Self)
        } else {
            Err(D::Error::custom("secret reference must use secret = true"))
        }
    }
}

/// An environment variable value: either a plain string or a secret reference.
#[derive(Clone)]
pub enum EnvValue {
    /// Inline plaintext value, e.g. `LOG_LEVEL = "debug"`
    Plain(String),
    /// Secret fetched from the manager, e.g. `API_KEY = { secret = true }`
    Secret(SecretEnvRef),
}

struct EnvValueVisitor;

impl<'de> Visitor<'de> for EnvValueVisitor {
    type Value = EnvValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a string or { secret = true }")
    }

    fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(EnvValue::Plain(value.to_string()))
    }

    fn visit_string<E>(self, value: String) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(EnvValue::Plain(value))
    }

    fn visit_map<A>(self, map: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        SecretEnvRef::deserialize(MapAccessDeserializer::new(map)).map(EnvValue::Secret)
    }
}

impl<'de> Deserialize<'de> for EnvValue {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(EnvValueVisitor)
    }
}

#[derive(Deserialize, Clone)]
pub struct ModuleConfig {
    pub name: String,
    pub namespace: String,
    pub version: String,
    pub wasm_path: String,
    /// Path to a pre-compiled native artifact (`.cwasm`).
    /// When present and compatible, the engine deserializes this instead of
    /// JIT-compiling the `.wasm`, reducing startup time to ~microseconds.
    #[serde(default)]
    pub cwasm_path: Option<String>,
    /// Path to a compiled `FileDescriptorSet` binary for this module's API.
    /// The first config occurrence of each unique (namespace, name, version)
    /// tuple must provide a non-empty existing schema path. Later duplicate
    /// instances may omit it; if present, it is still validated.
    #[serde(default)]
    pub schema_path: Option<String>,
    /// Whether this module has access to its namespace's database pool.
    /// Requires a `[database]` section in the engine config.
    #[serde(default)]
    pub database: bool,
    /// This module's contribution to its namespace pool's maximum size.
    /// Falls back to `[database].max_connections` when absent; contributions
    /// from all DB-enabled modules in the namespace are summed.
    #[serde(default)]
    pub db_max_connections: Option<usize>,
    /// Whether this module has access to the shared blobstore client.
    /// Requires a `[blobstore]` section in the engine config.
    #[serde(default)]
    pub blobstore: bool,
    /// Whether this module has access to the LLM inference API.
    /// Requires an `[llm]` section in the engine config.
    #[serde(default)]
    pub llm: bool,
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
    /// Path to a directory containing V-prefixed SQL migration files
    /// (e.g., `V1__create_tables.sql`). When set, migrations run at engine
    /// startup before the module handles traffic.
    #[serde(default)]
    pub migrations_path: Option<String>,
    /// Environment variables injected into the WASI context.
    /// Plain values are used directly; `{ secret = true }` values are
    /// resolved from secrets delivered by the manager at registration time.
    #[serde(default)]
    pub env: HashMap<String, EnvValue>,
    /// Module execution mode: service (default) or worker.
    #[serde(default)]
    pub mode: ModuleMode,
    /// Number of concurrent worker tasks polling the job queue. Only used when `mode = "worker"`.
    #[serde(default = "default_worker_concurrency")]
    pub worker_concurrency: usize,
    /// Fallback poll interval in seconds when no LISTEN notification arrives. Only used when `mode = "worker"`.
    #[serde(default = "default_worker_poll_interval_secs")]
    pub worker_poll_interval_secs: u64,
    /// Per-job timeout in seconds. Only used when `mode = "worker"`.
    #[serde(default = "default_worker_job_timeout_secs")]
    pub worker_job_timeout_secs: u64,
    /// Maximum delivery attempts before a job is marked dead. Only used when `mode = "worker"`.
    #[serde(default = "default_worker_max_attempts")]
    pub worker_max_attempts: u32,
}

fn default_request_timeout_secs() -> u64 {
    30
}

fn default_channel_capacity() -> usize {
    128
}

fn default_worker_concurrency() -> usize {
    4
}

fn default_worker_poll_interval_secs() -> u64 {
    2
}

fn default_worker_job_timeout_secs() -> u64 {
    300
}

fn default_worker_max_attempts() -> u32 {
    3
}

impl wr_common::config::Validatable for EngineConfig {
    fn validate(&self) -> Result<()> {
        self.validate_inner()
    }
}

impl EngineConfig {
    pub fn load(path: &str) -> Result<Self> {
        wr_common::config::load(path)
    }

    fn validate_inner(&self) -> Result<()> {
        use wr_common::config::Validator;
        let mut v = Validator::new();

        v.check(
            !self.listen_address.is_empty(),
            "listen_address is required",
        );
        v.check(
            self.allow_non_loopback_internal || is_loopback_addr(&self.listen_address),
            "listen_address must bind to loopback (127.0.0.1, ::1, or localhost); \
             set allow_non_loopback_internal = true to override",
        );
        v.check(
            self.node.proxy_address.starts_with("http://")
                && is_loopback_addr(&self.node.proxy_address),
            "node.proxy_address must be an absolute loopback HTTP URL",
        );
        v.check(
            self.node.control_address.starts_with("http://")
                && is_loopback_addr(&self.node.control_address),
            "node.control_address must be an absolute loopback HTTP URL",
        );
        if let Err(error) = self.node.peer_address() {
            v.check(false, format!("invalid node configuration: {error}"));
        }
        if let Some(deployment) = &self.deployment {
            v.check(
                !deployment.node_id.is_empty(),
                "deployment.node_id is required",
            );
            v.check(deployment.revision > 0, "deployment.revision must be > 0");
            v.check(
                deployment.bundle_digest.starts_with("sha256:")
                    && deployment.bundle_digest.len() == 71
                    && deployment.bundle_digest[7..]
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()),
                "deployment.bundle_digest must be sha256:<lowercase hex>",
            );
            v.check(
                !deployment.engine_slot.is_empty(),
                "deployment.engine_slot is required",
            );
        }

        if let Some(database) = &self.database {
            v.check(
                database.max_connections > 0,
                "database.max_connections must be > 0",
            );
            v.check(
                database.statement_timeout_secs > 0,
                "database.statement_timeout_secs must be > 0",
            );
            v.check(
                database.idle_in_transaction_timeout_secs > 0,
                "database.idle_in_transaction_timeout_secs must be > 0",
            );
        }
        if let Some(blobstore) = &self.blobstore {
            v.check(
                !blobstore.allowed_buckets.is_empty(),
                "blobstore.allowed_buckets must contain at least one bucket",
            );
            v.check(
                blobstore
                    .allowed_buckets
                    .iter()
                    .all(|bucket| !bucket.trim().is_empty()),
                "blobstore.allowed_buckets must not contain empty bucket names",
            );
        }
        if let Some(llm) = &self.llm {
            v.check(
                !llm.api_key_env.trim().is_empty(),
                "llm.api_key_env is required",
            );
            v.check(!llm.base_url.trim().is_empty(), "llm.base_url is required");
            v.check(llm.max_tokens_limit > 0, "llm.max_tokens_limit must be > 0");
        }

        let mut first_seen_modules = std::collections::HashSet::<(String, String, String)>::new();
        for module in &self.modules {
            let m = &module.name;
            v.check(!module.name.is_empty(), "module.name is required");
            v.check(!module.namespace.is_empty(), "module.namespace is required");
            v.check(!module.version.is_empty(), "module.version is required");
            if let Err(error) = ModuleId::parse(&module.namespace, &module.name, &module.version) {
                v.check(false, format!("invalid module identity '{m}': {error}"));
            }
            v.check(
                std::path::Path::new(&module.wasm_path).exists(),
                format!("wasm_path not found for module '{m}': {}", module.wasm_path),
            );
            let first_occurrence = first_seen_modules.insert((
                module.namespace.clone(),
                module.name.clone(),
                module.version.clone(),
            ));
            match module.schema_path.as_deref() {
                Some(schema_path) if schema_path.trim().is_empty() => {
                    v.check(
                        false,
                        format!("schema_path for module '{m}' must not be empty"),
                    );
                }
                Some(schema_path) => {
                    v.check(
                        std::path::Path::new(schema_path).exists(),
                        format!("schema_path not found for module '{m}': {schema_path}"),
                    );
                }
                None if first_occurrence => {
                    v.check(
                        false,
                        format!(
                            "schema_path is required for first occurrence of module '{m}' in namespace '{}' version '{}'",
                            module.namespace, module.version
                        ),
                    );
                }
                None => {}
            }
            v.check(
                !module.database || self.database.is_some(),
                format!("module '{m}' has database = true but no [database] section is configured"),
            );
            v.check(
                !module.blobstore || self.blobstore.is_some(),
                format!(
                    "module '{m}' has blobstore = true but no [blobstore] section is configured"
                ),
            );
            v.check(
                !module.llm || self.llm.is_some(),
                format!("module '{m}' has llm = true but no [llm] section is configured"),
            );
            if module.mode == ModuleMode::Worker {
                v.check(
                    module.database,
                    format!("module '{m}' has mode = \"worker\" but database is not enabled (job queue requires database)"),
                );
                v.check(
                    module.worker_concurrency > 0,
                    format!("module '{m}' worker_concurrency must be > 0"),
                );
                v.check(
                    module.worker_poll_interval_secs > 0,
                    format!("module '{m}' worker_poll_interval_secs must be > 0"),
                );
                v.check(
                    module.worker_job_timeout_secs > 0,
                    format!("module '{m}' worker_job_timeout_secs must be > 0"),
                );
                v.check(
                    module.worker_max_attempts > 0,
                    format!("module '{m}' worker_max_attempts must be > 0"),
                );
            }
            if let Some(mig_path) = &module.migrations_path {
                v.check(
                    module.database,
                    format!("module '{m}' has migrations_path but database is not enabled"),
                );
                v.check(
                    std::path::Path::new(mig_path).is_dir(),
                    format!("migrations_path for module '{m}' is not a directory: {mig_path}"),
                );
            }
        }

        if let Err(error) = crate::startup_db::StartupDbManifest::build(self) {
            v.check(
                false,
                format!("invalid database startup manifest: {error:#}"),
            );
        }

        v.finish()
    }
}

#[cfg(test)]
mod database_telemetry_tests {
    use serde::Deserialize;

    use super::{DatabaseConfig, EngineConfig};
    use wr_common::config::Validatable as _;

    #[derive(Deserialize)]
    struct TestConfig {
        database: DatabaseConfig,
    }

    #[test]
    fn database_query_text_disclosure_defaults_off_and_can_be_enabled() {
        let default: TestConfig = toml::from_str(
            r#"
            [database]
            url = "postgres://localhost/test"
            "#,
        )
        .expect("default database telemetry config");
        assert!(!default.database.telemetry.include_query_text);

        let enabled: TestConfig = toml::from_str(
            r#"
            [database]
            url = "postgres://localhost/test"

            [database.telemetry]
            include_query_text = true
            "#,
        )
        .expect("enabled database telemetry config");
        assert!(enabled.database.telemetry.include_query_text);
    }

    fn engine_with_database(database_fields: &str) -> EngineConfig {
        toml::from_str(&format!(
            r#"
listen_address = "127.0.0.1:9100"
[node]
proxy_address = "http://127.0.0.1:9001"
control_address = "http://127.0.0.1:9002"
peer_address = "https://127.0.0.1:9443"
[node.tls]
cert_path = "c.crt"
key_path = "c.key"
ca_cert_path = "ca.crt"
[database]
url = "postgres://localhost/test"
{database_fields}
"#
        ))
        .expect("engine config")
    }

    #[test]
    fn database_pool_and_timeouts_must_be_positive() {
        for field in [
            "max_connections = 0",
            "statement_timeout_secs = 0",
            "idle_in_transaction_timeout_secs = 0",
        ] {
            let error = engine_with_database(field)
                .validate()
                .expect_err("zero database setting must fail");
            assert!(error.to_string().contains("must be > 0"), "{error:#}");
        }
    }
}
