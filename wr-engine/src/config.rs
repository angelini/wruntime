use std::collections::HashMap;

use anyhow::Result;
use serde::Deserialize;
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
    #[serde(rename = "module", default)]
    pub modules: Vec<ModuleConfig>,
    /// Optional PostgreSQL connection pool shared across DB-enabled modules.
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
    /// modules that never yield to the host. Defaults to 100.
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

#[derive(Deserialize, Clone)]
pub struct LlmConfig {
    /// LLM provider. Currently only "anthropic" is supported.
    pub provider: String,
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

/// An environment variable value: either a plain string or a secret reference.
#[derive(Deserialize, Clone)]
#[serde(untagged)]
pub enum EnvValue {
    /// Inline plaintext value, e.g. `LOG_LEVEL = "debug"`
    Plain(String),
    /// Secret fetched from the manager, e.g. `API_KEY = { secret = true }`
    Secret { secret: bool },
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
    /// Optional — modules may omit this if they don't expose a schema.
    #[serde(default)]
    pub schema_path: Option<String>,
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
    pub worker_max_attempts: i32,
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

fn default_worker_max_attempts() -> i32 {
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
            !self.node.proxy_address.is_empty(),
            "node.proxy_address is required",
        );
        v.check(
            !self.node.control_address.is_empty(),
            "node.control_address is required",
        );

        for module in &self.modules {
            let m = &module.name;
            v.check(!module.name.is_empty(), "module.name is required");
            v.check(!module.namespace.is_empty(), "module.namespace is required");
            v.check(!module.version.is_empty(), "module.version is required");
            v.check(
                std::path::Path::new(&module.wasm_path).exists(),
                format!("wasm_path not found for module '{m}': {}", module.wasm_path),
            );
            if let Some(ref schema_path) = module.schema_path {
                v.check(
                    std::path::Path::new(schema_path).exists(),
                    format!("schema_path not found for module '{m}': {schema_path}"),
                );
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

        v.finish()
    }
}
