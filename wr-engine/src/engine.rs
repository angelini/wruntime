use anyhow::{Context, Result};
use bytes::Bytes;
use deadpool_postgres::Pool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Semaphore};
use tracing::{info, warn, Instrument};
use wasmtime::component::Component;
use wasmtime::{Engine, Trap};
use wasmtime_wasi_http::p2::bindings::ProxyPre;

use crate::registry::{InboundRequest, ModuleRegistry, ModuleTx};
use wr_engine::blobstore::BlobstoreRuntime;
use wr_engine::config::{BlobstoreLimits, EngineConfig, ModuleConfig, ModuleMode, ResourceLimits};
use wr_engine::llm::LlmRuntime;
use wr_engine::pool::{blob_key_prefix, module_schema};
use wr_engine::state::{DbTimeouts, ModuleServices, ModuleState};

struct DatabaseRuntime {
    admin_pool: Arc<Pool>,
    namespace_pools: HashMap<String, Arc<Pool>>,
    recovery_task: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for DatabaseRuntime {
    fn drop(&mut self) {
        if let Some(task) = self.recovery_task.take() {
            task.abort();
        }
    }
}

struct ResolvedServices {
    db_pool: Option<Arc<Pool>>,
    db_schema: Option<Arc<str>>,
    blobstore: Option<Arc<BlobstoreRuntime>>,
    blob_prefix: Option<Arc<str>>,
    blob_limits: BlobstoreLimits,
    llm: Option<Arc<LlmRuntime>>,
}

pub struct EngineRunner {
    engine: Arc<Engine>,
    config: EngineConfig,
    /// Engine-owned administrative pool, namespace pools, and DB background work.
    database: Option<DatabaseRuntime>,
    /// Normalized unique-schema startup work and summed namespace capacities.
    startup_db: wr_engine::startup_db::StartupDbManifest,
    /// Shared S3-compatible blobstore client, present when `[blobstore]` is configured.
    blobstore_client: Option<Arc<BlobstoreRuntime>>,
    /// Shared LLM inference client, present when `[llm]` is configured.
    llm_client: Option<Arc<LlmRuntime>>,
    /// Limits concurrent WASM instantiations to stay within the pooling
    /// allocator's `total_component_instances`.
    instance_semaphore: Arc<Semaphore>,
}

impl EngineRunner {
    pub fn new(config: EngineConfig) -> Result<Self> {
        let startup_db = wr_engine::startup_db::StartupDbManifest::build(&config)?;
        let engine = wr_engine::runtime::build_engine(&config.pool)?;

        let database = config
            .database
            .as_ref()
            .map(|db| {
                Ok::<_, anyhow::Error>(DatabaseRuntime {
                    admin_pool: Arc::new(wr_engine::pool::build_pool(&db.url, db.max_connections)?),
                    namespace_pools: HashMap::new(),
                    recovery_task: None,
                })
            })
            .transpose()?;

        let blobstore_client = config
            .blobstore
            .as_ref()
            .map(BlobstoreRuntime::new)
            .transpose()?
            .map(Arc::new);

        let llm_client = config
            .llm
            .as_ref()
            .map(LlmRuntime::new)
            .transpose()?
            .map(Arc::new);

        let instance_semaphore = Arc::new(Semaphore::new(
            config.pool.total_component_instances as usize,
        ));

        Ok(Self {
            engine: Arc::new(engine),
            config,
            database,
            startup_db,
            blobstore_client,
            llm_client,
            instance_semaphore,
        })
    }

    pub fn admin_pool(&self) -> Option<Arc<Pool>> {
        self.database
            .as_ref()
            .map(|database| database.admin_pool.clone())
    }

    pub async fn run_job_migrations(&self) -> Result<()> {
        if !self.startup_db.has_workers {
            return Ok(());
        }
        let pool = self
            .admin_pool()
            .context("worker mode requires an administrative database pool")?;
        wr_engine::job_migration::run_job_migrations(&pool).await
    }

    pub fn start_recovery_coordinator(&mut self) -> Result<()> {
        if !self.startup_db.has_workers {
            return Ok(());
        }
        let database = self
            .database
            .as_mut()
            .context("worker mode requires a database runtime")?;
        anyhow::ensure!(
            database.recovery_task.is_none(),
            "job recovery coordinator already started"
        );
        database.recovery_task = Some(wr_engine::worker::spawn_recovery_coordinator(
            database.admin_pool.clone(),
        ));
        Ok(())
    }

    /// Spawn a background task that increments the wasmtime epoch at the
    /// configured tick interval, enabling preemption of CPU-bound WASM code.
    pub fn spawn_epoch_ticker(&self) {
        let tick_ms = self.config.pool.epoch_tick_interval_ms;
        let engine = self.engine.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(tick_ms));
            loop {
                interval.tick().await;
                engine.increment_epoch();
            }
        });
    }

    /// Build per-namespace connection pools from manager-provided DB credentials.
    /// Must be called after registration and before loading modules.
    pub fn build_namespace_pools(
        &mut self,
        credentials: &[wr_common::wruntime::NamespaceDbCredential],
    ) -> Result<()> {
        let db_config = match &self.config.database {
            Some(db) => db,
            None => return Ok(()),
        };
        let database = self
            .database
            .as_mut()
            .context("database config has no runtime")?;

        for cred in credentials {
            let Some(max_size) = self
                .startup_db
                .namespace_capacities
                .get(&cred.namespace)
                .copied()
            else {
                continue;
            };
            let pool = wr_engine::pool::build_guest_pool(
                &db_config.url,
                &cred.role,
                &cred.password,
                max_size,
            )?;
            database
                .namespace_pools
                .insert(cred.namespace.clone(), Arc::new(pool));
        }
        Ok(())
    }

    /// Converge target-database roles, schemas, and grants for DB-enabled modules.
    /// Idempotent and safe across concurrent engine startup.
    pub async fn provision_schemas(
        &self,
        credentials: &[wr_common::wruntime::NamespaceDbCredential],
    ) -> Result<()> {
        use std::collections::{BTreeMap, BTreeSet};

        let pool = match self.admin_pool() {
            Some(pool) => pool,
            None => return Ok(()),
        };
        let credentials: HashMap<_, _> = credentials
            .iter()
            .map(|credential| (credential.namespace.as_str(), credential))
            .collect();
        let mut schemas: BTreeMap<&str, BTreeSet<String>> = BTreeMap::new();
        for schema in &self.startup_db.schemas {
            schemas
                .entry(schema.namespace.as_str())
                .or_default()
                .insert(schema.schema.clone());
        }

        let specifications = schemas
            .into_iter()
            .map(|(namespace, schemas)| {
                let credential = credentials.get(namespace).ok_or_else(|| {
                    anyhow::anyhow!("manager omitted database credentials for a namespace")
                })?;
                Ok(wr_engine::provisioning::NamespaceProvisioning {
                    namespace: namespace.to_string(),
                    role: credential.role.clone(),
                    password: credential.password.clone(),
                    schemas: schemas.into_iter().collect(),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        wr_engine::provisioning::provision_namespaces(&pool, &specifications).await
    }

    /// Run database migrations for every module that declares a `migrations_path`.
    /// Uses advisory locks to serialize across engine replicas and restricts
    /// `search_path` so migrations can only touch the module's own schema.
    pub async fn run_migrations(&self) -> Result<()> {
        let pool = match self.admin_pool() {
            Some(pool) => pool,
            None => return Ok(()),
        };
        for schema in &self.startup_db.schemas {
            if let Some(migrations_path) = &schema.migrations_path {
                let migrations_path = migrations_path.to_str().with_context(|| {
                    format!(
                        "migration path for module '{}.{}' is not valid UTF-8",
                        schema.namespace, schema.module
                    )
                })?;
                wr_engine::migration::run_module_migrations(
                    &pool,
                    &schema.schema,
                    migrations_path,
                    &schema.module,
                )
                .await
                .with_context(|| format!("migration failed for module '{}'", schema.module))?;
            }
        }
        Ok(())
    }

    /// Load and spawn a task for every module listed in the config, registering
    /// each module for dispatch by service HTTP requests and worker jobs.
    pub async fn load_modules(
        &self,
        registry: &ModuleRegistry,
        resolved_envs: &HashMap<(String, String), HashMap<String, String>>,
        engine_id: &str,
    ) -> Result<()> {
        for module_config in &self.config.modules {
            let env_vars = resolved_envs
                .get(&(module_config.namespace.clone(), module_config.name.clone()))
                .cloned()
                .unwrap_or_default();
            self.spawn_module(module_config, registry, env_vars, engine_id)
                .await?;
        }
        Ok(())
    }

    /// Resolve database pool, blobstore client, and LLM client for a module
    /// based on its config flags.
    fn resolve_module_services(
        &self,
        module_config: &ModuleConfig,
        module_namespace: &str,
        module_name: &str,
    ) -> ResolvedServices {
        let (db_pool, db_schema) = if module_config.database {
            let schema: Arc<str> = Arc::from(module_schema(module_namespace, module_name));
            let pool = self
                .database
                .as_ref()
                .and_then(|database| database.namespace_pools.get(&module_config.namespace))
                .cloned();
            (pool, Some(schema))
        } else {
            (None, None)
        };
        let (blobstore, blob_prefix) = if module_config.blobstore {
            (
                self.blobstore_client.clone(),
                Some(Arc::<str>::from(blob_key_prefix(module_namespace))),
            )
        } else {
            (None, None)
        };
        let llm = if module_config.llm {
            self.llm_client.clone()
        } else {
            None
        };
        let blob_limits = self
            .config
            .blobstore
            .as_ref()
            .map(|c| BlobstoreLimits {
                max_object_size: c.max_object_size,
                max_list_objects: c.max_list_objects,
            })
            .unwrap_or_default();
        ResolvedServices {
            db_pool,
            db_schema,
            blobstore,
            blob_prefix,
            blob_limits,
            llm,
        }
    }

    fn validate_component_capabilities(
        &self,
        component: &Component,
        module: &ModuleConfig,
    ) -> Result<()> {
        let component_type = component.component_type();
        let imports: Vec<&str> = component_type
            .imports(&self.engine)
            .map(|(name, _)| name)
            .collect();
        for (interface, enabled, capability) in [
            ("wruntime:db/database", module.database, "database"),
            ("wruntime:blobstore/store", module.blobstore, "blobstore"),
            ("wruntime:llm/inference", module.llm, "llm"),
        ] {
            if imports.iter().any(|name| name.starts_with(interface)) && !enabled {
                anyhow::bail!(
                    "module '{}' imports {interface} but {capability} capability is not enabled",
                    module.name
                );
            }
        }
        Ok(())
    }

    /// Load a WASM component, preferring a pre-compiled `.cwasm` artifact when
    /// available and compatible. Falls back to JIT compilation from `.wasm`.
    fn load_component(&self, module_config: &ModuleConfig) -> Result<Component> {
        if let Some(ref cwasm_path) = module_config.cwasm_path {
            let path = std::path::Path::new(cwasm_path);
            if path.exists() {
                // Safety: we only deserialize artifacts produced by our own
                // `precompile_components` step with a matching Engine config.
                match unsafe { Component::deserialize_file(&self.engine, path) } {
                    Ok(component) => {
                        info!(module = %module_config.name, "loaded pre-compiled component");
                        return Ok(component);
                    }
                    Err(e) => {
                        warn!(
                            module = %module_config.name,
                            error = %e,
                            "pre-compiled artifact incompatible, falling back to JIT",
                        );
                    }
                }
            }
        }
        Ok(Component::from_file(
            &self.engine,
            &module_config.wasm_path,
        )?)
    }

    async fn spawn_module(
        &self,
        module_config: &ModuleConfig,
        registry: &ModuleRegistry,
        env_vars: HashMap<String, String>,
        engine_id: &str,
    ) -> Result<()> {
        info!(module = %module_config.name, "loading module");

        let component = self.load_component(module_config)?;
        self.validate_component_capabilities(&component, module_config)?;
        let proxy_uri: hyper::Uri = self.config.node.proxy_address.parse()?;
        let http_pool =
            wr_common::http_pool::HttpClientPool::new(wr_common::http_pool::DEFAULT_POOL_SIZE);
        let module_name: Arc<str> = Arc::from(module_config.name.as_str());
        let module_namespace: Arc<str> = Arc::from(module_config.namespace.as_str());
        let module_version = module_config.version.clone();

        let linker = wr_engine::runtime::configure_linker(&self.engine)?;
        let svc = self.resolve_module_services(module_config, &module_namespace, &module_name);
        if module_config.database && svc.db_pool.is_none() {
            anyhow::bail!(
                "module '{}' database capability has no namespace pool",
                module_config.name
            );
        }
        if module_config.blobstore && svc.blobstore.is_none() {
            anyhow::bail!(
                "module '{}' blobstore capability has no runtime",
                module_config.name
            );
        }
        if module_config.llm && svc.llm.is_none() {
            anyhow::bail!(
                "module '{}' LLM capability has no runtime",
                module_config.name
            );
        }

        let pre = ProxyPre::new(linker.instantiate_pre(&component)?).map_err(|e| {
            let mode_str = if module_config.mode == ModuleMode::Worker {
                "worker"
            } else {
                "service"
            };
            anyhow::anyhow!(
                "module '{}' (mode {mode_str}) must export wasi:http/incoming-handler: {e}",
                module_config.name,
            )
        })?;
        let pre = Arc::new(pre);

        let (tx, rx) = mpsc::channel::<InboundRequest>(module_config.channel_capacity);
        registry
            .register(
                module_namespace.to_string(),
                module_name.to_string(),
                module_version.clone(),
                tx.clone(),
            )
            .await;

        let handler = HandlerContext {
            engine: self.engine.clone(),
            pre,
            instance_semaphore: self.instance_semaphore.clone(),
        };
        let db_timeouts = self
            .config
            .database
            .as_ref()
            .map(|db| DbTimeouts {
                statement_timeout_secs: db.statement_timeout_secs,
                idle_in_transaction_timeout_secs: db.idle_in_transaction_timeout_secs,
            })
            .unwrap_or_default();
        let db_telemetry_include_query_text = self
            .config
            .database
            .as_ref()
            .is_some_and(|database| database.telemetry.include_query_text);
        let module = ModuleContext {
            name: module_name.clone(),
            namespace: module_namespace.clone(),
            proxy_uri: proxy_uri.clone(),
            http_pool: http_pool.clone(),
            db_pool: svc.db_pool.clone(),
            db_schema: svc.db_schema.clone(),
            db_timeouts,
            db_telemetry_include_query_text,
            blobstore: svc.blobstore,
            blob_prefix: svc.blob_prefix,
            blob_limits: svc.blob_limits,
            llm: svc.llm,
            fs: module_config.fs.clone(),
            env_vars: Arc::new(env_vars),
            request_timeout: Duration::from_secs(module_config.request_timeout_secs),
            limits: self.config.limits.clone(),
            max_outbound_body_bytes: self.config.max_outbound_body_bytes,
        };
        tokio::spawn(http_handler_task(handler, module, rx));

        // For worker mode, also spawn the worker pool that pulls jobs from
        // the Postgres queue and dispatches them as HTTP requests.
        if module_config.mode == ModuleMode::Worker {
            let admin_pool = self.admin_pool().expect("worker mode requires database");
            let db_url = self
                .config
                .database
                .as_ref()
                .expect("worker mode requires database")
                .url
                .clone();
            wr_engine::worker::spawn_worker_pool(
                admin_pool,
                wr_engine::worker::WorkerPoolConfig {
                    namespace: module_namespace.to_string(),
                    name: module_name.to_string(),
                    version: module_version.clone(),
                    engine_id: engine_id.to_string(),
                    concurrency: wr_common::lifecycle::WorkerConcurrency::new(
                        module_config.worker_concurrency,
                    )?,
                    poll_interval: wr_common::lifecycle::PositiveDuration::from_secs(
                        module_config.worker_poll_interval_secs,
                        "worker poll interval",
                    )?
                    .get(),
                    job_timeout: wr_common::lifecycle::PositiveDuration::from_secs(
                        module_config.worker_job_timeout_secs,
                        "worker job timeout",
                    )?
                    .get(),
                    database_url: db_url,
                },
                tx,
            );
        }

        info!(module = %module_config.name, "module spawned");
        Ok(())
    }
}

/// Wasmtime engine and pre-instantiated component — shared across requests.
#[derive(Clone)]
struct HandlerContext {
    engine: Arc<Engine>,
    pre: Arc<ProxyPre<ModuleState>>,
    /// Shared semaphore that gates WASM instantiation to prevent pooling
    /// allocator exhaustion.
    instance_semaphore: Arc<Semaphore>,
}

/// Module identity and runtime config — shared across requests.
///
/// Uses `Arc<str>` and `Arc<HashMap>` fields so cloning `ModuleContext` is
/// O(1) reference-count bumps.
#[derive(Clone)]
struct ModuleContext {
    name: Arc<str>,
    namespace: Arc<str>,
    proxy_uri: hyper::Uri,
    /// Pool of HTTP/2 clients for outgoing WASM → proxy requests.
    /// Spreads requests across multiple TCP connections to avoid
    /// single-connection bottlenecks.
    http_pool: wr_common::http_pool::HttpClientPool<http_body_util::Full<bytes::Bytes>>,
    db_pool: Option<Arc<Pool>>,
    db_schema: Option<Arc<str>>,
    db_timeouts: DbTimeouts,
    db_telemetry_include_query_text: bool,
    blobstore: Option<Arc<BlobstoreRuntime>>,
    blob_prefix: Option<Arc<str>>,
    blob_limits: BlobstoreLimits,
    llm: Option<Arc<LlmRuntime>>,
    fs: Option<wr_engine::config::FsMode>,
    env_vars: Arc<HashMap<String, String>>,
    request_timeout: Duration,
    limits: ResourceLimits,
    max_outbound_body_bytes: usize,
}

/// Task that owns the module's channel receiver and spawns a sub-task per
/// inbound request, each with its own `Store` for isolation.
async fn http_handler_task(
    handler: HandlerContext,
    module: ModuleContext,
    mut rx: mpsc::Receiver<InboundRequest>,
) {
    while let Some(inbound) = rx.recv().await {
        let handler = handler.clone();
        let module = module.clone();
        let InboundRequest {
            request,
            response_tx,
            span,
        } = inbound;

        tokio::spawn(
            async move {
                // Worker-dispatched jobs carry x-wr-timeout with the job-level
                // timeout; use it instead of the default request_timeout_secs.
                let timeout = request
                    .headers()
                    .get("x-wr-timeout")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .map(Duration::from_secs)
                    .unwrap_or(module.request_timeout);

                let response = match tokio::time::timeout(
                    timeout,
                    dispatch_request(&handler, &module, request),
                )
                .await
                {
                    Ok(Ok(resp)) => resp,
                    Ok(Err(e)) => {
                        warn!(module = %module.name, error = %e, "inbound request error");
                        http::Response::builder()
                            .status(http::StatusCode::INTERNAL_SERVER_ERROR)
                            .body(Bytes::from("internal error"))
                            .unwrap()
                    }
                    Err(_elapsed) => {
                        warn!(
                            module = %module.name,
                            timeout_secs = timeout.as_secs(),
                            "request timed out"
                        );
                        http::Response::builder()
                            .status(http::StatusCode::GATEWAY_TIMEOUT)
                            .body(Bytes::from("request timed out"))
                            .unwrap()
                    }
                };

                let _ = response_tx.send(response);
            }
            .instrument(span),
        );
    }
}

/// Instantiate the component for one request and drive the WASI HTTP
/// incoming-handler, returning the response to the caller.
///
/// Acquires an instance permit from the shared semaphore before
/// instantiation to prevent pooling allocator exhaustion.
async fn dispatch_request(
    handler: &HandlerContext,
    module: &ModuleContext,
    request: http::Request<Bytes>,
) -> Result<http::Response<Bytes>> {
    // Acquire an instance slot — wait up to 1 s, then reject with 503.
    let _permit =
        match tokio::time::timeout(Duration::from_secs(1), handler.instance_semaphore.acquire())
            .await
        {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => anyhow::bail!("instance semaphore closed"),
            Err(_) => {
                warn!(module = %module.name, "instance pool exhausted, rejecting request");
                return Ok(http::Response::builder()
                    .status(http::StatusCode::SERVICE_UNAVAILABLE)
                    .header("Retry-After", "1")
                    .body(Bytes::from("instance pool exhausted"))
                    .unwrap());
            }
        };

    let state = ModuleState::new(
        module.name.clone(),
        module.namespace.clone(),
        module.proxy_uri.clone(),
        module.http_pool.clone(),
        ModuleServices {
            db_pool: module.db_pool.clone(),
            db_schema: module.db_schema.clone(),
            db_timeouts: module.db_timeouts.clone(),
            db_telemetry_include_query_text: module.db_telemetry_include_query_text,
            blobstore: module.blobstore.clone(),
            blob_prefix: module.blob_prefix.clone(),
            blob_limits: module.blob_limits,
            llm: module.llm.clone(),
            fs: module.fs.clone(),
            env_vars: module.env_vars.clone(),
            active_span: tracing::Span::current(),
            limits: module.limits.clone(),
            max_outbound_body_bytes: module.max_outbound_body_bytes,
        },
    )?;
    match wr_engine::runtime::run_incoming_handler(&handler.engine, &handler.pre, state, request)
        .await
    {
        Ok(resp) => Ok(resp),
        Err(e) => {
            if e.downcast_ref::<Trap>() == Some(&Trap::Interrupt) {
                return Ok(http::Response::builder()
                    .status(http::StatusCode::GATEWAY_TIMEOUT)
                    .body(Bytes::from("execution deadline exceeded"))
                    .unwrap());
            }
            Err(e)
        }
    }
}

/// Send `GET /__health` to a module instance and return whether it responds 2xx.
/// Returns `false` on send failure, timeout, or a non-2xx status.
pub async fn check_module_health(tx: &ModuleTx) -> bool {
    let request = match http::Request::builder()
        .method("GET")
        .uri("http://localhost/__health")
        .body(Bytes::new())
    {
        Ok(r) => r,
        Err(_) => return false,
    };

    let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
    if tx
        .send(InboundRequest {
            request,
            response_tx: resp_tx,
            span: tracing::Span::none(),
        })
        .await
        .is_err()
    {
        return false;
    }

    match tokio::time::timeout(Duration::from_secs(5), resp_rx).await {
        Ok(Ok(resp)) => resp.status().is_success(),
        _ => false,
    }
}
