use anyhow::{Context, Result};
use bytes::Bytes;
use deadpool_postgres::Pool;
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Full};
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Semaphore};
use tracing::{info, warn, Instrument};
use wasmtime::component::{Component, Linker};
use wasmtime::Store;
use wasmtime::{Config, Engine, InstanceAllocationStrategy, PoolingAllocationConfig, Trap};
use wasmtime_wasi_http::p2::{
    bindings::http::types::{ErrorCode, Scheme},
    bindings::ProxyPre,
    body::{HyperIncomingBody, HyperOutgoingBody},
    WasiHttpView as _,
};

use crate::registry::{InboundRequest, ModuleRegistry, ModuleTx};
use wr_engine::blobstore::BlobstoreRuntime;
use wr_engine::config::{EngineConfig, ModuleConfig, ModuleMode};
use wr_engine::llm::LlmRuntime;
use wr_engine::pool::{blob_key_prefix, module_schema};
use wr_engine::state::{DbTimeouts, ModuleServices, ModuleState};

struct ResolvedServices {
    db_pool: Option<Arc<Pool>>,
    db_schema: Option<Arc<str>>,
    blobstore: Option<Arc<BlobstoreRuntime>>,
    blob_prefix: Option<Arc<str>>,
    llm: Option<Arc<LlmRuntime>>,
}

pub struct EngineRunner {
    engine: Arc<Engine>,
    config: EngineConfig,
    /// Admin pool used only for schema provisioning and migrations.
    db_pool: Option<Arc<Pool>>,
    /// One pool per namespace with DB-enabled modules.
    db_pools: HashMap<String, Arc<Pool>>,
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
        let mut wt_config = Config::new();
        wt_config.wasm_component_model(true);
        wt_config.epoch_interruption(true);
        wt_config.memory_reservation(4 * (1 << 30));
        wt_config.memory_guard_size(32 * (1 << 20));
        wt_config.memory_init_cow(true);

        let mut pool = PoolingAllocationConfig::new();
        pool.total_component_instances(config.pool.total_component_instances);
        pool.max_memory_size(config.pool.max_memory_size);
        pool.total_memories(config.pool.total_component_instances);
        pool.total_tables(config.pool.total_component_instances);
        wt_config.allocation_strategy(InstanceAllocationStrategy::Pooling(pool));

        let engine = Engine::new(&wt_config)?;

        let db_pool = config
            .database
            .as_ref()
            .map(|db| wr_engine::pool::build_pool(&db.url, db.max_connections))
            .transpose()?
            .map(Arc::new);

        // Guest pools are built later from manager-provided credentials
        // via build_namespace_pools().
        let db_pools: HashMap<String, Arc<Pool>> = HashMap::new();

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
            db_pool,
            db_pools,
            blobstore_client,
            llm_client,
            instance_semaphore,
        })
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
        let db = match &self.config.database {
            Some(db) => db,
            None => return Ok(()),
        };

        // Sum max_connections per namespace from all DB-enabled modules
        let mut ns_max_conns: HashMap<String, usize> = HashMap::new();
        for module in &self.config.modules {
            if module.database {
                *ns_max_conns.entry(module.namespace.clone()).or_default() +=
                    module.db_max_connections.unwrap_or(db.max_connections);
            }
        }

        for cred in credentials {
            let max_size = ns_max_conns
                .get(&cred.namespace)
                .copied()
                .unwrap_or(db.max_connections);
            let pool =
                wr_engine::pool::build_guest_pool(&db.url, &cred.role, &cred.password, max_size)?;
            self.db_pools.insert(cred.namespace.clone(), Arc::new(pool));
        }
        Ok(())
    }

    /// For every DB-enabled module, ensure its Postgres schema and per-namespace
    /// role exist. Creates roles, schemas, and grants access.
    /// Idempotent — safe to run on every startup.
    pub async fn provision_schemas(
        &self,
        credentials: &[wr_common::wruntime::NamespaceDbCredential],
    ) -> Result<()> {
        let pool = match &self.db_pool {
            Some(p) => p,
            None => return Ok(()),
        };

        let client = pool
            .get()
            .await
            .context("failed to get DB connection for schema provisioning")?;

        // Create per-namespace roles
        for cred in credentials {
            client
                .batch_execute(&format!(
                    "DO $$ BEGIN \
                       IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{role}') THEN \
                         CREATE ROLE \"{role}\" LOGIN PASSWORD '{password}'; \
                       END IF; \
                     END $$; \
                     ALTER ROLE \"{role}\" PASSWORD '{password}';",
                    role = cred.role,
                    password = cred.password,
                ))
                .await
                .with_context(|| format!("failed to provision role '{}'", cred.role))?;
            info!(role = %cred.role, namespace = %cred.namespace, "db role provisioned");
        }

        // Build a lookup from namespace → role for grant statements
        let ns_roles: HashMap<&str, &str> = credentials
            .iter()
            .map(|c| (c.namespace.as_str(), c.role.as_str()))
            .collect();

        for module in &self.config.modules {
            if !module.database {
                continue;
            }
            let schema = module_schema(&module.namespace, &module.name);
            let result = client
                .execute(
                    &format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\""),
                    &[] as &[&(dyn tokio_postgres::types::ToSql + Sync)],
                )
                .await;
            match result {
                Ok(_) => {}
                Err(e) if e.code() == Some(&tokio_postgres::error::SqlState::DUPLICATE_SCHEMA) => {
                    // Race condition: another engine instance created the schema concurrently.
                }
                Err(e) => {
                    return Err(e)
                        .with_context(|| format!("failed to provision schema '{schema}'"));
                }
            }

            // Grant the namespace role full access to this module's schema
            if let Some(role) = ns_roles.get(module.namespace.as_str()) {
                client
                    .batch_execute(&format!(
                        "GRANT ALL ON SCHEMA \"{schema}\" TO \"{role}\"; \
                         GRANT ALL ON ALL TABLES IN SCHEMA \"{schema}\" TO \"{role}\"; \
                         GRANT ALL ON ALL SEQUENCES IN SCHEMA \"{schema}\" TO \"{role}\"; \
                         GRANT ALL ON ALL FUNCTIONS IN SCHEMA \"{schema}\" TO \"{role}\"; \
                         ALTER DEFAULT PRIVILEGES IN SCHEMA \"{schema}\" GRANT ALL ON TABLES TO \"{role}\"; \
                         ALTER DEFAULT PRIVILEGES IN SCHEMA \"{schema}\" GRANT ALL ON SEQUENCES TO \"{role}\"; \
                         ALTER DEFAULT PRIVILEGES IN SCHEMA \"{schema}\" GRANT ALL ON FUNCTIONS TO \"{role}\";"
                    ))
                    .await
                    .with_context(|| {
                        format!("failed to grant schema '{schema}' access to role '{role}'")
                    })?;
            }

            info!(schema, "schema provisioned");
        }

        Ok(())
    }

    /// Run database migrations for every module that declares a `migrations_path`.
    /// Uses advisory locks to serialize across engine replicas and restricts
    /// `search_path` so migrations can only touch the module's own schema.
    pub async fn run_migrations(&self) -> Result<()> {
        let pool = match &self.db_pool {
            Some(p) => p,
            None => return Ok(()),
        };
        for module in &self.config.modules {
            if let Some(mig_path) = &module.migrations_path {
                let schema = module_schema(&module.namespace, &module.name);
                wr_engine::migration::run_module_migrations(pool, &schema, mig_path, &module.name)
                    .await
                    .with_context(|| format!("migration failed for module '{}'", module.name))?;
            }
        }
        Ok(())
    }

    /// Load and spawn a task for every module listed in the config, registering
    /// HTTP-handler modules in `registry` so the inbound server can route to them.
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

    fn configure_linker(&self) -> Result<Linker<ModuleState>> {
        let mut linker: Linker<ModuleState> = Linker::new(&self.engine);
        wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
        wasmtime_wasi_http::p2::add_only_http_to_linker_async(&mut linker)?;
        wr_engine::db::wruntime::db::database::add_to_linker::<
            ModuleState,
            wasmtime::component::HasSelf<ModuleState>,
        >(&mut linker, |s| s)?;
        wr_engine::tracing::add_to_linker::<ModuleState, wasmtime::component::HasSelf<ModuleState>>(
            &mut linker,
            |s| s,
        )?;
        wr_engine::blobstore::add_to_linker::<
            ModuleState,
            wasmtime::component::HasSelf<ModuleState>,
        >(&mut linker, |s| s)?;
        wr_engine::llm::add_to_linker::<ModuleState, wasmtime::component::HasSelf<ModuleState>>(
            &mut linker,
            |s| s,
        )?;
        Ok(linker)
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
            let pool = self.db_pools.get(&module_config.namespace).cloned();
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
        ResolvedServices {
            db_pool,
            db_schema,
            blobstore,
            blob_prefix,
            llm,
        }
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
        let proxy_uri: hyper::Uri = self.config.node.proxy_address.parse()?;
        let http_pool =
            wr_common::http_pool::HttpClientPool::new(wr_common::http_pool::DEFAULT_POOL_SIZE);
        let module_name: Arc<str> = Arc::from(module_config.name.as_str());
        let module_namespace: Arc<str> = Arc::from(module_config.namespace.as_str());
        let module_version = module_config.version.clone();

        let linker = self.configure_linker()?;
        let svc = self.resolve_module_services(module_config, &module_namespace, &module_name);

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
        let module = ModuleContext {
            name: module_name.clone(),
            namespace: module_namespace.clone(),
            proxy_uri: proxy_uri.clone(),
            http_pool: http_pool.clone(),
            db_pool: svc.db_pool.clone(),
            db_schema: svc.db_schema.clone(),
            db_timeouts,
            blobstore: svc.blobstore,
            blob_prefix: svc.blob_prefix,
            llm: svc.llm,
            fs: module_config.fs.clone(),
            env_vars: Arc::new(env_vars),
            request_timeout: Duration::from_secs(module_config.request_timeout_secs),
        };
        tokio::spawn(http_handler_task(handler, module, rx));

        // For worker mode, also spawn the worker pool that pulls jobs from
        // the Postgres queue and dispatches them as HTTP requests.
        if module_config.mode == ModuleMode::Worker {
            let admin_pool = self.db_pool.clone().expect("worker mode requires database");
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
                    concurrency: module_config.worker_concurrency,
                    poll_interval: Duration::from_secs(module_config.worker_poll_interval_secs),
                    job_timeout: Duration::from_secs(module_config.worker_job_timeout_secs),
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
/// All `String` fields that were cloned per-request are now `Arc<str>` or
/// `Arc<HashMap>` so that cloning `ModuleContext` is O(1) reference-count
/// bumps instead of O(n) heap copies.
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
    blobstore: Option<Arc<BlobstoreRuntime>>,
    blob_prefix: Option<Arc<str>>,
    llm: Option<Arc<LlmRuntime>>,
    fs: Option<wr_engine::config::FsMode>,
    env_vars: Arc<HashMap<String, String>>,
    request_timeout: Duration,
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
            blobstore: module.blobstore.clone(),
            blob_prefix: module.blob_prefix.clone(),
            llm: module.llm.clone(),
            fs: module.fs.clone(),
            env_vars: module.env_vars.clone(),
            active_span: tracing::Span::current(),
        },
    )?;
    let mut store = Store::new(&handler.engine, state);
    // Yield back to tokio on every epoch tick so CPU-bound WASM doesn't
    // block the async runtime. The outer tokio::time::timeout still
    // enforces the total request deadline.
    store.set_epoch_deadline(1);
    store.epoch_deadline_async_yield_and_update(1);
    let proxy = handler.pre.instantiate_async(&mut store).await?;

    // ── Build the incoming request resource ──────────────────────────────
    let (req_parts, req_body) = request.into_parts();

    // Wrap the buffered Bytes as a HyperIncomingBody
    // (UnsyncBoxBody<Bytes, ErrorCode>).
    let hyper_body: HyperIncomingBody = UnsyncBoxBody::new(
        Full::new(req_body).map_err(|_: Infallible| ErrorCode::InternalError(None)),
    );
    let hyper_req = hyper::Request::from_parts(req_parts, hyper_body);
    let req_resource = store
        .data_mut()
        .http()
        .new_incoming_request(Scheme::Http, hyper_req)?;

    // ── Build the response outparam resource ─────────────────────────────
    let (resp_tx, resp_rx) =
        tokio::sync::oneshot::channel::<Result<hyper::Response<HyperOutgoingBody>, ErrorCode>>();
    let out_resource = store.data_mut().http().new_response_outparam(resp_tx)?;

    // ── Call the WASM incoming handler ───────────────────────────────────
    if let Err(e) = proxy
        .wasi_http_incoming_handler()
        .call_handle(&mut store, req_resource, out_resource)
        .await
    {
        if e.downcast_ref::<Trap>() == Some(&Trap::Interrupt) {
            return Ok(http::Response::builder()
                .status(http::StatusCode::GATEWAY_TIMEOUT)
                .body(Bytes::from("execution deadline exceeded"))
                .unwrap());
        }
        return Err(e.into());
    }

    // ── Collect and return the response ──────────────────────────────────
    // The `_permit` is held until this point, released on drop.
    match resp_rx.await {
        Ok(Ok(wasm_resp)) => {
            let (rp, rb) = wasm_resp.into_parts();
            let bytes = rb
                .collect()
                .await
                .map_err(|e| anyhow::anyhow!("collecting WASM response body: {e:?}"))?
                .to_bytes();
            Ok(http::Response::from_parts(rp, bytes))
        }
        Ok(Err(e)) => anyhow::bail!("WASM handler returned ErrorCode: {e:?}"),
        Err(_) => anyhow::bail!("WASM handler dropped the response outparam"),
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
