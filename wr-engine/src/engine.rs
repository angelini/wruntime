use anyhow::{Context, Result};
use bytes::Bytes;
use deadpool_postgres::Pool;
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Full};
use hyper_util::rt::TokioExecutor;
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info, warn, Instrument};
use wasmtime::component::{Component, Linker};
use wasmtime::Store;
use wasmtime::{Config, Engine, InstanceAllocationStrategy, PoolingAllocationConfig};
use wasmtime_wasi_http::p2::{
    bindings::http::types::{ErrorCode, Scheme},
    bindings::ProxyPre,
    body::{HyperIncomingBody, HyperOutgoingBody},
    WasiHttpView as _,
};

use crate::registry::{InboundRequest, ModuleRegistry, ModuleTx};
use wr_engine::blobstore::BlobstoreRuntime;
use wr_engine::config::{EngineConfig, ModuleConfig};
use wr_engine::pool::module_schema;
use wr_engine::state::{ModuleServices, ModuleState};

pub struct EngineRunner {
    engine: Arc<Engine>,
    config: EngineConfig,
    /// Admin pool used only for schema provisioning; shares creds with module pools.
    db_pool: Option<Arc<Pool>>,
    /// One pool per DB-enabled module, keyed by (namespace, name).
    db_pools: HashMap<(String, String), Arc<Pool>>,
    /// Shared S3-compatible blobstore client, present when `[blobstore]` is configured.
    blobstore_client: Option<Arc<BlobstoreRuntime>>,
}

impl EngineRunner {
    pub fn new(config: EngineConfig) -> Result<Self> {
        let mut wt_config = Config::new();
        wt_config.wasm_component_model(true);

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

        let mut db_pools: HashMap<(String, String), Arc<Pool>> = HashMap::new();
        if let Some(db) = &config.database {
            for module in &config.modules {
                if module.database {
                    let pool = wr_engine::pool::build_pool(
                        &db.url,
                        module.db_max_connections.unwrap_or(db.max_connections),
                    )?;
                    db_pools.insert(
                        (module.namespace.clone(), module.name.clone()),
                        Arc::new(pool),
                    );
                }
            }
        }

        let blobstore_client = config
            .blobstore
            .as_ref()
            .map(BlobstoreRuntime::new)
            .transpose()?
            .map(Arc::new);

        Ok(Self {
            engine: Arc::new(engine),
            config,
            db_pool,
            db_pools,
            blobstore_client,
        })
    }

    /// For every DB-enabled module, ensure its Postgres schema exists.
    /// Idempotent — safe to run on every startup.
    pub async fn provision_schemas(&self) -> Result<()> {
        let pool = match &self.db_pool {
            Some(p) => p,
            None => return Ok(()),
        };

        for module in &self.config.modules {
            if !module.database {
                continue;
            }
            let schema = module_schema(&module.namespace, &module.name);
            let client = pool
                .get()
                .await
                .context("failed to get DB connection for schema provisioning")?;
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
            info!(schema, "schema provisioned");
        }

        Ok(())
    }

    /// Load and spawn a task for every module listed in the config, registering
    /// HTTP-handler modules in `registry` so the inbound server can route to them.
    pub async fn load_modules(&self, registry: &ModuleRegistry) -> Result<()> {
        for module_config in &self.config.modules {
            self.spawn_module(module_config, registry).await?;
        }
        Ok(())
    }

    async fn spawn_module(
        &self,
        module_config: &ModuleConfig,
        registry: &ModuleRegistry,
    ) -> Result<()> {
        info!(module = %module_config.name, "loading module");

        let component = Component::from_file(&self.engine, &module_config.wasm_path)?;
        let proxy_uri: hyper::Uri = self.config.node.proxy_address.parse()?;
        let http_client = hyper_util::client::legacy::Client::builder(TokioExecutor::new())
            .http2_only(true)
            .build_http::<http_body_util::Full<bytes::Bytes>>();
        let module_name = module_config.name.clone();
        let module_namespace = module_config.namespace.clone();
        let module_version = module_config.version.clone();

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

        // Try to pre-link as a WASI HTTP Proxy world component first.
        // This succeeds when the component exports `wasi:http/incoming-handler`.
        match ProxyPre::new(linker.instantiate_pre(&component)?) {
            Ok(pre) => {
                let pre = Arc::new(pre);
                let (tx, rx) = mpsc::channel::<InboundRequest>(module_config.channel_capacity);
                registry
                    .register(
                        module_namespace.clone(),
                        module_name.clone(),
                        module_version.clone(),
                        tx,
                    )
                    .await;

                let (db_pool, db_schema) = if module_config.database {
                    let schema = module_schema(&module_namespace, &module_name);
                    let pool = self
                        .db_pools
                        .get(&(module_namespace.clone(), module_name.clone()))
                        .cloned();
                    (pool, Some(schema))
                } else {
                    (None, None)
                };
                let blobstore = if module_config.blobstore {
                    self.blobstore_client.clone()
                } else {
                    None
                };
                let handler = HandlerContext {
                    engine: self.engine.clone(),
                    pre,
                };
                let module = ModuleContext {
                    name: module_name.clone(),
                    namespace: module_namespace.clone(),
                    proxy_uri: proxy_uri.clone(),
                    http_client: http_client.clone(),
                    db_pool,
                    db_schema,
                    blobstore,
                    fs: module_config.fs.clone(),
                    request_timeout: Duration::from_secs(module_config.request_timeout_secs),
                };
                tokio::spawn(http_handler_task(handler, module, rx));
            }
            Err(_) => {
                // Fall back: spawn as a long-running task that calls `run`.
                let (db_pool, db_schema) = if module_config.database {
                    let schema = module_schema(&module_namespace, &module_name);
                    let pool = self
                        .db_pools
                        .get(&(module_namespace.clone(), module_name.clone()))
                        .cloned();
                    (pool, Some(schema))
                } else {
                    (None, None)
                };
                let blobstore = if module_config.blobstore {
                    self.blobstore_client.clone()
                } else {
                    None
                };
                let state = ModuleState::new(
                    module_name.clone(),
                    module_namespace.clone(),
                    proxy_uri,
                    http_client,
                    ModuleServices {
                        db_pool,
                        db_schema,
                        blobstore,
                        fs: module_config.fs.clone(),
                        active_span: tracing::Span::current(),
                    },
                )?;
                let mut store = Store::new(&self.engine, state);
                let instance = linker.instantiate_async(&mut store, &component).await?;

                tokio::spawn(async move {
                    match instance.get_func(&mut store, "run") {
                        Some(func) => {
                            if let Err(e) = func.call_async(&mut store, &[], &mut []).await {
                                error!(module = %module_name, error = %e, "module exited with error");
                            } else {
                                info!(module = %module_name, "module exited cleanly");
                            }
                        }
                        None => {
                            info!(module = %module_name, "no `run` export, module is idle");
                            std::future::pending::<()>().await;
                        }
                    }
                });
            }
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
}

/// Module identity and runtime config — shared across requests.
#[derive(Clone)]
struct ModuleContext {
    name: String,
    namespace: String,
    proxy_uri: hyper::Uri,
    /// Pooled HTTP client for outgoing WASM → proxy requests.
    /// `Client` is `Clone` (internally Arc-backed), so this is cheap to clone.
    http_client: hyper_util::client::legacy::Client<
        hyper_util::client::legacy::connect::HttpConnector,
        http_body_util::Full<bytes::Bytes>,
    >,
    db_pool: Option<Arc<Pool>>,
    db_schema: Option<String>,
    blobstore: Option<Arc<BlobstoreRuntime>>,
    fs: Option<wr_engine::config::FsMode>,
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
                let timeout = module.request_timeout;

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
async fn dispatch_request(
    handler: &HandlerContext,
    module: &ModuleContext,
    request: http::Request<Bytes>,
) -> Result<http::Response<Bytes>> {
    let state = ModuleState::new(
        module.name.clone(),
        module.namespace.clone(),
        module.proxy_uri.clone(),
        module.http_client.clone(),
        ModuleServices {
            db_pool: module.db_pool.clone(),
            db_schema: module.db_schema.clone(),
            blobstore: module.blobstore.clone(),
            fs: module.fs.clone(),
            active_span: tracing::Span::current(),
        },
    )?;
    let mut store = Store::new(&handler.engine, state);
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
    proxy
        .wasi_http_incoming_handler()
        .call_handle(&mut store, req_resource, out_resource)
        .await?;

    // ── Collect and return the response ──────────────────────────────────
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
