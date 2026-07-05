use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use crate::blobstore::{BlobError, BlobstoreRuntime};
use crate::config::{BlobstoreLimits, FsMode, ResourceLimits};
use crate::db::wruntime::db::database::DbError;
use crate::llm::{LlmError, LlmRuntime};
use deadpool_postgres::Pool;
use http_body_util::{BodyExt, Full};
use hyper::header::{HeaderName, HeaderValue};
use tempfile::TempDir;
use wasmtime::component::ResourceTable;
use wasmtime_wasi::{DirPerms, FilePerms, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};
use wasmtime_wasi_http::{
    p2::{
        bindings::http::types::ErrorCode,
        body::{HyperIncomingBody, HyperOutgoingBody},
        hyper_request_error,
        types::{HostFutureIncomingResponse, IncomingResponse, OutgoingRequestConfig},
        HttpResult, WasiHttpCtxView, WasiHttpHooks, WasiHttpView,
    },
    WasiHttpCtx,
};
use wr_common::http_headers::{WR_DESTINATION, WR_SOURCE, WR_SOURCE_NS};
use wr_common::http_pool::HttpClientPool;

/// Hooks that intercept every outbound HTTP request from a WASM module.
///
/// - Preserves the original destination in `x-wr-destination` so
///   wr-proxy can route the request to the correct engine.
/// - Tags the request with `x-wr-source` for metrics attribution.
/// - Rewrites the URI to point at wr-proxy.
/// - Uses a shared pool of HTTP/2 clients to spread outbound requests
///   across multiple TCP connections, avoiding single-connection
///   bottlenecks (frame contention, TCP HoL blocking).
struct ModuleHttpHooks {
    proxy_uri: hyper::Uri,
    module_name: Arc<str>,
    module_namespace: Arc<str>,
    /// Pool of HTTP/2 clients — round-robin across multiple connections.
    http_pool: HttpClientPool<Full<bytes::Bytes>>,
    /// When set, outbound requests are parented to this span instead of
    /// starting a new trace. Modules set this via `start-root`.
    outbound_parent: Arc<std::sync::Mutex<Option<tracing::Span>>>,
    /// Max buffered outbound request body size in bytes; larger bodies are
    /// rejected with `ErrorCode::HttpRequestBodySize`.
    max_outbound_body_bytes: usize,
}

impl WasiHttpHooks for ModuleHttpHooks {
    fn send_request(
        &mut self,
        mut request: hyper::Request<HyperOutgoingBody>,
        config: OutgoingRequestConfig,
    ) -> HttpResult<HostFutureIncomingResponse> {
        let original_uri = request.uri().to_string();

        // If the guest set an outbound parent (via `start-root`), parent to
        // that span so all outbound calls share one trace. Otherwise start a
        // new root trace per outbound call.
        let parent_lock = self.outbound_parent.lock().unwrap();
        let parent = parent_lock.clone().unwrap_or_else(tracing::Span::none);
        drop(parent_lock);
        let outbound_span = tracing::info_span!(
            parent: &parent,
            "engine.outbound_request",
            otel.name = format!("{} {}", request.method(), &original_uri),
            wr.source = %self.module_name,
            wr.destination = %original_uri,
            http.request.method = %request.method(),
            url.full = %original_uri,
            http.response.status_code = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );

        request.headers_mut().insert(
            HeaderName::from_static(WR_DESTINATION),
            HeaderValue::from_str(&original_uri).map_err(|_| ErrorCode::InternalError(None))?,
        );
        request.headers_mut().insert(
            HeaderName::from_static(WR_SOURCE),
            HeaderValue::from_str(&self.module_name).map_err(|_| ErrorCode::InternalError(None))?,
        );
        request.headers_mut().insert(
            HeaderName::from_static(WR_SOURCE_NS),
            HeaderValue::from_str(&self.module_namespace)
                .map_err(|_| ErrorCode::InternalError(None))?,
        );

        // Inject trace context so downstream services (proxy, destination engine)
        // join this trace instead of starting a new one.
        {
            let _guard = outbound_span.enter();
            wr_common::telemetry::inject_context(request.headers_mut());
        }

        // Preserve the original path+query; only replace scheme and authority.
        let path_and_query = request
            .uri()
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or("/");
        let scheme = self.proxy_uri.scheme_str().unwrap_or("http");
        let authority = self.proxy_uri.authority().map(|a| a.as_str()).unwrap_or("");
        let new_uri: hyper::Uri = format!("{scheme}://{authority}{path_and_query}")
            .parse()
            .map_err(|_| ErrorCode::InternalError(None))?;
        tracing::debug!(
            original = %original_uri,
            proxy_uri = %self.proxy_uri,
            rewritten = %new_uri,
            "outgoing request rewrite"
        );
        *request.uri_mut() = new_uri;

        let client = self.http_pool.get().clone();
        let between_bytes_timeout = config.between_bytes_timeout;
        let max_outbound_body_bytes = self.max_outbound_body_bytes;

        let handle = wasmtime_wasi::runtime::spawn(async move {
            Ok(async move {
                // Buffer the outgoing body up to `max_outbound_body_bytes`, aborting as
                // soon as the running total exceeds the cap so an oversized body is never
                // fully materialized. The pooled client requires a single concrete
                // Send + 'static body type (Full<Bytes>), so the under-cap path is buffered.
                let (parts, mut body) = request.into_parts();

                // Fast pre-check: reject without reading a frame if the body advertises an
                // upper bound over the cap. `upper` is usually absent for guest bodies, so
                // the running-total guard below is the authoritative check.
                if let Some(upper) = http_body::Body::size_hint(&body).upper() {
                    if upper > max_outbound_body_bytes as u64 {
                        return Err(ErrorCode::HttpRequestBodySize(Some(upper)));
                    }
                }

                let mut collected = bytes::BytesMut::new();
                while let Some(frame) = body.frame().await {
                    let frame = frame?;
                    if let Ok(data) = frame.into_data() {
                        if collected.len() + data.len() > max_outbound_body_bytes {
                            return Err(ErrorCode::HttpRequestBodySize(Some(
                                (collected.len() + data.len()) as u64,
                            )));
                        }
                        collected.extend_from_slice(data.as_ref());
                    }
                }
                let buffered = hyper::Request::from_parts(parts, Full::new(collected.freeze()));

                let resp = client.request(buffered).await.map_err(|e| {
                    tracing::warn!(error = ?e, "outgoing http request failed");
                    outbound_span.record("otel.status_code", "ERROR");
                    if e.is_connect() {
                        ErrorCode::ConnectionRefused
                    } else {
                        ErrorCode::InternalError(Some(e.to_string()))
                    }
                })?;

                let status = resp.status().as_u16();
                outbound_span.record("http.response.status_code", status);
                if status >= 400 {
                    outbound_span.record("otel.status_code", "ERROR");
                } else {
                    outbound_span.record("otel.status_code", "OK");
                }

                let (resp_parts, resp_body) = resp.into_parts();
                let incoming_body: HyperIncomingBody =
                    resp_body.map_err(hyper_request_error).boxed_unsync();

                Ok::<IncomingResponse, ErrorCode>(IncomingResponse {
                    resp: hyper::Response::from_parts(resp_parts, incoming_body),
                    worker: None,
                    between_bytes_timeout,
                })
            }
            .await)
        });

        Ok(HostFutureIncomingResponse::pending(handle))
    }
}

/// Postgres timeout configuration applied to every guest connection.
#[derive(Clone, Debug)]
pub struct DbTimeouts {
    /// `SET statement_timeout` value in seconds.
    pub statement_timeout_secs: u32,
    /// `SET idle_in_transaction_session_timeout` value in seconds.
    pub idle_in_transaction_timeout_secs: u32,
}

impl Default for DbTimeouts {
    fn default() -> Self {
        Self {
            statement_timeout_secs: 30,
            idle_in_transaction_timeout_secs: 60,
        }
    }
}

/// The four guest-creatable host resource kinds subject to per-store caps.
#[derive(Clone, Copy, Debug)]
pub enum ResourceKind {
    Span,
    DbTransaction,
    DbCursor,
    LlmStream,
}

/// Decrements the live-count for one resource kind when dropped.
///
/// Stored as a field inside the resource-state struct (`SpanState`, `TxState`,
/// `CursorState`, `CompletionStreamState`) so the decrement is tied to the
/// resource's lifetime: it fires exactly when the state is removed from the
/// `ResourceTable` (via `delete`), and — because a failed `ResourceTable::push`
/// consumes and drops the never-inserted state — a failed push self-corrects the
/// count. Never construct one except via `ResourceAccounting::try_track`.
pub struct CounterGuard {
    counter: Arc<AtomicU32>,
}

impl Drop for CounterGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Per-store live-resource accounting: one running count per resource kind plus
/// the configured caps. Cloneable (shares the atomics) and relocatable as a unit
/// into the capability structs.
///
/// Invariant: the live count is incremented only via `try_track` (which returns a
/// `CounterGuard`) and decremented only when that guard drops — i.e. after a
/// successful `ResourceTable::delete`, or when a failed `push` drops the state.
/// A failed push nets to zero; a failed/early-return delete never decrements.
/// Host calls on a store are serialized (`&mut self`), so the load-then-add in
/// `try_track` cannot race.
#[derive(Clone)]
pub struct ResourceAccounting {
    spans: Arc<AtomicU32>,
    db_transactions: Arc<AtomicU32>,
    db_cursors: Arc<AtomicU32>,
    llm_streams: Arc<AtomicU32>,
    limits: ResourceLimits,
}

impl ResourceAccounting {
    pub fn new(limits: ResourceLimits) -> Self {
        Self {
            spans: Arc::new(AtomicU32::new(0)),
            db_transactions: Arc::new(AtomicU32::new(0)),
            db_cursors: Arc::new(AtomicU32::new(0)),
            llm_streams: Arc::new(AtomicU32::new(0)),
            limits,
        }
    }

    fn slot(&self, kind: ResourceKind) -> (&Arc<AtomicU32>, u32) {
        match kind {
            ResourceKind::Span => (&self.spans, self.limits.max_spans),
            ResourceKind::DbTransaction => (&self.db_transactions, self.limits.max_db_transactions),
            ResourceKind::DbCursor => (&self.db_cursors, self.limits.max_db_cursors),
            ResourceKind::LlmStream => (&self.llm_streams, self.limits.max_llm_streams),
        }
    }

    /// Reserve one live slot for `kind`. On success increments the live-count and
    /// returns a `CounterGuard` the caller MUST move into the resource-state
    /// struct it is about to `ResourceTable::push`. Returns `None` when already at
    /// cap (no increment performed); the caller maps that to a trap (spans) or an
    /// error variant (DB/LLM).
    pub fn try_track(&self, kind: ResourceKind) -> Option<CounterGuard> {
        let (counter, cap) = self.slot(kind);
        if counter.load(Ordering::Relaxed) >= cap {
            return None;
        }
        counter.fetch_add(1, Ordering::Relaxed);
        Some(CounterGuard {
            counter: counter.clone(),
        })
    }
}

/// Optional services and capabilities for a module.
/// All fields default to `None`/no-op, so tests can simply use `Default::default()`.
pub struct ModuleServices {
    /// Shared connection pool, present when the module has DB access enabled.
    pub db_pool: Option<Arc<Pool>>,
    /// Postgres schema name for this module (`wr__{namespace}__{name}`).
    /// Set when DB access is enabled; used to scope all queries to the module's schema.
    pub db_schema: Option<Arc<str>>,
    /// Timeout configuration for guest DB connections.
    pub db_timeouts: DbTimeouts,
    /// Shared S3-compatible blobstore client, present when the module has blobstore access enabled.
    pub blobstore: Option<Arc<BlobstoreRuntime>>,
    /// S3 key prefix for namespace isolation (e.g. `wr/ecommerce/`).
    /// Set when blobstore access is enabled; transparently prepended to all object keys.
    pub blob_prefix: Option<Arc<str>>,
    /// Host-enforced blobstore size/list ceilings. Defaults in tests.
    pub blob_limits: BlobstoreLimits,
    /// Shared LLM inference client, present when the module has LLM access enabled.
    pub llm: Option<Arc<LlmRuntime>>,
    /// WASI filesystem mode (e.g. `FsMode::Tempdir`).
    pub fs: Option<FsMode>,
    /// Resolved environment variables for this module (plain + decrypted secrets).
    pub env_vars: Arc<std::collections::HashMap<String, String>>,
    /// The `engine.dispatch` span for the current request.
    /// Captured at `ModuleState` construction time so host functions can create
    /// child spans even when wasmtime's synchronous call stack is outside the
    /// async instrumented context.
    pub active_span: tracing::Span,
    /// Per-store resource caps. From `EngineConfig.limits`; defaults in tests.
    pub limits: ResourceLimits,
    /// Max outbound HTTP request body size in bytes. From
    /// `EngineConfig.max_outbound_body_bytes`; default in tests.
    pub max_outbound_body_bytes: usize,
}

impl Default for ModuleServices {
    fn default() -> Self {
        Self {
            db_pool: None,
            db_schema: None,
            db_timeouts: DbTimeouts::default(),
            blobstore: None,
            blob_prefix: None,
            blob_limits: BlobstoreLimits::default(),
            llm: None,
            fs: None,
            env_vars: Arc::new(std::collections::HashMap::new()),
            active_span: tracing::Span::none(),
            limits: ResourceLimits::default(),
            max_outbound_body_bytes: 16 * 1024 * 1024,
        }
    }
}

/// DB capability: pool, schema, timeouts, and transaction/cursor accounting.
pub(crate) struct DbCapability {
    pub(crate) pool: Arc<Pool>,
    pub(crate) schema: Option<Arc<str>>,
    pub(crate) timeouts: DbTimeouts,
    pub(crate) accounting: ResourceAccounting,
}

/// Blobstore capability: S3 runtime + namespace key prefix + host-enforced size/list limits.
pub(crate) struct BlobstoreCapability {
    pub(crate) runtime: Arc<BlobstoreRuntime>,
    pub(crate) prefix: Option<Arc<str>>,
    pub(crate) limits: BlobstoreLimits,
}

/// LLM capability: inference runtime and stream accounting.
pub(crate) struct LlmCapability {
    pub(crate) runtime: Arc<LlmRuntime>,
    pub(crate) accounting: ResourceAccounting,
}

/// Tracing capability (always present): request-level span, guest span stack,
/// shared outbound-parent handle, and live-span accounting.
pub(crate) struct TracingCapability {
    pub(crate) active_span: tracing::Span,
    pub(crate) span_stack: Vec<tracing::Span>,
    pub(crate) outbound_parent: Arc<std::sync::Mutex<Option<tracing::Span>>>,
    pub(crate) accounting: ResourceAccounting,
}

/// Filesystem capability: holds the ephemeral tempdir alive for the store's lifetime.
pub(crate) struct FsCapability {
    _root: Option<TempDir>,
}

struct ModuleCapabilities {
    db: Option<DbCapability>,
    blobstore: Option<BlobstoreCapability>,
    llm: Option<LlmCapability>,
    tracing: TracingCapability,
    _fs: FsCapability,
}

pub struct ModuleState {
    wasi: WasiCtx,
    http: WasiHttpCtx,
    table: ResourceTable,
    hooks: ModuleHttpHooks,
    capabilities: ModuleCapabilities,
}

impl ModuleState {
    pub fn new(
        module_name: Arc<str>,
        module_namespace: Arc<str>,
        proxy_uri: hyper::Uri,
        http_pool: HttpClientPool<Full<bytes::Bytes>>,
        services: ModuleServices,
    ) -> anyhow::Result<Self> {
        let mut builder = WasiCtxBuilder::new();
        builder.inherit_stdio();
        for (key, value) in services.env_vars.iter() {
            builder.env(key, value);
        }
        let fs_root = match services.fs.as_ref() {
            Some(FsMode::Tempdir) => {
                let dir = tempfile::tempdir()?;
                builder.preopened_dir(dir.path(), "/", DirPerms::all(), FilePerms::all())?;
                Some(dir)
            }
            None => None,
        };
        let outbound_parent = Arc::new(std::sync::Mutex::new(None));
        let accounting = ResourceAccounting::new(services.limits);
        let db = services.db_pool.map(|pool| DbCapability {
            pool,
            schema: services.db_schema,
            timeouts: services.db_timeouts,
            accounting: accounting.clone(),
        });
        let blobstore = services.blobstore.map(|runtime| BlobstoreCapability {
            runtime,
            prefix: services.blob_prefix,
            limits: services.blob_limits,
        });
        let llm = services.llm.map(|runtime| LlmCapability {
            runtime,
            accounting: accounting.clone(),
        });
        Ok(Self {
            wasi: builder.build(),
            http: WasiHttpCtx::new(),
            table: ResourceTable::new(),
            hooks: ModuleHttpHooks {
                proxy_uri,
                module_name,
                module_namespace,
                http_pool,
                outbound_parent: outbound_parent.clone(),
                max_outbound_body_bytes: services.max_outbound_body_bytes,
            },
            capabilities: ModuleCapabilities {
                db,
                blobstore,
                llm,
                tracing: TracingCapability {
                    active_span: services.active_span,
                    span_stack: Vec::new(),
                    outbound_parent,
                    accounting,
                },
                _fs: FsCapability { _root: fs_root },
            },
        })
    }

    pub(crate) fn db(&mut self) -> Result<&mut DbCapability, DbError> {
        self.capabilities
            .db
            .as_mut()
            .ok_or_else(|| DbError::Connection("no database configured for this module".into()))
    }

    pub(crate) fn blobstore(&mut self) -> Result<&mut BlobstoreCapability, BlobError> {
        self.capabilities
            .blobstore
            .as_mut()
            .ok_or_else(|| BlobError::Io("no blobstore configured for this module".into()))
    }

    pub(crate) fn llm(&mut self) -> Result<&mut LlmCapability, LlmError> {
        self.capabilities
            .llm
            .as_mut()
            .ok_or_else(|| LlmError::InvalidRequest("no LLM configured for this module".into()))
    }

    pub(crate) fn tracing_context(&mut self) -> &mut TracingCapability {
        &mut self.capabilities.tracing
    }

    pub(crate) fn tracing_mut(&mut self) -> (&mut TracingCapability, &mut ResourceTable) {
        (&mut self.capabilities.tracing, &mut self.table)
    }

    pub fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}

impl WasiView for ModuleState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

impl WasiHttpView for ModuleState {
    fn http(&mut self) -> WasiHttpCtxView<'_> {
        WasiHttpCtxView {
            ctx: &mut self.http,
            table: &mut self.table,
            hooks: &mut self.hooks,
        }
    }
}
