use std::sync::Arc;

use crate::blobstore::BlobstoreRuntime;
use crate::config::FsMode;
use crate::llm::LlmRuntime;
use deadpool_postgres::Pool;
use http_body_util::{BodyExt, Full};
use hyper::header::{HeaderName, HeaderValue};
use hyper_util::client::legacy::{connect::HttpConnector, Client};
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

/// Hooks that intercept every outbound HTTP request from a WASM module.
///
/// - Preserves the original destination in `x-wr-destination` so
///   wr-proxy can route the request to the correct engine.
/// - Tags the request with `x-wr-source` for metrics attribution.
/// - Rewrites the URI to point at wr-proxy.
/// - Uses a shared, connection-pooled HTTP client to avoid ephemeral port
///   exhaustion under load (`EADDRNOTAVAIL` / DnsError).
struct ModuleHttpHooks {
    proxy_uri: hyper::Uri,
    module_name: String,
    module_namespace: String,
    /// Shared across all requests for this module instance.
    http_client: Client<HttpConnector, Full<bytes::Bytes>>,
}

impl WasiHttpHooks for ModuleHttpHooks {
    fn send_request(
        &mut self,
        mut request: hyper::Request<HyperOutgoingBody>,
        config: OutgoingRequestConfig,
    ) -> HttpResult<HostFutureIncomingResponse> {
        let original_uri = request.uri().to_string();

        request.headers_mut().insert(
            HeaderName::from_static("x-wr-destination"),
            HeaderValue::from_str(&original_uri).map_err(|_| ErrorCode::InternalError(None))?,
        );
        request.headers_mut().insert(
            HeaderName::from_static("x-wr-source"),
            HeaderValue::from_str(&self.module_name).map_err(|_| ErrorCode::InternalError(None))?,
        );
        request.headers_mut().insert(
            HeaderName::from_static("x-wr-source-ns"),
            HeaderValue::from_str(&self.module_namespace)
                .map_err(|_| ErrorCode::InternalError(None))?,
        );

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

        let client = self.http_client.clone();
        let between_bytes_timeout = config.between_bytes_timeout;

        let handle = wasmtime_wasi::runtime::spawn(async move {
            Ok(async move {
                // Buffer the outgoing body so we can hand it to the pooled client,
                // which requires a Send + 'static body type (Full<Bytes>).
                let (parts, body) = request.into_parts();
                let body_bytes = body
                    .collect()
                    .await
                    .map_err(|e| ErrorCode::InternalError(Some(e.to_string())))?
                    .to_bytes();
                let buffered = hyper::Request::from_parts(parts, Full::new(body_bytes));

                let resp = client.request(buffered).await.map_err(|e| {
                    tracing::warn!(error = ?e, "outgoing http request failed");
                    if e.is_connect() {
                        ErrorCode::ConnectionRefused
                    } else {
                        ErrorCode::InternalError(Some(e.to_string()))
                    }
                })?;

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
}

impl Default for ModuleServices {
    fn default() -> Self {
        Self {
            db_pool: None,
            db_schema: None,
            db_timeouts: DbTimeouts::default(),
            blobstore: None,
            blob_prefix: None,
            llm: None,
            fs: None,
            env_vars: Arc::new(std::collections::HashMap::new()),
            active_span: tracing::Span::none(),
        }
    }
}

pub struct ModuleState {
    wasi: WasiCtx,
    http: WasiHttpCtx,
    table: ResourceTable,
    hooks: ModuleHttpHooks,
    /// Shared connection pool, present when the module has DB access enabled.
    pub db_pool: Option<Arc<Pool>>,
    /// Postgres schema name for this module (`wr__{namespace}__{name}`).
    /// Set when DB access is enabled; used to scope all queries to the module's schema.
    pub db_schema: Option<Arc<str>>,
    /// Timeout configuration for guest DB connections.
    pub db_timeouts: DbTimeouts,
    /// Ephemeral temp directory backing the module's WASI filesystem.
    /// `Some` only when `fs = "tempdir"` is set; kept alive so it isn't
    /// deleted until the store is dropped.
    _fs_root: Option<TempDir>,
    /// Shared S3-compatible blobstore client, present when the module has blobstore access enabled.
    pub blobstore: Option<Arc<BlobstoreRuntime>>,
    /// S3 key prefix for namespace isolation (e.g. `wr/ecommerce/`).
    pub blob_prefix: Option<Arc<str>>,
    /// Shared LLM inference client, present when the module has LLM access enabled.
    pub llm: Option<Arc<LlmRuntime>>,
    /// The `engine.dispatch` span for the current request.
    pub active_span: tracing::Span,
}

impl ModuleState {
    pub fn new(
        module_name: String,
        module_namespace: String,
        proxy_uri: hyper::Uri,
        http_client: Client<HttpConnector, Full<bytes::Bytes>>,
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
        Ok(Self {
            wasi: builder.build(),
            http: WasiHttpCtx::new(),
            table: ResourceTable::new(),
            hooks: ModuleHttpHooks {
                proxy_uri,
                module_name,
                module_namespace,
                http_client,
            },
            db_pool: services.db_pool,
            db_schema: services.db_schema,
            db_timeouts: services.db_timeouts,
            blobstore: services.blobstore,
            blob_prefix: services.blob_prefix,
            llm: services.llm,
            _fs_root: fs_root,
            active_span: services.active_span,
        })
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
