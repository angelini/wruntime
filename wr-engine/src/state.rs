use hyper::header::{HeaderName, HeaderValue};
use wasmtime::component::ResourceTable;
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};
use wasmtime_wasi_http::{
    HttpResult, WasiHttpCtx, WasiHttpView,
    bindings::http::types::ErrorCode,
    body::HyperOutgoingBody,
    types::{default_send_request, HostFutureIncomingResponse, OutgoingRequestConfig},
};

pub struct ModuleState {
    wasi:        WasiCtx,
    http:        WasiHttpCtx,
    table:       ResourceTable,
    module_name: String,
    /// Pre-parsed proxy URI so we don't re-parse on every request.
    proxy_uri:   hyper::Uri,
}

impl ModuleState {
    pub fn new(module_name: String, proxy_uri: hyper::Uri) -> Self {
        Self {
            wasi: WasiCtxBuilder::new().inherit_stdio().build(),
            http: WasiHttpCtx::new(),
            table: ResourceTable::new(),
            module_name,
            proxy_uri,
        }
    }
}

impl WasiView for ModuleState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx:   &mut self.wasi,
            table: &mut self.table,
        }
    }
}

impl WasiHttpView for ModuleState {
    fn ctx(&mut self) -> &mut WasiHttpCtx {
        &mut self.http
    }
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }

    /// Intercepts every outbound HTTP request from the WASM module.
    ///
    /// - Preserves the original destination in `x-wr-destination` so
    ///   wr-proxy can route the request to the correct engine.
    /// - Tags the request with `x-wr-source` for metrics attribution.
    /// - Rewrites the URI to point at wr-proxy.
    fn send_request(
        &mut self,
        mut request: hyper::Request<HyperOutgoingBody>,
        config: OutgoingRequestConfig,
    ) -> HttpResult<HostFutureIncomingResponse> {
        let original_uri = request.uri().to_string();

        request.headers_mut().insert(
            HeaderName::from_static("x-wr-destination"),
            HeaderValue::from_str(&original_uri)
                .map_err(|_| ErrorCode::InternalError(None))?,
        );
        request.headers_mut().insert(
            HeaderName::from_static("x-wr-source"),
            HeaderValue::from_str(&self.module_name)
                .map_err(|_| ErrorCode::InternalError(None))?,
        );

        // Preserve the original path+query; only replace scheme and authority.
        let path_and_query = request
            .uri()
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or("/");
        let scheme    = self.proxy_uri.scheme_str().unwrap_or("http");
        let authority = self.proxy_uri.authority().map(|a| a.as_str()).unwrap_or("");
        let new_uri: hyper::Uri = format!("{scheme}://{authority}{path_and_query}")
            .parse()
            .map_err(|_| ErrorCode::InternalError(None))?;
        *request.uri_mut() = new_uri;

        Ok(default_send_request(request, config))
    }
}
