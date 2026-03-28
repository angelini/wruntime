use anyhow::Result;
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Full};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info, warn};
use wasmtime::component::{Component, Linker};
use wasmtime::Store;
use wasmtime::{Config, Engine};
use wasmtime_wasi_http::{
    bindings::http::types::{ErrorCode, Scheme},
    bindings::ProxyPre,
    body::{HostIncomingBody, HyperIncomingBody, HyperOutgoingBody},
    types::{HostIncomingRequest, HostResponseOutparam},
    WasiHttpView,
};

use crate::config::{EngineConfig, ModuleConfig};
use crate::registry::{InboundRequest, ModuleRegistry};
use crate::state::ModuleState;

pub struct EngineRunner {
    engine: Arc<Engine>,
    config: EngineConfig,
}

impl EngineRunner {
    pub fn new(config: EngineConfig) -> Result<Self> {
        let mut wt_config = Config::new();
        wt_config.async_support(true);
        wt_config.wasm_component_model(true);
        let engine = Engine::new(&wt_config)?;
        Ok(Self {
            engine: Arc::new(engine),
            config,
        })
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
        let proxy_uri: hyper::Uri = self.config.proxy_address.parse()?;
        let module_name = module_config.name.clone();
        let module_version = module_config.version.clone();

        let mut linker: Linker<ModuleState> = Linker::new(&self.engine);
        wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
        wasmtime_wasi_http::add_only_http_to_linker_async(&mut linker)?;

        // Try to pre-link as a WASI HTTP Proxy world component first.
        // This succeeds when the component exports `wasi:http/incoming-handler`.
        match ProxyPre::new(linker.instantiate_pre(&component)?) {
            Ok(pre) => {
                let pre = Arc::new(pre);
                let (tx, rx) = mpsc::channel::<InboundRequest>(32);
                registry
                    .register(module_name.clone(), module_version.clone(), tx)
                    .await;

                let engine = self.engine.clone();
                tokio::spawn(http_handler_task(
                    engine,
                    pre,
                    proxy_uri,
                    module_name.clone(),
                    rx,
                ));
            }
            Err(_) => {
                // Fall back: spawn as a long-running task that calls `run`.
                let state = ModuleState::new(module_name.clone(), proxy_uri);
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

/// Task that owns the module's channel receiver and spawns a sub-task per
/// inbound request, each with its own `Store` for isolation.
async fn http_handler_task(
    engine: Arc<Engine>,
    pre: Arc<ProxyPre<ModuleState>>,
    proxy_uri: hyper::Uri,
    module_name: String,
    mut rx: mpsc::Receiver<InboundRequest>,
) {
    while let Some(inbound) = rx.recv().await {
        let engine = engine.clone();
        let pre = pre.clone();
        let proxy_uri = proxy_uri.clone();
        let module_name = module_name.clone();

        tokio::spawn(async move {
            if let Err(e) = dispatch_request(&engine, &pre, proxy_uri, &module_name, inbound).await
            {
                warn!(module = %module_name, error = %e, "inbound request error");
            }
        });
    }
}

/// Instantiate the component for one request and drive the WASI HTTP
/// incoming-handler, returning the response through `inbound.response_tx`.
async fn dispatch_request(
    engine: &Engine,
    pre: &ProxyPre<ModuleState>,
    proxy_uri: hyper::Uri,
    module_name: &str,
    inbound: InboundRequest,
) -> Result<()> {
    let state = ModuleState::new(module_name.to_string(), proxy_uri);
    let mut store = Store::new(engine, state);
    let proxy = pre.instantiate_async(&mut store).await?;

    // ── Build the incoming request resource ──────────────────────────────
    let (req_parts, req_body) = inbound.request.into_parts();

    // Wrap the buffered Bytes as a HyperIncomingBody
    // (UnsyncBoxBody<Bytes, ErrorCode>).
    let hyper_body: HyperIncomingBody = UnsyncBoxBody::new(
        Full::new(req_body).map_err(|_: Infallible| ErrorCode::InternalError(None)),
    );
    let host_body = HostIncomingBody::new(
        hyper_body,
        Duration::from_secs(30), // between-bytes timeout
        usize::MAX,              // field size limit (unconstrained for now)
    );

    let host_req = HostIncomingRequest::new(
        store.data_mut(),
        req_parts,
        Scheme::Http,
        Some(host_body),
        usize::MAX,
    )?;

    let req_resource = store.data_mut().table().push(host_req)?;

    // ── Build the response outparam resource ─────────────────────────────
    let (resp_tx, resp_rx) =
        tokio::sync::oneshot::channel::<Result<hyper::Response<HyperOutgoingBody>, ErrorCode>>();
    let out_resource = store
        .data_mut()
        .table()
        .push(HostResponseOutparam { result: resp_tx })?;

    // ── Call the WASM incoming handler ───────────────────────────────────
    proxy
        .wasi_http_incoming_handler()
        .call_handle(&mut store, req_resource, out_resource)
        .await?;

    // ── Collect the response and forward it to the inbound server ────────
    match resp_rx.await {
        Ok(Ok(wasm_resp)) => {
            let (rp, rb) = wasm_resp.into_parts();
            let bytes = rb
                .collect()
                .await
                .map_err(|e| anyhow::anyhow!("collecting WASM response body: {e:?}"))?
                .to_bytes();
            let _ = inbound
                .response_tx
                .send(http::Response::from_parts(rp, bytes));
        }
        Ok(Err(e)) => anyhow::bail!("WASM handler returned ErrorCode: {e:?}"),
        Err(_) => anyhow::bail!("WASM handler dropped the response outparam"),
    }

    Ok(())
}
