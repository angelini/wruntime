//! Workspace-internal WASM runtime primitives shared by the production engine
//! (`wr-engine`'s binary-crate `engine` module) and the `wr-tests` harness, so
//! both drive the exact same Wasmtime `Config`, linker, pre-instantiation, and
//! dispatch path instead of maintaining two diverging copies.
//!
//! This is NOT a stable public engine API. Its only stability guarantee is
//! "internal to this workspace" — it exists so tests can exercise the real
//! production runtime path. Do not treat these functions as an external contract.

use anyhow::Result;
use bytes::Bytes;
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Full};
use std::convert::Infallible;
use wasmtime::component::{Component, HasSelf, Linker};
use wasmtime::{Config, Engine, InstanceAllocationStrategy, PoolingAllocationConfig, Store};
use wasmtime_wasi_http::p2::{
    bindings::http::types::{ErrorCode, Scheme},
    bindings::ProxyPre,
    body::{HyperIncomingBody, HyperOutgoingBody},
    WasiHttpView as _,
};

use crate::config::PoolConfig;
use crate::state::ModuleState;

/// Build the Wasmtime `Engine` with the component-model + pooling-allocator
/// configuration used in production.
pub fn build_engine(pool: &PoolConfig) -> Result<Engine> {
    let mut wt_config = Config::new();
    wt_config.wasm_component_model(true);
    wt_config.epoch_interruption(true);
    wt_config.memory_reservation(4 * (1 << 30));
    wt_config.memory_guard_size(32 * (1 << 20));
    wt_config.memory_init_cow(true);

    let mut alloc = PoolingAllocationConfig::new();
    alloc.total_component_instances(pool.total_component_instances);
    alloc.max_memory_size(pool.max_memory_size);
    alloc.total_memories(pool.total_component_instances);
    alloc.total_tables(pool.total_component_instances);
    wt_config.allocation_strategy(InstanceAllocationStrategy::Pooling(alloc));

    Ok(Engine::new(&wt_config)?)
}

/// Build a `Linker` with WASI p2, WASI HTTP, and all four host bindings
/// registered in the exact order the engine requires.
pub fn configure_linker(engine: &Engine) -> Result<Linker<ModuleState>> {
    let mut linker: Linker<ModuleState> = Linker::new(engine);
    wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
    wasmtime_wasi_http::p2::add_only_http_to_linker_async(&mut linker)?;
    crate::db::wruntime::db::database::add_to_linker::<ModuleState, HasSelf<ModuleState>>(
        &mut linker,
        |s| s,
    )?;
    crate::tracing::add_to_linker::<ModuleState, HasSelf<ModuleState>>(&mut linker, |s| s)?;
    crate::blobstore::add_to_linker::<ModuleState, HasSelf<ModuleState>>(&mut linker, |s| s)?;
    crate::llm::add_to_linker::<ModuleState, HasSelf<ModuleState>>(&mut linker, |s| s)?;
    Ok(linker)
}

/// Pre-instantiate `component` into a `ProxyPre`. Callers that need a
/// domain-specific error message (e.g. production's "must export
/// wasi:http/incoming-handler") should build the `ProxyPre` inline instead of
/// calling this.
///
/// `_engine` is accepted for call-site symmetry with `build_engine` /
/// `configure_linker`; it is intentionally unused because `linker` already
/// carries the engine. Do not remove the parameter.
pub fn instantiate_pre(
    _engine: &Engine,
    linker: &Linker<ModuleState>,
    component: &Component,
) -> Result<ProxyPre<ModuleState>> {
    Ok(ProxyPre::new(linker.instantiate_pre(component)?)?)
}

/// Instantiate the component for one request and drive the WASI HTTP
/// incoming-handler, returning the fully-buffered response.
///
/// This is for the current fully-buffered inbound dispatch path ONLY:
/// `Request<Bytes>` in, `Response<Bytes>` out. It is not a streaming boundary;
/// inbound streaming would require a different signature.
///
/// This helper deliberately carries NO instance-semaphore gating, NO per-request
/// timeout, and NO `x-wr-timeout` handling — those policies stay at the
/// production call site (`engine::dispatch_request` / `http_handler_task`).
///
/// The `call_handle` error is returned RAW (no `.context(..)` wrapping): the
/// production call site downcasts it to `wasmtime::Trap::Interrupt` to map
/// epoch-deadline timeouts to 504. A context layer would defeat that downcast.
pub async fn run_incoming_handler(
    engine: &Engine,
    pre: &ProxyPre<ModuleState>,
    state: ModuleState,
    request: http::Request<Bytes>,
) -> Result<http::Response<Bytes>> {
    let mut store = Store::new(engine, state);
    store.set_epoch_deadline(1);
    store.epoch_deadline_async_yield_and_update(1);
    let proxy = pre.instantiate_async(&mut store).await?;

    let (req_parts, req_body) = request.into_parts();
    let hyper_body: HyperIncomingBody = UnsyncBoxBody::new(
        Full::new(req_body).map_err(|_: Infallible| ErrorCode::InternalError(None)),
    );
    let hyper_req = hyper::Request::from_parts(req_parts, hyper_body);
    let req_resource = store
        .data_mut()
        .http()
        .new_incoming_request(Scheme::Http, hyper_req)?;

    let (resp_tx, resp_rx) =
        tokio::sync::oneshot::channel::<Result<hyper::Response<HyperOutgoingBody>, ErrorCode>>();
    let out_resource = store.data_mut().http().new_response_outparam(resp_tx)?;

    proxy
        .wasi_http_incoming_handler()
        .call_handle(&mut store, req_resource, out_resource)
        .await?;

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
