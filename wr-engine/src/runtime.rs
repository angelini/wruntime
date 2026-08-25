//! Workspace-internal WASM runtime primitives shared by the production engine
//! and the integration-test harness.

use std::future::Future as _;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use http_body_util::BodyExt as _;
use tokio::sync::{oneshot, OwnedSemaphorePermit};
use wasmtime::component::{Component, HasSelf, Linker};
use wasmtime::{Config, Engine, InstanceAllocationStrategy, PoolingAllocationConfig, Store, Trap};
use wasmtime_wasi_http::p2::{
    bindings::http::types::Scheme, bindings::ProxyPre, body::HyperOutgoingBody, WasiHttpView as _,
};

use crate::config::PoolConfig;
use crate::state::ModuleState;
use crate::{EngineBodyError, EngineResponse, InboundBody};

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

/// Build a `Linker` with WASI p2, WASI HTTP, and all host bindings registered.
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

pub fn instantiate_pre(
    _engine: &Engine,
    linker: &Linker<ModuleState>,
    component: &Component,
) -> Result<ProxyPre<ModuleState>> {
    Ok(ProxyPre::new(linker.instantiate_pre(component)?)?)
}

#[derive(Debug)]
pub enum RuntimeError {
    Timeout,
    Failed(String),
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout => formatter.write_str("request timed out"),
            Self::Failed(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for RuntimeError {}

#[derive(Debug)]
enum RuntimeCompletion {
    Complete,
    Timeout,
    Failed(String),
}

struct RuntimeResponseBody {
    body: HyperOutgoingBody,
    completion: Option<oneshot::Receiver<RuntimeCompletion>>,
    task: Option<tokio::task::JoinHandle<()>>,
    release: Option<oneshot::Sender<()>>,
    failed: bool,
}

impl RuntimeResponseBody {
    fn release_owner(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }

    fn abort_and_join(&mut self) {
        self.release_owner();
        let Some(task) = self.task.take() else {
            return;
        };
        task.abort();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = task.await;
            });
        }
    }

    fn completion_error(completion: RuntimeCompletion) -> Option<EngineBodyError> {
        match completion {
            RuntimeCompletion::Complete => None,
            RuntimeCompletion::Timeout => Some(EngineBodyError(
                "request timed out after response headers".into(),
            )),
            RuntimeCompletion::Failed(message) => Some(EngineBodyError(message)),
        }
    }
}

impl Body for RuntimeResponseBody {
    type Data = Bytes;
    type Error = EngineBodyError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if self.failed {
            return Poll::Ready(None);
        }

        if let Some(completion) = self.completion.as_mut() {
            match Pin::new(completion).poll(cx) {
                Poll::Ready(Ok(result)) => {
                    self.completion = None;
                    if let Some(error) = Self::completion_error(result) {
                        self.failed = true;
                        self.abort_and_join();
                        return Poll::Ready(Some(Err(error)));
                    }
                }
                Poll::Ready(Err(_)) => {
                    self.completion = None;
                    self.failed = true;
                    self.abort_and_join();
                    return Poll::Ready(Some(Err(EngineBodyError(
                        "WASM response runtime ended unexpectedly".into(),
                    ))));
                }
                Poll::Pending => {}
            }
        }

        match Pin::new(&mut self.body).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => Poll::Ready(Some(Ok(frame))),
            Poll::Ready(Some(Err(error))) => {
                self.failed = true;
                self.abort_and_join();
                Poll::Ready(Some(Err(EngineBodyError(format!(
                    "WASM response body error: {error:?}"
                )))))
            }
            Poll::Ready(None) => {
                if self.completion.is_some() {
                    return Poll::Pending;
                }
                self.release_owner();
                if let Some(task) = self.task.as_mut() {
                    match Pin::new(task).poll(cx) {
                        Poll::Ready(_) => {
                            self.task = None;
                            Poll::Ready(None)
                        }
                        Poll::Pending => Poll::Pending,
                    }
                } else {
                    Poll::Ready(None)
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.failed || (self.body.is_end_stream() && self.completion.is_none())
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

impl Drop for RuntimeResponseBody {
    fn drop(&mut self) {
        self.abort_and_join();
    }
}

/// Start one guest request and return as soon as response headers are available.
/// The returned body owns cancellation of the guest task; the task owns the
/// store and optional instance permit until completion or cancellation.
pub async fn run_incoming_handler_streaming(
    engine: &Engine,
    pre: &ProxyPre<ModuleState>,
    state: ModuleState,
    request: http::Request<InboundBody>,
    timeout: Duration,
    permit: Option<OwnedSemaphorePermit>,
) -> Result<EngineResponse> {
    let engine = engine.clone();
    let pre = pre.clone();
    let (response_tx, mut response_rx) = oneshot::channel();
    let (completion_tx, mut completion_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let mut release_tx = Some(release_tx);

    let task = tokio::spawn(async move {
        let _permit = permit;
        let result = tokio::time::timeout(timeout, async move {
            let mut store = Store::new(&engine, state);
            store.set_epoch_deadline(1);
            store.epoch_deadline_async_yield_and_update(1);
            let proxy = pre.instantiate_async(&mut store).await?;

            let (parts, body) = request.into_parts();
            let request = hyper::Request::from_parts(parts, body);
            let request = store
                .data_mut()
                .http()
                .new_incoming_request(Scheme::Http, request)?;
            let response = store.data_mut().http().new_response_outparam(response_tx)?;
            proxy
                .wasi_http_incoming_handler()
                .call_handle(&mut store, request, response)
                .await
        })
        .await;

        let completion = match result {
            Err(_) => RuntimeCompletion::Timeout,
            Ok(Err(error)) if error.downcast_ref::<Trap>() == Some(&Trap::Interrupt) => {
                RuntimeCompletion::Timeout
            }
            Ok(Err(error)) => RuntimeCompletion::Failed(error.to_string()),
            Ok(Ok(())) => RuntimeCompletion::Complete,
        };
        let _ = completion_tx.send(completion);
        let _ = release_rx.await;
    });

    tokio::select! {
        response = &mut response_rx => {
            match response {
                Ok(Ok(response)) => {
                    let (parts, body) = response.into_parts();
                    let body = RuntimeResponseBody {
                        body,
                        completion: Some(completion_rx),
                        task: Some(task),
                        release: release_tx.take(),
                        failed: false,
                    }.boxed_unsync();
                    Ok(http::Response::from_parts(parts, body))
                }
                Ok(Err(error)) => {
                    if let Some(release) = release_tx.take() {
                        let _ = release.send(());
                    }
                    let _ = task.await;
                    Err(RuntimeError::Failed(format!("WASM handler returned ErrorCode: {error:?}")).into())
                }
                Err(_) => {
                    let completion = completion_rx.await.unwrap_or_else(|_| {
                        RuntimeCompletion::Failed("WASM handler dropped the response outparam".into())
                    });
                    if let Some(release) = release_tx.take() {
                        let _ = release.send(());
                    }
                    let _ = task.await;
                    match completion {
                        RuntimeCompletion::Timeout => Err(RuntimeError::Timeout.into()),
                        RuntimeCompletion::Failed(message) => Err(RuntimeError::Failed(message).into()),
                        RuntimeCompletion::Complete => Err(RuntimeError::Failed("WASM handler dropped the response outparam".into()).into()),
                    }
                }
            }
        }
        completion = &mut completion_rx => {
            let completion = completion.unwrap_or_else(|_| {
                RuntimeCompletion::Failed("WASM response runtime ended unexpectedly".into())
            });
            match response_rx.await {
                Ok(Ok(response)) => {
                    let (completion_tx, completion_rx) = oneshot::channel();
                    let _ = completion_tx.send(completion);
                    let (parts, body) = response.into_parts();
                    let body = RuntimeResponseBody {
                        body,
                        completion: Some(completion_rx),
                        task: Some(task),
                        release: release_tx.take(),
                        failed: false,
                    }.boxed_unsync();
                    Ok(http::Response::from_parts(parts, body))
                }
                Ok(Err(error)) => {
                    if let Some(release) = release_tx.take() {
                        let _ = release.send(());
                    }
                    let _ = task.await;
                    Err(RuntimeError::Failed(format!(
                        "WASM handler returned ErrorCode: {error:?}"
                    )).into())
                }
                Err(_) => {
                    if let Some(release) = release_tx.take() {
                        let _ = release.send(());
                    }
                    let _ = task.await;
                    match completion {
                    RuntimeCompletion::Timeout => Err(RuntimeError::Timeout.into()),
                    RuntimeCompletion::Failed(message) => Err(RuntimeError::Failed(message).into()),
                        RuntimeCompletion::Complete => Err(RuntimeError::Failed(
                            "WASM handler completed without response headers".into()
                        ).into()),
                    }
                }
            }
        }
    }
}

/// Buffered adapter used by worker persistence and focused host tests.
pub async fn run_incoming_handler(
    engine: &Engine,
    pre: &ProxyPre<ModuleState>,
    state: ModuleState,
    request: http::Request<Bytes>,
) -> Result<http::Response<Bytes>> {
    let (parts, body) = request.into_parts();
    let request = http::Request::from_parts(parts, crate::inbound_full(body));
    let response =
        run_incoming_handler_streaming(engine, pre, state, request, Duration::from_secs(30), None)
            .await?;
    let (parts, body) = response.into_parts();
    let body = body.collect().await?.to_bytes();
    Ok(http::Response::from_parts(parts, body))
}
