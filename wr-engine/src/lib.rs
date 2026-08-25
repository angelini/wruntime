use bytes::Bytes;
use http_body_util::{combinators::UnsyncBoxBody, BodyExt as _, Full};
use tokio::sync::{mpsc, oneshot};
use wasmtime_wasi_http::p2::bindings::http::types::ErrorCode;
use wasmtime_wasi_http::p2::body::HyperIncomingBody;

pub mod blobstore;
pub mod config;
pub mod db;
pub mod job_migration;
pub mod llm;
pub mod migration;
pub mod pool;
pub mod provisioning;
pub mod runtime;
pub mod startup_db;
pub mod state;
pub mod tracing;
pub mod worker;

pub type InboundBody = HyperIncomingBody;
pub type ResponseBody = UnsyncBoxBody<Bytes, EngineBodyError>;
pub type EngineResponse = http::Response<ResponseBody>;

#[derive(Debug)]
pub struct EngineBodyError(pub String);

impl std::fmt::Display for EngineBodyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for EngineBodyError {}

pub fn inbound_full(body: Bytes) -> InboundBody {
    Full::new(body)
        .map_err(|never: std::convert::Infallible| -> ErrorCode { match never {} })
        .boxed_unsync()
}

pub fn response_full(body: Bytes) -> ResponseBody {
    Full::new(body)
        .map_err(|never: std::convert::Infallible| -> EngineBodyError { match never {} })
        .boxed_unsync()
}

pub fn inbound_network(body: hyper::body::Incoming) -> InboundBody {
    body.map_err(ErrorCode::from).boxed_unsync()
}

/// A single inbound request dispatched to a WASM module task.
/// Used by both the inbound HTTP server and the worker pool.
pub struct InboundRequest {
    pub request: http::Request<InboundBody>,
    pub response_tx: oneshot::Sender<EngineResponse>,
    /// Trace span carried through the channel for context propagation.
    pub span: ::tracing::Span,
}

/// Channel sender for dispatching requests to a module handler.
pub type ModuleTx = mpsc::Sender<InboundRequest>;
