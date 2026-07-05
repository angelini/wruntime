use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

pub mod blobstore;
pub mod config;
pub mod db;
pub mod llm;
pub mod migration;
pub mod pool;
pub mod runtime;
pub mod state;
pub mod tracing;
pub mod worker;

/// A single inbound request dispatched to a WASM module task.
/// Used by both the inbound HTTP server and the worker pool.
pub struct InboundRequest {
    pub request: http::Request<Bytes>,
    pub response_tx: oneshot::Sender<http::Response<Bytes>>,
    /// Trace span carried through the channel for context propagation.
    pub span: ::tracing::Span,
}

/// Channel sender for dispatching requests to a module handler.
pub type ModuleTx = mpsc::Sender<InboundRequest>;
