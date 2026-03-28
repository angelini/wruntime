use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot, RwLock};

/// A single inbound HTTP request dispatched from the engine's inbound server
/// to a WASM module task.
pub struct InboundRequest {
    pub request:     http::Request<Bytes>,
    pub response_tx: oneshot::Sender<http::Response<Bytes>>,
}

pub type ModuleTx = mpsc::Sender<InboundRequest>;

/// Maps module names to the channel used to dispatch inbound HTTP requests.
#[derive(Clone, Default)]
pub struct ModuleRegistry {
    inner: Arc<RwLock<HashMap<String, ModuleTx>>>,
}

impl ModuleRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register(&self, name: String, tx: ModuleTx) {
        self.inner.write().await.insert(name, tx);
    }

    pub async fn sender(&self, name: &str) -> Option<ModuleTx> {
        self.inner.read().await.get(name).cloned()
    }
}
