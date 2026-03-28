use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot, RwLock};

/// A single inbound HTTP request dispatched from the engine's inbound server
/// to a WASM module task.
pub struct InboundRequest {
    pub request: http::Request<Bytes>,
    pub response_tx: oneshot::Sender<http::Response<Bytes>>,
}

pub type ModuleTx = mpsc::Sender<InboundRequest>;

struct InstanceList {
    senders: Vec<ModuleTx>,
    /// Monotonic counter used for round-robin selection.
    next: Arc<AtomicUsize>,
}

/// Maps (module_name, version) to one or more running instance channels.
/// Multiple senders for the same key are served in round-robin order.
#[derive(Clone, Default)]
pub struct ModuleRegistry {
    inner: Arc<RwLock<HashMap<(String, String), InstanceList>>>,
}

impl ModuleRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new instance for (name, version). May be called multiple
    /// times for the same key; each call appends another sender.
    pub async fn register(&self, name: String, version: String, tx: ModuleTx) {
        let mut map = self.inner.write().await;
        let entry = map.entry((name, version)).or_insert_with(|| InstanceList {
            senders: Vec::new(),
            next: Arc::new(AtomicUsize::new(0)),
        });
        entry.senders.push(tx);
    }

    /// Return the next sender for (name, version) using round-robin selection,
    /// or `None` if no instances are registered for that key.
    pub async fn next_sender(&self, name: &str, version: &str) -> Option<ModuleTx> {
        let map = self.inner.read().await;
        let entry = map.get(&(name.to_string(), version.to_string()))?;
        if entry.senders.is_empty() {
            return None;
        }
        let idx = entry.next.fetch_add(1, Ordering::Relaxed) % entry.senders.len();
        Some(entry.senders[idx].clone())
    }
}
