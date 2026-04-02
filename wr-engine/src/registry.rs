use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::RwLock;

pub use wr_engine::worker::{InboundRequest, ModuleTx};

struct InstanceList {
    senders: Vec<ModuleTx>,
    /// Monotonic counter used for round-robin selection.
    next: Arc<AtomicUsize>,
}

type RegistryMap = Arc<RwLock<HashMap<(String, String, String), InstanceList>>>;

/// Maps (namespace, module_name, version) to one or more running instance channels.
/// Multiple senders for the same key are served in round-robin order.
#[derive(Clone, Default)]
pub struct ModuleRegistry {
    inner: RegistryMap,
}

impl ModuleRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new instance for (namespace, name, version). May be called multiple
    /// times for the same key; each call appends another sender.
    pub async fn register(&self, namespace: String, name: String, version: String, tx: ModuleTx) {
        let mut map = self.inner.write().await;
        let entry = map
            .entry((namespace, name, version))
            .or_insert_with(|| InstanceList {
                senders: Vec::new(),
                next: Arc::new(AtomicUsize::new(0)),
            });
        entry.senders.push(tx);
    }

    /// Return the next sender for (namespace, name, version) using round-robin selection,
    /// or `None` if no instances are registered for that key.
    pub async fn next_sender(
        &self,
        namespace: &str,
        name: &str,
        version: &str,
    ) -> Option<ModuleTx> {
        let map = self.inner.read().await;
        let entry = map.get(&(namespace.to_string(), name.to_string(), version.to_string()))?;
        if entry.senders.is_empty() {
            return None;
        }
        let idx = entry.next.fetch_add(1, Ordering::Relaxed) % entry.senders.len();
        Some(entry.senders[idx].clone())
    }
}
