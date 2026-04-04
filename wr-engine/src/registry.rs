use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use hashbrown::HashMap;
use tokio::sync::RwLock;

pub use wr_engine::worker::{InboundRequest, ModuleTx};

// ── Zero-allocation key types ───────────────────────────────────────────────

/// Owned key stored in the registry HashMap.
#[derive(Clone, Eq, PartialEq)]
struct ModuleKey(Arc<str>, Arc<str>, Arc<str>);

impl Hash for ModuleKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
        self.1.hash(state);
        self.2.hash(state);
    }
}

// ── Registry ────────────────────────────────────────────────────────────────

struct InstanceList {
    senders: Vec<ModuleTx>,
    /// Monotonic counter used for round-robin selection.
    next: Arc<AtomicUsize>,
}

type RegistryMap = Arc<RwLock<HashMap<ModuleKey, InstanceList>>>;

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
        let key = ModuleKey(Arc::from(namespace), Arc::from(name), Arc::from(version));
        let mut map = self.inner.write().await;
        let entry = map.entry(key).or_insert_with(|| InstanceList {
            senders: Vec::new(),
            next: Arc::new(AtomicUsize::new(0)),
        });
        entry.senders.push(tx);
    }

    /// Return the next sender for (namespace, name, version) using round-robin selection,
    /// or `None` if no instances are registered for that key.
    ///
    /// Uses hashbrown's `raw_entry` API to look up by borrowed `&str` slices
    /// without allocating owned Strings or Arcs on every request.
    pub async fn next_sender(
        &self,
        namespace: &str,
        name: &str,
        version: &str,
    ) -> Option<ModuleTx> {
        let map = self.inner.read().await;

        let hash = {
            use std::hash::BuildHasher;
            let mut hasher = map.hasher().build_hasher();
            namespace.hash(&mut hasher);
            name.hash(&mut hasher);
            version.hash(&mut hasher);
            hasher.finish()
        };

        let entry = map
            .raw_entry()
            .from_hash(hash, |k| {
                k.0.as_ref() == namespace && k.1.as_ref() == name && k.2.as_ref() == version
            })?
            .1;

        if entry.senders.is_empty() {
            return None;
        }
        let idx = entry.next.fetch_add(1, Ordering::Relaxed) % entry.senders.len();
        Some(entry.senders[idx].clone())
    }
}
