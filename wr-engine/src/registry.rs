use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use hashbrown::HashMap;
use tokio::sync::RwLock;
use wr_common::identity::ModuleId;

pub use wr_engine::{InboundRequest, ModuleTx};

struct InstanceList {
    senders: Vec<ModuleTx>,
    /// Monotonic counter used for round-robin selection.
    next: AtomicUsize,
}

type RegistryMap = Arc<RwLock<HashMap<ModuleId, InstanceList>>>;

/// Maps typed module identities to one or more running instance channels.
/// Multiple senders for the same key are served in round-robin order.
#[derive(Clone, Default)]
pub struct ModuleRegistry {
    inner: RegistryMap,
}

impl ModuleRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new instance. May be called multiple times for the same key;
    /// each call appends another sender.
    pub async fn register(&self, id: ModuleId, tx: ModuleTx) {
        let mut map = self.inner.write().await;
        let entry = map.entry(id).or_insert_with(|| InstanceList {
            senders: Vec::new(),
            next: AtomicUsize::new(0),
        });
        entry.senders.push(tx);
    }

    /// Return the next sender for `id` using round-robin selection, or `None`
    /// if no instances are registered for the key.
    pub async fn next_sender(&self, id: &ModuleId) -> Option<ModuleTx> {
        let map = self.inner.read().await;
        let entry = map.get(id)?;
        if entry.senders.is_empty() {
            return None;
        }
        let idx = entry.next.fetch_add(1, Ordering::Relaxed) % entry.senders.len();
        Some(entry.senders[idx].clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tokio::sync::{mpsc, oneshot};

    fn id() -> ModuleId {
        ModuleId::parse("shop", "orders", "1.0.0").unwrap()
    }

    #[tokio::test]
    async fn typed_lookup_round_robins_instances() {
        let registry = ModuleRegistry::new();
        let (first, mut first_rx) = mpsc::channel(1);
        let (second, mut second_rx) = mpsc::channel(1);
        registry.register(id(), first).await;
        registry.register(id(), second).await;

        for expected_first in [true, false, true, false] {
            let sender = registry.next_sender(&id()).await.unwrap();
            let (response_tx, _response_rx) = oneshot::channel();
            sender
                .send(InboundRequest {
                    request: http::Request::new(wr_engine::inbound_full(Bytes::new())),
                    response_tx,
                    span: tracing::Span::none(),
                })
                .await
                .unwrap();
            assert_eq!(first_rx.try_recv().is_ok(), expected_first);
            assert_eq!(second_rx.try_recv().is_ok(), !expected_first);
        }
    }
}
