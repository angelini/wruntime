use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use hyper_util::client::legacy::{connect::HttpConnector, Client};
use hyper_util::rt::TokioExecutor;

/// A round-robin pool of HTTP/2 clients.
///
/// HTTP/2 multiplexes all streams onto a single TCP connection per host.
/// Under high concurrency this creates bottlenecks: frame serialization
/// contention, TCP head-of-line blocking, and flow-control stalls.
///
/// `HttpClientPool` spreads requests across `N` independent `Client`
/// instances, each maintaining its own HTTP/2 connection, to get
/// `N`-way parallelism on the wire.
pub struct HttpClientPool<B> {
    clients: Arc<Vec<Client<HttpConnector, B>>>,
    next: Arc<AtomicUsize>,
}

// Manual Clone impl to avoid requiring `B: Clone`.
// `Client` is internally Arc-backed and always Clone regardless of B.
// The Vec is behind an Arc, so cloning is just a refcount bump.
impl<B> Clone for HttpClientPool<B> {
    fn clone(&self) -> Self {
        Self {
            clients: self.clients.clone(),
            next: self.next.clone(),
        }
    }
}

impl<B> HttpClientPool<B>
where
    B: http_body::Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    /// Create a pool of `size` independent HTTP/2 clients.
    ///
    /// Each client maintains its own connection pool, so concurrent
    /// requests are distributed across multiple TCP connections to
    /// the same host.
    pub fn new(size: usize) -> Self {
        assert!(size > 0, "pool size must be at least 1");
        let clients: Vec<_> = (0..size)
            .map(|_| {
                Client::builder(TokioExecutor::new())
                    .http2_only(true)
                    .build_http()
            })
            .collect();
        Self {
            clients: Arc::new(clients),
            next: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Return the next client in round-robin order.
    pub fn get(&self) -> &Client<HttpConnector, B> {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        &self.clients[idx]
    }

    /// Number of clients in the pool.
    pub fn size(&self) -> usize {
        self.clients.len()
    }
}

/// Default pool size for service-to-service HTTP/2 connections.
pub const DEFAULT_POOL_SIZE: usize = 4;
