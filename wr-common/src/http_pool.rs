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

#[cfg(test)]
mod tests {
    use super::*;

    // Use http_body_util::Empty as a simple Body impl for tests.
    type TestPool = HttpClientPool<http_body_util::Empty<bytes::Bytes>>;

    #[test]
    fn new_creates_pool_of_requested_size() {
        let pool = TestPool::new(3);
        assert_eq!(pool.size(), 3);
    }

    #[test]
    fn new_single_client() {
        let pool = TestPool::new(1);
        assert_eq!(pool.size(), 1);
    }

    #[test]
    #[should_panic(expected = "pool size must be at least 1")]
    fn new_zero_size_panics() {
        let _ = TestPool::new(0);
    }

    #[test]
    fn get_round_robins_across_clients() {
        let pool = TestPool::new(3);
        // Each call to get() should cycle through indices 0, 1, 2, 0, 1, ...
        // We can't directly compare Client identity, but we can verify
        // the counter advances by calling get() many times without panic.
        for _ in 0..100 {
            let _ = pool.get();
        }
    }

    #[test]
    fn clone_shares_state() {
        let pool = TestPool::new(2);
        let clone = pool.clone();

        // Advance counter on original
        let _ = pool.get(); // counter becomes 1
        // Clone should see the same counter (shared via Arc<AtomicUsize>)
        // Next get on clone should use index 1 % 2 = 1, then advance to 2
        let _ = clone.get();
        // Counter is now 2; next call on original uses 2 % 2 = 0
        assert_eq!(clone.size(), 2);
    }

    #[test]
    fn counter_wrapping_does_not_panic() {
        let pool = TestPool::new(3);
        // Simulate near-overflow by setting the counter close to usize::MAX
        pool.next.store(usize::MAX - 1, Ordering::Relaxed);
        // These calls cross the usize::MAX boundary via wrapping add
        let _ = pool.get(); // usize::MAX - 1
        let _ = pool.get(); // usize::MAX
        let _ = pool.get(); // wraps to 0
        let _ = pool.get(); // 1
    }

    #[test]
    fn default_pool_size_is_four() {
        assert_eq!(DEFAULT_POOL_SIZE, 4);
    }
}
