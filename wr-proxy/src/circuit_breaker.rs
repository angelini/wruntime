use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use failsafe::{backoff, failure_policy, Config, StateMachine};

use crate::config::CircuitBreakerConfig;

pub type EngineBreaker = StateMachine<failure_policy::ConsecutiveFailures<backoff::Constant>, ()>;

#[derive(Clone)]
pub struct CircuitBreakerRegistry {
    inner: Arc<Mutex<HashMap<Arc<str>, EngineBreaker>>>,
    config: CircuitBreakerConfig,
    resolver_calls: Arc<AtomicUsize>,
}

impl CircuitBreakerRegistry {
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            config,
            resolver_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Resolve the breaker for `addr` while preparing a routing snapshot.
    pub(crate) fn resolve(&self, addr: &str) -> EngineBreaker {
        self.resolver_calls.fetch_add(1, Ordering::Relaxed);
        let mut map = self.inner.lock().unwrap();
        if let Some(breaker) = map.get(addr) {
            return breaker.clone();
        }

        let key: Arc<str> = Arc::from(addr);
        let breaker = self.build_breaker();
        map.insert(key, breaker.clone());
        breaker
    }

    pub fn open_duration_secs(&self) -> u64 {
        self.config.open_duration_secs
    }

    /// Remove registry membership for addresses absent from the published table.
    pub(crate) fn evict_missing(&self, active: &HashSet<Arc<str>>) {
        self.inner
            .lock()
            .unwrap()
            .retain(|key, _| active.contains(key));
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn resolver_calls(&self) -> usize {
        self.resolver_calls.load(Ordering::Relaxed)
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn reset_resolver_calls(&self) {
        self.resolver_calls.store(0, Ordering::Relaxed);
    }

    fn build_breaker(&self) -> EngineBreaker {
        Config::new()
            .failure_policy(failure_policy::consecutive_failures(
                self.config.failure_threshold,
                backoff::constant(Duration::from_secs(self.config.open_duration_secs)),
            ))
            .build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_reuses_borrowed_key_and_eviction_resets_membership() {
        let registry = CircuitBreakerRegistry::new(CircuitBreakerConfig {
            failure_threshold: 1,
            open_duration_secs: 30,
        });
        let old = registry.resolve("http://engine");
        old.on_error();
        assert!(!registry.resolve("http://engine").is_call_permitted());

        registry.evict_missing(&HashSet::new());
        let fresh = registry.resolve("http://engine");
        assert!(fresh.is_call_permitted());
        assert!(!old.is_call_permitted());
    }
}
