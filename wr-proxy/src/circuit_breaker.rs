use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use failsafe::{backoff, failure_policy, Config, StateMachine};

use crate::config::CircuitBreakerConfig;

pub type EngineBreaker = StateMachine<failure_policy::ConsecutiveFailures<backoff::Constant>, ()>;

#[derive(Clone)]
pub struct CircuitBreakerRegistry {
    inner: Arc<Mutex<HashMap<String, EngineBreaker>>>,
    config: CircuitBreakerConfig,
}

impl CircuitBreakerRegistry {
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            config,
        }
    }

    /// Returns the breaker for `addr`, creating one in the Closed state on first access.
    pub fn get_or_create(&self, addr: &str) -> EngineBreaker {
        let mut map = self.inner.lock().unwrap();
        map.entry(addr.to_string())
            .or_insert_with(|| self.build_breaker())
            .clone()
    }

    pub fn open_duration_secs(&self) -> u64 {
        self.config.open_duration_secs
    }

    /// Removes breakers for addresses no longer present in the routing table.
    pub fn evict_missing(&self, active: &HashSet<&str>) {
        self.inner
            .lock()
            .unwrap()
            .retain(|k: &String, _| active.contains(k.as_str()));
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
