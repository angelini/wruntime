use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use failsafe::{backoff, failure_policy, Config, Instrument, StateMachine};

use crate::config::CircuitBreakerConfig;

const CLOSED: u8 = 0;
const OPEN: u8 = 1;
const HALF_OPEN: u8 = 2;

type InnerBreaker =
    StateMachine<failure_policy::ConsecutiveFailures<backoff::Constant>, BreakerTelemetry>;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum BreakerTargetClass {
    LocalEngine,
    RemoteProxy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BreakerState {
    Closed,
    Open,
    HalfOpen,
}

#[derive(Clone, Debug)]
struct BreakerTelemetry {
    state: Arc<AtomicU8>,
    open_until: Arc<Mutex<Option<Instant>>>,
    open_duration: Duration,
}

impl BreakerTelemetry {
    fn new(open_duration: Duration) -> Self {
        Self {
            state: Arc::new(AtomicU8::new(CLOSED)),
            open_until: Arc::new(Mutex::new(None)),
            open_duration,
        }
    }

    fn snapshot(&self) -> BreakerState {
        match self.state.load(Ordering::Acquire) {
            OPEN => {
                let deadline_elapsed = self
                    .open_until
                    .lock()
                    .unwrap()
                    .is_some_and(|deadline| Instant::now() >= deadline);
                if deadline_elapsed {
                    BreakerState::HalfOpen
                } else {
                    BreakerState::Open
                }
            }
            HALF_OPEN => BreakerState::HalfOpen,
            _ => BreakerState::Closed,
        }
    }
}

impl Instrument for BreakerTelemetry {
    fn on_call_rejected(&self) {}

    fn on_open(&self) {
        *self.open_until.lock().unwrap() = Some(Instant::now() + self.open_duration);
        self.state.store(OPEN, Ordering::Release);
    }

    fn on_half_open(&self) {
        self.state.store(HALF_OPEN, Ordering::Release);
    }

    fn on_closed(&self) {
        *self.open_until.lock().unwrap() = None;
        self.state.store(CLOSED, Ordering::Release);
    }
}

/// The sole forwarding interface to failsafe, with a read-only telemetry mirror.
struct EngineBreakerInner {
    machine: InnerBreaker,
    telemetry: BreakerTelemetry,
    operation: Mutex<()>,
}

#[derive(Clone)]
pub struct EngineBreaker {
    inner: Arc<EngineBreakerInner>,
}

impl EngineBreaker {
    pub fn is_call_permitted(&self) -> bool {
        let _operation = self.inner.operation.lock().unwrap();
        self.inner.machine.is_call_permitted()
    }

    pub fn on_success(&self) {
        let _operation = self.inner.operation.lock().unwrap();
        self.inner.machine.on_success();
    }

    pub fn on_error(&self) {
        let _operation = self.inner.operation.lock().unwrap();
        self.inner.machine.on_error();
    }

    fn state(&self) -> BreakerState {
        self.inner.telemetry.snapshot()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BreakerCounts {
    pub total: u32,
    pub closed: u32,
    pub open: u32,
    pub half_open: u32,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CircuitBreakerSummary {
    pub local_engine: BreakerCounts,
    pub remote_proxy: BreakerCounts,
}

struct RegistryEntry {
    class: BreakerTargetClass,
    breaker: EngineBreaker,
}

#[derive(Clone)]
pub struct CircuitBreakerRegistry {
    inner: Arc<Mutex<HashMap<Arc<str>, RegistryEntry>>>,
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
    pub(crate) fn resolve(
        &self,
        addr: &str,
        class: BreakerTargetClass,
    ) -> Result<EngineBreaker, BreakerTargetClass> {
        self.resolver_calls.fetch_add(1, Ordering::Relaxed);
        let mut map = self.inner.lock().unwrap();
        if let Some(entry) = map.get(addr) {
            return if entry.class == class {
                Ok(entry.breaker.clone())
            } else {
                Err(entry.class)
            };
        }

        let key: Arc<str> = Arc::from(addr);
        let breaker = self.build_breaker();
        map.insert(
            key,
            RegistryEntry {
                class,
                breaker: breaker.clone(),
            },
        );
        Ok(breaker)
    }

    pub fn open_duration_secs(&self) -> u64 {
        self.config.open_duration_secs
    }

    /// Snapshot aggregate telemetry without asking any breaker for permission.
    pub fn snapshot(&self) -> CircuitBreakerSummary {
        let map = self.inner.lock().unwrap();
        let mut summary = CircuitBreakerSummary::default();
        for entry in map.values() {
            let counts = match entry.class {
                BreakerTargetClass::LocalEngine => &mut summary.local_engine,
                BreakerTargetClass::RemoteProxy => &mut summary.remote_proxy,
            };
            counts.total = counts.total.saturating_add(1);
            match entry.breaker.state() {
                BreakerState::Closed => counts.closed = counts.closed.saturating_add(1),
                BreakerState::Open => counts.open = counts.open.saturating_add(1),
                BreakerState::HalfOpen => counts.half_open = counts.half_open.saturating_add(1),
            }
        }
        summary
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
        build_engine_breaker(
            self.config.failure_threshold,
            Duration::from_secs(self.config.open_duration_secs),
        )
    }
}

fn build_engine_breaker(failure_threshold: u32, open_duration: Duration) -> EngineBreaker {
    let telemetry = BreakerTelemetry::new(open_duration);
    let machine = Config::new()
        .failure_policy(failure_policy::consecutive_failures(
            failure_threshold,
            backoff::constant(open_duration),
        ))
        .instrument(telemetry.clone())
        .build();
    EngineBreaker {
        inner: Arc::new(EngineBreakerInner {
            machine,
            telemetry,
            operation: Mutex::new(()),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrapper_mirrors_transitions_without_snapshot_probes() {
        let registry = CircuitBreakerRegistry::new(CircuitBreakerConfig {
            failure_threshold: 1,
            open_duration_secs: 30,
        });
        let breaker = registry
            .resolve("http://engine", BreakerTargetClass::LocalEngine)
            .unwrap();
        assert!(breaker.is_call_permitted());
        breaker.on_error();
        let first = registry.snapshot();
        let second = registry.snapshot();
        assert_eq!(first, second);
        assert_eq!(first.local_engine.open, 1);
        assert!(!breaker.is_call_permitted());
    }

    #[test]
    fn read_only_snapshot_reflects_elapsed_open_deadline_and_half_open_recovery() {
        let duration = Duration::from_millis(20);
        let wrapper = build_engine_breaker(1, duration);
        let raw = Config::new()
            .failure_policy(failure_policy::consecutive_failures(
                1,
                backoff::constant(duration),
            ))
            .build();

        assert_eq!(wrapper.is_call_permitted(), raw.is_call_permitted());
        wrapper.on_error();
        raw.on_error();
        assert_eq!(wrapper.state(), BreakerState::Open);
        std::thread::sleep(Duration::from_millis(30));

        // Snapshot derives effective half-open state from the monotonic deadline
        // without granting the probe that makes failsafe's lazy transition.
        assert_eq!(wrapper.state(), BreakerState::HalfOpen);
        assert_eq!(wrapper.is_call_permitted(), raw.is_call_permitted());
        assert_eq!(wrapper.state(), BreakerState::HalfOpen);
        wrapper.on_success();
        raw.on_success();
        assert_eq!(wrapper.state(), BreakerState::Closed);
        assert_eq!(wrapper.is_call_permitted(), raw.is_call_permitted());
    }

    #[test]
    fn concurrent_wrapper_operations_preserve_failsafe_parity() {
        const WORKERS: usize = 32;
        let duration = Duration::from_secs(30);
        let wrapper = build_engine_breaker((WORKERS + 1) as u32, duration);
        let raw = Config::new()
            .failure_policy(failure_policy::consecutive_failures(
                (WORKERS + 1) as u32,
                backoff::constant(duration),
            ))
            .build();
        let barrier = Arc::new(std::sync::Barrier::new(WORKERS));
        let mut threads = Vec::new();
        for _ in 0..WORKERS {
            let wrapper = wrapper.clone();
            let raw = raw.clone();
            let barrier = Arc::clone(&barrier);
            threads.push(std::thread::spawn(move || {
                assert_eq!(wrapper.is_call_permitted(), raw.is_call_permitted());
                barrier.wait();
                wrapper.on_error();
                raw.on_error();
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(wrapper.state(), BreakerState::Closed);
        assert_eq!(wrapper.is_call_permitted(), raw.is_call_permitted());
        wrapper.on_error();
        raw.on_error();
        assert_eq!(wrapper.state(), BreakerState::Open);
        assert_eq!(wrapper.is_call_permitted(), raw.is_call_permitted());
    }

    #[test]
    fn resolve_reuses_classified_key_rejects_ambiguity_and_eviction_resets_membership() {
        let registry = CircuitBreakerRegistry::new(CircuitBreakerConfig {
            failure_threshold: 1,
            open_duration_secs: 30,
        });
        let old = registry
            .resolve("http://engine", BreakerTargetClass::LocalEngine)
            .unwrap();
        old.on_error();
        assert!(registry
            .resolve("http://engine", BreakerTargetClass::RemoteProxy)
            .is_err());
        assert!(!registry
            .resolve("http://engine", BreakerTargetClass::LocalEngine)
            .unwrap()
            .is_call_permitted());

        registry.evict_missing(&HashSet::new());
        let fresh = registry
            .resolve("http://engine", BreakerTargetClass::RemoteProxy)
            .unwrap();
        assert!(fresh.is_call_permitted());
        assert_eq!(registry.snapshot().remote_proxy.closed, 1);
        assert!(!old.is_call_permitted());
    }
}
