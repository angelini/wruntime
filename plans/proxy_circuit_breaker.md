# Plan: Per-Destination Circuit Breaker in `wr-proxy`

Adds a `failsafe`-backed circuit breaker to `wr-proxy` to protect downstream `wr-engine` instances from receiving traffic when they are overloaded or unhealthy. Breakers are maintained per engine address so a degraded engine does not affect routing to healthy ones.

---

## Step 1 — Add dependency (`wr-proxy/Cargo.toml`)

```toml
failsafe = { version = "1", features = ["futures"] }
```

---

## Step 2 — Add `CircuitBreakerConfig` to proxy config (`src/config.rs`)

```rust
#[derive(Deserialize, Clone)]
pub struct CircuitBreakerConfig {
    pub failure_threshold: u32,   // consecutive failures before opening
    pub success_threshold: u32,   // consecutive successes to close from half-open
    pub open_duration_secs: u64,  // how long the breaker stays open
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self { failure_threshold: 5, success_threshold: 2, open_duration_secs: 30 }
    }
}
```

Add to `ProxyConfig`:
```rust
#[serde(default)]
pub circuit_breaker: CircuitBreakerConfig,
```

TOML shape (`examples/config/proxy.toml`):
```toml
[circuit_breaker]
failure_threshold  = 5
success_threshold  = 2
open_duration_secs = 30
```

The block is optional — defaults apply when omitted.

---

## Step 3 — New file: `src/circuit_breaker.rs`

A `CircuitBreakerRegistry` keyed by engine address (`String`), backed by `Arc<Mutex<HashMap<String, CircuitBreaker>>>`.

- `get_or_create(addr)` — returns (or lazily creates) a breaker for that address
- `evict_missing(active_addrs)` — removes stale entries when engines drop from the routing table

The `failsafe` circuit breaker is `Clone` — the registry returns a clone per call, and the internal `Arc` state is shared. The `Mutex` is held only for the map lookup, not across the async request.

```rust
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use failsafe::{Config, CircuitBreaker, backoff, failure_policy};

pub type EngineBreaker = CircuitBreaker<failure_policy::ConsecutiveFailures<backoff::Constant>>;

#[derive(Clone)]
pub struct CircuitBreakerRegistry {
    inner: Arc<Mutex<HashMap<String, EngineBreaker>>>,
    config: crate::config::CircuitBreakerConfig,
}

impl CircuitBreakerRegistry {
    pub fn new(config: crate::config::CircuitBreakerConfig) -> Self {
        Self { inner: Arc::new(Mutex::new(HashMap::new())), config }
    }

    pub fn get_or_create(&self, addr: &str) -> EngineBreaker {
        let mut map = self.inner.lock().unwrap();
        map.entry(addr.to_string())
            .or_insert_with(|| self.build_breaker())
            .clone()
    }

    pub fn evict_missing(&self, active: &std::collections::HashSet<&str>) {
        self.inner.lock().unwrap().retain(|k, _| active.contains(k.as_str()));
    }

    fn build_breaker(&self) -> EngineBreaker {
        Config::new()
            .failure_policy(failure_policy::consecutive_failures(
                self.config.failure_threshold,
                backoff::constant(Duration::from_secs(self.config.open_duration_secs)),
            ))
            .success_threshold(self.config.success_threshold)
            .build()
    }
}
```

---

## Step 4 — Wrap each attempt in `ForwardService` (`src/layers/forward.rs`)

Add the registry to `ForwardService`. Inside the candidate loop, each per-engine attempt is gated through `cb.call(async { ... })`.

**Failure classification:**

| Signal | Breaker |
|---|---|
| Network / connect error | Failure |
| 503 Service Unavailable | Failure |
| 429 Too Many Requests | Failure |
| Other 5xx | Failure |
| 2xx, 3xx, 4xx (client errors) | Success |

```rust
let cb = cb_registry.get_or_create(&forward_addr);
match cb.call(async {
    let resp = client.request(...).await?;
    match resp.status() {
        s if s.is_server_error() || s == StatusCode::TOO_MANY_REQUESTS => {
            Err(anyhow::anyhow!("engine overload: {}", s))
        }
        _ => Ok(resp),
    }
}).await {
    Err(failsafe::Error::Rejected) => {
        tracing::warn!(engine = %forward_addr, "circuit open, skipping candidate");
        continue; // skip without a real request
    }
    Err(failsafe::Error::Inner(_)) => continue, // real error, already recorded
    Ok(resp) => return Ok(resp),
}
```

When all candidates are skipped (all circuits open), the caller receives `503 Service Unavailable`.

---

## Step 5 — Stale entry cleanup in `sync_routing_table` (`src/routing.rs`)

After writing a new routing table, collect the active engine addresses and call `evict_missing`. Pass the registry as a new parameter.

```rust
pub async fn sync_routing_table(
    mut client: ManagerServiceClient<...>,
    table: CachedRoutingTable,
    ttl_secs: u64,
    schema_trigger: Arc<Notify>,
    cb_registry: Arc<CircuitBreakerRegistry>,  // new
) {
    // ... existing sync logic ...
    let active: HashSet<&str> = new_table.iter()
        .flat_map(|rule| rule.engine_addresses.iter().map(String::as_str))
        .collect();
    cb_registry.evict_missing(&active);
}
```

An in-flight `ForwardService` clone may hold a cloned `EngineBreaker` at eviction time — that is safe because the breaker's internal state is `Arc`-backed and continues to function for the in-flight request. If the address later reappears, `get_or_create` produces a fresh breaker in the Closed state.

---

## Step 6 — Wire through `main.rs`

```rust
let cb_registry = Arc::new(CircuitBreakerRegistry::new(config.circuit_breaker.clone()));

// Both internal + external stacks share the same registry —
// breaker state is about the downstream engine health, not the listener.
let svc = ForwardService::new(cb_registry.clone());

// Pass to sync task:
tokio::spawn(sync_routing_table(..., cb_registry.clone()));
```

Sharing one registry across both stacks ensures a trip on the internal listener also protects the external listener, and vice versa.

---

## Step 7 — Export module (`src/lib.rs`)

```rust
pub mod circuit_breaker;
```

---

## File changes summary

| File | Change |
|---|---|
| `wr-proxy/Cargo.toml` | Add `failsafe = { version = "1", features = ["futures"] }` |
| `src/config.rs` | Add `CircuitBreakerConfig`, wire into `ProxyConfig` with `#[serde(default)]` |
| `src/circuit_breaker.rs` | New: `CircuitBreakerRegistry` with `get_or_create` and `evict_missing` |
| `src/lib.rs` | Add `pub mod circuit_breaker` |
| `src/layers/forward.rs` | Carry `Arc<CircuitBreakerRegistry>`, wrap attempts in `cb.call()`, classify failures |
| `src/routing.rs` | Accept registry param, call `evict_missing` after table update |
| `src/main.rs` | Construct registry from config, pass to `ForwardService` and `sync_routing_table` |
| `examples/config/proxy.toml` | Add optional `[circuit_breaker]` section |
