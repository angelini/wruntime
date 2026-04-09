# Plan: Request Handling Resilience

Prevents a single slow module from cascading into cluster-wide exhaustion. Four changes to proxy and engine, ordered by implementation dependency.

**Context:** small payloads, high fan-out. Body streaming is not a concern — the bottleneck is connection and slot exhaustion when a downstream module is slow.

---

## Step 1 — Proxy-level request timeout (`wr-proxy`)

Today the proxy forwards to engines with no deadline. If a module is slow, the proxy holds the connection open for the engine's full `request_timeout` (30s default). Every caller blocked on that module is also holding its own semaphore permit and channel slot.

### 1a. Add `request_timeout_secs` to `ProxyConfig` (`wr-proxy/src/config.rs`)

```rust
#[derive(Deserialize, Clone)]
pub struct ProxyConfig {
    // ...existing fields...
    /// Maximum time (seconds) the proxy will wait for an engine response.
    /// Should be shorter than the engine's request_timeout to free caller
    /// resources before the engine gives up internally.
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,
}

fn default_request_timeout_secs() -> u64 {
    15
}
```

Add a validation check: `v.check(self.request_timeout_secs > 0, "request_timeout_secs must be > 0")`.

### 1b. Wrap the forward call in `ForwardService` (`wr-proxy/src/layers/forward.rs`)

The timeout wraps the `client.request()` call. On expiry, record a circuit breaker failure and return 504.

```rust
// ForwardService gains a timeout field:
pub struct ForwardService {
    pool: HttpClientPool<ProxyBody>,
    mtls_pool: wr_common::tls::HttpsClientPool<ProxyBody>,
    cb_registry: Arc<CircuitBreakerRegistry>,
    request_timeout: Duration,
}

// In call(), wrap the result future (currently lines 109-122):
let result = tokio::time::timeout(
    self.request_timeout,
    async {
        match &destination {
            Destination::LocalEngine(_) => local_client
                .request(forward_req)
                .await
                .map_err(|e| anyhow::anyhow!("forward failed: {e}")),
            Destination::RemoteProxy(_) => mtls_client
                .request(forward_req)
                .await
                .map_err(|e| anyhow::anyhow!("forward failed: {e}")),
        }
    }
    .instrument(span.clone()),
)
.await;

match result {
    Ok(Ok(resp)) => {
        // ...existing status recording + circuit breaker logic (lines 124-155)...
    }
    Ok(Err(e)) => {
        cb.on_error();
        span.record("otel.status_code", "ERROR");
        Ok(super::error_response(
            http::StatusCode::SERVICE_UNAVAILABLE,
            &format!("forward failed: {e}"),
        ))
    }
    Err(_elapsed) => {
        cb.on_error();
        span.record("otel.status_code", "ERROR");
        Ok(super::error_response(
            http::StatusCode::GATEWAY_TIMEOUT,
            "engine did not respond in time",
        ))
    }
}
```

### 1c. Wire timeout into `ForwardService::new`

Pass `Duration::from_secs(config.request_timeout_secs)` from `main.rs` when constructing the service.

### 1d. Example config (`examples/config/proxy.toml`)

```toml
request_timeout_secs = 15
```

---

## Step 2 — Queue-depth-aware routing via heartbeats

Replace blind round-robin with load-aware routing. Engines report per-module queue depth in heartbeats; the proxy uses it to prefer less-loaded engines.

### 2a. Extend the protobuf heartbeat message (`proto/wruntime.proto`)

Add a `queue_depth` field to `ModuleDescriptor` (or a new nested message in `HeartbeatRequest`):

```protobuf
message ModuleQueueStatus {
  string module_name = 1;
  string namespace   = 2;
  string version     = 3;
  uint32 queue_depth = 4;  // current mpsc channel len()
  uint32 capacity    = 5;  // channel_capacity from config
}

message HeartbeatRequest {
  string                      engine_id       = 1;
  repeated ModuleDescriptor   healthy_modules = 2;
  repeated ModuleQueueStatus  queue_status    = 3;
}
```

### 2b. Report queue depth from engine (`wr-engine/src/engine.rs`)

The engine already has access to the mpsc `Sender` for each module. `tokio::sync::mpsc::Sender` does not expose `len()`, so track queue depth with a shared `Arc<AtomicUsize>` that is incremented on send and decremented on receive:

```rust
pub struct QueueDepth(pub Arc<AtomicUsize>);

// In spawn_module(), when creating the channel:
let depth = Arc::new(AtomicUsize::new(0));

// Wrap the sender to track depth:
// - increment in server.rs after successful try_send
// - decrement in http_handler_task after rx.recv()
```

Collect `QueueDepth` values from all modules during the heartbeat loop and populate `queue_status` in the `HeartbeatRequest`.

### 2c. Propagate queue depth through manager to routing table (`wr-manager`)

Two options:
- **Option A:** Manager stores queue depth per-rule and includes it in `RoutingTable`. Simple, but adds a field to every routing rule.
- **Option B:** Proxy receives queue depth directly from engines via the `NodeService` control plane (engines already connect to proxy). Avoids the manager hop entirely.

**Choose Option B** — the proxy already has a gRPC control plane (`control_address`) and engines report heartbeats through it. The proxy's `NodeService` (`wr-proxy/src/node_service.rs`) can store queue depth per engine/module and expose it to the routing layer.

### 2d. Store engine load in proxy (`wr-proxy/src/node_service.rs`)

```rust
/// Per-engine queue depth, updated on each heartbeat.
pub struct EngineLoad {
    /// Map from (namespace, module, version) -> queue_depth
    pub modules: HashMap<(String, String, String), u32>,
    pub last_updated: Instant,
}

/// Shared across NodeService (writer) and RoutingService (reader).
pub type LoadIndex = Arc<RwLock<HashMap<String, EngineLoad>>>;
```

Update the `LoadIndex` in the existing `Heartbeat` RPC handler.

### 2e. Use load in routing selection (`wr-proxy/src/layers/routing.rs`)

Replace `select_round_robin` with `select_least_loaded`:

```rust
fn select_least_loaded(
    load_index: &LoadIndex,
    candidates: &[VersionedCandidate],
    counters: &RoundRobinCounters,
    key: (Arc<str>, Arc<str>, Arc<str>),
) -> VersionedCandidate {
    let loads = load_index.blocking_read();

    // Find the candidate with the lowest queue depth.
    // Fall back to round-robin when load data is unavailable.
    let best = candidates.iter().min_by_key(|c| {
        let addr = c.dest.address();
        loads
            .get(addr)
            .and_then(|el| {
                el.modules
                    .get(&(key.0.to_string(), key.1.to_string(), key.2.to_string()))
                    .copied()
            })
            .unwrap_or(0)
    });

    match best {
        Some(c) => c.clone(),
        None => select_round_robin(counters, key, candidates),
    }
}
```

Note: `blocking_read()` is acceptable here because the lock is held only for a HashMap lookup. If contention becomes measurable, switch to a lock-free structure (e.g. `arc-swap`).

### 2f. Wire `LoadIndex` into `RoutingLayer`

Pass the `LoadIndex` from `main.rs` into `RoutingLayer::new`, store it in `RoutingService`, and call `select_least_loaded` instead of `select_round_robin` in the `call()` method (currently line 302).

---

## Step 3 — Tune circuit breaker for faster recovery

The current defaults (5 failures, 30s open) are too aggressive for high-fan-out traffic. A 30-second blackout on an engine hosting multiple modules takes out all of them.

### 3a. Reduce defaults (`wr-proxy/src/config.rs`)

```rust
impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            open_duration_secs: 10,  // was 30
        }
    }
}
```

### 3b. Add `success_threshold` for half-open probing

The `failsafe` crate already supports `success_threshold` in its `ConsecutiveFailures` policy. Expose it in config:

```rust
#[derive(Deserialize, Clone)]
pub struct CircuitBreakerConfig {
    pub failure_threshold: u32,
    pub open_duration_secs: u64,
    /// Consecutive successes in half-open state before closing the breaker.
    #[serde(default = "default_success_threshold")]
    pub success_threshold: u32,
}

fn default_success_threshold() -> u32 {
    2
}
```

### 3c. Update `build_breaker` (`wr-proxy/src/circuit_breaker.rs`)

```rust
fn build_breaker(&self) -> EngineBreaker {
    Config::new()
        .failure_policy(failure_policy::consecutive_failures(
            self.config.failure_threshold,
            backoff::constant(Duration::from_secs(self.config.open_duration_secs)),
        ))
        .success_policy(success_policy::consecutive_successes(
            self.config.success_threshold,
        ))
        .build()
}
```

### 3d. Update example configs and validation

Add `success_threshold` to `examples/config/proxy.toml`:

```toml
[circuit_breaker]
failure_threshold = 5
open_duration_secs = 10
success_threshold = 2
```

Add validation: `v.check(self.circuit_breaker.success_threshold > 0, "circuit_breaker.success_threshold must be > 0")`.

---

## Step 4 — Deadline propagation across hops

When module A calls module B, A's request timeout is ticking. If A has 5s remaining and B takes 25s, B does work that A will never see. Propagate a deadline header so each hop can short-circuit.

### 4a. Define the header

```
x-wr-deadline: 1744200005  (Unix timestamp, seconds)
```

Seconds precision is sufficient. Using a timestamp rather than a duration avoids clock-skew-insensitive relative values that accumulate rounding errors across hops.

### 4b. Inject deadline in engine's WASI HTTP view (`wr-engine/src/engine.rs`)

When the engine intercepts an outbound HTTP call from a WASM module, it knows the module's remaining time budget (from the `tokio::time::timeout` wrapping `dispatch_request`). Inject `x-wr-deadline` before forwarding to the proxy:

```rust
// In the WasiHttpView send_request implementation, before forwarding:
if !request.headers().contains_key("x-wr-deadline") {
    // First hop — set deadline from the request timeout.
    let deadline = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + remaining_timeout.as_secs();
    request.headers_mut().insert(
        "x-wr-deadline",
        HeaderValue::from_str(&deadline.to_string()).unwrap(),
    );
}
// If x-wr-deadline already exists, leave it — the original caller's
// deadline is the authoritative one.
```

### 4c. Check deadline in proxy before forwarding (`wr-proxy/src/layers/forward.rs`)

Before forwarding to an engine, check if the deadline has already passed:

```rust
// At the top of the forwarding logic, after extracting destination:
if let Some(deadline_val) = req.headers().get("x-wr-deadline") {
    if let Ok(deadline) = deadline_val.to_str().unwrap_or("0").parse::<u64>() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        if now >= deadline {
            return Ok(super::error_response(
                http::StatusCode::GATEWAY_TIMEOUT,
                "upstream deadline expired",
            ));
        }
    }
}
```

### 4d. Use deadline as timeout ceiling in engine's `dispatch_request`

When the engine receives a request with `x-wr-deadline`, use the remaining time as the timeout instead of the default `request_timeout_secs`:

```rust
// In http_handler_task, when computing the timeout (currently lines 531-537):
let timeout = if let Some(deadline_val) = request.headers().get("x-wr-deadline") {
    let deadline = deadline_val
        .to_str()
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    if now >= deadline {
        // Already expired — fail immediately.
        Duration::from_secs(0)
    } else {
        Duration::from_secs(deadline - now)
            .min(module.request_timeout)  // never exceed local limit
    }
} else {
    // Existing logic: check x-wr-timeout, fall back to request_timeout
    request
        .headers()
        .get("x-wr-timeout")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(module.request_timeout)
};
```

### 4e. Propagate through proxy transparently

The proxy already passes through unrecognized headers. `x-wr-deadline` needs no special handling in the routing layer — it flows from caller → proxy → engine naturally. The only proxy interaction is the expiry check in step 4c.

---

## Implementation order

1. **Step 1** (proxy timeout) — no dependencies, immediate value. Prevents the proxy from holding connections indefinitely.
2. **Step 3** (circuit breaker tuning) — config-only change, no new code paths. Reduces blast radius of existing failure detection.
3. **Step 4** (deadline propagation) — depends on step 1 conceptually (proxy timeout becomes the initial deadline ceiling for first-hop requests). Prevents wasted work across hops.
4. **Step 2** (queue-depth routing) — largest change, touches proto + engine + proxy. Benefits from steps 1/3/4 already being in place so the system degrades gracefully while load awareness is rolled out.

## Verification

After each step, run `just tidy` and `just ecommerce-inline`. Steps 2 and 4 modify proto and engine, so also run `just test` to catch integration test regressions. The ecommerce example with multiple engine instances (`just ecommerce`) is the best manual test for load-aware routing (step 2).
