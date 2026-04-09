# Namespace-Fair Backpressure & Circuit Breaking

## Context

Currently, one namespace can starve others in two ways:
1. **Proxy**: `ForwardService::poll_ready()` always returns `Ready` — no admission control. One namespace can flood all HTTP/2 streams and tokio tasks.
2. **Engine**: The global `instance_semaphore` (default 1000) is shared across all modules. One namespace can exhaust the entire WASM instance pool.
3. **Circuit breaker**: Keyed by engine address only. If namespace A causes 5xx on an engine, the breaker trips for ALL namespaces on that engine.

## Plan

### 1. Proxy: Per-Namespace Admission Control

New Tower layer `AdmissionLayer` between `TracingLayer` and `RoutingLayer`.

**New file: `wr-proxy/src/layers/admission.rs`**
- `NamespaceLimits` struct holding `default_max_concurrent: usize` and `per_namespace: HashMap<String, usize>` overrides
- Lazily creates `tokio::sync::Semaphore` per source namespace via `DashMap<String, Arc<Semaphore>>`
- Extracts source namespace from `x-wr-source-ns` header; uses `"_external"` for requests without it
- `try_acquire_owned()` — non-blocking. If at capacity → 429 with `Retry-After: 1` and JSON body identifying the namespace
- `OwnedSemaphorePermit` stored in request extensions, dropped when response completes (RAII)

**`wr-proxy/src/config.rs`** — add:
```toml
[namespace_limits]
default_max_concurrent = 200
[namespace_limits.overrides]
ecommerce = 500
```

**`wr-proxy/src/main.rs`** — insert into both internal and external stacks:
```
TracingLayer → AdmissionLayer → RoutingLayer → EgressLayer → ForwardService
```

### 2. Proxy: Namespace-Aware Circuit Breaker

**`wr-proxy/src/circuit_breaker.rs`**
- Change key from `Arc<str>` (engine addr) to `(Arc<str>, Arc<str>)` (engine addr, source namespace)
- `get_or_create(&self, addr: &str, namespace: &str)` — new signature
- `evict_missing` retains entries where the engine address is active (ignores namespace dimension)

**`wr-proxy/src/layers/forward.rs`**
- Extract `x-wr-source-ns` from request headers *before* line 63 (headers get stripped for local engine destinations at line 67-69)
- Pass source namespace to `cb_registry.get_or_create(&forward_addr, &source_ns)`
- Use `"_external"` when header is absent

### 3. Engine: Per-Namespace Instance Quotas

**`wr-engine/src/config.rs`** — add optional `max_instances: Option<u32>` to `ModuleConfig`

**`wr-engine/src/engine.rs`**
- Build `namespace_semaphores: HashMap<String, Arc<Semaphore>>` at startup by summing `max_instances` across modules per namespace
- Validate sum doesn't exceed `total_component_instances` (fail at startup if it does)
- Unconfigured namespaces share the remaining global pool freely (opt-in fairness)
- In `dispatch_request()`: acquire namespace semaphore *first* (if configured), then global semaphore. Namespace timeout → 503 `"namespace instance quota exhausted"`

### 4. Config Examples & Docs

- `examples/config/proxy.toml` — add commented `[namespace_limits]` section
- `examples/config/engine.toml` — add `max_instances` example
- `docs/configuration.md` — document new config sections

## Key Design Decisions

- **`try_acquire` (non-blocking) at proxy**, not `acquire` (blocking). Queuing at the proxy holds connections open — the exact resource exhaustion we're preventing.
- **Source namespace for proxy admission** (the caller causing load). **Destination namespace for engine quotas** (what consumes instance slots).
- **Namespace semaphore acquired before global semaphore** at the engine — prevents reserving a global slot a namespace can't use.
- **DashMap for lazy semaphore creation** — avoids needing to know all namespaces at startup. New dependency: `dashmap`.

## Files to Modify

| File | Change |
|------|--------|
| `wr-proxy/src/layers/admission.rs` | **New** — AdmissionLayer + NamespaceLimits |
| `wr-proxy/src/layers/mod.rs` | Export AdmissionLayer |
| `wr-proxy/src/main.rs` | Insert AdmissionLayer in service stacks |
| `wr-proxy/src/config.rs` | Add NamespaceLimitsConfig |
| `wr-proxy/src/circuit_breaker.rs` | Composite key (addr, namespace) |
| `wr-proxy/src/layers/forward.rs` | Extract source-ns, pass to CB |
| `wr-proxy/Cargo.toml` | Add dashmap dependency |
| `wr-engine/src/config.rs` | Add max_instances to ModuleConfig |
| `wr-engine/src/engine.rs` | Per-namespace semaphores |
| `examples/config/proxy.toml` | Namespace limits example |
| `examples/config/engine.toml` | max_instances example |
| `docs/configuration.md` | Document new settings |

## Verification

1. `just tidy` — formatting and lints pass
2. `just test` — existing tests pass
3. `just ecommerce-inline` — zero WARN lines, end-to-end works
4. Manual: configure `[namespace_limits]` with a low `default_max_concurrent`, send concurrent requests from one namespace, verify 429s fire before saturating the proxy
