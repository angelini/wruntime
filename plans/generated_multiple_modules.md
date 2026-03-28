# Execution Plan: Multiple Module Instances & Versions

## Overview

This plan enables `wr-engine` to run multiple instances of the same module (same name, same or different version) with load balancing and health-aware routing in `wr-proxy`.

---

## Phase 1 — Version-Aware Routing Table

**Goal:** Make versions a first-class concept in the routing table so `wr-proxy` can route by version.

### 1.1 Update `proto/wruntime.proto`

Extend `RoutingRule` to carry the module version alongside its name:

```proto
message RoutingRule {
  string rule_id        = 1;
  string source_module  = 2;
  string destination_module  = 3;
  string destination_version = 4;  // add: semver string, e.g. "1.2.0"
  string engine_id      = 5;
  string engine_address = 6;
}
```

Regenerate `wr-common` via `cargo build -p wr-common`.

### 1.2 Update `wr-manager/src/service.rs`

- `upsert_routing_rule`: treat `(destination_module, destination_version, engine_id)` as the composite key instead of `rule_id` alone, so the same module at different versions or on different engines gets distinct rules.

### 1.3 Update `wr-proxy/src/layers/routing.rs`

- Read optional `x-wr-version` request header.
- If header is present: match `RoutingRule` on both `destination_module` **and** `destination_version`; return `503` if no matching rule exists.
- If header is absent: among all rules with the matching `destination_module`, pick the rule with the highest semver `destination_version` (latest wins).
- Store resolved `(engine_address, module_name, module_version)` in request extensions.

### 1.4 Update `wr-proxy/src/layers/forward.rs`

- Inject `x-wr-version` header (resolved version) before forwarding, so the engine knows which version was requested.

---

## Phase 2 — Multiple Instances & Load Balancing

**Goal:** Allow many running copies of `(module_name, module_version)` and spread traffic across them.

### 2.1 Update `wr-engine/src/registry.rs`

Replace the single `ModuleTx` per module name with a list of senders keyed by `(name, version)`:

```rust
// Before
HashMap<String, ModuleTx>

// After
HashMap<(String, String), Vec<ModuleTx>>  // (name, version) → instances
```

Add a `next_sender(name, version)` method that returns senders in round-robin order (use an `AtomicUsize` counter per entry).

### 2.2 Update `wr-engine/src/engine.rs`

- `spawn_module`: register the module channel under `(name, version)` rather than `name` alone.
- Allow multiple `spawn_module` calls for the same `(name, version)` — each appends a new sender to the registry list.

### 2.3 Update `wr-engine/src/server.rs`

- Read `x-wr-version` from inbound request headers.
- Use `registry.next_sender(module_name, version)` to dispatch; return `503` if no sender found.

### 2.4 Update `wr-engine/src/config.rs`

Allow the same module to be declared multiple times in `engine.toml` (e.g., two workers for the same version):

```toml
[[module]]
name     = "order-service"
version  = "1.0.0"
wasm_path = "modules/order_service.wasm"

[[module]]
name     = "order-service"
version  = "1.0.0"
wasm_path = "modules/order_service.wasm"
```

Remove any deduplication logic that currently rejects duplicate `(name, version)` pairs.

---

## Phase 3 — Health Monitoring & Failover

**Goal:** Remove unhealthy instances from routing automatically.

### 3.1 Extend heartbeat with per-module health

Update `EngineRegistration` / `Heartbeat` proto message to include a list of currently healthy module names+versions:

```proto
message Heartbeat {
  string engine_id = 1;
  repeated ModuleDescriptor healthy_modules = 2;  // add
}
```

### 3.2 Update `wr-manager/src/state.rs`

Track health per `(engine_id, module_name, module_version)`:

```rust
module_health: HashMap<(String, String, String), Instant>
//              (engine_id, module_name, version)  → last_healthy_at
```

Update `monitor_heartbeats` to mark routing rules as unhealthy when their module's last healthy timestamp exceeds the timeout threshold.

### 3.3 Add a "healthy rules" filter to `wr-proxy/src/routing.rs`

- Expose a `healthy: bool` flag on `RoutingRule` (or filter unhealthy rules out of the cached table entirely on sync).
- The routing layer's candidate selection (Phase 1.3) must only consider healthy rules.
- If all rules for a `(module, version)` are unhealthy, return `503`.

### 3.4 Update `wr-proxy/src/routing.rs` sync loop

After fetching the routing table, strip or mark rules whose `(engine_id, module_name, version)` trio is flagged unhealthy by the manager.

---

## Phase 4 — Integration Tests

**Goal:** Verify all new behaviour end-to-end in `wr-tests`.

| Test | What it covers |
|---|---|
| Route to explicit version via `x-wr-version` header | Phase 1 routing |
| Route to latest version when no header is set | Phase 1 default |
| `503` when requested version has no running instance | Phase 1 error path |
| Requests distributed across two instances of same version | Phase 2 load balancing |
| Traffic shifts to remaining instance when one is deregistered | Phase 3 failover |
| `503` when all instances of a module are unhealthy | Phase 3 full failure |

Add test helpers in `wr-tests/src/lib.rs` to:
- Spin up multiple engine instances on separate ephemeral ports.
- Register multiple rules for the same `(module, version)` across those engines.
- Simulate a failed heartbeat by stopping heartbeats from one engine instance.

---

## Dependency Order

```
Phase 1 (proto + routing) → Phase 2 (engine registry) → Phase 3 (health) → Phase 4 (tests)
```

Phase 2 can begin in parallel with Phase 1 once the proto change is merged, since the engine-side registry change is independent of the proxy routing logic.
