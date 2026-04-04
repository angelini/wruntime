# Cross-Node Proxy Routing

Engines route to their local proxy. Proxies forward to peer proxies on other nodes when
the destination module lives on a different node. A single manager remains the central
registry. Multiple nodes can be simulated on one machine for local development and
integration testing.

---

## Model: Node

A **node** is the unit of co-location: one proxy plus N engines running on the same host.
The proxy's externally-reachable address is the node's identity — every engine on the node
points to that address; every peer proxy forwards cross-node traffic to that address.

```
Node A                          Node B
┌────────────────────┐          ┌────────────────────┐
│  proxy :9001       │◄────────►│  proxy :9002       │
│  engine A1 :9100   │          │  engine B1 :9200   │
│  engine A2 :9101   │          │  engine B2 :9201   │
└────────────────────┘          └────────────────────┘
         │                               │
         └──────────► manager :9000 ◄───┘
```

Routing decision inside a proxy:

```
rule.proxy_address == self.proxy_address  →  forward to rule.engine_address  (local)
rule.proxy_address != self.proxy_address  →  forward to rule.proxy_address   (remote)
```

---

## Phase 1 — Node config model

Add a `[node]` section to `EngineConfig` and `ProxyConfig`. The only required field is
`proxy_address`: the fully-qualified HTTP address of the local proxy, as reachable by
peer proxies.

### `engine.toml`

```toml
listen_address  = "0.0.0.0:9100"
manager_address = "http://127.0.0.1:9000"

[node]
proxy_address = "http://127.0.0.1:9001"   # local proxy; used to rewrite WASM outbound calls
                                           # and sent to the manager on registration

[[module]]
# ...
```

### `proxy.toml`

```toml
listen_address  = "0.0.0.0:9001"
manager_address = "http://127.0.0.1:9000"

[node]
proxy_address = "http://127.0.0.1:9001"   # this proxy's own address, as reachable by peers

[cache]
# ...
```

### Code changes

- **`wr-engine/src/config.rs`** — add `node: NodeConfig` field to `EngineConfig`.
  Remove the top-level `proxy_address` field (it moves into `[node]`).
- **`wr-proxy/src/config.rs`** — add `node: NodeConfig` field to `ProxyConfig`.
- **`wr-common/src/` (new file `node.rs`)** — shared `NodeConfig` struct:

```rust
#[derive(Debug, Deserialize)]
pub struct NodeConfig {
    pub proxy_address: String,
}
```

  Export from `wr-common` so both crates can reference it without duplication.

---

## Phase 2 — Proto: carry `proxy_address` through the registration and routing table

```proto
// proto/wruntime.proto

message EngineRegistration {
  string engine_id      = 1;
  string address        = 2;   // engine's own listen address
  string proxy_address  = 4;   // ADD: local proxy address (node identity)
  repeated ModuleDescriptor modules = 3;
}

message RoutingRule {
  string rule_id               = 1;
  string source_module         = 2;
  string destination_module    = 3;
  string engine_id             = 4;
  string engine_address        = 5;
  string destination_version   = 6;
  bool   healthy               = 7;
  string source_namespace      = 8;
  string destination_namespace = 9;
  string proxy_address         = 10;  // ADD: node proxy address for this engine
}
```

Regenerate gRPC bindings via `wr-common/build.rs` (no code changes needed there —
`tonic-build` picks up the updated proto automatically).

---

## Phase 3 — Engine: send `proxy_address` during registration

**`wr-engine/src/main.rs`**

When building `EngineRegistration`, populate the new field:

```rust
let registration = EngineRegistration {
    engine_id: engine_id.clone(),
    address: config.listen_address.clone(),
    proxy_address: config.node.proxy_address.clone(),  // ADD
    modules: module_descriptors,
};
```

When building each `RoutingRule` in the upsert loop, also carry it:

```rust
RoutingRule {
    // ... existing fields ...
    proxy_address: config.node.proxy_address.clone(),  // ADD
    ..
}
```

No change to the heartbeat loop — it does not touch routing rules.

---

## Phase 4 — Manager: store `proxy_address` in routing rules

**`wr-manager/src/service.rs`** — `upsert_routing_rule` handler

The manager currently overwrites the rule wholesale. It already stores whatever the
engine sends. Because `proxy_address` is now part of the `RoutingRule` proto message,
no logic change is required — the field is stored and returned transparently.

Verify: `GetRoutingTable` returns the full `RoutingRule` struct, so `proxy_address` will
be present in every rule delivered to proxies.

---

## Phase 5 — Proxy routing layer: local vs. remote dispatch

**`wr-proxy/src/main.rs`** — pass `self_proxy_address` from `config.node.proxy_address`
into the routing layer at startup.

**`wr-proxy/src/layers/routing.rs`**

Extend `ResolvedDestination` to carry the hop type:

```rust
pub enum Destination {
    LocalEngine(String),   // forward directly to engine_address
    RemoteProxy(String),   // forward to peer proxy_address
}

pub struct ResolvedDestination(pub Destination);
```

In the rule-selection logic, after round-robin picks a rule:

```rust
let destination = if rule.proxy_address == self_proxy_address {
    Destination::LocalEngine(rule.engine_address.clone())
} else {
    Destination::RemoteProxy(rule.proxy_address.clone())
};
req.extensions_mut().insert(ResolvedDestination(destination));
```

When forwarding to a `RemoteProxy`, the proxy-to-proxy request must reach the peer proxy
with all routing context intact. Inject the same `x-wr-module`, `x-wr-namespace`,
`x-wr-version` headers that would normally be injected for an engine (the peer proxy's
routing layer does not re-run on these — see Phase 6).

---

## Phase 6 — Proxy forward layer: header handling per destination type

**`wr-proxy/src/layers/forward.rs`**

Branch on `ResolvedDestination`:

```rust
match dest {
    Destination::LocalEngine(addr) => {
        // Current behavior: strip x-wr-destination, x-wr-source
        req.headers_mut().remove("x-wr-destination");
        req.headers_mut().remove("x-wr-source");
        forward_to(addr, req).await
    }
    Destination::RemoteProxy(addr) => {
        // Preserve x-wr-destination so the peer proxy can route
        // Mark as a proxy hop to suppress re-validation
        req.headers_mut().insert(
            "x-wr-via-proxy",
            HeaderValue::from_static("1"),
        );
        forward_to(addr, req).await
    }
}
```

**`wr-proxy/src/layers/schema.rs`** (schema validation layer)

Skip validation when the request came from a peer proxy — it was already validated at
the ingress proxy:

```rust
if req.headers().contains_key("x-wr-via-proxy") {
    return self.inner.call(req).await;
}
// ... existing validation logic ...
```

Strip `x-wr-via-proxy` before forwarding to the engine so the header does not leak into
WASM module requests.

---

## Phase 7 — Integration test: multi-node simulation

The existing test helpers already support ephemeral ports on `127.0.0.1`. Running two
proxies on different ports naturally simulates two nodes on one machine.

### New helpers (`wr-tests/tests/helpers.rs`)

```rust
pub struct Node {
    pub proxy_address: String,   // "http://127.0.0.1:{port}"
    pub proxy_shutdown: oneshot::Sender<()>,
}

/// Spin up a proxy that knows its own address and is synced to the manager.
pub async fn start_node(mgr_addr: &str, self_proxy_address: &str) -> Result<Node> { ... }
```

`start_node` passes `self_proxy_address` into the routing layer and starts the proxy on
an ephemeral port. Callers use the returned `proxy_address` when registering engines that
belong to this node.

### `register_module` update

Add a `proxy_address: &str` parameter. This value is embedded in the `RoutingRule` sent
to the manager, so the routing table correctly reflects node membership.

### New integration test: `test_cross_node_routing`

```
1. start_manager()
2. start_node(mgr, "http://127.0.0.1:{portA}") → node_a
3. start_node(mgr, "http://127.0.0.1:{portB}") → node_b
4. spawn_stub_engine() → engine_b_addr
5. register_module(mgr, engine_b_addr, proxy=node_b.proxy_address, module="inventory")
6. sync both nodes' routing tables
7. POST http://node_a.proxy_address/items with x-wr-destination: inventory.store
8. Assert: request arrived at engine_b (not engine_a's proxy directly)
9. Assert: response is 200 with correct body
```

This test does not need real WASM modules — the stub engine echoes the path back.

---

## Phase 8 — Local multi-node Justfile targets

Add config files and Justfile tasks for a two-node local setup:

```
node-a/
  proxy.toml       # listen :9001, node.proxy_address = "http://127.0.0.1:9001"
  engine-1.toml    # listen :9100, node.proxy_address = "http://127.0.0.1:9001"
  engine-2.toml    # listen :9101, node.proxy_address = "http://127.0.0.1:9001"

node-b/
  proxy.toml       # listen :9002, node.proxy_address = "http://127.0.0.1:9002"
  engine-1.toml    # listen :9200, node.proxy_address = "http://127.0.0.1:9002"
```

```justfile
# Start the shared manager
manager:
    cargo run -p wr-manager -- --config manager.toml

# Start node A (proxy + two engines)
node-a-proxy:
    cargo run -p wr-proxy -- --config node-a/proxy.toml
node-a-engine-1:
    cargo run -p wr-engine -- --config node-a/engine-1.toml
node-a-engine-2:
    cargo run -p wr-engine -- --config node-a/engine-2.toml

# Start node B
node-b-proxy:
    cargo run -p wr-proxy -- --config node-b/proxy.toml
node-b-engine-1:
    cargo run -p wr-engine -- --config node-b/engine-1.toml
```

Because `proxy_address` is just a URL, running two proxies on `127.0.0.1:9001` and
`127.0.0.1:9002` is identical to running them on separate physical hosts — the routing
logic uses string comparison on the URL and does not inspect network interfaces.

---

## Implementation order

1. Phase 1 — `NodeConfig` struct, update `EngineConfig` and `ProxyConfig`
2. Phase 2 — proto field additions, regenerate
3. Phase 3 — engine sends `proxy_address`
4. Phase 4 — verify manager stores/returns it (likely no-op)
5. Phase 5 — proxy routing layer local/remote split
6. Phase 6 — forward layer header handling, schema validation skip
7. Phase 7 — test helpers and `test_cross_node_routing`
8. Phase 8 — Justfile and local multi-node config files
