# Plan: Namespaces

Add a mandatory namespace to every module identity. Inter-module HTTP calls use the format `http://{serviceName}.{namespaceName}/path`. The manager treats `(namespace, name, version)` as the unique module key, allowing two services with the same name to coexist in different namespaces.

---

## 1. Proto (`proto/wruntime.proto`)

Add `namespace` to every message that identifies a module.

```protobuf
message ModuleDescriptor {
  string name      = 1;
  string version   = 2;
  string namespace = 3;  // ADD — mandatory, rejected empty by manager
  bytes  proto_schema = 4;
}

message RoutingRule {
  string rule_id             = 1;
  string source_module       = 2;
  string source_namespace    = 3;  // ADD
  string destination_module  = 4;
  string destination_namespace = 5;  // ADD
  string engine_id           = 6;
  string engine_address      = 7;
  string destination_version = 8;
  bool   healthy             = 9;
}

// GetSchema / UploadSchema requests — add namespace field
message GetSchemaRequest {
  string module_name = 1;
  string version     = 2;
  string namespace   = 3;  // ADD
}

message UploadSchemaRequest {
  string module_name = 1;
  string version     = 2;
  string namespace   = 3;  // ADD
  bytes  schema      = 4;
}

// HeartbeatRequest healthy_modules already uses ModuleDescriptor, so namespace is covered.
```

Bump field numbers appropriately; regenerate `wr-common` via `tonic-build`.

---

## 2. Manager state (`wr-manager/src/state.rs`)

Change every keyed collection that uses module identity:

| Before | After |
|--------|-------|
| `schemas: HashMap<(String, String), Vec<u8>>` | `schemas: HashMap<(String, String, String), Vec<u8>>` — key: `(namespace, name, version)` |
| `module_health: HashMap<(String, String, String), Instant>` | key: `(engine_id, namespace, name, version)` |

The `routing_table` stores `RoutingRule` which now carries namespace fields — no structural change to the table itself.

**Validation in `RegisterEngine` handler (`wr-manager/src/service.rs`):** reject any `ModuleDescriptor` with an empty `namespace` with `Status::invalid_argument`.

The heartbeat monitor background task (`ManagerState::start_heartbeat_monitor`) must update its health-check lookup key to include namespace.

---

## 3. Engine config (`wr-engine/src/config.rs`)

Add `namespace` to `ModuleConfig`. No engine-level namespace — a single engine can host modules from different namespaces.

```toml
# engine.toml
[[module]]
name        = "order-service"
namespace   = "payments"          # ADD — required field
version     = "1.0.0"
wasm_path   = "modules/order_service.wasm"
schema_path = "schemas/order_service.binpb"
database    = true
```

```rust
pub struct ModuleConfig {
    pub name:        String,
    pub namespace:   String,   // ADD
    pub version:     String,
    pub wasm_path:   String,
    pub schema_path: Option<String>,
    pub database:    bool,
}
```

Config loading should error if `namespace` is empty or missing.

---

## 4. Engine — module state and HTTP interception (`wr-engine/src/state.rs`)

`ModuleState` stores the module's own namespace so it can set the correct source header and parse outbound destinations.

```rust
pub struct ModuleState {
    pub module_name:      String,
    pub module_namespace: String,   // ADD
    pub proxy_uri:        Uri,
    // ...
}
```

**`send_request` rewrite logic:**

The WASM module calls `http://inventory-service.payments/items`. The host is `{service}.{namespace}`.

```
Original URI:  http://inventory-service.payments/items
  ↓
x-wr-destination: http://inventory-service.payments/items   (unchanged — full URI preserved)
x-wr-source:      order-service                             (unchanged)
x-wr-source-ns:   payments                                  (ADD — source namespace)
Rewrite URI →  http://{proxy_address}/items
```

No change to how the URI path is extracted; the proxy reads namespace from the `x-wr-destination` host field.

---

## 5. Proxy routing layer (`wr-proxy/src/layers/routing.rs`)

Parse the namespace out of `x-wr-destination`:

```rust
// host = "inventory-service.payments"
let (module_name, namespace) = host
    .split_once('.')
    .ok_or(/* 400 Bad Request — no namespace in destination */)?;
```

Return a `400 Bad Request` with a structured JSON error body if the host has no `.` separator (enforces the mandatory namespace requirement at the network boundary).

Filter routing rules by **both** `destination_module == module_name` AND `destination_namespace == namespace`.

Inject a new `x-wr-namespace` header so the engine can dispatch correctly:

```
x-wr-module:    inventory-service   (existing)
x-wr-version:   1.0.0               (existing)
x-wr-namespace: payments            (ADD)
```

The round-robin counter key becomes `(namespace, module_name, version)`.

---

## 6. Engine inbound server (`wr-engine/src/server.rs`)

Read the new `x-wr-namespace` header alongside `x-wr-module` and `x-wr-version`. Return `400` if it is missing.

Pass namespace into the registry lookup:

```rust
let instance = registry.get(&(namespace, module_name, version))?;
```

---

## 7. Module registry (`wr-engine/src/registry.rs`)

Change the registry key from `(name, version)` to `(namespace, name, version)`:

```rust
pub struct ModuleRegistry {
    modules: HashMap<(String, String, String), mpsc::Sender<InboundRequest>>,
    //              (namespace, name, version)
}
```

---

## 8. Engine registration (`wr-engine/src/main.rs`)

Pass `namespace` when building `ModuleDescriptor` for each module. Pass `namespace` when constructing `ModuleState`. No other structural changes.

---

## 9. Schema cache (`wr-proxy/src/schema.rs`)

Cache key changes from `(name, version)` to `(namespace, name, version)`. The `GetSchema` gRPC call already carries namespace after the proto change.

---

## 10. Integration tests (`wr-tests/tests/integration_test.rs`)

All test module configs must add a `namespace` field. Test cases to add or update:

- Two modules with the same name in different namespaces are independently routable.
- A request to `http://svc.ns-a/` does not route to a module registered under `ns-b`, even if names match.
- A request with a host that has no `.` separator returns `400` from the proxy.
- Schema validation is namespace-scoped (uploading a schema for `svc@ns-a` does not affect `svc@ns-b`).

---

## Change surface summary

| File | Change |
|------|--------|
| `proto/wruntime.proto` | Add `namespace` to `ModuleDescriptor`, `RoutingRule`, schema RPCs |
| `wr-common/src/lib.rs` | Regenerated (no manual edits) |
| `wr-manager/src/state.rs` | Namespace in `schemas` and `module_health` keys |
| `wr-manager/src/service.rs` | Validate non-empty namespace; update rule upsert/lookup |
| `wr-engine/src/config.rs` | Add `namespace: String` to `ModuleConfig` |
| `wr-engine/src/state.rs` | Add `module_namespace` to `ModuleState`; add `x-wr-source-ns` header |
| `wr-engine/src/main.rs` | Pass namespace into `ModuleDescriptor` and `ModuleState` |
| `wr-engine/src/registry.rs` | Key: `(namespace, name, version)` |
| `wr-engine/src/server.rs` | Read `x-wr-namespace` header; pass to registry |
| `wr-proxy/src/layers/routing.rs` | Parse namespace from host; filter + inject `x-wr-namespace` |
| `wr-proxy/src/schema.rs` | Key: `(namespace, name, version)` |
| `wr-tests/tests/integration_test.rs` | Add `namespace` to all configs; new namespace-isolation tests |
| `engine.toml` (example) | Add `namespace` to each `[[module]]` |
