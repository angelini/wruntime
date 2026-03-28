# Wruntime

A distributed WASM module networking runtime. WASM modules running inside **wr-engine** make ordinary HTTP calls to each other; Wruntime intercepts those calls, routes them through **wr-proxy**, validates them against their protobuf schemas, and delivers them to the correct destination engine. A central **wr-manager** holds the routing table, module registry, schemas, and metrics.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  wr-engine A                     wr-engine B                │
│  ┌──────────────────┐            ┌──────────────────┐       │
│  │  order-service   │            │ inventory-service │       │
│  │  (WASM module)   │            │  (WASM module)   │       │
│  └────────┬─────────┘            └────────▲─────────┘       │
│           │ HTTP (intercepted)            │ HTTP             │
│           │ x-wr-source / x-wr-destination│                  │
└───────────┼───────────────────────────────┼─────────────────┘
            │                               │
            ▼                               │
┌───────────────────────┐                  │
│       wr-proxy        │──────────────────┘
│                       │   forwards to engine B
│  MetricsLayer         │
│  SchemaValidationLayer│
│  RoutingLayer         │
│  ForwardService       │
└──────────┬────────────┘
           │ gRPC (routing table, schemas, metrics)
           ▼
┌───────────────────────┐
│      wr-manager       │
│                       │
│  Engine registry      │
│  Routing table        │
│  Schema store         │
│  Metrics buffer       │
└───────────────────────┘
```

### Components

| Binary | Default port | Role |
|--------|-------------|------|
| `wr-manager` | `9000` (gRPC) | Central registry — engines register here, proxies sync routing and schemas from here |
| `wr-proxy` | `9001` (HTTP) | Intercepts and routes inter-module traffic; validates request bodies against protobuf schemas |
| `wr-engine` | `9100` (HTTP) | Loads WASM modules, runs them, and receives forwarded requests |

### Request flow

```
WASM module makes HTTP call to "http://inventory-service/items"
  │
  ▼  [WasiHttpView::send_request intercepts — transparent to the module]
  │  Adds headers:
  │    x-wr-source:      "order-service"
  │    x-wr-destination: "http://inventory-service/items"
  │  Rewrites URI to wr-proxy
  │
  ▼
wr-proxy
  │  1. MetricsLayer       — records start time
  │  2. SchemaValidation   — decodes body with prost-reflect against the
  │                          module's FileDescriptorSet; returns 400 on failure
  │  3. RoutingLayer       — resolves destination engine address from table,
  │                          injects x-wr-module header
  │  4. ForwardService     — strips x-wr-* headers, forwards to engine
  │
  ▼
wr-engine (destination)
  │  Inbound HTTP server reads x-wr-module, dispatches to WASM instance
  │
  ▼
inventory-service WASM module handles the request
```

---

## Prerequisites

| Tool | Purpose |
|------|---------|
| Rust + Cargo (stable) | Build all binaries |
| `protoc` | Compile `.proto` schemas to `FileDescriptorSet` binaries for schema validation (optional at runtime if modules have no schema) |
| `wasm-tools` or `cargo-component` | Build WASM component modules |

Install Rust via [rustup](https://rustup.rs). Install `protoc` via your system package manager or from [github.com/protocolbuffers/protobuf/releases](https://github.com/protocolbuffers/protobuf/releases).

---

## Building

```bash
cargo build --release
```

Binaries are placed in `target/release/`:

```
target/release/wr-manager
target/release/wr-proxy
target/release/wr-engine
```

---

## Running

Start the three components **in order**: manager first, then proxy, then engines.

### 1. wr-manager

```bash
./target/release/wr-manager manager.toml
```

`manager.toml`:

```toml
listen_address                = "0.0.0.0:9000"
engine_heartbeat_timeout_secs = 30
```

### 2. wr-proxy

```bash
./target/release/wr-proxy proxy.toml
```

`proxy.toml`:

```toml
listen_address  = "0.0.0.0:9001"
manager_address = "http://127.0.0.1:9000"

[cache]
routing_table_ttl_secs = 5   # how often to poll the manager for routing updates
schema_ttl_secs        = 60  # how often to sync module schemas

[metrics]
flush_interval_secs = 10
queue_depth         = 1000
```

The proxy connects to the manager at startup, then polls for routing table and schema updates in the background.

### 3. wr-engine

```bash
./target/release/wr-engine engine.toml
```

`engine.toml`:

```toml
listen_address  = "0.0.0.0:9100"
manager_address = "http://127.0.0.1:9000"
proxy_address   = "http://127.0.0.1:9001"

[[module]]
name        = "order-service"
version     = "1.0.0"
wasm_path   = "modules/order_service.wasm"
schema_path = "schemas/order_service.binpb"   # optional

[[module]]
name      = "inventory-service"
version   = "1.0.0"
wasm_path = "modules/inventory_service.wasm"
# schema_path omitted — schema validation skipped for this module
```

On startup the engine:
1. Loads every listed WASM component from disk.
2. Registers itself and its modules with the manager (including schema bytes).
3. Starts an inbound HTTP server on `listen_address`.
4. Sends a heartbeat to the manager every 10 seconds.
5. Deregisters cleanly on `Ctrl+C`.

#### Routing rules

Engines register themselves but do not create routing rules automatically — you create rules via the manager's gRPC API (or a management tool) after the engine is running:

```
# example using grpcurl
grpcurl -plaintext -d '{
  "rule_id": "r1",
  "source_module": "order-service",
  "destination_module": "inventory-service",
  "engine_id": "<engine-uuid>",
  "engine_address": "http://127.0.0.1:9100"
}' 127.0.0.1:9000 wruntime.ManagerService/UpsertRoutingRule
```

---

## gRPC API (`proto/wruntime.proto`)

All inter-service communication uses the `wruntime.ManagerService` gRPC service.

### Engine lifecycle

| RPC | Request | Response | Description |
|-----|---------|----------|-------------|
| `RegisterEngine` | `EngineRegistration` | `{ accepted }` | Engine announces itself and its modules |
| `DeregisterEngine` | `{ engine_id }` | — | Engine removes itself on shutdown |
| `Heartbeat` | `{ engine_id }` | — | Sent every 10 s; manager logs engines that go silent |
| `ListEngines` | — | `[EngineRegistration]` | Returns all currently registered engines |

### Routing table

| RPC | Request | Response | Description |
|-----|---------|----------|-------------|
| `GetRoutingTable` | — | `RoutingTable` | Returns the full versioned table |
| `UpsertRoutingRule` | `RoutingRule` | — | Insert or update a rule by `rule_id` |
| `DeleteRoutingRule` | `{ rule_id }` | — | Remove a rule; increments table version |

A `RoutingRule` has the fields:

```protobuf
message RoutingRule {
  string rule_id            = 1;  // stable identifier for this rule
  string source_module      = 2;  // module that initiates the call
  string destination_module = 3;  // module name used as the HTTP host
  string engine_id          = 4;  // UUID of the destination engine
  string engine_address     = 5;  // HTTP base URL of the destination engine
}
```

### Schemas

| RPC | Description |
|-----|-------------|
| `UploadSchema` | Store a `FileDescriptorSet` for `(module, version)` |
| `GetSchema` | Retrieve the stored schema bytes |

Schemas are automatically uploaded when engines register (if `schema_path` is set in `engine.toml`). You can also push a schema independently:

```bash
grpcurl -plaintext -d "{
  \"module\": \"inventory-service\",
  \"version\": \"1.0.0\",
  \"proto_schema\": \"$(base64 -i schemas/inventory_service.binpb)\"
}" 127.0.0.1:9000 wruntime.ManagerService/UploadSchema
```

### Metrics

| RPC | Description |
|-----|-------------|
| `ReportMetrics` | Proxy sends a batch of `RequestMetrics` |
| `GetMetricsSummary` | Returns up to 10 000 most-recent entries |

Each `RequestMetrics` entry records: source module, destination module, duration (ms), HTTP status, and any error string.

---

## Protobuf schemas for modules

The proxy validates request bodies against a module's schema when one is registered. Schemas are compiled `FileDescriptorSet` binaries produced by `protoc`.

### Writing a schema

```protobuf
// inventory_service.proto
syntax = "proto3";
package inventory;

message GetItemsRequest {
  string category = 1;
}

message GetItemsResponse {
  repeated string items = 1;
}

service InventoryService {
  rpc GetItems (GetItemsRequest) returns (GetItemsResponse);
}
```

### Compiling to a FileDescriptorSet

```bash
protoc \
  --descriptor_set_out=schemas/inventory_service.binpb \
  --include_imports \
  inventory_service.proto
```

The resulting `.binpb` file is the value of `schema_path` in `engine.toml`.

### How validation works

When the proxy receives a request with `x-wr-destination: http://inventory-service/inventory.InventoryService/GetItems` it:

1. Parses the host (`inventory-service`) as the module name.
2. Looks up the `DescriptorPool` for that module.
3. Resolves the input message type for the RPC path (`/inventory.InventoryService/GetItems`).
4. Attempts to decode the raw request body as that message type using `prost-reflect`.

On failure the proxy returns `400 Bad Request`:

```json
{
  "error":       "schema_validation_failed",
  "detail":      "schema validation failed for inventory-service/inventory.InventoryService/GetItems: ...",
  "source":      "order-service",
  "destination": "inventory-service"
}
```

If no schema is registered for a module, validation is skipped and the request is forwarded unchanged.

---

## Building a compatible WASM module

Modules must be **WASI Preview 2 components** that implement the `wasi:http/incoming-handler` world. They make outbound calls using the standard `wasi:http/outgoing-handler` interface — all routing is transparent.

### Toolchain

```bash
rustup target add wasm32-wasip2
cargo install cargo-component   # WASI component tooling
```

### Cargo.toml

```toml
[package]
name    = "inventory-service"
version = "1.0.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
wit-bindgen = "0.36"

[package.metadata.component]
package = "wruntime:inventory-service"
```

### WIT world

Your component must implement the WASI HTTP proxy world. With `cargo-component` this is done automatically. The relevant WIT interface is:

```wit
// WASI HTTP proxy world (provided by the host)
world proxy {
  export wasi:http/incoming-handler@0.2.0;
  import wasi:http/outgoing-handler@0.2.0;
}
```

### Example: handling an incoming request

```rust
// src/lib.rs
wit_bindgen::generate!({ world: "proxy" });

use exports::wasi::http::incoming_handler::Guest;
use wasi::http::types::*;

struct Component;
export!(Component);

impl Guest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let headers = Fields::new();
        let resp = OutgoingResponse::new(headers);
        resp.set_status_code(200).unwrap();

        let body = resp.body().unwrap();
        {
            let stream = body.write().unwrap();
            stream.write(b"hello from inventory-service").unwrap();
        }
        OutgoingBody::finish(body, None).unwrap();
        ResponseOutparam::set(response_out, Ok(resp));
    }
}
```

### Example: making an outbound call to another module

Use the module name as the HTTP host. The engine intercepts the call and routes it via the proxy:

```rust
use wasi::http::{outgoing_handler, types::*};

fn call_inventory(category: &str) -> Vec<u8> {
    let headers = Fields::new();
    let req = OutgoingRequest::new(headers);

    // Use the destination module name as the host — routing is automatic.
    req.set_authority(Some("inventory-service")).unwrap();
    req.set_path_with_query(Some(
        "/inventory.InventoryService/GetItems"
    )).unwrap();
    req.set_scheme(Some(&Scheme::Http)).unwrap();
    req.set_method(&Method::Post).unwrap();

    // Write the serialised protobuf body
    let body = req.body().unwrap();
    {
        let stream = body.write().unwrap();
        stream.write(category.as_bytes()).unwrap();
    }
    OutgoingBody::finish(body, None).unwrap();

    let future = outgoing_handler::handle(req, None).unwrap();
    let resp = future.get().unwrap().unwrap().unwrap();

    let body = resp.consume().unwrap();
    let stream = body.stream().unwrap();
    stream.read(u64::MAX).unwrap()
}
```

### Building the component

```bash
cargo component build --release
# produces: target/wasm32-wasip2/release/inventory_service.wasm
```

Copy the `.wasm` file to the path referenced by `wasm_path` in `engine.toml`.

---

## Testing

```bash
cargo test
```

The `wr-tests` crate contains integration tests that spin up in-process instances of all three services on random ports — no external processes or files required:

- Manager RPC coverage (register, deregister, heartbeat, routing rules, metrics)
- Proxy routing end-to-end with a stub engine
- Schema validation: invalid protobuf bodies rejected with structured JSON errors
- Pass-through when no schema is cached
- All three example TOML files parse without error

---

## Project layout

```
runtime/
├── proto/
│   └── wruntime.proto      # single source of truth for all gRPC messages
├── wr-common/              # generated proto types (tonic + prost)
├── wr-manager/             # central registry gRPC server
├── wr-proxy/               # HTTP routing + schema validation proxy
│   └── src/layers/         # Tower middleware stack
├── wr-engine/              # WASM runtime (wasmtime) + inbound HTTP server
├── wr-tests/               # integration tests
├── manager.toml            # example manager config
├── proxy.toml              # example proxy config
└── engine.toml             # example engine config
```
