# Wruntime

A distributed WASM module networking runtime. WASM modules running inside **wr-engine** make ordinary HTTP calls to each other; Wruntime intercepts those calls, routes them through **wr-proxy**, validates them against their protobuf schemas, and delivers them to the correct destination engine. A central **wr-manager** holds the routing table, module registry, schemas, and metrics.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  wr-engine A                     wr-engine B                │
│  ┌──────────────────┐            ┌───────────────────┐      │
│  │  order-service   │            │ inventory-service │      │
│  │  (WASM module)   │            │  (WASM module)    │      │
│  └────────┬─────────┘            └────────▲──────────┘      │
│           │ HTTP (intercepted)            │ HTTP            │
│           │ x-wr-source / x-wr-destination│                 │
└───────────┼───────────────────────────────┼─────────────────┘
            │                               │
            ▼                               │
┌───────────────────────┐                   │
│       wr-proxy        │───────────────────┘
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
  │  1. TracingLayer       — opens an OTel span; injects W3C traceparent header
  │  2. MetricsLayer       — records start time
  │  3. SchemaValidation   — enforces gRPC path format; decodes body with
  │                          prost-reflect against the module's FileDescriptorSet;
  │                          returns 404 if path is not a known RPC,
  │                          503 if schema not yet synced,
  │                          400 if body fails protobuf decoding
  │  4. RoutingLayer       — reads optional x-wr-version header; defaults to
  │                          highest semver among healthy rules for the module;
  │                          returns 503 if no healthy instance matches;
  │                          injects x-wr-module and x-wr-version headers;
  │                          round-robins across multiple healthy instances
  │                          at the same version
  │  5. ForwardService     — strips x-wr-destination / x-wr-source, injects
  │                          traceparent, forwards to destination engine
  │
  ▼
wr-engine (destination)
  │  Inbound HTTP server reads x-wr-module + x-wr-version, dispatches to
  │  the correct WASM instance via round-robin among loaded instances
  │
  ▼
inventory-service WASM module handles the request
```

---

## Prerequisites

| Tool | Purpose |
|------|---------|
| Rust + Cargo (stable) | Build all binaries |
| [`just`](https://github.com/casey/just) | Run project recipes (see `Justfile`) |
| `protoc` | Compile `.proto` schemas to `FileDescriptorSet` binaries — required for every module |
| `wasm-tools` or `cargo-component` | Build WASM component modules |

Install Rust via [rustup](https://rustup.rs). Install `just` via `cargo install just` or your system package manager. Install `protoc` via your system package manager or from [github.com/protocolbuffers/protobuf/releases](https://github.com/protocolbuffers/protobuf/releases).

---

## Building

```bash
just build          # debug build
just build-release  # release build
```

Release binaries are placed in `target/release/`:

```
target/release/wr-manager
target/release/wr-proxy
target/release/wr-engine
```

---

## Running

Start the three components **in order**: manager first, then proxy, then engines.

```bash
just manager   # dev (cargo run)
just proxy
just engine

just manager-release   # release binaries
just proxy-release
just engine-release
```

### 1. wr-manager

```bash
just manager
```

`manager.toml`:

```toml
listen_address                = "0.0.0.0:9000"
engine_heartbeat_timeout_secs = 30
```

### 2. wr-proxy

```bash
just proxy
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
just engine
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
schema_path = "schemas/order_service.binpb"

[[module]]
name        = "inventory-service"
version     = "1.0.0"
wasm_path   = "modules/inventory_service.wasm"
schema_path = "schemas/inventory_service.binpb"
```

> **`schema_path` is required.** Every module must declare a compiled `FileDescriptorSet`. The engine will refuse to start if the file is absent, and the proxy will reject requests with `503` until the schema has been synced from the manager.

On startup the engine:
1. Loads every listed WASM component from disk.
2. Registers itself and its modules with the manager (including schema bytes).
3. Starts an inbound HTTP server on `listen_address`.
4. Sends a heartbeat to the manager every 10 seconds, reporting all loaded modules as healthy.
5. Deregisters cleanly on `Ctrl+C`, which immediately marks its routing rules as unhealthy.

#### Routing rules

Engines register themselves but do not create routing rules automatically — you create rules via the manager's gRPC API (or a management tool) after the engine is running:

```
# example using grpcurl
grpcurl -plaintext -d '{
  "rule_id": "r1",
  "source_module": "order-service",
  "destination_module": "inventory-service",
  "destination_version": "1.0.0",
  "engine_id": "<engine-uuid>",
  "engine_address": "http://127.0.0.1:9100"
}' 127.0.0.1:9000 wruntime.ManagerService/UpsertRoutingRule
```

To run **multiple instances** of the same module version across different engines, create one rule per engine pointing at the same `(destination_module, destination_version)`. The proxy round-robins across all healthy rules for that pair.

To deploy a **new version** alongside the old one, register a new engine with `version = "2.0.0"` and add a corresponding rule. Callers that omit `x-wr-version` are automatically upgraded to the highest semver. Callers that pin a version with the `x-wr-version` request header continue to reach the older instance.

---

## gRPC API (`proto/wruntime.proto`)

All inter-service communication uses the `wruntime.ManagerService` gRPC service.

### Engine lifecycle

| RPC | Request | Response | Description |
|-----|---------|----------|-------------|
| `RegisterEngine` | `EngineRegistration` | `{ accepted }` | Engine announces itself and its modules |
| `DeregisterEngine` | `{ engine_id }` | — | Engine removes itself on shutdown |
| `Heartbeat` | `{ engine_id, healthy_modules }` | — | Sent every 10 s; carries the list of currently healthy modules; manager uses this to update per-module health and mark routing rules unhealthy when a module goes silent |
| `ListEngines` | — | `[EngineRegistration]` | Returns all currently registered engines |

### Routing table

| RPC | Request | Response | Description |
|-----|---------|----------|-------------|
| `GetRoutingTable` | — | `RoutingTable` | Returns the full versioned table |
| `UpsertRoutingRule` | `RoutingRule` | — | Insert or update a rule by `rule_id`; always marks the rule healthy |
| `DeleteRoutingRule` | `{ rule_id }` | — | Remove a rule; increments table version |

A `RoutingRule` has the fields:

```protobuf
message RoutingRule {
  string rule_id             = 1;  // stable identifier for this rule
  string source_module       = 2;  // module that initiates the call
  string destination_module  = 3;  // module name used as the HTTP host
  string engine_id           = 4;  // UUID of the destination engine
  string engine_address      = 5;  // HTTP base URL of the destination engine
  string destination_version = 6;  // semver of the destination module, e.g. "1.2.0"
  bool   healthy             = 7;  // set by manager; false = proxy will not route to this rule
}
```

The `healthy` field is managed entirely by the manager — it is always set to `true` on `UpsertRoutingRule` and is flipped to `false` automatically when the engine's heartbeat stops reporting the module as healthy, or immediately on `DeregisterEngine`. The routing table version is incremented whenever health status changes, so proxies pick up failover events within one TTL cycle.

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

Every module **must** declare a protobuf schema. The proxy enforces two rules on every inter-module request:

1. **Path must be a gRPC method** — the path must have the form `/package.ServiceName/MethodName` and must match a method declared in the module's schema. Any other path returns `404`.
2. **Body must decode as the method's input message** — the raw request body is decoded with `prost-reflect` against the `FileDescriptorSet`. A body that fails decoding returns `400`.

Schemas are compiled `FileDescriptorSet` binaries produced by `protoc`.

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
4. Decodes the raw request body as that message type using `prost-reflect`.

The proxy returns an error and stops the request at each stage of failure:

| Condition | Status | `"error"` field |
|---|---|---|
| Schema not yet synced from manager | `503` | `"schema_not_cached"` |
| Path not in `/package.Service/Method` format, or method not in schema | `404` | `"method_not_found"` |
| Body fails protobuf decoding | `400` | `"schema_validation_failed"` |

Example `404` response for an unrecognised path:

```json
{
  "error":       "method_not_found",
  "detail":      "path '/items' does not match any RPC in the schema for ecommerce.inventory — all inter-service calls must use gRPC paths (/package.Service/Method)",
  "source":      "order-service",
  "destination": "inventory-service"
}
```

---

## Building a compatible WASM module

Modules must be **WASI Preview 2 components** that implement the `wasi:http/incoming-handler` world. They make outbound calls using the standard `wasi:http/outgoing-handler` interface — all routing is transparent.

All inter-module communication **must use gRPC-style paths** and **protobuf-encoded bodies**. The proxy enforces both. Plain HTTP paths and JSON bodies will be rejected.

### Toolchain

```bash
rustup target add wasm32-wasip2
cargo install cargo-component   # WASI component tooling
cargo install just              # task runner
```

`protoc` must also be installed — it is invoked by `prost-build` at compile time to generate Rust types from your `.proto` file.

### Cargo.toml

```toml
[package]
name    = "inventory-service"
version = "1.0.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
wit-bindgen-rt = "0.44"
prost          = "0.13"   # protobuf encode / decode

[build-dependencies]
prost-build = "0.13"      # generates Rust types from .proto at compile time

[package.metadata.component]
package = "wruntime:inventory-service"
```

### build.rs

```rust
fn main() {
    prost_build::compile_protos(
        &["schemas/inventory_service.proto"],
        &["schemas"],
    ).unwrap();
    println!("cargo:rerun-if-changed=schemas/inventory_service.proto");
}
```

Include the generated types in your module:

```rust
mod proto {
    include!(concat!(env!("OUT_DIR"), "/inventory.rs"));
}
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

### Example: making an outbound call to another module

Use `{module}.{namespace}` as the HTTP authority. The engine intercepts the call and routes it via the proxy. The path **must** be a gRPC method path and the body **must** be a protobuf-encoded message:

```rust
use prost::Message;
use wasi::http::{outgoing_handler, types::*};

fn buy_item(product_id: &str, quantity: i64) -> proto::BuyResponse {
    // Encode the protobuf request body.
    let body_bytes = proto::BuyRequest {
        product_id: product_id.to_string(),
        quantity,
    }
    .encode_to_vec();

    let headers = Fields::new();
    headers.set("content-type", &[b"application/x-protobuf".to_vec()]).unwrap();
    let req = OutgoingRequest::new(headers);

    // Authority is "{module}.{namespace}" — routing is automatic.
    // Omit x-wr-version to reach the latest deployed version, or set it to
    // pin a specific semver.
    req.set_authority(Some("inventory-service.myapp")).unwrap();
    req.set_path_with_query(Some("/inventory.InventoryService/Buy")).unwrap();
    req.set_scheme(Some(&Scheme::Http)).unwrap();
    req.set_method(&Method::Post).unwrap();

    let out_body = req.body().unwrap();
    {
        let stream = out_body.write().unwrap();
        stream.blocking_write_and_flush(&body_bytes).unwrap();
    }
    OutgoingBody::finish(out_body, None).unwrap();

    let future = outgoing_handler::handle(req, None).unwrap();
    loop {
        match future.get() {
            Some(Ok(Ok(resp))) => {
                let incoming = resp.consume().unwrap();
                let stream = incoming.stream().unwrap();
                let mut bytes = Vec::new();
                loop {
                    match stream.blocking_read(8192) {
                        Ok(chunk) if chunk.is_empty() => break,
                        Ok(chunk) => bytes.extend_from_slice(&chunk),
                        Err(_) => break,
                    }
                }
                return proto::BuyResponse::decode(bytes.as_slice()).unwrap();
            }
            None => future.subscribe().block(),
            _ => panic!("request failed"),
        }
    }
}
```

### Example: handling an incoming gRPC-style request

```rust
impl Guest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let path = request.path_with_query().unwrap_or_default();
        let body = read_body(request.consume().unwrap());

        let (status, resp_bytes) = match path.as_str() {
            "/inventory.InventoryService/Buy" => {
                match proto::BuyRequest::decode(body.as_slice()) {
                    Ok(req) => {
                        let resp = process_buy(req);
                        (200, resp.encode_to_vec())
                    }
                    Err(e) => (400, format!(r#"{{"error":"{e}"}}"#).into_bytes()),
                }
            }
            _ => (404, b"not found".to_vec()),
        };

        send_response(response_out, status, resp_bytes);
    }
}
```

### Building the component

```bash
cargo component build --release
# produces: target/wasm32-wasip2/release/inventory_service.wasm
```

Copy the `.wasm` file to the path referenced by `wasm_path` in `engine.toml`.

---

## Database access

WASM modules can query Postgres through a host-provided interface defined in `wit/db.wit`. The engine holds a shared connection pool; the module never owns a connection directly.

### Engine configuration

Add a `[database]` section to `engine.toml` and set `database = true` on each module that should have access:

```toml
[database]
url             = "postgres://user:pass@localhost:5432/mydb"
max_connections = 10   # default: 8

[[module]]
name      = "order-service"
version   = "1.0.0"
wasm_path = "modules/order_service.wasm"
database  = true       # opt in to DB access

[[module]]
name      = "inventory-service"
version   = "1.0.0"
wasm_path = "modules/inventory_service.wasm"
# database omitted — no DB access for this module
```

### Guest-side setup

Add the WIT path to your component's `Cargo.toml` and import the interface:

```toml
[package.metadata.component.target]
path = "../../wit"   # path to the repo's wit/ directory
```

Then generate bindings and use them in your module:

```rust
// src/lib.rs
wit_bindgen::generate!({
    path:  "../../wit",
    world: "db-access",
});

use wruntime::db::database::{self, PgValue};
```

### Example: querying Postgres from a WASM module

```rust
use wruntime::db::database::{self, DbError, PgValue};

/// Look up an order by its integer ID and return the status string.
fn get_order_status(order_id: i32) -> Result<Option<String>, DbError> {
    let rows = database::query(
        "SELECT status FROM orders WHERE id = $1",
        &[PgValue::Int4(order_id)],
    )?;

    Ok(rows.first().and_then(|row| {
        match row.columns.first().map(|c| &c.value) {
            Some(PgValue::Text(s)) => Some(s.clone()),
            _ => None,
        }
    }))
}

/// Insert a new order and return the number of rows affected.
fn create_order(id: i32, status: &str, total: &str) -> Result<u64, DbError> {
    database::execute(
        "INSERT INTO orders (id, status, total) VALUES ($1, $2, $3::numeric)",
        &[
            PgValue::Int4(id),
            PgValue::Text(status.to_string()),
            PgValue::Numeric(total.to_string()),
        ],
    )
}
```

### `pg-value` type mapping

| Variant | Postgres type | Rust encoding |
|---|---|---|
| `PgValue::Null` | SQL NULL | — |
| `PgValue::Boolean(bool)` | `BOOL` | `bool` |
| `PgValue::Int2(i16)` | `SMALLINT` | `i16` |
| `PgValue::Int4(i32)` | `INTEGER` | `i32` |
| `PgValue::Int8(i64)` | `BIGINT` | `i64` |
| `PgValue::Float4(f32)` | `REAL` | `f32` |
| `PgValue::Float8(f64)` | `DOUBLE PRECISION` | `f64` |
| `PgValue::Text(String)` | `TEXT` / `VARCHAR` / `CHAR` | `String` |
| `PgValue::Bytea(Vec<u8>)` | `BYTEA` | `Vec<u8>` |
| `PgValue::Timestamptz(i64)` | `TIMESTAMPTZ` | µs since Unix epoch (UTC) |
| `PgValue::Date(i32)` | `DATE` | days since Unix epoch |
| `PgValue::Time(i64)` | `TIME` | µs since midnight |
| `PgValue::Numeric(String)` | `NUMERIC` / `DECIMAL` | decimal string (lossless) |
| `PgValue::Uuid((u64, u64))` | `UUID` | 128-bit value as `(high, low)` |
| `PgValue::Jsonb(String)` | `JSON` / `JSONB` | serialised JSON string |
| `PgValue::Oid(u32)` | `OID` | `u32` |

Parameters are bound positionally as `$1`, `$2`, … in the SQL string. Use explicit casts (e.g. `$1::numeric`, `$1::jsonb`) when Postgres cannot infer the type from context.

---

## Testing

```bash
just test                    # all tests
just test-integration        # wr-tests crate only
just test-one <test_name>    # single test by name
just test-db                 # integration tests with a local Postgres instance
```

The `wr-tests` crate contains integration tests that spin up in-process instances of all three services on random ports — no external processes or files required:

- Manager RPC coverage (register, deregister, heartbeat, routing rules, metrics)
- Proxy routing end-to-end with a stub engine
- Schema validation: invalid protobuf bodies rejected with `400`; non-gRPC paths rejected with `404`; missing schema returns `503`
- All three example TOML files parse without error
- Version routing: `x-wr-version` header routes to the correct instance; no header routes to the highest semver
- Returns 503 when the requested version has no healthy instance
- Load balancing: requests distributed across multiple instances of the same `(module, version)`
- Failover: deregistering an instance immediately redirects traffic to remaining healthy instances
- Full failure: 503 when all instances are unhealthy

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
