# Wruntime

A distributed WASM module networking runtime. WASM modules running inside **wr-engine** make ordinary HTTP calls to each other; Wruntime intercepts those calls, routes them through **wr-proxy**, validates them against their protobuf schemas, and delivers them to the correct destination engine. A central **wr-manager** holds the routing table, module registry, schemas, and metrics.

---

## Architecture

A **node** is one `wr-proxy` co-located with one or more `wr-engine` instances. Nodes are independent — each proxy handles its own inbound traffic and forwards cross-node requests directly to the peer proxy, which then routes locally to its engines.

```
                         ┌───────────────────────┐
                         │      wr-manager        │
                         │                        │
                         │  Engine registry       │
                         │  Routing table         │
                         │  Schema store          │
                         │  Metrics buffer        │
                         └──────────┬─────────────┘
                                    │ gRPC (all nodes)
               ┌────────────────────┴─────────────────────┐
               │                                          │
               ▼                                          ▼
┌─────────────────────────────┐        ┌─────────────────────────────┐
│           Node A            │        │           Node B            │
│                             │        │                             │
│  ┌───────────────────────┐  │        │  ┌───────────────────────┐  │
│  │      wr-proxy A       │◄─┼────────┼─►│      wr-proxy B       │  │
│  │  TracingLayer         │  │  HTTP  │  │  TracingLayer         │  │
│  │  MetricsLayer         │  │        │  │  MetricsLayer         │  │
│  │  SchemaValidationLayer│  │        │  │  (skipped for relayed │  │
│  │  RoutingLayer         │  │        │  │   x-wr-via-proxy reqs)│  │
│  │  ForwardService       │  │        │  │  RoutingLayer         │  │
│  └──────────┬────────────┘  │        │  │  ForwardService       │  │
│             │ local         │        │  └──────────┬────────────┘  │
│             ▼               │        │             │ local         │
│  ┌───────────────────────┐  │        │  ┌──────────▼────────────┐  │
│  │      wr-engine A      │  │        │  │      wr-engine B      │  │
│  │  ┌─────────────────┐  │  │        │  │  ┌─────────────────┐  │  │
│  │  │  order-service  │  │  │        │  │  │inventory-service│  │  │
│  │  │  (WASM module)  │  │  │        │  │  │  (WASM module)  │  │  │
│  │  └─────────────────┘  │  │        │  │  └─────────────────┘  │  │
│  └───────────────────────┘  │        │  └───────────────────────┘  │
└─────────────────────────────┘        └─────────────────────────────┘
```

### Components

| Binary | Default port | Role |
|--------|-------------|------|
| `wr-manager` | `9000` (gRPC) | Central registry — engines register here, proxies sync routing and schemas from here |
| `wr-proxy` | `9001` (HTTP) | Intercepts and routes inter-module traffic; validates schemas; forwards cross-node requests to peer proxies |
| `wr-engine` | `9100` (HTTP) | Loads WASM modules, runs them, and receives forwarded requests |

A **node** groups one `wr-proxy` with one or more `wr-engine` instances behind a shared externally-reachable proxy address. Each node knows its own address via `[node] proxy_address` in its config files; the engine sends this value to the manager on registration so the routing table can distinguish local from remote destinations.

### Request flow

```
WASM module makes HTTP call to "http://inventory-service.ecommerce/items"
  │
  ▼  [WasiHttpView::send_request intercepts — transparent to the module]
  │  Adds headers:
  │    x-wr-source:      "order-service.ecommerce"
  │    x-wr-destination: "http://inventory-service.ecommerce/items"
  │  Rewrites URI to the local wr-proxy (Node A)
  │
  ▼
wr-proxy A  (Node A)
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
  │                          injects x-wr-module, x-wr-namespace, x-wr-version;
  │                          round-robins across multiple healthy instances
  │                          at the same version;
  │                          resolves destination as LocalEngine or RemoteProxy
  │  5. ForwardService     — strips x-wr-destination / x-wr-source, injects
  │                          traceparent, then:
  │
  ├── destination is on Node A (LocalEngine) ─────────────────────────────────┐
  │     strips x-wr-destination / x-wr-source / x-wr-via-proxy                │
  │     forwards directly to wr-engine A                                       │
  │                                                                            ▼
  │                                                                    wr-engine A
  │
  └── destination is on Node B (RemoteProxy) ──────────────────────────────────┐
        sets x-wr-via-proxy: 1                                                  │
        forwards to wr-proxy B                                                  │
                                                                               ▼
                                                               wr-proxy B  (Node B)
                                                                 SchemaValidation skipped
                                                                 (x-wr-via-proxy already set)
                                                                 RoutingLayer routes locally
                                                                               │
                                                                               ▼
                                                                       wr-engine B

wr-engine (destination)
  │  Inbound HTTP server reads x-wr-module + x-wr-version + x-wr-namespace,
  │  dispatches to the correct WASM instance via round-robin
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
| `cargo-component` | Build WASM component modules |

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

[node]
proxy_address = "http://127.0.0.1:9001"   # this proxy's own address, as reachable by peers

[cache]
routing_table_ttl_secs = 5   # how often to poll the manager for routing updates
schema_ttl_secs        = 60  # how often to sync module schemas

[metrics]
flush_interval_secs = 10
queue_depth         = 1000
```

`proxy_address` must match how peer nodes (and engines on this node) will reach this proxy. The routing layer uses it to distinguish rules whose `proxy_address` matches this node — those are forwarded directly to the local engine; all others are forwarded to the peer proxy that owns that address.

The proxy connects to the manager at startup, then polls for routing table and schema updates in the background.

### 3. wr-engine

```bash
just engine
```

`engine.toml`:

```toml
listen_address  = "0.0.0.0:9100"
manager_address = "http://127.0.0.1:9000"

[node]
proxy_address = "http://127.0.0.1:9001"   # local proxy; WASM outbound calls are rewritten to
                                           # this address, and it is sent to the manager on
                                           # registration so peers can find this node

[[module]]
name        = "order-service"
namespace   = "ecommerce"
version     = "1.0.0"
wasm_path   = "modules/order_service.wasm"
schema_path = "schemas/order_service.binpb"

[[module]]
name        = "inventory-service"
namespace   = "ecommerce"
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
  "source_namespace": "ecommerce",
  "destination_module": "inventory-service",
  "destination_namespace": "ecommerce",
  "destination_version": "1.0.0",
  "engine_id": "<engine-uuid>",
  "engine_address": "http://127.0.0.1:9100",
  "proxy_address": "http://127.0.0.1:9001"
}' 127.0.0.1:9000 wruntime.ManagerService/UpsertRoutingRule
```

`proxy_address` tells every proxy which node owns this rule. A proxy whose own `[node] proxy_address` matches will route directly to `engine_address`; all other proxies will relay to `proxy_address` and let that node route locally.

To run **multiple instances** of the same module version across different engines (on the same or different nodes), create one rule per engine pointing at the same `(destination_module, destination_namespace, destination_version)`. The proxy round-robins across all healthy rules for that tuple.

To deploy a **new version** alongside the old one, register a new engine with `version = "2.0.0"` and add a corresponding rule. Callers that omit `x-wr-version` are automatically upgraded to the highest semver. Callers that pin a version with the `x-wr-version` request header continue to reach the older instance.

#### Multi-node deployment

Node B config files follow the same structure — just use different ports and a matching `proxy_address`:

```toml
# node-b/proxy.toml
listen_address  = "0.0.0.0:9002"
manager_address = "http://127.0.0.1:9000"

[node]
proxy_address = "http://node-b-host:9002"

# node-b/engine.toml
listen_address  = "0.0.0.0:9200"
manager_address = "http://127.0.0.1:9000"

[node]
proxy_address = "http://node-b-host:9002"
```

When a module on Node A calls a module whose routing rule has `proxy_address = "http://node-b-host:9002"`, Node A's proxy adds `x-wr-via-proxy: 1` and forwards the request to Node B's proxy. Node B's `SchemaValidationLayer` skips re-validation (the header signals it was already validated at ingress), and `RoutingLayer` resolves the destination as a local engine.

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
  string rule_id               = 1;   // stable identifier for this rule
  string source_module         = 2;   // module that initiates the call
  string destination_module    = 3;   // module name used as the HTTP host
  string engine_id             = 4;   // UUID of the destination engine
  string engine_address        = 5;   // HTTP base URL of the destination engine
  string destination_version   = 6;   // semver of the destination module, e.g. "1.2.0"
  bool   healthy               = 7;   // set by manager; false = proxy will not route to this rule
  string destination_namespace = 8;   // namespace of the destination module
  string source_namespace      = 9;   // namespace of the source module
  string proxy_address         = 10;  // externally-reachable address of the node's proxy
}
```

`proxy_address` is set automatically from the engine's `[node] proxy_address` when the engine registers. The routing layer on each proxy compares this field against its own `[node] proxy_address` to decide whether to forward the request directly to the local `engine_address` (`LocalEngine`) or to relay it to the peer proxy at `proxy_address` (`RemoteProxy`).

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

## Module SDK (`wr-sdk` + `wr-build`)

Two crates eliminate boilerplate from wruntime WASM modules:

- **`wr-sdk`** — shared WASI helpers that every module links against: `http_rpc`, `read_body`, `send_response`, `err_body`, `log`, export macros, and the `Guest` / `RunGuest` traits.
- **`wr-build`** — a `build.rs` library providing a `prost-build` `ServiceGenerator` that emits typed gRPC client structs from `.proto` files.

### Building a handler module (HTTP request/response)

`Cargo.toml`:

```toml
[package]
name    = "inventory-service"
version = "1.0.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
prost   = "0.13"
wr-sdk  = { path = "../../wr-sdk" }

[build-dependencies]
prost-build = "0.13"

[package.metadata.component]
package = "wruntime:inventory-service"

[package.metadata.component.target]
world = "wruntime:inventory-service/inventory-service"
```

`build.rs`:

```rust
fn main() {
    prost_build::compile_protos(
        &["schemas/inventory_service.proto"],
        &["schemas"],
    ).unwrap();
    println!("cargo:rerun-if-changed=schemas/inventory_service.proto");
}
```

`src/lib.rs`:

```rust
mod proto {
    include!(concat!(env!("OUT_DIR"), "/inventory.rs"));
}

use wr_sdk::bindings::wasi::http::types::{IncomingRequest, ResponseOutparam};
use wr_sdk::io::{err_body, read_body, send_response};
use prost::Message;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

impl wr_sdk::Guest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let path  = request.path_with_query().unwrap_or_default();
        let body  = read_body(request.consume().unwrap());

        let (status, resp) = match path.as_str() {
            "/inventory.InventoryService/GetItems" => {
                match proto::GetItemsRequest::decode(body.as_slice()) {
                    Ok(req) => (200, process_get_items(req).encode_to_vec()),
                    Err(e)  => err_body(400, &e.to_string()),
                }
            }
            _ => err_body(404, &format!("no handler for {path}")),
        };

        send_response(response_out, status, resp);
    }
}
```

### Building a runner module (long-running task that calls other services)

Add `wr-build` as a build dependency to get generated typed clients from your `.proto` file.

`Cargo.toml`:

```toml
[dependencies]
prost   = "0.13"
wr-sdk  = { path = "../../wr-sdk" }

[build-dependencies]
prost-build = "0.13"
wr-build    = { path = "../../wr-build" }

[package.metadata.component]
package = "wruntime:client"

[package.metadata.component.target]
world = "wruntime:client/client"
```

`build.rs`:

```rust
fn main() {
    prost_build::Config::new()
        .service_generator(Box::new(wr_build::WrClientGenerator))
        .compile_protos(&["schemas/inventory_service.proto"], &["schemas"])
        .unwrap();
}
```

`WrClientGenerator` appends a typed `{ServiceName}Client` struct to the generated file. For a service `InventoryService` in package `inventory`, the generated client looks like:

```rust
pub struct InventoryServiceClient { authority: String }

impl InventoryServiceClient {
    pub fn new(authority: impl Into<String>) -> Self { ... }

    pub fn get_items(&self, req: GetItemsRequest) -> Result<GetItemsResponse, String> { ... }
    // one method per RPC; keyword names are escaped (e.g. r#return)
}
```

`src/lib.rs`:

```rust
mod proto {
    include!(concat!(env!("OUT_DIR"), "/inventory.rs"));
}

use proto::InventoryServiceClient;

struct Component;
wr_sdk::export_run!(Component);

impl wr_sdk::RunGuest for Component {
    fn run() {
        wr_sdk::log::log("starting");

        let client = InventoryServiceClient::new("inventory-service.myapp");

        match client.get_items(proto::GetItemsRequest { category: "books".into() }) {
            Ok(resp) => wr_sdk::log::log(&format!("items: {:?}", resp.items)),
            Err(e)   => wr_sdk::log::log(&format!("error: {e}")),
        }
    }
}
```

### SDK reference

| Item | Description |
|------|-------------|
| `wr_sdk::http::http_rpc(authority, path, body)` | POST a protobuf body to `http://{authority}{path}`; returns `(status, bytes)` |
| `wr_sdk::io::read_body(incoming)` | Drain an `IncomingBody` into `Vec<u8>` |
| `wr_sdk::io::send_response(out, status, body)` | Write a response with the given status and body |
| `wr_sdk::io::err_body(status, msg)` | Return `(status, {"error":"msg"})` |
| `wr_sdk::log::log(msg)` | Write a line to WASI stderr |
| `wr_sdk::export!(T with_types_in wr_sdk::bindings)` | Register `T` as the `wasi:http/incoming-handler` implementation |
| `wr_sdk::export_run!(T)` | Register `T::run()` as the WASM `run` export |
| `wr_sdk::Guest` | Trait for HTTP handler modules (`fn handle(request, response_out)`) |
| `wr_sdk::RunGuest` | Trait for runner modules (`fn run()`) |
| `wr_sdk::bindings::wruntime::db::database` | DB access types — same as used by the host |

### Building the component

```bash
cargo component build --release
# produces: target/wasm32-wasip1/release/inventory_service.wasm
```

Copy the `.wasm` file to the path referenced by `wasm_path` in `engine.toml`.

---

## Database access

WASM modules can query Postgres through a host-provided interface defined in `wit/db.wit`. The engine holds a shared connection pool; the module never owns a connection directly.

When using `wr-sdk`, the database types (`PgValue`, `Row`, etc.) are available at `wr_sdk::bindings::wruntime::db::database` — no separate `wit_bindgen::generate!` call is required.

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

---

## Filesystem access

By default WASM modules have no filesystem access. Set `fs = "tempdir"` in a `[[module]]` block to mount an ephemeral writable directory at `/`:

```toml
[[module]]
name        = "order-service"
namespace   = "ecommerce"
version     = "1.0.0"
wasm_path   = "modules/order_service.wasm"
schema_path = "schemas/order_service.binpb"
fs          = "tempdir"
```

The directory is created fresh on the host for each store and deleted when the store is dropped:

- For **HTTP handler modules** a new directory is created per request (each request gets its own store).
- For **runner modules** the directory lives for the lifetime of the module.

The directory is empty on creation. It is not shared between module instances or across requests. Use it for scratch space, caching, or temporary files — do not rely on it for durable state.

| Value | Effect |
|-------|--------|
| `fs = "tempdir"` | Mount an ephemeral temp directory at `/` |
| *(omitted)* | No filesystem access (default) |

### Example: querying Postgres from a WASM module

```rust
use wr_sdk::bindings::wruntime::db::database::{self, PgValue};

/// Look up an order by its integer ID and return the status string.
fn get_order_status(order_id: i32) -> Option<String> {
    let rows = database::query(
        "SELECT status FROM orders WHERE id = $1",
        &[PgValue::Int4(order_id)],
    ).ok()?;

    match rows.first()?.columns.first().map(|c| &c.value) {
        Some(PgValue::Text(s)) => Some(s.clone()),
        _ => None,
    }
}

/// Insert a new order and return the number of rows affected.
fn create_order(id: i32, status: &str, total: &str) -> u64 {
    database::execute(
        "INSERT INTO orders (id, status, total) VALUES ($1, $2, $3::numeric)",
        &[
            PgValue::Int4(id),
            PgValue::Text(status.to_string()),
            PgValue::Numeric(total.to_string()),
        ],
    ).unwrap_or(0)
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
- Cross-node routing: request originating on Node A is relayed to Node B's proxy when the destination engine lives on Node B; schema validation is skipped on the second hop (`x-wr-via-proxy`)

---

## Project layout

```
wruntime/
├── proto/
│   └── wruntime.proto      # single source of truth for all gRPC messages
├── wr-common/              # generated proto types (tonic + prost); shared NodeConfig
├── wr-manager/             # central registry gRPC server
├── wr-proxy/               # HTTP routing + schema validation proxy
│   └── src/layers/         # Tower middleware stack
├── wr-engine/              # WASM runtime (wasmtime) + inbound HTTP server
├── wr-sdk/                 # WASM module SDK: http_rpc, io, log, export macros
├── wr-build/               # build.rs helper: WrClientGenerator for typed gRPC clients
├── wr-tests/               # integration tests
├── ecommerce-example/      # example: inventory (handler) + client (runner) modules
├── node-a/                 # example multi-node: Node A configs (proxy :9001, engines :9100/:9101)
├── node-b/                 # example multi-node: Node B configs (proxy :9002, engine :9200)
├── manager.toml            # example manager config
├── proxy.toml              # example single-node proxy config
└── engine.toml             # example single-node engine config
```
