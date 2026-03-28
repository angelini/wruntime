# Wruntime — Execution Plan

## Overview

Wruntime is a distributed WASM module network composed of three services:

- **`wr-engine`** — runs multiple WASM modules in one process, intercepts their HTTP traffic
- **`wr-proxy`** — receives HTTP from engines, routes it to the correct destination engine
- **`wr-manager`** — central registry; manages engines, routing rules, schemas, and metrics

All inter-service communication uses **gRPC** via `tonic`. Module schemas are defined
as **Protocol Buffer** (`.proto`) files.

The plan is structured in phases. Each phase produces a testable increment.

---

## Repository Structure

```
wruntime/
├── Cargo.toml               # workspace
├── proto/
│   └── wruntime.proto       # all gRPC service and message definitions
├── wr-common/               # generated protobuf types + shared build logic
│   └── build.rs
├── wr-engine/
├── wr-proxy/
└── wr-manager/
```

**`Cargo.toml` (workspace)**
```toml
[workspace]
members = ["wr-common", "wr-engine", "wr-proxy", "wr-manager"]
resolver = "2"
```

---

## Phase 1 — Workspace, Proto Definitions, and `wr-common`

All inter-service communication depends on shared generated types. Defining them
in a single `.proto` file and generating Rust code in `wr-common` prevents drift
across crates.

### 1.1 Create the workspace and `wr-common` crate

```
cargo new --lib wr-common
```

### 1.2 Define `proto/wruntime.proto`

This file is the single source of truth for all message shapes and service contracts.

```protobuf
syntax = "proto3";
package wruntime;

// ── Shared messages ───────────────────────────────────────────────────────

message ModuleDescriptor {
  string name          = 1;
  string version       = 2;
  bytes  proto_schema  = 3;  // serialised FileDescriptorSet for this module's API
}

message EngineRegistration {
  string                  engine_id = 1;
  string                  address   = 2;  // host:port wr-proxy uses to reach this engine
  repeated ModuleDescriptor modules = 3;
}

message RoutingRule {
  string rule_id            = 1;
  string source_module      = 2;
  string destination_module = 3;
  string engine_id          = 4;
  string engine_address     = 5;
}

message RoutingTable {
  repeated RoutingRule rules = 1;
  uint64               version = 2;  // incremented on every write
}

message RequestMetrics {
  string source      = 1;
  string destination = 2;
  uint64 duration_ms = 3;
  uint32 status      = 4;
  string error       = 5;
}

// ── ManagerService ────────────────────────────────────────────────────────

service ManagerService {
  // Engine lifecycle
  rpc RegisterEngine   (RegisterEngineRequest)   returns (RegisterEngineResponse);
  rpc DeregisterEngine (DeregisterEngineRequest) returns (DeregisterEngineResponse);
  rpc Heartbeat        (HeartbeatRequest)        returns (HeartbeatResponse);
  rpc ListEngines      (ListEnginesRequest)      returns (ListEnginesResponse);

  // Routing table
  rpc GetRoutingTable  (GetRoutingTableRequest)  returns (GetRoutingTableResponse);
  rpc UpsertRoutingRule(RoutingRule)             returns (UpsertRoutingRuleResponse);
  rpc DeleteRoutingRule(DeleteRoutingRuleRequest)returns (DeleteRoutingRuleResponse);

  // Schemas
  rpc GetSchema        (GetSchemaRequest)        returns (GetSchemaResponse);
  rpc UploadSchema     (UploadSchemaRequest)     returns (UploadSchemaResponse);

  // Metrics
  rpc ReportMetrics    (ReportMetricsRequest)    returns (ReportMetricsResponse);
  rpc GetMetricsSummary(GetMetricsSummaryRequest)returns (GetMetricsSummaryResponse);
}

message RegisterEngineRequest   { EngineRegistration registration = 1; }
message RegisterEngineResponse  { bool accepted = 1; }
message DeregisterEngineRequest { string engine_id = 1; }
message DeregisterEngineResponse{}
message HeartbeatRequest        { string engine_id = 1; }
message HeartbeatResponse       {}
message ListEnginesRequest      {}
message ListEnginesResponse     { repeated EngineRegistration engines = 1; }

message GetRoutingTableRequest  {}
message GetRoutingTableResponse { RoutingTable table = 1; }
message UpsertRoutingRuleResponse{}
message DeleteRoutingRuleRequest { string rule_id = 1; }
message DeleteRoutingRuleResponse{}

message GetSchemaRequest   { string module = 1; string version = 2; }
message GetSchemaResponse  { bytes proto_schema = 1; }  // FileDescriptorSet bytes
message UploadSchemaRequest{ string module = 1; string version = 2; bytes proto_schema = 3; }
message UploadSchemaResponse{}

message ReportMetricsRequest    { repeated RequestMetrics metrics = 1; }
message ReportMetricsResponse   {}
message GetMetricsSummaryRequest{}
message GetMetricsSummaryResponse{ repeated RequestMetrics metrics = 1; }
```

### 1.3 Compile the proto in `wr-common/build.rs`

```rust
// wr-common/build.rs
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["../proto/wruntime.proto"], &["../proto"])?;
    Ok(())
}
```

`tonic_build` runs `protoc` at compile time and emits Rust types + client/server
stubs into `OUT_DIR`. All three binaries depend on `wr-common` and get the generated
types for free.

### 1.4 Dependencies for `wr-common`

```toml
[dependencies]
prost       = "0.13"
tonic       = "0.12"
uuid        = { version = "1", features = ["v4"] }

[build-dependencies]
tonic-build = "0.12"
```

---

## Phase 2 — `wr-engine`

The engine loads WASM modules, manages their lifecycle, and intercepts all of their
outbound HTTP traffic via `WasiHttpView`, redirecting it to `wr-proxy`.

### 2.1 TOML configuration

```toml
# engine.toml
manager_address = "http://127.0.0.1:9000"   # gRPC address of wr-manager
proxy_address   = "http://127.0.0.1:9001"   # address of wr-proxy (plain HTTP)

[[module]]
name        = "order-service"
version     = "1.2.0"
wasm_path   = "./modules/order-service.wasm"
schema_path = "./schemas/order-service.binpb"  # compiled FileDescriptorSet

[[module]]
name        = "inventory-service"
version     = "0.9.1"
wasm_path   = "./modules/inventory-service.wasm"
schema_path = "./schemas/inventory-service.binpb"
```

`schema_path` points to a `FileDescriptorSet` binary produced by:
```
protoc --descriptor_set_out=inventory-service.binpb inventory_service.proto
```

### 2.2 Wasmtime setup

```
cargo new --bin wr-engine
```

Dependencies:
```toml
wasmtime           = { version = "41", features = ["component-model"] }
wasmtime-wasi      = "41"
wasmtime-wasi-http = { version = "41", features = ["default-send-request"] }
tokio              = { version = "1", features = ["full"] }
tonic              = "0.12"
prost              = "0.13"
hyper              = { version = "1", features = ["http1", "http2"] }
anyhow             = "1"
toml               = "0.8"
serde              = { version = "1", features = ["derive"] }
wr-common          = { path = "../wr-common" }
```

### 2.3 Per-module state

Each loaded WASM module gets its own `ModuleState`. These must not be shared.

```rust
struct ModuleState {
    wasi:        WasiCtx,
    http:        WasiHttpCtx,
    table:       ResourceTable,
    module_name: String,
    proxy_addr:  String,
}
```

### 2.4 Intercepting HTTP via `WasiHttpView`

Override `send_request` to redirect every outbound request from the WASM module
to `wr-proxy` instead of its original destination.

```rust
impl WasiHttpView for ModuleState {
    fn ctx(&mut self)   -> &mut WasiHttpCtx  { &mut self.http  }
    fn table(&mut self) -> &mut ResourceTable { &mut self.table }

    fn send_request(
        &mut self,
        mut request: hyper::Request<HyperOutgoingBody>,
        config: OutgoingRequestConfig,
    ) -> Result<HostFutureIncomingResponse, ErrorCode> {
        // Preserve the original destination so wr-proxy can route it.
        let original_uri = request.uri().to_string();
        request.headers_mut()
            .insert("x-wr-destination", HeaderValue::from_str(&original_uri)?);
        request.headers_mut()
            .insert("x-wr-source", HeaderValue::from_static(&self.module_name));

        // Rewrite the URI to point at wr-proxy.
        *request.uri_mut() = self.proxy_addr.parse()?;

        wasmtime_wasi_http::default_send_request(request, config)
    }
}
```

### 2.5 Module loader

Build an `EngineRunner` that:

1. Constructs a shared `wasmtime::Engine` with `async_support(true)` and
   `wasm_component_model(true)`.
2. Iterates the module list from config, loading each `.wasm` file into a
   `wasmtime::component::Component`.
3. Creates a dedicated `Store<ModuleState>` per module.
4. Builds a `Linker<ModuleState>`, calling `wasmtime_wasi::p2::add_to_linker_async`
   and `wasmtime_wasi_http::add_to_linker_only_http_async`.
5. Instantiates each component and spawns a Tokio task to drive it.

### 2.6 Registration with `wr-manager` via gRPC

On startup, after all modules are loaded, the engine connects to the manager using
the generated `ManagerServiceClient` and calls `RegisterEngine`.

```rust
use wr_common::wruntime::manager_service_client::ManagerServiceClient;
use wr_common::wruntime::RegisterEngineRequest;

let mut client = ManagerServiceClient::connect(config.manager_address).await?;

let schema_bytes = std::fs::read(&module_config.schema_path)?;

let response = client.register_engine(RegisterEngineRequest {
    registration: Some(EngineRegistration {
        engine_id: engine_id.to_string(),
        address:   config.listen_address.clone(),
        modules:   modules.iter().map(|m| ModuleDescriptor {
            name:         m.name.clone(),
            version:      m.version.clone(),
            proto_schema: schema_bytes.clone(),
        }).collect(),
    }),
}).await?;
```

On shutdown (SIGTERM), call `DeregisterEngine`. Send `Heartbeat` every 10 seconds
from a background task. Retry with exponential backoff if the manager is unreachable.

---

## Phase 3 — `wr-proxy`

The proxy sits between engines. It receives HTTP from a source engine, resolves the
destination engine from its routing table, validates the request against the
destination module's protobuf schema, and forwards it.

### 3.1 Setup

```
cargo new --bin wr-proxy
```

Dependencies:
```toml
tower         = { version = "0.5", features = ["full"] }
hyper         = { version = "1", features = ["http1", "http2", "server"] }
hyper-util    = "0.1"
tokio         = { version = "1", features = ["full"] }
tonic         = "0.12"
prost         = "0.13"
prost-reflect = "0.14"   # dynamic protobuf message decoding and validation
anyhow        = "1"
wr-common     = { path = "../wr-common" }
```

### 3.2 Local routing table cache

The proxy stores an `Arc<RwLock<RoutingTable>>` in memory. A background task calls
`GetRoutingTable` on `ManagerService` at a configurable interval (default: 5s),
comparing the `version` field to avoid unnecessary updates.

```rust
use wr_common::wruntime::manager_service_client::ManagerServiceClient;

async fn sync_routing_table(
    manager_addr: String,
    table: Arc<RwLock<RoutingTable>>,
) {
    let mut client = ManagerServiceClient::connect(manager_addr).await.unwrap();
    loop {
        if let Ok(resp) = client.get_routing_table(GetRoutingTableRequest {}).await {
            let incoming = resp.into_inner().table.unwrap();
            let current_version = table.read().await.version;
            if incoming.version > current_version {
                *table.write().await = incoming;
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
```

### 3.3 Tower middleware stack

```
Request
  │
  ▼
MetricsLayer           — record start time, source, destination
  │
  ▼
SchemaValidationLayer  — decode request body as protobuf, validate against module schema
  │
  ▼
RoutingLayer           — resolve destination engine address from routing table
  │
  ▼
ForwardLayer           — forward to resolved engine, strip x-wr-* headers
  │
  ▼
Response
```

**`MetricsLayer`** — records latency and status on the way out.

**`SchemaValidationLayer`** — reads `x-wr-destination`, fetches the `FileDescriptorSet`
for that module from the schema cache, uses `prost-reflect` to dynamically decode the
request body and verify it conforms to the expected message type. Returns `400` on
failure with a structured error body.

**`RoutingLayer`** — looks up the `RoutingRule` matching `x-wr-destination` in the
local routing table, injects the resolved `engine_address` into request extensions.
Returns `502` if no rule matches.

**`ForwardLayer`** — reads the resolved address from request extensions, strips
internal `x-wr-*` headers, forwards the original request.

### 3.4 Schema cache

The proxy maintains a `HashMap<(module, version), FileDescriptorSet>` populated
lazily on first access. Schemas are fetched by calling `GetSchema` on `ManagerService`
and decoded with `prost::Message::decode`. A TTL (default: 60s) triggers a background
refresh.

```rust
use prost::Message;
use prost_types::FileDescriptorSet;

let resp = client.get_schema(GetSchemaRequest {
    module:  "inventory-service".into(),
    version: "0.9.1".into(),
}).await?;

let fds = FileDescriptorSet::decode(resp.into_inner().proto_schema.as_ref())?;
```

### 3.5 Metrics accumulation

The proxy accumulates `RequestMetrics` in a bounded `tokio::sync::mpsc` queue.
A background task drains this queue and calls `ReportMetrics` on `ManagerService`
every 10 seconds.

```rust
client.report_metrics(ReportMetricsRequest { metrics: batch }).await?;
```

---

## Phase 4 — `wr-manager`

The manager implements `ManagerService` as a `tonic` gRPC server. It is the central
authority for engine registration, routing rules, schemas, and aggregated metrics.

### 4.1 Setup

```
cargo new --bin wr-manager
```

Dependencies:
```toml
tonic    = "0.12"
prost    = "0.13"
tokio    = { version = "1", features = ["full"] }
uuid     = { version = "1", features = ["v4"] }
anyhow   = "1"
wr-common = { path = "../wr-common" }
```

### 4.2 In-memory state

```rust
struct ManagerState {
    engines:       HashMap<String, EngineRegistration>,
    routing_table: RoutingTable,
    schemas:       HashMap<(String, String), Vec<u8>>,  // FileDescriptorSet bytes
    metrics:       Vec<RequestMetrics>,
}
```

Wrap in `Arc<RwLock<ManagerState>>` and share across the gRPC handler struct.

### 4.3 gRPC service implementation

Implement the `ManagerService` trait generated by `tonic_build`:

```rust
use wr_common::wruntime::manager_service_server::{ManagerService, ManagerServiceServer};

#[tonic::async_trait]
impl ManagerService for Manager {
    async fn register_engine(
        &self,
        request: Request<RegisterEngineRequest>,
    ) -> Result<Response<RegisterEngineResponse>, Status> {
        let reg = request.into_inner().registration.unwrap();
        let mut state = self.state.write().await;
        // Store schemas extracted from ModuleDescriptors
        for module in &reg.modules {
            state.schemas.insert(
                (module.name.clone(), module.version.clone()),
                module.proto_schema.clone(),
            );
        }
        state.engines.insert(reg.engine_id.clone(), reg);
        Ok(Response::new(RegisterEngineResponse { accepted: true }))
    }

    async fn get_routing_table(
        &self,
        _: Request<GetRoutingTableRequest>,
    ) -> Result<Response<GetRoutingTableResponse>, Status> {
        let state = self.state.read().await;
        Ok(Response::new(GetRoutingTableResponse {
            table: Some(state.routing_table.clone()),
        }))
    }

    // ... implement remaining RPCs
}
```

Start the server:
```rust
Server::builder()
    .add_service(ManagerServiceServer::new(manager))
    .serve(addr)
    .await?;
```

### 4.4 Routing table versioning

Every write to the routing table increments `RoutingTable::version`. Proxies
use this field for cache invalidation.

### 4.5 Engine status tracking

The manager marks an engine as `Unhealthy` if it hasn't received a `Heartbeat` RPC
within a configurable window (default: 30s). Engines call `Heartbeat` every 10 seconds
from a background task.

---

## Phase 5 — Integration and End-to-End Flow

### 5.1 Startup sequence

1. Start `wr-manager` — listens for gRPC on `0.0.0.0:9000`.
2. Start one or more `wr-proxy` instances — each establishes a gRPC connection to
   the manager and syncs its routing table.
3. Start one or more `wr-engine` instances — each registers via `RegisterEngine` RPC.

### 5.2 Request flow

```
WASM module (in wr-engine)
  │  HTTP request to "http://inventory-service/items"
  │
  ▼  [WasiHttpView::send_request intercepts]
  │  Adds headers:
  │    x-wr-source:      "order-service"
  │    x-wr-destination: "http://inventory-service/items"
  │  Rewrites URI to wr-proxy address
  │
  ▼
wr-proxy
  │  MetricsLayer records start time
  │  SchemaValidationLayer decodes body with prost-reflect, checks against
  │    FileDescriptorSet for inventory-service
  │  RoutingLayer resolves destination engine address
  │  ForwardLayer strips x-wr-* headers, forwards to destination engine
  │
  ▼
wr-engine (destination)
  │  Receives plain HTTP request
  │  Routes to the correct WASM module instance
  │
  ▼
WASM module (inventory-service)
```

### 5.3 Integration tests

Write integration tests in a `tests/` directory at the workspace root that:

- Spin up a `wr-manager` tonic server on a random port using `tokio::net::TcpListener`.
- Spin up a `wr-proxy` pointed at the test manager.
- Load a minimal WASM component that makes one HTTP call.
- Assert that the proxy received and routed the request correctly.
- Assert that `ReportMetrics` was called and metrics were recorded.

---

## Phase 6 — Schema Validation

### 6.1 Schema format

Each module's API is described by a **Protocol Buffer** (`.proto`) file. This is
compiled to a `FileDescriptorSet` binary using `protoc`:

```
protoc \
  --descriptor_set_out=inventory-service.binpb \
  --include_imports \
  inventory_service.proto
```

The `FileDescriptorSet` bytes are embedded in `ModuleDescriptor.proto_schema` when
the engine registers, and stored by the manager for distribution to proxies.

A minimal example module schema:
```protobuf
syntax = "proto3";
package inventory;

message GetItemsRequest  { string category = 1; }
message GetItemsResponse { repeated string items = 1; }

service InventoryService {
  rpc GetItems(GetItemsRequest) returns (GetItemsResponse);
}
```

### 6.2 Validation in `wr-proxy` with `prost-reflect`

`prost-reflect` provides a `DescriptorPool` that can load a `FileDescriptorSet` at
runtime, enabling dynamic message decoding without code generation.

```rust
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use prost::Message;
use prost_types::FileDescriptorSet;

// Build the pool from the cached FileDescriptorSet bytes
let fds = FileDescriptorSet::decode(schema_bytes)?;
let pool = DescriptorPool::from_file_descriptor_set(fds)?;

// Resolve the expected message type from the destination path
// e.g. x-wr-destination: "http://inventory-service/inventory.InventoryService/GetItems"
let message_desc: MessageDescriptor = pool
    .get_message_by_name("inventory.GetItemsRequest")
    .ok_or(Status::invalid_argument("unknown message type"))?;

// Attempt to decode the request body as that message type
let dynamic_msg = DynamicMessage::decode(message_desc, request_body)?;
// If decode succeeds, the body is structurally valid protobuf
```

On failure return `400` with:
```json
{
  "error": "schema_validation_failed",
  "detail": "...",
  "source": "order-service",
  "destination": "inventory-service"
}
```

### 6.3 Schema upload workflow

When an engine registers, the manager extracts the `proto_schema` bytes from each
`ModuleDescriptor` and stores them keyed by `(name, version)`. Proxies call `GetSchema`
on demand. Schemas can also be uploaded directly via the `UploadSchema` RPC to support
pre-registration of schemas before engines come online.

---

## Phase 7 — Configuration System

### 7.1 Engine config (`engine.toml`)

Already described in Phase 2.1. Add validation on load — fail fast if a referenced
`.wasm` file or `schema_path` does not exist.

### 7.2 Proxy config (`proxy.toml`)

```toml
listen_address  = "0.0.0.0:9001"
manager_address = "http://127.0.0.1:9000"  # gRPC

[cache]
routing_table_ttl_secs = 5
schema_ttl_secs        = 60

[metrics]
flush_interval_secs = 10
queue_depth         = 1000
```

### 7.3 Manager config (`manager.toml`)

```toml
listen_address                = "0.0.0.0:9000"
engine_heartbeat_timeout_secs = 30
```

---

## Dependency Summary

| Crate | Used in |
|-------|---------|
| `wasmtime` | `wr-engine` |
| `wasmtime-wasi` | `wr-engine` |
| `wasmtime-wasi-http` | `wr-engine` |
| `tonic` | `wr-common`, `wr-engine`, `wr-proxy`, `wr-manager` |
| `prost` | `wr-common`, `wr-engine`, `wr-proxy`, `wr-manager` |
| `prost-reflect` | `wr-proxy` |
| `prost-types` | `wr-proxy` (for `FileDescriptorSet`) |
| `tonic-build` | `wr-common` (build dep) |
| `tower` | `wr-proxy` |
| `hyper` | `wr-engine`, `wr-proxy` |
| `tokio` | all |
| `toml` | `wr-engine`, `wr-proxy`, `wr-manager` |
| `uuid` | `wr-common`, `wr-manager` |
