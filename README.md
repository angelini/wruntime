# Wruntime

A distributed runtime that networks WASM modules via transparent HTTP interception. Modules make ordinary HTTP calls to each other — Wruntime intercepts, validates, routes, and delivers them automatically.

```
┌────────────┐  ①  http://example.echo/Echo  ┌────────────┐
│   caller   │ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─►   │    echo    │
│   (WASM)   │        (appears direct)        │   (WASM)   │
└──────┬─────┘                                └──────▲─────┘
       │                                             │
       │ ② intercepted                   ④ routed   │
       │                                             │
       │         ┌─────────────────┐                 │
       └────────►│    wr-proxy     ├─────────────────┘
                 │                 │
                 │  routes         │
                 │  load-balances  │
                 │  streams        │
                 └────────┬────────┘
                          │ ③ syncs
                   ┌──────▼──────┐
                   │  wr-manager │
                   └─────────────┘
```

Modules address each other using `http://{namespace}.{module}/{Method}` URLs. The runtime handles service discovery, version routing, load balancing across instances, and OpenTelemetry tracing — all transparent to the module code. Request and response bodies are streamed through the proxy with zero buffering.

## Quick start: Echo service

Two WASM modules — **echo** returns whatever it receives, **caller** sends a message to echo and prints the result.

### 1. Define the schema

```protobuf
// schemas/echo.proto
syntax = "proto3";
package echo;

service EchoService {
  rpc Echo (EchoRequest) returns (EchoResponse);
}

message EchoRequest  { string message = 1; }
message EchoResponse { string message = 1; }
```

Compile it:

```bash
protoc --descriptor_set_out=schemas/echo.binpb --include_imports echo.proto
```

### 2. Echo module (handler)

`build.rs`:

```rust
fn main() {
    prost_build::Config::new()
        .service_generator(Box::new(wr_build::WrServiceGenerator))
        .compile_protos(&["schemas/echo.proto"], &["schemas"])
        .unwrap();
}
```

`src/lib.rs`:

```rust
mod proto { include!(concat!(env!("OUT_DIR"), "/echo.rs")); }

use wr_sdk::bindings::wasi::http::types::{IncomingRequest, ResponseOutparam};
use wr_sdk::io::{read_body, send_response};
use wr_sdk::ServiceError;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let path = request.path_with_query().unwrap_or_default();
        let body = read_body(request.consume().unwrap());
        let (status, resp) = proto::echo_service_router(&Component, &path, &body);
        send_response(response_out, status, resp);
    }
}

impl proto::EchoService for Component {
    fn echo(&self, req: proto::EchoRequest) -> Result<proto::EchoResponse, ServiceError> {
        Ok(proto::EchoResponse { message: req.message })
    }
}
```

`WrServiceGenerator` generates a trait (`EchoService`) and a router function (`echo_service_router`) from the proto definition — you implement the trait and wire up the router in `handle`.

### 3. Caller module (runner)

`build.rs`:

```rust
fn main() {
    prost_build::Config::new()
        .service_generator(Box::new(wr_build::WrClientGenerator))
        .compile_protos(&["schemas/echo.proto"], &["schemas"])
        .unwrap();
}
```

`src/lib.rs`:

```rust
mod proto { include!(concat!(env!("OUT_DIR"), "/echo.rs")); }

use proto::EchoServiceClient;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let client = EchoServiceClient::new("example.echo");

        match client.echo(proto::EchoRequest { message: "hello".into() }) {
            Ok(resp) => wr_sdk::io::send_response(response_out, 200, resp.encode_to_vec()),
            Err(e)   => wr_sdk::log::log(&format!("error: {e}")),
        }
    }
}
```

`WrClientGenerator` generates a typed `EchoServiceClient` struct with one method per RPC. The client calls `http://example.echo/Echo` under the hood via `wr_sdk::http::http_rpc`.

### 4. Configure and run

`engine.toml`:

```toml
listen_address  = "0.0.0.0:9100"
manager_address = "http://127.0.0.1:9000"

[node]
proxy_address = "http://127.0.0.1:9001"

[[module]]
name        = "echo"
namespace   = "example"
version     = "1.0.0"
wasm_path   = "target/wasm32-wasip2/debug/echo.wasm"
schema_path = "schemas/echo.binpb"

[[module]]
name        = "caller"
namespace   = "example"
version     = "1.0.0"
wasm_path   = "target/wasm32-wasip2/debug/caller.wasm"
schema_path = "schemas/echo.binpb"
```

```bash
# Build the WASM components
cargo component build -p echo
cargo component build -p caller

# Start the services (in separate terminals, or background them)
just manager
just proxy
just engine ./engine.toml

# Invoke the caller through the proxy
wr-cli invoke --destination http://example.caller/run
```

## Host bindings

WASM modules can access host-provided capabilities through WIT interfaces:

| Binding | WIT | Access via | Description |
|---------|-----|-----------|-------------|
| **Database** | `wit/db.wit` | `wr_sdk::bindings::wruntime::db::database` | Parameterized SQL queries and transactions against a shared Postgres pool |
| **Blobstore** | `wit/blobstore.wit` | `wr_sdk::bindings::wruntime::blobstore::store` | S3-compatible object storage (put, get, delete, list, head) |
| **Tracing** | `wit/tracing.wit` | `wr_sdk::bindings::wruntime::tracing::span` | Create and annotate OpenTelemetry spans from within modules |

See [docs/host-bindings.md](docs/host-bindings.md) for configuration and usage examples.

## Prerequisites

| Tool | Purpose |
|------|---------|
| Rust + Cargo (stable) | Build all binaries |
| [`just`](https://github.com/casey/just) | Run project recipes (see `Justfile`) |
| `protoc` | Compile `.proto` schemas to `FileDescriptorSet` binaries |
| `cargo-component` | Build WASM component modules |
| [`sccache`](https://github.com/mozilla/sccache) | Compilation cache — speeds up rebuilds and fresh clones (install: `cargo install sccache`) |

```bash
just build          # debug build
just build-release  # release build
just test           # all tests
```

## Project layout

```
wruntime/
├── proto/
│   └── wruntime.proto      # single source of truth for all gRPC messages
├── wr-common/              # generated proto types (tonic + prost); shared NodeConfig
├── wr-manager/             # central registry gRPC server
├── wr-proxy/               # streaming HTTP routing proxy
├── wr-engine/              # WASM runtime (wasmtime) + inbound HTTP server
├── wr-sdk/                 # WASM module SDK: http_rpc, io, log, export macros
├── wr-build/               # build.rs helper: service/client generators from proto
├── wr-cli/                 # CLI: invoke modules, list engines/services, query metrics
├── wr-tests/               # integration tests
├── wit/                    # WIT interfaces (db, blobstore, tracing)
├── examples/
│   ├── config/             # example single-node configs
│   ├── ecommerce/          # example: inventory (handler) + client (runner)
│   └── multi-node/         # example multi-node deployment
```

## Documentation

- [Architecture](docs/architecture.md) — detailed system diagram, request flow, internal headers
- [Configuration](docs/configuration.md) — manager, proxy, and engine TOML configs; health checks; routing rules; multi-node setup
- [gRPC API](docs/grpc-api.md) — `ManagerService` RPC reference (engine lifecycle, routing, schemas, metrics)
- [Protobuf Schemas](docs/schemas.md) — writing, compiling, and validation behavior
- [Module SDK](docs/sdk.md) — `wr-sdk` + `wr-build` reference; handler and runner module guides
- [Host Bindings](docs/host-bindings.md) — database, blobstore, tracing, and filesystem access
- [Testing](docs/testing.md) — running integration tests
