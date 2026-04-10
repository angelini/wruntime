# Wruntime

> [!NOTE]
> This project was an experiement in LLM assisted development. Much of the code in this repo was written with Claude.

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

use wr_sdk::prelude::*;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

impl ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        proto::echo_service_handle(&Component, request, response_out);
    }
}

impl proto::EchoService for Component {
    fn echo(&self, req: proto::EchoRequest) -> Result<proto::EchoResponse, ServiceError> {
        Ok(proto::EchoResponse { message: req.message })
    }
}
```

`WrServiceGenerator` generates a trait (`EchoService`) and a `_handle` function (`echo_service_handle`) from the proto definition — you implement the trait and delegate `handle` to the generated function.

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

use prost::Message;
use proto::EchoServiceClient;
use wr_sdk::prelude::*;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

impl ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let client = EchoServiceClient::new("example.echo");

        match client.echo(proto::EchoRequest { message: "hello".into() }) {
            Ok(resp) => send_response(response_out, 200, resp.encode_to_vec()),
            Err(e)   => wr_sdk::log::log(&format!("error: {e}")),
        }
    }
}
```

`WrClientGenerator` generates a typed `EchoServiceClient` struct with one method per RPC. The client calls `http://example.echo/Echo` under the hood via `wr_sdk::http::http_request`.

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
wr-cli --manager http://127.0.0.1:9000 invoke --destination http://example.caller/run
```

## Host bindings

WASM modules can access host-provided capabilities through WIT interfaces:

| Binding | WIT | Access via | Description |
|---------|-----|-----------|-------------|
| **Database** | `wit/db.wit` | `wr_sdk::bindings::wruntime::db::database` | Parameterized SQL queries and transactions against a shared Postgres pool |
| **Blobstore** | `wit/blobstore.wit` | `wr_sdk::bindings::wruntime::blobstore::store` | S3-compatible object storage (put, get, delete, list, head) |
| **Tracing** | `wit/tracing.wit` | `wr_sdk::bindings::wruntime::tracing::span` | Create and annotate OpenTelemetry spans from within modules |
| **LLM** | `wit/llm.wit` | `wr_sdk::bindings::wruntime::llm::inference` | Claude API (and other LLM providers) — completions, streaming, tool use |

See [docs/host-bindings.md](docs/host-bindings.md) for configuration and usage examples.

## Deployment

Bundle once, deploy anywhere — the CLI packages cross-compiled binaries, WASM modules, and configs into a single tarball that works with both systemd and Docker. Shared settings (target, db_url, format, etc.) can live in a `wr-deploy.toml` so commands stay short.

```bash
# Bundle a node (proxy + engine) — target defaults to x86_64-unknown-linux-gnu
wr-cli node bundle --engine-config engine.toml

# Deploy to a remote host via SSH (format defaults to systemd)
wr-cli node deploy wr-node-bundle.tar.gz deploy@10.0.1.50 \
    --db-url "postgres://postgres@10.0.1.1:5432/wruntime" \
    --manager http://10.0.1.1:9000

# Or with a wr-deploy.toml providing db_url, just the positional args:
wr-cli node deploy wr-node-bundle.tar.gz deploy@10.0.1.50 \
    --manager http://10.0.1.1:9000
```

Manager deployment follows the same pattern (`wr managers bundle` / `wr managers deploy`). See [docs/deployment.md](docs/deployment.md) for the deploy config reference, multi-node cluster setup, bundle structure, and template variables.

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
├── wr-sdk/                 # WASM module SDK: http, io, db, tracing, llm, export macros
├── wr-build/               # build.rs helper: service/client generators from proto
├── wr-cli/                 # CLI: invoke modules, list engines/services, query metrics (requires --manager or WR_MANAGER)
├── wr-tests/               # integration tests
├── wit/                    # WIT interfaces (db, blobstore, tracing, llm)
├── examples/
│   ├── config/             # example single-node configs
│   ├── ecommerce/          # example: inventory (handler) + client (runner)
│   ├── codegen/            # example: LLM agent sandbox (code generation)
│   ├── stockmarket/        # example: multi-module trading system
│   └── multi-node/         # example multi-node deployment
```

## Documentation

- [Architecture](docs/architecture.md) — detailed system diagram, request flow, internal headers
- [Configuration](docs/configuration.md) — manager, proxy, and engine TOML configs; health checks; routing rules; multi-node setup
- [gRPC API](docs/grpc-api.md) — `ManagerService` and `NodeService` RPC reference, worker job queue API
- [Protobuf Schemas](docs/schemas.md) — writing, compiling, and validation behavior
- [Module SDK](docs/sdk.md) — `wr-sdk` + `wr-build` reference; handler and runner module guides
- [Host Bindings](docs/host-bindings.md) — database, blobstore, tracing, LLM, and filesystem access
- [Deployment](docs/deployment.md) — bundle, deploy, multi-node clusters, systemd and Docker
- [Testing](docs/testing.md) — running integration tests
