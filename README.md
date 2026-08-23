# Wruntime

> [!NOTE]
> This project was an experiment in LLM-assisted development. Much of the code
> in this repo was written with Claude.

A distributed runtime that networks WASM modules via transparent HTTP
interception. Modules make ordinary HTTP calls to each other — Wruntime
intercepts, routes, and delivers them automatically.

```
                                 ①  http://example.echo/echo.EchoService/Echo  
┌────────────┐                                ┌────────────┐
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

Modules address each other using `http://{namespace}.{module}/{proto_package}.{ProtoServiceName}/{ProtoMethodName}` URLs. The runtime handles service discovery, version routing, circuit-breaker-aware load balancing across instances, and OpenTelemetry tracing — all transparent to the module code. Request and response bodies are streamed through the proxy with zero buffering.

## Echo service API sketch

The snippets below illustrate the guest API; they are not standalone workspace crates. The runnable Echo component lives in [`examples/multi-node/echo`](examples/multi-node/echo) and is exercised by the multi-node Just recipes.

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
protoc --descriptor_set_out=schemas/echo.binpb --include_imports schemas/echo.proto
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

// The complete local world is shown in docs/agents/guest-module-author/module_template.md.
#[allow(dead_code, unused_imports)]
mod bindings {
    wit_bindgen::generate!({ path: "wit", world: "echo", generate_all });
}

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

// The complete local world is shown in docs/agents/guest-module-author/module_template.md.
#[allow(dead_code, unused_imports)]
mod bindings {
    wit_bindgen::generate!({ path: "wit", world: "caller", generate_all });
}

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
            Err(e)   => wr_sdk::log!("error: {e}"),
        }
    }
}
```

`WrClientGenerator` generates a typed `EchoServiceClient` struct with one method per RPC. The client calls `http://example.echo/echo.EchoService/Echo` under the hood via `wr_sdk::http::http_request`.

### 4. Run the executable example

The repository's multi-node example supplies the component crate, schema, engine/proxy configs, and invocation. Prepare Postgres and local certificates before running it:

```bash
just dev-up
just certs
just multi-node-inline
```

For an interactive topology that stays running, use `just multi-node`. Its Echo service is invoked through Node A and routed over mTLS to Node B. Manager-facing CLI commands use `https://127.0.0.1:9000` and, by default, the CA/client certificates under `certs/`.

## Host bindings

WASM modules can access host-provided capabilities through WIT interfaces:

| Binding | WIT | Preferred SDK surface | Description |
| --------- | ----- | ----------------------- | ------------- |
| **Database** | `wit/db.wit` | `wr_sdk::db` builders and owned rows | Parameterized SQL queries, transactions, and streaming through a per-namespace Postgres pool |
| **Blobstore** | `wit/blobstore.wit` | `wr_sdk::blobstore::bucket` scoped handle | S3-compatible object storage constrained to a host-configured bucket allowlist |
| **Tracing** | `wit/tracing.wit` | `span!`, `set_attrs!`, and `event!` | Typed OpenTelemetry spans and batched attributes from within modules |
| **LLM** | `wit/llm.wit` | `wr_sdk::llm::CompletionBuilder` | Validated Anthropic Claude completions, streaming, and tool use |

Prefer these facades for application code. `wr_sdk::bindings::wruntime::*` exposes the raw WIT bindings as an intentional escape hatch for unsupported operations and protocol/negative tests.

See [docs/host-bindings.md](docs/host-bindings.md) for configuration and usage examples.

## Deployment

Bundle once, deploy anywhere — the CLI packages cross-compiled binaries, WASM modules, and configs into a single tarball that works with both systemd and Docker. Shared settings (target, db_url, format, etc.) can live in a `wr-deploy.toml` so commands stay short.

```bash
# Bundle a node (proxy + engine) — target defaults to x86_64-unknown-linux-gnu
wr-cli node bundle --engine-config engine.toml

# Deploy to a remote host via SSH (format defaults to systemd)
wr-cli node deploy --node-id node-a wr-node-bundle.tar.gz deploy@10.0.1.50 \
    --db-url "postgres://postgres@10.0.1.1:5432/wruntime" \
    --manager https://10.0.1.1:9000

# Or with a wr-deploy.toml providing db_url, just the positional args:
wr-cli node deploy --node-id node-a wr-node-bundle.tar.gz deploy@10.0.1.50 \
    --manager https://10.0.1.1:9000
```

Every node deployment receives a manager-owned monotonic revision and exits successfully only after that exact bundle digest and engine-slot inventory is healthy and routable. Use `wr-cli node rollback --node-id <id> <remote>` for an explicit retained-release rollback and `wr-cli node inspect-bundle` for bundle inspection. Manager deployment follows the same packaging pattern. See [docs/deployment.md](docs/deployment.md) for lifecycle, release layout, and configuration details.

## Prerequisites

| Tool | Purpose |
| ------ | --------- |
| Rust + Cargo (stable) | Build all binaries |
| [`just`](https://github.com/casey/just) | Run project recipes (see `Justfile`) |
| `protoc` | Compile `.proto` schemas to `FileDescriptorSet` binaries |
| `wasm32-wasip2` target | `rustup target add wasm32-wasip2` — build WASM component modules |
| [`wasm-tools`](https://github.com/bytecodealliance/wasm-tools) | Strip/inspect WASM components (install: `cargo install --locked wasm-tools`) |

```bash
just build               # debug build
just build-release       # release build
just dev-up              # start Postgres/RustFS for integration tests and examples
just multi-node          # run two local proxy nodes and three engines
just test                # all tests with test DB/S3 env vars set
just test-wasm           # WASM host binding tests
just validate-ecommerce  # ecommerce inline run with zero-warning enforcement
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
├── wr-cli/                 # CLI: invoke modules, list engines/services, query metrics
├── wr-tests/               # integration tests
├── wit/                    # WIT interfaces (db, blobstore, tracing, llm)
├── examples/
│   ├── config/             # example single-node configs
│   ├── ecommerce/          # example: inventory (handler) + client (runner)
│   ├── codegen/            # example: LLM agent sandbox (code generation)
│   ├── stockmarket/        # example: multi-module trading system
│   └── multi-node/         # local and deployment multi-node topology
```

## Documentation

- [Agent guide](docs/agents/README.md) — choose guest module author or wruntime maintainer mode
- [Architecture](docs/architecture.md) — detailed system diagram, request flow, internal headers
- [Configuration](docs/configuration.md) — manager, proxy, and engine TOML configs; health checks; routing rules; multi-node setup
- [gRPC API](docs/grpc-api.md) — `ManagerService` and `NodeService` RPC reference, worker job queue API
- [Protobuf Schemas](docs/schemas.md) — writing, compiling, and validation behavior
- [Module SDK](docs/sdk.md) — `wr-sdk` + `wr-build` reference; handler and runner module guides
- [Host Bindings](docs/host-bindings.md) — database, blobstore, tracing, LLM, and filesystem access
- [Deployment](docs/deployment.md) — bundle, deploy, multi-node clusters, systemd and Docker
- [Testing](docs/testing.md) — running integration tests
