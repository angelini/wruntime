# Wruntime

A distributed WASI Preview 2 runtime for building networks of WebAssembly modules.
Guests call logical HTTP endpoints such as
`http://ecommerce.inventory/inventory.InventoryService/GetItems`; Wruntime
intercepts the call, discovers a healthy module instance, and routes it locally
or across nodes.

> [!NOTE]
> Wruntime is an experimental, pre-1.0 project built in part through
> LLM-assisted development. The custom `wruntime:*` guest WIT APIs may change
> incompatibly; pin the runtime and SDK versions used by guest modules.

```mermaid
flowchart LR
    subgraph data_plane["Streaming data plane"]
        caller["Caller WASM"] -->|"logical HTTP"| source_engine["Source wr-engine<br/>WASI HTTP interception"]
        source_engine -->|"loopback"| proxy_a["wr-proxy A"]
        proxy_a --> route{"Selected instance<br/>location"}
        route -->|"local"| local_engine["Local wr-engine"]
        route -->|"peer"| proxy_b["wr-proxy B"]
        proxy_b -->|"local"| remote_engine["Remote wr-engine"]
        local_engine --> destination["Destination WASM"]
        remote_engine --> destination
    end

    subgraph control_plane["Control plane"]
        postgres[("Shared PostgreSQL")] <--> managers["Active-active<br/>wr-manager cluster"]
    end

    proxy_a -.->|"engine registration, readiness,<br/>and deployment identity"| managers
    managers -.->|"route sync and scheduled jobs"| proxy_a
```

The proxy streams internal and cross-node request and response bodies without
buffering. Public ingress is a separate trust boundary: it strips reserved
headers, buffers one bounded request body, and transcodes supported protobuf,
canonical protobuf JSON, or flat form input to validated protobuf wire bytes.

## Scope

Wruntime currently provides:

- logical service discovery, semantic-version routing, load balancing, circuit
  breaking, and OpenTelemetry tracing;
- multi-node peer routing over mTLS and active-active managers with PostgreSQL
  persistence and gossip-based liveness;
- protobuf service modules plus durable, at-least-once workers and schedules;
- optional public ingress with request transcoding, schema validation, and deny-by-default,
  allowlisted external HTTP egress;
- guest capabilities for PostgreSQL, S3-compatible blob storage, tracing,
  Anthropic Claude, namespace-scoped secrets/environment values, and ephemeral
  scratch filesystems;
- systemd and Docker deployment bundles, exact-revision readiness checks,
  retained-release rollback, and coherent cluster status.

See [Architecture](docs/architecture.md) for the full request and control-plane
flows.

## Quick start

A source checkout requires:

- stable Rust and Cargo;
- [`just`](https://github.com/casey/just), `protoc`, and Python 3;
- the `wasm32-wasip2` Rust target and
  [`wasm-tools`](https://github.com/bytecodealliance/wasm-tools);
- Docker with Compose for PostgreSQL, RustFS, and local observability services;
- OpenSSL for local example secrets.

```bash
rustup target add wasm32-wasip2

just dev-up
just certs
just multi-node-inline
```

A successful run sends an Echo request through Node A, across an mTLS peer
connection, to a WASM module on Node B. Stop the shared development services
with `just dev-down`. See the [multi-node example](examples/multi-node/) for the
interactive topology and port map.

## Build a guest module

Start with the [Guest Module Author guide](docs/agents/guest-module-author/README.md):

- [module template](docs/agents/guest-module-author/module_template.md) — current
  manifest, WIT world, build script, and validation shape;
- [API guide](docs/agents/guest-module-author/api_guide.md) — preferred SDK usage
  and lifecycle semantics;
- [worked examples](docs/agents/guest-module-author/examples.md) — production
  patterns and their supporting configuration.

Exact guest contracts live in [`wr-sdk/src/`](wr-sdk/src/),
[`wr-build/src/lib.rs`](wr-build/src/lib.rs), and root [`wit/`](wit/). Prefer the
SDK facades and generated protobuf clients over raw WIT bindings unless an
operation is not otherwise exposed.

## Executable examples

| Example | Demonstrates |
| --- | --- |
| [Ecommerce](examples/ecommerce/) | Generated client/service calls, PostgreSQL migrations, load balancing, tracing |
| [Stockmarket](examples/stockmarket/) | Multiple services, persistence, and configurable replicas |
| [Codegen](examples/codegen/) | Workers, LLM, database, blobstore, egress, and scratch filesystem |
| [Multi-node](examples/multi-node/) | Cross-node placement and mTLS peer routing |

Use `just` with no arguments to list every build, test, example, and deployment
validation recipe. Common maintainer commands are:

```bash
just build
just tidy
just test
just test-wasm
just validate-ecommerce
```

Testing prerequisites and the change-sensitive validation matrix are linked
from [Testing](docs/testing.md). To inspect the repository-local CLI, run
`just cli --help`.

## Repository map

| Path | Purpose |
| --- | --- |
| `wr-manager/` | Active-active registry, routing state, schedules, secrets, and cluster status |
| `wr-proxy/` | Local/peer routing, public ingress, egress policy, and circuit breaking |
| `wr-engine/` | Wasmtime component execution, host capabilities, workers, and module lifecycle |
| `wr-sdk/`, `wr-sdk-macros/`, `wr-build/` | Guest SDK, macros, and protobuf service/client generators |
| `wr-cli/` | Development, operations, certificate, and deployment CLI |
| `wr-common/`, `proto/`, `wit/` | Shared Rust types, control-plane protobuf, and guest host ABI |
| `wr-tests/` | Integration and WASM host-binding tests |
| `examples/` | Executable guest applications and local topologies |
| `docs/` | Public guides, references, and contributor workflows |

## Documentation

- [Architecture](docs/architecture.md) — topology, trust boundaries, request flow,
  clustering, workers, and schedules
- [Configuration](docs/configuration.md) — manager, proxy, engine, module, ingress,
  egress, and capability configuration
- [Module SDK](docs/sdk.md) — `wr-sdk` and `wr-build` overview
- [Host bindings](docs/host-bindings.md) — database, blobstore, tracing, LLM,
  environment, and filesystem behavior
- [Schemas](docs/schemas.md) — protobuf descriptors, validation, and RPC paths
- [Control-plane and job APIs](docs/grpc-api.md) — manager/node gRPC and worker HTTP
  RPC contracts
- [Deployment](docs/deployment.md) — bundles, systemd/Docker lifecycle, mTLS,
  rollback, and cluster status
- [Testing](docs/testing.md) — local infrastructure, focused tests, and full
  validation
- [Contributor modes](docs/agents/README.md) — guest-module and runtime-maintainer
  workflows
