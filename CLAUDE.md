# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

`just` is the task runner. Run `just` with no arguments to list all recipes.

```bash
# Build
just build           # debug build
just build-release   # release build
just check           # compile check only

# Test
just test            # all tests
just test-integration # wr-tests only
just test-one <name> # single test by name

# Lint & format
just fmt             # cargo fmt --all
just lint            # cargo clippy -D warnings
just tidy            # fmt + lint

# Run services (debug)
just manager
just proxy
just engine

# Dev infrastructure (Docker Compose — Postgres, Grafana/LGTM, Garage S3)
just dev-up
just dev-down
just dev-logs [service]

# Ecommerce example
just build-example   # compile WASM components + protobuf schemas
just example         # build-example + run
just example-inline  # build-example + run with single invocation, exits on failure
```

## Verification

After refactoring, always run `just tidy` and `just example-inline` to verify formatting, lints, and end-to-end correctness.

**Prerequisites:** `rustc`, `cargo`, `just`, `protoc` (for proto code generation). WASM module development additionally requires `cargo-component` and `wasm-tools`.

**Integration tests with a real DB:** set `WRUNTIME_TEST_DB_URL=postgres://postgres@localhost:5433/wruntime_test` before running tests (matches the `just dev-up` Postgres instance); omitting it skips DB-backed test cases.

## Architecture

Cargo workspace (`wr-common`, `wr-engine`, `wr-proxy`, `wr-manager`, `wr-cli`, `wr-tests`) implementing a distributed runtime that networks WASM modules via transparent HTTP interception.

### Three-Service System

| Service | Default Port | Role |
|---|---|---|
| `wr-manager` | 9000 (gRPC) | Central registry — routing table, schemas, metrics, heartbeat monitor |
| `wr-proxy` | 9001 (HTTP) | Intercepts inter-module traffic, validates schemas, routes to engines |
| `wr-engine` | 9100 (HTTP) | Runs WASM modules via wasmtime WASI component model |

### Request Flow

1. A WASM module makes an HTTP call to another module (e.g., `http://ecommerce.inventory/items`)
2. `WasiHttpView` intercepts it, attaches `x-wr-source` / `x-wr-destination` (format: `namespace.module`) headers, rewrites the URI to point at `wr-proxy`
3. `wr-proxy` validates the body against a cached protobuf schema, resolves the destination engine from its cached routing table, injects `x-wr-module` / `x-wr-namespace` / `x-wr-version`, then forwards to the target `wr-engine`
4. The destination `wr-engine` dispatches to the correct WASM instance via `ModuleRegistry` (round-robin across instances)

### Key Design Details

**Module identity** — every module is identified by the triple `(namespace, name, version)`. This tuple is used for routing table lookups, schema storage, and engine registry dispatch.

**`wr-common`** — generated gRPC types from `proto/wruntime.proto` via `tonic-build` in `build.rs`. Shared by all other crates.

**`wr-proxy` middleware stack** (Tower layers, evaluated in order):
1. `TracingLayer` — root OTel span per request (captures source, destination, status, duration)
2. `SchemaValidationLayer` — validates protobuf bodies via `prost-reflect`; rejects with structured JSON errors; skipped if no schema cached
3. `RoutingLayer` — resolves destination engine from local routing table cache (TTL-based); injects `ResolvedDestination` as a request extension
4. `ForwardService` — reads `ResolvedDestination` extension, strips internal headers, proxies to engine

**`wr-engine`** — uses wasmtime 41 with the WASI component model. On startup: loads WASM components → registers with manager → starts 10-second heartbeat loop. Modules can optionally have a PostgreSQL pool (`deadpool-postgres`) and a blobstore (S3-compatible via `rust-s3`) exposed to WASM via custom host bindings.

**`wr-manager` state** — pure in-memory (`state.rs`). No persistence; rebuilt from engine re-registrations after restart. Background task monitors heartbeats every 10 seconds — marks routing rules unhealthy and bumps the routing table version when an engine times out (default 30 s).

**`wr-proxy` sync** — two background tasks: `sync_routing_table()` polls manager every `routing_table_ttl_secs`; `sync_schemas()` fetches module schemas on demand. Request metrics are collected via OpenTelemetry traces (no custom metrics pipeline).

**Schemas** — stored as compiled protobuf `FileDescriptorSet` bytes (`.binpb` files). Declared per module in `engine.toml`; uploaded to the manager on engine registration; fetched by the proxy on demand.

This project targets WASI Preview 2 and all guest WASM modules should be built to target Preview 2.

### WIT Host Bindings (async)

Host interfaces are defined under `wit/` (`db.wit`, `blobstore.wit`, `tracing.wit`) and implemented in `wr-engine`. All host bindings use async — the `bindgen!` macro is invoked with `imports: { default: async }`, and every `Host` / `HostTransaction` trait method is `async fn`. Do not use `block_in_place` or `block_on` in host implementations.

```rust
wasmtime::component::bindgen!({
    path:  "../wit/db.wit",
    world: "db-access",
    imports: { default: async },
    // ...
});

impl Host for ModuleState {
    async fn query(&mut self, sql: String, params: Vec<PgValue>) -> Result<Vec<Row>, DbError> {
        // ...
    }
}
```

### Configuration

Each service reads a TOML config file. Examples in `examples/config/` (`manager.toml`, `proxy.toml`, `engine.toml`). Modules and their optional `.binpb` schemas are declared under `[[module]]` in `engine.toml`.

### Integration Tests

`wr-tests/tests/integration_test.rs` spins up all three services in-process on ephemeral ports. Helpers in `tests/helpers.rs` provide `start_manager()`, `start_proxy()`, `stub_engine()`, and schema/payload builders. Tests cover: manager RPC operations, proxy routing (including round-robin across multiple engines), schema validation, pass-through when no schema is cached, TOML config parsing, and DB/blobstore host bindings.

DB tests that call host methods must `.await` them — all host trait methods are async:

```rust
let rows = state.query("SELECT 1".into(), vec![]).await.expect("query");
```

### Examples

`examples/ecommerce/` contains two WASM components (separate Cargo workspaces, excluded from the main workspace):
- **inventory** — PostgreSQL-backed service (seed, stock check, buy, return)
- **client** — drives 100 buy/return transactions against inventory via `http://ecommerce.inventory/...`

Multiple engine configs (`engine-inventory-1.toml`, `engine-inventory-2.toml`, `engine-client.toml`) demonstrate running several engine instances with load-balanced routing.

`examples/multi-node/` contains `node-a/` and `node-b/` config directories for multi-node deployments.

`examples/config/` contains the base service configs (`manager.toml`, `proxy.toml`, `engine.toml`).
