# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Build all workspace crates
cargo build --release

# Run tests (integration tests only, no external deps needed)
cargo test

# Run a single test
cargo test <test_name>

# Run tests for a specific crate
cargo test -p wr-tests

# Check for compile errors without building
cargo check

# Run individual services
cargo run -p wr-manager -- --config examples/config/manager.toml
cargo run -p wr-proxy   -- --config examples/config/proxy.toml
cargo run -p wr-engine  -- --config examples/config/engine.toml

# CLI management tool
cargo run -p wr-cli -- --manager http://127.0.0.1:9000 engines list
cargo run -p wr-cli -- --manager http://127.0.0.1:9000 engines get <engine_id>
cargo run -p wr-cli -- --manager http://127.0.0.1:9000 metrics
```

**Prerequisites:** `rustc`, `cargo`, `protoc` (for proto code generation). WASM module development additionally requires `cargo-component` and `wasm-tools`.

**Integration tests with a real DB:** set `WRUNTIME_TEST_DB_URL=postgres://...` before running tests; omitting it skips DB-backed test cases.

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
1. `MetricsLayer` — records request start time
2. `SchemaValidationLayer` — validates protobuf bodies via `prost-reflect`; rejects with structured JSON errors; skipped if no schema cached
3. `RoutingLayer` — resolves destination engine from local routing table cache (TTL-based); injects `ResolvedDestination` as a request extension
4. `ForwardService` — reads `ResolvedDestination` extension, strips internal headers, proxies to engine

**`wr-engine`** — uses wasmtime 41 with the WASI component model. On startup: loads WASM components → registers with manager → starts 10-second heartbeat loop. Modules can optionally have a PostgreSQL pool (`deadpool-postgres`) exposed to WASM via custom `wruntime::db::database` bindings.

**`wr-manager` state** — pure in-memory (`state.rs`). No persistence; rebuilt from engine re-registrations after restart. Background task monitors heartbeats every 10 seconds — marks routing rules unhealthy and bumps the routing table version when an engine times out (default 30 s).

**`wr-proxy` sync** — two background tasks: `sync_routing_table()` polls manager every `routing_table_ttl_secs`; `flush_metrics()` batches and sends `RequestMetrics` every `flush_interval_secs`.

**Schemas** — stored as compiled protobuf `FileDescriptorSet` bytes (`.binpb` files). Declared per module in `engine.toml`; uploaded to the manager on engine registration; fetched by the proxy on demand.

### Configuration

Each service reads a TOML config file. Examples in `examples/config/` (`manager.toml`, `proxy.toml`, `engine.toml`). Modules and their optional `.binpb` schemas are declared under `[[module]]` in `engine.toml`.

### Integration Tests

`wr-tests/tests/integration_test.rs` spins up all three services in-process on ephemeral ports. Helpers in `tests/helpers.rs` provide `start_manager()`, `start_proxy()`, `stub_engine()`, and schema/payload builders. Tests cover: manager RPC operations, proxy routing (including round-robin across multiple engines), schema validation, pass-through when no schema is cached, and TOML config parsing.

### Examples

`examples/ecommerce/` contains two WASM components (separate Cargo workspaces, excluded from the main workspace):
- **inventory** — PostgreSQL-backed service (seed, stock check, buy, return)
- **client** — drives 100 buy/return transactions against inventory via `http://ecommerce.inventory/...`

Multiple engine configs (`engine-inventory-1.toml`, `engine-inventory-2.toml`, `engine-client.toml`) demonstrate running several engine instances with load-balanced routing.

`examples/multi-node/` contains `node-a/` and `node-b/` config directories for multi-node deployments.

`examples/config/` contains the base service configs (`manager.toml`, `proxy.toml`, `engine.toml`).
