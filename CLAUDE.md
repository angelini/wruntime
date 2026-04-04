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
just test-wasm       # build WASM test guests + run host binding tests

# Lint & format
just fmt             # cargo fmt --all
just lint            # cargo clippy -D warnings
just tidy            # fmt + lint

# Run services (debug)
just manager
just proxy
just engine

# Dev infrastructure (Docker Compose — Postgres, Grafana/LGTM, RustFS S3)
just dev-up
just dev-down
just dev-logs [service]

# Ecommerce example
just build-ecommerce   # compile WASM components + protobuf schemas
just ecommerce         # build + run
just ecommerce-inline  # build + run with single invocation, exits on failure

# Codegen example (LLM agent sandbox)
just build-codegen   # compile WASM components + protobuf schemas
just codegen         # build + run
just codegen-inline  # build + run with single invocation, exits on failure
```

## Verification

After refactoring, always run `just tidy` and `just ecommerce-inline` to verify formatting, lints, and end-to-end correctness. Treat any `WARN` log lines in `just ecommerce-inline` output as bugs that need to be fixed — a clean run should produce zero warnings. When changing host bindings (`wr-engine/src/db.rs`, `wr-engine/src/blobstore.rs`, `wr-engine/src/tracing.rs`), WIT interfaces (`wit/`), the SDK (`wr-sdk/`), or the WASM guest test harness (`wr-tests/guests/`, `wr-tests/tests/wasm_host_test.rs`), also run `just test-wasm`.

**Keep docs in sync with code changes.** When modifying architecture (adding/removing layers, changing request flow, changing config), update `CLAUDE.md`, `README.md`, and the relevant files in `docs/` (`architecture.md`, `configuration.md`, `schemas.md`, etc.) in the same change. Stale docs are worse than no docs. **When modifying `wr-sdk/`, `wr-build/`, or `wit/` interfaces, also update `docs/agents/api_reference.md`** — this is the authoritative API reference used by agents building guest modules.

### Agent Documentation (`docs/agents/`)

`docs/agents/` contains structured documentation for AI agents building WASM guest modules. When creating new guest modules, consult these docs for templates, API signatures, and patterns. The key files are:

- `module_template.md` — fill-in-the-blank skeleton for new modules
- `api_reference.md` — exact function signatures for all guest-callable APIs (**must be kept in sync with code**)
- `constraints.md` — hard rules and common mistakes
- `decision_matrix.md` — choose handler vs. handler+client
- `codegen.md` — proto-to-Rust code generation mapping
- `examples.md` — index of real code in the repo

**Prerequisites:** `rustc`, `cargo`, `just`, `protoc` (for proto code generation), `sccache` (compilation cache — `cargo install sccache`). WASM module development additionally requires `cargo-component` and `wasm-tools`.

**Integration tests with a real DB:** set `WRT_TEST_DB_URL=postgres://postgres@localhost:5433/wruntime_test` before running tests (matches the `just dev-up` Postgres instance); omitting it skips DB-backed test cases. `just test-wasm` sets all required env vars (DB + S3) automatically using the `just dev-up` defaults.

## Architecture

Cargo workspace (`wr-common`, `wr-engine`, `wr-proxy`, `wr-manager`, `wr-cli`, `wr-tests`) implementing a distributed runtime that networks WASM modules via transparent HTTP interception.

### Three-Service System

| Service | Default Port | Role |
|---|---|---|
| `wr-manager` | 9000 (gRPC) + 9010 (gossip) | Registry — routing table, schemas, heartbeat monitor. Runs active-active; chitchat gossip for manager liveness |
| `wr-proxy` | 9001 (HTTP) + 9002 (gRPC control plane) | Streaming header-based router — intercepts inter-module traffic, routes to engines; bodies flow through without buffering. Control plane handles engine registration/heartbeats |
| `wr-engine` | 9100 (HTTP) | Runs WASM modules via wasmtime WASI component model |

### Request Flow

1. A WASM module makes an HTTP call to another module (e.g., `http://ecommerce.inventory/items`)
2. `WasiHttpView` intercepts it, attaches `x-wr-source` / `x-wr-destination` (format: `namespace.module`) headers, rewrites the URI to point at `wr-proxy`
3. `wr-proxy` resolves the destination engine from its cached routing table (header-only inspection), injects `x-wr-module` / `x-wr-namespace` / `x-wr-version`, then streams the request body through to the target `wr-engine`
4. The destination `wr-engine` dispatches to the correct WASM instance via `ModuleRegistry` (round-robin across instances)

### Key Design Details

**Module identity** — every module is identified by the triple `(namespace, name, version)`. This tuple is used for routing table lookups, schema storage, and engine registry dispatch.

**`wr-common`** — generated gRPC types from `proto/wruntime.proto` via `tonic-build` in `build.rs`. Shared by all other crates.

**`wr-proxy` middleware stack** (Tower layers, evaluated in order):
1. `TracingLayer` — root OTel span per request (captures source, destination, status, duration)
2. `RoutingLayer` — single routing table read per request; resolves destination engine from local routing table cache (TTL-based); injects `ResolvedDestination` as a request extension; when egress is enabled and no internal route matches, sets `ExternalEgress` extension
3. `EgressLayer` — handles `ExternalEgress` requests (domain allowlist, external forward); passes internal requests through
4. `ForwardService` — reads `ResolvedDestination` extension, strips internal headers, streams request/response bodies to/from engine without buffering

The proxy uses a custom `ProxyBody` type that wraps `hyper::body::Incoming` behind a `Pin<Box<dyn Body + Send>>`, enabling streaming without the `Sync` requirement that `BoxBody` imposes. All layers only inspect headers — bodies flow through untouched.

**`wr-engine`** — uses wasmtime 43 with the WASI component model. On startup: provisions DB schemas → runs migrations (via `refinery`) → loads WASM components → registers with manager → starts 10-second heartbeat loop. Modules can optionally have a PostgreSQL pool (`deadpool-postgres`) and a blobstore (S3-compatible via `rust-s3`) exposed to WASM via custom host bindings.

**Database migrations** — modules can declare `migrations_path` in `engine.toml` pointing to a directory of `V{n}__description.sql` files. Migrations run on the engine (host side) at startup using [refinery](https://github.com/rust-db/refinery) with tokio-postgres. Each module's migrations are schema-isolated (`search_path` set to the module's schema only) and serialized across engine replicas via Postgres advisory locks. Routing rules are not registered until migrations complete. See `docs/configuration.md` for details.

**`wr-manager` state** — persisted to Postgres (`db.rs`). Engines, routing rules, schemas, and secrets are stored in database tables; ephemeral state (heartbeats, module health timestamps) remains in-memory (`state.rs`). Migrations run automatically on startup (`migrate.rs`). Multiple manager instances can run active-active — concurrent writes are serialized via `SELECT ... FOR UPDATE NOWAIT` on a lock sentinel row. Each manager registers in the `wr_managers` table, heartbeats every 15 s, and participates in a [chitchat](https://docs.rs/chitchat) gossip mesh (UDP) for phi-accrual failure detection. The `ListManagers` gRPC RPC returns all healthy managers, enabling peer discovery from any seed. Manager config requires a `[cluster]` section with `cluster_id` and `gossip_listen_address`. Background task monitors engine heartbeats every 5 seconds — marks routing rules unhealthy and bumps the routing table version when an engine times out (default 30 s).

**`wr-cli`** — requires `--manager` (or `WR_MANAGER` env var) pointing at any single manager. The CLI does **not** have database access — all communication is via gRPC. Use `ListManagers` RPC for peer discovery from a seed address. Deployment commands: `wr managers init|bundle|deploy|status` for manager deployment, `wr node init|bundle|deploy|status` for engine+proxy node deployment. Both support systemd and Docker deployment formats.

**`wr-proxy` sync** — one background task: `sync_routing_table()` polls manager every `routing_table_ttl_secs`. Request metrics are collected via OpenTelemetry traces (no custom metrics pipeline).

**Schemas** — stored as compiled protobuf `FileDescriptorSet` bytes (`.binpb` files). Declared per module in `engine.toml`; uploaded to the manager on engine registration. Schemas are used for code generation (`wr-build`) and discovery, but are **not** validated by the proxy at runtime — the proxy is a streaming router that never inspects request or response bodies.

This project targets WASI Preview 2 and all guest WASM modules should be built to target Preview 2.

### WIT Host Bindings (async)

Host interfaces are defined under `wit/` (`db.wit`, `blobstore.wit`, `tracing.wit`, `llm.wit`) and implemented in `wr-engine`. All host bindings use async — the `bindgen!` macro is invoked with `imports: { default: async }`, and every `Host` / `HostTransaction` trait method is `async fn`. Do not use `block_in_place` or `block_on` in host implementations.

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

`wr-tests/tests/integration_test.rs` spins up all three services in-process on ephemeral ports. Helpers in `tests/helpers.rs` provide `start_manager()`, `start_proxy()`, `stub_engine()`, and schema/payload builders. Tests cover: manager RPC operations, proxy routing (including round-robin across multiple engines), cross-node forwarding, egress, external ingress, TOML config parsing, and DB/blobstore host bindings.

DB tests that call host methods must `.await` them — all host trait methods are async:

```rust
let rows = state.query("SELECT 1".into(), vec![]).await.expect("query");
```

### Examples

`examples/ecommerce/` contains two WASM components (separate Cargo workspaces, excluded from the main workspace):
- **inventory** — PostgreSQL-backed service (seed, stock check, buy, return)
- **client** — drives 100 buy/return transactions against inventory via `http://ecommerce.inventory/...`

Multiple engine configs (`engine-inventory-1.toml`, `engine-inventory-2.toml`, `engine-client.toml`) demonstrate running several engine instances with load-balanced routing.

`examples/codegen/` contains an LLM agent sandbox example — a WASM module that uses the LLM host binding to run code generation tasks.

`examples/stockmarket/` contains a multi-module trading system example with multiple interacting services.

`examples/multi-node/` contains `node-a/` and `node-b/` config directories for multi-node deployments.

`examples/config/` contains the base service configs (`manager.toml`, `proxy.toml`, `engine.toml`).
