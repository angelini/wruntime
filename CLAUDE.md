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

# Dev workflow (continuous compilation via bacon)
just watch                  # cargo build + all example WASM guests (re-runs on file save)
just watch check            # cargo check only
just watch clippy           # clippy -D warnings
just watch test             # cargo test
just watch build-ecommerce  # WASM ecommerce guests only
just watch build-codegen    # WASM codegen guests only
just watch build-stockmarket # WASM stockmarket guests only

# Certificates (required before running services)
just certs             # generate local CA + localhost certs

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

# Stockmarket example
just build-stockmarket       # compile WASM components + protobuf schemas
just stockmarket             # build + run (1 exchange engine)
just stockmarket exchanges=3 # build + run with N exchange engines
just stockmarket-inline      # build + run single invocation, exits on failure
```

## Verification

After refactoring, always run `just tidy` and `just ecommerce-inline` to verify formatting, lints, and end-to-end correctness. Treat any `WARN` log lines in `just ecommerce-inline` output as bugs that need to be fixed — a clean run should produce zero warnings. When changing host bindings (`wr-engine/src/db.rs`, `wr-engine/src/blobstore.rs`, `wr-engine/src/tracing.rs`), WIT interfaces (`wit/`), the SDK (`wr-sdk/`), or the WASM guest test harness (`wr-tests/guests/`, `wr-tests/tests/wasm_host_test.rs`), also run `just test-wasm`.

**Keep docs in sync with code changes.** When modifying architecture (adding/removing layers, changing request flow, changing config), update `CLAUDE.md`, `README.md`, and the relevant files in `docs/` (`architecture.md`, `configuration.md`, `schemas.md`, etc.) in the same change. **When modifying `wr-sdk/`, `wr-build/`, or `wit/` interfaces, also update `docs/agents/api_reference.md`.**

### Agent Documentation (`docs/agents/`)

`docs/agents/` contains structured documentation for AI agents building WASM guest modules. Key files: `module_template.md`, `api_reference.md` (must stay in sync with code), `constraints.md`, `decision_matrix.md`, `codegen.md`, `examples.md`.

**Prerequisites:** `rustc`, `cargo`, `just`, `protoc`, `sccache`, `taplo`. WASM modules additionally require the `wasm32-wasip2` target (`rustup target add wasm32-wasip2`) and `wasm-tools`. Cross-compilation requires `zig` and `cargo-zigbuild`.

**Integration tests with a real DB:** set `WRT_TEST_DB_URL=postgres://postgres@localhost:5433/wruntime_test` (matches `just dev-up`); omitting it skips DB-backed tests. `just test-wasm` sets all required env vars automatically.

## Architecture

Cargo workspace (`wr-common`, `wr-engine`, `wr-proxy`, `wr-manager`, `wr-cli`, `wr-tests`) implementing a distributed runtime that networks WASM modules via transparent HTTP interception.

### Three-Service System

| Service | Default Port | Role |
|---|---|---|
| `wr-manager` | 9000 (mTLS gRPC) + 9010 (gossip) | Registry — routing table, schemas, heartbeats. Active-active with chitchat gossip. State persisted to Postgres (`wr_system` schema) |
| `wr-proxy` | 9001 (HTTP, loopback) + 9002 (gRPC control, loopback) + 9443 (mTLS peer) | Streaming header-based router — inspects headers only, bodies flow through untouched |
| `wr-engine` | 9100 (HTTP, loopback) | Runs WASM modules via wasmtime WASI component model |

### Request Flow

1. WASM module makes HTTP call (e.g., `http://ecommerce.inventory/items`)
2. `WasiHttpView` intercepts, attaches `x-wr-source` / `x-wr-destination` headers, rewrites URI to proxy
3. Proxy resolves destination engine from cached routing table, streams request through
4. Destination engine dispatches to correct WASM instance via `ModuleRegistry` (round-robin)

### Key Design Details

**Module identity** — `(namespace, name, version)` triple used for routing, schema storage, and dispatch.

**mTLS** — all inter-service network traffic uses mutual TLS. Proxy loopback listener (`:9001`) is plain HTTP; cross-node traffic uses the mTLS peer listener (`:9443`). `just certs` generates localhost certificates for local dev.

**`wr-engine`** — on startup: registers with manager → receives per-namespace DB credentials → provisions schemas/roles → runs migrations → builds connection pools → loads WASM components → starts heartbeat loop. DB-enabled modules connect through per-namespace roles (`wr_ns_{namespace}`). Guest roles are never granted access to the `wr_system` schema, so WASM modules cannot read manager system tables.

**Database migrations** — `migrations_path` in `engine.toml`, `V{n}__description.sql` files, run via refinery at startup. Schema-isolated and serialized across replicas via advisory locks. See `docs/configuration.md`.

**`wr-cli`** — all communication via gRPC to manager (`--manager` or `WR_MANAGER`). No direct DB access. TLS cert args default to `certs/` for local dev.

**Schemas** — compiled protobuf `FileDescriptorSet` (`.binpb`) uploaded on registration. Used for codegen/discovery, **not** validated by proxy at runtime.

This project targets WASI Preview 2.

### WIT Host Bindings (async)

Host interfaces defined under `wit/` and implemented in `wr-engine`. All host bindings use async — `bindgen!` with `imports: { default: async }`. Do not use `block_in_place` or `block_on` in host implementations.

```rust
wasmtime::component::bindgen!({
    path:  "../wit/db.wit",
    world: "db-access",
    imports: { default: async },
});

impl Host for ModuleState {
    async fn query(&mut self, sql: String, params: Vec<PgValue>) -> Result<Vec<Row>, DbError> {
        // ...
    }
}
```

### Configuration

Each service reads a TOML config file. Examples in `examples/config/`. Modules declared under `[[module]]` in `engine.toml`. All services require TLS config (`[node.tls]` or `[tls]`) with `cert_path`, `key_path`, `ca_cert_path`.

### Integration Tests

`wr-tests/tests/` — spins up all services in-process on ephemeral ports. Helpers in `tests/helpers.rs` provide `start_manager()`, `start_proxy()`, `stub_engine()`, and schema/payload builders. DB host method calls must be `.await`ed (async trait methods).

### Examples

- `examples/ecommerce/` — inventory + client modules, multiple engine instances with load-balanced routing
- `examples/stockmarket/` — exchange + ledger + simulator, supports N parallel exchange engines
- `examples/codegen/` — LLM agent sandbox using the LLM host binding
- `examples/multi-node/` — multi-node deployment configs
- `examples/config/` — base service configs
