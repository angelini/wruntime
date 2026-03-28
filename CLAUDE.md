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
cargo run -p wr-manager -- --config manager.toml
cargo run -p wr-proxy   -- --config proxy.toml
cargo run -p wr-engine  -- --config engine.toml
```

**Prerequisites:** `rustc`, `cargo`, `protoc` (for proto code generation). WASM module development additionally requires `cargo-component` and `wasm-tools`.

## Architecture

This is a Cargo workspace (`wr-common`, `wr-engine`, `wr-proxy`, `wr-manager`, `wr-tests`) that implements a distributed runtime for networking WASM modules together via transparent HTTP interception.

### Three-Service System

| Service | Default Port | Role |
|---|---|---|
| `wr-manager` | 9000 (gRPC) | Central registry — routing table, schemas, metrics |
| `wr-proxy` | 9001 (HTTP) | Intercepts inter-module traffic, validates schemas, routes to engines |
| `wr-engine` | 9100 (HTTP) | Runs WASM modules via wasmtime WASI component model |

### Request Flow

1. A WASM module makes an HTTP call to another module (e.g., `http://inventory-service/items`)
2. `WasiHttpView` intercepts it, attaches `x-wr-source` / `x-wr-destination` headers, rewrites the URI to point at `wr-proxy`
3. `wr-proxy` validates the body against a cached protobuf schema, resolves the destination engine from its cached routing table, injects `x-wr-module`, then forwards to the target `wr-engine`
4. The destination `wr-engine` dispatches to the correct WASM instance based on the URI

### Key Design Details

**`wr-common`** — generated gRPC types from `proto/wruntime.proto` via `tonic-build`. Shared by all other crates.

**`wr-proxy` middleware stack** (Tower layers, evaluated in order):
1. `MetricsLayer` — records request start time
2. `SchemaValidationLayer` — validates protobuf bodies via `prost-reflect`; rejects with structured JSON errors
3. `RoutingLayer` — resolves destination engine from local routing table cache (TTL-based)
4. `ForwardService` — strips internal headers and proxies to the resolved engine

**`wr-engine`** — uses `wasmtime` 41 with the WASI component model. On startup it registers all configured modules with the manager and begins a 10-second heartbeat loop. Schema validation is optional per module.

**`wr-manager` state** — pure in-memory (`state.rs`). No persistence layer; state is rebuilt from engine re-registrations after a restart.

### Configuration

Each service reads a TOML config file. Examples are at `manager.toml`, `proxy.toml`, and `engine.toml` in the repo root. Modules and their optional `.binpb` protobuf schemas are declared under `[[module]]` sections in `engine.toml`.

### Integration Tests

`wr-tests/tests/integration_test.rs` spins up all three services in-process on ephemeral ports. Tests cover: manager RPC operations, proxy routing, schema validation (invalid bodies → structured JSON error), pass-through when no schema is cached, and TOML config parsing for all three services.
