# Plan: WASM Guest Test Harness for Host Bindings

## Context

Host bindings (db, tracing, blobstore) are currently tested by calling `ModuleState` methods directly from Rust, bypassing the WASM boundary entirely. This means WIT type marshalling, resource lifecycle (transactions, spans), and component-model canonicalization are untested. Blobstore has zero tests. The goal is to create slim WASM guest modules that exercise each host interface through the real engine code path.

## Approach

Create three minimal guest WASM components (one per host interface) under `wr-tests/guests/`, plus a test harness that loads them via the same `Component` → `Linker` → `ProxyPre` → `dispatch_request` path as production.

## Directory Structure

```
wr-tests/guests/
  db-guest/          # exercises wruntime:db/database
    Cargo.toml
    wit/world.wit
    src/lib.rs
  tracing-guest/     # exercises wruntime:tracing/span
    Cargo.toml
    wit/world.wit
    src/lib.rs
  blobstore-guest/   # exercises wruntime:blobstore/store
    Cargo.toml
    wit/world.wit
    src/lib.rs
wr-tests/tests/
  wasm_host_test.rs  # new test file
  helpers.rs         # add wasm_module_pre(), dispatch_to_wasm(), blobstore_client()
```

## Guest Module Design

Each guest implements `wr_sdk::ServiceGuest`, routes by request path, uses `serde_json` for request/response (no protobuf). Depends only on `wr-sdk`, `wit-bindgen-rt`, `serde_json`.

### db-guest endpoints

| Path | Operation | Tests |
|---|---|---|
| `/execute` | JSON `{"sql":"...","params":[...]}` → `database::execute()` → `{"affected":N}` | Execute + param marshalling |
| `/query` | JSON body → `database::query()` → `{"rows":[...]}` | Query + row/column marshalling |
| `/query-types` | Query returning all PgValue variants | Full type round-trip through WASM boundary |
| `/transaction-commit` | begin → execute → commit → query to verify | Transaction resource lifecycle |
| `/transaction-rollback` | begin → execute → rollback → verify absent | Rollback |
| `/transaction-drop` | begin → execute → drop handle → verify absent | Implicit rollback on resource drop |
| `/error` | Invalid SQL → return DbError variant | Error marshalling |

### tracing-guest endpoints

| Path | Operation | Tests |
|---|---|---|
| `/start-span` | `span::start()` with attrs | Span resource creation |
| `/span-attributes` | Start + `set_attribute()` multiple times | Attribute setting |
| `/span-event` | Start + `record_event()` | Event recording |
| `/span-error` | Start + `set_error()` | Error marking |
| `/nested-spans` | Two concurrent spans, interleaved ops, ordered drop | Multiple resource handles |

### blobstore-guest endpoints

| Path | Operation | Tests |
|---|---|---|
| `/put` | JSON `{"bucket","key","data"(base64)}` → `put_object()` | Put through WASM |
| `/get` | JSON `{"bucket","key"}` → `get_object()` → `{"data":"..."}` | Get + data round-trip |
| `/delete` | Delete then get → verify not-found | Delete |
| `/list` | Put 3 objects, `list_objects()` with prefix | List filtering |
| `/head` | Put then `head_object()` → metadata | Head metadata |
| `/round-trip` | Put + get, compare | End-to-end integrity |
| `/not-found` | Get nonexistent key → BlobError | Error variant marshalling |

## Test Harness (helpers.rs additions)

### `wasm_module_pre(wasm_path)` → `(Arc<Engine>, Arc<ProxyPre<ModuleState>>)`

Replicates `wr-engine/src/engine.rs:39-166` linker setup:
```rust
let mut config = Config::new();
config.wasm_component_model(true);  // match engine.rs:42 exactly
let engine = Engine::new(&config)?;
let component = Component::from_file(&engine, wasm_path)?;
let mut linker = Linker::new(&engine);
// add WASI p2, HTTP, db, tracing, blobstore — same as engine.rs:148-162
let pre = ProxyPre::new(linker.instantiate_pre(&component)?)?;
```

### `dispatch_to_wasm(engine, pre, state, request)` → `Response<Bytes>`

Standalone version of `engine.rs:347-405` — builds Store, instantiates, calls incoming-handler, collects response.

### `blobstore_client()` → `Option<Arc<BlobstoreRuntime>>`

Reads `WRUNTIME_TEST_S3_ENDPOINT` / `WRUNTIME_TEST_S3_ACCESS_KEY` / `WRUNTIME_TEST_S3_SECRET_KEY` env vars. Returns `None` when absent (test skips).

## Build Integration

**Root `Cargo.toml`** — add to exclude list:
```toml
exclude = [
    ...,
    "wr-tests/guests/db-guest",
    "wr-tests/guests/tracing-guest",
    "wr-tests/guests/blobstore-guest",
]
```

**Justfile recipes:**
```just
build-test-guests:
    (cd wr-tests/guests/db-guest && cargo component build --release --target wasm32-wasip2)
    (cd wr-tests/guests/tracing-guest && cargo component build --release --target wasm32-wasip2)
    (cd wr-tests/guests/blobstore-guest && cargo component build --release --target wasm32-wasip2)

test-wasm: build-test-guests
    cargo test -p wr-tests wasm_host_test
```

**`wr-tests/Cargo.toml`** — add `wasmtime-wasi` and `wasmtime-wasi-http` dev-dependencies (version 43, matching wr-engine).

## WASM Path Resolution

Use `concat!(env!("CARGO_MANIFEST_DIR"), "/guests/db-guest/target/wasm32-wasip2/release/db_guest.wasm")`. Tests skip gracefully (not fail) if the file doesn't exist, printing a message to run `just build-test-guests`.

## Key Files to Modify

- `Cargo.toml` (workspace root) — add excludes
- `Justfile` — add `build-test-guests` and `test-wasm` recipes
- `wr-tests/Cargo.toml` — add wasmtime-wasi, wasmtime-wasi-http deps
- `wr-tests/tests/helpers.rs` — add `wasm_module_pre()`, `dispatch_to_wasm()`, `blobstore_client()`
- **New**: `wr-tests/tests/wasm_host_test.rs`
- **New**: `wr-tests/guests/{db,tracing,blobstore}-guest/` (3 crates)

## Key Files to Reference

- `wr-engine/src/engine.rs:39-43` — Engine config (match exactly)
- `wr-engine/src/engine.rs:148-162` — Linker setup (replicate in helper)
- `wr-engine/src/engine.rs:347-405` — `dispatch_request` (replicate in helper)
- `wr-engine/src/state.rs:134-173` — `ModuleState::new()` signature
- `examples/ecommerce/inventory/Cargo.toml` — template for guest Cargo.toml
- `examples/ecommerce/inventory/wit/world.wit` — template for guest WIT world

## Implementation Order

1. Create `db-guest` crate (Cargo.toml, wit/world.wit, src/lib.rs)
2. Update workspace exclude, build it with `cargo component build`
3. Add `wasm_module_pre()` + `dispatch_to_wasm()` helpers
4. Add `wasmtime-wasi`/`wasmtime-wasi-http` to wr-tests deps
5. Write `wasm_host_test.rs` with first DB test, verify end-to-end
6. Complete all db-guest endpoints and tests
7. Create `tracing-guest` + tests
8. Create `blobstore-guest` + `blobstore_client()` helper + tests
9. Add Justfile recipes

## Verification

```bash
just dev-up                    # start Postgres + S3
just build-test-guests         # compile WASM guests
just test-wasm                 # run WASM boundary tests
```

DB tests require `WRUNTIME_TEST_DB_URL`, blobstore tests require `WRUNTIME_TEST_S3_*` env vars. Tests skip gracefully when env vars or WASM files are absent.
