# Plan: WASM Guest Test Harness for Host Bindings

> **STATUS: IMPLEMENTED — historical.** This plan has shipped and is superseded by the guests under `wr-tests/guests/` (`db-guest`, `tracing-guest`, `blobstore-guest`, plus `http-guest` and `llm-guest`) and the `wr-tests/tests/wasm_host_test.rs` suite, which exercises DB, tracing, blobstore, LLM, and HTTP (egress/ingress) through the real engine code path. The directory layout, env-var names, and Justfile recipes below describe the original plan and may differ from the current tree (e.g. the live env vars are `WRT_TEST_DB_URL` / `WRT_TEST_S3_*`, and the guest set is larger). Retained for historical context only — do not treat it as pending work.

## Context

Host bindings (db, tracing, blobstore) are currently tested by calling `ModuleState` methods directly from Rust, bypassing the WASM boundary entirely. This means WIT type marshalling, resource lifecycle (transactions, spans), and component-model canonicalization are untested. Blobstore has zero tests. The goal is to create slim WASM guest modules that exercise each host interface through the real engine code path.

## Approach

Create three minimal guest WASM components (one per host interface) under `wr-tests/guests/`, plus a test harness that loads them via the same `Component` → `Linker` → `ProxyPre` → `dispatch_request` path as production.

## Directory Structure

```
wr-tests/guests/
  schemas/
    db_test.proto         # service + message definitions for db-guest
    tracing_test.proto    # service + message definitions for tracing-guest
    blobstore_test.proto  # service + message definitions for blobstore-guest
  db-guest/               # exercises wruntime:db/database
    Cargo.toml            # depends on wr-sdk, wr-build (build-dep), prost, wit-bindgen-rt
    build.rs              # uses WrServiceGenerator to codegen from db_test.proto
    wit/world.wit
    src/lib.rs            # implements generated DbTestService trait
  tracing-guest/          # exercises wruntime:tracing/span
    Cargo.toml
    build.rs              # uses WrServiceGenerator to codegen from tracing_test.proto
    wit/world.wit
    src/lib.rs            # implements generated TracingTestService trait
  blobstore-guest/        # exercises wruntime:blobstore/store
    Cargo.toml
    build.rs              # uses WrServiceGenerator to codegen from blobstore_test.proto
    wit/world.wit
    src/lib.rs            # implements generated BlobstoreTestService trait
wr-tests/tests/
  wasm_host_test.rs       # new test file — sends protobuf-encoded requests, decodes protobuf responses
  helpers.rs              # add wasm_module_pre(), dispatch_to_wasm(), blobstore_client()
```

## Guest Module Design

Each guest uses the same protobuf codegen pipeline as production modules: a `.proto` file defines a service, `wr-build::WrServiceGenerator` generates a typed trait + router, and `prost` handles encoding/decoding. This ensures the test guests exercise the full auto-generated code path (trait dispatch, protobuf marshalling, router matching) through the WASM boundary — not just the host bindings.

Each guest has:
- `schemas/<name>.proto` — service + message definitions
- `build.rs` — uses `WrServiceGenerator` (same as ecommerce examples)
- `src/lib.rs` — implements the generated trait, calls host bindings in method bodies

The test harness sends protobuf-encoded requests and decodes protobuf responses, matching production behavior. The `.binpb` descriptor sets are also compiled so proxy schema validation can be tested end-to-end if desired.

### db-guest

**Proto** (`schemas/db_test.proto`):
```protobuf
syntax = "proto3";
package test;

service DbTestService {
  rpc Execute           (ExecuteRequest)           returns (ExecuteResponse);
  rpc Query             (QueryRequest)             returns (QueryResponse);
  rpc QueryTypes        (QueryTypesRequest)        returns (QueryTypesResponse);
  rpc TransactionCommit (TransactionCommitRequest) returns (TransactionCommitResponse);
  rpc TransactionRollback (TransactionRollbackRequest) returns (TransactionRollbackResponse);
  rpc TransactionDrop   (TransactionDropRequest)   returns (TransactionDropResponse);
  rpc Error             (ErrorRequest)             returns (ErrorResponse);
}
```

| RPC | Operation | Tests |
|---|---|---|
| `Execute` | `database::execute()` with SQL + params | Execute + param marshalling |
| `Query` | `database::query()` | Query + row/column marshalling |
| `QueryTypes` | Query returning all PgValue variants | Full type round-trip through WASM boundary |
| `TransactionCommit` | begin → execute → commit → query to verify | Transaction resource lifecycle |
| `TransactionRollback` | begin → execute → rollback → verify absent | Rollback |
| `TransactionDrop` | begin → execute → drop handle → verify absent | Implicit rollback on resource drop |
| `Error` | Invalid SQL → return DbError variant | Error marshalling |

### tracing-guest

**Proto** (`schemas/tracing_test.proto`):
```protobuf
syntax = "proto3";
package test;

service TracingTestService {
  rpc StartSpan      (StartSpanRequest)      returns (StartSpanResponse);
  rpc SpanAttributes (SpanAttributesRequest) returns (SpanAttributesResponse);
  rpc SpanEvent      (SpanEventRequest)      returns (SpanEventResponse);
  rpc SpanError      (SpanErrorRequest)      returns (SpanErrorResponse);
  rpc NestedSpans    (NestedSpansRequest)    returns (NestedSpansResponse);
}
```

| RPC | Operation | Tests |
|---|---|---|
| `StartSpan` | `span::start()` with attrs | Span resource creation |
| `SpanAttributes` | Start + `set_attribute()` multiple times | Attribute setting |
| `SpanEvent` | Start + `record_event()` | Event recording |
| `SpanError` | Start + `set_error()` | Error marking |
| `NestedSpans` | Two concurrent spans, interleaved ops, ordered drop | Multiple resource handles |

### blobstore-guest

**Proto** (`schemas/blobstore_test.proto`):
```protobuf
syntax = "proto3";
package test;

service BlobstoreTestService {
  rpc Put      (PutRequest)      returns (PutResponse);
  rpc Get      (GetRequest)      returns (GetResponse);
  rpc Delete   (DeleteRequest)   returns (DeleteResponse);
  rpc List     (ListRequest)     returns (ListResponse);
  rpc Head     (HeadRequest)     returns (HeadResponse);
  rpc RoundTrip (RoundTripRequest) returns (RoundTripResponse);
  rpc NotFound (NotFoundRequest) returns (NotFoundResponse);
}
```

| RPC | Operation | Tests |
|---|---|---|
| `Put` | Protobuf `PutRequest {bucket, key, data}` → `put_object()` | Put through WASM |
| `Get` | `GetRequest {bucket, key}` → `get_object()` → `GetResponse {data}` | Get + data round-trip |
| `Delete` | Delete then get → verify not-found | Delete |
| `List` | Put 3 objects, `list_objects()` with prefix | List filtering |
| `Head` | Put then `head_object()` → metadata | Head metadata |
| `RoundTrip` | Put + get, compare | End-to-end integrity |
| `NotFound` | Get nonexistent key → BlobError | Error variant marshalling |

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

Standalone version of `engine.rs:347-405` — builds Store, instantiates, calls incoming-handler, collects response. The request body must be protobuf-encoded (using `prost::Message::encode_to_vec`), and the response body is decoded with `prost::Message::decode`.

### `blobstore_client()` → `Option<Arc<BlobstoreRuntime>>`

Reads `WRUNTIME_TEST_S3_ENDPOINT` / `WRUNTIME_TEST_S3_ACCESS_KEY` / `WRUNTIME_TEST_S3_SECRET_KEY` env vars. Returns `None` when absent (test skips).

### Proto types in tests

The test file includes the generated proto types for building requests/asserting responses:
```rust
// Include the generated proto types for each test guest
mod db_proto {
    include!(concat!(env!("OUT_DIR"), "/test.rs"));  // or use a shared proto build
}
```

Alternatively, since `wr-tests` is not a WASM crate, it can have its own `build.rs` that compiles the same `.proto` files (without `WrServiceGenerator` — just plain `prost_build`) to get the message types for constructing requests and decoding responses in test assertions.

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
build-test-schemas:
    protoc --descriptor_set_out=wr-tests/guests/schemas/db_test.binpb \
           --include_imports wr-tests/guests/schemas/db_test.proto
    protoc --descriptor_set_out=wr-tests/guests/schemas/tracing_test.binpb \
           --include_imports wr-tests/guests/schemas/tracing_test.proto
    protoc --descriptor_set_out=wr-tests/guests/schemas/blobstore_test.binpb \
           --include_imports wr-tests/guests/schemas/blobstore_test.proto

build-test-guests: build-test-schemas
    (cd wr-tests/guests/db-guest && cargo component build --release --target wasm32-wasip2)
    (cd wr-tests/guests/tracing-guest && cargo component build --release --target wasm32-wasip2)
    (cd wr-tests/guests/blobstore-guest && cargo component build --release --target wasm32-wasip2)

test-wasm: build-test-guests
    cargo test -p wr-tests wasm_host_test
```

**`wr-tests/Cargo.toml`** — add `wasmtime-wasi`, `wasmtime-wasi-http`, and `prost` dev-dependencies (wasmtime version 43, matching wr-engine). The `prost` dep is needed to encode requests and decode responses in the test harness.

**Guest `Cargo.toml`** dependencies (each guest):
```toml
[dependencies]
wr-sdk = { path = "../../../../wr-sdk" }
prost = "0.13"
wit-bindgen-rt = "0.41.0"

[build-dependencies]
wr-build = { path = "../../../../wr-build" }
prost-build = "0.13"
```

**Guest `build.rs`** (example for db-guest):
```rust
fn main() {
    prost_build::Config::new()
        .service_generator(Box::new(wr_build::WrServiceGenerator))
        .compile_protos(&["../schemas/db_test.proto"], &["../schemas"])
        .unwrap();
    println!("cargo:rerun-if-changed=../schemas/db_test.proto");
}
```

## WASM Path Resolution

Use `concat!(env!("CARGO_MANIFEST_DIR"), "/guests/db-guest/target/wasm32-wasip2/release/db_guest.wasm")`. Tests skip gracefully (not fail) if the file doesn't exist, printing a message to run `just build-test-guests`.

## Key Files to Modify

- `Cargo.toml` (workspace root) — add excludes
- `Justfile` — add `build-test-schemas`, `build-test-guests`, and `test-wasm` recipes
- `wr-tests/Cargo.toml` — add wasmtime-wasi, wasmtime-wasi-http, prost deps
- `wr-tests/build.rs` — **New**: compile test `.proto` files with plain `prost_build` (no service generator) so test code has access to request/response message types
- `wr-tests/tests/helpers.rs` — add `wasm_module_pre()`, `dispatch_to_wasm()`, `blobstore_client()`
- **New**: `wr-tests/tests/wasm_host_test.rs`
- **New**: `wr-tests/guests/schemas/{db_test,tracing_test,blobstore_test}.proto`
- **New**: `wr-tests/guests/{db,tracing,blobstore}-guest/` (3 crates, each with build.rs using WrServiceGenerator)

## Key Files to Reference

- `wr-engine/src/engine.rs:39-43` — Engine config (match exactly)
- `wr-engine/src/engine.rs:148-162` — Linker setup (replicate in helper)
- `wr-engine/src/engine.rs:347-405` — `dispatch_request` (replicate in helper)
- `wr-engine/src/state.rs:134-173` — `ModuleState::new()` signature
- `examples/ecommerce/inventory/Cargo.toml` — template for guest Cargo.toml
- `examples/ecommerce/inventory/wit/world.wit` — template for guest WIT world

## Implementation Order

1. Create `wr-tests/guests/schemas/` with all three `.proto` files
2. Create `db-guest` crate (Cargo.toml, build.rs with WrServiceGenerator, wit/world.wit, src/lib.rs implementing generated trait)
3. Update workspace exclude, build it with `cargo component build`
4. Add `wr-tests/build.rs` — plain `prost_build` compile of the `.proto` files (message types only, no service generator) so tests can construct/decode protobuf
5. Add `wasm_module_pre()` + `dispatch_to_wasm()` helpers
6. Add `wasmtime-wasi`/`wasmtime-wasi-http`/`prost` to wr-tests deps
7. Write `wasm_host_test.rs` with first DB test — send protobuf-encoded `ExecuteRequest`, decode `ExecuteResponse`, verify end-to-end
8. Complete all db-guest RPCs and tests
9. Create `tracing-guest` + tests
10. Create `blobstore-guest` + `blobstore_client()` helper + tests
11. Add Justfile recipes (`build-test-schemas`, `build-test-guests`, `test-wasm`)

## Verification

```bash
just dev-up                    # start Postgres + S3
just build-test-guests         # compile WASM guests
just test-wasm                 # run WASM boundary tests
```

DB tests require `WRUNTIME_TEST_DB_URL`, blobstore tests require `WRUNTIME_TEST_S3_*` env vars. Tests skip gracefully when env vars or WASM files are absent.
