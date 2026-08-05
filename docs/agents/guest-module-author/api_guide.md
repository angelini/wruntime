# Guest API Guide

This is a discovery and semantic guide, not an exhaustive signature reference. Use these authorities for exact APIs:

| Need | Exact source |
|---|---|
| Guest SDK APIs | [`wr-sdk/src/*.rs`](../../../wr-sdk/src/) |
| Generated clients/routers | [`wr-build/src/lib.rs`](../../../wr-build/src/lib.rs) |
| Host ABI | root [`wit/*.wit`](../../../wit/) |
| Runtime enforcement | [`wr-engine/src/`](../../../wr-engine/src/) |
| Capability concepts/config | [`docs/host-bindings.md`](../../host-bindings.md) |
| Working examples | [examples guide](./examples.md) |

## Task-to-API map

| Task | Prefer | Raw escape hatch |
|---|---|---|
| Serve protobuf RPCs | generated service trait and `_handle` | generated `_router` plus `wr_sdk::io` |
| Call a module RPC | generated `{Service}Client` | typed `wr_sdk::http` request |
| Submit/query worker jobs | generated `*WorkerServiceClient` | `wr_sdk::jobs` |
| Query PostgreSQL | `wr_sdk::db` builders, owned rows, and `FromRow` | `wr_sdk::bindings::wruntime::db::database` |
| Use S3-compatible storage | `wr_sdk::blobstore::bucket` scoped handle | raw blobstore WIT binding |
| Create spans and events | `span!`, `root_span!`, `set_attrs!`, and `event!` | tracing WIT binding |
| Call an LLM | `wr_sdk::llm::CompletionBuilder` with validated value types | LLM inference WIT binding |
| Write a diagnostic | `wr_sdk::log!` | — |
| Read configuration/secret values | `std::env::var` | WASI CLI environment binding |
| Use scratch files | `std::fs` with `fs = "tempdir"` | WASI filesystem bindings |

Prefer the SDK facade: it owns validation, strict decoding, and ergonomic resource handling before and after a host call. Raw bindings are an intentional escape hatch for unsupported operations and protocol/negative tests, not the default application API; host validation still applies to raw calls.

## Lifecycle and synchronous calls

Guest APIs are synchronous from the guest's perspective. Do not add `async`/`await` around host calls. Host implementations may be asynchronous internally.

`ServiceGuest::init` runs once before the first request and is suitable for one-time SDK setup. The export macro intercepts `GET /__health`; `health_check()` returns `true` by default and custom `false` yields 503 without entering `handle`. Exact lifecycle behavior is in [`wr-sdk/src/lib.rs`](../../../wr-sdk/src/lib.rs).

```rust
impl wr_sdk::ServiceGuest for Component {
    fn init() {
        // One-time guest setup.
    }

    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        proto::orders_service_handle(&Component, request, response_out);
    }
}
```

## Capability prerequisites

A guest's local `world.wit` declares imports and emits component metadata. Enable matching DB/blobstore/LLM flags in its `[[module]]` entry. Import/config mismatches fail module startup validation. Tracing is available without a per-module flag; filesystem requires `fs = "tempdir"`; external HTTP hosts must match the proxy's `[egress].allowed_domains` policy.

`wr_sdk::bindings` supplies compatible convenience types, but it does not replace the guest's local `wit_bindgen::generate!` block for the guest world.

## Database

Bind owned typed values and choose cardinality explicitly:

```rust
use wr_sdk::db::{query_as, FromRow};

#[derive(FromRow)]
struct Item {
    id: i64,
    name: String,
}

let item = query_as::<Item>("SELECT id, name FROM items WHERE id = $1")
    .bind(item_id)
    .fetch_exactly_one()?;
```

`query`, `query_as`, and `query_scalar` provide `execute`, `fetch_first`, `fetch_optional`, `fetch_exactly_one`, `fetch_all`, and `stream` terminals; transactions expose the same builders. `fetch_optional` rejects more than one row, `fetch_exactly_one` rejects zero or multiple rows, and `fetch_first` rejects zero rows. A `Row` owns its data: `get("name")` rejects missing or duplicate names and `get_at(0)` rejects a missing index. Type mismatches and a SQL `NULL` decoded into a non-`Option<T>` are errors with column, expected-type, and actual/null-type context. Use `Option<T>` for nullable values, `Json<T>` (with the `wr-sdk` `serde` feature) for JSONB serde conversion, and `#[derive(FromRow)]` with `#[wr_db(rename = "...")]` or `#[wr_db(flatten)]` for named rows; decoding never supplies `Default` fallbacks.

Parameters implement `EncodePg`; ambiguous integers and heterogeneous arrays are rejected, and raw SQL nulls carry a `PgType`. `BatchSize::new` rejects zero. A stream is a synchronous iterator of `Result<T, DbError>`; reaching an empty batch ends it, a decode/host error terminates it, and dropping it early releases the host cursor. Transactions roll back on drop unless consumed by `commit` or `rollback`. Put schema DDL in module migrations, not request handlers. Exact types and signatures remain in [`wr-sdk/src/db.rs`](../../../wr-sdk/src/db.rs) and [`wit/db.wit`](../../../wit/db.wit).

## Blobstore

Scope repeated operations to one validated bucket handle:

```rust
let reports = wr_sdk::blobstore::bucket("reports")?;
reports.put("daily/result.json", payload)?;
let saved = reports.get("daily/result.json")?;
let daily = reports.list("daily/")?;
```

`Bucket::put`, `get`, `delete`, `head`, and `list` validate and normalize each key or prefix. The engine independently enforces the non-empty bucket allowlist, namespace key isolation, object-size limits, and list-count limits. Object operations are fully buffered from the guest perspective. Use the raw store binding only when the facade does not expose the needed operation or for intentional protocol tests.

## Tracing

```rust
let span = wr_sdk::span!("orders.create", "order.id" => order_id, "retry" => false);
wr_sdk::set_attrs!(span, "order.total" => 45.99, "order.items" => 3_i64);
wr_sdk::event!(span, "validated", "checks" => vec!["stock", "payment"]);
wr_sdk::tracing::set_error(&span, "failed");
```

`set_attrs!` accepts arbitrary late keys and sends all fields in one guest/host crossing. Attribute values are strings, booleans, signed 64-bit integers, 64-bit floats, or homogeneous arrays of those types. Convert `u64`, `usize`, and unsigned arrays explicitly with a checked conversion such as `i64::try_from(value)?`, or intentionally record text; the SDK never converts them lossily.

Spans end on drop. Keep stable, low-cardinality attributes and never attach secrets.

## Logging

```rust
wr_sdk::log!("processed item {item_id}: {status}");
```

`log!` accepts normal formatting syntax without allocating an intermediate `format!` string.

## LLM

```rust
use wr_sdk::llm::{CompletionBuilder, MaxTokens};

let text = CompletionBuilder::sonnet()
    .system("Answer concisely.")
    .user(prompt)
    .max_tokens(MaxTokens::new(512)?)
    .complete_text()?;
```

Dynamic models use `ModelName::parse`; optional temperature and tool setters require `Temperature::new` and `ToolSchema::parse`. Invalid model names, zero token limits, non-finite/out-of-range temperatures, and non-object/invalid JSON tool schemas fail before the host call. Streaming yields zero or more text deltas, exactly one usage event, one stop event, then `None`; errors may surface while advancing the stream. Streaming rejects tool-enabled requests before an upstream call. Use non-streaming `complete()` for tool use. Dropping a stream cancels it. The host retains provider credentials, enforces limits, and exposes no API key to the guest.

## Workers

Generated worker clients use canonical job types `/{package}.{WorkerService}/{Method}`. A non-empty worker version pins exact matching; an empty ad-hoc version permits any matching namespace/name worker. Manager schedules are always version-pinned.

Jobs can be delivered more than once after lease expiry/retry. Make handlers idempotent. Generated result helpers return `None` for pending/running, decode complete results, and surface dead jobs as errors. Inspect [`wr-sdk/src/jobs.rs`](../../../wr-sdk/src/jobs.rs) and [`wr-build/src/lib.rs`](../../../wr-build/src/lib.rs) for exact status/options methods.

## Environment, filesystem, and HTTP

```toml
[module.env]
LOG_LEVEL = "info"
API_TOKEN = { secret = true }
```

The engine resolves a secret with the same namespace/key and passes only its plaintext value as a guest environment variable. Guests do not receive manager secret-store access or secret identifiers. Missing secrets fail registration/startup.

Prefer generated clients for protobuf module calls and typed `wr_sdk::http` for custom calls. Outbound module authorities are `namespace.module`; external hosts require proxy [egress permission](../../configuration.md#external-egress). Request bodies remain subject to host limits.
