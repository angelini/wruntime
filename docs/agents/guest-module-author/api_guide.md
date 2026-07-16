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
| Query PostgreSQL | `wr_sdk::db` typed helpers and prelude | `wr_sdk::bindings::wruntime::db::database` |
| Use S3-compatible storage | validated `wr_sdk::blobstore` names plus store binding | raw blobstore WIT binding |
| Create spans | `span!`, `root_span!`, `wr_sdk::tracing` | tracing WIT binding |
| Call an LLM | `wr_sdk::llm::CompletionBuilder` | LLM inference WIT binding |
| Read configuration/secret values | `std::env::var` | WASI CLI environment binding |
| Use scratch files | `std::fs` with `fs = "tempdir"` | WASI filesystem bindings |

Typed SDK values are preferred because they reject malformed authority, path, header, timeout, JSON, numeric, timestamp, bucket/key, model, token, temperature, and tool-schema inputs before crossing the host boundary. Raw bindings remain useful for unsupported operations and negative tests; the host validates them independently.

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

Prefer typed row extraction, validated parameter wrappers, and `wr_sdk::db::transaction()`:

```rust
let rows = database::query(
    "SELECT name FROM items WHERE id = $1",
    &[PgValue::Int8(item_id)],
)?;
let name: String = rows.first().ok_or_else(|| ServiceError::not_found("item"))?.get(0)?;
```

A transaction rolls back when its resource/guard is dropped without commit. A row cursor cancels and releases its connection on drop. Completed transaction resources reject further operations. Input conversion is strict; unknown result-column types are read leniently as null with a host warning. Put schema DDL in module migrations, not request handlers.

## Blobstore

Validate bucket/key values before calling the store binding:

```rust
let bucket = wr_sdk::blobstore::BucketName::parse("reports")?;
let key = wr_sdk::blobstore::ObjectKey::parse("daily/result.json")?;
store::put_object(bucket.as_str(), key.as_str(), payload)?;
```

The engine enforces a non-empty bucket allowlist, namespace key isolation, object-size limits, and list-count limits. Object operations are fully buffered from the guest perspective.

## Tracing

```rust
let span = wr_sdk::span!("orders.create", "order.id" => order_id, "retry" => false);
wr_sdk::tracing::record_event(&span, "validated", &[]);
wr_sdk::tracing::set_error(&span, "failed");
```

Spans end on drop. Keep stable, low-cardinality attributes and never attach secrets.

## LLM

```rust
let text = wr_sdk::llm::CompletionBuilder::sonnet()
    .system("Answer concisely.")
    .user(prompt)
    .max_tokens(512)
    .complete_text()?;
```

Streaming yields zero or more text deltas, exactly one usage event, one stop event, then `None`; errors may surface while advancing the stream. Streaming rejects tool-enabled requests before an upstream call. Use non-streaming `complete()` for tool use. Dropping a stream cancels it. The host retains provider credentials, enforces limits, and exposes no API key to the guest.

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
