# Host Bindings

> **Building a new guest module?** See [`docs/agents/api_reference.md`](agents/api_reference.md) for exact function signatures of all host bindings.

WASM modules running in `wr-engine` can access host-provided capabilities through WIT interfaces defined under `wit/`. When using `wr-sdk`, these types are available via `wr_sdk::bindings` — no separate `wit_bindgen::generate!` call is required.

> **Compatibility policy.** The `wruntime:*` WIT packages (`wruntime:db`, `wruntime:llm`, `wruntime:blobstore`, `wruntime:tracing`) are **pre-1.0 and may change incompatibly** at any time until the project declares a stable API. Pin the runtime and SDK versions you build against, and expect to update guest code when these interfaces change.

## Database (Postgres)

Defined in `wit/db.wit`. Provides parameterized SQL queries and transactions against a shared Postgres connection pool managed by the engine.

### Engine configuration

Add a `[database]` section to `engine.toml` and set `database = true` on each module that should have access:

```toml
[database]
url             = "postgres://user:pass@localhost:5432/mydb"
max_connections = 10   # default: 8

[[module]]
name      = "order-service"
version   = "1.0.0"
wasm_path = "modules/order_service.wasm"
database  = true       # opt in to DB access

[[module]]
name      = "inventory-service"
version   = "1.0.0"
wasm_path = "modules/inventory_service.wasm"
# database omitted — no DB access for this module
```

### Example: querying Postgres from a WASM module

```rust
use wr_sdk::bindings::wruntime::db::database::{self, PgValue};

/// Look up an order by its integer ID and return the status string.
fn get_order_status(order_id: i32) -> Option<String> {
    let rows = database::query(
        "SELECT status FROM orders WHERE id = $1",
        &[PgValue::Int4(order_id)],
    ).ok()?;

    match rows.first()?.columns.first().map(|c| &c.value) {
        Some(PgValue::Text(s)) => Some(s.clone()),
        _ => None,
    }
}

/// Insert a new order and return the number of rows affected.
fn create_order(id: i32, status: &str, total: &str) -> u64 {
    database::execute(
        "INSERT INTO orders (id, status, total) VALUES ($1, $2, $3::numeric)",
        &[
            PgValue::Int4(id),
            PgValue::Text(status.to_string()),
            PgValue::Numeric(total.to_string()),
        ],
    ).unwrap_or(0)
}
```

### `pg-value` type mapping

| Variant | Postgres type | Rust encoding |
|---|---|---|
| `PgValue::Null` | SQL NULL | — |
| `PgValue::Boolean(bool)` | `BOOL` | `bool` |
| `PgValue::Int2(i16)` | `SMALLINT` | `i16` |
| `PgValue::Int4(i32)` | `INTEGER` | `i32` |
| `PgValue::Int8(i64)` | `BIGINT` | `i64` |
| `PgValue::Float4(f32)` | `REAL` | `f32` |
| `PgValue::Float8(f64)` | `DOUBLE PRECISION` | `f64` |
| `PgValue::Text(String)` | `TEXT` / `VARCHAR` / `CHAR` | `String` |
| `PgValue::Bytea(Vec<u8>)` | `BYTEA` | `Vec<u8>` |
| `PgValue::Timestamptz(i64)` | `TIMESTAMPTZ` | µs since Unix epoch (UTC) |
| `PgValue::Timestamp(i64)` | `TIMESTAMP` | µs since Unix epoch (naive) |
| `PgValue::Date(i32)` | `DATE` | days since Unix epoch |
| `PgValue::Time(i64)` | `TIME` | µs since midnight |
| `PgValue::Interval(PgInterval)` | `INTERVAL` | `{ months, days, microseconds }` |
| `PgValue::Numeric(String)` | `NUMERIC` / `DECIMAL` | decimal string (lossless) |
| `PgValue::Uuid((u64, u64))` | `UUID` | 128-bit value as `(high, low)` |
| `PgValue::Jsonb(String)` | `JSON` / `JSONB` | serialised JSON string |
| `PgValue::Oid(u32)` | `OID` | `u32` |
| `PgValue::BoolArray(Vec<Option<bool>>)` | `BOOL[]` | `Vec<Option<bool>>` |
| `PgValue::Int2Array(Vec<Option<i16>>)` | `INT2[]` | `Vec<Option<i16>>` |
| `PgValue::Int4Array(Vec<Option<i32>>)` | `INT4[]` | `Vec<Option<i32>>` |
| `PgValue::Int8Array(Vec<Option<i64>>)` | `INT8[]` | `Vec<Option<i64>>` |
| `PgValue::Float4Array(Vec<Option<f32>>)` | `FLOAT4[]` | `Vec<Option<f32>>` |
| `PgValue::Float8Array(Vec<Option<f64>>)` | `FLOAT8[]` | `Vec<Option<f64>>` |
| `PgValue::TextArray(Vec<Option<String>>)` | `TEXT[]` | `Vec<Option<String>>` |
| `PgValue::TimestamptzArray(Vec<Option<i64>>)` | `TIMESTAMPTZ[]` | `Vec<Option<i64>>` |
| `PgValue::TimestampArray(Vec<Option<i64>>)` | `TIMESTAMP[]` | `Vec<Option<i64>>` |
| `PgValue::UuidArray(Vec<Option<(u64, u64)>>)` | `UUID[]` | `Vec<Option<(u64, u64)>>` |
| `PgValue::JsonbArray(Vec<Option<String>>)` | `JSONB[]` | `Vec<Option<String>>` |

Parameters are bound positionally as `$1`, `$2`, … in the SQL string. Use explicit casts (e.g. `$1::numeric`, `$1::jsonb`) when Postgres cannot infer the type from context.

### Input validation

Parameter values are converted strictly. A value that cannot be represented in its target Postgres type is **rejected** with `DbError::Query(...)` (a descriptive message) rather than silently coerced. This applies to: malformed `Jsonb`/`JsonbArray` JSON, a non-numeric `Numeric` string, an out-of-range `Timestamp`/`Timestamptz`/`Time` value, and invalid elements inside array variants. There is a deliberate **read-path asymmetry**: on the way *out*, a result column of a Postgres type the engine does not explicitly map is logged as a warning and returned as `PgValue::Null` (lenient), whereas input conversion is strict.

## Blobstore (S3-compatible)

Defined in `wit/blobstore.wit`. Provides object storage operations against an S3-compatible backend configured on the engine.

Available functions:

| Function | Description |
|----------|-------------|
| `put-object(bucket, key, data)` | Upload an object |
| `get-object(bucket, key)` | Download an object's bytes |
| `delete-object(bucket, key)` | Remove an object; returns `NotFound` when it is missing |
| `list-objects(bucket, prefix)` | List objects matching a prefix |
| `head-object(bucket, key)` | Get object metadata (size, etag, last-modified) |

Access via `wr_sdk::bindings::wruntime::blobstore::store`.

### Example: storing and retrieving objects

```rust
use wr_sdk::bindings::wruntime::blobstore::store;

fn save_report(report_id: &str, data: &[u8]) {
    store::put_object("reports", &format!("daily/{report_id}.bin"), data)
        .expect("put_object failed");
}

fn load_report(report_id: &str) -> Vec<u8> {
    store::get_object("reports", &format!("daily/{report_id}.bin"))
        .expect("get_object failed")
}

fn list_reports() -> Vec<String> {
    store::list_objects("reports", Some("daily/"))
        .expect("list_objects failed")
        .into_iter()
        .map(|meta| meta.key)
        .collect()
}
```

### Limits and errors

`BlobError` has four variants: `NotFound`, `AccessDenied`, `Io`, and `TooLarge`. The engine's required, non-empty `[blobstore].allowed_buckets` list constrains every guest bucket argument; a bucket outside it returns `AccessDenied` before any S3 request. Host-enforced limits are global across modules:

- `max_object_size` (default **16 MiB**) caps both `put_object` uploads and `get_object` downloads. An oversized download is aborted mid-stream — never fully buffered — and returns `BlobError::TooLarge`.
- `max_list_objects` (default **1000**) caps `list_objects`; exceeding it returns `BlobError::TooLarge` rather than silently truncating.

See [configuration.md](configuration.md#blobstore) for the config keys.

## Tracing (OpenTelemetry)

Defined in `wit/tracing.wit`. Allows modules to create and annotate OpenTelemetry spans that appear alongside the proxy's own request traces.

```rust
use wr_sdk::tracing;

let span = tracing::start("process-order", &[("order.id", "123")]);
tracing::set_attribute(&span, "order.total", "45.99");
tracing::record_event(&span, "validation-passed", &[]);
// span ends when dropped
```

Access via `wr_sdk::bindings::wruntime::tracing::span`.

Each request has a ceiling on the number of concurrently live guest-created spans (`[limits] max_spans`, default **1024**). A span resource is created by `start`/`start-root` and freed when dropped. If a guest tries to open a span beyond the cap, the guest instance is **trapped** (the request fails) — this protects the engine's resource table; it does not crash the engine. Drop spans you no longer need to stay under the cap.

## LLM Inference

Defined in `wit/llm.wit`. Allows modules to call LLM APIs (currently Anthropic Claude) through a host binding. The engine holds the API key — guests never see credentials.

### Engine configuration

Add an `[llm]` section to `engine.toml` and set `llm = true` on each module that should have access:

```toml
[llm]
provider         = "anthropic"
api_key_env      = "ANTHROPIC_API_KEY"   # env var read at startup
base_url         = "https://api.anthropic.com"  # optional, this is the default
max_tokens_limit = 8192                  # host-enforced ceiling per request

[[module]]
name        = "my-agent"
namespace   = "example"
version     = "1.0.0"
wasm_path   = "modules/my_agent.wasm"
schema_path = "schemas/my_agent.binpb"
llm         = true
```

### Example: calling Claude from a WASM module

```rust
use wr_sdk::llm::CompletionBuilder;

fn summarize(text: &str) -> String {
    CompletionBuilder::sonnet()
        .system("You are a concise summarizer.")
        .user(text)
        .max_tokens(256)
        .complete_text()
        .expect("completion failed")
}

// Streaming example
fn stream_response(prompt: &str) -> String {
    let stream = CompletionBuilder::sonnet()
        .user(prompt)
        .max_tokens(1024)
        .stream()
        .expect("stream failed");
    wr_sdk::llm::collect_stream(stream).expect("collect failed")
}
```

Access via `wr_sdk::bindings::wruntime::llm::inference` (raw WIT binding) or `wr_sdk::llm` (ergonomic helpers).

### Streaming

`complete-stream` returns a `CompletionStream` cursor whose `next()` yields typed `StreamEvent` values in a guaranteed order: zero or more `TextDelta`, then exactly one `Usage`, then exactly one `Stop`, then `None` (idempotent thereafter). `usage()` returns `None` until the terminal `Usage` event has been observed via `next()`. Stream-level errors, transport failures, and truncated streams surface as an `LlmError` from `next()`.

Tool-use is **not** supported while streaming: `complete-stream` pre-rejects tool-enabled requests with `LlmError::InvalidRequest` before any upstream call — use `complete()` for tool calls. Extended-thinking, signature, and citation deltas from the upstream API are dropped (they have no WIT representation). The `wr_sdk::llm::collect_stream` helper (used above) drains the cursor and accumulates the text deltas into a `String`. See [`api_reference.md`](agents/api_reference.md#wruntimellminference010) for the full type/ordering contract.

## Filesystem

By default WASM modules have no filesystem access. Set `fs = "tempdir"` in a `[[module]]` block to mount an ephemeral writable directory at `/`:

```toml
[[module]]
name        = "order-service"
namespace   = "ecommerce"
version     = "1.0.0"
wasm_path   = "modules/order_service.wasm"
schema_path = "schemas/order_service.binpb"
fs          = "tempdir"
```

The directory is created fresh on the host for each store and deleted when the store is dropped:

- For **HTTP handler modules** a new directory is created per request (each request gets its own store).
- For **runner modules** the directory lives for the lifetime of the module.

The directory is empty on creation. It is not shared between module instances or across requests. Use it for scratch space, caching, or temporary files — do not rely on it for durable state.

| Value | Effect |
|-------|--------|
| `fs = "tempdir"` | Mount an ephemeral temp directory at `/` |
| *(omitted)* | No filesystem access (default) |
