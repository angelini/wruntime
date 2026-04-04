# Host Bindings

> **Building a new guest module?** See [`docs/agents/api_reference.md`](agents/api_reference.md) for exact function signatures of all host bindings.

WASM modules running in `wr-engine` can access host-provided capabilities through WIT interfaces defined under `wit/`. When using `wr-sdk`, these types are available via `wr_sdk::bindings` — no separate `wit_bindgen::generate!` call is required.

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
| `PgValue::Date(i32)` | `DATE` | days since Unix epoch |
| `PgValue::Time(i64)` | `TIME` | µs since midnight |
| `PgValue::Numeric(String)` | `NUMERIC` / `DECIMAL` | decimal string (lossless) |
| `PgValue::Uuid((u64, u64))` | `UUID` | 128-bit value as `(high, low)` |
| `PgValue::Jsonb(String)` | `JSON` / `JSONB` | serialised JSON string |
| `PgValue::Oid(u32)` | `OID` | `u32` |

Parameters are bound positionally as `$1`, `$2`, … in the SQL string. Use explicit casts (e.g. `$1::numeric`, `$1::jsonb`) when Postgres cannot infer the type from context.

## Blobstore (S3-compatible)

Defined in `wit/blobstore.wit`. Provides object storage operations against an S3-compatible backend configured on the engine.

Available functions:

| Function | Description |
|----------|-------------|
| `put-object(bucket, key, data)` | Upload an object |
| `get-object(bucket, key)` | Download an object's bytes |
| `delete-object(bucket, key)` | Remove an object |
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
