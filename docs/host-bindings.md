# Host Bindings

> **Building a new guest module?** Use the guest [API guide](agents/guest-module-author/api_guide.md) for preferred usage and semantic contracts. Exact APIs are owned by root [`wit/*.wit`](../wit/) and [`wr-sdk/src/*.rs`](../wr-sdk/src/).

WASM modules running in `wr-engine` access host capabilities through WIT interfaces defined under `wit/`. `wr_sdk::bindings` supplies compatible SDK convenience types. Every guest still needs a local `wit_bindgen::generate!` block for its own world so the component records that world's imports, exports, and metadata; see the [module template](agents/guest-module-author/module_template.md).

DB, blobstore, and LLM imports require matching per-module opt-ins and valid engine provider configuration. The engine validates these imports before loading the module; mismatches fail startup before readiness. Host implementations continue to enforce authorization, scope, input, and resource limits on every call.

> **Compatibility policy.** The `wruntime:*` WIT packages (`wruntime:db`, `wruntime:llm`, `wruntime:blobstore`, `wruntime:tracing`) are **pre-1.0 and may change incompatibly** at any time until the project declares a stable API. Pin the runtime and SDK versions you build against, and expect to update guest code when these interfaces change.

## Database (Postgres)

Defined in `wit/db.wit`. Provides parameterized SQL queries and transactions through a Postgres connection pool created per namespace by the engine.

### Engine configuration

Add a `[database]` section to `engine.toml` and set `database = true` on each module that should have access:

```toml
[database]
url             = "postgres://user:pass@localhost:5432/mydb"
max_connections = 20   # default contribution from each DB-enabled module

[[module]]
name               = "order-service"
namespace          = "ecommerce"
version            = "1.0.0"
wasm_path          = "modules/order_service.wasm"
schema_path        = "schemas/order_service.binpb"
database           = true # opt in to DB access
db_max_connections = 10   # override this module's contribution

[[module]]
name        = "inventory-service"
namespace   = "ecommerce"
version     = "1.0.0"
wasm_path   = "modules/inventory_service.wasm"
schema_path = "schemas/inventory_service.binpb"
# database omitted — no DB access for this module
```

The engine has one eager administrative pool capped by `[database].max_connections`. Each namespace gets one guest pool whose maximum size is the checked sum of `db_max_connections` (or the `[database].max_connections` default) contributed by every configured DB-enabled module instance in that namespace. Each worker entry also uses one non-pooled `LISTEN` session. The manager generates and stores the namespace credential; the engine uses its administrative pool to converge the role, admin-owned schemas, and grants before readiness. The guest pool authenticates as that namespace role. A module-specific `search_path` selects the default schema for unqualified SQL but does not prevent fully qualified access to other granted schemas in the same namespace. Namespace roles may create objects in granted schemas but cannot drop the schemas or access `wr__jobs`/`wr_system`.

### Transport and resources

The raw WIT interface supports query, execute, streaming query, transactions, and transaction-scoped equivalents. Exact signatures and transport records are owned by [`wit/db.wit`](../wit/db.wit); preferred guest builders and row decoding are documented in the [guest API guide](agents/guest-module-author/api_guide.md#database).

The engine acquires a clean-recycled pool connection under the namespace role, then reapplies the module `search_path` and statement/idle-transaction timeouts on every checkout. Cleanup failure or timeout discards the physical session. Transactions retain one connection until commit, rollback, or drop. At most one transaction cursor may be active; other transaction operations reject until that cursor is drained and protocol synchronization completes. A PostgreSQL statement/stream failure poisons the transaction, so a later `commit` rolls back and returns an error rather than reporting false success. Local parameter conversion and post-execution result conversion do not poison it.

Each `next-batch` accepts `1..=1024`; an empty batch is end-of-stream, and invalid batch sizes consume no rows. Dropping a transaction cursor early drains and synchronizes it before releasing its parent relationship. Completed transaction resources reject later use. Raw guest SQL that issues `BEGIN`, `COMMIT`, or `ROLLBACK` can desynchronize host-owned transaction resources and is unsupported; use the lifecycle methods. `[limits].max_db_transactions` and `max_db_cursors` cap live resources; zero intentionally denies creation, and over-cap creation returns `db-error::connection` without weakening capability or namespace enforcement. Ordinary calls borrow briefly, independent transactions and non-transaction cursors retain a connection, and transaction cursors share their parent's connection; no universal ratio to namespace-pool capacity applies.

### Typed values and strict errors

`pg-value` carries an explicit `pg-type` for SQL `NULL`, so null parameters and null results retain their intended PostgreSQL type. Parameters are bound positionally as `$1`, `$2`, …; a typed null used in an incompatible SQL context, malformed JSONB/numeric/temporal data, or an invalid array element returns an error rather than being coerced.

Result conversion is equally strict. A supported SQL null remains a typed null for SDK `Option<T>` decoding. A PostgreSQL result type with no `pg-value` representation returns `db-error::unsupported-result-type` through query and streaming paths; its payload identifies the optional column name, zero-based column index, PostgreSQL type name, and OID. It is never converted to null and does not rely on a warning. SDK row errors additionally preserve missing/duplicate column, expected/actual type, non-optional-null, cardinality, and JSONB decode context.

### Engine-owned telemetry

Every raw WIT or SDK-builder DB operation creates an engine-owned client span; guest opt-in is neither required nor available. The stable attributes are `db.system.name=postgresql`, `db.operation.name`, `db.namespace` when known, `db.response.returned_rows`, and `error.type` on failure. Query, execute, transaction operations, and a stream's complete lifetime use the guest span as parent when one is active. A stream records one span and accumulates returned rows; exhaustion finishes it successfully, while a host/decode error or early cursor drop finishes it once as an error/cancellation.

`db.query.text` is omitted by default. [`[database.telemetry] include_query_text`](configuration.md#database-telemetry) opts in to whitespace-normalized statement text; bind values are never interpolated or recorded, while SQL literals/comments remain potentially sensitive.

## Blobstore (S3-compatible)

Defined in `wit/blobstore.wit`. Provides object storage operations against an S3-compatible backend configured on the engine.

Available functions:

| Function | Description |
| ---------- | ------------- |
| `put-object(bucket, key, data)` | Upload an object |
| `get-object(bucket, key)` | Download an object's bytes |
| `delete-object(bucket, key)` | Remove an object; returns `NotFound` when it is missing |
| `list-objects(bucket, prefix)` | List objects matching a prefix |
| `head-object(bucket, key)` | Get object metadata (size, etag, last-modified) |

Prefer the scoped `wr_sdk::blobstore::bucket` facade. Raw access remains available via `wr_sdk::bindings::wruntime::blobstore::store` as an escape hatch.

### Example: storing and retrieving objects

```rust
use wr_sdk::blobstore::bucket;

fn save_report(report_id: &str, data: &[u8]) {
    bucket("reports")
        .expect("valid bucket")
        .put(&format!("daily/{report_id}.bin"), data)
        .expect("put failed");
}

fn load_report(report_id: &str) -> Vec<u8> {
    bucket("reports")
        .expect("valid bucket")
        .get(&format!("daily/{report_id}.bin"))
        .expect("get failed")
}

fn list_reports() -> Vec<String> {
    bucket("reports")
        .expect("valid bucket")
        .list("daily/")
        .expect("list failed")
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
let span = wr_sdk::span!("process-order", "order.id" => "123");
wr_sdk::set_attrs!(
    span,
    "order.total" => 45.99,
    "order.flags" => vec![true, false]
);
wr_sdk::event!(span, "validation-passed", "attempt" => 1_i64);
// span ends when dropped
```

Initial span fields, late attributes, and event fields use typed scalar or homogeneous-array values. A `set_attrs!` invocation may add arbitrary late keys and batches them into one WIT call. Convert `u64`, `usize`, and unsigned arrays explicitly to signed values with checked conversion, or record them intentionally as text; implicit lossy conversion is not provided.

Raw access is available via `wr_sdk::bindings::wruntime::tracing::span`.

Each request has a ceiling on the number of concurrently live guest-created spans (`[limits] max_spans`, default **1024**). A span resource is created by `start`/`start-root` and freed when dropped. If a guest tries to open a span beyond the cap, the guest instance is **trapped** (the request fails) — this protects the engine's resource table; it does not crash the engine. Drop spans you no longer need to stay under the cap.

## LLM Inference

Defined in `wit/llm.wit`. Allows modules to call LLM APIs (currently Anthropic Claude) through a host binding. The engine holds the API key — guests never see credentials.

### LLM provider configuration

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
use wr_sdk::llm::{CompletionBuilder, MaxTokens};

fn summarize(text: &str) -> String {
    CompletionBuilder::sonnet()
        .system("You are a concise summarizer.")
        .user(text)
        .max_tokens(MaxTokens::new(256).expect("non-zero token limit"))
        .complete_text()
        .expect("completion failed")
}

// Streaming example
fn stream_response(prompt: &str) -> String {
    let stream = CompletionBuilder::sonnet()
        .user(prompt)
        .max_tokens(MaxTokens::new(1024).expect("non-zero token limit"))
        .stream()
        .expect("stream failed");
    wr_sdk::llm::collect_stream(stream).expect("collect failed")
}
```

Access via `wr_sdk::bindings::wruntime::llm::inference` (raw WIT binding) or `wr_sdk::llm` (ergonomic helpers).

### Streaming

`complete-stream` returns a `CompletionStream` cursor whose `next()` yields typed `StreamEvent` values in a guaranteed order: zero or more `TextDelta`, then exactly one `Usage`, then exactly one `Stop`, then `None` (idempotent thereafter). `usage()` returns `None` until the terminal `Usage` event has been observed via `next()`. Stream-level errors, transport failures, and truncated streams surface as an `LlmError` from `next()`.

Tool-use is **not** supported while streaming: `complete-stream` pre-rejects tool-enabled requests with `LlmError::InvalidRequest` before any upstream call — use `complete()` for tool calls. Extended-thinking, signature, and citation deltas from the upstream API are dropped (they have no WIT representation). The `wr_sdk::llm::collect_stream` helper (used above) drains the cursor and accumulates the text deltas into a `String`. See the guest API guide's [LLM semantics](agents/guest-module-author/api_guide.md#llm); exact stream types remain owned by [`wit/llm.wit`](../wit/llm.wit).

The LLM API key is host configuration and never enters the guest sandbox. General guest secrets use a separate module environment mechanism:

```toml
[module.env]
PUBLIC_MODE = "production"
API_TOKEN = { secret = true }
```

The manager resolves `API_TOKEN` in the module namespace during registration. The engine passes only the resolved value as an environment variable; the guest receives no secret-store API, identifier, or provider credential. Missing secrets fail registration/startup. See [module environment values](configuration.md#module-environment-values).

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

The directory is created fresh on the host for every dispatch and deleted when that dispatch's store is dropped. This applies to both service requests and worker jobs: each gets a new store and therefore a new empty temp directory. It is not shared between module instances or dispatches. Use it only for scratch space or temporary files — do not rely on it for cross-request caching or durable state.

| Value | Effect |
| --- | --- |
| `fs = "tempdir"` | Mount an ephemeral temp directory at `/` |
| *(omitted)* | No filesystem access (default) |
