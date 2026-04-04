# API Reference

Exact function signatures for all APIs callable from wruntime guest modules. **Do not guess APIs that are not listed here.**

> **Maintainers:** keep this file in sync with the source. When modifying `wr-sdk/`, `wr-build/`, or `wit/` interfaces, update the corresponding section below.

## wr_sdk::io

Source: `wr-sdk/src/io.rs`

```rust
/// Drain an IncomingBody into a Vec<u8>.
pub fn read_body(incoming: IncomingBody) -> Vec<u8>

/// Write a response with the given status and body bytes.
/// Content-Type is set to application/x-protobuf.
pub fn send_response(response_out: ResponseOutparam, status: u16, body: Vec<u8>)

/// Return a JSON error body: (status, b'{"error":"msg"}')
pub fn err_body(status: u16, msg: &str) -> (u16, Vec<u8>)
```

## wr_sdk::http

Source: `wr-sdk/src/http.rs`

```rust
/// POST protobuf body to http://{authority}{path}.
/// Returns (http_status, response_bytes) on success.
/// The authority is the module address: "namespace.module" (e.g. "ecommerce.inventory").
pub fn http_rpc(authority: &str, path: &str, body: &[u8]) -> Result<(u16, Vec<u8>), String>
```

## wr_sdk::log

Source: `wr-sdk/src/log.rs`

```rust
/// Write msg + newline to WASI stderr.
pub fn log(msg: &str)
```

## wr_sdk::tracing

Source: `wr-sdk/src/tracing.rs`

```rust
/// Start a new child span under the current request span.
pub fn start(name: &str, attrs: &[(&str, &str)]) -> ActiveSpan

/// Record a key/value attribute on a span.
pub fn set_attribute(span: &ActiveSpan, key: &str, value: &str)

/// Record a point-in-time event on a span.
pub fn record_event(span: &ActiveSpan, name: &str, attrs: &[(&str, &str)])

/// Mark the span as failed.
pub fn set_error(span: &ActiveSpan, message: &str)
```

`ActiveSpan` ends automatically when dropped. Type: `wr_sdk::bindings::wruntime::tracing::span::ActiveSpan`.

## wr_sdk traits and macros

Source: `wr-sdk/src/lib.rs`

```rust
/// Trait for HTTP handler modules.
pub trait ServiceGuest {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam);

    /// Called on GET /__health. Return false to mark unhealthy. Default: true.
    fn health_check() -> bool { true }
}

/// Error type returned by generated service traits.
pub struct ServiceError {
    pub status: u16,
    pub message: String,
}

impl ServiceError {
    pub fn bad_request(msg: impl Into<String>) -> Self   // 400
    pub fn not_found(msg: impl Into<String>) -> Self     // 404
    pub fn conflict(msg: impl Into<String>) -> Self      // 409
    pub fn internal(msg: impl Into<String>) -> Self      // 500
}
```

Export macros:

```rust
// Register T as wasi:http/incoming-handler (handler modules)
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

```

## wruntime:db/database@0.4.0

Source: `wit/db.wit`. Import path: `wr_sdk::bindings::wruntime::db::database`.

### Top-level functions

```rust
/// SELECT — returns all matching rows.
/// Parameters bound positionally as $1, $2, ...
database::query(sql: &str, params: &[PgValue]) -> Result<Vec<Row>, DbError>

/// INSERT/UPDATE/DELETE — returns rows affected.
database::execute(sql: &str, params: &[PgValue]) -> Result<u64, DbError>

/// SELECT with streaming cursor.
database::query_stream(sql: &str, params: &[PgValue]) -> Result<RowCursor, DbError>

/// Begin a transaction (issues BEGIN).
database::begin_transaction() -> Result<Transaction, DbError>
```

### Transaction resource

```rust
impl Transaction {
    fn query(&self, sql: &str, params: &[PgValue]) -> Result<Vec<Row>, DbError>
    fn execute(&self, sql: &str, params: &[PgValue]) -> Result<u64, DbError>
    fn query_stream(&self, sql: &str, params: &[PgValue]) -> Result<RowCursor, DbError>
    fn commit(&self) -> Result<(), DbError>
    fn rollback(&self) -> Result<(), DbError>
}
// Dropping without commit automatically rolls back.
```

### RowCursor resource

```rust
impl RowCursor {
    /// Pull up to max rows. Empty list = exhausted.
    fn next_batch(&self, max: u32) -> Result<Vec<Row>, DbError>
}
// Dropping cancels the query and returns connection to pool.
```

### Types

```rust
struct Row {
    columns: Vec<Column>,
}

struct Column {
    name: String,
    value: PgValue,
}

enum DbError {
    Connection(String),
    Query(String),
}

struct PgInterval {
    months: i32,
    days: i32,
    microseconds: i64,
}
```

### PgValue variants

| Variant | Postgres type | Rust type |
|---------|--------------|-----------|
| `PgValue::Null` | SQL NULL | — |
| `PgValue::Boolean(bool)` | `BOOL` | `bool` |
| `PgValue::Int2(i16)` | `SMALLINT` | `i16` |
| `PgValue::Int4(i32)` | `INTEGER` | `i32` |
| `PgValue::Int8(i64)` | `BIGINT` | `i64` |
| `PgValue::Float4(f32)` | `REAL` | `f32` |
| `PgValue::Float8(f64)` | `DOUBLE PRECISION` | `f64` |
| `PgValue::Text(String)` | `TEXT / VARCHAR / CHAR` | `String` |
| `PgValue::Bytea(Vec<u8>)` | `BYTEA` | `Vec<u8>` |
| `PgValue::Timestamptz(i64)` | `TIMESTAMPTZ` | microseconds since Unix epoch (UTC) |
| `PgValue::Timestamp(i64)` | `TIMESTAMP` | microseconds since Unix epoch (naive) |
| `PgValue::Date(i32)` | `DATE` | days since Unix epoch |
| `PgValue::Time(i64)` | `TIME` | microseconds since midnight |
| `PgValue::Interval(PgInterval)` | `INTERVAL` | `{ months, days, microseconds }` |
| `PgValue::Numeric(String)` | `NUMERIC / DECIMAL` | decimal string (lossless) |
| `PgValue::Uuid((u64, u64))` | `UUID` | `(high_u64, low_u64)` |
| `PgValue::Jsonb(String)` | `JSON / JSONB` | serialized JSON string |
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

## wruntime:blobstore/store@0.1.0

Source: `wit/blobstore.wit`. Import path: `wr_sdk::bindings::wruntime::blobstore::store`.

```rust
/// Upload bytes. Creates or overwrites.
store::put_object(bucket: &str, key: &str, data: &[u8]) -> Result<(), BlobError>

/// Download full object.
store::get_object(bucket: &str, key: &str) -> Result<Vec<u8>, BlobError>

/// Delete object. Succeeds even if object does not exist.
store::delete_object(bucket: &str, key: &str) -> Result<(), BlobError>

/// List objects, optionally filtered by key prefix.
store::list_objects(bucket: &str, prefix: Option<&str>) -> Result<Vec<ObjectMeta>, BlobError>

/// Get metadata without downloading body.
store::head_object(bucket: &str, key: &str) -> Result<ObjectMeta, BlobError>
```

### Types

```rust
struct ObjectMeta {
    key: String,
    size: u64,           // bytes
    last_modified: i64,  // seconds since Unix epoch (UTC)
    etag: String,        // may include surrounding quotes
}

enum BlobError {
    NotFound(String),
    AccessDenied(String),
    Io(String),
}
```

## wruntime:tracing/span@0.1.0

Source: `wit/tracing.wit`. Import path: `wr_sdk::bindings::wruntime::tracing::span`.

```rust
/// Start a new child span. Returns an ActiveSpan resource.
span::start(name: &str, attrs: &[(String, String)]) -> ActiveSpan

impl ActiveSpan {
    fn set_attribute(&self, key: &str, value: &str)
    fn record_event(&self, name: &str, attrs: &[(String, String)])
    fn set_error(&self, message: &str)
}
// Span ends when ActiveSpan is dropped.
```

Note: the `wr_sdk::tracing` helpers accept `&[(&str, &str)]` and convert to owned strings internally. Prefer using `wr_sdk::tracing::start()` over the raw WIT binding.

## wruntime:llm/inference@0.1.0

Source: `wit/llm.wit`. Import path: `wr_sdk::bindings::wruntime::llm::inference`.

### Top-level functions

```rust
/// Single-shot completion.
inference::complete(req: &CompletionRequest) -> Result<CompletionResponse, LlmError>

/// Streaming completion — returns a cursor that yields text deltas.
inference::complete_stream(req: &CompletionRequest) -> Result<CompletionStream, LlmError>
```

### CompletionStream resource

```rust
impl CompletionStream {
    /// Pull the next text chunk. Returns None when stream is finished.
    fn next(&self) -> Result<Option<String>, LlmError>

    /// Final usage stats (available after stream exhausted).
    fn usage(&self) -> Option<TokenUsage>
}
// Dropping cancels the stream.
```

### Types

```rust
struct CompletionRequest {
    model: String,                // e.g. "claude-sonnet-4-6"
    messages: Vec<Message>,
    system: Option<String>,
    max_tokens: u32,
    temperature: Option<f32>,
    tools: Vec<ToolDef>,
}

struct Message {
    role: MessageRole,   // User | Assistant
    content: String,
}

struct ToolDef {
    name: String,
    description: String,
    input_schema: String,   // JSON Schema string
}

struct CompletionResponse {
    completion: Completion,
    usage: TokenUsage,
    stop_reason: String,    // "end_turn" | "tool_use" | "max_tokens"
}

enum Completion {
    Text(String),
    ToolCalls(Vec<ToolUse>),
}

struct ToolUse {
    id: String,
    name: String,
    input: String,   // JSON-encoded arguments
}

struct TokenUsage {
    input_tokens: u32,
    output_tokens: u32,
}

enum LlmError {
    InvalidRequest(String),
    Auth(String),
    RateLimited(Option<u32>),   // retry-after seconds
    Api(String),
}
```

## wr_sdk::llm

Source: `wr-sdk/src/llm.rs`

```rust
/// Builder for completion requests.
pub struct CompletionBuilder { /* ... */ }

impl CompletionBuilder {
    pub fn new(model: &str) -> Self
    pub fn sonnet() -> Self              // claude-sonnet-4-6
    pub fn haiku() -> Self               // claude-haiku-4-5-20251001
    pub fn system(self, s: impl Into<String>) -> Self
    pub fn user(self, content: impl Into<String>) -> Self
    pub fn assistant(self, content: impl Into<String>) -> Self
    pub fn max_tokens(self, n: u32) -> Self
    pub fn temperature(self, t: f32) -> Self
    pub fn tool(self, name: &str, description: &str, schema: &str) -> Self
    pub fn complete(self) -> Result<CompletionResponse, LlmError>
    pub fn stream(self) -> Result<CompletionStream, LlmError>
    pub fn complete_text(self) -> Result<String, LlmError>  // text-only shorthand
}

/// Collect a stream into a single string.
pub fn collect_stream(stream: CompletionStream) -> Result<String, LlmError>
```

## wr_sdk::jobs

Source: `wr-sdk/src/jobs.rs`

```rust
/// Submit a job to a worker module's engine-managed queue.
/// `engine_authority` is the worker's `namespace.name` (e.g. "codegen.worker").
/// Returns the job_id on success.
pub fn submit_job(
    engine_authority: &str,
    job_type: &str,
    payload: &[u8],
) -> Result<String, String>

/// Submit a job with explicit timeout and retry settings.
/// Pass 0 for timeout_secs or max_attempts to use the worker's defaults.
pub fn submit_job_with_options(
    engine_authority: &str,
    job_type: &str,
    payload: &[u8],
    timeout_secs: i32,
    max_attempts: i32,
) -> Result<String, String>

/// Query the status of a previously submitted job.
pub fn get_job_status(engine_authority: &str, job_id: &str) -> Result<JobStatus, String>
```

### Types

```rust
pub struct JobStatus {
    pub job_id: String,
    pub status: String,         // "pending" | "running" | "complete" | "failed"
    pub result: Vec<u8>,
    pub error_message: String,
    pub attempt: i32,
    pub max_attempts: i32,
}
```

## wr-build code generators

Source: `wr-build/src/lib.rs`. Used in `build.rs`.

```rust
/// Generates a trait + router function per proto service.
/// Use for handler modules.
pub struct WrServiceGenerator;

/// Generates a Client struct per proto service.
/// Use for client/runner modules.
pub struct WrClientGenerator;

/// Runs two generators on every service definition.
/// Use when a module is both handler and client.
pub struct WrCombinedGenerator<A, B>;

impl<A, B> WrCombinedGenerator<A, B> {
    pub fn new(a: A, b: B) -> Self;
}
```

All implement `prost_build::ServiceGenerator`.
