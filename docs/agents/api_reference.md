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

/// Send response with application/json content-type.
pub fn send_json_response(response_out: ResponseOutparam, status: u16, body: Vec<u8>)

/// Send response with custom content-type.
pub fn send_response_with_content_type(
    response_out: ResponseOutparam, status: u16, body: Vec<u8>, content_type: &str,
)

/// Response returned by generated service routers.
pub struct ServiceResponse {
    pub status: u16,
    pub body: Vec<u8>,
    pub content_type: &'static str,
}

impl ServiceResponse {
    pub fn new(status: u16, body: Vec<u8>, content_type: &'static str) -> Self;
    pub fn protobuf(status: u16, body: Vec<u8>) -> Self;
    pub fn json(status: u16, body: Vec<u8>) -> Self;
    pub fn json_error(status: u16, msg: &str) -> Self;
}

/// Write a generated service response with its declared content-type.
pub fn send_service_response(response_out: ResponseOutparam, response: ServiceResponse)

/// Return a JSON error body: (status, b'{"error":"msg"}')
pub fn err_body(status: u16, msg: &str) -> (u16, Vec<u8>)

/// Serialize a value as JSON and return (status, body). Requires `serde` feature.
#[cfg(feature = "serde")]
pub fn json_body(status: u16, value: &impl serde::Serialize) -> (u16, Vec<u8>)
```

## wr_sdk::http

Source: `wr-sdk/src/http.rs`

```rust
/// Errors from outbound HTTP requests.
pub enum HttpError {
    /// Non-success HTTP status with response body.
    Status { code: u16, body: Vec<u8> },
    /// Transport-level failure (DNS, connection refused, timeout).
    Transport(String),
    /// Failed to decode the response body.
    Decode(String),
}

impl HttpError {
    pub fn status_code(&self) -> Option<u16>;
    pub fn is_status(&self, code: u16) -> bool;
}

/// HTTP method for outbound requests.
pub enum Method { Get, Post, Put, Delete, Patch, Head, Options }

/// An outbound HTTP request descriptor.
pub struct HttpRequest<'a> {
    pub authority: &'a str,
    pub path: &'a str,
    pub method: Method,
    pub headers: &'a [(&'a str, &'a [u8])],
    pub body: &'a [u8],
}

/// An HTTP response with status and body.
pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

impl HttpResponse {
    /// Interpret body as UTF-8.
    pub fn text(&self) -> Result<&str, HttpError>;
    /// Decode body as protobuf message.
    pub fn decode<T: prost::Message + Default>(&self) -> Result<T, HttpError>;
    /// Return Err if status is not 2xx.
    pub fn error_for_status(self) -> Result<Self, HttpError>;
}

/// Timeout configuration for HTTP requests.
pub struct Timeouts {
    pub connect: Option<Duration>,
    pub first_byte: Option<Duration>,
    pub between_bytes: Option<Duration>,
}

impl Timeouts {
    pub fn uniform(d: Duration) -> Self;
}

/// Execute an HTTP request.
pub fn http_request(req: &HttpRequest) -> Result<HttpResponse, HttpError>;

/// Execute an HTTP request with timeouts.
pub fn http_request_with_timeouts(req: &HttpRequest, timeouts: &Timeouts) -> Result<HttpResponse, HttpError>;

/// Legacy convenience wrapper: POST protobuf body to http://{authority}{path}.
/// New code should prefer http_request for typed errors and method flexibility.
pub fn http_rpc(authority: &str, path: &str, body: &[u8]) -> Result<(u16, Vec<u8>), String>;
```

Generated clients now return `Result<T, HttpError>` instead of `Result<T, String>`.
Use `e.is_status(409)` instead of `e.contains("HTTP 409")` for status matching.
`HttpError` implements `Display` so `format!("{e}")` works everywhere.
`From<HttpError> for ServiceError` is provided for `?` propagation in handlers.

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

/// Set a span attribute. Accepts any Display type (no manual .to_string() needed).
pub fn set_attr(span: &ActiveSpan, key: &str, value: impl Display)

/// Record a point-in-time event on a span.
pub fn record_event(span: &ActiveSpan, name: &str, attrs: &[(&str, &str)])

/// Mark the span as failed.
pub fn set_error(span: &ActiveSpan, message: &str)
```

### span! macro

```rust
/// Create a span with attributes that accept any Display value (no manual .to_string()).
let sp = wr_sdk::span!("inventory.buy", "product.id" => req.product_id, "qty" => req.quantity);
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

Error conversions — all error types convert to `ServiceError` via `From`, enabling `?`:

- `From<HttpError> for ServiceError` — HTTP client errors
- `From<DbError> for ServiceError` — database errors
- `From<BlobError> for ServiceError` — blobstore errors
- `From<LlmError> for ServiceError` — LLM errors

## wr_sdk::prelude

Source: `wr-sdk/src/prelude.rs`. Import with `use wr_sdk::prelude::*` to get:

`IncomingRequest`, `Method`, `ResponseOutparam`, `database`, `PgValue`, `UnpackRow`,
`err_body`, `read_body`, `send_response`, `send_json_response`,
`send_service_response`, `ServiceResponse`, `json_body` (requires `serde` feature),
`ServiceError`, `tracing`, `ServiceGuest`.

## wr_sdk::db

Source: `wr-sdk/src/db.rs`

### FromPgValue trait

```rust
/// Trait for types extractable from a PgValue column.
pub trait FromPgValue: Sized {
    fn from_pg(col: usize, val: &PgValue) -> Result<Self, ServiceError>;
}
// Implemented for: i64, i32, String, bool, f64
```

### Row helpers

```rust
impl Row {
    /// Generic typed column extraction via FromPgValue.
    fn get<T: FromPgValue>(&self, col: usize) -> Result<T, ServiceError>;

    fn get_text(&self, col: usize) -> Result<&str, ServiceError>;
    fn get_i64(&self, col: usize) -> Result<i64, ServiceError>;
    fn get_i32(&self, col: usize) -> Result<i32, ServiceError>;
    fn get_bool(&self, col: usize) -> Result<bool, ServiceError>;
    fn get_f64(&self, col: usize) -> Result<f64, ServiceError>;
    fn get_jsonb(&self, col: usize) -> Result<&str, ServiceError>;
}
```

### UnpackRow trait

```rust
/// Extract multiple typed columns from a row in one call.
/// Implemented for tuples of 2–8 elements where each element is FromPgValue.
pub trait UnpackRow<T> {
    fn unpack(&self) -> Result<T, ServiceError>;
}

// Example:
let (trade_id, buyer, seller, qty, price): (i64, String, String, i64, i64) =
    row.unpack()?;
```

### Transaction guard (auto-rollback on drop)

```rust
/// Begin a transaction wrapped in TxGuard. Rolls back automatically on drop.
pub fn transaction() -> Result<TxGuard, ServiceError>;

impl TxGuard {
    fn query(&self, sql: &str, params: &[PgValue]) -> Result<Vec<Row>, ServiceError>;
    fn execute(&self, sql: &str, params: &[PgValue]) -> Result<u64, ServiceError>;
    fn commit(self) -> Result<(), ServiceError>;  // consumes guard, no rollback
}
```

## Generated code (wr-build)

The service generator emits a router returning `wr_sdk::io::ServiceResponse` and a `_handle` function alongside each router. Successful generated responses are protobuf; generated errors are JSON.

```rust
pub fn inventory_service_router<T: InventoryService>(
    svc: &T,
    path: &str,
    body: &[u8],
) -> wr_sdk::io::ServiceResponse;
```

The service generator emits a `_handle` function alongside each router:

```rust
/// Default ServiceGuest handler — reads body, routes, sends response.
pub fn inventory_service_handle<T: InventoryService>(
    svc: &T,
    request: IncomingRequest,
    response_out: ResponseOutparam,
);
```

Usage in handler modules:

```rust
impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        proto::inventory_service_handle(&Component, request, response_out);
    }
}
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
    TooLarge(String),  // upload/download/listing exceeds a host-configured limit
}
```

Host-enforced limits (from the engine `[blobstore]` config, global across modules):
`max_object_size` (default 16 MiB) caps both `put_object` uploads and `get_object` downloads —
an oversized download is aborted mid-stream, never fully buffered; `max_list_objects` (default 1000)
caps `list_objects`. Exceeding either returns `BlobError::TooLarge`.

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

/// Streaming completion — returns a cursor of typed stream events.
inference::complete_stream(req: &CompletionRequest) -> Result<CompletionStream, LlmError>
```

Types:

```rust
enum StreamEvent {
    TextDelta(String),   // incremental text
    Usage(TokenUsage),   // final aggregate usage (emitted once, before Stop)
    Stop(String),        // terminal stop_reason, emitted once
}
```

### CompletionStream resource

```rust
impl CompletionStream {
    /// Pull the next event. Ok(None) when finished (idempotent thereafter).
    /// Event order: zero+ TextDelta, then exactly one Usage, then one Stop, then None.
    fn next(&self) -> Result<Option<StreamEvent>, LlmError>

    /// Final usage — None until the terminal Usage event has been observed via next().
    fn usage(&self) -> Option<TokenUsage>
}
// Dropping cancels the stream.
```

Tool-use is not supported while streaming: `complete_stream` rejects tool-enabled requests with `LlmError::InvalidRequest` before any upstream call; use `complete()` for tool calls. Stream-level errors, transport failures, and truncated streams surface as an `LlmError` from `next()`. Extended-thinking / signature / citation deltas are dropped (no WIT representation).

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

enum StreamEvent {
    TextDelta(String),
    Usage(TokenUsage),
    Stop(String),
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
    worker_version: &str,
    job_type: &str,
    payload: &[u8],
) -> Result<String, HttpError>

/// Submit a job with explicit timeout and retry settings.
/// Pass 0 for timeout_secs or max_attempts to use engine-configured worker defaults.
pub fn submit_job_with_options(
    engine_authority: &str,
    worker_version: &str,
    job_type: &str,
    payload: &[u8],
    timeout_secs: i32,
    max_attempts: i32,
) -> Result<String, HttpError>

/// Query the status of a previously submitted job.
pub fn get_job_status(engine_authority: &str, job_id: &str) -> Result<JobStatus, HttpError>
```

worker_version must be non-empty; submit calls encode it in SubmitJobRequest.worker_version and send x-wr-version for route pinning. Empty versions return HttpError::Transport("worker_version is required") without issuing HTTP.

### Types

```rust
pub struct JobStatus {
    pub job_id: String,
    pub status: String,         // "pending" | "running" | "complete" | "failed" | "dead"
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

/// Generates a versioned job submission client for services ending with WorkerService.
/// Clients store authority and version, use new(authority, version), submit jobs,
/// query raw status, and fetch typed optional results.
pub struct WrWorkerClientGenerator;

/// Runs two generators on every service definition.
/// Use when a module is both handler and client.
pub struct WrCombinedGenerator<A, B>;

impl<A, B> WrCombinedGenerator<A, B> {
    pub fn new(a: A, b: B) -> Self;
}
```

All implement `prost_build::ServiceGenerator`. Generated paths require non-empty proto packages and use `/{proto_package}.{ProtoServiceName}/{ProtoMethodName}`.

```rust
pub struct WorkerServiceClient { authority: String, version: String }
impl WorkerServiceClient {
    pub fn new(authority: impl Into<String>, version: impl Into<String>) -> Self;
    pub fn process_task(&self, req: ProcessTaskRequest) -> Result<String, wr_sdk::http::HttpError>;
    pub fn process_task_with_options(
        &self,
        req: ProcessTaskRequest,
        timeout_secs: i32,
        max_attempts: i32,
    ) -> Result<String, wr_sdk::http::HttpError>;
    pub fn get_status(&self, job_id: &str) -> Result<wr_sdk::jobs::JobStatus, wr_sdk::http::HttpError>;
    pub fn get_process_task_result(
        &self,
        job_id: &str,
    ) -> Result<Option<ProcessTaskResponse>, wr_sdk::http::HttpError>;
}
```

Result helpers map pending/running to `Ok(None)`, complete to a decoded non-empty result, completed empty/invalid result to `HttpError::Decode`, failed/dead to `HttpError::Status { code: 500, body }`, not-found propagates `404`, and unknown status to `HttpError::Decode`.
