# Worker Mode for Guest WASM Modules

## Context

Currently wruntime has two module types: **service guests** (HTTP request handlers with per-request instantiation) and **run guests** (single long-running instance). The codegen example implements a worker pattern manually using a run guest that polls a coordinator for tasks, but this forces every worker to re-implement the polling loop, task claiming, status tracking, and retry logic. This change builds worker functionality into the engine so worker modules only need to implement typed HTTP handlers — the engine pulls jobs from a queue and dispatches them as HTTP requests.

## Design Overview

Workers are **not a new guest type**. A worker is a regular service guest (`export wasi:http/incoming-handler`) with `mode = "worker"` in `engine.toml`. The engine manages the job lifecycle and dispatches jobs to the module as HTTP requests:

- **Same WIT world** as service modules — `export wasi:http/incoming-handler`
- **Same SDK** — `ServiceGuest` trait, `export!` macro, `WrServiceGenerator` for typed handlers
- **Same `ProxyPre` instantiation path** — per-request isolation, identical host bindings
- **New `WrWorkerClientGenerator`** — generates typed job submission clients for other modules
- Engine-managed job queue in Postgres (`wr__jobs` schema)
- Configurable concurrency (N worker tasks polling the queue in parallel)
- gRPC endpoints on the engine for job submission and status retrieval

### How dispatch works

The engine's worker loop:
1. Claims a job from the Postgres queue (`job_type`, `payload`)
2. Constructs an HTTP request: `POST /{job_type}` with the payload as the request body, `x-wr-job-id` header for tracing
3. Sends it to the module via the existing `ProxyPre` + `ModuleRegistry` channel (same `InboundRequest` path as proxy-originated traffic)
4. Maps the HTTP response to job outcome: 2xx → success (body = result), 4xx/5xx → failure (body = error message)

This means the worker module's handlers are indistinguishable from service handlers — they can even be called directly via HTTP during development/testing.

## Implementation Steps

### 1. No WIT or SDK Guest Changes

Workers use the existing service guest world (`export wasi:http/incoming-handler`), the existing `ServiceGuest` trait, and the existing `export!` macro. No changes to `wit/`, `wr-sdk/src/lib.rs`, or `wr-build`'s `WrServiceGenerator`.

The worker module's `wit/world.wit` is identical to any service module:

```wit
package codegen:worker@1.0.0;

world worker {
  import wruntime:db/database@0.4.0;
  import wruntime:tracing/span;
  import wruntime:blobstore/store@0.1.0;
  import wruntime:llm/inference@0.1.0;
  import wasi:cli/stderr@0.2.6;
  import wasi:cli/environment@0.2.6;
  import wasi:cli/exit@0.2.6;
  import wasi:http/types@0.2.6;
  import wasi:http/outgoing-handler@0.2.6;
  import wasi:io/streams@0.2.6;
  import wasi:io/poll@0.2.6;
  import wasi:io/error@0.2.6;
  import wasi:clocks/monotonic-clock@0.2.6;
  import wasi:random/random@0.2.6;
  export wasi:http/incoming-handler@0.2.6;
}
```

### 2. Code Generation — `wr-build/src/lib.rs`

Add one new generator: **`WrWorkerClientGenerator`** (mirrors `WrClientGenerator` pattern). The worker module itself uses the existing `WrServiceGenerator` — its handlers are standard HTTP handlers.

**`WrWorkerClientGenerator`** — generates typed job submission clients:

```rust
// For a proto service `TaskWorker` with RPC `ProcessTask`:
pub struct TaskWorkerClient {
    authority: String,  // worker's namespace.name, e.g. "codegen.worker"
}

impl TaskWorkerClient {
    pub fn new(authority: impl Into<String>) -> Self { ... }

    pub fn process_task(&self, req: ProcessTaskRequest) -> Result<String, String> {
        // Serializes req, submits job via engine gRPC, returns job_id
        let payload = req.encode_to_vec();
        let job_type = "/codegen.task_worker/ProcessTask";
        wr_sdk::jobs::submit_job(&self.authority, job_type, &payload)
    }
}
```

The client submits the job via the engine's `SubmitJob` gRPC endpoint. Returns the `job_id` for status polling.

**Schema tracking:** Worker modules declare `schema_path` in `engine.toml` just like service modules. The proto file defines the worker's RPC methods (job types) and their request/response message types. These schemas are uploaded to the manager on engine registration, providing discoverability and enabling typed client generation.

### 3. SDK Job Helpers — New file `wr-sdk/src/jobs.rs`

Low-level functions used by `WrWorkerClientGenerator` output and directly by modules:

```rust
pub fn submit_job(engine_authority: &str, job_type: &str, payload: &[u8]) -> Result<String, String>
pub fn submit_job_with_options(engine_authority: &str, job_type: &str, payload: &[u8], timeout_secs: i32, max_attempts: i32) -> Result<String, String>
pub fn get_job_status(engine_authority: &str, job_id: &str) -> Result<JobStatus, String>
```

These use `wr_sdk::http::http_rpc()` to call the engine's gRPC endpoints through the proxy.

### 4. Engine Config — `wr-engine/src/config.rs`

Add `ModuleMode` enum and worker-specific fields to `ModuleConfig`:

```rust
#[derive(Deserialize, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ModuleMode {
    #[default]
    Service,
    Run,
    Worker,
}

// New fields on ModuleConfig:
pub mode: ModuleMode,                          // default: Service
pub worker_concurrency: usize,                 // default: 4
pub worker_poll_interval_secs: u64,            // default: 2
pub worker_job_timeout_secs: u64,              // default: 300
pub worker_max_attempts: i32,                  // default: 3
```

Example `engine.toml`:
```toml
[[module]]
name = "worker"
namespace = "codegen"
version = "1.0.0"
wasm_path = "worker.wasm"
schema_path = "./schemas/worker.binpb"   # proto schema for job types (same system as services)
mode = "worker"
database = true
worker_concurrency = 4
worker_job_timeout_secs = 600
```

### 5. Postgres Job Queue Schema

New `wr__jobs` schema provisioned by the engine at startup when database is configured:

```sql
CREATE SCHEMA IF NOT EXISTS wr__jobs;

CREATE TABLE IF NOT EXISTS wr__jobs.jobs (
    job_id            TEXT        PRIMARY KEY,
    worker_namespace  TEXT        NOT NULL,
    worker_name       TEXT        NOT NULL,
    worker_version    TEXT        NOT NULL,
    job_type          TEXT        NOT NULL DEFAULT '/',
    payload           BYTEA      NOT NULL DEFAULT '',
    status            TEXT        NOT NULL DEFAULT 'pending',
    result            BYTEA,
    error_message     TEXT,
    attempt           INT         NOT NULL DEFAULT 0,
    max_attempts      INT         NOT NULL DEFAULT 3,
    timeout_secs      INT         NOT NULL DEFAULT 300,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at        TIMESTAMPTZ,
    completed_at      TIMESTAMPTZ,
    claimed_by        TEXT,
    source_namespace  TEXT        NOT NULL DEFAULT '',
    source_module     TEXT        NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_jobs_pending
    ON wr__jobs.jobs (worker_namespace, worker_name, created_at)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_jobs_stale
    ON wr__jobs.jobs (claimed_at)
    WHERE status IN ('claimed', 'running');

-- Trigger to notify workers immediately on job insert
CREATE OR REPLACE FUNCTION wr__jobs.notify_new_job() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('wr_jobs_' || NEW.worker_namespace || '_' || NEW.worker_name, NEW.job_id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_notify_new_job
    AFTER INSERT ON wr__jobs.jobs
    FOR EACH ROW EXECUTE FUNCTION wr__jobs.notify_new_job();
```

### 6. Worker Engine Module — New file `wr-engine/src/worker.rs`

Core worker pool logic. Workers reuse `ProxyPre` and the `ModuleRegistry` channel — the worker loop acts as an internal client that sends `InboundRequest`s to the same `http_handler_task` that handles proxy traffic.

- **`worker_pool_task()`**: Spawns `worker_concurrency` sub-tasks + LISTEN connection + stale recovery task
- **`listen_task()`**: Dedicated `tokio-postgres` connection that runs `LISTEN wr_jobs_{namespace}_{name}`. On each notification, sends a wake signal to all worker loops via a `tokio::sync::Notify`. This connection is separate from the deadpool — it must stay open for the lifetime of the worker pool.
- **`worker_loop()`**: Claim-dispatch loop per worker task:
  1. Wait for wake signal from `Notify` (or fall back to `poll_interval` timeout as a safety net — ensures progress even if a notification is missed)
  2. `claim_job()` via `SELECT...FOR UPDATE SKIP LOCKED`
  3. Update status to `running`
  4. Build an `http::Request<Bytes>`: `POST /{job_type}` with payload body, `x-wr-job-id: {job_id}` header
  5. Send via `ModuleTx` channel (same `InboundRequest` as proxy-originated requests) — this dispatches through `ProxyPre` with fresh `Store` + `ModuleState` per request
  6. Await the response via the oneshot `response_tx`
  7. On 2xx → status `complete`, store response body as result bytes
  8. On 4xx/5xx or error → status `failed`, increment attempt; if retries remain → reset to `pending`, else → `dead`
  9. After processing a job, loop immediately back to step 2 (drain pending jobs before waiting for next notification)
- **`recover_stale_jobs()`**: Every 30s, reset timed-out jobs

### 7. Engine Integration — `wr-engine/src/engine.rs`

Three-way dispatch in `spawn_module()` based on config `mode`:

```
match module_config.mode {
    Worker → ProxyPre (same as Service), register in ModuleRegistry,
             then also spawn worker_pool_task with the ModuleTx sender
    Service → ProxyPre (current path, unchanged)
    Run → run export (current path, unchanged)
}
```

Key difference from Service: worker modules register in `ModuleRegistry` (so the worker loop can dispatch to them) but do **not** register routing rules with the manager (they don't receive external HTTP traffic). The worker pool task holds a clone of the `ModuleTx` sender and pushes `InboundRequest`s into it.

Workers get the same host bindings (DB, blobstore, LLM, tracing) via `ModuleState`.

### 8. gRPC Job Endpoints — `wr-engine/src/server.rs`

Add gRPC endpoints handled directly by the engine server (not dispatched to WASM):

- `POST /wruntime.WorkerService/SubmitJob` → insert into `wr__jobs.jobs`, return `job_id`
- `POST /wruntime.WorkerService/GetJobStatus` → query by `job_id`, return status/result

Proto messages to add to `proto/wruntime.proto`:

```protobuf
message SubmitJobRequest {
    string worker_namespace = 1;
    string worker_name = 2;
    string worker_version = 3;
    string job_type = 4;
    bytes payload = 5;
    int32 timeout_secs = 6;
    int32 max_attempts = 7;
}

message SubmitJobResponse {
    string job_id = 1;
}

message GetJobStatusRequest {
    string job_id = 1;
}

message GetJobStatusResponse {
    string job_id = 1;
    string status = 2;
    bytes result = 3;
    string error_message = 4;
    int32 attempt = 5;
    int32 max_attempts = 6;
}
```

The engine needs the DB pool passed to the server for these endpoints.

### 9. Engine Startup — `wr-engine/src/main.rs`

1. Provision `wr__jobs` schema/tables (between DB provisioning and module migrations)
2. Skip routing-rule upsert for `mode = "worker"` modules (they don't receive external HTTP traffic)
3. Workers ARE registered with manager (including their proto schema via `schema_path`) for visibility, heartbeat, and schema discovery — just no routing rules

### 10. Codegen Worker Migration — `examples/codegen/worker/`

Convert from run guest to service guest with worker mode:
- **`wit/world.wit`**: Export `wasi:http/incoming-handler` instead of `run`
- **`src/lib.rs`**: Replace `RunGuest::run()` polling loop with `ServiceGuest::handle()` + typed handlers via `WrServiceGenerator`. The handlers are identical to any service module — the engine delivers jobs as HTTP requests.
- **`build.rs`**: Switch from `WrClientGenerator` to `WrServiceGenerator` (for the handler trait) + keep `WrClientGenerator` for outbound calls to collector/agent
- **`engine.toml`**: Set `mode = "worker"`
- **Coordinator changes**: The coordinator's `ClaimTask`/`UpdateTaskStatus` RPCs can be simplified — the engine handles job lifecycle. The coordinator still owns the external REST API for task creation, but submits jobs to the engine's `SubmitJob` endpoint instead of maintaining its own queue.

### 11. Documentation Updates

- `CLAUDE.md` — Add worker mode to architecture, add `WrWorkerClientGenerator` to codegen section
- `docs/agents/api_reference.md` — Add worker mode, `WrWorkerClientGenerator`, job submission helpers
- `docs/agents/module_template.md` — Add worker template (noting it's a service guest with `mode = "worker"`)
- `docs/agents/decision_matrix.md` — Add worker to decision matrix
- `docs/configuration.md` — Add worker config fields

## Files to Modify/Create

| File | Action |
|------|--------|
| `wr-build/src/lib.rs` | Modify — Add `WrWorkerClientGenerator` |
| `wr-sdk/src/jobs.rs` | **Create** — Job submission/status helpers |
| `wr-engine/src/config.rs` | Modify — Add `ModuleMode`, worker config fields |
| `wr-engine/src/worker.rs` | **Create** — Worker pool, job claiming, HTTP dispatch |
| `wr-engine/src/engine.rs` | Modify — Three-way dispatch in `spawn_module()` |
| `wr-engine/src/server.rs` | Modify — Add SubmitJob/GetJobStatus gRPC endpoints |
| `wr-engine/src/main.rs` | Modify — Job schema provisioning, skip routing for workers |
| `wr-engine/src/lib.rs` | Modify — Add `pub mod worker;` |
| `proto/wruntime.proto` | Modify — Add SubmitJob/GetJobStatus messages |
| `examples/codegen/worker/src/lib.rs` | Modify — Convert to `ServiceGuest` + `WrServiceGenerator` |
| `examples/codegen/worker/wit/world.wit` | Modify — Export `wasi:http/incoming-handler` |
| `examples/codegen/worker/build.rs` | Modify — Use `WrServiceGenerator` |
| `examples/codegen/engine.toml` | Modify — Set `mode = "worker"` |
| `examples/codegen/coordinator/src/lib.rs` | Modify — Submit jobs via engine instead of own queue |
| Docs (`CLAUDE.md`, `docs/agents/*`) | Modify — Document worker mode + codegen |

## Verification

1. `just tidy` — formatting and lints pass
2. `just test` — existing tests still pass
3. `just test-wasm` — no WIT changes needed, but verify existing tests pass
4. `just codegen-inline` — end-to-end validation with migrated worker example
5. Verify: coordinator creates a task → submits job via gRPC → engine claims job → dispatches as HTTP request to worker module → handler processes it → engine records result
