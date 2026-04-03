# Plan: Add Schedules

## Context

To replace the recurring-loop pattern that Run mode previously enabled, we add a Schedule feature. The manager owns schedule definitions, evaluates them on a timer, and submits jobs to engines via their existing `/wruntime.WorkerService/SubmitJob` HTTP/2 endpoint. Schedules are managed via gRPC (upsert, list, delete) and can be loaded from a standalone `schedules.toml` config file via the CLI.

---

## Design

- **Schedule definitions** live in a standalone config file (`schedules.toml`) and are sent to the manager via new gRPC endpoints (Upsert, List, Delete).
- **Manager owns evaluation**: a background task checks due schedules every N seconds and submits jobs to engines.
- **Manager submits jobs** by calling the engine's existing `/wruntime.WorkerService/SubmitJob` HTTP/2 endpoint (protobuf body). Requires adding an HTTP client (`hyper`) to the manager.
- **First fire** is configurable per schedule via an `immediate` boolean (default false).
- Jobs land in the existing `wr__jobs.jobs` table and are processed by the existing worker pool — no changes to job infrastructure.

## Schedule config format (`schedules.toml`)

```toml
[[schedule]]
worker_namespace = "codegen"
worker_name      = "worker"
worker_version   = "1.0.0"
job_type         = "/Cleanup/Run"
interval_secs    = 300
immediate        = false   # optional, default false
payload          = ""      # optional, UTF-8 string payload
timeout_secs     = 300     # optional, default 300
max_attempts     = 3       # optional, default 3
```

## Proto changes (`proto/wruntime.proto`)

Add new messages:

```protobuf
message Schedule {
  string schedule_id       = 1;  // UUID, assigned by manager on create
  string worker_namespace  = 2;
  string worker_name       = 3;
  string worker_version    = 4;
  string job_type          = 5;
  bytes  payload           = 6;
  uint64 interval_secs     = 7;
  bool   immediate         = 8;
  int32  timeout_secs      = 9;
  int32  max_attempts      = 10;
  bool   enabled           = 11;
}

message UpsertScheduleRequest  { Schedule schedule = 1; }
message UpsertScheduleResponse { string schedule_id = 1; }
message DeleteScheduleRequest  { string schedule_id = 1; }
message DeleteScheduleResponse {}
message ListSchedulesRequest   { string worker_namespace = 1; } // empty = all
message ListSchedulesResponse  { repeated Schedule schedules = 1; }
```

Add RPCs to `ManagerService`:
```protobuf
rpc UpsertSchedule (UpsertScheduleRequest) returns (UpsertScheduleResponse);
rpc DeleteSchedule (DeleteScheduleRequest) returns (DeleteScheduleResponse);
rpc ListSchedules  (ListSchedulesRequest)  returns (ListSchedulesResponse);
```

## Manager DB migration (`migrations/V4__schedules.sql`)

```sql
CREATE TABLE wr_schedules (
    schedule_id      TEXT PRIMARY KEY,
    worker_namespace TEXT NOT NULL,
    worker_name      TEXT NOT NULL,
    worker_version   TEXT NOT NULL,
    job_type         TEXT NOT NULL,
    payload          BYTEA NOT NULL DEFAULT '',
    interval_secs    BIGINT NOT NULL,
    immediate        BOOLEAN NOT NULL DEFAULT FALSE,
    timeout_secs     INT NOT NULL DEFAULT 300,
    max_attempts     INT NOT NULL DEFAULT 3,
    enabled          BOOLEAN NOT NULL DEFAULT TRUE,
    last_fired_at    TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

## Implementation steps

1. **`proto/wruntime.proto`** — Add Schedule messages and RPCs (above).

2. **`wr-manager/migrations/V4__schedules.sql`** — New migration file (above).

3. **`wr-manager/src/migrate.rs`** — Add `V4_SQL` constant and entry in `MIGRATIONS` array.

4. **`wr-manager/src/db.rs`** — Add functions:
   - `upsert_schedule(pool, schedule) -> Result<String>` — INSERT ... ON CONFLICT (schedule_id) DO UPDATE
   - `delete_schedule(pool, schedule_id) -> Result<()>`
   - `list_schedules(pool, namespace_filter) -> Result<Vec<Schedule>>`
   - `get_due_schedules(pool) -> Result<Vec<Schedule>>` — `WHERE enabled = true AND (last_fired_at IS NULL OR last_fired_at + interval_secs * interval '1 second' <= now())`
   - `mark_schedule_fired(pool, schedule_id) -> Result<()>` — `UPDATE ... SET last_fired_at = now()`

5. **`wr-manager/src/service.rs`** — Implement the three new RPCs: `upsert_schedule`, `delete_schedule`, `list_schedules`. Follow the existing secrets pattern.

6. **`wr-manager/src/scheduler.rs`** (new) — Background task:
   - Spawned in `main.rs` alongside `monitor_heartbeats`
   - Loop on configurable interval (default 10s):
     1. Call `db::get_due_schedules()`
     2. For each due schedule, resolve a healthy engine address from the routing table
     3. POST `SubmitJobRequest` protobuf to `http://{engine_address}/wruntime.WorkerService/SubmitJob` via hyper HTTP/2 client
     4. On success, call `db::mark_schedule_fired()`
     5. On failure, log warning and retry next tick
   - For schedules with `immediate = true` and `last_fired_at IS NULL`, fire immediately on first evaluation

7. **`wr-manager/src/config.rs`** — Add `scheduler_interval_secs` field (default 10).

8. **`wr-manager/src/main.rs`** — Spawn the scheduler background task.

9. **`wr-manager/Cargo.toml`** — Add `hyper`, `hyper-util`, `http-body-util`, `bytes` dependencies for the HTTP/2 client.

10. **`wr-cli/src/cmd/schedules.rs`** (new) — CLI subcommands:
    - `wr schedules upsert --file schedules.toml` — reads file, calls UpsertSchedule for each entry
    - `wr schedules list [--namespace X]` — calls ListSchedules
    - `wr schedules delete <schedule_id>` — calls DeleteSchedule
    - Follow the pattern in `wr-cli/src/cmd/secrets.rs`

11. **`wr-cli/src/cmd/mod.rs`** + **`wr-cli/src/main.rs`** — Register the `schedules` subcommand.

## Edge cases

- **Overlapping jobs**: If a scheduled job is still running when the next interval fires, a new job is submitted. Workers handle this naturally (jobs queue as `pending`).
- **No healthy engine**: If no engine hosts the target worker module, log warning and skip. Fires on next evaluation when an engine is available.
- **Multiple managers**: `get_due_schedules` + `mark_schedule_fired` use `FOR UPDATE SKIP LOCKED` so concurrent managers don't double-fire.

## Documentation updates

- **`CLAUDE.md`** — Add schedule config format and CLI commands
- **`README.md`** — Add schedules section showing config + CLI usage
- **`docs/configuration.md`** — Document `schedules.toml` format
- **`docs/agents/decision_matrix.md`** — Note that recurring tasks use Worker + Schedule

## Verification

- `just tidy` — compiles clean
- `just test` — all tests pass
- Manual test: `just dev-up`, start manager + engine with a worker module, `wr schedules upsert --file schedules.toml`, verify jobs appear in the DB on schedule
