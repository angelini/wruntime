# Plan: Add Schedules

## Context

Replace the recurring-loop pattern with a Schedule feature. The manager owns schedule definitions, evaluates them on a background timer, and submits jobs to engines via their existing `/wruntime.WorkerService/SubmitJob` HTTP/2 endpoint (raw protobuf, not tonic gRPC). Schedules are managed via gRPC RPCs and a CLI subcommand, with optional deployment integration.

**Multi-manager safety:** Each manager runs its own scheduler loop. `FOR UPDATE SKIP LOCKED` prevents double-firing — concurrent managers claim disjoint sets of due schedules.

---

## Design

- **Schedule definitions** live in a standalone config file (`schedules.toml`) and are sent to the manager via gRPC RPCs (Upsert, List, Delete).
- **Manager owns evaluation**: a background task checks due schedules every 10 seconds and submits jobs to engines.
- **Manager submits jobs** by calling the engine's existing `/wruntime.WorkerService/SubmitJob` HTTP/2 endpoint (protobuf body). Uses a hyper HTTP/2 client.
- **First fire** is configurable per schedule via an `immediate` boolean (default false).
- Jobs land in the existing `wr__jobs.jobs` table and are processed by the existing worker pool.
- **Natural key** `(worker_namespace, worker_name, worker_version, job_type)` makes re-applying the same config idempotent.

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

Messages:
- `Schedule` — full schedule representation with all fields
- `UpsertScheduleRequest` — fields for creating/updating (uses natural key)
- `UpsertScheduleResponse` — returns `schedule_id`
- `DeleteScheduleRequest` — identifies by natural key
- `ListSchedulesRequest` — optional `worker_namespace` filter
- `ListSchedulesResponse` — list of `Schedule` messages

RPCs added to `ManagerService`:
- `UpsertSchedule`
- `DeleteSchedule`
- `ListSchedules`

## Manager DB migration (`V5__schedules.sql`)

```sql
CREATE TABLE IF NOT EXISTS wr_schedules (
    schedule_id       TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    worker_namespace  TEXT NOT NULL,
    worker_name       TEXT NOT NULL,
    worker_version    TEXT NOT NULL,
    job_type          TEXT NOT NULL,
    interval_secs     INT NOT NULL CHECK (interval_secs > 0),
    immediate         BOOL NOT NULL DEFAULT FALSE,
    payload           BYTEA NOT NULL DEFAULT ''::bytea,
    timeout_secs      INT NOT NULL DEFAULT 300,
    max_attempts      INT NOT NULL DEFAULT 3,
    enabled           BOOL NOT NULL DEFAULT TRUE,
    last_fired_at     TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (worker_namespace, worker_name, worker_version, job_type)
);
```

## Implementation

### DB functions (`wr-manager/src/db.rs`)

- `upsert_schedule` — INSERT ON CONFLICT DO UPDATE, RETURNING schedule_id
- `delete_schedule` — DELETE by natural key
- `list_schedules` — SELECT with optional namespace filter
- `claim_due_schedules(txn)` — SELECT ... FOR UPDATE SKIP LOCKED (multi-manager safe)
- `mark_schedule_fired(txn, id)` — UPDATE last_fired_at = NOW()
- `resolve_engine_for_worker` — find healthy engine from routing table

### Scheduler background task (`wr-manager/src/scheduler.rs`)

Loop every 10s:
1. Begin transaction
2. `claim_due_schedules(&txn)` — locks due rows with SKIP LOCKED
3. For each: resolve engine address, POST SubmitJobRequest via hyper HTTP/2
4. On success: `mark_schedule_fired(&txn, id)`
5. On failure: warn + skip (retries next tick)
6. Commit transaction

### CLI (`wr-cli/src/cmd/schedules.rs`)

- `wr schedules apply --file schedules.toml` — reads TOML, upserts each entry
- `wr schedules list [--namespace X]` — table display
- `wr schedules delete --namespace X --name Y --version Z --job-type T`

### Deployment integration

- `schedules_path` field in `wr-deploy.toml` (optional)
- `wr node deploy` applies schedules automatically after engine registration when configured

## Multi-manager behavior

- Each manager spawns its own scheduler loop (10s interval)
- `claim_due_schedules` uses `FOR UPDATE SKIP LOCKED` — only one manager fires each due schedule
- If a manager crashes mid-transaction, the row lock is released and another manager picks it up next tick
- No leader election needed — lock-free coordination via Postgres row locks

## Edge cases

- **Overlapping jobs**: If a scheduled job is still running when the next interval fires, a new job is submitted. Workers handle this naturally (jobs queue as `pending`).
- **No healthy engine**: If no engine hosts the target worker module, log warning and skip. Fires on next evaluation when an engine is available.
- **Immediate flag**: Schedules with `immediate = true` and `last_fired_at IS NULL` fire on first evaluation.

## Verification

- `just tidy` — compiles clean
- `just test` — all tests pass
- Manual test: `just dev-up`, start manager + engine with a worker module, `wr schedules apply --file schedules.toml`, verify jobs appear on schedule
