# Multi-Manager Implementation Plan

Persist `wr-manager` state to Postgres so multiple manager instances can run active-active behind a load balancer.

## Design Principles

- All persisted reads/writes go directly to Postgres — no write-through cache, ensuring multi-manager consistency
- Ephemeral state (heartbeats, `module_health`) stays in-memory, rebuilt on startup
- Concurrent write protection via `SELECT ... FOR UPDATE NOWAIT` on a lock sentinel row; lock failure → `Status::aborted` (no retry)

---

## Phase 1: Database Schema & Migrations

### 1.1 Create migration file

**File:** `wr-manager/migrations/V1__initial.sql`

```sql
-- Lock sentinel: single row holds the authoritative routing table version
CREATE TABLE IF NOT EXISTS wr_manager_lock (
    id      INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    version BIGINT NOT NULL DEFAULT 0
);
INSERT INTO wr_manager_lock VALUES (1, 0) ON CONFLICT DO NOTHING;

-- Engines: full EngineRegistration serialised as protobuf BYTEA
CREATE TABLE IF NOT EXISTS wr_engines (
    engine_id     TEXT PRIMARY KEY,
    address       TEXT NOT NULL,
    proxy_address TEXT NOT NULL,
    registration  BYTEA NOT NULL,       -- prost-encoded EngineRegistration
    registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Routing rules: columns (not blob) so UPDATE SET healthy = false WHERE engine_id works
CREATE TABLE IF NOT EXISTS wr_routing_rules (
    rule_id               TEXT PRIMARY KEY,
    source_namespace      TEXT NOT NULL DEFAULT '',
    source_module         TEXT NOT NULL DEFAULT '',
    destination_namespace TEXT NOT NULL DEFAULT '',
    destination_module    TEXT NOT NULL DEFAULT '',
    destination_version   TEXT NOT NULL DEFAULT '',
    engine_id             TEXT NOT NULL,
    engine_address        TEXT NOT NULL DEFAULT '',
    proxy_address         TEXT NOT NULL DEFAULT '',
    healthy               BOOL NOT NULL DEFAULT TRUE,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_routing_rules_engine ON wr_routing_rules (engine_id);

-- Schemas: keyed by (namespace, module, version)
CREATE TABLE IF NOT EXISTS wr_schemas (
    namespace    TEXT NOT NULL,
    module_name  TEXT NOT NULL,
    version      TEXT NOT NULL,
    proto_schema BYTEA NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (namespace, module_name, version)
);

```

### 1.2 Create embedded migration runner

**File:** `wr-manager/src/migrate.rs`

- Define `MIGRATIONS: &[(i32, &str)]` using `include_str!` to embed the SQL file
- `run_migrations(client)` — iterate the slice, execute each statement, track applied versions

---

## Phase 2: Dependencies & Configuration

### 2.1 Add Cargo dependencies

**File:** `wr-manager/Cargo.toml`

```toml
deadpool-postgres = { version = "0.14", features = ["rt_tokio_1"] }
tokio-postgres    = { version = "0.7",  features = ["with-chrono-0_4"] }
```

### 2.2 Add config structs

**File:** `wr-manager/src/config.rs`

```rust
pub struct DatabaseConfig {
    pub url: String,
    pub max_connections: usize,   // default 10
}
```

Add to `ManagerConfig`:
```rust
pub database: DatabaseConfig,        // required
```

### 2.3 Update example config

**File:** `examples/config/manager.toml`

```toml
[database]
url             = "postgres://postgres@localhost:5433/wruntime_example"
max_connections = 10
```

---

## Phase 3: Connection Pool

### 3.1 Create pool builder

**File:** `wr-manager/src/pool.rs`

- `build_pool(url, max_size) -> Pool` — mirrors the existing `wr-engine/src/pool.rs` pattern

---

## Phase 4: Database Operations

### 4.1 Implement DB module

**File:** `wr-manager/src/db.rs`

#### Engine operations
```rust
pub async fn upsert_engine_and_schemas(pool: &Pool, reg: &EngineRegistration) -> Result<(), Status>
pub async fn deregister_engine(pool: &Pool, engine_id: &str) -> Result<(), Status>
pub async fn list_engines(pool: &Pool) -> Result<Vec<EngineRegistration>, Status>
```

#### Routing operations
```rust
pub async fn upsert_routing_rule(pool: &Pool, rule: &RoutingRule) -> Result<(), Status>
pub async fn delete_routing_rule(pool: &Pool, rule_id: &str) -> Result<bool, Status>
pub async fn get_routing_table(pool: &Pool) -> Result<RoutingTable, Status>
pub async fn mark_rules_unhealthy_for_engine(pool: &Pool, engine_id: &str) -> Result<(), Status>
```

#### Schema operations
```rust
pub async fn upsert_schema(pool: &Pool, ns: &str, module: &str, ver: &str, data: &[u8]) -> Result<(), Status>
pub async fn get_schema(pool: &Pool, ns: &str, module: &str, ver: &str) -> Result<Vec<u8>, Status>
```

#### Private helpers
```rust
async fn acquire_global_lock(txn: &Transaction<'_>) -> Result<u64, Status>
async fn increment_version(txn: &Transaction<'_>) -> Result<u64, Status>
fn map_lock_err(e: tokio_postgres::Error) -> Status  // LOCK_NOT_AVAILABLE → Status::aborted
```

---

## Phase 5: Locking Strategy

All version-bumping operations (routing rule mutations, `DeregisterEngine`, health monitor writes) use this transaction pattern:

```sql
BEGIN;
SELECT version FROM wr_manager_lock WHERE id = 1 FOR UPDATE NOWAIT;
-- LOCK_NOT_AVAILABLE → ROLLBACK, return Status::aborted("concurrent write conflict")
... mutate routing rules ...
UPDATE wr_manager_lock SET version = version + 1 WHERE id = 1;
COMMIT;
```

- **`RegisterEngine`** locks its own engine row (`SELECT engine_id ... FOR UPDATE NOWAIT`) but does **not** bump the routing table version (schemas and engine registration don't affect routing)
- **`DeregisterEngine`** acquires the global lock, marks all routing rules for the engine as unhealthy (UPDATE, not DELETE — matches current in-memory behavior), removes the engine from `wr_engines`, cleans up heartbeat/module_health in-memory state, and bumps the routing table version
- **`monitor_heartbeats`** uses `NOWAIT` too — on contention it logs a warning and skips, retrying on the next 10-second tick

---

## Phase 6: State & Service Refactor

### 6.1 Slim down `ManagerState`

**File:** `wr-manager/src/state.rs`

Remove persisted fields (`engines`, `routing_table`, `schemas`). Retain only ephemeral state:

```rust
pub struct ManagerState {
    pub heartbeats:    HashMap<String, Instant>,
    pub module_health: HashMap<(String, String, String, String), Instant>,
}
```

Update `monitor_heartbeats` signature to accept `pool: Pool`.

### 6.2 Update `Manager` struct

**File:** `wr-manager/src/service.rs`

```rust
pub struct Manager {
    state: SharedState,   // only ephemeral maps
    pool:  Pool,          // deadpool-postgres
}
```

Replace all in-memory read/write operations with `db::*` calls.

---

## Phase 7: Startup Sequence

**File:** `wr-manager/src/main.rs`

1. Load `ManagerConfig`
2. `build_pool(&config.database.url, max_connections)`
3. `run_migrations(&pool.get().await?.client())`
4. `Manager::new(shared_state, pool)`
5. Spawn `monitor_heartbeats(state, pool, timeout_secs)`
6. Serve gRPC

---

## Phase 8: Integration Tests

**File:** `wr-tests/tests/helpers.rs`

Two helpers need updating:
- `start_manager()` — simple startup, gains a `pool` parameter gated on `WRUNTIME_TEST_DB_URL`
- `start_manager_with_monitor(timeout_secs)` — starts manager + heartbeat monitor, returns `(addr, SharedState)`; gains a `pool` parameter passed through to both `Manager::new` and `monitor_heartbeats`

Same pattern as engine DB tests: skip if env var absent, run migrations if set

---

## Multi-Manager Behavior Summary

| Operation | Behavior |
|---|---|
| **Reads** (`GetRoutingTable`, `GetSchema`, `ListEngines`) | Go straight to Postgres — all managers see the same view |
| **Writes** | Compete on `wr_manager_lock` — one wins, losers get `Status::aborted` (engine retries on next cycle) |
| **Heartbeats** | Remain per-instance; engines should send heartbeats to all manager addresses for active-active |
| **Routing table version** | Single authoritative counter in `wr_manager_lock.version` — proxies polling any manager see a monotonically increasing sequence |

---

## File Change Summary

| File | Change |
|---|---|
| `wr-manager/Cargo.toml` | Add `deadpool-postgres`, `tokio-postgres` |
| `wr-manager/src/config.rs` | Add `DatabaseConfig` |
| `wr-manager/src/state.rs` | Remove persisted fields; add `pool` to `monitor_heartbeats` signature |
| `wr-manager/src/service.rs` | Add `pool: Pool`; replace in-memory ops with `db::*` calls |
| `wr-manager/src/main.rs` | Pool init, migrations |
| `wr-manager/src/pool.rs` | **New** — `build_pool` |
| `wr-manager/src/migrate.rs` | **New** — embedded migration runner |
| `wr-manager/src/db.rs` | **New** — all DB operations |
| `wr-manager/migrations/V1__initial.sql` | **New** — DDL |
| `examples/config/manager.toml` | Add `[database]` section |
| `wr-tests/tests/helpers.rs` | Update `start_manager()` for pool |
