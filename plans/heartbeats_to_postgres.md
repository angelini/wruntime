# Plan: Move Engine Heartbeats from Chitchat to Postgres

## Context

Chitchat gossip currently carries `hb/{engine_id}` keys for engine heartbeats. With 3s heartbeat intervals and a 10s timeout, the gossip protocol's convergence time limits the system to ~100-300 engines depending on manager count. Moving heartbeats to Postgres eliminates convergence pressure entirely — all managers see the same heartbeat state immediately via shared DB. Chitchat stays for its original purpose: phi-accrual failure detection between managers.

After this change, chitchat carries zero application keys.

---

## Step 1: Migration — add `last_heartbeat` to `wr_engines`

**New file:** `wr-manager/migrations/V4__engine_heartbeats.sql`
```sql
ALTER TABLE wr_engines ADD COLUMN last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW();
```

**Edit:** `wr-manager/src/migrate.rs`
- Add `const V4_SQL: &str = include_str!("../migrations/V4__engine_heartbeats.sql");`
- Extend `MIGRATIONS` to include `(4, V4_SQL)`

---

## Step 2: New DB functions

**Edit:** `wr-manager/src/db.rs`

Add `heartbeat_engine` — single-row UPDATE on primary key:
```rust
pub async fn heartbeat_engine(pool: &Pool, engine_id: &str) -> Result<(), Status>
// UPDATE wr_engines SET last_heartbeat = NOW() WHERE engine_id = $1
```

Add `get_stale_engine_rules` — returns rule IDs for engines past timeout:
```rust
pub async fn get_stale_engine_rules(pool: &Pool, timeout_secs: f64) -> Result<Vec<String>, Status>
// SELECT r.rule_id FROM wr_routing_rules r
// JOIN wr_engines e ON r.engine_id = e.engine_id
// WHERE e.last_heartbeat < NOW() - make_interval(secs => $1) AND r.healthy = TRUE
```

Add `get_healthy_engine_rules` — returns rule IDs for engines that have recovered:
```rust
pub async fn get_recovered_engine_rules(pool: &Pool, timeout_secs: f64) -> Result<Vec<String>, Status>
// SELECT r.rule_id FROM wr_routing_rules r
// JOIN wr_engines e ON r.engine_id = e.engine_id
// WHERE e.last_heartbeat >= NOW() - make_interval(secs => $1) AND r.healthy = FALSE
```

Also update `upsert_engine_and_schemas` — add `last_heartbeat = NOW()` to the `ON CONFLICT DO UPDATE SET` clause so re-registration refreshes the heartbeat.

---

## Step 3: Strip application methods from `ClusterHandle`

**Edit:** `wr-manager/src/cluster.rs`

Remove:
- `set_engine_heartbeat()`
- `remove_engine()`
- `get_all_heartbeats()`
- `cleanup_stale_heartbeats()`
- `now_millis()` helper
- Unused imports: `HashMap`, `SystemTime`, `UNIX_EPOCH`

Keep:
- `ClusterHandle` struct with `handle: ChitchatHandle`
- `new()` — bootstraps chitchat for manager liveness
- `shutdown()` — graceful teardown

Update doc comment to reflect chitchat is now manager-liveness only.

---

## Step 4: Rewrite `monitor_heartbeats`

**Edit:** `wr-manager/src/state.rs`

Remove:
- `ManagerState` struct, `SharedState` type alias, `new_state()`
- Both gossip and local-only branches

New simplified function:
```rust
pub async fn monitor_heartbeats(pool: Pool, timeout_secs: u64, interval: Duration) {
    loop {
        tick.tick().await;
        // Mark stale engines unhealthy
        let stale = db::get_stale_engine_rules(&pool, timeout_secs as f64).await;
        for rule_id in stale { db::set_rule_health(&pool, &rule_id, false).await; }
        // Recover engines that are heartbeating again
        let recovered = db::get_recovered_engine_rules(&pool, timeout_secs as f64).await;
        for rule_id in recovered { db::set_rule_health(&pool, &rule_id, true).await; }
    }
}
```

Single code path — no gossip branch, no local-only branch. All paths use Postgres.

---

## Step 5: Simplify `Manager` service

**Edit:** `wr-manager/src/service.rs`

Struct changes:
- Remove `state: SharedState` field
- Remove `cluster: Option<Arc<ClusterHandle>>` field
- Remove `with_cluster()` method
- New constructor: `pub fn new(pool: Pool, crypto: Arc<SecretCrypto>) -> Self`

`heartbeat()` — replace body with:
```rust
let engine_id = request.into_inner().engine_id;
db::heartbeat_engine(&self.pool, &engine_id).await?;
Ok(Response::new(HeartbeatResponse {}))
```

`register_engine()` — remove:
- Lines 86-99 (in-memory state updates)
- Lines 102-104 (chitchat propagation)
- DB insert already sets `last_heartbeat = NOW()` via column default

`deregister_engine()` — remove:
- Lines 170-175 (in-memory state cleanup)
- Lines 178-180 (chitchat removal)
- DB already handles everything

---

## Step 6: Update `main.rs` wiring

**Edit:** `wr-manager/src/main.rs`

- Remove `let shared = state::new_state();`
- Change `Manager::new(shared.clone(), db_pool.clone(), crypto)` → `Manager::new(db_pool.clone(), crypto)`
- Remove `.with_cluster(cluster_handle.clone())`
- Change monitor spawn to `state::monitor_heartbeats(db_pool.clone(), config.engine_heartbeat_timeout_secs, Duration::from_secs(5))`
- Keep chitchat bootstrap (lines 71-94) — still needed for manager liveness
- Keep `cluster_handle` creation — still held for graceful shutdown

---

## Step 7: Update `lib.rs` exports

**Edit:** `wr-manager/src/lib.rs`

No module removals needed — `state` module still exists (exports `monitor_heartbeats`). But `SharedState`, `new_state` are no longer exported.

---

## Step 8: Update test helpers

**Edit:** `wr-tests/tests/helpers.rs`

Add helper:
```rust
pub async fn backdate_engine_heartbeat(pool: &deadpool_postgres::Pool, engine_id: &str, secs_ago: i64) {
    let client = pool.get().await.unwrap();
    client.execute(
        "UPDATE wr_engines SET last_heartbeat = NOW() - make_interval(secs => $1::double precision) WHERE engine_id = $2",
        &[&(secs_ago as f64), &engine_id],
    ).await.unwrap();
}
```

`start_manager_with_monitor()`:
- Remove `let state = new_state();`
- Constructor: `Manager::new(pool.clone(), crypto)`
- Monitor: `monitor_heartbeats(pool, timeout_secs, Duration::from_millis(200))`
- Return `Result<String>` (just the address, no `SharedState`)

`ClusteredManager` struct:
- Remove `state` field
- Remove `cluster` field (no longer needed for test assertions)
- Keep only `addr: String`

`start_manager_cluster()`:
- Remove `let state = new_state();` per manager
- Constructor: `Manager::new(pool.clone(), crypto)` (no `.with_cluster()`)
- Monitor: `monitor_heartbeats(pool.clone(), heartbeat_timeout_secs, Duration::from_millis(200))`
- Push `ClusteredManager { addr: grpc_url }`
- Keep chitchat bootstrap (manager liveness still needed)

Remove `use wr_manager::state::new_state` import.

---

## Step 9: Update health tests

**Edit:** `wr-tests/tests/health_test.rs`

All 6 tests replace in-memory state manipulation with DB backdating:

| Test | Change |
|------|--------|
| `test_heartbeat_timeout_marks_module_unhealthy` | Replace `state.module_health` backdate → `backdate_engine_heartbeat(&pool, "hc-e1", 60)` |
| `test_heartbeat_keeps_module_healthy` | Remove `_state` from return. No other changes. |
| `test_heartbeat_missing_module_becomes_unhealthy` | This test's semantics change — per-module health no longer exists. Rewrite: backdate engine heartbeat, send heartbeat (refreshes timestamp), verify stays healthy. Or remove if redundant with test 1. |
| `test_module_health_recovery_after_heartbeat` | Backdate via DB, then send heartbeat (triggers `db::heartbeat_engine`), wait for monitor, verify recovery. |
| `test_unhealthy_module_excluded_from_routing` | Backdate via DB instead of state manipulation. |
| `test_health_change_bumps_routing_table_version` | Backdate via DB instead of state manipulation. |

All tests: `let (mgr_addr, state) =` becomes `let mgr_addr =`, add `pool.clone()` where needed.

---

## Step 10: Update multi-manager tests

**Edit:** `wr-tests/tests/multi_manager_test.rs`

| Test | Change |
|------|--------|
| `test_heartbeat_gossip_across_managers` | Rewrite: heartbeat to manager-1, verify rule healthy via manager-2's routing table (DB is shared). Remove `managers[1].cluster.get_all_heartbeats()` assertion. Remove 2s gossip wait. |
| `test_health_preserved_across_managers` | Remove 2s gossip wait (DB is immediate). Reduce to 500ms for monitor tick. |
| `test_health_convergence_on_missed_heartbeat` | Remove 500ms gossip wait. Wait only for timeout + monitor tick. |
| `test_single_manager_cluster` | Adjust `ClusteredManager` field access. |
| `test_manager_self_registration` | No changes — only queries `wr_managers`. |

Update module comment at top — tests no longer verify chitchat gossip, they verify DB-based health monitoring across multiple managers.

---

## Verification

```bash
just tidy              # fmt + clippy
just test              # all tests including health + multi-manager
just ecommerce-inline  # e2e, zero warnings
```
