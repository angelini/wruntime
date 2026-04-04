# Plan: Per-Module Database Isolation

## Goal

Each module `(namespace, name)` gets a fully isolated view of the single Postgres database. Module `foo.bar` cannot see tables owned by `foo.other`, even within the same namespace. Multiple instances of the same module (e.g., two engines both running `foo.bar`) share the same schema — isolation key is `(namespace, name)`, not version or instance.

---

## Core Mechanism: One Schema per Module + `search_path`

PostgreSQL schemas provide the right isolation primitive within a single database:

- One schema per `(namespace, name)` pair: `wr_{namespace}__{name}`
  - Double underscore separates namespace from name to avoid ambiguity
  - Non-alphanumeric characters in namespace/name are replaced with `_`
  - Example: module `inventory` in namespace `ecommerce` → schema `wr_ecommerce__inventory`
- Each module's connection pool sets `search_path = "wr_ecommerce__inventory", public` on every new connection
- Unqualified table references (`SELECT * FROM items`) resolve to the module's own schema
- Other modules' schemas are invisible by default

This approach keeps the implementation simple: no new Postgres users, no credential management, no changes to `db.wit`.

---

## Isolation Strength

`search_path` prevents **accidental** cross-module access. A WASM module can still write SQL that explicitly qualifies a foreign schema (`SELECT * FROM "wr_ecommerce__other".items`), because the connection role has read access to the whole database.

This is acceptable for the current threat model: operators deploy trusted WASM modules; the goal is preventing table name collisions and accidental data leakage, not defending against malicious module code.

If stronger isolation is needed later, the role-per-module approach described in the appendix can be layered on top with minimal changes to the host code.

---

## Implementation Phases

### Phase 1 — Foundation (no behavior change)

Pure, side-effect-free scaffolding. Safe to merge independently; nothing calls the new code yet.

- Add `module_schema()` helper (see §Changes/1)
- Add `db_schema: Option<String>` to `ModuleState` (see §Changes/4), populated from `module_schema()` when `database = true`

**Verification:** `cargo check` + unit test for `module_schema()` covering edge cases (hyphens, dots, mixed case).

---

### Phase 2 — Schema provisioning at startup

Run `CREATE SCHEMA IF NOT EXISTS` for each DB-enabled module before accepting traffic. Idempotent; safe to run on every restart.

- Implement engine startup sequence §Changes/7 steps (a) and (b)
- Admin pool borrows the existing `[database]` URL — no config changes

**Verification:** Start the engine against a real Postgres instance; inspect `\dn` to confirm schemas appear. Restart and confirm idempotency (no errors on second run).

---

### Phase 3 — Per-module pool + `search_path` enforcement

Actually enforce isolation. Two sub-steps that can land together:

1. Build a `HashMap<(namespace, name), Arc<Pool>>` at startup (§Changes/3); pass the correct pool into each `ModuleState`
2. Issue `SET search_path = "<schema>", public` at the top of `Host::query`, `Host::execute`, and `Host::begin_transaction` (§Changes/5)

**Verification:** Run the ecommerce example — `engine-inventory-1` and `engine-inventory-2` both resolve to `wr__ecommerce__inventory`; rows written by one instance are visible to the other.

---

### Phase 4 — Integration tests

Cover correctness and regression.

- Two in-process engines with distinct modules (`foo.bar`, `foo.other`), both using `WRUNTIME_TEST_DB_URL`
- Assert `foo.other` cannot see `foo.bar`'s tables (query returns error or empty result)
- Assert two instances of `foo.bar` share the same schema (cross-instance row visibility)

See §Changes/8 for full test spec.

---

## Changes Required

### 1. Schema naming helper (`wr-engine/src/pool.rs` or new `wr-engine/src/schema.rs`)

Add a pure function:

```rust
/// Returns the Postgres schema name for a module.
/// Format: `wr_{namespace}__{name}` with non-alphanumeric chars replaced by `_`.
pub fn module_schema(namespace: &str, name: &str) -> String {
    let sanitize = |s: &str| {
        s.chars()
            .map(|c| if c.is_alphanumeric() { c } else { '_' })
            .collect::<String>()
    };
    format!("wr__{}__{}", sanitize(namespace), sanitize(name))
}
```

### 2. Schema provisioning at engine startup (`wr-engine/src/main.rs` or `engine.rs`)

After the global pool is created, for each DB-enabled module, run once:

```sql
CREATE SCHEMA IF NOT EXISTS "wr__ecommerce__inventory";
```

This is idempotent — safe to run every startup. Uses a short-lived connection from the shared admin pool (the existing `[database]` URL, which must have `CREATE` privilege on the database).

### 3. Per-module pool with `search_path` (`wr-engine/src/pool.rs`)

Replace the single shared pool with a pool per `(namespace, name)` pair. Use deadpool-postgres's `post_create` hook to set `search_path` on every new connection:

```rust
pub fn build_module_pool(
    database_url: &str,
    max_size: usize,
    schema: &str,
) -> anyhow::Result<Pool> {
    let schema = schema.to_string();
    let mut cfg = Config::new();
    cfg.url = Some(database_url.to_string());
    cfg.pool = Some(PoolConfig { max_size, ..Default::default() });

    // deadpool-postgres ManagerConfig lets us hook into connection creation.
    // After the connection is established, set search_path so all unqualified
    // table references resolve to this module's schema.
    let mgr_config = deadpool_postgres::ManagerConfig {
        recycling_method: deadpool_postgres::RecyclingMethod::Fast,
    };
    // Use a custom manager or the `post_create` callback once deadpool exposes it.
    // As of deadpool-postgres 0.14 the cleanest path is a hook via
    // `ManagerConfig` + a custom `Manager` wrapper, or issuing SET on checkout.
    cfg.manager = Some(mgr_config);

    // Alternative that works today without custom manager:
    // Store `schema` in a wrapper and issue SET search_path inside db.rs
    // before every query (cheap — parsed by Postgres, not round-tripped).
    cfg.create_pool(Some(Runtime::Tokio1), tokio_postgres::NoTls)
        .map_err(Into::into)
}
```

**Practical implementation note:** deadpool-postgres does not expose a post-connect callback in all versions. The cleanest available option is to issue `SET search_path` at the start of the `query`/`execute`/`begin_transaction` host functions in `db.rs`, using the schema stored in `ModuleState`. This is a single cheap round-trip statement that Postgres executes without disk I/O — acceptable overhead.

### 4. `ModuleState` carries the schema name (`wr-engine/src/state.rs`)

Add `db_schema: Option<String>` to `ModuleState`. Populated from `module_schema(namespace, name)` when `database = true`.

```rust
pub struct ModuleState {
    // ... existing fields ...
    pub db_schema: Option<String>,
}
```

### 5. `db.rs` host functions set `search_path` on each connection

At the top of `Host::query`, `Host::execute`, and `Host::begin_transaction`, after acquiring the connection from the pool:

```rust
if let Some(schema) = &self.db_schema {
    client
        .execute(
            &format!("SET search_path = \"{schema}\", public"),
            &[],
        )
        .await
        .map_err(|e| DbError::Connection(e.to_string()))?;
}
```

For transactions, issue it once after `BEGIN`.

### 6. Config (`wr-engine/src/config.rs`)

No changes to the config file format. The schema name is derived automatically from `module.namespace` and `module.name`. The existing `[database]` section provides the admin connection URL used for both provisioning and module pools (same credentials — isolation is enforced by `search_path`, not by Postgres roles).

### 7. Engine startup sequence (`wr-engine/src/main.rs`)

```
1. Parse config
2. If [database] configured:
   a. Build admin pool (existing logic)
   b. For each module with database = true:
      i.  Compute schema name via module_schema()
      ii. Run: CREATE SCHEMA IF NOT EXISTS "<schema>"
3. For each module with database = true:
   - Build a per-module pool (same URL, same creds, just tagged with schema)
   - Store in a HashMap<(namespace, name), Arc<Pool>>
4. Start module instances, passing the correct pool + schema name into ModuleState
```

### 8. Tests (`wr-tests/tests/integration_test.rs`)

- Two in-process engines, each with a different module (`foo.bar`, `foo.other`)
- Both use the same `WRUNTIME_TEST_DB_URL`
- `foo.bar` creates and populates a table `items`
- Verify `foo.other` cannot see `foo.bar`'s `items` (query returns error or empty)
- Verify two instances of `foo.bar` share the same schema (row written by instance 1 is visible to instance 2)

---

## Ecommerce Example

The `inventory` module in `ecommerce-example` uses `engine-inventory-1.toml` and `engine-inventory-2.toml`. Both run the same module (`inventory.ecommerce`), so both map to schema `wr__ecommerce__inventory`. Tables created by one instance are immediately visible to the other — correct behaviour for load-balanced replicas.

---

## Appendix: Role-per-Module (Strong Isolation)

If the threat model requires preventing malicious WASM modules from escaping their schema via explicit qualification:

1. Create a Postgres role per module: `CREATE ROLE "wr__ecommerce__inventory" LOGIN PASSWORD '...'`
2. Grant it only: `GRANT USAGE, CREATE ON SCHEMA "wr__ecommerce__inventory" TO "wr__ecommerce__inventory"`
3. `REVOKE ALL ON SCHEMA public FROM "wr__ecommerce__inventory"` (optional)
4. Each module's pool connects **as that role** using a generated or configured password
5. Attempting to read `"wr__ecommerce__other".items` fails with a permission error at the Postgres level

This requires storing or generating per-module passwords and a more complex provisioning step. It can be added later as an opt-in config flag (`isolation = "strict"`) without changing `db.wit` or the WASM module side.
