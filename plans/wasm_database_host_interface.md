# Plan: Host-provided database interface for WASM modules

## Goal

Expose PostgreSQL access to WASM modules through a WIT-defined host interface
implemented inside `wr-engine`. The WASM module never holds a database connection
— it calls imported functions that the host resolves against a shared connection
pool. The sandbox boundary is preserved; the module sees typed rows, not raw TCP.

---

## Architecture overview

```
WASM module
  │  calls wruntime:db/database.query(sql, params)
  ▼
wasmtime Linker  (host implementation in wr-engine)
  │  acquires a pooled connection
  ▼
deadpool-postgres → tokio-postgres → PostgreSQL
```

The WIT interface is defined once in `wit/db.wit`. `wr-engine` compiles host
bindings from it via `wasmtime::component::bindgen!`. Each module instance
receives an `Option<Arc<Pool>>` — absent when no database is configured for
that module, present otherwise.

---

## Phase 1 — WIT interface definition

**New file:** `wit/db.wit`

```wit
package wruntime:db@0.1.0;

interface database {
    /// A single column value — None represents SQL NULL.
    record column {
        name:  string,
        value: option<string>,
    }

    /// One result row returned by a query.
    record row {
        columns: list<column>,
    }

    variant db-error {
        /// Could not acquire or use a connection.
        connection(string),
        /// The database rejected the query.
        query(string),
    }

    /// Execute a parameterised SELECT and return all matching rows.
    /// Parameters are bound positionally as $1, $2, … in the SQL string.
    query: func(
        sql:    string,
        params: list<string>,
    ) -> result<list<row>, db-error>;

    /// Execute a parameterised INSERT / UPDATE / DELETE.
    /// Returns the number of rows affected.
    execute: func(
        sql:    string,
        params: list<string>,
    ) -> result<u64, db-error>;
}

/// The world the host exposes to every WASM module that opts in to DB access.
world db-access {
    import database;
}
```

The `string`-typed parameters keep the interface simple and avoid the need for
a full SQL type system in WIT. The host converts each `&str` to a
`tokio_postgres` parameter using the `text` wire format.

---

## Phase 2 — Dependencies

Add to `wr-engine/Cargo.toml`:

```toml
tokio-postgres  = { version = "0.7", features = ["with-uuid-1"] }
deadpool-postgres = { version = "0.14", features = ["rt_tokio_1"] }
```

`deadpool-postgres` sits on top of `tokio-postgres` and provides an
async-aware pool that integrates directly with Tokio. No additional executor
glue is required.

---

## Phase 3 — Host bindings

**New file:** `wr-engine/src/db.rs`

Use `wasmtime::component::bindgen!` to generate host-side types and the trait
the engine must implement:

```rust
wasmtime::component::bindgen!({
    path:  "wit/db.wit",
    world: "db-access",
    async: true,
});
```

This emits:
- `wruntime::db::database::{Column, Row, DbError}` — the WIT record/variant types.
- `wruntime::db::database::HostDatabase` — an async trait with `query` and `execute`.
- `wruntime::db::database::add_to_linker` — registers the implementation with a
  `wasmtime::component::Linker<T>`.

Implement `HostDatabase` for `ModuleState`:

```rust
#[async_trait::async_trait]
impl wruntime::db::database::HostDatabase for ModuleState {
    async fn query(
        &mut self,
        sql: String,
        params: Vec<String>,
    ) -> Result<Vec<Row>, DbError> {
        let pool = self.db_pool.as_ref()
            .ok_or_else(|| DbError::Connection("no database configured for this module".into()))?;

        let client = pool.get().await
            .map_err(|e| DbError::Connection(e.to_string()))?;

        let params_ref: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            params.iter().map(|s| s as _).collect();

        let rows = client.query(&sql, &params_ref).await
            .map_err(|e| DbError::Query(e.to_string()))?;

        Ok(rows.iter().map(pg_row_to_wit).collect())
    }

    async fn execute(
        &mut self,
        sql: String,
        params: Vec<String>,
    ) -> Result<u64, DbError> {
        let pool = self.db_pool.as_ref()
            .ok_or_else(|| DbError::Connection("no database configured for this module".into()))?;

        let client = pool.get().await
            .map_err(|e| DbError::Connection(e.to_string()))?;

        let params_ref: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            params.iter().map(|s| s as _).collect();

        let n = client.execute(&sql, &params_ref).await
            .map_err(|e| DbError::Query(e.to_string()))?;

        Ok(n)
    }
}

fn pg_row_to_wit(row: &tokio_postgres::Row) -> Row {
    let columns = row.columns().iter().enumerate().map(|(i, col)| {
        Column {
            name:  col.name().to_string(),
            value: row.get::<_, Option<String>>(i),
        }
    }).collect();
    Row { columns }
}
```

---

## Phase 4 — Connection pool

**New file:** `wr-engine/src/pool.rs`

Build and return a `deadpool_postgres::Pool` from a connection string. The pool
is created once at engine startup and shared (via `Arc`) across all instances of
modules that have database access enabled.

```rust
use deadpool_postgres::{Config, Pool, Runtime};

pub fn build_pool(database_url: &str, max_size: usize) -> anyhow::Result<Pool> {
    let mut cfg = Config::new();
    cfg.url     = Some(database_url.to_string());
    cfg.pool    = Some(deadpool_postgres::PoolConfig { max_size, ..Default::default() });
    cfg.create_pool(Some(Runtime::Tokio1), tokio_postgres::NoTls)
        .map_err(Into::into)
}
```

`Arc<Pool>` is then stored on `ModuleState` so each per-request store gets a
cheap handle to the shared pool without cloning connections.

---

## Phase 5 — Config changes

### `EngineConfig` — `wr-engine/src/config.rs`

Add an optional top-level `[database]` table and a per-module `database` flag:

```toml
# engine.toml

[database]
url          = "postgres://user:pass@localhost:5432/mydb"
max_connections = 10          # default: 8

[[module]]
name     = "order-service"
version  = "1.0.0"
wasm_path = "modules/order_service.wasm"
database = true               # opt in to DB access
```

Corresponding Rust additions:

```rust
#[derive(Deserialize, Clone)]
pub struct DatabaseConfig {
    pub url:             String,
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
}

fn default_max_connections() -> usize { 8 }

// Added to EngineConfig:
pub database: Option<DatabaseConfig>,

// Added to ModuleConfig:
#[serde(default)]
pub database: bool,
```

Validation should reject `module.database = true` when no `[database]` section
is present.

---

## Phase 6 — Wire up in `wr-engine`

### `wr-engine/src/engine.rs`

Three changes:

**1. Build the pool once in `EngineRunner::new`:**

```rust
pub struct EngineRunner {
    engine:  Arc<Engine>,
    config:  EngineConfig,
    db_pool: Option<Arc<deadpool_postgres::Pool>>,
}

impl EngineRunner {
    pub fn new(config: EngineConfig) -> Result<Self> {
        let db_pool = config.database.as_ref()
            .map(|db| pool::build_pool(&db.url, db.max_connections))
            .transpose()?
            .map(Arc::new);
        // … existing engine setup …
        Ok(Self { engine: Arc::new(engine), config, db_pool })
    }
}
```

**2. Register the DB host implementation with the linker in `spawn_module`:**

```rust
// After existing add_to_linker_async calls:
wruntime::db::database::add_to_linker(&mut linker, |s: &mut ModuleState| s)?;
```

**3. Pass the pool to `ModuleState::new`:**

```rust
let db_pool = if module_config.database { self.db_pool.clone() } else { None };
let state   = ModuleState::new(module_name.clone(), proxy_uri, db_pool);
```

### `wr-engine/src/state.rs`

Add `db_pool` to `ModuleState`:

```rust
pub struct ModuleState {
    wasi:        WasiCtx,
    http:        WasiHttpCtx,
    table:       ResourceTable,
    module_name: String,
    proxy_uri:   hyper::Uri,
    pub db_pool: Option<Arc<deadpool_postgres::Pool>>,
}
```

---

## Phase 7 — Guest-side usage (reference)

A WASM module written in Rust uses `cargo-component` with the same `wit/db.wit`
to generate guest bindings:

```rust
// In the WASM module crate (not wr-engine):
wit_bindgen::generate!({ path: "../../wit/db.wit", world: "db-access" });

use wruntime::db::database;

fn get_order(id: &str) -> Result<Option<String>, database::DbError> {
    let rows = database::query(
        "SELECT status FROM orders WHERE id = $1",
        &[id],
    )?;
    Ok(rows.first().and_then(|r| r.columns.first()?.value.clone()))
}
```

The `wit/` directory at the repo root acts as the single source of truth shared
between the host (`wr-engine`) and any guest module crates.

---

## Phase 8 — Integration test

Add to `wr-tests/tests/integration_test.rs`.

Because Postgres is an external dependency, the test is gated behind a
`WRUNTIME_TEST_DB_URL` environment variable and skipped when absent:

```rust
#[tokio::test]
async fn test_database_query_from_wasm() {
    let db_url = match std::env::var("WRUNTIME_TEST_DB_URL") {
        Ok(u)  => u,
        Err(_) => return, // skip when no DB available
    };
    // 1. Start wr-manager and wr-engine configured with the DB URL.
    // 2. Load a minimal WASM component that calls database::query.
    // 3. Invoke the module via the inbound server.
    // 4. Assert the response contains expected row data.
}
```

For unit testing the host implementation in isolation (no WASM, no Postgres),
mock the pool with a `tokio-postgres` test server or use a struct that
implements the trait directly.

---

## File change summary

| File | Change |
|---|---|
| `wit/db.wit` | **New** — WIT interface definition |
| `wr-engine/Cargo.toml` | Add `tokio-postgres`, `deadpool-postgres` |
| `wr-engine/src/db.rs` | **New** — `bindgen!` invocation + `HostDatabase` impl |
| `wr-engine/src/pool.rs` | **New** — `build_pool` helper |
| `wr-engine/src/config.rs` | Add `DatabaseConfig`, `ModuleConfig.database` |
| `wr-engine/src/state.rs` | Add `db_pool: Option<Arc<Pool>>` to `ModuleState` |
| `wr-engine/src/engine.rs` | Build pool, register linker impl, pass pool to state |
| `engine.toml` | Add `[database]` example section |
| `wr-tests/tests/integration_test.rs` | Add `test_database_query_from_wasm` |

---

## Open questions

- **TLS** — `tokio-postgres::NoTls` is used above. Production deployments will
  need `tokio-postgres-rustls` or `postgres-native-tls`. This can be a follow-up
  driven by config (`database.tls = "require" | "prefer" | "disable"`).
- **Type fidelity** — All values are serialised as `String`. Richer WIT types
  (integers, booleans, bytes) could be added to `wit/db.wit` in a later version
  without breaking existing callers.
- **Transactions** — The current interface is stateless (one connection per
  call). A transaction resource type could be added to `wit/db.wit` as a
  follow-up, exposing `begin`, `commit`, and `rollback`.
