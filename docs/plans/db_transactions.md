# Plan: Transaction support for `db.wit`

## Goal

Extend the `wruntime:db/database` WIT interface with a `transaction` resource so
WASM modules can execute multiple statements atomically. The host acquires a
dedicated pooled connection per transaction, issues raw `BEGIN`/`COMMIT`/`ROLLBACK`
SQL, and stores the live connection in wasmtime's `ResourceTable`.

---

## Design decisions

### WIT `resource` vs stateless functions

Transactions are inherently stateful — the connection must remain open between
`begin`, intermediate queries, and `commit`/`rollback`. WIT `resource` types are
the right primitive: the host manages the lifetime, and the guest holds an opaque
handle.

### `BEGIN`/`COMMIT`/`ROLLBACK` over `tokio_postgres::Transaction<'_>`

`tokio_postgres::Transaction<'_>` borrows from its parent `Client`, making it
impossible to store both in the same struct without unsafe self-referential
tricks. Issuing raw SQL avoids this entirely: the host stores the bare
`deadpool_postgres::Object` and drives the transaction lifecycle manually.
Behaviour is identical; the safety guarantee comes from the resource's explicit
`drop` rollback (see §3 below).

### Implicit rollback on drop

If the WASM module drops the transaction handle without committing, the host
issues `ROLLBACK` synchronously in the WIT `drop` handler using the existing
`block_in_place` + `block_on` pattern, then returns the connection to the pool.

---

## Phase 1 — WIT interface changes

**File:** `wit/db.wit`

Bump the package version to `0.2.0` and add the `transaction` resource plus
`begin-transaction` to the `database` interface.

```wit
package wruntime:db@0.2.0;

interface database {
    // … existing pg-value, column, row, db-error definitions unchanged …

    /// A live database transaction.
    ///
    /// The host issues `BEGIN` when this resource is created.
    /// Dropping the handle without calling `commit` causes the host to
    /// issue `ROLLBACK` automatically.
    resource transaction {
        /// Execute a SELECT inside this transaction.
        query: func(
            sql:    string,
            params: list<pg-value>,
        ) -> result<list<row>, db-error>;

        /// Execute an INSERT / UPDATE / DELETE inside this transaction.
        execute: func(
            sql:    string,
            params: list<pg-value>,
        ) -> result<u64, db-error>;

        /// Commit the transaction.
        /// Returns an error if the database rejects the commit.
        /// The handle is consumed and must not be used afterwards.
        commit: func() -> result<_, db-error>;

        /// Roll back the transaction explicitly.
        /// The handle is consumed and must not be used afterwards.
        rollback: func() -> result<_, db-error>;
    }

    // … existing query and execute functions …

    /// Acquire a connection and begin a transaction (issues `BEGIN`).
    begin-transaction: func() -> result<transaction, db-error>;
}

world db-access {
    import database;
}
```

---

## Phase 2 — `TxState` host struct

**File:** `wr-engine/src/db.rs`

Add a struct that wraps the pooled connection and tracks whether the transaction
has been finalised:

```rust
/// Host-side state for an active WIT `transaction` resource.
pub struct TxState {
    /// The dedicated pooled connection for this transaction.
    client: deadpool_postgres::Object,
    /// Set to `true` after `commit` or `rollback` so the `drop` handler
    /// does not issue a redundant `ROLLBACK`.
    done: bool,
}
```

Store it in `ModuleState`'s existing `ResourceTable` via
`self.table().push(tx_state)`. The `push` call returns a
`wasmtime::component::Resource<Transaction>` handle that is returned to the
guest.

---

## Phase 3 — `HostTransaction` implementation

wasmtime's `bindgen!` emits a `HostTransaction` trait with one method per
resource function plus a `drop` method. Implement it for `ModuleState`:

```rust
impl HostTransaction for ModuleState {
    fn query(
        &mut self,
        handle: Resource<Transaction>,
        sql: String,
        params: Vec<PgValue>,
    ) -> Result<Vec<Row>, DbError> {
        let state = self.table().get(&handle)
            .map_err(|_| DbError::Connection("invalid transaction handle".into()))?;
        // use block_in_place + block_on, same pattern as existing Host impl
        // run state.client.query(sql, params_ref).await
    }

    fn execute(
        &mut self,
        handle: Resource<Transaction>,
        sql: String,
        params: Vec<PgValue>,
    ) -> Result<u64, DbError> {
        // same pattern as query above
    }

    fn commit(&mut self, handle: Resource<Transaction>) -> Result<(), DbError> {
        let state = self.table().get_mut(&handle)
            .map_err(|_| DbError::Connection("invalid transaction handle".into()))?;
        // block_in_place: state.client.execute("COMMIT", &[]).await
        state.done = true;
        Ok(())
    }

    fn rollback(&mut self, handle: Resource<Transaction>) -> Result<(), DbError> {
        let state = self.table().get_mut(&handle)
            .map_err(|_| DbError::Connection("invalid transaction handle".into()))?;
        // block_in_place: state.client.execute("ROLLBACK", &[]).await
        state.done = true;
        Ok(())
    }

    fn drop(&mut self, handle: Resource<Transaction>) -> wasmtime::Result<()> {
        let state = self.table().delete(handle)?;
        if !state.done {
            // Implicit rollback — ignore errors; connection returns to pool.
            let _ = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(state.client.execute("ROLLBACK", &[]))
            });
        }
        // `state` drops here, returning the connection to deadpool.
        Ok(())
    }
}
```

---

## Phase 4 — `HostDatabase::begin_transaction`

Add the new function to the existing `impl Host for ModuleState` block:

```rust
fn begin_transaction(&mut self) -> Result<Resource<Transaction>, DbError> {
    let pool = match &self.db_pool {
        Some(p) => p.clone(),
        None => return Err(DbError::Connection(
            "no database configured for this module".into(),
        )),
    };

    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(async move {
            let client = pool.get().await
                .map_err(|e| DbError::Connection(e.to_string()))?;
            client.execute("BEGIN", &[]).await
                .map_err(|e| DbError::Query(e.to_string()))?;
            let state = TxState { client, done: false };
            self.table()
                .push(state)
                .map_err(|e| DbError::Connection(e.to_string()))
        })
    })
}
```

---

## Phase 5 — bindgen update

Update the `bindgen!` invocation in `db.rs` so wasmtime generates the
`HostTransaction` trait:

```rust
wasmtime::component::bindgen!({
    path:               "../wit",
    world:              "db-access",
    additional_derives: [PartialEq],
    with: {
        "wruntime:db/database/transaction": TxState,
    },
});
```

The `with` key tells wasmtime to use `TxState` as the Rust type backing the
`transaction` resource instead of generating an empty placeholder.

---

## Phase 6 — Tests

### Unit tests (no Postgres required)

```rust
#[test]
fn begin_transaction_returns_error_when_no_pool() {
    let mut state = ModuleState::new("test".into(), proxy_uri(), None);
    let result = state.begin_transaction();
    assert!(matches!(result, Err(DbError::Connection(_))));
}
```

### Integration tests (require `WRUNTIME_TEST_DB_URL`)

```rust
// Commit path
#[tokio::test(flavor = "multi_thread")]
async fn test_transaction_commit() {
    // begin → CREATE TEMP TABLE → INSERT → commit → query confirms row exists
}

// Rollback path
#[tokio::test(flavor = "multi_thread")]
async fn test_transaction_rollback() {
    // begin → INSERT → rollback → query confirms row absent
}

// Implicit rollback on drop
#[tokio::test(flavor = "multi_thread")]
async fn test_transaction_implicit_rollback_on_drop() {
    // begin → INSERT → drop handle → query confirms row absent
}
```

---

## File change summary

| File | Change |
|---|---|
| `wit/db.wit` | Add `transaction` resource and `begin-transaction` function; bump to `0.2.0` |
| `wr-engine/src/db.rs` | Add `TxState` struct; implement `HostTransaction` and `begin_transaction`; update `bindgen!` `with` map |

No changes are required to `Cargo.toml`, `pool.rs`, `state.rs`, `engine.rs`, or
any config files — all new behaviour fits within the existing dependency set and
`ModuleState` structure.

---

## Open questions

- **Savepoints** — `SAVEPOINT`/`RELEASE SAVEPOINT`/`ROLLBACK TO SAVEPOINT` could
  be exposed as additional resource methods if nested transaction semantics are
  needed later.
- **Isolation level** — `begin-transaction` could accept an optional
  `isolation-level` enum (`read-committed | repeatable-read | serializable`) and
  pass it to the `BEGIN` statement.
- **Error on use-after-commit** — Currently calling `query` on a committed
  transaction will run against the recycled connection in a new implicit
  transaction. A `done` guard check at the start of `query`/`execute` could
  return `DbError::Connection("transaction already finalised")` instead.
