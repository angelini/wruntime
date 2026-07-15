use std::pin::Pin;

use crate::state::CounterGuard;

/// Host-side state for an active WIT `transaction` resource.
///
/// Holds the dedicated pooled connection for the duration of the transaction.
/// `done` is set to `true` after `commit` or `rollback` so the `drop` handler
/// does not issue a redundant ROLLBACK.
pub struct TxState {
    pub(crate) client: deadpool_postgres::Object,
    pub(crate) done: bool,
    pub(crate) _count: CounterGuard,
}

/// Host-side state for an active WIT `row-cursor` resource.
///
/// Wraps a `tokio_postgres::RowStream` and optionally owns the pooled
/// connection that produced it (for non-transactional queries).  When the
/// cursor is created inside a transaction the connection is owned by `TxState`
/// instead, so `_conn` is `None`.
pub struct CursorState {
    pub(crate) stream: Pin<Box<tokio_postgres::RowStream>>,
    /// Keeps the connection alive for non-transactional cursors.
    pub(crate) _conn: Option<deadpool_postgres::Object>,
    pub(crate) done: bool,
    pub(crate) _count: CounterGuard,
}

wasmtime::component::bindgen!({
    path:  "../wit/db.wit",
    world: "db-access",
    imports: { default: async },
    additional_derives: [PartialEq],
    with: {
        "wruntime:db/database@0.4.0.transaction": TxState,
        "wruntime:db/database@0.4.0.row-cursor": CursorState,
    },
});
