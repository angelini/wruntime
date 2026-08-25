use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::state::CounterGuard;

use super::telemetry::DbSpan;
use super::wruntime::db::database::DbError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TxPhase {
    Active,
    Poisoned,
    Completed,
}

#[derive(Debug)]
struct TxLifecycleState {
    phase: TxPhase,
    active_cursor: bool,
    discard: bool,
}

/// Synchronous transaction ordering state shared with transaction cursors.
/// No lock guard from this type is held across an await point.
#[derive(Clone, Debug)]
pub(crate) struct TxLifecycle(Arc<Mutex<TxLifecycleState>>);

impl TxLifecycle {
    pub(crate) fn new() -> Self {
        Self(Arc::new(Mutex::new(TxLifecycleState {
            phase: TxPhase::Active,
            active_cursor: false,
            discard: false,
        })))
    }

    fn state(&self) -> MutexGuard<'_, TxLifecycleState> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(crate) fn ensure_operation(&self) -> Result<(), DbError> {
        let state = self.state();
        if state.phase == TxPhase::Completed {
            Err(DbError::Query("transaction already completed".into()))
        } else if state.active_cursor {
            Err(DbError::Query(
                "transaction has an active row cursor".into(),
            ))
        } else if state.discard {
            Err(DbError::Connection(
                "transaction connection is unusable".into(),
            ))
        } else {
            Ok(())
        }
    }

    pub(crate) fn ensure_terminal(&self) -> Result<(), DbError> {
        self.ensure_operation()
    }

    pub(crate) fn reserve_cursor(&self) -> Result<(), DbError> {
        let mut state = self.state();
        if state.phase == TxPhase::Completed {
            return Err(DbError::Query("transaction already completed".into()));
        }
        if state.discard {
            return Err(DbError::Connection(
                "transaction connection is unusable".into(),
            ));
        }
        if state.active_cursor {
            return Err(DbError::Query(
                "transaction has an active row cursor".into(),
            ));
        }
        state.active_cursor = true;
        Ok(())
    }

    pub(crate) fn release_cursor(&self) {
        self.state().active_cursor = false;
    }

    pub(crate) fn mark_postgres_error(&self) {
        let mut state = self.state();
        if state.phase != TxPhase::Completed {
            state.phase = TxPhase::Poisoned;
        }
    }

    pub(crate) fn mark_discard(&self) {
        let mut state = self.state();
        state.discard = true;
        if state.phase != TxPhase::Completed {
            state.phase = TxPhase::Poisoned;
        }
    }

    pub(crate) fn is_poisoned(&self) -> bool {
        self.state().phase == TxPhase::Poisoned
    }

    fn must_discard(&self) -> bool {
        self.state().discard
    }

    pub(crate) fn complete(&self) {
        let mut state = self.state();
        state.phase = TxPhase::Completed;
        state.active_cursor = false;
    }
}

/// Host-side state for a WIT `transaction` resource.
pub struct TxState {
    /// `None` after a terminal command releases or discards the connection.
    pub(crate) client: Option<deadpool_postgres::Object>,
    pub(crate) lifecycle: TxLifecycle,
    pub(crate) _count: CounterGuard,
}

impl Drop for TxState {
    fn drop(&mut self) {
        if !self.lifecycle.must_discard() {
            return;
        }
        let Some(client) = self.client.take() else {
            return;
        };
        drop(deadpool_postgres::Object::take(client));
    }
}

/// Host-side state for a WIT `row-cursor` resource.
pub struct CursorState {
    pub(crate) stream: Option<Pin<Box<tokio_postgres::RowStream>>>,
    /// Keeps the connection alive for non-transactional cursors.
    pub(crate) conn: Option<deadpool_postgres::Object>,
    /// Parent table index for a transaction cursor.
    pub(crate) parent: Option<u32>,
    pub(crate) lifecycle: Option<TxLifecycle>,
    pub(crate) done: bool,
    pub(crate) telemetry: DbSpan,
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
