use wasmtime::component::Resource;

use wr_common::pool::pg_error_string;

use super::bindings::{CursorOwner, CursorResourceState, CursorState, TxState};
use super::host::{execute_statement, open_row_stream, query_rows, HostDbError};
use super::telemetry::DbOperation;
use super::wruntime::db::database::{DbError, HostTransaction, PgValue, Row};
use crate::state::{ModuleState, ResourceKind};

struct TerminalGuard {
    lifecycle: super::bindings::TxLifecycle,
    armed: bool,
}

impl TerminalGuard {
    fn new(lifecycle: super::bindings::TxLifecycle) -> Self {
        Self {
            lifecycle,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        if self.armed {
            self.lifecycle.mark_discard();
        }
    }
}

fn public_transaction_error(
    lifecycle: &super::bindings::TxLifecycle,
    error: HostDbError,
) -> DbError {
    if error.is_postgres() {
        lifecycle.mark_postgres_error();
    }
    error.into_public()
}

impl HostTransaction for ModuleState {
    async fn query(
        &mut self,
        self_: Resource<TxState>,
        sql: String,
        params: Vec<PgValue>,
    ) -> Result<Vec<Row>, DbError> {
        let mut telemetry = self.start_db_span(DbOperation::TransactionQuery, &sql);
        let result = async {
            let state = self
                .table()
                .get(&self_)
                .map_err(|error| DbError::Connection(error.to_string()))?;
            state.lifecycle.ensure_operation()?;
            let client = state.client()?;
            query_rows(client, sql, params)
                .await
                .map_err(|error| public_transaction_error(&state.lifecycle, error))
        }
        .await;
        telemetry.finish_result(&result, |rows| rows.len() as u64);
        result
    }

    async fn execute(
        &mut self,
        self_: Resource<TxState>,
        sql: String,
        params: Vec<PgValue>,
    ) -> Result<u64, DbError> {
        let mut telemetry = self.start_db_span(DbOperation::TransactionExecute, &sql);
        let result = async {
            let state = self
                .table()
                .get(&self_)
                .map_err(|error| DbError::Connection(error.to_string()))?;
            state.lifecycle.ensure_operation()?;
            let client = state.client()?;
            execute_statement(client, sql, params)
                .await
                .map_err(|error| public_transaction_error(&state.lifecycle, error))
        }
        .await;
        telemetry.finish_result(&result, |affected| *affected);
        result
    }

    async fn query_stream(
        &mut self,
        self_: Resource<TxState>,
        sql: String,
        params: Vec<PgValue>,
    ) -> Result<Resource<CursorState>, DbError> {
        let mut telemetry = self.start_db_span(DbOperation::TransactionStream, &sql);
        let lifecycle = match self.table().get(&self_) {
            Ok(state) => state.lifecycle.clone(),
            Err(error) => {
                let error = DbError::Connection(error.to_string());
                telemetry.finish_error(&error);
                return Err(error);
            }
        };
        if let Err(error) = lifecycle.reserve_cursor() {
            telemetry.finish_error(&error);
            return Err(error);
        }

        let guard = match self.db().and_then(|database| {
            database
                .accounting
                .try_track(ResourceKind::DbCursor)
                .ok_or_else(|| DbError::Connection("db cursor cap exceeded".into()))
        }) {
            Ok(guard) => guard,
            Err(error) => {
                lifecycle.release_cursor();
                telemetry.finish_error(&error);
                return Err(error);
            }
        };

        let stream = {
            let state = match self.table().get(&self_) {
                Ok(state) => state,
                Err(error) => {
                    lifecycle.release_cursor();
                    let error = DbError::Connection(error.to_string());
                    telemetry.finish_error(&error);
                    return Err(error);
                }
            };
            let client = match state.client() {
                Ok(client) => client,
                Err(error) => {
                    lifecycle.release_cursor();
                    telemetry.finish_error(&error);
                    return Err(error);
                }
            };
            match open_row_stream(client, sql, params).await {
                Ok(stream) => stream,
                Err(error) => {
                    lifecycle.release_cursor();
                    let error = public_transaction_error(&lifecycle, error);
                    telemetry.finish_error(&error);
                    return Err(error);
                }
            }
        };

        let parent = self_.rep();
        match self.table().push_child(
            CursorState {
                state: CursorResourceState::Active {
                    stream: Box::pin(stream),
                    owner: CursorOwner::Transaction {
                        parent,
                        lifecycle: lifecycle.clone(),
                    },
                },
                telemetry,
                _count: guard,
            },
            &self_,
        ) {
            Ok(cursor) => Ok(cursor),
            Err(error) => {
                lifecycle.release_cursor();
                Err(DbError::Connection(error.to_string()))
            }
        }
    }

    async fn commit(&mut self, self_: Resource<TxState>) -> Result<(), DbError> {
        self.finish_transaction(self_, true).await
    }

    async fn rollback(&mut self, self_: Resource<TxState>) -> Result<(), DbError> {
        self.finish_transaction(self_, false).await
    }

    async fn drop(&mut self, rep: Resource<TxState>) -> wasmtime::Result<()> {
        let mut state = self.table().delete(rep)?;
        state.lifecycle.complete();
        let Some(client) = state.take_active() else {
            return Ok(());
        };
        if state.lifecycle.must_discard() {
            drop(deadpool_postgres::Object::take(client));
        } else if let Err(error) = client.execute("ROLLBACK", &[]).await {
            tracing::warn!(error = %pg_error_string(&error), "transaction rollback during drop failed");
            drop(deadpool_postgres::Object::take(client));
        }
        Ok(())
    }
}

impl ModuleState {
    async fn finish_transaction(
        &mut self,
        transaction: Resource<TxState>,
        commit: bool,
    ) -> Result<(), DbError> {
        let (lifecycle, poisoned) = {
            let state = self
                .table()
                .get(&transaction)
                .map_err(|error| DbError::Connection(error.to_string()))?;
            state.lifecycle.ensure_terminal()?;
            (state.lifecycle.clone(), state.lifecycle.is_poisoned())
        };
        let command = if commit && poisoned {
            "ROLLBACK"
        } else if commit {
            "COMMIT"
        } else {
            "ROLLBACK"
        };
        let mut terminal_guard = TerminalGuard::new(lifecycle.clone());
        let terminal_result = {
            let state = self
                .table()
                .get(&transaction)
                .map_err(|error| DbError::Connection(error.to_string()))?;
            state.client()?.execute(command, &[]).await
        };
        terminal_guard.disarm();

        if let Err(error) = terminal_result {
            return Err(DbError::Query(format!(
                "transaction terminal command failed: {}",
                pg_error_string(&error)
            )));
        }

        lifecycle.complete();
        self.table()
            .get_mut(&transaction)
            .map_err(|error| DbError::Connection(error.to_string()))?
            .complete();

        if commit && poisoned {
            return Err(DbError::Query(
                "transaction was aborted by PostgreSQL and rolled back".into(),
            ));
        }
        Ok(())
    }
}
