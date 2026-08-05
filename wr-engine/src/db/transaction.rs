use wasmtime::component::Resource;

use wr_common::pool::pg_error_string;

use super::bindings::{CursorState, TxState};
use super::host::{execute_statement, open_row_stream, query_rows};
use super::telemetry::DbOperation;
use super::wruntime::db::database::{DbError, HostTransaction, PgValue, Row};
use crate::state::{ModuleState, ResourceKind};

// ── HostTransaction implementation ───────────────────────────────────────────

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
            if state.done {
                return Err(DbError::Query("transaction already completed".into()));
            }
            query_rows(&state.client, sql.as_str(), params).await
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
            if state.done {
                return Err(DbError::Query("transaction already completed".into()));
            }
            execute_statement(&state.client, sql.as_str(), params).await
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
        let state = match self.table().get(&self_) {
            Ok(state) => state,
            Err(error) => {
                let error = DbError::Connection(error.to_string());
                telemetry.finish_error(&error);
                return Err(error);
            }
        };
        if state.done {
            let error = DbError::Query("transaction already completed".into());
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
                telemetry.finish_error(&error);
                return Err(error);
            }
        };
        let state = match self.table().get(&self_) {
            Ok(state) => state,
            Err(error) => {
                let error = DbError::Connection(error.to_string());
                telemetry.finish_error(&error);
                return Err(error);
            }
        };
        let stream = match open_row_stream(&state.client, sql.as_str(), params).await {
            Ok(stream) => stream,
            Err(error) => {
                telemetry.finish_error(&error);
                return Err(error);
            }
        };
        self.table()
            .push(CursorState {
                stream: Box::pin(stream),
                _conn: None,
                done: false,
                telemetry,
                _count: guard,
            })
            .map_err(|error| DbError::Connection(error.to_string()))
    }

    async fn commit(&mut self, self_: Resource<TxState>) -> Result<(), DbError> {
        let state = self
            .table()
            .get(&self_)
            .map_err(|e| DbError::Connection(e.to_string()))?;
        if state.done {
            return Err(DbError::Query("transaction already completed".into()));
        }
        state
            .client
            .execute("COMMIT", &[])
            .await
            .map_err(|e| DbError::Query(pg_error_string(&e)))?;
        self.table()
            .get_mut(&self_)
            .map_err(|e| DbError::Connection(e.to_string()))?
            .done = true;
        Ok(())
    }

    async fn rollback(&mut self, self_: Resource<TxState>) -> Result<(), DbError> {
        let state = self
            .table()
            .get(&self_)
            .map_err(|e| DbError::Connection(e.to_string()))?;
        if state.done {
            return Err(DbError::Query("transaction already completed".into()));
        }
        state
            .client
            .execute("ROLLBACK", &[])
            .await
            .map_err(|e| DbError::Query(pg_error_string(&e)))?;
        self.table()
            .get_mut(&self_)
            .map_err(|e| DbError::Connection(e.to_string()))?
            .done = true;
        Ok(())
    }

    async fn drop(&mut self, rep: Resource<TxState>) -> wasmtime::Result<()> {
        let state = self.table().delete(rep)?;
        if !state.done {
            let _ = state.client.execute("ROLLBACK", &[]).await;
        }
        Ok(())
    }
}
