use wasmtime::component::Resource;

use wr_common::pool::pg_error_string;

use super::bindings::{CursorState, TxState};
use super::host::{execute_statement, open_row_stream, query_rows};
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
        let state = self
            .table()
            .get(&self_)
            .map_err(|e| DbError::Connection(e.to_string()))?;
        query_rows(&state.client, sql.as_str(), params).await
    }

    async fn execute(
        &mut self,
        self_: Resource<TxState>,
        sql: String,
        params: Vec<PgValue>,
    ) -> Result<u64, DbError> {
        let state = self
            .table()
            .get(&self_)
            .map_err(|e| DbError::Connection(e.to_string()))?;
        execute_statement(&state.client, sql.as_str(), params).await
    }

    async fn query_stream(
        &mut self,
        self_: Resource<TxState>,
        sql: String,
        params: Vec<PgValue>,
    ) -> Result<Resource<CursorState>, DbError> {
        let guard = self
            .db()?
            .accounting
            .try_track(ResourceKind::DbCursor)
            .ok_or_else(|| DbError::Connection("db cursor cap exceeded".into()))?;
        let state = self
            .table()
            .get(&self_)
            .map_err(|e| DbError::Connection(e.to_string()))?;
        let stream = open_row_stream(&state.client, sql.as_str(), params).await?;
        self.table()
            .push(CursorState {
                stream: Box::pin(stream),
                _conn: None, // connection owned by TxState
                done: false,
                _count: guard,
            })
            .map_err(|e| DbError::Connection(e.to_string()))
    }

    async fn commit(&mut self, self_: Resource<TxState>) -> Result<(), DbError> {
        let state = self
            .table()
            .get(&self_)
            .map_err(|e| DbError::Connection(e.to_string()))?;
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
