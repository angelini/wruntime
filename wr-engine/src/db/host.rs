use wasmtime::component::Resource;

use wr_common::pool::pg_error_string;

use super::bindings::{CursorState, TxState};
use super::connection::get_prepared_connection;
use super::params::prepare_params;
use super::rows::pg_row_to_wit;
use super::wruntime::db::database::{DbError, Host, PgValue, Row};
use crate::state::{ModuleState, ResourceKind};

pub(crate) async fn query_rows(
    client: &deadpool_postgres::Object,
    sql: &str,
    params: Vec<PgValue>,
) -> Result<Vec<Row>, DbError> {
    let pg_params = prepare_params(params)?;
    let params_ref: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
        pg_params.iter().map(|p| p as _).collect();
    let rows = client
        .query(sql, &params_ref)
        .await
        .map_err(|e| DbError::Query(pg_error_string(&e)))?;
    Ok(rows.iter().map(pg_row_to_wit).collect())
}

pub(crate) async fn execute_statement(
    client: &deadpool_postgres::Object,
    sql: &str,
    params: Vec<PgValue>,
) -> Result<u64, DbError> {
    let pg_params = prepare_params(params)?;
    let params_ref: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
        pg_params.iter().map(|p| p as _).collect();
    client
        .execute(sql, &params_ref)
        .await
        .map_err(|e| DbError::Query(pg_error_string(&e)))
}

pub(crate) async fn open_row_stream(
    client: &deadpool_postgres::Object,
    sql: &str,
    params: Vec<PgValue>,
) -> Result<tokio_postgres::RowStream, DbError> {
    let pg_params = prepare_params(params)?;
    let params_ref: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
        pg_params.iter().map(|p| p as _).collect();
    client
        .query_raw(sql, params_ref)
        .await
        .map_err(|e| DbError::Query(pg_error_string(&e)))
}

impl Host for ModuleState {
    fn query(
        &mut self,
        sql: String,
        params: Vec<PgValue>,
    ) -> impl std::future::Future<Output = Result<Vec<Row>, DbError>> + Send {
        let prepared = self
            .db()
            .map(|db| (db.pool.clone(), db.schema.clone(), db.timeouts.clone()));
        async move {
            let (pool, schema, timeouts) = prepared?;
            let client = get_prepared_connection(&pool, &schema, &timeouts).await?;
            query_rows(&client, sql.as_str(), params).await
        }
    }

    fn execute(
        &mut self,
        sql: String,
        params: Vec<PgValue>,
    ) -> impl std::future::Future<Output = Result<u64, DbError>> + Send {
        let prepared = self
            .db()
            .map(|db| (db.pool.clone(), db.schema.clone(), db.timeouts.clone()));
        async move {
            let (pool, schema, timeouts) = prepared?;
            let client = get_prepared_connection(&pool, &schema, &timeouts).await?;
            execute_statement(&client, sql.as_str(), params).await
        }
    }

    async fn query_stream(
        &mut self,
        sql: String,
        params: Vec<PgValue>,
    ) -> Result<Resource<CursorState>, DbError> {
        let (pool, schema, timeouts, guard) = {
            let db = self.db()?;
            let guard = db
                .accounting
                .try_track(ResourceKind::DbCursor)
                .ok_or_else(|| DbError::Connection("db cursor cap exceeded".into()))?;
            (
                db.pool.clone(),
                db.schema.clone(),
                db.timeouts.clone(),
                guard,
            )
        };
        let client = get_prepared_connection(&pool, &schema, &timeouts).await?;
        let stream = open_row_stream(&client, sql.as_str(), params).await?;
        self.table()
            .push(CursorState {
                stream: Box::pin(stream),
                _conn: Some(client),
                done: false,
                _count: guard,
            })
            .map_err(|e| DbError::Connection(e.to_string()))
    }

    async fn begin_transaction(&mut self) -> Result<Resource<TxState>, DbError> {
        let (pool, schema, timeouts, guard) = {
            let db = self.db()?;
            let guard = db
                .accounting
                .try_track(ResourceKind::DbTransaction)
                .ok_or_else(|| DbError::Connection("db transaction cap exceeded".into()))?;
            (
                db.pool.clone(),
                db.schema.clone(),
                db.timeouts.clone(),
                guard,
            )
        };
        let client = get_prepared_connection(&pool, &schema, &timeouts).await?;
        client
            .execute("BEGIN", &[])
            .await
            .map_err(|e| DbError::Query(pg_error_string(&e)))?;
        self.table()
            .push(TxState {
                client,
                done: false,
                _count: guard,
            })
            .map_err(|e| DbError::Connection(e.to_string()))
    }
}
