use wasmtime::component::Resource;

use wr_common::pool::pg_error_string;

use super::bindings::{CursorOwner, CursorResourceState, CursorState, TxResourceState, TxState};
use super::connection::get_prepared_connection;
use super::params::PreparedGuestQuery;
use super::rows::pg_row_to_wit;
use super::telemetry::DbOperation;
use super::wruntime::db::database::{DbError, Host, PgValue, Row};
use crate::state::{ModuleState, ResourceKind};

pub(crate) enum HostDbError {
    Parameter(DbError),
    Postgres(DbError),
    Result(DbError),
}

impl HostDbError {
    pub(crate) fn into_public(self) -> DbError {
        match self {
            Self::Parameter(error) | Self::Postgres(error) | Self::Result(error) => error,
        }
    }

    pub(crate) fn is_postgres(&self) -> bool {
        matches!(self, Self::Postgres(_))
    }
}

pub(crate) async fn query_rows(
    client: &deadpool_postgres::Object,
    sql: String,
    params: Vec<PgValue>,
) -> Result<Vec<Row>, HostDbError> {
    let prepared = PreparedGuestQuery::new(sql, params).map_err(HostDbError::Parameter)?;
    let rows = client
        .query(prepared.sql(), &prepared.raw_params())
        .await
        .map_err(|e| HostDbError::Postgres(DbError::Query(pg_error_string(&e))))?;
    rows.iter()
        .map(pg_row_to_wit)
        .collect::<Result<_, _>>()
        .map_err(HostDbError::Result)
}

pub(crate) async fn execute_statement(
    client: &deadpool_postgres::Object,
    sql: String,
    params: Vec<PgValue>,
) -> Result<u64, HostDbError> {
    let prepared = PreparedGuestQuery::new(sql, params).map_err(HostDbError::Parameter)?;
    client
        .execute(prepared.sql(), &prepared.raw_params())
        .await
        .map_err(|e| HostDbError::Postgres(DbError::Query(pg_error_string(&e))))
}

pub(crate) async fn open_row_stream(
    client: &deadpool_postgres::Object,
    sql: String,
    params: Vec<PgValue>,
) -> Result<tokio_postgres::RowStream, HostDbError> {
    let prepared = PreparedGuestQuery::new(sql, params).map_err(HostDbError::Parameter)?;
    client
        .query_raw(prepared.sql(), prepared.raw_params())
        .await
        .map_err(|e| HostDbError::Postgres(DbError::Query(pg_error_string(&e))))
}

impl Host for ModuleState {
    fn query(
        &mut self,
        sql: String,
        params: Vec<PgValue>,
    ) -> impl std::future::Future<Output = Result<Vec<Row>, DbError>> + Send {
        let mut telemetry = self.start_db_span(DbOperation::Query, &sql);
        let prepared = self
            .db()
            .map(|db| (db.pool.clone(), db.schema.clone(), db.timeouts.clone()));
        async move {
            let result = async {
                let (pool, schema, timeouts) = prepared?;
                let client = get_prepared_connection(&pool, &schema, &timeouts).await?;
                query_rows(&client, sql, params)
                    .await
                    .map_err(HostDbError::into_public)
            }
            .await;
            telemetry.finish_result(&result, |rows| rows.len() as u64);
            result
        }
    }

    fn execute(
        &mut self,
        sql: String,
        params: Vec<PgValue>,
    ) -> impl std::future::Future<Output = Result<u64, DbError>> + Send {
        let mut telemetry = self.start_db_span(DbOperation::Execute, &sql);
        let prepared = self
            .db()
            .map(|db| (db.pool.clone(), db.schema.clone(), db.timeouts.clone()));
        async move {
            let result = async {
                let (pool, schema, timeouts) = prepared?;
                let client = get_prepared_connection(&pool, &schema, &timeouts).await?;
                execute_statement(&client, sql, params)
                    .await
                    .map_err(HostDbError::into_public)
            }
            .await;
            telemetry.finish_result(&result, |affected| *affected);
            result
        }
    }

    async fn query_stream(
        &mut self,
        sql: String,
        params: Vec<PgValue>,
    ) -> Result<Resource<CursorState>, DbError> {
        let mut telemetry = self.start_db_span(DbOperation::Stream, &sql);
        let prepared = (|| {
            let db = self.db()?;
            let guard = db
                .accounting
                .try_track(ResourceKind::DbCursor)
                .ok_or_else(|| DbError::Connection("db cursor cap exceeded".into()))?;
            Ok((
                db.pool.clone(),
                db.schema.clone(),
                db.timeouts.clone(),
                guard,
            ))
        })();
        let (pool, schema, timeouts, guard) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                telemetry.finish_error(&error);
                return Err(error);
            }
        };
        let client = match get_prepared_connection(&pool, &schema, &timeouts).await {
            Ok(client) => client,
            Err(error) => {
                telemetry.finish_error(&error);
                return Err(error);
            }
        };
        let stream = match open_row_stream(&client, sql, params).await {
            Ok(stream) => stream,
            Err(error) => {
                let error = error.into_public();
                telemetry.finish_error(&error);
                return Err(error);
            }
        };
        self.table()
            .push(CursorState {
                state: CursorResourceState::Active {
                    stream: Box::pin(stream),
                    owner: CursorOwner::Connection {
                        _lease: Box::new(client),
                    },
                },
                telemetry,
                _count: guard,
            })
            .map_err(|error| DbError::Connection(error.to_string()))
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
            .map_err(|error| DbError::Query(pg_error_string(&error)))?;
        self.table()
            .push(TxState {
                state: TxResourceState::Active(Box::new(client)),
                lifecycle: super::bindings::TxLifecycle::new(),
                _count: guard,
            })
            .map_err(|error| DbError::Connection(error.to_string()))
    }
}
