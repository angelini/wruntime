use chrono::Timelike as _;
use tokio::runtime::Handle;
use wasmtime::component::Resource;
use wasmtime_wasi_http::WasiHttpView as _;

/// Host-side state for an active WIT `transaction` resource.
///
/// Holds the dedicated pooled connection for the duration of the transaction.
/// `done` is set to `true` after `commit` or `rollback` so the `drop` handler
/// does not issue a redundant ROLLBACK.
pub struct TxState {
    client: deadpool_postgres::Object,
    done: bool,
}

wasmtime::component::bindgen!({
    path:               "../wit",
    world:              "db-access",
    additional_derives: [PartialEq],
    with: {
        "wruntime:db/database.transaction": TxState,
    },
});

use wruntime::db::database::{Column, DbError, Host, HostTransaction, PgValue, Row};

use crate::state::ModuleState;

// ── PgParam ──────────────────────────────────────────────────────────────────

/// Owned, typed Postgres parameter converted from the WIT `pg-value` variant.
///
/// Implements `ToSql` so a `Vec<PgParam>` can be passed directly to
/// `tokio_postgres` without boxing each concrete Rust type individually.
#[derive(Debug)]
enum PgParam {
    Null,
    Boolean(bool),
    Int2(i16),
    Int4(i32),
    Int8(i64),
    Float4(f32),
    Float8(f64),
    Text(String),
    Bytea(Vec<u8>),
    Timestamptz(chrono::DateTime<chrono::Utc>),
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
    Numeric(rust_decimal::Decimal),
    Uuid(uuid::Uuid),
    Jsonb(serde_json::Value),
    Oid(u32),
}

impl From<PgValue> for PgParam {
    fn from(v: PgValue) -> Self {
        match v {
            PgValue::Null => PgParam::Null,
            PgValue::Boolean(b) => PgParam::Boolean(b),
            PgValue::Int2(i) => PgParam::Int2(i),
            PgValue::Int4(i) => PgParam::Int4(i),
            PgValue::Int8(i) => PgParam::Int8(i),
            PgValue::Float4(f) => PgParam::Float4(f),
            PgValue::Float8(f) => PgParam::Float8(f),
            PgValue::Text(s) => PgParam::Text(s),
            PgValue::Bytea(b) => PgParam::Bytea(b),
            PgValue::Timestamptz(micros) => {
                let dt = chrono::DateTime::from_timestamp_micros(micros)
                    .unwrap_or(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH);
                PgParam::Timestamptz(dt)
            }
            PgValue::Date(days) => {
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                PgParam::Date(epoch + chrono::Duration::days(days as i64))
            }
            PgValue::Time(micros) => {
                let secs = (micros / 1_000_000) as u32;
                let nano = ((micros % 1_000_000) * 1_000) as u32;
                let t = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nano)
                    .unwrap_or(chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap());
                PgParam::Time(t)
            }
            PgValue::Numeric(s) => {
                PgParam::Numeric(s.parse().unwrap_or(rust_decimal::Decimal::ZERO))
            }
            PgValue::Uuid((hi, lo)) => {
                PgParam::Uuid(uuid::Uuid::from_u128((hi as u128) << 64 | lo as u128))
            }
            PgValue::Jsonb(s) => {
                PgParam::Jsonb(serde_json::from_str(&s).unwrap_or(serde_json::Value::Null))
            }
            PgValue::Oid(o) => PgParam::Oid(o),
        }
    }
}

impl tokio_postgres::types::ToSql for PgParam {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        buf: &mut bytes::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            PgParam::Null => Ok(tokio_postgres::types::IsNull::Yes),
            PgParam::Boolean(v) => v.to_sql(ty, buf),
            PgParam::Int2(v) => v.to_sql(ty, buf),
            PgParam::Int4(v) => v.to_sql(ty, buf),
            PgParam::Int8(v) => v.to_sql(ty, buf),
            PgParam::Float4(v) => v.to_sql(ty, buf),
            PgParam::Float8(v) => v.to_sql(ty, buf),
            PgParam::Text(v) => v.to_sql(ty, buf),
            PgParam::Bytea(v) => v.to_sql(ty, buf),
            PgParam::Timestamptz(v) => v.to_sql(ty, buf),
            PgParam::Date(v) => v.to_sql(ty, buf),
            PgParam::Time(v) => v.to_sql(ty, buf),
            PgParam::Numeric(v) => v.to_sql(ty, buf),
            PgParam::Uuid(v) => v.to_sql(ty, buf),
            PgParam::Jsonb(v) => v.to_sql(ty, buf),
            PgParam::Oid(v) => v.to_sql(ty, buf),
        }
    }

    /// Always returns `true`; each variant delegates to its inner type's
    /// `to_sql`, which handles type compatibility at serialisation time.
    fn accepts(_: &tokio_postgres::types::Type) -> bool {
        true
    }

    fn to_sql_checked(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut bytes::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        self.to_sql(ty, out)
    }
}

// ── Host implementation ──────────────────────────────────────────────────────

impl Host for ModuleState {
    fn query(&mut self, sql: String, params: Vec<PgValue>) -> Result<Vec<Row>, DbError> {
        let pool = match &self.db_pool {
            Some(p) => p.clone(),
            None => {
                return Err(DbError::Connection(
                    "no database configured for this module".into(),
                ))
            }
        };
        let schema = self.db_schema.clone();

        tokio::task::block_in_place(|| {
            Handle::current().block_on(async move {
                let client = pool
                    .get()
                    .await
                    .map_err(|e| DbError::Connection(e.to_string()))?;

                if let Some(s) = &schema {
                    client
                        .execute(
                            &format!("SET search_path = \"{s}\", public"),
                            &[] as &[&(dyn tokio_postgres::types::ToSql + Sync)],
                        )
                        .await
                        .map_err(|e| DbError::Connection(e.to_string()))?;
                }

                let pg_params: Vec<PgParam> = params.into_iter().map(PgParam::from).collect();
                let params_ref: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                    pg_params.iter().map(|p| p as _).collect();

                let rows = client
                    .query(sql.as_str(), &params_ref)
                    .await
                    .map_err(|e| DbError::Query(e.to_string()))?;

                Ok(rows.iter().map(pg_row_to_wit).collect())
            })
        })
    }

    fn execute(&mut self, sql: String, params: Vec<PgValue>) -> Result<u64, DbError> {
        let pool = match &self.db_pool {
            Some(p) => p.clone(),
            None => {
                return Err(DbError::Connection(
                    "no database configured for this module".into(),
                ))
            }
        };
        let schema = self.db_schema.clone();

        tokio::task::block_in_place(|| {
            Handle::current().block_on(async move {
                let client = pool
                    .get()
                    .await
                    .map_err(|e| DbError::Connection(e.to_string()))?;

                if let Some(s) = &schema {
                    client
                        .execute(
                            &format!("SET search_path = \"{s}\", public"),
                            &[] as &[&(dyn tokio_postgres::types::ToSql + Sync)],
                        )
                        .await
                        .map_err(|e| DbError::Connection(e.to_string()))?;
                }

                let pg_params: Vec<PgParam> = params.into_iter().map(PgParam::from).collect();
                let params_ref: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                    pg_params.iter().map(|p| p as _).collect();

                client
                    .execute(sql.as_str(), &params_ref)
                    .await
                    .map_err(|e| DbError::Query(e.to_string()))
            })
        })
    }

    fn begin_transaction(&mut self) -> Result<Resource<TxState>, DbError> {
        let pool = match &self.db_pool {
            Some(p) => p.clone(),
            None => {
                return Err(DbError::Connection(
                    "no database configured for this module".into(),
                ))
            }
        };
        let schema = self.db_schema.clone();

        let client = tokio::task::block_in_place(|| {
            Handle::current().block_on(async move {
                let client = pool
                    .get()
                    .await
                    .map_err(|e| DbError::Connection(e.to_string()))?;
                client
                    .execute("BEGIN", &[])
                    .await
                    .map_err(|e| DbError::Query(e.to_string()))?;
                if let Some(s) = &schema {
                    client
                        .execute(
                            &format!("SET search_path = \"{s}\", public"),
                            &[] as &[&(dyn tokio_postgres::types::ToSql + Sync)],
                        )
                        .await
                        .map_err(|e| DbError::Connection(e.to_string()))?;
                }
                Ok::<_, DbError>(client)
            })
        })?;

        self.table()
            .push(TxState {
                client,
                done: false,
            })
            .map_err(|e| DbError::Connection(e.to_string()))
    }
}

// ── HostTransaction implementation ───────────────────────────────────────────

impl HostTransaction for ModuleState {
    fn query(
        &mut self,
        self_: Resource<TxState>,
        sql: String,
        params: Vec<PgValue>,
    ) -> Result<Vec<Row>, DbError> {
        let state = self
            .table()
            .get(&self_)
            .map_err(|e| DbError::Connection(e.to_string()))?;

        tokio::task::block_in_place(|| {
            Handle::current().block_on(async {
                let pg_params: Vec<PgParam> = params.into_iter().map(PgParam::from).collect();
                let params_ref: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                    pg_params.iter().map(|p| p as _).collect();
                let rows = state
                    .client
                    .query(sql.as_str(), &params_ref)
                    .await
                    .map_err(|e| DbError::Query(e.to_string()))?;
                Ok(rows.iter().map(pg_row_to_wit).collect())
            })
        })
    }

    fn execute(
        &mut self,
        self_: Resource<TxState>,
        sql: String,
        params: Vec<PgValue>,
    ) -> Result<u64, DbError> {
        let state = self
            .table()
            .get(&self_)
            .map_err(|e| DbError::Connection(e.to_string()))?;

        tokio::task::block_in_place(|| {
            Handle::current().block_on(async {
                let pg_params: Vec<PgParam> = params.into_iter().map(PgParam::from).collect();
                let params_ref: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                    pg_params.iter().map(|p| p as _).collect();
                state
                    .client
                    .execute(sql.as_str(), &params_ref)
                    .await
                    .map_err(|e| DbError::Query(e.to_string()))
            })
        })
    }

    fn commit(&mut self, self_: Resource<TxState>) -> Result<(), DbError> {
        {
            let state = self
                .table()
                .get(&self_)
                .map_err(|e| DbError::Connection(e.to_string()))?;
            tokio::task::block_in_place(|| {
                Handle::current().block_on(state.client.execute("COMMIT", &[]))
            })
            .map_err(|e| DbError::Query(e.to_string()))?;
        }
        self.table()
            .get_mut(&self_)
            .map_err(|e| DbError::Connection(e.to_string()))?
            .done = true;
        Ok(())
    }

    fn rollback(&mut self, self_: Resource<TxState>) -> Result<(), DbError> {
        {
            let state = self
                .table()
                .get(&self_)
                .map_err(|e| DbError::Connection(e.to_string()))?;
            tokio::task::block_in_place(|| {
                Handle::current().block_on(state.client.execute("ROLLBACK", &[]))
            })
            .map_err(|e| DbError::Query(e.to_string()))?;
        }
        self.table()
            .get_mut(&self_)
            .map_err(|e| DbError::Connection(e.to_string()))?
            .done = true;
        Ok(())
    }

    fn drop(&mut self, rep: Resource<TxState>) -> wasmtime::Result<()> {
        let state = self.table().delete(rep)?;
        if !state.done {
            let _ = tokio::task::block_in_place(|| {
                Handle::current().block_on(state.client.execute("ROLLBACK", &[]))
            });
        }
        Ok(())
    }
}

// ── Row conversion ───────────────────────────────────────────────────────────

fn pg_row_to_wit(row: &tokio_postgres::Row) -> Row {
    let columns = row
        .columns()
        .iter()
        .enumerate()
        .map(|(i, col)| Column {
            name: col.name().to_string(),
            value: pg_col_to_wit(row, i, col.type_()),
        })
        .collect();
    Row { columns }
}

fn pg_col_to_wit(row: &tokio_postgres::Row, i: usize, ty: &tokio_postgres::types::Type) -> PgValue {
    use tokio_postgres::types::Type;

    match *ty {
        Type::BOOL => opt(row.get::<_, Option<bool>>(i), PgValue::Boolean),
        Type::INT2 => opt(row.get::<_, Option<i16>>(i), PgValue::Int2),
        Type::INT4 => opt(row.get::<_, Option<i32>>(i), PgValue::Int4),
        Type::INT8 => opt(row.get::<_, Option<i64>>(i), PgValue::Int8),
        Type::FLOAT4 => opt(row.get::<_, Option<f32>>(i), PgValue::Float4),
        Type::FLOAT8 => opt(row.get::<_, Option<f64>>(i), PgValue::Float8),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => {
            opt(row.get::<_, Option<String>>(i), PgValue::Text)
        }
        Type::BYTEA => opt(row.get::<_, Option<Vec<u8>>>(i), PgValue::Bytea),
        Type::TIMESTAMPTZ => opt(
            row.get::<_, Option<chrono::DateTime<chrono::Utc>>>(i),
            |dt| PgValue::Timestamptz(dt.timestamp_micros()),
        ),
        // TIMESTAMP (no tz): treat as UTC microseconds since epoch.
        Type::TIMESTAMP => opt(row.get::<_, Option<chrono::NaiveDateTime>>(i), |dt| {
            PgValue::Timestamptz(dt.and_utc().timestamp_micros())
        }),
        Type::DATE => opt(row.get::<_, Option<chrono::NaiveDate>>(i), |d| {
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            PgValue::Date((d - epoch).num_days() as i32)
        }),
        Type::TIME => opt(row.get::<_, Option<chrono::NaiveTime>>(i), |t| {
            let micros =
                t.num_seconds_from_midnight() as i64 * 1_000_000 + t.nanosecond() as i64 / 1_000;
            PgValue::Time(micros)
        }),
        Type::NUMERIC => opt(row.get::<_, Option<rust_decimal::Decimal>>(i), |d| {
            PgValue::Numeric(d.to_string())
        }),
        Type::UUID => opt(row.get::<_, Option<uuid::Uuid>>(i), |u| {
            let n = u.as_u128();
            PgValue::Uuid(((n >> 64) as u64, n as u64))
        }),
        Type::JSON | Type::JSONB => opt(row.get::<_, Option<serde_json::Value>>(i), |v| {
            PgValue::Jsonb(v.to_string())
        }),
        Type::OID => opt(row.get::<_, Option<u32>>(i), PgValue::Oid),
        _ => {
            tracing::warn!(
                col  = %row.columns()[i].name(),
                pg_type = %ty,
                "unsupported column type, returning null",
            );
            PgValue::Null
        }
    }
}

#[inline]
fn opt<T, F: FnOnce(T) -> PgValue>(val: Option<T>, f: F) -> PgValue {
    val.map_or(PgValue::Null, f)
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::wruntime::db::database::{DbError, Host, PgValue};
    use crate::state::ModuleState;

    fn proxy_uri() -> hyper::Uri {
        "http://127.0.0.1:9001".parse().unwrap()
    }

    // ── no-pool tests (sync — block_in_place is never reached) ───────────────

    #[test]
    fn test_query_returns_error_when_no_pool() {
        let mut state = ModuleState::new("test".into(), "test".into(), proxy_uri(), None, None);
        let result = state.query("SELECT 1".into(), vec![]);
        assert!(
            matches!(result, Err(DbError::Connection(_))),
            "expected Connection error, got {result:?}",
        );
    }

    #[test]
    fn test_execute_returns_error_when_no_pool() {
        let mut state = ModuleState::new("test".into(), "test".into(), proxy_uri(), None, None);
        let result = state.execute("SELECT 1".into(), vec![]);
        assert!(
            matches!(result, Err(DbError::Connection(_))),
            "expected Connection error, got {result:?}",
        );
    }

    #[test]
    fn test_begin_transaction_returns_error_when_no_pool() {
        let mut state = ModuleState::new("test".into(), "test".into(), proxy_uri(), None, None);
        let result = state.begin_transaction();
        assert!(
            matches!(result, Err(DbError::Connection(_))),
            "expected Connection error, got {result:?}",
        );
    }

    // ── real-Postgres tests (multi-thread runtime required for block_in_place) ─

    /// Skip the test if `WRUNTIME_TEST_DB_URL` is not set.
    fn db_url() -> Option<String> {
        std::env::var("WRUNTIME_TEST_DB_URL").ok()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_with_postgres() {
        let url = match db_url() {
            Some(u) => u,
            None => return,
        };

        let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            Some(Arc::new(pool)),
            None,
        );

        let rows = state
            .query(
                "SELECT $1::text AS echo".into(),
                vec![PgValue::Text("hello".into())],
            )
            .expect("query");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].columns[0].name, "echo");
        assert_eq!(rows[0].columns[0].value, PgValue::Text("hello".into()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_execute_with_postgres() {
        let url = match db_url() {
            Some(u) => u,
            None => return,
        };

        let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            Some(Arc::new(pool)),
            None,
        );

        // DDL returns 0 rows affected.
        let n = state
            .execute("CREATE TEMP TABLE _wr_db_test (id INT)".into(), vec![])
            .expect("create table");
        assert_eq!(n, 0);

        // DML returns the actual affected-row count.
        let n = state
            .execute("INSERT INTO _wr_db_test VALUES (1)".into(), vec![])
            .expect("insert");
        assert_eq!(n, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_parameterised_with_postgres() {
        let url = match db_url() {
            Some(u) => u,
            None => return,
        };

        let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            Some(Arc::new(pool)),
            None,
        );

        let rows = state
            .query(
                "SELECT $1::text AS a, $2::text AS b".into(),
                vec![PgValue::Text("foo".into()), PgValue::Text("bar".into())],
            )
            .expect("query");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].columns[0].name, "a");
        assert_eq!(rows[0].columns[0].value, PgValue::Text("foo".into()));
        assert_eq!(rows[0].columns[1].name, "b");
        assert_eq!(rows[0].columns[1].value, PgValue::Text("bar".into()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_typed_columns_with_postgres() {
        let url = match db_url() {
            Some(u) => u,
            None => return,
        };

        let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            Some(Arc::new(pool)),
            None,
        );

        let rows = state
            .query(
                "SELECT \
                    true::bool       AS b, \
                    42::int2         AS i2, \
                    1000::int4       AS i4, \
                    9999999999::int8 AS i8, \
                    1.5::float4      AS f4, \
                    2.5::float8      AS f8, \
                    NULL::text       AS n"
                    .into(),
                vec![],
            )
            .expect("query");

        assert_eq!(rows.len(), 1);
        let cols = &rows[0].columns;
        assert_eq!(cols[0].value, PgValue::Boolean(true));
        assert_eq!(cols[1].value, PgValue::Int2(42));
        assert_eq!(cols[2].value, PgValue::Int4(1000));
        assert_eq!(cols[3].value, PgValue::Int8(9_999_999_999));
        assert_eq!(cols[4].value, PgValue::Float4(1.5));
        assert_eq!(cols[5].value, PgValue::Float8(2.5));
        assert_eq!(cols[6].value, PgValue::Null);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_transaction_commit() {
        use super::wruntime::db::database::{Host, HostTransaction};

        let url = match db_url() {
            Some(u) => u,
            None => return,
        };

        let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            Some(Arc::new(pool)),
            None,
        );

        // Setup: create a temp table outside the transaction.
        Host::execute(
            &mut state,
            "CREATE TEMP TABLE _wr_tx_commit_test (val INT)".into(),
            vec![],
        )
        .expect("create table");

        let tx = state.begin_transaction().expect("begin");
        let rep = tx.rep();

        HostTransaction::execute(
            &mut state,
            wasmtime::component::Resource::new_borrow(rep),
            "INSERT INTO _wr_tx_commit_test VALUES (42)".into(),
            vec![],
        )
        .expect("insert");

        HostTransaction::commit(&mut state, wasmtime::component::Resource::new_borrow(rep))
            .expect("commit");

        // Release the resource first so its connection is returned to the pool.
        // done=true means no ROLLBACK is issued.
        HostTransaction::drop(&mut state, tx).expect("drop");

        // After the connection is back in the pool, Host::query reacquires it
        // and can see the TEMP TABLE (TEMP tables are connection-scoped).
        let rows = Host::query(
            &mut state,
            "SELECT val FROM _wr_tx_commit_test".into(),
            vec![],
        )
        .expect("query after commit");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].columns[0].value, PgValue::Int4(42));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_transaction_rollback() {
        use super::wruntime::db::database::{Host, HostTransaction};

        let url = match db_url() {
            Some(u) => u,
            None => return,
        };

        let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            Some(Arc::new(pool)),
            None,
        );

        Host::execute(
            &mut state,
            "CREATE TEMP TABLE _wr_tx_rollback_test (val INT)".into(),
            vec![],
        )
        .expect("create table");

        let tx = state.begin_transaction().expect("begin");
        let rep = tx.rep();

        HostTransaction::execute(
            &mut state,
            wasmtime::component::Resource::new_borrow(rep),
            "INSERT INTO _wr_tx_rollback_test VALUES (99)".into(),
            vec![],
        )
        .expect("insert");

        HostTransaction::rollback(&mut state, wasmtime::component::Resource::new_borrow(rep))
            .expect("rollback");

        // Release the resource first so its connection is returned to the pool.
        HostTransaction::drop(&mut state, tx).expect("drop");

        // After the connection is back in the pool, Host::query reacquires it
        // and can see the TEMP TABLE with the rolled-back INSERT absent.
        let rows = Host::query(
            &mut state,
            "SELECT val FROM _wr_tx_rollback_test".into(),
            vec![],
        )
        .expect("query after rollback");
        assert_eq!(rows.len(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_transaction_implicit_rollback_on_drop() {
        use super::wruntime::db::database::{Host, HostTransaction};

        let url = match db_url() {
            Some(u) => u,
            None => return,
        };

        let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            Some(Arc::new(pool)),
            None,
        );

        Host::execute(
            &mut state,
            "CREATE TEMP TABLE _wr_tx_drop_test (val INT)".into(),
            vec![],
        )
        .expect("create table");

        let tx = state.begin_transaction().expect("begin");
        let rep = tx.rep();

        HostTransaction::execute(
            &mut state,
            wasmtime::component::Resource::new_borrow(rep),
            "INSERT INTO _wr_tx_drop_test VALUES (7)".into(),
            vec![],
        )
        .expect("insert");

        // Drop without committing — host must issue implicit ROLLBACK.
        HostTransaction::drop(&mut state, tx).expect("drop");

        let rows = Host::query(
            &mut state,
            "SELECT val FROM _wr_tx_drop_test".into(),
            vec![],
        )
        .expect("query after implicit rollback");
        assert_eq!(rows.len(), 0);
    }
}
