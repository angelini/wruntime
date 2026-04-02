use chrono::Timelike as _;
use futures::StreamExt as _;
use wasmtime::component::Resource;

/// Format a tokio-postgres error with its full source chain.
///
/// `tokio_postgres::Error::fmt` just prints "db error" for database errors —
/// the actual message (column name, constraint, syntax detail) lives in the
/// `source()` chain.  This helper walks the chain so callers see the real
/// Postgres error instead of the opaque "db error" string.
fn pg_error_string(e: &tokio_postgres::Error) -> String {
    use std::error::Error;
    let mut msg = e.to_string();
    let mut source = e.source();
    while let Some(cause) = source {
        msg.push_str(": ");
        msg.push_str(&cause.to_string());
        source = cause.source();
    }
    msg
}

/// Host-side state for an active WIT `transaction` resource.
///
/// Holds the dedicated pooled connection for the duration of the transaction.
/// `done` is set to `true` after `commit` or `rollback` so the `drop` handler
/// does not issue a redundant ROLLBACK.
pub struct TxState {
    client: deadpool_postgres::Object,
    done: bool,
}

/// Host-side state for an active WIT `row-cursor` resource.
///
/// Wraps a `tokio_postgres::RowStream` and optionally owns the pooled
/// connection that produced it (for non-transactional queries).  When the
/// cursor is created inside a transaction the connection is owned by `TxState`
/// instead, so `_conn` is `None`.
pub struct CursorState {
    stream: std::pin::Pin<Box<tokio_postgres::RowStream>>,
    /// Keeps the connection alive for non-transactional cursors.
    _conn: Option<deadpool_postgres::Object>,
    done: bool,
}

wasmtime::component::bindgen!({
    path:               "../wit/db.wit",
    world:              "db-access",
    additional_derives: [PartialEq],
    with: {
        "wruntime:db/database.transaction": TxState,
        "wruntime:db/database.row-cursor":  CursorState,
    },
    imports: { default: async },
});

use wruntime::db::database::{
    Column, DbError, Host, HostRowCursor, HostTransaction, PgInterval, PgValue, Row,
};

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
    Timestamp(chrono::NaiveDateTime),
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
    Interval(PgIntervalRaw),
    Numeric(rust_decimal::Decimal),
    Uuid(uuid::Uuid),
    Jsonb(serde_json::Value),
    Oid(u32),
    BoolArray(Vec<Option<bool>>),
    Int2Array(Vec<Option<i16>>),
    Int4Array(Vec<Option<i32>>),
    Int8Array(Vec<Option<i64>>),
    Float4Array(Vec<Option<f32>>),
    Float8Array(Vec<Option<f64>>),
    TextArray(Vec<Option<String>>),
    TimestamptzArray(Vec<Option<chrono::DateTime<chrono::Utc>>>),
    TimestampArray(Vec<Option<chrono::NaiveDateTime>>),
    UuidArray(Vec<Option<uuid::Uuid>>),
    JsonbArray(Vec<Option<serde_json::Value>>),
}

/// Raw Postgres INTERVAL: 8-byte microseconds + 4-byte days + 4-byte months
/// (big-endian on the wire).  Implements `ToSql`/`FromSql` directly because
/// `tokio-postgres` has no built-in mapping for INTERVAL.
#[derive(Debug, Clone, PartialEq)]
struct PgIntervalRaw {
    microseconds: i64,
    days: i32,
    months: i32,
}

impl tokio_postgres::types::FromSql<'_> for PgIntervalRaw {
    fn from_sql(
        _ty: &tokio_postgres::types::Type,
        raw: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if raw.len() != 16 {
            return Err("invalid INTERVAL length".into());
        }
        let microseconds = i64::from_be_bytes(raw[0..8].try_into().unwrap());
        let days = i32::from_be_bytes(raw[8..12].try_into().unwrap());
        let months = i32::from_be_bytes(raw[12..16].try_into().unwrap());
        Ok(PgIntervalRaw {
            microseconds,
            days,
            months,
        })
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        *ty == tokio_postgres::types::Type::INTERVAL
    }
}

impl tokio_postgres::types::ToSql for PgIntervalRaw {
    fn to_sql(
        &self,
        _ty: &tokio_postgres::types::Type,
        buf: &mut bytes::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        use bytes::BufMut;
        buf.put_i64(self.microseconds);
        buf.put_i32(self.days);
        buf.put_i32(self.months);
        Ok(tokio_postgres::types::IsNull::No)
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        *ty == tokio_postgres::types::Type::INTERVAL
    }

    tokio_postgres::types::to_sql_checked!();
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
            PgValue::Timestamp(micros) => {
                let dt = chrono::DateTime::from_timestamp_micros(micros)
                    .unwrap_or(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH)
                    .naive_utc();
                PgParam::Timestamp(dt)
            }
            PgValue::Interval(iv) => PgParam::Interval(PgIntervalRaw {
                microseconds: iv.microseconds,
                days: iv.days,
                months: iv.months,
            }),
            PgValue::Jsonb(s) => {
                PgParam::Jsonb(serde_json::from_str(&s).unwrap_or(serde_json::Value::Null))
            }
            PgValue::Oid(o) => PgParam::Oid(o),
            PgValue::BoolArray(a) => PgParam::BoolArray(a),
            PgValue::Int2Array(a) => PgParam::Int2Array(a),
            PgValue::Int4Array(a) => PgParam::Int4Array(a),
            PgValue::Int8Array(a) => PgParam::Int8Array(a),
            PgValue::Float4Array(a) => PgParam::Float4Array(a),
            PgValue::Float8Array(a) => PgParam::Float8Array(a),
            PgValue::TextArray(a) => PgParam::TextArray(a),
            PgValue::TimestamptzArray(a) => PgParam::TimestamptzArray(
                a.into_iter()
                    .map(|o| o.and_then(chrono::DateTime::from_timestamp_micros))
                    .collect(),
            ),
            PgValue::TimestampArray(a) => PgParam::TimestampArray(
                a.into_iter()
                    .map(|o| {
                        o.and_then(|micros| {
                            chrono::DateTime::from_timestamp_micros(micros).map(|dt| dt.naive_utc())
                        })
                    })
                    .collect(),
            ),
            PgValue::UuidArray(a) => PgParam::UuidArray(
                a.into_iter()
                    .map(|o| {
                        o.map(|(hi, lo)| uuid::Uuid::from_u128((hi as u128) << 64 | lo as u128))
                    })
                    .collect(),
            ),
            PgValue::JsonbArray(a) => PgParam::JsonbArray(
                a.into_iter()
                    .map(|o| o.map(|s| serde_json::from_str(&s).unwrap_or(serde_json::Value::Null)))
                    .collect(),
            ),
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
            PgParam::Timestamp(v) => v.to_sql(ty, buf),
            PgParam::Interval(v) => v.to_sql(ty, buf),
            PgParam::Jsonb(v) => v.to_sql(ty, buf),
            PgParam::Oid(v) => v.to_sql(ty, buf),
            PgParam::BoolArray(v) => v.to_sql(ty, buf),
            PgParam::Int2Array(v) => v.to_sql(ty, buf),
            PgParam::Int4Array(v) => v.to_sql(ty, buf),
            PgParam::Int8Array(v) => v.to_sql(ty, buf),
            PgParam::Float4Array(v) => v.to_sql(ty, buf),
            PgParam::Float8Array(v) => v.to_sql(ty, buf),
            PgParam::TextArray(v) => v.to_sql(ty, buf),
            PgParam::TimestamptzArray(v) => v.to_sql(ty, buf),
            PgParam::TimestampArray(v) => v.to_sql(ty, buf),
            PgParam::UuidArray(v) => v.to_sql(ty, buf),
            PgParam::JsonbArray(v) => v.to_sql(ty, buf),
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
    fn query(
        &mut self,
        sql: String,
        params: Vec<PgValue>,
    ) -> impl std::future::Future<Output = Result<Vec<Row>, DbError>> + Send {
        let pool = match &self.db_pool {
            Some(p) => p.clone(),
            None => {
                return futures::future::Either::Left(std::future::ready(Err(DbError::Connection(
                    "no database configured for this module".into(),
                ))))
            }
        };
        let schema = self.db_schema.clone();
        futures::future::Either::Right(async move {
            let client = pool
                .get()
                .await
                .map_err(|e| DbError::Connection(e.to_string()))?;
            if let Some(s) = &schema {
                client
                    .execute(
                        &format!("SET search_path = \"{s}\""),
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
                .map_err(|e| DbError::Query(pg_error_string(&e)))?;
            Ok(rows.iter().map(pg_row_to_wit).collect())
        })
    }

    fn execute(
        &mut self,
        sql: String,
        params: Vec<PgValue>,
    ) -> impl std::future::Future<Output = Result<u64, DbError>> + Send {
        let pool = match &self.db_pool {
            Some(p) => p.clone(),
            None => {
                return futures::future::Either::Left(std::future::ready(Err(DbError::Connection(
                    "no database configured for this module".into(),
                ))))
            }
        };
        let schema = self.db_schema.clone();
        futures::future::Either::Right(async move {
            let client = pool
                .get()
                .await
                .map_err(|e| DbError::Connection(e.to_string()))?;
            if let Some(s) = &schema {
                client
                    .execute(
                        &format!("SET search_path = \"{s}\""),
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
                .map_err(|e| DbError::Query(pg_error_string(&e)))
        })
    }

    async fn query_stream(
        &mut self,
        sql: String,
        params: Vec<PgValue>,
    ) -> Result<Resource<CursorState>, DbError> {
        let pool = match &self.db_pool {
            Some(p) => p.clone(),
            None => {
                return Err(DbError::Connection(
                    "no database configured for this module".into(),
                ))
            }
        };
        let schema = self.db_schema.clone();
        let client = pool
            .get()
            .await
            .map_err(|e| DbError::Connection(e.to_string()))?;
        if let Some(s) = &schema {
            client
                .execute(
                    &format!("SET search_path = \"{s}\""),
                    &[] as &[&(dyn tokio_postgres::types::ToSql + Sync)],
                )
                .await
                .map_err(|e| DbError::Connection(e.to_string()))?;
        }
        let pg_params: Vec<PgParam> = params.into_iter().map(PgParam::from).collect();
        let params_ref: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            pg_params.iter().map(|p| p as _).collect();
        let stream = client
            .query_raw(sql.as_str(), params_ref)
            .await
            .map_err(|e| DbError::Query(pg_error_string(&e)))?;
        self.table()
            .push(CursorState {
                stream: Box::pin(stream),
                _conn: Some(client),
                done: false,
            })
            .map_err(|e| DbError::Connection(e.to_string()))
    }

    async fn begin_transaction(&mut self) -> Result<Resource<TxState>, DbError> {
        let pool = match &self.db_pool {
            Some(p) => p.clone(),
            None => {
                return Err(DbError::Connection(
                    "no database configured for this module".into(),
                ))
            }
        };
        let schema = self.db_schema.clone();
        let client = async move {
            let client = pool
                .get()
                .await
                .map_err(|e| DbError::Connection(e.to_string()))?;
            client
                .execute("BEGIN", &[])
                .await
                .map_err(|e| DbError::Query(pg_error_string(&e)))?;
            if let Some(s) = &schema {
                client
                    .execute(
                        &format!("SET search_path = \"{s}\""),
                        &[] as &[&(dyn tokio_postgres::types::ToSql + Sync)],
                    )
                    .await
                    .map_err(|e| DbError::Connection(e.to_string()))?;
            }
            Ok::<_, DbError>(client)
        }
        .await?;
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
        let pg_params: Vec<PgParam> = params.into_iter().map(PgParam::from).collect();
        let params_ref: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            pg_params.iter().map(|p| p as _).collect();
        let rows = state
            .client
            .query(sql.as_str(), &params_ref)
            .await
            .map_err(|e| DbError::Query(pg_error_string(&e)))?;
        Ok(rows.iter().map(pg_row_to_wit).collect())
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
        let pg_params: Vec<PgParam> = params.into_iter().map(PgParam::from).collect();
        let params_ref: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            pg_params.iter().map(|p| p as _).collect();
        state
            .client
            .execute(sql.as_str(), &params_ref)
            .await
            .map_err(|e| DbError::Query(pg_error_string(&e)))
    }

    async fn query_stream(
        &mut self,
        self_: Resource<TxState>,
        sql: String,
        params: Vec<PgValue>,
    ) -> Result<Resource<CursorState>, DbError> {
        let state = self
            .table()
            .get(&self_)
            .map_err(|e| DbError::Connection(e.to_string()))?;
        let pg_params: Vec<PgParam> = params.into_iter().map(PgParam::from).collect();
        let params_ref: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            pg_params.iter().map(|p| p as _).collect();
        let stream = state
            .client
            .query_raw(sql.as_str(), params_ref)
            .await
            .map_err(|e| DbError::Query(pg_error_string(&e)))?;
        self.table()
            .push(CursorState {
                stream: Box::pin(stream),
                _conn: None, // connection owned by TxState
                done: false,
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

// ── HostRowCursor implementation ─────────────────────────────────────────

impl HostRowCursor for ModuleState {
    async fn next_batch(
        &mut self,
        self_: Resource<CursorState>,
        max: u32,
    ) -> Result<Vec<Row>, DbError> {
        let cursor = self
            .table()
            .get_mut(&self_)
            .map_err(|e| DbError::Connection(e.to_string()))?;
        if cursor.done {
            return Ok(vec![]);
        }
        let mut rows = Vec::with_capacity(max.min(256) as usize);
        for _ in 0..max {
            match cursor.stream.next().await {
                Some(Ok(pg_row)) => rows.push(pg_row_to_wit(&pg_row)),
                Some(Err(e)) => return Err(DbError::Query(e.to_string())),
                None => {
                    cursor.done = true;
                    break;
                }
            }
        }
        Ok(rows)
    }

    async fn drop(&mut self, rep: Resource<CursorState>) -> wasmtime::Result<()> {
        self.table().delete(rep)?;
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
        Type::TIMESTAMP => opt(row.get::<_, Option<chrono::NaiveDateTime>>(i), |dt| {
            PgValue::Timestamp(dt.and_utc().timestamp_micros())
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
        Type::INTERVAL => opt(row.get::<_, Option<PgIntervalRaw>>(i), |iv| {
            PgValue::Interval(PgInterval {
                months: iv.months,
                days: iv.days,
                microseconds: iv.microseconds,
            })
        }),
        Type::OID => opt(row.get::<_, Option<u32>>(i), PgValue::Oid),
        Type::BOOL_ARRAY => opt(
            row.get::<_, Option<Vec<Option<bool>>>>(i),
            PgValue::BoolArray,
        ),
        Type::INT2_ARRAY => opt(
            row.get::<_, Option<Vec<Option<i16>>>>(i),
            PgValue::Int2Array,
        ),
        Type::INT4_ARRAY => opt(
            row.get::<_, Option<Vec<Option<i32>>>>(i),
            PgValue::Int4Array,
        ),
        Type::INT8_ARRAY => opt(
            row.get::<_, Option<Vec<Option<i64>>>>(i),
            PgValue::Int8Array,
        ),
        Type::FLOAT4_ARRAY => opt(
            row.get::<_, Option<Vec<Option<f32>>>>(i),
            PgValue::Float4Array,
        ),
        Type::FLOAT8_ARRAY => opt(
            row.get::<_, Option<Vec<Option<f64>>>>(i),
            PgValue::Float8Array,
        ),
        Type::TEXT_ARRAY | Type::VARCHAR_ARRAY => opt(
            row.get::<_, Option<Vec<Option<String>>>>(i),
            PgValue::TextArray,
        ),
        Type::TIMESTAMPTZ_ARRAY => opt(
            row.get::<_, Option<Vec<Option<chrono::DateTime<chrono::Utc>>>>>(i),
            |arr| {
                PgValue::TimestamptzArray(
                    arr.into_iter()
                        .map(|o| o.map(|dt| dt.timestamp_micros()))
                        .collect(),
                )
            },
        ),
        Type::TIMESTAMP_ARRAY => opt(
            row.get::<_, Option<Vec<Option<chrono::NaiveDateTime>>>>(i),
            |arr| {
                PgValue::TimestampArray(
                    arr.into_iter()
                        .map(|o| o.map(|dt| dt.and_utc().timestamp_micros()))
                        .collect(),
                )
            },
        ),
        Type::UUID_ARRAY => opt(row.get::<_, Option<Vec<Option<uuid::Uuid>>>>(i), |arr| {
            PgValue::UuidArray(
                arr.into_iter()
                    .map(|o| {
                        o.map(|u| {
                            let n = u.as_u128();
                            ((n >> 64) as u64, n as u64)
                        })
                    })
                    .collect(),
            )
        }),
        Type::JSON_ARRAY | Type::JSONB_ARRAY => opt(
            row.get::<_, Option<Vec<Option<serde_json::Value>>>>(i),
            |arr| PgValue::JsonbArray(arr.into_iter().map(|o| o.map(|v| v.to_string())).collect()),
        ),
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

    use super::wruntime::db::database::{DbError, Host, HostRowCursor, PgInterval, PgValue};
    use crate::state::{ModuleServices, ModuleState};

    fn proxy_uri() -> hyper::Uri {
        "http://127.0.0.1:9001".parse().unwrap()
    }

    fn test_http_client() -> hyper_util::client::legacy::Client<
        hyper_util::client::legacy::connect::HttpConnector,
        http_body_util::Full<bytes::Bytes>,
    > {
        hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
            .build_http()
    }

    // ── no-pool tests ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_query_returns_error_when_no_pool() {
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            test_http_client(),
            Default::default(),
        )
        .expect("state");
        let result = state.query("SELECT 1".into(), vec![]).await;
        assert!(
            matches!(result, Err(DbError::Connection(_))),
            "expected Connection error, got {result:?}",
        );
    }

    #[tokio::test]
    async fn test_execute_returns_error_when_no_pool() {
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            test_http_client(),
            Default::default(),
        )
        .expect("state");
        let result = state.execute("SELECT 1".into(), vec![]).await;
        assert!(
            matches!(result, Err(DbError::Connection(_))),
            "expected Connection error, got {result:?}",
        );
    }

    #[tokio::test]
    async fn test_begin_transaction_returns_error_when_no_pool() {
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            test_http_client(),
            Default::default(),
        )
        .expect("state");
        let result = state.begin_transaction().await;
        assert!(
            matches!(result, Err(DbError::Connection(_))),
            "expected Connection error, got {result:?}",
        );
    }

    #[tokio::test]
    async fn test_query_stream_returns_error_when_no_pool() {
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            test_http_client(),
            Default::default(),
        )
        .expect("state");
        let result = state.query_stream("SELECT 1".into(), vec![]).await;
        assert!(
            matches!(result, Err(DbError::Connection(_))),
            "expected Connection error, got {result:?}",
        );
    }

    // ── real-Postgres tests ───────────────────────────────────────────────────

    /// Skip the test if `WRT_TEST_DB_URL` is not set.
    fn db_url() -> Option<String> {
        std::env::var("WRT_TEST_DB_URL").ok()
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
            test_http_client(),
            ModuleServices {
                db_pool: Some(Arc::new(pool)),
                ..Default::default()
            },
        )
        .expect("state");

        let rows = state
            .query(
                "SELECT $1::text AS echo".into(),
                vec![PgValue::Text("hello".into())],
            )
            .await
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
            test_http_client(),
            ModuleServices {
                db_pool: Some(Arc::new(pool)),
                ..Default::default()
            },
        )
        .expect("state");

        // DDL returns 0 rows affected.
        let n = state
            .execute("CREATE TEMP TABLE _wr_db_test (id INT)".into(), vec![])
            .await
            .expect("create table");
        assert_eq!(n, 0);

        // DML returns the actual affected-row count.
        let n = state
            .execute("INSERT INTO _wr_db_test VALUES (1)".into(), vec![])
            .await
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
            test_http_client(),
            ModuleServices {
                db_pool: Some(Arc::new(pool)),
                ..Default::default()
            },
        )
        .expect("state");

        let rows = state
            .query(
                "SELECT $1::text AS a, $2::text AS b".into(),
                vec![PgValue::Text("foo".into()), PgValue::Text("bar".into())],
            )
            .await
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
            test_http_client(),
            ModuleServices {
                db_pool: Some(Arc::new(pool)),
                ..Default::default()
            },
        )
        .expect("state");

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
            .await
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
            test_http_client(),
            ModuleServices {
                db_pool: Some(Arc::new(pool)),
                ..Default::default()
            },
        )
        .expect("state");

        // Setup: create a temp table outside the transaction.
        Host::execute(
            &mut state,
            "CREATE TEMP TABLE _wr_tx_commit_test (val INT)".into(),
            vec![],
        )
        .await
        .expect("create table");

        let tx = state.begin_transaction().await.expect("begin");
        let rep = tx.rep();

        HostTransaction::execute(
            &mut state,
            wasmtime::component::Resource::new_borrow(rep),
            "INSERT INTO _wr_tx_commit_test VALUES (42)".into(),
            vec![],
        )
        .await
        .expect("insert");

        HostTransaction::commit(&mut state, wasmtime::component::Resource::new_borrow(rep))
            .await
            .expect("commit");

        // Release the resource first so its connection is returned to the pool.
        // done=true means no ROLLBACK is issued.
        HostTransaction::drop(&mut state, tx).await.expect("drop");

        // After the connection is back in the pool, Host::query reacquires it
        // and can see the TEMP TABLE (TEMP tables are connection-scoped).
        let rows = Host::query(
            &mut state,
            "SELECT val FROM _wr_tx_commit_test".into(),
            vec![],
        )
        .await
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
            test_http_client(),
            ModuleServices {
                db_pool: Some(Arc::new(pool)),
                ..Default::default()
            },
        )
        .expect("state");

        Host::execute(
            &mut state,
            "CREATE TEMP TABLE _wr_tx_rollback_test (val INT)".into(),
            vec![],
        )
        .await
        .expect("create table");

        let tx = state.begin_transaction().await.expect("begin");
        let rep = tx.rep();

        HostTransaction::execute(
            &mut state,
            wasmtime::component::Resource::new_borrow(rep),
            "INSERT INTO _wr_tx_rollback_test VALUES (99)".into(),
            vec![],
        )
        .await
        .expect("insert");

        HostTransaction::rollback(&mut state, wasmtime::component::Resource::new_borrow(rep))
            .await
            .expect("rollback");

        // Release the resource first so its connection is returned to the pool.
        HostTransaction::drop(&mut state, tx).await.expect("drop");

        // After the connection is back in the pool, Host::query reacquires it
        // and can see the TEMP TABLE with the rolled-back INSERT absent.
        let rows = Host::query(
            &mut state,
            "SELECT val FROM _wr_tx_rollback_test".into(),
            vec![],
        )
        .await
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
            test_http_client(),
            ModuleServices {
                db_pool: Some(Arc::new(pool)),
                ..Default::default()
            },
        )
        .expect("state");

        Host::execute(
            &mut state,
            "CREATE TEMP TABLE _wr_tx_drop_test (val INT)".into(),
            vec![],
        )
        .await
        .expect("create table");

        let tx = state.begin_transaction().await.expect("begin");
        let rep = tx.rep();

        HostTransaction::execute(
            &mut state,
            wasmtime::component::Resource::new_borrow(rep),
            "INSERT INTO _wr_tx_drop_test VALUES (7)".into(),
            vec![],
        )
        .await
        .expect("insert");

        // Drop without committing — host must issue implicit ROLLBACK.
        HostTransaction::drop(&mut state, tx).await.expect("drop");

        let rows = Host::query(
            &mut state,
            "SELECT val FROM _wr_tx_drop_test".into(),
            vec![],
        )
        .await
        .expect("query after implicit rollback");
        assert_eq!(rows.len(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_stream_with_postgres() {
        let url = match db_url() {
            Some(u) => u,
            None => return,
        };

        let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            test_http_client(),
            ModuleServices {
                db_pool: Some(Arc::new(pool)),
                ..Default::default()
            },
        )
        .expect("state");

        let cursor = state
            .query_stream("SELECT generate_series(1, 5) AS n".into(), vec![])
            .await
            .expect("query_stream");
        let rep = cursor.rep();

        // Fetch in batches of 2
        let batch1 = HostRowCursor::next_batch(
            &mut state,
            wasmtime::component::Resource::new_borrow(rep),
            2,
        )
        .await
        .expect("batch1");
        assert_eq!(batch1.len(), 2);

        let batch2 = HostRowCursor::next_batch(
            &mut state,
            wasmtime::component::Resource::new_borrow(rep),
            2,
        )
        .await
        .expect("batch2");
        assert_eq!(batch2.len(), 2);

        let batch3 = HostRowCursor::next_batch(
            &mut state,
            wasmtime::component::Resource::new_borrow(rep),
            2,
        )
        .await
        .expect("batch3");
        assert_eq!(batch3.len(), 1);

        // Stream exhausted
        let batch4 = HostRowCursor::next_batch(
            &mut state,
            wasmtime::component::Resource::new_borrow(rep),
            2,
        )
        .await
        .expect("batch4");
        assert!(batch4.is_empty());

        HostRowCursor::drop(&mut state, cursor).await.expect("drop");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_stream_drop_mid_iteration() {
        let url = match db_url() {
            Some(u) => u,
            None => return,
        };

        let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            test_http_client(),
            ModuleServices {
                db_pool: Some(Arc::new(pool)),
                ..Default::default()
            },
        )
        .expect("state");

        let cursor = state
            .query_stream("SELECT generate_series(1, 100) AS n".into(), vec![])
            .await
            .expect("query_stream");
        let rep = cursor.rep();

        // Fetch only the first batch, then drop
        let batch = HostRowCursor::next_batch(
            &mut state,
            wasmtime::component::Resource::new_borrow(rep),
            5,
        )
        .await
        .expect("batch");
        assert_eq!(batch.len(), 5);

        HostRowCursor::drop(&mut state, cursor).await.expect("drop");

        // Verify the connection is usable again by running another query
        let rows = state
            .query("SELECT 1 AS ok".into(), vec![])
            .await
            .expect("query after drop");
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_stream_in_transaction() {
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
            test_http_client(),
            ModuleServices {
                db_pool: Some(Arc::new(pool)),
                ..Default::default()
            },
        )
        .expect("state");

        let tx = state.begin_transaction().await.expect("begin");
        let tx_rep = tx.rep();

        let cursor = HostTransaction::query_stream(
            &mut state,
            wasmtime::component::Resource::new_borrow(tx_rep),
            "SELECT generate_series(1, 3) AS n".into(),
            vec![],
        )
        .await
        .expect("query_stream in tx");
        let cursor_rep = cursor.rep();

        let batch = HostRowCursor::next_batch(
            &mut state,
            wasmtime::component::Resource::new_borrow(cursor_rep),
            10,
        )
        .await
        .expect("batch");
        assert_eq!(batch.len(), 3);

        // Drain the cursor
        let empty = HostRowCursor::next_batch(
            &mut state,
            wasmtime::component::Resource::new_borrow(cursor_rep),
            10,
        )
        .await
        .expect("empty");
        assert!(empty.is_empty());

        HostRowCursor::drop(&mut state, cursor)
            .await
            .expect("drop cursor");

        HostTransaction::commit(
            &mut state,
            wasmtime::component::Resource::new_borrow(tx_rep),
        )
        .await
        .expect("commit");
        HostTransaction::drop(&mut state, tx)
            .await
            .expect("drop tx");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_naive_timestamp_with_postgres() {
        let url = match db_url() {
            Some(u) => u,
            None => return,
        };

        let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            test_http_client(),
            ModuleServices {
                db_pool: Some(Arc::new(pool)),
                ..Default::default()
            },
        )
        .expect("state");

        // Use epoch to avoid timezone ambiguity.
        let rows = state
            .query(
                "SELECT '2000-01-01 00:00:00'::timestamp AS ts".into(),
                vec![],
            )
            .await
            .expect("query");

        assert_eq!(rows.len(), 1);
        // Should be Timestamp, not Timestamptz
        match &rows[0].columns[0].value {
            PgValue::Timestamp(micros) => {
                // 2000-01-01 00:00:00 UTC = 946684800 seconds since Unix epoch
                assert_eq!(*micros, 946_684_800_000_000);
            }
            other => panic!("expected Timestamp, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_naive_timestamp_param_roundtrip() {
        let url = match db_url() {
            Some(u) => u,
            None => return,
        };

        let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            test_http_client(),
            ModuleServices {
                db_pool: Some(Arc::new(pool)),
                ..Default::default()
            },
        )
        .expect("state");

        let micros: i64 = 1_718_451_000_000_000;
        let rows = state
            .query(
                "SELECT $1::timestamp AS ts".into(),
                vec![PgValue::Timestamp(micros)],
            )
            .await
            .expect("query");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].columns[0].value, PgValue::Timestamp(micros));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_interval_with_postgres() {
        let url = match db_url() {
            Some(u) => u,
            None => return,
        };

        let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            test_http_client(),
            ModuleServices {
                db_pool: Some(Arc::new(pool)),
                ..Default::default()
            },
        )
        .expect("state");

        let rows = state
            .query(
                "SELECT '1 year 2 months 3 days 4 hours 5 minutes 6 seconds'::interval AS iv"
                    .into(),
                vec![],
            )
            .await
            .expect("query");

        assert_eq!(rows.len(), 1);
        match &rows[0].columns[0].value {
            PgValue::Interval(iv) => {
                assert_eq!(iv.months, 14); // 1 year + 2 months
                assert_eq!(iv.days, 3);
                // 4h5m6s = 14706 seconds = 14706000000 microseconds
                assert_eq!(iv.microseconds, 14_706_000_000);
            }
            other => panic!("expected Interval, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_interval_param_roundtrip() {
        let url = match db_url() {
            Some(u) => u,
            None => return,
        };

        let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            test_http_client(),
            ModuleServices {
                db_pool: Some(Arc::new(pool)),
                ..Default::default()
            },
        )
        .expect("state");

        let iv = PgInterval {
            months: 14,
            days: 3,
            microseconds: 14_706_000_000,
        };
        let rows = state
            .query(
                "SELECT $1::interval AS iv".into(),
                vec![PgValue::Interval(iv)],
            )
            .await
            .expect("query");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].columns[0].value, PgValue::Interval(iv));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_int4_array_with_postgres() {
        let url = match db_url() {
            Some(u) => u,
            None => return,
        };

        let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            test_http_client(),
            ModuleServices {
                db_pool: Some(Arc::new(pool)),
                ..Default::default()
            },
        )
        .expect("state");

        let rows = state
            .query("SELECT ARRAY[1, 2, NULL, 4]::int4[] AS arr".into(), vec![])
            .await
            .expect("query");

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].columns[0].value,
            PgValue::Int4Array(vec![Some(1), Some(2), None, Some(4)])
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_text_array_with_postgres() {
        let url = match db_url() {
            Some(u) => u,
            None => return,
        };

        let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            test_http_client(),
            ModuleServices {
                db_pool: Some(Arc::new(pool)),
                ..Default::default()
            },
        )
        .expect("state");

        let rows = state
            .query(
                "SELECT ARRAY['hello', NULL, 'world']::text[] AS arr".into(),
                vec![],
            )
            .await
            .expect("query");

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].columns[0].value,
            PgValue::TextArray(vec![Some("hello".into()), None, Some("world".into()),])
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_array_param_roundtrip() {
        let url = match db_url() {
            Some(u) => u,
            None => return,
        };

        let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            test_http_client(),
            ModuleServices {
                db_pool: Some(Arc::new(pool)),
                ..Default::default()
            },
        )
        .expect("state");

        let arr = vec![Some(10), None, Some(30)];
        let rows = state
            .query(
                "SELECT $1::int4[] AS arr".into(),
                vec![PgValue::Int4Array(arr.clone())],
            )
            .await
            .expect("query");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].columns[0].value, PgValue::Int4Array(arr));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_array_any_query() {
        let url = match db_url() {
            Some(u) => u,
            None => return,
        };

        let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            test_http_client(),
            ModuleServices {
                db_pool: Some(Arc::new(pool)),
                ..Default::default()
            },
        )
        .expect("state");

        // Common pattern: WHERE id = ANY($1::int4[])
        let rows = state
            .query(
                "SELECT unnest($1::int4[]) AS n".into(),
                vec![PgValue::Int4Array(vec![Some(1), Some(2), Some(3)])],
            )
            .await
            .expect("query");

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].columns[0].value, PgValue::Int4(1));
        assert_eq!(rows[1].columns[0].value, PgValue::Int4(2));
        assert_eq!(rows[2].columns[0].value, PgValue::Int4(3));
    }
}
