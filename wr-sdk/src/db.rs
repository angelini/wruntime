use std::cell::Cell;

use crate::bindings::wruntime::db::database::{self, DbError, PgValue, Row, Transaction};
use crate::bindings::wruntime::tracing::span::ActiveSpan;
use crate::ServiceError;

// ── Optional DB tracing ────────────────────────────────────────────────────

thread_local! {
    static DB_TRACING: Cell<bool> = const { Cell::new(false) };
}

/// Enable automatic tracing spans for all `db::*` helpers and `TxGuard` methods.
/// Call once before first DB use; `ServiceGuest::init()` is preferred.
pub fn enable_tracing() {
    DB_TRACING.with(|c| c.set(true));
}

fn tracing_enabled() -> bool {
    DB_TRACING.with(|c| c.get())
}

/// Internal span wrapper — `None` when tracing is disabled, avoiding host calls.
struct DbSpan(Option<ActiveSpan>);

impl DbSpan {
    fn start(operation: &str, sql: &str, param_count: usize) -> Self {
        if !tracing_enabled() {
            return Self(None);
        }
        let span = crate::tracing::start_owned(
            &format!("db.{operation}"),
            vec![
                ("db.operation".into(), operation.into()),
                ("db.statement".into(), sql.into()),
                ("db.params.count".into(), param_count.to_string()),
            ],
        );
        Self(Some(span))
    }

    fn set_rows(&self, count: usize) {
        if let Some(s) = &self.0 {
            crate::tracing::set_attr(s, "db.rows", count);
        }
    }

    fn set_rows_affected(&self, count: u64) {
        if let Some(s) = &self.0 {
            crate::tracing::set_attr(s, "db.rows_affected", count);
        }
    }

    fn set_error(&self, msg: &str) {
        if let Some(s) = &self.0 {
            crate::tracing::set_error(s, msg);
        }
    }
}

// ── FromPgValue trait ───────────────────────────────────────────────────────

/// Trait for types extractable from a `PgValue` column.
pub trait FromPgValue: Sized {
    fn from_pg(col: usize, val: &PgValue) -> Result<Self, ServiceError>;
}

impl FromPgValue for i64 {
    fn from_pg(col: usize, val: &PgValue) -> Result<Self, ServiceError> {
        match val {
            PgValue::Int8(v) => Ok(*v),
            other => Err(type_err(col, "int8", other)),
        }
    }
}

impl FromPgValue for i32 {
    fn from_pg(col: usize, val: &PgValue) -> Result<Self, ServiceError> {
        match val {
            PgValue::Int4(v) => Ok(*v),
            other => Err(type_err(col, "int4", other)),
        }
    }
}

impl FromPgValue for String {
    fn from_pg(col: usize, val: &PgValue) -> Result<Self, ServiceError> {
        match val {
            PgValue::Text(s) => Ok(s.clone()),
            PgValue::Jsonb(s) => Ok(s.clone()),
            other => Err(type_err(col, "text", other)),
        }
    }
}

impl FromPgValue for bool {
    fn from_pg(col: usize, val: &PgValue) -> Result<Self, ServiceError> {
        match val {
            PgValue::Boolean(v) => Ok(*v),
            other => Err(type_err(col, "boolean", other)),
        }
    }
}

impl FromPgValue for f64 {
    fn from_pg(col: usize, val: &PgValue) -> Result<Self, ServiceError> {
        match val {
            PgValue::Float8(v) => Ok(*v),
            other => Err(type_err(col, "float8", other)),
        }
    }
}

// ── Row helpers ─────────────────────────────────────────────────────────────

impl Row {
    /// Extract a typed column by index using the `FromPgValue` trait.
    pub fn get<T: FromPgValue>(&self, col: usize) -> Result<T, ServiceError> {
        let column = self.columns.get(col).ok_or_else(|| col_err(col))?;
        T::from_pg(col, &column.value)
    }

    /// Extract a `TEXT` column by index as a borrowed `&str`.
    pub fn get_text(&self, col: usize) -> Result<&str, ServiceError> {
        match &self.columns.get(col).ok_or_else(|| col_err(col))?.value {
            PgValue::Text(s) => Ok(s),
            other => Err(type_err(col, "text", other)),
        }
    }

    /// Extract a `BIGINT` / `INT8` column by index.
    pub fn get_i64(&self, col: usize) -> Result<i64, ServiceError> {
        self.get(col)
    }

    /// Extract an `INTEGER` / `INT4` column by index.
    pub fn get_i32(&self, col: usize) -> Result<i32, ServiceError> {
        self.get(col)
    }

    /// Extract a `BOOLEAN` column by index.
    pub fn get_bool(&self, col: usize) -> Result<bool, ServiceError> {
        self.get(col)
    }

    /// Extract a `FLOAT8` / `DOUBLE PRECISION` column by index.
    pub fn get_f64(&self, col: usize) -> Result<f64, ServiceError> {
        self.get(col)
    }

    /// Extract a `JSONB` / `JSON` column by index as a borrowed `&str`.
    pub fn get_jsonb(&self, col: usize) -> Result<&str, ServiceError> {
        match &self.columns.get(col).ok_or_else(|| col_err(col))?.value {
            PgValue::Jsonb(s) => Ok(s),
            other => Err(type_err(col, "jsonb", other)),
        }
    }
}

// ── Tuple extraction ────────────────────────────────────────────────────────

/// Extract multiple typed columns from a row in one call.
///
/// ```rust,ignore
/// let (trade_id, buyer, seller, qty, price): (i64, String, String, i64, i64) =
///     row.unpack()?;
/// ```
pub trait UnpackRow<T> {
    fn unpack(&self) -> Result<T, ServiceError>;
}

macro_rules! impl_unpack {
    ($($idx:tt: $T:ident),+) => {
        impl<$($T: FromPgValue),+> UnpackRow<($($T,)+)> for Row {
            fn unpack(&self) -> Result<($($T,)+), ServiceError> {
                Ok(($($T::from_pg($idx, &self.columns.get($idx).ok_or_else(|| col_err($idx))?.value)?,)+))
            }
        }
    };
}

impl_unpack!(0: A, 1: B);
impl_unpack!(0: A, 1: B, 2: C);
impl_unpack!(0: A, 1: B, 2: C, 3: D);
impl_unpack!(0: A, 1: B, 2: C, 3: D, 4: E);
impl_unpack!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F);
impl_unpack!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G);
impl_unpack!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G, 7: H);

// ── Convenience query helpers ──────────────────────────────────────────────

/// Execute a query and return a single scalar value from the first row / first column.
///
/// Returns a `not_found` error if the query returns no rows.
///
/// ```rust,ignore
/// let count: i64 = db::query_scalar("SELECT COUNT(*) FROM trades", &[])?;
/// ```
pub fn query_scalar<T: FromPgValue>(sql: &str, params: &[PgValue]) -> Result<T, ServiceError> {
    let span = DbSpan::start("query", sql, params.len());
    let rows = match database::query(sql, params) {
        Ok(r) => r,
        Err(e) => {
            let se = ServiceError::from(e);
            span.set_error(&se.message);
            return Err(se);
        }
    };
    span.set_rows(rows.len());
    let row = rows
        .first()
        .ok_or_else(|| ServiceError::not_found("query returned no rows"))?;
    row.get(0)
}

/// Execute a query and unpack the first row into a tuple.
///
/// Returns a `not_found` error if the query returns no rows.
///
/// ```rust,ignore
/// let (id, name, stock): (i64, String, i64) =
///     db::query_one("SELECT id, name, stock FROM inventory WHERE id = $1", &[...])?;
/// ```
pub fn query_one<T>(sql: &str, params: &[PgValue]) -> Result<T, ServiceError>
where
    Row: UnpackRow<T>,
{
    let span = DbSpan::start("query", sql, params.len());
    let rows = match database::query(sql, params) {
        Ok(r) => r,
        Err(e) => {
            let se = ServiceError::from(e);
            span.set_error(&se.message);
            return Err(se);
        }
    };
    span.set_rows(rows.len());
    let row = rows
        .first()
        .ok_or_else(|| ServiceError::not_found("query returned no rows"))?;
    row.unpack()
}

fn col_err(col: usize) -> ServiceError {
    ServiceError::internal(format!("column {col} out of bounds"))
}

fn type_err(col: usize, expected: &str, got: &PgValue) -> ServiceError {
    ServiceError::internal(format!("column {col}: expected {expected}, got {got:?}"))
}

// ── From<DbError> for ServiceError ──────────────────────────────────────────

impl From<DbError> for ServiceError {
    fn from(e: DbError) -> Self {
        match e {
            DbError::Connection(msg) => ServiceError::internal(format!("db connection: {msg}")),
            DbError::Query(msg) => ServiceError::internal(format!("db query: {msg}")),
        }
    }
}

// ── Transaction guard ───────────────────────────────────────────────────────

/// A transaction wrapper that auto-rollbacks on drop unless committed.
///
/// ```rust,ignore
/// let tx = wr_sdk::db::transaction()?;
/// tx.query("SELECT ...", &[])?;
/// tx.execute("UPDATE ...", &[])?;
/// tx.commit()?; // consumes the guard — no rollback
/// ```
pub struct TxGuard {
    inner: Option<Transaction>,
    span: DbSpan,
}

/// Begin a transaction and return a guard that auto-rollbacks on drop.
pub fn transaction() -> Result<TxGuard, ServiceError> {
    let span = DbSpan::start("transaction", "BEGIN", 0);
    let tx = match database::begin_transaction() {
        Ok(t) => t,
        Err(e) => {
            let se = ServiceError::from(e);
            span.set_error(&se.message);
            return Err(se);
        }
    };
    Ok(TxGuard {
        inner: Some(tx),
        span,
    })
}

impl TxGuard {
    pub fn query(&self, sql: &str, params: &[PgValue]) -> Result<Vec<Row>, ServiceError> {
        let span = DbSpan::start("query", sql, params.len());
        match self.inner.as_ref().unwrap().query(sql, params) {
            Ok(rows) => {
                span.set_rows(rows.len());
                Ok(rows)
            }
            Err(e) => {
                let se = ServiceError::from(e);
                span.set_error(&se.message);
                Err(se)
            }
        }
    }

    pub fn execute(&self, sql: &str, params: &[PgValue]) -> Result<u64, ServiceError> {
        let span = DbSpan::start("execute", sql, params.len());
        match self.inner.as_ref().unwrap().execute(sql, params) {
            Ok(n) => {
                span.set_rows_affected(n);
                Ok(n)
            }
            Err(e) => {
                let se = ServiceError::from(e);
                span.set_error(&se.message);
                Err(se)
            }
        }
    }

    /// Execute a query and return a single scalar value from the first row / first column.
    pub fn query_scalar<T: FromPgValue>(
        &self,
        sql: &str,
        params: &[PgValue],
    ) -> Result<T, ServiceError> {
        let rows = self.query(sql, params)?;
        let row = rows
            .first()
            .ok_or_else(|| ServiceError::not_found("query returned no rows"))?;
        row.get(0)
    }

    /// Commit the transaction, consuming the guard.
    pub fn commit(mut self) -> Result<(), ServiceError> {
        match self.inner.take().unwrap().commit() {
            Ok(()) => Ok(()),
            Err(e) => {
                let se = ServiceError::from(e);
                self.span.set_error(&se.message);
                Err(se)
            }
        }
    }
}

impl Drop for TxGuard {
    fn drop(&mut self) {
        if let Some(tx) = self.inner.take() {
            self.span.set_error("transaction rolled back");
            let _ = tx.rollback();
        }
    }
}
