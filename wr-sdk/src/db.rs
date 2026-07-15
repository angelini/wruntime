use std::cell::Cell;

use crate::bindings::wruntime::db::database::{self, DbError, PgValue, Row, Transaction};
use crate::bindings::wruntime::tracing::span::ActiveSpan;
use crate::ServiceError;

#[derive(Clone, Debug)]
pub struct Jsonb(String);
impl Jsonb {
    pub fn parse(value: &str) -> Result<Self, ServiceError> {
        serde_json::from_str::<serde_json::Value>(value)
            .map_err(|e| ServiceError::bad_request(format!("invalid JSONB: {e}")))?;
        Ok(Self(value.to_string()))
    }
}
impl From<Jsonb> for PgValue {
    fn from(value: Jsonb) -> Self {
        PgValue::Jsonb(value.0)
    }
}

#[derive(Clone, Debug)]
pub struct PgNumeric(String);
impl PgNumeric {
    pub fn parse(value: &str) -> Result<Self, ServiceError> {
        value.parse::<rust_decimal::Decimal>().map_err(|error| {
            ServiceError::bad_request(format!("invalid PostgreSQL numeric: {error}"))
        })?;
        Ok(Self(value.to_string()))
    }
}
impl From<PgNumeric> for PgValue {
    fn from(value: PgNumeric) -> Self {
        PgValue::Numeric(value.0)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PgTimeMicros(i64);
impl PgTimeMicros {
    pub fn new(value: i64) -> Result<Self, ServiceError> {
        if (0..86_400_000_000).contains(&value) {
            Ok(Self(value))
        } else {
            Err(ServiceError::bad_request(
                "time microseconds must be within one day",
            ))
        }
    }
}
impl From<PgTimeMicros> for PgValue {
    fn from(value: PgTimeMicros) -> Self {
        PgValue::Time(value.0)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PgTimestampMicros(i64);
impl PgTimestampMicros {
    pub fn new(value: i64) -> Result<Self, ServiceError> {
        chrono::DateTime::from_timestamp_micros(value)
            .map(|_| Self(value))
            .ok_or_else(|| ServiceError::bad_request("timestamp microseconds out of range"))
    }
}
impl From<PgTimestampMicros> for PgValue {
    fn from(value: PgTimestampMicros) -> Self {
        PgValue::Timestamp(value.0)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PgDateDays(i32);
impl PgDateDays {
    pub fn new(value: i32) -> Result<Self, ServiceError> {
        let ce_days = value
            .checked_add(719_163)
            .ok_or_else(|| ServiceError::bad_request("date days out of range"))?;
        chrono::NaiveDate::from_num_days_from_ce_opt(ce_days)
            .map(|_| Self(value))
            .ok_or_else(|| ServiceError::bad_request("date days out of range"))
    }
}
impl From<PgDateDays> for PgValue {
    fn from(value: PgDateDays) -> Self {
        PgValue::Date(value.0)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct BatchSize(std::num::NonZeroU32);
impl BatchSize {
    pub fn new(value: u32) -> Result<Self, ServiceError> {
        std::num::NonZeroU32::new(value)
            .map(Self)
            .ok_or_else(|| ServiceError::bad_request("batch size must be > 0"))
    }
    pub fn get(self) -> u32 {
        self.0.get()
    }
}

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
                (
                    "db.operation".into(),
                    crate::tracing::AttributeValue::Text(operation.into()),
                ),
                (
                    "db.statement".into(),
                    crate::tracing::AttributeValue::Text(sql.into()),
                ),
                (
                    "db.params.count".into(),
                    crate::tracing::AttributeValue::Signed(param_count as i64),
                ),
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

enum TransactionState {
    Active(Transaction),
    Completed,
}

/// An active transaction that auto-rollbacks on drop unless committed.
///
/// ```rust,ignore
/// let tx = wr_sdk::db::transaction()?;
/// tx.query("SELECT ...", &[])?;
/// tx.execute("UPDATE ...", &[])?;
/// tx.commit()?; // consumes the guard — no rollback
/// ```
pub struct ActiveTransaction {
    state: TransactionState,
    span: DbSpan,
}

/// Compatibility alias for the active transaction guard.
pub type TxGuard = ActiveTransaction;

/// Begin a transaction and return a guard that auto-rollbacks on drop.
pub fn transaction() -> Result<ActiveTransaction, ServiceError> {
    let span = DbSpan::start("transaction", "BEGIN", 0);
    let tx = match database::begin_transaction() {
        Ok(t) => t,
        Err(e) => {
            let se = ServiceError::from(e);
            span.set_error(&se.message);
            return Err(se);
        }
    };
    Ok(ActiveTransaction {
        state: TransactionState::Active(tx),
        span,
    })
}

impl ActiveTransaction {
    fn inner(&self) -> &Transaction {
        match &self.state {
            TransactionState::Active(transaction) => transaction,
            TransactionState::Completed => unreachable!("completed transaction is consumed"),
        }
    }
    pub fn query(&self, sql: &str, params: &[PgValue]) -> Result<Vec<Row>, ServiceError> {
        let span = DbSpan::start("query", sql, params.len());
        match self.inner().query(sql, params) {
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
        match self.inner().execute(sql, params) {
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
        let transaction = match std::mem::replace(&mut self.state, TransactionState::Completed) {
            TransactionState::Active(transaction) => transaction,
            TransactionState::Completed => unreachable!("completed transaction is consumed"),
        };
        match transaction.commit() {
            Ok(()) => Ok(()),
            Err(e) => {
                let se = ServiceError::from(e);
                self.span.set_error(&se.message);
                Err(se)
            }
        }
    }
}

impl Drop for ActiveTransaction {
    fn drop(&mut self) {
        if let TransactionState::Active(transaction) =
            std::mem::replace(&mut self.state, TransactionState::Completed)
        {
            self.span.set_error("transaction rolled back");
            let _ = transaction.rollback();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_db_values_reject_invalid_inputs() {
        assert!(Jsonb::parse("not-json").is_err());
        assert!(PgNumeric::parse("nope").is_err());
        assert!(PgTimeMicros::new(-1).is_err());
        assert!(PgTimeMicros::new(86_400_000_000).is_err());
        assert!(BatchSize::new(0).is_err());
    }
}
