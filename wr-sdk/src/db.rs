use crate::bindings::wruntime::db::database::{self, DbError, PgValue, Row, Transaction};
use crate::ServiceError;

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
}

/// Begin a transaction and return a guard that auto-rollbacks on drop.
pub fn transaction() -> Result<TxGuard, ServiceError> {
    let tx = database::begin_transaction()?;
    Ok(TxGuard { inner: Some(tx) })
}

impl TxGuard {
    pub fn query(&self, sql: &str, params: &[PgValue]) -> Result<Vec<Row>, ServiceError> {
        self.inner
            .as_ref()
            .unwrap()
            .query(sql, params)
            .map_err(ServiceError::from)
    }

    pub fn execute(&self, sql: &str, params: &[PgValue]) -> Result<u64, ServiceError> {
        self.inner
            .as_ref()
            .unwrap()
            .execute(sql, params)
            .map_err(ServiceError::from)
    }

    /// Commit the transaction, consuming the guard.
    pub fn commit(mut self) -> Result<(), ServiceError> {
        self.inner
            .take()
            .unwrap()
            .commit()
            .map_err(ServiceError::from)
    }
}

impl Drop for TxGuard {
    fn drop(&mut self) {
        if let Some(tx) = self.inner.take() {
            let _ = tx.rollback();
        }
    }
}
