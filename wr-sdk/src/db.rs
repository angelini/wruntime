use crate::bindings::wruntime::db::database::{self, DbError, PgValue, Row, Transaction};
use crate::ServiceError;

// ── Row helpers ─────────────────────────────────────────────────────────────

impl Row {
    /// Extract a `TEXT` column by index, returning a `ServiceError` on type mismatch.
    pub fn get_text(&self, col: usize) -> Result<&str, ServiceError> {
        match &self.columns.get(col).ok_or_else(|| col_err(col))?.value {
            PgValue::Text(s) => Ok(s),
            other => Err(type_err(col, "text", other)),
        }
    }

    /// Extract a `BIGINT` / `INT8` column by index.
    pub fn get_i64(&self, col: usize) -> Result<i64, ServiceError> {
        match &self.columns.get(col).ok_or_else(|| col_err(col))?.value {
            PgValue::Int8(v) => Ok(*v),
            other => Err(type_err(col, "int8", other)),
        }
    }

    /// Extract an `INTEGER` / `INT4` column by index.
    pub fn get_i32(&self, col: usize) -> Result<i32, ServiceError> {
        match &self.columns.get(col).ok_or_else(|| col_err(col))?.value {
            PgValue::Int4(v) => Ok(*v),
            other => Err(type_err(col, "int4", other)),
        }
    }

    /// Extract a `BOOLEAN` column by index.
    pub fn get_bool(&self, col: usize) -> Result<bool, ServiceError> {
        match &self.columns.get(col).ok_or_else(|| col_err(col))?.value {
            PgValue::Boolean(v) => Ok(*v),
            other => Err(type_err(col, "boolean", other)),
        }
    }

    /// Extract a `FLOAT8` / `DOUBLE PRECISION` column by index.
    pub fn get_f64(&self, col: usize) -> Result<f64, ServiceError> {
        match &self.columns.get(col).ok_or_else(|| col_err(col))?.value {
            PgValue::Float8(v) => Ok(*v),
            other => Err(type_err(col, "float8", other)),
        }
    }

    /// Extract a `JSONB` / `JSON` column by index.
    pub fn get_jsonb(&self, col: usize) -> Result<&str, ServiceError> {
        match &self.columns.get(col).ok_or_else(|| col_err(col))?.value {
            PgValue::Jsonb(s) => Ok(s),
            other => Err(type_err(col, "jsonb", other)),
        }
    }
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
