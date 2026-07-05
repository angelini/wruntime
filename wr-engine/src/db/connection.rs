use std::sync::Arc;

use super::wruntime::db::database::DbError;
use crate::state::DbTimeouts;

/// Configure a pooled connection for guest use: set the search_path to the
/// module's schema and apply statement/idle-in-transaction timeouts.
///
/// Uses `batch_execute` so all SET commands travel in a single round-trip.
pub(crate) async fn prepare_connection(
    client: &deadpool_postgres::Object,
    schema: &Option<Arc<str>>,
    timeouts: &DbTimeouts,
) -> Result<(), DbError> {
    use std::fmt::Write;
    let mut sql = String::new();
    if let Some(s) = schema {
        write!(sql, "SET search_path = \"{s}\"; ").unwrap();
    }
    write!(
        sql,
        "SET statement_timeout = '{}s'; SET idle_in_transaction_session_timeout = '{}s';",
        timeouts.statement_timeout_secs, timeouts.idle_in_transaction_timeout_secs
    )
    .unwrap();
    client
        .batch_execute(&sql)
        .await
        .map_err(|e| DbError::Connection(e.to_string()))?;
    Ok(())
}

// ── Host implementation ──────────────────────────────────────────────────────

/// Acquires a connection from the pool and sets schema/timeouts.
/// Takes cloned fields to avoid borrowing ModuleState across await points
/// (ModuleState contains non-Send WASI streams).
pub(crate) async fn get_prepared_connection(
    pool: &deadpool_postgres::Pool,
    schema: &Option<Arc<str>>,
    timeouts: &DbTimeouts,
) -> Result<deadpool_postgres::Object, DbError> {
    let client = pool
        .get()
        .await
        .map_err(|e| DbError::Connection(e.to_string()))?;
    prepare_connection(&client, schema, timeouts).await?;
    Ok(client)
}
