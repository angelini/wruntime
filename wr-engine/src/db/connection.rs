use std::sync::Arc;

use super::wruntime::db::database::DbError;
use crate::state::DbTimeouts;

/// Configure a pooled connection for guest use: set the search_path to the
/// module's schema and apply statement/idle-in-transaction timeouts.
///
/// Uses `batch_execute` so all SET commands travel in a single round-trip.
pub(crate) async fn prepare_connection(
    client: &deadpool_postgres::Object,
    schema: &Arc<str>,
    timeouts: &DbTimeouts,
) -> Result<(), DbError> {
    use std::fmt::Write;
    let mut sql = String::new();
    let quoted = schema.replace('"', "\"\"");
    write!(sql, "SET search_path = \"{quoted}\", pg_catalog; ").unwrap();
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
    schema: &Arc<str>,
    expected_database: &Arc<str>,
    expected_user: &Arc<str>,
    timeouts: &DbTimeouts,
) -> Result<deadpool_postgres::Object, DbError> {
    let client = pool
        .get()
        .await
        .map_err(|e| DbError::Connection(e.to_string()))?;
    if !expected_database.is_empty() || !expected_user.is_empty() {
        let identity = match client
            .query_one("SELECT current_database(), session_user, current_user", &[])
            .await
        {
            Ok(identity) => identity,
            Err(error) => {
                drop(deadpool_postgres::Object::take(client));
                return Err(DbError::Connection(error.to_string()));
            }
        };
        let database: String = identity.get(0);
        let session_user: String = identity.get(1);
        let current_user: String = identity.get(2);
        if database != expected_database.as_ref()
            || session_user != expected_user.as_ref()
            || current_user != expected_user.as_ref()
        {
            drop(deadpool_postgres::Object::take(client));
            return Err(DbError::Connection(
                "PostgreSQL guest connection identity mismatch".into(),
            ));
        }
    }
    if let Err(error) = prepare_connection(&client, schema, timeouts).await {
        drop(deadpool_postgres::Object::take(client));
        return Err(error);
    }
    Ok(client)
}
