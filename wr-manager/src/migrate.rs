use anyhow::{Context, Result};
use tracing::info;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("./migrations");
}

/// Run all pending manager migrations.
///
/// The entire run is serialized across active-active managers by a session-level
/// advisory lock. Refinery handles embedded SQL ordering, checksums, grouped
/// execution, and migration history in `refinery_schema_history`.
///
/// The connection's `search_path` determines where tables are created. In
/// production the pool sets `search_path = wr_system`; tests use an isolated
/// per-test schema.
pub async fn run_migrations(client: &mut deadpool_postgres::Object) -> Result<()> {
    // Include `public` in the search_path so V7 can find pre-migration tables
    // that still live in public and move them to the target schema. Applied to
    // the session so it persists for refinery's internal transactions.
    let row = client
        .query_one("SHOW search_path", &[])
        .await
        .context("failed to read search_path")?;
    let current: String = row.get(0);
    if !current.contains("public") {
        client
            .batch_execute(&format!("SET search_path = {current}, public"))
            .await
            .context("failed to append public to search_path")?;
    }

    client
        .batch_execute("SELECT pg_advisory_lock(hashtext('wr-manager-migrations'))")
        .await
        .context("failed to acquire migration advisory lock")?;

    let result = async {
        let client_wrapper: &mut deadpool_postgres::ClientWrapper = client;
        let pg_client: &mut tokio_postgres::Client = client_wrapper;

        embedded::migrations::runner()
            .set_grouped(true)
            .run_async(pg_client)
            .await
            .context("manager migration execution failed")?;

        info!("manager migrations complete");
        Ok::<(), anyhow::Error>(())
    }
    .await;

    if let Err(e) = client
        .batch_execute("SELECT pg_advisory_unlock(hashtext('wr-manager-migrations'))")
        .await
    {
        tracing::warn!(error = %e, "failed to release migration advisory lock");
    }

    result
}
