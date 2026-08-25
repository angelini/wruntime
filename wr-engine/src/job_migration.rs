use anyhow::{Context, Result};
use deadpool_postgres::Pool;
use tracing::info;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("./migrations/jobs");
}

const JOB_MIGRATION_LOCK_DOMAIN: i32 = 0x5752_4a4d;
const JOB_MIGRATION_LOCK_KEY: i32 = 1;
const JOB_MIGRATION_HISTORY: &str = "job_schema_history";

/// Apply the engine-owned job queue schema before workers or recovery start.
///
/// The physical session is detached from the pool before taking the advisory
/// lock, so cancellation, panic, or an unlock error closes the session rather
/// than recycling a connection that may still hold the lock.
pub async fn run_job_migrations(pool: &Pool) -> Result<()> {
    let pooled = pool
        .get()
        .await
        .context("failed to acquire job migration connection")?;
    let mut client = deadpool_postgres::Object::take(pooled);

    client
        .execute(
            "SELECT pg_advisory_lock($1, $2)",
            &[&JOB_MIGRATION_LOCK_DOMAIN, &JOB_MIGRATION_LOCK_KEY],
        )
        .await
        .context("failed to acquire job migration lock")?;

    let result = async {
        client
            .batch_execute("CREATE SCHEMA IF NOT EXISTS wr__jobs; SET search_path = wr__jobs")
            .await
            .context("failed to initialize job migration schema")?;
        let client_wrapper: &mut deadpool_postgres::ClientWrapper = &mut client;
        let pg_client: &mut tokio_postgres::Client = client_wrapper;
        let mut runner = embedded::migrations::runner();
        runner.set_migration_table_name(JOB_MIGRATION_HISTORY);
        let report = runner
            .run_async(pg_client)
            .await
            .context("job queue migration execution failed")?;
        info!(
            applied = report.applied_migrations().len(),
            "job queue migrations complete"
        );
        Ok::<(), anyhow::Error>(())
    }
    .await;

    let unlocked = client
        .query_one(
            "SELECT pg_advisory_unlock($1, $2)",
            &[&JOB_MIGRATION_LOCK_DOMAIN, &JOB_MIGRATION_LOCK_KEY],
        )
        .await
        .context("failed to release job migration lock")?
        .get::<_, bool>(0);
    anyhow::ensure!(unlocked, "job migration advisory lock was not held");

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_migration_lock_domain_is_distinct() {
        assert_ne!(JOB_MIGRATION_LOCK_DOMAIN, JOB_MIGRATION_LOCK_KEY);
        assert_ne!(JOB_MIGRATION_HISTORY, "refinery_schema_history");
    }
}
