mod helpers;

use anyhow::Result;
use helpers::db::{require_db_url, skip_without_db};

#[tokio::test(flavor = "multi_thread")]
async fn fresh_and_concurrent_job_migrations_converge() -> Result<()> {
    if skip_without_db("fresh_and_concurrent_job_migrations_converge") {
        return Ok(());
    }
    let url = require_db_url();
    let setup = wr_engine::pool::build_pool(&url, 2)?;
    let client = setup.get().await?;
    client
        .batch_execute("DROP SCHEMA IF EXISTS wr__jobs CASCADE")
        .await?;
    drop(client);

    let left = wr_engine::pool::build_pool(&url, 1)?;
    let right = wr_engine::pool::build_pool(&url, 1)?;
    let (left_result, right_result) = tokio::join!(
        wr_engine::job_migration::run_job_migrations(&left),
        wr_engine::job_migration::run_job_migrations(&right),
    );
    left_result?;
    right_result?;

    let client = setup.get().await?;
    assert_eq!(
        client
            .query_one("SELECT count(*) FROM wr__jobs.job_schema_history", &[],)
            .await?
            .get::<_, i64>(0),
        2
    );
    assert!(client
        .query_one(
            "SELECT EXISTS (SELECT FROM information_schema.columns \
             WHERE table_schema = 'wr__jobs' AND table_name = 'jobs' \
             AND column_name = 'lease_expires_at')",
            &[],
        )
        .await?
        .get::<_, bool>(0));
    assert!(client
        .query_one(
            "SELECT to_regclass('wr__jobs.idx_jobs_running_lease') IS NOT NULL",
            &[],
        )
        .await?
        .get::<_, bool>(0));

    wr_engine::job_migration::run_job_migrations(&setup).await?;
    assert_eq!(
        client
            .query_one("SELECT count(*) FROM wr__jobs.job_schema_history", &[],)
            .await?
            .get::<_, i64>(0),
        2
    );
    client
        .batch_execute("DROP SCHEMA IF EXISTS wr__jobs CASCADE")
        .await?;
    Ok(())
}
