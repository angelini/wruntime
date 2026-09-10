mod helpers;

use anyhow::Result;
use helpers::db::{require_db_url, skip_without_db};

static JOB_MIGRATION_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn assert_index_plan(
    client: &deadpool_postgres::Object,
    sql: &str,
    expected_index: &str,
) -> Result<()> {
    let plan = client
        .query_one(&format!("EXPLAIN (FORMAT JSON) {sql}"), &[])
        .await?
        .get::<_, serde_json::Value>(0)
        .to_string();
    assert!(
        plan.contains(expected_index),
        "expected {expected_index} in query plan: {plan}"
    );
    assert!(
        !plan.contains("\"Node Type\":\"Sort\""),
        "inventory query must not add an explicit sort: {plan}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn fresh_and_concurrent_job_migrations_converge() -> Result<()> {
    if skip_without_db("fresh_and_concurrent_job_migrations_converge") {
        return Ok(());
    }
    let _guard = JOB_MIGRATION_TEST_LOCK.lock().await;
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
        1
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
    for index in [
        "idx_jobs_admin_created",
        "idx_jobs_admin_status_created",
        "idx_jobs_admin_worker_created",
    ] {
        assert!(
            client
                .query_one(
                    "SELECT to_regclass('wr__jobs.' || $1) IS NOT NULL",
                    &[&index],
                )
                .await?
                .get::<_, bool>(0),
            "missing V3 inventory index {index}"
        );
    }
    let definitions = client
        .query(
            "SELECT indexdef FROM pg_indexes \
             WHERE schemaname = 'wr__jobs' AND indexname LIKE 'idx_jobs_admin_%' \
             ORDER BY indexname",
            &[],
        )
        .await?;
    assert_eq!(definitions.len(), 3);
    assert!(definitions.iter().all(|row| row
        .get::<_, String>(0)
        .contains("created_at DESC, job_id DESC")));

    client
        .execute(
            "INSERT INTO wr__jobs.jobs \
             (job_id, worker_namespace, worker_name, worker_version, status, attempt, error_message, created_at) \
             SELECT 'plan-' || value, 'ns' || value % 50, 'worker' || value % 100, \
                    '1.0.0', CASE WHEN value % 3 = 0 THEN 'dead' ELSE 'pending' END, \
                    CASE WHEN value % 3 = 0 THEN 3 ELSE 0 END, \
                    CASE WHEN value % 3 = 0 THEN 'failed' ELSE NULL END, \
                    now() - value * interval '1 second' \
             FROM generate_series(1, 6000) AS value",
            &[],
        )
        .await?;
    client.batch_execute("ANALYZE wr__jobs.jobs").await?;
    assert_index_plan(
        &client,
        "SELECT job_id FROM wr__jobs.jobs ORDER BY created_at DESC, job_id DESC LIMIT 50",
        "idx_jobs_admin_created",
    )
    .await?;
    assert_index_plan(
        &client,
        "SELECT job_id FROM wr__jobs.jobs WHERE status = 'dead' \
         ORDER BY created_at DESC, job_id DESC LIMIT 50",
        "idx_jobs_admin_status_created",
    )
    .await?;
    assert_index_plan(
        &client,
        "SELECT job_id FROM wr__jobs.jobs \
         WHERE worker_namespace = 'ns0' AND worker_name = 'worker0' \
           AND worker_version = '1.0.0' \
         ORDER BY created_at DESC, job_id DESC LIMIT 50",
        "idx_jobs_admin_worker_created",
    )
    .await?;

    let malformed_dead = client
        .execute(
            "INSERT INTO wr__jobs.jobs \
             (job_id, worker_namespace, worker_name, worker_version, status, attempt, error_message) \
             VALUES ('malformed-dead', 'ns', 'worker', '1.0.0', 'dead', 1, NULL)",
            &[],
        )
        .await;
    assert!(malformed_dead.is_err());
    let incomplete_claim = client
        .execute(
            "INSERT INTO wr__jobs.jobs \
             (job_id, worker_namespace, worker_name, worker_version, status, attempt) \
             VALUES ('incomplete-claim', 'ns', 'worker', '1.0.0', 'running', 1)",
            &[],
        )
        .await;
    assert!(incomplete_claim.is_err());

    wr_engine::job_migration::run_job_migrations(&setup).await?;
    assert_eq!(
        client
            .query_one("SELECT count(*) FROM wr__jobs.job_schema_history", &[],)
            .await?
            .get::<_, i64>(0),
        1
    );
    client
        .batch_execute("DROP SCHEMA IF EXISTS wr__jobs CASCADE")
        .await?;
    Ok(())
}
