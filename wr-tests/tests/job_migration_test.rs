mod helpers;

use anyhow::Result;
use helpers::db::{require_db_url, skip_without_db};

mod embedded_job_migrations {
    use refinery::embed_migrations;
    embed_migrations!("../wr-engine/migrations/jobs");
}

static JOB_MIGRATION_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn run_embedded_job_migrations(
    client: &mut deadpool_postgres::Object,
    target: refinery::Target,
) -> Result<()> {
    let client_wrapper: &mut deadpool_postgres::ClientWrapper = client;
    let pg_client: &mut deadpool_postgres::tokio_postgres::Client = client_wrapper;
    let mut runner = embedded_job_migrations::migrations::runner().set_target(target);
    runner.set_migration_table_name("job_schema_history");
    runner.run_async(pg_client).await?;
    Ok(())
}

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
        4
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

    wr_engine::job_migration::run_job_migrations(&setup).await?;
    assert_eq!(
        client
            .query_one("SELECT count(*) FROM wr__jobs.job_schema_history", &[],)
            .await?
            .get::<_, i64>(0),
        4
    );
    client
        .batch_execute("DROP SCHEMA IF EXISTS wr__jobs CASCADE")
        .await?;
    Ok(())
}

#[tokio::test]
async fn v4_repairs_legacy_dead_rows_and_enforces_lifecycle_constraint() -> Result<()> {
    if skip_without_db("v4_repairs_legacy_dead_rows_and_enforces_lifecycle_constraint") {
        return Ok(());
    }
    let _guard = JOB_MIGRATION_TEST_LOCK.lock().await;
    let pool = wr_engine::pool::build_pool(&require_db_url(), 1)?;
    let mut client = pool.get().await?;
    client
        .batch_execute(
            "DROP SCHEMA IF EXISTS wr__jobs CASCADE; \
             CREATE SCHEMA wr__jobs; \
             SET search_path = wr__jobs",
        )
        .await?;

    run_embedded_job_migrations(&mut client, refinery::Target::Version(1)).await?;
    assert_eq!(
        client
            .query_one("SELECT count(*) FROM job_schema_history", &[])
            .await?
            .get::<_, i64>(0),
        1
    );

    client
        .execute(
            "INSERT INTO jobs \
             (job_id, worker_namespace, worker_name, worker_version, status, \
              attempt, max_attempts, error_message, result, completed_at, \
              claimed_at, claimed_by, claim_id) \
             VALUES ('legacy-null-failure', 'legacy', 'worker', '1.0.0', 'dead', \
                     1, 3, NULL, decode('0102', 'hex'), now(), now(), 'old-engine', \
                     '00000000-0000-0000-0000-000000000001'::uuid)",
            &[],
        )
        .await?;

    run_embedded_job_migrations(&mut client, refinery::Target::Version(3)).await?;
    assert_eq!(
        client
            .query_one("SELECT count(*) FROM job_schema_history", &[])
            .await?
            .get::<_, i64>(0),
        3
    );
    client
        .execute(
            "INSERT INTO jobs \
             (job_id, worker_namespace, worker_name, worker_version, status, \
              attempt, max_attempts, error_message, result, completed_at) \
             VALUES ('legacy-empty-failure', 'legacy', 'worker', '1.0.0', 'dead', \
                     0, 5, '', decode('03', 'hex'), now())",
            &[],
        )
        .await?;

    run_embedded_job_migrations(&mut client, refinery::Target::Latest).await?;
    assert_eq!(
        client
            .query_one("SELECT count(*) FROM job_schema_history", &[])
            .await?
            .get::<_, i64>(0),
        4
    );

    let rows = client
        .query(
            "SELECT job_id, attempt, max_attempts, error_message, result, completed_at, \
                    claimed_at, claimed_by, claim_id, lease_expires_at \
             FROM jobs WHERE job_id LIKE 'legacy-%' ORDER BY job_id",
            &[],
        )
        .await?;
    assert_eq!(rows.len(), 2);
    for row in &rows {
        assert_eq!(row.get::<_, i32>(1), row.get::<_, i32>(2));
        assert_eq!(
            row.get::<_, String>(3),
            "legacy dead job: failure unavailable"
        );
        assert!(row.get::<_, Option<Vec<u8>>>(4).is_none());
        assert!(row.get::<_, Option<std::time::SystemTime>>(5).is_none());
        assert!(row.get::<_, Option<std::time::SystemTime>>(6).is_none());
        assert!(row.get::<_, Option<String>>(7).is_none());
        assert!(row.get::<_, Option<uuid::Uuid>>(8).is_none());
        assert!(row.get::<_, Option<std::time::SystemTime>>(9).is_none());
    }

    assert!(client
        .query_one(
            "SELECT convalidated FROM pg_constraint \
             WHERE conrelid = 'jobs'::regclass AND conname = 'jobs_dead_lifecycle_valid'",
            &[],
        )
        .await?
        .get::<_, bool>(0));
    for regression in [
        "UPDATE jobs SET attempt = max_attempts - 1 WHERE job_id = 'legacy-null-failure'",
        "UPDATE jobs SET error_message = NULL WHERE job_id = 'legacy-null-failure'",
        "UPDATE jobs SET result = decode('ff', 'hex') WHERE job_id = 'legacy-null-failure'",
    ] {
        assert!(
            client.execute(regression, &[]).await.is_err(),
            "V4 constraint accepted regression: {regression}"
        );
    }

    client
        .batch_execute("DROP SCHEMA IF EXISTS wr__jobs CASCADE")
        .await?;
    Ok(())
}
