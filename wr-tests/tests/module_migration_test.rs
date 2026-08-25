mod helpers;

use anyhow::{Context, Result};
use helpers::db::{require_db_url, skip_without_db};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread")]
async fn cancelled_module_migration_releases_session_lock() -> Result<()> {
    if skip_without_db("cancelled_module_migration_releases_session_lock") {
        return Ok(());
    }
    let url = require_db_url();
    let pool = wr_engine::pool::build_pool(&url, 3)?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let schema = format!("wr__migration_cancel_{suffix}");
    let admin = pool.get().await?;
    admin
        .batch_execute(&format!("CREATE SCHEMA \"{schema}\""))
        .await?;

    let directory = tempfile::tempdir()?;
    let migration = directory.path().join("V1__blocking.sql");
    std::fs::write(&migration, "SELECT pg_sleep(1);")?;
    let run_pool = pool.clone();
    let run_schema = schema.clone();
    let run_path = directory.path().to_string_lossy().into_owned();
    let task = tokio::spawn(async move {
        wr_engine::migration::run_module_migrations(
            &run_pool,
            &run_schema,
            &run_path,
            "cancellation-test",
        )
        .await
    });

    let mut observed = false;
    for _ in 0..100 {
        observed = admin
            .query_one(
                "SELECT EXISTS (SELECT FROM pg_stat_activity WHERE query LIKE '%pg_sleep(1)%' AND pid <> pg_backend_pid())",
                &[],
            )
            .await?
            .get(0);
        if observed {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    anyhow::ensure!(observed, "blocking migration never acquired its session");
    task.abort();
    let _ = task.await;

    std::fs::write(&migration, "CREATE TABLE migration_probe (id INT);")?;
    tokio::time::timeout(
        Duration::from_secs(5),
        wr_engine::migration::run_module_migrations(
            &pool,
            &schema,
            directory.path().to_string_lossy().as_ref(),
            "cancellation-test",
        ),
    )
    .await
    .context("migration lock remained stranded after cancellation")??;
    assert!(admin
        .query_one(
            "SELECT to_regclass($1) IS NOT NULL",
            &[&format!("{schema}.migration_probe")]
        )
        .await?
        .get::<_, bool>(0));
    admin
        .batch_execute(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_module_migration_does_not_strand_lock() -> Result<()> {
    if skip_without_db("failed_module_migration_does_not_strand_lock") {
        return Ok(());
    }
    let url = require_db_url();
    let pool = wr_engine::pool::build_pool(&url, 2)?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let schema = format!("wr__migration_failure_{suffix}");
    let admin = pool.get().await?;
    admin
        .batch_execute(&format!("CREATE SCHEMA \"{schema}\""))
        .await?;
    let directory = tempfile::tempdir()?;
    let migration = directory.path().join("V1__repairable.sql");
    std::fs::write(&migration, "THIS IS NOT SQL;")?;
    assert!(wr_engine::migration::run_module_migrations(
        &pool,
        &schema,
        directory.path().to_string_lossy().as_ref(),
        "failure-test",
    )
    .await
    .is_err());
    std::fs::write(&migration, "CREATE TABLE repaired (id INT);")?;
    tokio::time::timeout(
        Duration::from_secs(5),
        wr_engine::migration::run_module_migrations(
            &pool,
            &schema,
            directory.path().to_string_lossy().as_ref(),
            "failure-test",
        ),
    )
    .await
    .context("failed migration stranded its lock")??;
    admin
        .batch_execute(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .await?;
    Ok(())
}
