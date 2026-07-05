#[allow(dead_code, unused_imports)]
mod helpers;
use helpers::*;

use anyhow::Result;

/// Cold race: two managers run migrations concurrently against one empty schema.
/// Both must succeed (the advisory lock serializes the check-run-record
/// sequence, so no duplicate-key / relation-already-exists errors) and the
/// `wr_migrations` table must end up with exactly one row per migration version.
#[tokio::test]
async fn test_concurrent_run_migrations_cold_race() -> Result<()> {
    let schema = "mig_concurrent";
    let pool_a = manager_pool_in_schema(schema).await;
    let pool_b = wr_common::pool::build_pool_with_search_path(&require_db_url(), 1, schema)
        .expect("failed to build second migration pool");

    let mut client_a = pool_a.get().await.expect("conn a");
    let mut client_b = pool_b.get().await.expect("conn b");

    let (ra, rb) = tokio::join!(
        wr_manager::migrate::run_migrations(&mut client_a),
        wr_manager::migrate::run_migrations(&mut client_b),
    );
    ra.expect("run_migrations A failed");
    rb.expect("run_migrations B failed");

    let versions: Vec<i32> = client_a
        .query("SELECT version FROM wr_migrations ORDER BY version", &[])
        .await?
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(versions, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);

    Ok(())
}

/// A second run after the first fully completed is a clean no-op: still exactly
/// one row per version, no errors.
#[tokio::test]
async fn test_run_migrations_second_run_is_noop() -> Result<()> {
    let schema = "mig_noop";
    let pool = manager_pool_in_schema(schema).await;
    let mut client = pool.get().await.expect("conn");

    wr_manager::migrate::run_migrations(&mut client)
        .await
        .expect("first run");
    wr_manager::migrate::run_migrations(&mut client)
        .await
        .expect("second run");

    let versions: Vec<i32> = client
        .query("SELECT version FROM wr_migrations ORDER BY version", &[])
        .await?
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(versions, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);

    Ok(())
}
