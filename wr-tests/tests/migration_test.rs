#[allow(dead_code, unused_imports)]
mod helpers;
use helpers::*;

use anyhow::{Context, Result};

async fn assert_manager_schema_ready(client: &deadpool_postgres::Object) -> Result<()> {
    let app_tables_exist: bool = client
        .query_one(
            "SELECT to_regclass('wr_engines') IS NOT NULL
                    AND to_regclass('wr_routing_rules') IS NOT NULL
                    AND to_regclass('wr_schemas') IS NOT NULL",
            &[],
        )
        .await?
        .get(0);
    assert!(app_tables_exist, "expected manager application tables");

    let latest_constraint_exists: bool = client
        .query_one(
            "SELECT EXISTS(
                SELECT 1
                FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE n.nspname = current_schema()
                  AND t.relname = 'wr_routing_rules'
                  AND c.conname = 'wr_routing_rules_peer_address_not_empty'
            )",
            &[],
        )
        .await?
        .get(0);
    assert!(
        latest_constraint_exists,
        "expected latest manager schema constraint"
    );

    Ok(())
}

/// Cold race: two managers run migrations concurrently against one empty schema.
/// Both must succeed; the advisory lock serializes manager startup migrations so
/// active-active managers do not race on application DDL.
#[tokio::test]
async fn test_concurrent_run_migrations_cold_race() -> Result<()> {
    let schema = "mig_concurrent";
    let pool_a = manager_pool_in_schema(schema).await;
    let pool_b = wr_common::pool::build_pool_with_search_path(&require_db_url(), 1, schema)
        .context("failed to build second migration pool")?;

    let mut client_a = pool_a.get().await.context("conn a")?;
    let mut client_b = pool_b.get().await.context("conn b")?;

    let (ra, rb) = tokio::join!(
        wr_manager::migrate::run_migrations(&mut client_a),
        wr_manager::migrate::run_migrations(&mut client_b),
    );
    ra.context("run_migrations A failed")?;
    rb.context("run_migrations B failed")?;

    assert_manager_schema_ready(&client_a).await?;

    Ok(())
}

/// Repeated startup against an already-migrated schema succeeds and leaves the
/// manager application schema available.
#[tokio::test]
async fn test_run_migrations_second_run_is_noop() -> Result<()> {
    let schema = "mig_noop";
    let pool = manager_pool_in_schema(schema).await;
    let mut client = pool.get().await.context("conn")?;

    wr_manager::migrate::run_migrations(&mut client)
        .await
        .context("first run")?;
    wr_manager::migrate::run_migrations(&mut client)
        .await
        .context("second run")?;

    assert_manager_schema_ready(&client).await?;

    Ok(())
}
