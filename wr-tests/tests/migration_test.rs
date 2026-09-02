mod helpers;
use helpers::db::{manager_pool_in_schema, require_db_url};

use anyhow::{Context, Result};

async fn assert_manager_schema_ready(client: &deadpool_postgres::Object) -> Result<()> {
    let app_tables_exist: bool = client
        .query_one(
            "SELECT to_regclass('wr_engines') IS NOT NULL
                    AND to_regclass('wr_routing_rules') IS NOT NULL
                    AND to_regclass('wr_schemas') IS NOT NULL
                    AND to_regclass('wr_nodes') IS NOT NULL
                    AND to_regclass('wr_node_deployments') IS NOT NULL
                    AND to_regclass('wr_node_operations') IS NOT NULL
                    AND to_regclass('wr_node_operation_slots') IS NOT NULL
                    AND to_regclass('wr_node_operation_events') IS NOT NULL
                    AND to_regclass('wr_node_slot_authority') IS NOT NULL
                    AND to_regclass('wr_node_slot_observations') IS NOT NULL
                    AND to_regclass('wr_node_agent_policies') IS NOT NULL
                    AND to_regclass('wr_node_agent_attestations') IS NOT NULL
                    AND to_regclass('wr_node_operation_result_receipts') IS NOT NULL
                    AND to_regclass('wr_node_release_deletions') IS NOT NULL",
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

    let lifecycle_columns_exist: bool = client
        .query_one(
            "SELECT EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'wr_nodes' AND column_name = 'target_revision'
            ) AND EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'wr_engines' AND column_name = 'deployment_engine_slot'
            ) AND EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'wr_node_operations' AND column_name = 'forward_deadline'
            ) AND EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'wr_node_operations' AND column_name = 'agent_instance_id'
            ) AND EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'wr_node_operation_slots' AND column_name = 'source_digest'
            ) AND EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'wr_node_slot_observations' AND column_name = 'backend_query_error'
            ) AND EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'wr_node_agent_policies' AND column_name = 'binary_digest'
            ) AND EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'wr_node_agent_policies' AND column_name = 'capabilities'
            ) AND EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'wr_node_operations' AND column_name = 'cleanup_delete_allowlist'
            ) AND EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'wr_node_deployments' AND column_name = 'allocated_by'
                  AND is_nullable = 'NO'
            ) AND EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'wr_engines' AND column_name = 'job_queue_id'
            ) AND EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'wr_engines' AND column_name = 'job_admin_address'
            ) AND to_regclass('idx_wr_engines_job_admin_delegates') IS NOT NULL",
            &[],
        )
        .await?
        .get(0);
    assert!(
        lifecycle_columns_exist,
        "expected lifecycle authority, fencing, evidence, and job-admin delegation columns"
    );

    let lifecycle_constraints_exist: bool = client
        .query_one(
            "SELECT COUNT(*) = 6 FROM pg_constraint c
             JOIN pg_class t ON t.oid = c.conrelid
             JOIN pg_namespace n ON n.oid = t.relnamespace
             WHERE n.nspname = current_schema()
               AND t.relname = 'wr_node_operations'
               AND c.conname IN (
                 'wr_node_operations_phase_check',
                 'wr_node_operations_restoration_terminal_check',
                 'wr_node_operations_forward_deadline_check',
                 'wr_node_operations_commit_phase_check',
                 'wr_node_operations_phase_state_check',
                 'wr_node_operations_lease_owner_check'
               )",
            &[],
        )
        .await?
        .get(0);
    assert!(
        lifecycle_constraints_exist,
        "expected lifecycle state constraints"
    );

    Ok(())
}

#[tokio::test]
async fn test_v16_to_v17_preserves_operation_and_event_history() -> Result<()> {
    let pool = manager_pool_in_schema("mig_v17_upgrade").await;
    let client = pool.get().await.context("upgrade connection")?;
    for sql in [
        include_str!("../../wr-manager/migrations/V1__initial.sql"),
        include_str!("../../wr-manager/migrations/V2__secrets.sql"),
        include_str!("../../wr-manager/migrations/V3__managers.sql"),
        include_str!("../../wr-manager/migrations/V4__engine_heartbeats.sql"),
        include_str!("../../wr-manager/migrations/V5__schedules.sql"),
        include_str!("../../wr-manager/migrations/V6__peer_address.sql"),
        include_str!("../../wr-manager/migrations/V7__system_schema.sql"),
        include_str!("../../wr-manager/migrations/V8__module_heartbeats.sql"),
        include_str!("../../wr-manager/migrations/V9__schedule_leases.sql"),
        include_str!("../../wr-manager/migrations/V10__routing_rule_proxy_address.sql"),
        include_str!("../../wr-manager/migrations/V11__drop_routing_rule_proxy_address.sql"),
        include_str!("../../wr-manager/migrations/V12__routing_rule_peer_address_not_empty.sql"),
        include_str!("../../wr-manager/migrations/V13__schedule_positive_counts.sql"),
        include_str!("../../wr-manager/migrations/V14__node_deployments.sql"),
        include_str!("../../wr-manager/migrations/V15__engine_draining.sql"),
        include_str!("../../wr-manager/migrations/V16__node_operations.sql"),
    ] {
        client.batch_execute(sql).await?;
    }
    let operation_id = uuid::Uuid::new_v4();
    client
        .execute("INSERT INTO wr_nodes(node_id) VALUES ('upgrade-node')", &[])
        .await?;
    client
        .execute(
            "INSERT INTO wr_node_operations
               (operation_id, node_id, request_token, actor, action, state,
                request_payload, policy)
             VALUES ($1, 'upgrade-node', 'old-token', 'old-actor', 'restart',
                     'paused', '\\x01', '\\x02')",
            &[&operation_id],
        )
        .await?;
    client
        .execute(
            "INSERT INTO wr_node_operation_slots
               (operation_id, node_id, engine_slot, rollout_order, next_step)
             VALUES ($1, 'upgrade-node', 'blue', 0, 'stop_slot')",
            &[&operation_id],
        )
        .await?;
    client
        .execute(
            "INSERT INTO wr_node_operation_events
               (operation_id, actor, event_code, detail)
             VALUES ($1, 'old-actor', 'OLD_EVENT', 'retained')",
            &[&operation_id],
        )
        .await?;

    client
        .batch_execute(include_str!(
            "../../wr-manager/migrations/V17__node_agent_cutover.sql"
        ))
        .await?;
    let row = client
        .query_one(
            "SELECT phase, forward_deadline IS NOT NULL AS has_deadline
             FROM wr_node_operations WHERE operation_id = $1",
            &[&operation_id],
        )
        .await?;
    assert_eq!(row.get::<_, String>("phase"), "forward");
    assert!(row.get::<_, bool>("has_deadline"));
    let next_step: String = client
        .query_one(
            "SELECT next_step FROM wr_node_operation_slots WHERE operation_id = $1",
            &[&operation_id],
        )
        .await?
        .get(0);
    assert_eq!(next_step, "stop_backend");
    let event: String = client
        .query_one(
            "SELECT event_code FROM wr_node_operation_events WHERE operation_id = $1",
            &[&operation_id],
        )
        .await?
        .get(0);
    assert_eq!(event, "OLD_EVENT");

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
