mod helpers;
use helpers::db::{manager_pool_in_schema, require_db_url};

use anyhow::{Context, Result};

async fn assert_manager_schema_ready(client: &deadpool_postgres::Object) -> Result<()> {
    let app_tables_exist: bool = client
        .query_one(
            "SELECT to_regclass('wr_manager_lock') IS NOT NULL
                    AND to_regclass('wr_engines') IS NOT NULL
                    AND to_regclass('wr_routing_rules') IS NOT NULL
                    AND to_regclass('wr_schemas') IS NOT NULL
                    AND to_regclass('wr_secrets') IS NOT NULL
                    AND to_regclass('wr_managers') IS NOT NULL
                    AND to_regclass('wr_schedules') IS NOT NULL
                    AND to_regclass('wr_module_heartbeats') IS NOT NULL
                    AND to_regclass('wr_nodes') IS NOT NULL
                    AND to_regclass('wr_node_deployments') IS NOT NULL
                    AND to_regclass('wr_node_operations') IS NOT NULL
                    AND to_regclass('wr_node_operation_targets') IS NOT NULL
                    AND to_regclass('wr_node_operation_engine_target_details') IS NOT NULL
                    AND to_regclass('wr_node_operation_slots') IS NULL
                    AND to_regclass('wr_node_operation_events') IS NOT NULL
                    AND to_regclass('wr_node_slot_authority') IS NOT NULL
                    AND to_regclass('wr_node_slot_observations') IS NOT NULL
                    AND to_regclass('wr_node_agent_policies') IS NOT NULL
                    AND to_regclass('wr_node_agent_attestations') IS NOT NULL
                    AND to_regclass('wr_node_operation_result_receipts') IS NOT NULL
                    AND to_regclass('wr_node_release_deletions') IS NOT NULL
                    AND to_regclass('wr_node_release_cleanup') IS NOT NULL
                    AND to_regclass('wr_node_release_cleanup_generations') IS NOT NULL
                    AND to_regclass('wr_node_release_cleanup_events') IS NOT NULL
                    AND to_regclass('wr_node_release_cleanup_result_receipts') IS NOT NULL
                    AND to_regclass('wr_manager_rollouts') IS NOT NULL
                    AND to_regclass('wr_manager_rollout_guard') IS NOT NULL
                    AND to_regclass('wr_manager_rollout_members') IS NOT NULL
                    AND to_regclass('wr_manager_rollout_events') IS NOT NULL
                    AND to_regclass('wr_node_slot_owners') IS NOT NULL
                    AND to_regclass('wr_proxy_inventory') IS NOT NULL",
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
                  AND table_name = 'wr_node_operation_targets' AND column_name = 'source_digest'
            ) AND EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'wr_node_operation_engine_target_details'
                  AND column_name = 'transition_kind' AND is_nullable = 'NO'
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
                  AND table_name = 'wr_node_agent_policies' AND column_name = 'retention_count'
            ) AND NOT EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'wr_node_agent_policies' AND column_name = 'config_digest'
            ) AND NOT EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'wr_node_agent_attestations' AND column_name IN ('config_digest', 'retention_count')
            ) AND NOT EXISTS(
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
            ) AND EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'wr_engines' AND column_name = 'slot_generation'
                  AND data_type = 'bytea'
            ) AND EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'wr_node_deployments' AND column_name = 'revision_digest'
                  AND is_nullable = 'NO'
            ) AND EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'wr_manager_rollout_guard'
                  AND column_name = 'recovery_permit_principal_uri'
            ) AND EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'wr_manager_rollouts'
                  AND column_name IN ('reset_request_digest', 'reset_evidence_digest', 'reset_policy_generation', 'reset_policy_digest')
                GROUP BY table_name HAVING COUNT(*) = 4
            ) AND NOT EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND ((table_name = 'wr_manager_rollouts' AND column_name IN ('recovery_of', 'executor_id', 'lease_epoch', 'lease_expires_at', 'deployment_leaf_fingerprint'))
                    OR (table_name = 'wr_manager_rollout_members' AND column_name IN ('expected_host_digest', 'expected_config_digest', 'expected_backend', 'expected_executable_digest', 'expected_backend_spec_digest', 'expected_credential_digest', 'expected_old_selector_digest', 'expected_new_selector_digest'))
                    OR (table_name = 'wr_managers' AND column_name = 'rollout_lease_epoch'))
            ) AND EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'wr_node_operations' AND column_name = 'lease_epoch'
            ) AND EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'wr_managers' AND column_name = 'policy_generation'
            ) AND to_regclass('wr_manager_rollouts_one_recovery') IS NULL
              AND to_regclass('idx_wr_engines_job_admin_delegates') IS NOT NULL",
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

    let gossip_column_absent: bool = client
        .query_one(
            "SELECT NOT EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'wr_managers'
                  AND column_name = 'gossip_address'
            )",
            &[],
        )
        .await?
        .get(0);
    assert!(
        gossip_column_absent,
        "latest manager migration must remove gossip_address"
    );

    let proxy_process_id_contract: bool = client
        .query_one(
            "SELECT EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'wr_proxy_inventory'
                  AND column_name = 'process_instance_id'
                  AND data_type = 'text'
                  AND is_nullable = 'NO'
            ) AND EXISTS(
                SELECT 1 FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE n.nspname = current_schema()
                  AND t.relname = 'wr_proxy_inventory'
                  AND c.conname = 'wr_proxy_inventory_process_instance_id_length'
            )",
            &[],
        )
        .await?
        .get(0);
    assert!(
        proxy_process_id_contract,
        "proxy lifecycle process IDs must use constrained text storage"
    );
    for invalid in [String::new(), "x".repeat(256)] {
        assert!(
            client
                .execute(
                    "INSERT INTO wr_proxy_inventory (proxy_id,node_id,process_instance_id,registration) VALUES ('proxy-migration','node-migration',$1,'\\x'::bytea)",
                    &[&invalid],
                )
                .await
                .is_err(),
            "database constraint accepted an invalid proxy process ID"
        );
    }
    let maximum = "m".repeat(255);
    client
        .execute(
            "INSERT INTO wr_proxy_inventory (proxy_id,node_id,process_instance_id,registration) VALUES ('proxy-migration','node-migration',$1,'\\x'::bytea)",
            &[&maximum],
        )
        .await?;

    let tables: Vec<String> = client
        .query_one(
            "SELECT array_agg(tablename ORDER BY tablename)::text[]
             FROM pg_tables WHERE schemaname = current_schema()
               AND tablename <> 'refinery_schema_history'",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(
        tables,
        [
            "wr_engines",
            "wr_manager_lock",
            "wr_manager_rollout_events",
            "wr_manager_rollout_guard",
            "wr_manager_rollout_members",
            "wr_manager_rollouts",
            "wr_managers",
            "wr_module_heartbeats",
            "wr_node_agent_attestations",
            "wr_node_agent_policies",
            "wr_node_deployments",
            "wr_node_operation_engine_target_details",
            "wr_node_operation_events",
            "wr_node_operation_result_receipts",
            "wr_node_operation_targets",
            "wr_node_operations",
            "wr_node_release_cleanup",
            "wr_node_release_cleanup_events",
            "wr_node_release_cleanup_generations",
            "wr_node_release_cleanup_result_receipts",
            "wr_node_release_deletions",
            "wr_node_slot_authority",
            "wr_node_slot_observations",
            "wr_node_slot_owners",
            "wr_nodes",
            "wr_proxy_inventory",
            "wr_routing_rules",
            "wr_schedules",
            "wr_schemas",
            "wr_secrets",
        ]
        .map(String::from)
        .to_vec(),
        "fresh V1 must create the complete manager catalog"
    );

    // Pin the complete application catalog (all columns/defaults/types, keys,
    // checks/FKs, indexes, functions, triggers, and owned sequences). The
    // repository test service is pinned to PostgreSQL 18, so pg_catalog
    // rendering is deterministic for this baseline.
    let catalog_digest: String = client
        .query_one(
            r#"WITH catalog_lines AS (
                SELECT 1 AS category, c.relname AS object_name, a.attnum AS ordinal,
                       format('COLUMN|%s|%s|%s|%s|%s', c.relname, a.attname,
                              pg_catalog.format_type(a.atttypid, a.atttypmod), a.attnotnull,
                              COALESCE(pg_get_expr(d.adbin, d.adrelid), '')) AS line
                  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                  JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
                  LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
                 WHERE n.nspname = current_schema() AND c.relkind = 'r'
                   AND c.relname <> 'refinery_schema_history'
                UNION ALL
                SELECT 2, c.relname, 0,
                       format('CONSTRAINT|%s|%s|%s|%s|%s|%s', c.relname, con.conname,
                              con.contype, con.condeferrable, con.condeferred,
                              pg_get_constraintdef(con.oid))
                  FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid
                  JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = current_schema() AND c.relname <> 'refinery_schema_history'
                UNION ALL
                SELECT 3, i.indexname, 0,
                       'INDEX|' || i.indexname || '|' || replace(i.indexdef, current_schema() || '.', '')
                  FROM pg_indexes i WHERE i.schemaname = current_schema()
                   AND i.tablename <> 'refinery_schema_history'
                UNION ALL
                SELECT 4, p.proname, 0,
                       'FUNCTION|' || p.proname || '|' ||
                       replace(replace(pg_get_functiondef(p.oid), E'\n', ' '), current_schema() || '.', '')
                  FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
                 WHERE n.nspname = current_schema()
                UNION ALL
                SELECT 5, t.tgname, 0,
                       format('TRIGGER|%s|%s|%s|%s|%s', c.relname, t.tgname,
                              t.tgdeferrable, t.tginitdeferred,
                              replace(pg_get_triggerdef(t.oid), current_schema() || '.', ''))
                  FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
                  JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = current_schema() AND NOT t.tgisinternal
                UNION ALL
                SELECT 6, c.relname, 0, 'SEQUENCE|' || c.relname
                  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = current_schema() AND c.relkind = 'S'
            )
            SELECT md5(string_agg(line, E'\n' ORDER BY category, object_name, ordinal, line))
              FROM catalog_lines"#,
            &[],
        )
        .await?
        .get(0);
    assert_eq!(catalog_digest, "0426433291ee97f40db0e46fddd3a373");

    let detail_triggers: Vec<String> = client
        .query_one(
            "SELECT array_agg(c.relname || ':' || t.tgname || ':' || t.tgdeferrable || ':' || t.tginitdeferred ORDER BY t.tgname)::text[]
             FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = current_schema() AND NOT t.tgisinternal
               AND t.tgname IN ('wr_node_operation_target_detail_required', 'wr_node_operation_engine_detail_required')",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(detail_triggers, vec![
        "wr_node_operation_engine_target_details:wr_node_operation_engine_detail_required:true:true".to_string(),
        "wr_node_operation_targets:wr_node_operation_target_detail_required:true:true".to_string(),
    ]);
    let function_exists: bool = client
        .query_one(
            "SELECT to_regprocedure('wr_check_node_operation_engine_target_detail()') IS NOT NULL",
            &[],
        )
        .await?
        .get(0);
    assert!(
        function_exists,
        "expected operation target-detail trigger function"
    );
    assert_eq!(
        client
            .query_one(
                "SELECT count(*) FROM wr_manager_lock WHERE id = 1 AND version = 0",
                &[]
            )
            .await?
            .get::<_, i64>(0),
        1
    );
    assert_eq!(
        client
            .query_one(
                "SELECT count(*) FROM wr_manager_rollout_guard WHERE singleton",
                &[]
            )
            .await?
            .get::<_, i64>(0),
        1
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
    let versions: Vec<i32> = client
        .query_one(
            "SELECT array_agg(version::integer ORDER BY version)
             FROM refinery_schema_history",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(
        versions,
        vec![1],
        "fresh manager history must contain only V1"
    );

    Ok(())
}
