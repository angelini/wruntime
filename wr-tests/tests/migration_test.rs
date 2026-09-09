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
                    AND to_regclass('wr_node_slot_owners') IS NOT NULL",
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
                  AND table_name = 'wr_manager_rollout_members'
                  AND column_name = 'expected_credential_digest'
            ) AND EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'wr_managers' AND column_name = 'policy_generation'
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

#[tokio::test]
async fn test_v30_terminalizes_legacy_cleanup_and_preserves_provenance() -> Result<()> {
    let pool = manager_pool_in_schema("mig_v30_cleanup_upgrade").await;
    let client = pool.get().await.context("V30 upgrade connection")?;
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
        include_str!("../../wr-manager/migrations/V17__node_agent_cutover.sql"),
        include_str!("../../wr-manager/migrations/V18__node_operation_result_receipts.sql"),
        include_str!("../../wr-manager/migrations/V19__node_agent_policy_and_retention.sql"),
        include_str!("../../wr-manager/migrations/V20__resolved_releases_and_proxy_operations.sql"),
        include_str!("../../wr-manager/migrations/V21__deployment_allocation_actor.sql"),
        include_str!("../../wr-manager/migrations/V22__job_admin_delegates.sql"),
        include_str!("../../wr-manager/migrations/V23__drop_manager_gossip_address.sql"),
        include_str!("../../wr-manager/migrations/V24__proxy_source_verification_step.sql"),
        include_str!("../../wr-manager/migrations/V25__manager_rollout_create_identity.sql"),
        include_str!("../../wr-manager/migrations/V26__manager_policy_rollout_state.sql"),
        include_str!("../../wr-manager/migrations/V27__fenced_engine_ownership.sql"),
        include_str!("../../wr-manager/migrations/V28__manager_rollout_artifact_evidence.sql"),
    ] {
        client.batch_execute(sql).await?;
    }

    let operation_id = uuid::Uuid::new_v4();
    client
        .execute(
            "INSERT INTO wr_nodes(node_id) VALUES ('legacy-cleanup-node')",
            &[],
        )
        .await?;
    client
        .execute(
            "INSERT INTO wr_node_operations
               (operation_id, node_id, request_token, actor, action, state,
                request_payload, policy, committed, committed_at, phase, forward_deadline)
             VALUES ($1, 'legacy-cleanup-node', 'legacy-cleanup', 'operator-a',
                     'rolling_upgrade', 'paused', '\\x01', '\\x02', TRUE, NOW(),
                     'committed_cleanup', NOW() + INTERVAL '5 minutes')",
            &[&operation_id],
        )
        .await?;
    client
        .execute(
            "INSERT INTO wr_node_operation_slots
               (operation_id, node_id, engine_slot, rollout_order, next_step)
             VALUES ($1, 'legacy-cleanup-node', 'blue', 0, 'cleanup_release')",
            &[&operation_id],
        )
        .await?;
    client
        .execute(
            "INSERT INTO wr_node_release_deletions
               (node_id, revision, bundle_digest, resolved_release_digest, operation_id)
             VALUES ('legacy-cleanup-node', 7, 'sha256:legacy', 'sha256:resolved', $1)",
            &[&operation_id],
        )
        .await?;

    client
        .batch_execute(include_str!(
            "../../wr-manager/migrations/V29__node_operation_termination_evidence.sql"
        ))
        .await?;
    client
        .batch_execute(include_str!(
            "../../wr-manager/migrations/V30__node_release_cleanup.sql"
        ))
        .await?;

    let operation = client
        .query_one(
            "SELECT phase, state, committed, committed_at IS NOT NULL AS committed_at
             FROM wr_node_operations WHERE operation_id = $1",
            &[&operation_id],
        )
        .await?;
    assert_eq!(operation.get::<_, String>("phase"), "complete");
    assert_eq!(operation.get::<_, String>("state"), "succeeded");
    assert!(operation.get::<_, bool>("committed"));
    assert!(operation.get::<_, bool>("committed_at"));
    let slot = client
        .query_one(
            "SELECT next_step, complete FROM wr_node_operation_slots WHERE operation_id = $1",
            &[&operation_id],
        )
        .await?;
    assert_eq!(slot.get::<_, String>("next_step"), "complete");
    assert!(slot.get::<_, bool>("complete"));
    let deletion = client
        .query_one(
            "SELECT operation_id, cleanup_generation FROM wr_node_release_deletions
             WHERE node_id = 'legacy-cleanup-node' AND revision = 7",
            &[],
        )
        .await?;
    assert_eq!(
        deletion.get::<_, Option<uuid::Uuid>>("operation_id"),
        Some(operation_id)
    );
    assert_eq!(deletion.get::<_, Option<i64>>("cleanup_generation"), None);
    let retired_columns: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM information_schema.columns
             WHERE table_schema = current_schema() AND table_name = 'wr_node_operations'
               AND column_name LIKE 'cleanup_%'",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(retired_columns, 0);

    let proxy_termination_evidence = vec![0x08, 0x01];
    let engine_termination_evidence = vec![0x08, 0x02];
    client
        .execute(
            "UPDATE wr_node_operations SET proxy_effect_termination_evidence = $2
             WHERE operation_id = $1",
            &[&operation_id, &proxy_termination_evidence],
        )
        .await?;
    client
        .execute(
            "UPDATE wr_node_operation_slots SET effect_termination_evidence = $2
             WHERE operation_id = $1 AND engine_slot = 'blue'",
            &[&operation_id, &engine_termination_evidence],
        )
        .await?;

    let active_id = uuid::Uuid::new_v4();
    client
        .execute(
            "INSERT INTO wr_node_operations
               (operation_id, node_id, request_token, actor, action, state,
                request_payload, policy, phase, forward_deadline)
             VALUES ($1, 'legacy-cleanup-node', 'active-v30', 'operator-a',
                     'restart', 'queued', '\\x01', '\\x02', 'forward',
                     NOW() + INTERVAL '5 minutes')",
            &[&active_id],
        )
        .await?;
    let rejected = client
        .batch_execute(include_str!(
            "../../wr-manager/migrations/V31__node_operation_targets.sql"
        ))
        .await
        .expect_err("V31 must reject nonterminal operations");
    assert!(rejected.as_db_error().is_some_and(|error| {
        error
            .message()
            .contains("requires all node operations to be terminal")
    }));
    client
        .execute(
            "UPDATE wr_node_operations SET state = 'failed', phase = 'complete'
             WHERE operation_id = $1",
            &[&active_id],
        )
        .await?;
    client
        .batch_execute(include_str!(
            "../../wr-manager/migrations/V31__node_operation_targets.sql"
        ))
        .await?;
    let converted: i64 = client
        .query_one(
            "SELECT count(*) FROM wr_node_operation_targets WHERE operation_id = $1",
            &[&operation_id],
        )
        .await?
        .get(0);
    assert_eq!(converted, 2);
    let converted_evidence = client
        .query(
            "SELECT target_kind, effect_termination_evidence
             FROM wr_node_operation_targets WHERE operation_id = $1 ORDER BY target_kind",
            &[&operation_id],
        )
        .await?;
    assert_eq!(
        converted_evidence[0].get::<_, Option<Vec<u8>>>("effect_termination_evidence"),
        Some(engine_termination_evidence)
    );
    assert_eq!(
        converted_evidence[1].get::<_, Option<Vec<u8>>>("effect_termination_evidence"),
        Some(proxy_termination_evidence)
    );
    assert!(client
        .query_one(
            "SELECT to_regclass('wr_node_operation_slots') IS NULL
                    AND EXISTS (
                        SELECT 1 FROM wr_node_release_deletions
                        WHERE node_id = 'legacy-cleanup-node' AND revision = 7
                          AND operation_id = $1 AND cleanup_generation IS NULL
                    )",
            &[&operation_id],
        )
        .await?
        .get::<_, bool>(0));

    client
        .execute(
            "INSERT INTO wr_node_agent_policies
           (node_id, protocol_version, config_digest, backend, retention_count, actor,
            policy_version, binary_digest, manager_endpoint, client_cert_path, client_key_path,
            ca_cert_path, deployment_root, runtime_dir, compose_project, systemctl_path,
            docker_path, poll_interval_seconds, renew_interval_seconds, capabilities)
         VALUES ('legacy-cleanup-node', 'operator-engine-lifecycle-v1', 'sha256:config',
                 'systemd', 7, 'operator-a', 1, $1, 'https://manager:9000', '/a', '/b',
                 '/c', '/opt/wruntime', '/run/wruntime', '', '/usr/bin/systemctl', '', 5, 5,
                 ARRAY['typed-backend-v1','continuous-lease-v1','typed-backend-v1'])",
            &[&format!("sha256:{}", "a".repeat(64))],
        )
        .await?;
    client
        .execute(
            "INSERT INTO wr_node_agent_attestations
           (node_id, agent_instance_id, authenticated_principal, protocol_version,
            binary_digest, config_digest, backend, capabilities, observed_at, retention_count)
         VALUES ('legacy-cleanup-node', 'activation-v31', 'agent-a',
                 'operator-engine-lifecycle-v1', $1, 'sha256:config', 'systemd',
                 ARRAY['typed-backend-v1','continuous-lease-v1','typed-backend-v1'], NOW(), 7)",
            &[&format!("sha256:{}", "a".repeat(64))],
        )
        .await?;
    client
        .batch_execute(include_str!(
            "../../wr-manager/migrations/V32__narrow_node_agent_attestation.sql"
        ))
        .await?;
    let narrow = client
        .query_one(
            "SELECT p.retention_count, p.capabilities, a.capabilities
         FROM wr_node_agent_policies p JOIN wr_node_agent_attestations a USING (node_id)
         WHERE p.node_id = 'legacy-cleanup-node'",
            &[],
        )
        .await?;
    assert_eq!(narrow.get::<_, i32>("retention_count"), 7);
    assert_eq!(
        narrow.get::<_, Vec<String>>(1),
        vec!["continuous-lease-v1", "typed-backend-v1"]
    );
    assert_eq!(
        narrow.get::<_, Vec<String>>(2),
        vec!["continuous-lease-v1", "typed-backend-v1"]
    );
    let broad_absent: bool = client.query_one(
        "SELECT NOT EXISTS (
             SELECT 1 FROM information_schema.columns
             WHERE table_schema = current_schema() AND table_name = 'wr_node_agent_policies'
               AND column_name IN ('config_digest','manager_endpoint','deployment_root','runtime_dir')
         ) AND NOT EXISTS (
             SELECT 1 FROM information_schema.columns
             WHERE table_schema = current_schema() AND table_name = 'wr_node_agent_attestations'
               AND column_name IN ('config_digest','retention_count')
         )",
        &[],
    ).await?.get(0);
    assert!(broad_absent);

    let rejected = client
        .batch_execute(include_str!(
            "../../wr-manager/migrations/V33__node_operation_action_matrix.sql"
        ))
        .await
        .expect_err("V33 must reject ambiguous legacy action history");
    assert!(rejected.as_db_error().is_some_and(|error| {
        error
            .message()
            .contains("cannot cut over legacy node operation actions")
    }));
    client
        .execute(
            "UPDATE wr_node_operations SET action = 'restart' WHERE operation_id = $1",
            &[&operation_id],
        )
        .await?;
    client
        .batch_execute(include_str!(
            "../../wr-manager/migrations/V33__node_operation_action_matrix.sql"
        ))
        .await?;
    let transition: String = client
        .query_one(
            "SELECT transition_kind FROM wr_node_operation_engine_target_details
             WHERE operation_id = $1 AND target_key = 'blue'",
            &[&operation_id],
        )
        .await?
        .get(0);
    assert_eq!(transition, "restart");
    for action in ["deployment", "rollback"] {
        client
            .execute(
                "INSERT INTO wr_node_operations
                   (operation_id, node_id, request_token, actor, action, state,
                    request_payload, policy, phase, forward_deadline)
                 VALUES ($1, 'legacy-cleanup-node', $2, 'operator-a', $3, 'failed',
                         '\\x01', '\\x02', 'complete', NOW() + INTERVAL '5 minutes')",
                &[&uuid::Uuid::new_v4(), &format!("valid-{action}"), &action],
            )
            .await?;
    }
    let invalid_action = client
        .execute(
            "UPDATE wr_node_operations SET action = 'drain' WHERE operation_id = $1",
            &[&operation_id],
        )
        .await
        .expect_err("retired actions must be rejected");
    assert!(invalid_action.as_db_error().is_some());
    let invalid_transition = client
        .execute(
            "UPDATE wr_node_operation_engine_target_details SET transition_kind = 'invalid'
             WHERE operation_id = $1 AND target_key = 'blue'",
            &[&operation_id],
        )
        .await
        .expect_err("unknown transitions must be rejected");
    assert!(invalid_transition.as_db_error().is_some());

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
    let tail: Vec<i32> = client
        .query_one(
            "SELECT array_agg(version::integer ORDER BY applied_on, version)
             FROM refinery_schema_history WHERE version IN (29, 30, 31, 32, 33)",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(
        tail,
        vec![29, 30, 31, 32, 33],
        "V33 must follow preserved V29 through V32"
    );

    Ok(())
}
