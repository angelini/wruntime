mod helpers;
use helpers::{
    db::manager_pool,
    manager::{
        get_default_rule_health, manager_client, register_test_module_ready, start_manager_cluster,
    },
    proxy::TEST_SELF_PEER,
    wait::{
        wait_for_default_rule_health, wait_for_manager_absent, wait_for_manager_count,
        DEFAULT_WAIT_TIMEOUT,
    },
    wasm::minimal_file_descriptor_set,
};

use std::time::Duration;

use wr_common::wruntime::{
    BackendProcessState, BeginDeploymentRequest, EngineRegistration, ExpectedEngine,
    FinalizeDeploymentRequest, GetClusterStatusRequest, HeartbeatRequest, LifecycleStatus,
    ListManagersRequest, ModuleDescriptor, NodeOperationAction, NodeOperationStepKind,
    ProcessLifecycleState, RegisterEngineRequest, ReportNodeObservationRequest,
    ReportStepResultRequest, RolloutPolicy, ServiceKind, SubmitOperationRequest,
};

// ── Multi-manager integration tests ──────────────────────────────────────────
//
// These tests verify PostgreSQL-backed health and manager lease visibility
// across multiple managers sharing the same control plane.

#[tokio::test]
async fn test_operation_survives_manager_loss_and_reconciles_without_repeating_effect() {
    let pool = manager_pool().await;
    let managers = start_manager_cluster(pool.clone(), 2, 30).await.unwrap();
    let digest = format!("sha256:{}", "7".repeat(64));
    let resolved = format!("sha256:{}", "8".repeat(64));
    let deployment = wr_manager::db::begin_deployment(
        &pool,
        &BeginDeploymentRequest {
            node_id: "takeover-node".into(),
            attempt_token: "takeover-operation".into(),
            bundle_digest: digest.clone(),
            expected_engines: vec![ExpectedEngine {
                engine_slot: "blue".into(),
                modules: vec![],
            }],
        },
        "operator-a",
    )
    .await
    .unwrap()
    .record;
    wr_manager::db::finalize_deployment(
        &pool,
        &FinalizeDeploymentRequest {
            node_id: "takeover-node".into(),
            attempt_token: "takeover-operation".into(),
            revision: deployment.revision,
            bundle_digest: digest.clone(),
            resolved_release_digest: resolved.clone(),
        },
        "operator-a",
    )
    .await
    .unwrap();
    let policy = helpers::node_agent::systemd_policy("takeover-node", 2);
    wr_manager::operations::put_agent_policy(&pool, "operator-a", &policy)
        .await
        .unwrap();
    assert!(wr_manager::operations::attest(
        &pool,
        "agent-a",
        &helpers::node_agent::attestation(&policy, "activation-a"),
    )
    .await
    .unwrap()
    .is_empty());
    let submitted = wr_manager::operations::submit(
        &pool,
        "operator-a",
        &SubmitOperationRequest {
            node_id: "takeover-node".into(),
            request_token: "takeover-operation".into(),
            action: NodeOperationAction::InitialApply as i32,
            engine_slots: vec!["blue".into()],
            target_revision: deployment.revision,
            bundle_digest: digest.clone(),
            resolved_release_digest: resolved.clone(),
            policy: Some(RolloutPolicy {
                max_unavailable: 1,
                canary_slot: String::new(),
                pause_after_canary: false,
                allow_downtime: true,
                deadline_seconds: 300,
            }),
        },
    )
    .await
    .unwrap();

    let select = wr_manager::operations::claim(&pool, "takeover-node", "activation-a", "agent-a")
        .await
        .unwrap()
        .unwrap()
        .instruction
        .unwrap();
    assert_eq!(select.step, NodeOperationStepKind::SelectRelease as i32);
    let target = select.target.as_ref().unwrap();
    wr_manager::operations::report_step(
        &pool,
        &ReportStepResultRequest {
            node_id: select.node_id.clone(),
            operation_id: select.operation_id.clone(),
            lease_epoch: select.lease_epoch,
            step: select.step,
            agent_instance_id: select.agent_instance_id.clone(),
            observed_revision: target.revision,
            observed_digest: target.bundle_digest.clone(),
            observed_resolved_release_digest: target.resolved_release_digest.clone(),
            backend_instance_id: "proxy-old".into(),
            process_instance_id: "proxy-process-old".into(),
            ..Default::default()
        },
        "agent-a",
    )
    .await
    .unwrap();
    let start = wr_manager::operations::claim(&pool, "takeover-node", "activation-a", "agent-a")
        .await
        .unwrap()
        .unwrap()
        .instruction
        .unwrap();
    assert_eq!(start.step, NodeOperationStepKind::StartBackend as i32);
    let before = wr_manager::operations::get(&pool, &submitted.operation_id)
        .await
        .unwrap();
    let deadline = before.forward_deadline;
    let events_before = wr_manager::operations::events(&pool, &submitted.operation_id)
        .await
        .unwrap();
    assert!(before.proxy_effect_delivered_at.is_some());

    managers[0].abort_service();
    assert!(manager_client(&managers[1].addr).await.is_ok());
    pool.get()
        .await
        .unwrap()
        .execute(
            "UPDATE wr_node_operations SET lease_expires_at = NOW() - INTERVAL '1 second' WHERE operation_id = $1",
            &[&uuid::Uuid::parse_str(&submitted.operation_id).unwrap()],
        )
        .await
        .unwrap();
    let stale = wr_manager::operations::report_step(
        &pool,
        &ReportStepResultRequest {
            node_id: start.node_id.clone(),
            operation_id: start.operation_id.clone(),
            lease_epoch: start.lease_epoch,
            step: start.step,
            agent_instance_id: start.agent_instance_id.clone(),
            ..Default::default()
        },
        "agent-a",
    )
    .await
    .expect_err("expired manager-A lease must be fenced");
    assert_eq!(stale.code(), tonic::Code::Aborted);
    assert!(
        wr_manager::operations::claim(&pool, "takeover-node", "activation-a", "agent-a")
            .await
            .unwrap()
            .is_none()
    );

    wr_manager::operations::resume(&pool, &submitted.operation_id, "operator-b")
        .await
        .unwrap();
    assert!(wr_manager::operations::attest(
        &pool,
        "agent-b",
        &helpers::node_agent::attestation(&policy, "activation-b"),
    )
    .await
    .unwrap()
    .is_empty());
    let inspection =
        wr_manager::operations::claim(&pool, "takeover-node", "activation-b", "agent-b")
            .await
            .unwrap()
            .unwrap()
            .instruction
            .unwrap();
    assert_eq!(
        inspection.step,
        NodeOperationStepKind::InspectBackend as i32
    );
    assert!(inspection.lease_epoch > start.lease_epoch);
    let target = inspection.target.as_ref().unwrap();
    wr_manager::operations::report_observation(
        &pool,
        &ReportNodeObservationRequest {
            node_id: inspection.node_id.clone(),
            operation_id: inspection.operation_id.clone(),
            agent_instance_id: inspection.agent_instance_id.clone(),
            lease_epoch: inspection.lease_epoch,
            lifecycle: Some(LifecycleStatus {
                state: ProcessLifecycleState::Ready as i32,
                service_kind: ServiceKind::Proxy as i32,
                process_instance_id: "proxy-process-new".into(),
                ..Default::default()
            }),
            backend_state: BackendProcessState::Running as i32,
            backend_instance_id: "proxy-new".into(),
            observed_revision: target.revision,
            observed_digest: target.bundle_digest.clone(),
            observed_resolved_release_digest: target.resolved_release_digest.clone(),
            ..Default::default()
        },
        "agent-b",
    )
    .await
    .unwrap();
    let continued =
        wr_manager::operations::claim(&pool, "takeover-node", "activation-b", "agent-b")
            .await
            .unwrap()
            .unwrap()
            .instruction
            .unwrap();
    assert_eq!(
        continued.step,
        NodeOperationStepKind::InspectBackend as i32,
        "takeover may repeat read-only inspection but must not repeat StartBackend"
    );
    let after = wr_manager::operations::get(&pool, &submitted.operation_id)
        .await
        .unwrap();
    assert_eq!(after.forward_deadline, deadline);
    assert!(after.proxy_effect_delivered_at.is_some());
    let events_after = wr_manager::operations::events(&pool, &submitted.operation_id)
        .await
        .unwrap();
    assert!(events_after.len() > events_before.len());
    assert!(events_after
        .windows(2)
        .all(|pair| pair[0].sequence < pair[1].sequence));
}

/// Engine heartbeats to manager-1; manager-2 sees the engine as healthy
/// immediately via shared Postgres.
#[tokio::test]
async fn test_heartbeat_visible_across_managers() {
    let pool = manager_pool().await;
    let managers = start_manager_cluster(pool.clone(), 2, 30).await.unwrap();

    // Register engine + routing rule via manager-1
    let mut c1 = manager_client(&managers[0].addr).await.unwrap();
    register_test_module_ready(
        &pool,
        &mut c1,
        "engine-1",
        "http://127.0.0.1:19100",
        "ns",
        "svc",
        "1.0.0",
    )
    .await
    .unwrap();

    // Shared database writes are immediately visible.

    // Manager-2 can see the healthy rule via the shared DB.
    let mut c2 = manager_client(&managers[1].addr).await.unwrap();
    let (healthy, _) = get_default_rule_health(&mut c2, "engine-1", "ns", "svc", "1.0.0")
        .await
        .unwrap();
    assert!(
        healthy,
        "rule should be healthy (heartbeat written to shared Postgres)"
    );
}

#[tokio::test]
async fn test_deployment_desired_state_is_visible_across_managers() {
    let pool = manager_pool().await;
    let managers = start_manager_cluster(pool.clone(), 2, 30).await.unwrap();
    let mut first = manager_client(&managers[0].addr).await.unwrap();
    let deployment = wr_manager::db::begin_deployment(
        &pool,
        &BeginDeploymentRequest {
            node_id: "shared-node".into(),
            attempt_token: "shared-attempt".into(),
            bundle_digest: format!("sha256:{}", "3".repeat(64)),
            expected_engines: vec![ExpectedEngine {
                engine_slot: "primary".into(),
                modules: vec![],
            }],
        },
        "operator-a",
    )
    .await
    .unwrap()
    .record;

    let conditions = wr_manager::db::deployment_conditions(&pool, &deployment, 30.0, 30.0)
        .await
        .unwrap();
    assert_eq!(deployment.revision, 1);
    assert_eq!(conditions[0].0, "MISSING_ENGINE");

    let mut second = manager_client(&managers[1].addr).await.unwrap();

    let first_status = first
        .get_cluster_status(GetClusterStatusRequest {})
        .await
        .unwrap()
        .into_inner();
    let second_status = second
        .get_cluster_status(GetClusterStatusRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        first_status.routing_table_version,
        second_status.routing_table_version
    );
    assert_eq!(first_status.nodes, second_status.nodes);
    assert_eq!(first_status.engines, second_status.engines);
    assert_eq!(first_status.services, second_status.services);
    assert!(first_status.response_at.is_some());
    assert!(second_status.response_at.is_some());
}

/// Engine heartbeats to manager-1; manager-2 can also verify health via
/// the routing table (rule stays healthy).
#[tokio::test]
async fn test_health_preserved_across_managers() {
    let pool = manager_pool().await;
    let managers = start_manager_cluster(pool.clone(), 2, 2).await.unwrap();

    let mut c1 = manager_client(&managers[0].addr).await.unwrap();
    register_test_module_ready(
        &pool,
        &mut c1,
        "engine-2",
        "http://127.0.0.1:19200",
        "ns",
        "svc2",
        "1.0.0",
    )
    .await
    .unwrap();

    // Check via manager-2 that the rule is still healthy
    let mut c2 = manager_client(&managers[1].addr).await.unwrap();
    let (healthy, _) = get_default_rule_health(&mut c2, "engine-2", "ns", "svc2", "1.0.0")
        .await
        .unwrap();
    assert!(
        healthy,
        "rule should be healthy (heartbeat in shared Postgres)"
    );
}

/// When heartbeats stop, all managers eventually detect the unhealthy state.
#[tokio::test]
async fn test_health_convergence_on_missed_heartbeat() {
    let pool = manager_pool().await;
    // 1-second timeout so unhealthy detection is fast
    let managers = start_manager_cluster(pool.clone(), 2, 1).await.unwrap();

    let mut c1 = manager_client(&managers[0].addr).await.unwrap();
    register_test_module_ready(
        &pool,
        &mut c1,
        "engine-3",
        "http://127.0.0.1:19300",
        "ns",
        "svc3",
        "1.0.0",
    )
    .await
    .unwrap();

    // Verify healthy via manager-2
    let mut c2 = manager_client(&managers[1].addr).await.unwrap();
    let (healthy, _) = wait_for_default_rule_health(
        &mut c2,
        "engine-3",
        "ns",
        "svc3",
        "1.0.0",
        true,
        DEFAULT_WAIT_TIMEOUT,
    )
    .await
    .unwrap();
    assert!(healthy, "should be healthy after heartbeat");

    // Stop heartbeating — wait for timeout + monitor cycle
    let (healthy, _) = wait_for_default_rule_health(
        &mut c2,
        "engine-3",
        "ns",
        "svc3",
        "1.0.0",
        false,
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert!(!healthy, "should be unhealthy after missed heartbeat");
}

/// A one-manager cluster can register a module, process heartbeats, and keep routes healthy.
#[tokio::test]
async fn test_single_manager_cluster() {
    let pool = manager_pool().await;
    let managers = start_manager_cluster(pool.clone(), 1, 30).await.unwrap();

    let mut c = manager_client(&managers[0].addr).await.unwrap();
    register_test_module_ready(
        &pool,
        &mut c,
        "engine-solo",
        "http://127.0.0.1:19400",
        "ns",
        "solo",
        "1.0.0",
    )
    .await
    .unwrap();

    let (healthy, _) = get_default_rule_health(&mut c, "engine-solo", "ns", "solo", "1.0.0")
        .await
        .unwrap();
    assert!(healthy, "single-manager cluster should work normally");
}

/// Manager self-registration in wr_managers table works correctly.
#[tokio::test]
async fn test_manager_self_registration() {
    let pool = manager_pool().await;
    let _managers = start_manager_cluster(pool.clone(), 2, 30).await.unwrap();

    // Query wr_managers directly — should have 2 rows
    let client = pool.get().await.unwrap();
    let rows = client
        .query("SELECT manager_id, grpc_address FROM wr_managers", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 2, "two managers should be registered");

    // Both should have non-empty runtime addresses.
    for row in &rows {
        let grpc: String = row.get(1);
        assert!(grpc.starts_with("http://"), "grpc_address should be a URL");
    }
}

/// Module-level health converges across managers via shared Postgres: an engine
/// reports only one of its two modules; the module whose heartbeat ages out has
/// its route marked unhealthy, and a second manager observes the same outcome.
#[tokio::test]
async fn test_module_health_convergence_across_managers() {
    let pool = manager_pool().await;
    let managers = start_manager_cluster(pool.clone(), 2, 1).await.unwrap();

    let mut c1 = manager_client(&managers[0].addr).await.unwrap();
    c1.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "mm-e1".into(),
            address: "http://127.0.0.1:19500".into(),
            proxy_address: TEST_SELF_PEER.into(),
            peer_address: TEST_SELF_PEER.into(),
            modules: vec![
                ModuleDescriptor {
                    name: "mm-a".into(),
                    namespace: "mm-ns".into(),
                    version: "1.0.0".into(),
                    proto_schema: minimal_file_descriptor_set(),
                },
                ModuleDescriptor {
                    name: "mm-b".into(),
                    namespace: "mm-ns".into(),
                    version: "1.0.0".into(),
                    proto_schema: minimal_file_descriptor_set(),
                },
            ],
            secrets: vec![],
            db_namespaces: vec![],
            deployment: None,
            job_queue_id: String::new(),
            job_admin_address: String::new(),
        }),
    })
    .await
    .unwrap();

    // Intentional elapsed-time interval: heartbeat only mm-a for longer than the 1s timeout.
    let mut interval = tokio::time::interval(Duration::from_millis(200));
    for _ in 0..8 {
        c1.heartbeat(HeartbeatRequest {
            engine_id: "mm-e1".into(),
            healthy_modules: vec![ModuleDescriptor {
                name: "mm-a".into(),
                namespace: "mm-ns".into(),
                version: "1.0.0".into(),
                proto_schema: vec![],
            }],
        })
        .await
        .unwrap();
        interval.tick().await;
    }

    // Manager-2 sees the shared outcome.
    let mut c2 = manager_client(&managers[1].addr).await.unwrap();
    wait_for_default_rule_health(
        &mut c2,
        "mm-e1",
        "mm-ns",
        "mm-a",
        "1.0.0",
        true,
        DEFAULT_WAIT_TIMEOUT,
    )
    .await
    .unwrap();
    wait_for_default_rule_health(
        &mut c2,
        "mm-e1",
        "mm-ns",
        "mm-b",
        "1.0.0",
        false,
        DEFAULT_WAIT_TIMEOUT,
    )
    .await
    .unwrap();
    let (a_healthy, _) = get_default_rule_health(&mut c2, "mm-e1", "mm-ns", "mm-a", "1.0.0")
        .await
        .unwrap();
    let (b_healthy, _) = get_default_rule_health(&mut c2, "mm-e1", "mm-ns", "mm-b", "1.0.0")
        .await
        .unwrap();
    assert!(a_healthy, "reported module healthy via shared Postgres");
    assert!(!b_healthy, "omitted module unhealthy via shared Postgres");
}

#[tokio::test]
async fn test_single_manager_list_managers_returns_self() {
    let pool = manager_pool().await;
    let managers = start_manager_cluster(pool.clone(), 1, 30).await.unwrap();
    let mut c = manager_client(&managers[0].addr).await.unwrap();

    let infos = c
        .list_managers(ListManagersRequest {})
        .await
        .unwrap()
        .into_inner()
        .managers;

    assert_eq!(infos.len(), 1);
    assert_eq!(infos[0].manager_id, managers[0].manager_id);
    assert!(!infos[0].grpc_address.is_empty());
}

#[tokio::test]
async fn test_list_managers_converges_with_expected_identities_and_addresses_from_each_seed() {
    let pool = manager_pool().await;
    let managers = start_manager_cluster(pool.clone(), 2, 30).await.unwrap();
    let mut expected = managers
        .iter()
        .map(|manager| (manager.manager_id.clone(), manager.addr.clone()))
        .collect::<Vec<_>>();
    expected.sort();

    for seed in &managers {
        let mut client = manager_client(&seed.addr).await.unwrap();
        let infos = wait_for_manager_count(&mut client, 2, Duration::from_secs(10))
            .await
            .unwrap();
        let mut observed = infos
            .into_iter()
            .map(|info| (info.manager_id, info.grpc_address))
            .collect::<Vec<_>>();
        observed.sort();

        assert_eq!(
            observed, expected,
            "seed {} must report both exact runtime identity/address pairs",
            seed.addr
        );
    }
}

#[tokio::test]
async fn test_blocked_stale_reaper_does_not_suspend_self_heartbeat() {
    let pool = manager_pool().await;
    wr_manager::db::register_manager(&pool, "live-manager", "http://127.0.0.1:9000")
        .await
        .unwrap();
    wr_manager::db::register_manager(&pool, "stale-manager", "http://127.0.0.1:9001")
        .await
        .unwrap();
    pool.get()
        .await
        .unwrap()
        .execute(
            "UPDATE wr_managers SET last_heartbeat = NOW() - INTERVAL '10 seconds' WHERE manager_id = $1",
            &[&"stale-manager"],
        )
        .await
        .unwrap();

    let mut blocker = pool.get().await.unwrap();
    let transaction = blocker.transaction().await.unwrap();
    transaction
        .query_one(
            "SELECT manager_id FROM wr_managers WHERE manager_id = $1 FOR UPDATE",
            &[&"stale-manager"],
        )
        .await
        .unwrap();
    let before: f64 = pool
        .get()
        .await
        .unwrap()
        .query_one(
            "SELECT EXTRACT(EPOCH FROM last_heartbeat)::double precision FROM wr_managers WHERE manager_id = $1",
            &[&"live-manager"],
        )
        .await
        .unwrap()
        .get(0);

    let admission = wr_common::lifecycle_service::AdmissionGate::closed();
    admission.open();
    let mut tasks = wr_common::task_group::TaskGroup::new();
    let heartbeat_pool = pool.clone();
    tasks.spawn("test-manager-heartbeat", move |cancellation| {
        wr_manager::db::run_manager_heartbeat_owned(
            heartbeat_pool,
            "live-manager".into(),
            Duration::from_millis(20),
            admission,
            cancellation,
        )
    });
    let reaper_pool = pool.clone();
    tasks.spawn("test-manager-reaper", move |cancellation| {
        wr_manager::db::run_stale_manager_reaper_owned(
            reaper_pool,
            1,
            Duration::from_millis(10),
            cancellation,
        )
    });

    tokio::time::sleep(Duration::from_millis(150)).await;
    let after: f64 = pool
        .get()
        .await
        .unwrap()
        .query_one(
            "SELECT EXTRACT(EPOCH FROM last_heartbeat)::double precision FROM wr_managers WHERE manager_id = $1",
            &[&"live-manager"],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        after > before,
        "self heartbeat must advance while stale-row cleanup is lock-blocked"
    );

    transaction.rollback().await.unwrap();
    let report = tasks
        .shutdown(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert!(report.is_clean(), "{report:?}");
}

#[tokio::test]
async fn test_stale_manager_lease_is_unlisted_without_reaping_row() {
    let pool = manager_pool().await;
    let managers = start_manager_cluster(pool.clone(), 2, 30).await.unwrap();
    let survivor = &managers[0];
    let victim = &managers[1];
    let mut c = manager_client(&survivor.addr).await.unwrap();

    wait_for_manager_count(&mut c, 2, Duration::from_secs(10))
        .await
        .unwrap();

    pool.get()
        .await
        .unwrap()
        .execute(
            "UPDATE wr_managers SET last_heartbeat = NOW() - INTERVAL '10 seconds' WHERE manager_id = $1",
            &[&victim.manager_id],
        )
        .await
        .unwrap();

    wait_for_manager_absent(&mut c, &victim.manager_id, Duration::from_secs(5))
        .await
        .unwrap();
    let status = c
        .get_cluster_status(GetClusterStatusRequest {})
        .await
        .unwrap()
        .into_inner();
    let victim_status = status
        .managers
        .iter()
        .find(|manager| manager.manager_id == victim.manager_id)
        .expect("stale manager row remains visible in composed status");
    assert_eq!(
        victim_status.membership,
        wr_common::wruntime::ManagerMembershipState::Dead as i32
    );
    assert_eq!(victim_status.conditions[0].code, "STALE_MANAGER_HEARTBEAT");

    let retained: bool = pool
        .get()
        .await
        .unwrap()
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM wr_managers WHERE manager_id = $1)",
            &[&victim.manager_id],
        )
        .await
        .unwrap()
        .get(0);
    assert!(retained, "stale liveness must not immediately reap the row");
    assert_eq!(
        wr_manager::db::cleanup_stale_managers(&pool, 300)
            .await
            .unwrap(),
        0,
        "the long reap threshold must retain a row already excluded from liveness"
    );
    assert_eq!(
        wr_manager::db::cleanup_stale_managers(&pool, 5)
            .await
            .unwrap(),
        1,
        "reaping uses its own configured threshold"
    );
}
