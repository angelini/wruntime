mod helpers;
use helpers::{
    manager::{
        backdate_engine_heartbeat, get_default_rule_health, get_routing_table_version,
        manager_trio_with_monitor, register_test_module_raw, register_test_module_ready,
        sync_table, synced_routing_table,
    },
    proxy::{proxy_get, start_proxy, TEST_SELF_PEER},
    stubs::spawn_stub_engine,
    wait::{
        wait_for_default_rule_health, wait_for_routing_table_version_gt, wait_for_rule_health,
        DEFAULT_WAIT_TIMEOUT,
    },
    wasm::minimal_file_descriptor_set,
};

use anyhow::Result;
use http::StatusCode;

use wr_common::wruntime::{
    BeginDeploymentRequest, DeploymentInventoryV1, DeploymentMetadata, EngineRegistration,
    ExpectedEngine, ExpectedModule, FinalizeDeploymentRequest, HeartbeatRequest, ModuleDescriptor,
    NodeOperationAction, RolloutPolicy, RoutingRule, SubmitOperationRequest,
};

#[tokio::test]
async fn staged_registration_remains_non_serving_without_exact_slot_authority() -> Result<()> {
    let pool = helpers::db::manager_pool().await;
    let digest = format!("sha256:{}", "a".repeat(64));
    let deployment = wr_manager::db::begin_deployment(
        &pool,
        &BeginDeploymentRequest {
            node_id: "staged-authority-node".into(),
            attempt_token: "staged-authority".into(),
            bundle_digest: digest.clone(),
            inventory: Some(DeploymentInventoryV1 {
                schema_version: 1,
                engines: vec![ExpectedEngine {
                    engine_slot: "blue".into(),
                    modules: vec![ExpectedModule {
                        identity: Some(wr_common::wruntime::ModuleIdentity {
                            namespace: "staged-ns".into(),
                            name: "staged-service".into(),
                            version: "1.0.0".into(),
                        }),
                        proto_schema_digest: wr_common::deployment_contract::schema_digest(
                            &minimal_file_descriptor_set(),
                        ),
                    }],
                    ..Default::default()
                }],
            }),
        },
        "operator-a",
    )
    .await?
    .record;
    let finalized = wr_manager::db::finalize_deployment(
        &pool,
        &FinalizeDeploymentRequest {
            node_id: deployment.node_id,
            attempt_token: deployment.attempt_token,
            revision: deployment.revision,
            bundle_digest: digest.clone(),
            resolved_release_digest: format!("sha256:{}", "b".repeat(64)),
        },
        "operator-a",
    )
    .await?
    .record;
    let operation = wr_manager::operations::submit(
        &pool,
        "operator-a",
        &SubmitOperationRequest {
            node_id: finalized.node_id.clone(),
            request_token: "staged-authority".into(),
            action: NodeOperationAction::Deployment as i32,
            engine_slot: String::new(),
            target_revision: finalized.revision,
            bundle_digest: finalized.bundle_digest.clone(),
            policy: Some(RolloutPolicy {
                max_unavailable: 1,
                allow_downtime: true,
                deadline_seconds: 300,
            }),
            resolved_release_digest: finalized.resolved_release_digest.clone(),
        },
    )
    .await?;
    let module = ModuleDescriptor {
        name: "staged-service".into(),
        version: "1.0.0".into(),
        proto_schema: minimal_file_descriptor_set(),
        namespace: "staged-ns".into(),
    };
    let password = wr_manager::crypto::SecretCrypto::generate_random_password();
    let crypto = wr_manager::crypto::SecretCrypto::from_hex(&password)?;
    let activation_id = uuid::Uuid::new_v4().to_string();
    let registration = wr_manager::db::register_engine_and_routes(
        &pool,
        &crypto,
        &EngineRegistration {
            engine_id: "staged-engine".into(),
            address: "http://127.0.0.1:19100".into(),
            modules: vec![module.clone()],
            proxy_address: "http://127.0.0.1:19001".into(),
            secrets: vec![],
            peer_address: TEST_SELF_PEER.into(),
            db_namespaces: vec![],
            job_queue_id: String::new(),
            job_admin_address: String::new(),
            deployment: Some(DeploymentMetadata {
                node_id: finalized.node_id,
                revision: finalized.revision,
                bundle_digest: digest,
                engine_slot: "blue".into(),
                operation_id: operation.operation_id,
                revision_digest: finalized.revision_digest,
            }),
        },
        &activation_id,
    )
    .await?;
    wr_manager::db::publish_engine_readiness(
        &pool,
        "staged-engine",
        &[module],
        &registration.fence,
    )
    .await?;
    wr_manager::db::update_route_health(&pool, 30.0, 30.0).await?;
    let table = wr_manager::db::get_routing_table(&pool, 0)
        .await?
        .expect("routing table");
    assert_eq!(table.rules.len(), 1);
    assert!(
        !table.rules[0].healthy,
        "staged registration must not serve"
    );

    Ok(())
}

#[tokio::test]
async fn route_health_publication_participates_in_operation_evidence_lock() -> Result<()> {
    let (pool, _addr, mut manager) = helpers::manager::manager_trio().await?;
    let module = ModuleDescriptor {
        name: "serialized-service".into(),
        version: "1.0.0".into(),
        proto_schema: minimal_file_descriptor_set(),
        namespace: "serialized-ns".into(),
    };
    let response = helpers::manager::register_managed_engine(
        &pool,
        &mut manager,
        EngineRegistration {
            engine_id: "serialized-engine".into(),
            address: "http://127.0.0.1:19101".into(),
            modules: vec![module.clone()],
            proxy_address: "http://127.0.0.1:19001".into(),
            secrets: vec![],
            peer_address: TEST_SELF_PEER.into(),
            db_namespaces: vec![],
            job_queue_id: String::new(),
            job_admin_address: String::new(),
            deployment: None,
        },
    )
    .await?;
    let fence = response.into_inner().fence.expect("registration fence");
    wr_manager::db::publish_engine_readiness(&pool, "serialized-engine", &[module], &fence).await?;
    backdate_engine_heartbeat(&pool, "serialized-engine", 60).await;

    let mut holder = pool.get().await?;
    let transaction = holder.transaction().await?;
    transaction
        .query_one(
            "SELECT version FROM wr_manager_lock WHERE id = 1 FOR UPDATE",
            &[],
        )
        .await?;
    let publisher_pool = pool.clone();
    let mut publisher = tokio::spawn(async move {
        wr_manager::db::update_route_health(&publisher_pool, 1.0, 1.0).await
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut publisher)
            .await
            .is_err(),
        "route health must not update outside the shared evidence lock"
    );
    let healthy: bool = pool
        .get()
        .await?
        .query_one(
            "SELECT healthy FROM wr_routing_rules
             WHERE rule_id = 'serialized-engine/serialized-ns/serialized-service/1.0.0'",
            &[],
        )
        .await?
        .get(0);
    assert!(healthy, "blocked publisher has not changed route evidence");
    transaction.commit().await?;
    let (stale, recovered) = publisher.await??;
    assert_eq!(
        stale,
        vec!["serialized-engine/serialized-ns/serialized-service/1.0.0"]
    );
    assert!(recovered.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_heartbeat_timeout_marks_module_unhealthy() -> Result<()> {
    let (pool, _mgr_addr, mut mgr) = manager_trio_with_monitor(1).await?;

    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;
    register_test_module_ready(
        &pool,
        &mut mgr,
        "hc-e1",
        &engine_addr,
        "hc-ns",
        "heartbeat-svc",
        "1.0.0",
    )
    .await?;

    let (healthy, _) =
        get_default_rule_health(&mut mgr, "hc-e1", "hc-ns", "heartbeat-svc", "1.0.0").await?;
    assert!(
        healthy,
        "module should be healthy after readiness heartbeat"
    );

    // Backdate the engine heartbeat so the monitor considers it stale.
    backdate_engine_heartbeat(&pool, "hc-e1", 60).await;

    let (healthy, _) = wait_for_default_rule_health(
        &mut mgr,
        "hc-e1",
        "hc-ns",
        "heartbeat-svc",
        "1.0.0",
        false,
        DEFAULT_WAIT_TIMEOUT,
    )
    .await?;
    assert!(
        !healthy,
        "module should be unhealthy after heartbeat timeout"
    );

    let _ = engine_shutdown.send(());
    Ok(())
}

/// A heartbeat refreshes the engine's last_heartbeat timestamp and prevents
/// the monitor from marking its routing rules unhealthy.
#[tokio::test]
async fn test_heartbeat_keeps_module_healthy() -> Result<()> {
    let (pool, _mgr_addr, mut mgr) = manager_trio_with_monitor(2).await?;

    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;
    let fence = register_test_module_ready(
        &pool,
        &mut mgr,
        "hc-keep-e1",
        &engine_addr,
        "hc-keep-ns",
        "kept-svc",
        "1.0.0",
    )
    .await?;

    // Intentional elapsed-time interval: this test proves repeated heartbeats keep the route healthy across monitor ticks.
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(300));
    for _ in 0..5 {
        mgr.heartbeat(HeartbeatRequest {
            engine_id: "hc-keep-e1".into(),
            healthy_modules: vec![ModuleDescriptor {
                name: "kept-svc".into(),
                namespace: "hc-keep-ns".into(),
                version: "1.0.0".into(),
                proto_schema: vec![],
            }],

            fence: Some(fence.clone()),
        })
        .await?;
        interval.tick().await;
    }

    let (healthy, _) =
        get_default_rule_health(&mut mgr, "hc-keep-e1", "hc-keep-ns", "kept-svc", "1.0.0").await?;
    assert!(healthy, "module should remain healthy with heartbeats");

    let _ = engine_shutdown.send(());
    Ok(())
}

/// When an engine's heartbeat goes stale and then a fresh heartbeat arrives,
/// the monitor recovers the routing rules.
#[tokio::test]
async fn test_engine_health_recovery_after_heartbeat() -> Result<()> {
    let (pool, _mgr_addr, mut mgr) = manager_trio_with_monitor(1).await?;

    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;
    let fence = register_test_module_ready(
        &pool,
        &mut mgr,
        "hc-rec-e1",
        &engine_addr,
        "hc-rec-ns",
        "recovering-svc",
        "1.0.0",
    )
    .await?;

    // Backdate engine heartbeat so the monitor marks it unhealthy.
    backdate_engine_heartbeat(&pool, "hc-rec-e1", 60).await;

    let (healthy, _) = wait_for_default_rule_health(
        &mut mgr,
        "hc-rec-e1",
        "hc-rec-ns",
        "recovering-svc",
        "1.0.0",
        false,
        DEFAULT_WAIT_TIMEOUT,
    )
    .await?;
    assert!(!healthy, "module should be unhealthy before recovery");

    // Send a heartbeat — refreshes last_heartbeat in the DB.
    mgr.heartbeat(HeartbeatRequest {
        engine_id: "hc-rec-e1".into(),
        healthy_modules: vec![ModuleDescriptor {
            name: "recovering-svc".into(),
            namespace: "hc-rec-ns".into(),
            version: "1.0.0".into(),
            proto_schema: vec![],
        }],

        fence: Some(fence.clone()),
    })
    .await?;

    let (healthy, _) = wait_for_default_rule_health(
        &mut mgr,
        "hc-rec-e1",
        "hc-rec-ns",
        "recovering-svc",
        "1.0.0",
        true,
        DEFAULT_WAIT_TIMEOUT,
    )
    .await?;
    assert!(healthy, "module should recover after heartbeat");

    let _ = engine_shutdown.send(());
    Ok(())
}

/// An unhealthy module is excluded from proxy routing — requests get 503.
#[tokio::test]
async fn test_unhealthy_module_excluded_from_routing() -> Result<()> {
    let (pool, mgr_addr, mut mgr) = manager_trio_with_monitor(1).await?;

    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;
    register_test_module_ready(
        &pool,
        &mut mgr,
        "hc-route-e1",
        &engine_addr,
        "hc-route-ns",
        "routed-svc",
        "1.0.0",
    )
    .await?;

    let table = synced_routing_table(&mgr_addr).await?;
    let initial_proxy_version = table.version().await;
    let proxy = start_proxy(table.clone()).await?;

    // Module is healthy — routing should work.
    let (status, _) = proxy_get(proxy, "hc-route-ns", "routed-svc", Some("1.0.0")).await?;
    assert_eq!(status, StatusCode::OK);

    // Backdate engine heartbeat and wait for monitor to mark unhealthy.
    backdate_engine_heartbeat(&pool, "hc-route-e1", 60).await;
    let (_, observed_version) = wait_for_default_rule_health(
        &mut mgr,
        "hc-route-e1",
        "hc-route-ns",
        "routed-svc",
        "1.0.0",
        false,
        DEFAULT_WAIT_TIMEOUT,
    )
    .await?;

    // Health rows become visible just before their routing-version publication.
    // If the observation raced that publication, wait for the next typed
    // manager version before the one-shot proxy sync. Production refresh loops
    // retry naturally; this fixture must not mistake the intermediate version
    // for completed convergence.
    let target_version = if observed_version > initial_proxy_version {
        observed_version
    } else {
        wait_for_routing_table_version_gt(&mut mgr, initial_proxy_version, DEFAULT_WAIT_TIMEOUT)
            .await?
    };
    sync_table(&mgr_addr, &table).await?;
    assert!(
        table.version().await >= target_version,
        "proxy routing snapshot did not reach unhealthy publication version {target_version}"
    );

    // Request should get 503 because no healthy instances remain.
    let (status, _) = proxy_get(proxy, "hc-route-ns", "routed-svc", Some("1.0.0")).await?;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

    let _ = engine_shutdown.send(());
    Ok(())
}

/// Routing table version is incremented when health status changes.
#[tokio::test]
async fn test_health_change_bumps_routing_table_version() -> Result<()> {
    let (pool, _mgr_addr, mut mgr) = manager_trio_with_monitor(1).await?;

    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;
    register_test_module_ready(
        &pool,
        &mut mgr,
        "hc-ver-e1",
        &engine_addr,
        "hc-ver-ns",
        "ver-svc",
        "1.0.0",
    )
    .await?;

    // Record the initial version.
    let version_before = get_routing_table_version(&mut mgr).await?;

    // Backdate engine heartbeat so the monitor marks the module unhealthy.
    backdate_engine_heartbeat(&pool, "hc-ver-e1", 60).await;

    let version_after =
        wait_for_routing_table_version_gt(&mut mgr, version_before, DEFAULT_WAIT_TIMEOUT).await?;

    assert!(
        version_after > version_before,
        "routing table version should increase on health change: before={version_before}, after={version_after}"
    );

    let _ = engine_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn test_only_omitted_module_route_unhealthy_then_recovers() -> Result<()> {
    let (pool, _addr, mut mgr) = manager_trio_with_monitor(1).await?;

    let registration = helpers::manager::register_managed_engine(
        &pool,
        &mut mgr,
        EngineRegistration {
            engine_id: "mh-e1".into(),
            address: "http://127.0.0.1:9800".into(),
            proxy_address: TEST_SELF_PEER.into(),
            peer_address: TEST_SELF_PEER.into(),
            modules: vec![
                ModuleDescriptor {
                    name: "mod-a".into(),
                    namespace: "mh-ns".into(),
                    version: "1.0.0".into(),
                    proto_schema: minimal_file_descriptor_set(),
                },
                ModuleDescriptor {
                    name: "mod-b".into(),
                    namespace: "mh-ns".into(),
                    version: "1.0.0".into(),
                    proto_schema: minimal_file_descriptor_set(),
                },
            ],
            secrets: vec![],
            db_namespaces: vec![],
            deployment: None,
            job_queue_id: String::new(),
            job_admin_address: String::new(),
        },
    )
    .await?;

    // Intentional elapsed-time interval: keep the engine and mod-a fresh while mod-b remains omitted.
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(200));
    for _ in 0..8 {
        mgr.heartbeat(HeartbeatRequest {
            engine_id: "mh-e1".into(),
            healthy_modules: vec![ModuleDescriptor {
                name: "mod-a".into(),
                namespace: "mh-ns".into(),
                version: "1.0.0".into(),
                proto_schema: vec![],
            }],

            fence: registration.get_ref().fence.clone(),
        })
        .await?;
        interval.tick().await;
    }
    wait_for_default_rule_health(
        &mut mgr,
        "mh-e1",
        "mh-ns",
        "mod-a",
        "1.0.0",
        true,
        DEFAULT_WAIT_TIMEOUT,
    )
    .await?;
    wait_for_default_rule_health(
        &mut mgr,
        "mh-e1",
        "mh-ns",
        "mod-b",
        "1.0.0",
        false,
        DEFAULT_WAIT_TIMEOUT,
    )
    .await?;

    let (a_healthy, _) =
        get_default_rule_health(&mut mgr, "mh-e1", "mh-ns", "mod-a", "1.0.0").await?;
    let (b_healthy, _) =
        get_default_rule_health(&mut mgr, "mh-e1", "mh-ns", "mod-b", "1.0.0").await?;
    assert!(a_healthy, "reported module stays healthy");
    assert!(!b_healthy, "omitted module's route becomes unhealthy");

    // Report BOTH modules — only mod-b should recover; mod-a stays healthy.
    mgr.heartbeat(HeartbeatRequest {
        engine_id: "mh-e1".into(),
        healthy_modules: vec![
            ModuleDescriptor {
                name: "mod-a".into(),
                namespace: "mh-ns".into(),
                version: "1.0.0".into(),
                proto_schema: vec![],
            },
            ModuleDescriptor {
                name: "mod-b".into(),
                namespace: "mh-ns".into(),
                version: "1.0.0".into(),
                proto_schema: vec![],
            },
        ],

        fence: registration.get_ref().fence.clone(),
    })
    .await?;
    wait_for_default_rule_health(
        &mut mgr,
        "mh-e1",
        "mh-ns",
        "mod-b",
        "1.0.0",
        true,
        DEFAULT_WAIT_TIMEOUT,
    )
    .await?;

    let (a_healthy, _) =
        get_default_rule_health(&mut mgr, "mh-e1", "mh-ns", "mod-a", "1.0.0").await?;
    let (b_healthy, _) =
        get_default_rule_health(&mut mgr, "mh-e1", "mh-ns", "mod-b", "1.0.0").await?;
    assert!(a_healthy, "mod-a still healthy");
    assert!(b_healthy, "mod-b recovers once reported again");
    Ok(())
}

#[tokio::test]
async fn test_engine_stale_marks_all_module_routes_unhealthy() -> Result<()> {
    let (pool, _addr, mut mgr) = manager_trio_with_monitor(1).await?;

    let registration = helpers::manager::register_managed_engine(
        &pool,
        &mut mgr,
        EngineRegistration {
            engine_id: "mh-stale-e1".into(),
            address: "http://127.0.0.1:9810".into(),
            proxy_address: TEST_SELF_PEER.into(),
            peer_address: TEST_SELF_PEER.into(),
            modules: vec![
                ModuleDescriptor {
                    name: "stale-a".into(),
                    namespace: "mh-ns".into(),
                    version: "1.0.0".into(),
                    proto_schema: minimal_file_descriptor_set(),
                },
                ModuleDescriptor {
                    name: "stale-b".into(),
                    namespace: "mh-ns".into(),
                    version: "1.0.0".into(),
                    proto_schema: minimal_file_descriptor_set(),
                },
            ],
            secrets: vec![],
            db_namespaces: vec![],
            deployment: None,
            job_queue_id: String::new(),
            job_admin_address: String::new(),
        },
    )
    .await?;

    mgr.heartbeat(HeartbeatRequest {
        engine_id: "mh-stale-e1".into(),
        healthy_modules: vec![
            ModuleDescriptor {
                name: "stale-a".into(),
                namespace: "mh-ns".into(),
                version: "1.0.0".into(),
                proto_schema: vec![],
            },
            ModuleDescriptor {
                name: "stale-b".into(),
                namespace: "mh-ns".into(),
                version: "1.0.0".into(),
                proto_schema: vec![],
            },
        ],

        fence: registration.get_ref().fence.clone(),
    })
    .await?;
    wr_manager::db::update_route_health(&pool, 30.0, 30.0)
        .await
        .map_err(|status| anyhow::anyhow!("update_route_health failed: {status}"))?;

    let (a_healthy, _) =
        get_default_rule_health(&mut mgr, "mh-stale-e1", "mh-ns", "stale-a", "1.0.0").await?;
    let (b_healthy, _) =
        get_default_rule_health(&mut mgr, "mh-stale-e1", "mh-ns", "stale-b", "1.0.0").await?;
    assert!(a_healthy, "stale-a starts healthy after heartbeat");
    assert!(b_healthy, "stale-b starts healthy after heartbeat");

    backdate_engine_heartbeat(&pool, "mh-stale-e1", 60).await;
    let (a_healthy, _) = wait_for_default_rule_health(
        &mut mgr,
        "mh-stale-e1",
        "mh-ns",
        "stale-a",
        "1.0.0",
        false,
        DEFAULT_WAIT_TIMEOUT,
    )
    .await?;
    let (b_healthy, _) = wait_for_default_rule_health(
        &mut mgr,
        "mh-stale-e1",
        "mh-ns",
        "stale-b",
        "1.0.0",
        false,
        DEFAULT_WAIT_TIMEOUT,
    )
    .await?;
    assert!(!a_healthy, "stale engine marks all its routes unhealthy");
    assert!(!b_healthy, "stale engine marks all its routes unhealthy");
    Ok(())
}

#[tokio::test]
async fn test_registration_alone_remains_unhealthy_after_sweep() -> Result<()> {
    let (pool, _addr, mut mgr) = manager_trio_with_monitor(30).await?;

    register_test_module_raw(
        &pool,
        &mut mgr,
        "mh-seed-e1",
        "http://127.0.0.1:9820",
        "mh-ns",
        "seeded-svc",
        "1.0.0",
    )
    .await?;

    let (healthy, _) =
        get_default_rule_health(&mut mgr, "mh-seed-e1", "mh-ns", "seeded-svc", "1.0.0").await?;
    assert!(!healthy, "raw registration starts default route unhealthy");

    wr_manager::db::update_route_health(&pool, 30.0, 30.0)
        .await
        .map_err(|status| anyhow::anyhow!("update_route_health failed: {status}"))?;
    let (healthy, _) =
        get_default_rule_health(&mut mgr, "mh-seed-e1", "mh-ns", "seeded-svc", "1.0.0").await?;
    assert!(
        !healthy,
        "health recompute without heartbeat keeps route unhealthy"
    );

    tokio::time::sleep(std::time::Duration::from_millis(600)).await;

    let (healthy, _) =
        get_default_rule_health(&mut mgr, "mh-seed-e1", "mh-ns", "seeded-svc", "1.0.0").await?;
    assert!(
        !healthy,
        "monitor sweep without heartbeat keeps route unhealthy"
    );
    Ok(())
}

#[tokio::test]
async fn test_reregister_resets_stale_module_readiness() -> Result<()> {
    let (pool, _addr, mut mgr) = manager_trio_with_monitor(30).await?;

    register_test_module_ready(
        &pool,
        &mut mgr,
        "mh-rereg-e1",
        "http://127.0.0.1:9825",
        "mh-ns",
        "rereg-svc",
        "1.0.0",
    )
    .await?;
    let (healthy, _) =
        get_default_rule_health(&mut mgr, "mh-rereg-e1", "mh-ns", "rereg-svc", "1.0.0").await?;
    assert!(healthy, "route starts healthy after ready registration");

    let fence = register_test_module_raw(
        &pool,
        &mut mgr,
        "mh-rereg-e1",
        "http://127.0.0.1:9825",
        "mh-ns",
        "rereg-svc",
        "1.0.0",
    )
    .await?;
    let (healthy, _) =
        get_default_rule_health(&mut mgr, "mh-rereg-e1", "mh-ns", "rereg-svc", "1.0.0").await?;
    assert!(!healthy, "re-registration resets the route to unhealthy");

    wr_manager::db::update_route_health(&pool, 30.0, 30.0)
        .await
        .map_err(|status| anyhow::anyhow!("update_route_health failed: {status}"))?;
    let (healthy, _) =
        get_default_rule_health(&mut mgr, "mh-rereg-e1", "mh-ns", "rereg-svc", "1.0.0").await?;
    assert!(
        !healthy,
        "stale pre-registration readiness cannot recover route"
    );

    mgr.heartbeat(HeartbeatRequest {
        engine_id: "mh-rereg-e1".into(),
        healthy_modules: vec![ModuleDescriptor {
            name: "rereg-svc".into(),
            namespace: "mh-ns".into(),
            version: "1.0.0".into(),
            proto_schema: vec![],
        }],

        fence: Some(fence.clone()),
    })
    .await?;
    wr_manager::db::update_route_health(&pool, 30.0, 30.0)
        .await
        .map_err(|status| anyhow::anyhow!("update_route_health failed: {status}"))?;
    let (healthy, _) =
        get_default_rule_health(&mut mgr, "mh-rereg-e1", "mh-ns", "rereg-svc", "1.0.0").await?;
    assert!(
        healthy,
        "fresh heartbeat recovers route after re-registration"
    );
    Ok(())
}

#[tokio::test]
async fn test_malformed_module_entry_skipped_not_fatal() -> Result<()> {
    let (pool, _addr, mut mgr) = manager_trio_with_monitor(30).await?;

    let registration = helpers::manager::register_managed_engine(
        &pool,
        &mut mgr,
        EngineRegistration {
            engine_id: "mh-bad-e1".into(),
            address: "http://127.0.0.1:9830".into(),
            proxy_address: TEST_SELF_PEER.into(),
            peer_address: TEST_SELF_PEER.into(),
            modules: vec![
                ModuleDescriptor {
                    name: "good-svc".into(),
                    namespace: "mh-ns".into(),
                    version: "1.0.0".into(),
                    proto_schema: minimal_file_descriptor_set(),
                },
                ModuleDescriptor {
                    name: "other-svc".into(),
                    namespace: "mh-ns".into(),
                    version: "1.0.0".into(),
                    proto_schema: minimal_file_descriptor_set(),
                },
            ],
            secrets: vec![],
            db_namespaces: vec![],
            deployment: None,
            job_queue_id: String::new(),
            job_admin_address: String::new(),
        },
    )
    .await?;

    // One valid entry + one with an empty version. The whole request must succeed.
    let resp = mgr
        .heartbeat(HeartbeatRequest {
            engine_id: "mh-bad-e1".into(),
            healthy_modules: vec![
                ModuleDescriptor {
                    name: "good-svc".into(),
                    namespace: "mh-ns".into(),
                    version: "1.0.0".into(),
                    proto_schema: vec![],
                },
                ModuleDescriptor {
                    name: "other-svc".into(),
                    namespace: "mh-ns".into(),
                    version: String::new(), // malformed -> skipped, not fatal
                    proto_schema: vec![],
                },
            ],

            fence: registration.get_ref().fence.clone(),
        })
        .await;
    assert!(resp.is_ok(), "malformed entry must not fail the heartbeat");

    wr_manager::db::update_route_health(&pool, 30.0, 30.0)
        .await
        .map_err(|status| anyhow::anyhow!("update_route_health failed: {status}"))?;

    let (good_healthy, _) =
        get_default_rule_health(&mut mgr, "mh-bad-e1", "mh-ns", "good-svc", "1.0.0").await?;
    let (other_healthy, _) =
        get_default_rule_health(&mut mgr, "mh-bad-e1", "mh-ns", "other-svc", "1.0.0").await?;
    assert!(good_healthy, "valid module becomes healthy");
    assert!(
        !other_healthy,
        "malformed/omitted module route remains unhealthy"
    );
    Ok(())
}

#[tokio::test]
async fn test_admin_route_without_module_heartbeat_flips_unhealthy() -> Result<()> {
    let (pool, _addr, mut mgr) = manager_trio_with_monitor(30).await?;

    // A registered, fresh engine with a real module (whose route stays healthy).
    register_test_module_ready(
        &pool,
        &mut mgr,
        "mh-admin-e1",
        "http://127.0.0.1:9840",
        "mh-ns",
        "real-svc",
        "1.0.0",
    )
    .await?;

    // Admin override for a module that no engine reports: engine is fresh, but
    // there is no matching module heartbeat -> the sweep must flip it unhealthy.
    mgr.upsert_routing_rule(RoutingRule {
        rule_id: "mh-admin-ghost".into(),
        source_namespace: String::new(),
        source_module: String::new(),
        destination_namespace: "mh-ns".into(),
        destination_module: "ghost-svc".into(),
        destination_version: "1.0.0".into(),
        engine_id: "mh-admin-e1".into(),
        engine_address: "http://127.0.0.1:9840".into(),
        peer_address: TEST_SELF_PEER.into(),
        healthy: true,
    })
    .await?;

    let (ghost_healthy, _) =
        wait_for_rule_health(&mut mgr, "ghost-svc", false, DEFAULT_WAIT_TIMEOUT).await?;
    let (real_healthy, _) =
        get_default_rule_health(&mut mgr, "mh-admin-e1", "mh-ns", "real-svc", "1.0.0").await?;
    assert!(
        !ghost_healthy,
        "admin route with no module heartbeat flips unhealthy on the sweep"
    );
    assert!(real_healthy, "real module route stays healthy");
    Ok(())
}
