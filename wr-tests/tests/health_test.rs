#[allow(dead_code, unused_imports)]
mod helpers;
use helpers::*;

use anyhow::Result;
use http::StatusCode;

use wr_common::wruntime::{
    EngineRegistration, HeartbeatRequest, ModuleDescriptor, RegisterEngineRequest, RoutingRule,
};

#[tokio::test]
async fn test_heartbeat_timeout_marks_module_unhealthy() -> Result<()> {
    let (pool, _mgr_addr, mut mgr) = manager_trio_with_monitor(1).await?;

    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;
    register_test_module(
        &mut mgr,
        "hc-e1",
        &engine_addr,
        "hc-ns",
        "heartbeat-svc",
        "1.0.0",
    )
    .await?;

    // The module is initially healthy (registration sets last_heartbeat).
    let (healthy, _) = get_rule_health(&mut mgr, "heartbeat-svc").await?;
    assert!(healthy, "module should be healthy after registration");

    // Backdate the engine heartbeat so the monitor considers it stale.
    backdate_engine_heartbeat(&pool, "hc-e1", 60).await;

    // Wait for the monitor to run (200ms tick in tests) — add padding.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // The module should now be marked unhealthy.
    let (healthy, _) = get_rule_health(&mut mgr, "heartbeat-svc").await?;
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
    let (_pool, _mgr_addr, mut mgr) = manager_trio_with_monitor(2).await?;

    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;
    register_test_module(
        &mut mgr,
        "hc-keep-e1",
        &engine_addr,
        "hc-keep-ns",
        "kept-svc",
        "1.0.0",
    )
    .await?;

    // Send heartbeats continuously through several monitor ticks (200ms each).
    for _ in 0..5 {
        mgr.heartbeat(HeartbeatRequest {
            engine_id: "hc-keep-e1".into(),
            healthy_modules: vec![ModuleDescriptor {
                name: "kept-svc".into(),
                namespace: "hc-keep-ns".into(),
                version: "1.0.0".into(),
                proto_schema: vec![],
            }],
        })
        .await?;
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    }

    // Module should still be healthy.
    let (healthy, _) = get_rule_health(&mut mgr, "kept-svc").await?;
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
    register_test_module(
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

    // Wait for the monitor tick (200ms interval) to detect the stale timestamp.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let (healthy, _) = get_rule_health(&mut mgr, "recovering-svc").await?;
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
    })
    .await?;

    // Wait for the next monitor tick to see the fresh timestamp.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let (healthy, _) = get_rule_health(&mut mgr, "recovering-svc").await?;
    assert!(healthy, "module should recover after heartbeat");

    let _ = engine_shutdown.send(());
    Ok(())
}

/// An unhealthy module is excluded from proxy routing — requests get 503.
#[tokio::test]
async fn test_unhealthy_module_excluded_from_routing() -> Result<()> {
    let (pool, mgr_addr, mut mgr) = manager_trio_with_monitor(1).await?;

    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;
    register_test_module(
        &mut mgr,
        "hc-route-e1",
        &engine_addr,
        "hc-route-ns",
        "routed-svc",
        "1.0.0",
    )
    .await?;

    let table = synced_routing_table(&mgr_addr).await?;
    let proxy = start_proxy(table.clone()).await?;

    // Module is healthy — routing should work.
    let (status, _) = proxy_get(proxy, "hc-route-ns", "routed-svc", Some("1.0.0")).await?;
    assert_eq!(status, StatusCode::OK);

    // Backdate engine heartbeat and wait for monitor to mark unhealthy.
    backdate_engine_heartbeat(&pool, "hc-route-e1", 60).await;

    // Wait for the monitor tick (200ms interval) to detect the stale timestamp.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Re-sync the routing table — the rule should now be unhealthy.
    sync_table(&mgr_addr, &table).await?;

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
    register_test_module(
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

    // Wait for the monitor tick (200ms interval) to detect the stale timestamp.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let version_after = get_routing_table_version(&mut mgr).await?;

    assert!(
        version_after > version_before,
        "routing table version should increase on health change: before={version_before}, after={version_after}"
    );

    let _ = engine_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn test_only_omitted_module_route_unhealthy_then_recovers() -> Result<()> {
    let (_pool, _addr, mut mgr) = manager_trio_with_monitor(1).await?;

    mgr.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "mh-e1".into(),
            address: "http://127.0.0.1:9800".into(),
            proxy_address: String::new(),
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
        }),
    })
    .await?;

    // Heartbeat reporting ONLY mod-a for longer than the module timeout (1s).
    // This keeps the engine and mod-a fresh while mod-b's seeded heartbeat ages out.
    for _ in 0..8 {
        mgr.heartbeat(HeartbeatRequest {
            engine_id: "mh-e1".into(),
            healthy_modules: vec![ModuleDescriptor {
                name: "mod-a".into(),
                namespace: "mh-ns".into(),
                version: "1.0.0".into(),
                proto_schema: vec![],
            }],
        })
        .await?;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let (a_healthy, _) = get_rule_health(&mut mgr, "mod-a").await?;
    let (b_healthy, _) = get_rule_health(&mut mgr, "mod-b").await?;
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
    })
    .await?;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let (a_healthy, _) = get_rule_health(&mut mgr, "mod-a").await?;
    let (b_healthy, _) = get_rule_health(&mut mgr, "mod-b").await?;
    assert!(a_healthy, "mod-a still healthy");
    assert!(b_healthy, "mod-b recovers once reported again");
    Ok(())
}

#[tokio::test]
async fn test_engine_stale_marks_all_module_routes_unhealthy() -> Result<()> {
    let (pool, _addr, mut mgr) = manager_trio_with_monitor(1).await?;

    mgr.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "mh-stale-e1".into(),
            address: "http://127.0.0.1:9810".into(),
            proxy_address: String::new(),
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
        }),
    })
    .await?;

    backdate_engine_heartbeat(&pool, "mh-stale-e1", 60).await;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let (a_healthy, _) = get_rule_health(&mut mgr, "stale-a").await?;
    let (b_healthy, _) = get_rule_health(&mut mgr, "stale-b").await?;
    assert!(!a_healthy, "stale engine marks all its routes unhealthy");
    assert!(!b_healthy, "stale engine marks all its routes unhealthy");
    Ok(())
}

#[tokio::test]
async fn test_registration_seed_survives_first_sweep() -> Result<()> {
    let (_pool, _addr, mut mgr) = manager_trio_with_monitor(30).await?;

    register_test_module(
        &mut mgr,
        "mh-seed-e1",
        "http://127.0.0.1:9820",
        "mh-ns",
        "seeded-svc",
        "1.0.0",
    )
    .await?;

    // No heartbeat sent. Let several monitor ticks run. Without the registration
    // seed, the module-heartbeat join would fail and the route would flip
    // unhealthy on the first sweep.
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;

    let (healthy, _) = get_rule_health(&mut mgr, "seeded-svc").await?;
    assert!(
        healthy,
        "seeded route stays healthy through the first sweep"
    );
    Ok(())
}

#[tokio::test]
async fn test_malformed_module_entry_skipped_not_fatal() -> Result<()> {
    let (_pool, _addr, mut mgr) = manager_trio_with_monitor(30).await?;

    mgr.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "mh-bad-e1".into(),
            address: "http://127.0.0.1:9830".into(),
            proxy_address: String::new(),
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
        }),
    })
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
        })
        .await;
    assert!(resp.is_ok(), "malformed entry must not fail the heartbeat");

    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    // Engine liveness was bumped and neither route was starved.
    let (good_healthy, _) = get_rule_health(&mut mgr, "good-svc").await?;
    let (other_healthy, _) = get_rule_health(&mut mgr, "other-svc").await?;
    assert!(good_healthy, "valid module stays healthy");
    assert!(
        other_healthy,
        "other route not starved by the malformed entry"
    );
    Ok(())
}

#[tokio::test]
async fn test_admin_route_without_module_heartbeat_flips_unhealthy() -> Result<()> {
    let (_pool, _addr, mut mgr) = manager_trio_with_monitor(30).await?;

    // A registered, fresh engine with a real module (whose route stays healthy).
    register_test_module(
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

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let (ghost_healthy, _) = get_rule_health(&mut mgr, "ghost-svc").await?;
    let (real_healthy, _) = get_rule_health(&mut mgr, "real-svc").await?;
    assert!(
        !ghost_healthy,
        "admin route with no module heartbeat flips unhealthy on the sweep"
    );
    assert!(real_healthy, "real module route stays healthy");
    Ok(())
}
