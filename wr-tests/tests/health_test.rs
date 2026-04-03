#[allow(dead_code, unused_imports)]
mod helpers;
use helpers::*;

use anyhow::Result;
use http::StatusCode;

use wr_common::wruntime::{HeartbeatRequest, ModuleDescriptor};

#[tokio::test]
async fn test_heartbeat_timeout_marks_module_unhealthy() -> Result<()> {
    let pool = manager_pool().await;
    let mgr_addr = start_manager_with_monitor(pool.clone(), 1).await?;
    let mut mgr = manager_client(&mgr_addr).await?;

    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;
    register_module(
        &mut mgr,
        EngineSpec {
            id: "hc-e1",
            addr: &engine_addr,
            proxy_address: "",
        },
        ModuleSpec {
            namespace: "hc-ns",
            name: "heartbeat-svc",
            version: "1.0.0",
            schema: minimal_file_descriptor_set(),
        },
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
    let pool = manager_pool().await;
    let mgr_addr = start_manager_with_monitor(pool.clone(), 2).await?;
    let mut mgr = manager_client(&mgr_addr).await?;

    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;
    register_module(
        &mut mgr,
        EngineSpec {
            id: "hc-keep-e1",
            addr: &engine_addr,
            proxy_address: "",
        },
        ModuleSpec {
            namespace: "hc-keep-ns",
            name: "kept-svc",
            version: "1.0.0",
            schema: minimal_file_descriptor_set(),
        },
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
    let pool = manager_pool().await;
    let mgr_addr = start_manager_with_monitor(pool.clone(), 1).await?;
    let mut mgr = manager_client(&mgr_addr).await?;

    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;
    register_module(
        &mut mgr,
        EngineSpec {
            id: "hc-rec-e1",
            addr: &engine_addr,
            proxy_address: "",
        },
        ModuleSpec {
            namespace: "hc-rec-ns",
            name: "recovering-svc",
            version: "1.0.0",
            schema: minimal_file_descriptor_set(),
        },
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
        healthy_modules: vec![],
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
    let pool = manager_pool().await;
    let mgr_addr = start_manager_with_monitor(pool.clone(), 1).await?;
    let mut mgr = manager_client(&mgr_addr).await?;

    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;
    register_module(
        &mut mgr,
        EngineSpec {
            id: "hc-route-e1",
            addr: &engine_addr,
            proxy_address: "",
        },
        ModuleSpec {
            namespace: "hc-route-ns",
            name: "routed-svc",
            version: "1.0.0",
            schema: minimal_file_descriptor_set(),
        },
    )
    .await?;

    let table = wr_proxy::routing::new_routing_table();
    sync_table(&mgr_addr, &table).await?;
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
    let pool = manager_pool().await;
    let mgr_addr = start_manager_with_monitor(pool.clone(), 1).await?;
    let mut mgr = manager_client(&mgr_addr).await?;

    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;
    register_module(
        &mut mgr,
        EngineSpec {
            id: "hc-ver-e1",
            addr: &engine_addr,
            proxy_address: "",
        },
        ModuleSpec {
            namespace: "hc-ver-ns",
            name: "ver-svc",
            version: "1.0.0",
            schema: minimal_file_descriptor_set(),
        },
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
