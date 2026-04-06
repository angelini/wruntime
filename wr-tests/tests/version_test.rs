#[allow(dead_code, unused_imports)]
mod helpers;
use helpers::*;

use anyhow::Result;
use http::StatusCode;

use wr_common::wruntime::DeregisterEngineRequest;

#[tokio::test]
async fn test_proxy_routes_to_explicit_version() -> Result<()> {
    let (_pool, mgr_addr, mut mgr) = manager_trio().await?;

    let (e1_addr, e1_shutdown) = spawn_identified_stub("engine-v1").await?;
    let (e2_addr, e2_shutdown) = spawn_identified_stub("engine-v2").await?;

    register_test_module(
        &mut mgr,
        "e1",
        &e1_addr,
        "ver-ns",
        "versioned-service",
        "1.0.0",
    )
    .await?;
    register_test_module(
        &mut mgr,
        "e2",
        &e2_addr,
        "ver-ns",
        "versioned-service",
        "2.0.0",
    )
    .await?;

    let table = synced_routing_table(&mgr_addr).await?;
    let proxy = start_proxy(table).await?;

    let (s, body) = proxy_get(proxy, "ver-ns", "versioned-service", Some("1.0.0")).await?;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body, "engine-v1", "x-wr-version: 1.0.0 should route to v1");

    let (s, body) = proxy_get(proxy, "ver-ns", "versioned-service", Some("2.0.0")).await?;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body, "engine-v2", "x-wr-version: 2.0.0 should route to v2");

    let _ = e1_shutdown.send(());
    let _ = e2_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn test_proxy_load_balances_across_versions_without_header() -> Result<()> {
    let (_pool, mgr_addr, mut mgr) = manager_trio().await?;

    let (e1_addr, e1_shutdown) = spawn_identified_stub("engine-v1").await?;
    let (e2_addr, e2_shutdown) = spawn_identified_stub("engine-v2").await?;

    register_test_module(
        &mut mgr,
        "e1",
        &e1_addr,
        "latest-ns",
        "latest-service",
        "1.0.0",
    )
    .await?;
    register_test_module(
        &mut mgr,
        "e2",
        &e2_addr,
        "latest-ns",
        "latest-service",
        "2.0.0",
    )
    .await?;

    let table = synced_routing_table(&mgr_addr).await?;
    let proxy = start_proxy(table).await?;

    // No x-wr-version → should load-balance across all versions
    let mut saw_v1 = false;
    let mut saw_v2 = false;
    for _ in 0..10 {
        let (s, body) = proxy_get(proxy, "latest-ns", "latest-service", None).await?;
        assert_eq!(s, StatusCode::OK);
        saw_v1 |= body == "engine-v1";
        saw_v2 |= body == "engine-v2";
    }
    assert!(
        saw_v1,
        "engine-v1 should receive traffic without version header"
    );
    assert!(
        saw_v2,
        "engine-v2 should receive traffic without version header"
    );

    let _ = e1_shutdown.send(());
    let _ = e2_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn test_proxy_returns_503_for_missing_version() -> Result<()> {
    let (_pool, mgr_addr, mut mgr) = manager_trio().await?;

    let (e1_addr, _stub) = spawn_identified_stub("engine-v1").await?;
    register_test_module(
        &mut mgr,
        "e1",
        &e1_addr,
        "mv-ns",
        "missing-ver-service",
        "1.0.0",
    )
    .await?;

    let table = synced_routing_table(&mgr_addr).await?;
    let proxy = start_proxy(table).await?;

    let (s, _) = proxy_get(proxy, "mv-ns", "missing-ver-service", Some("9.0.0")).await?;
    assert_eq!(s, StatusCode::SERVICE_UNAVAILABLE, "unknown version → 503");

    Ok(())
}

#[tokio::test]
async fn test_proxy_routes_semver_range_to_highest_satisfying() -> Result<()> {
    let (_pool, mgr_addr, mut mgr) = manager_trio().await?;

    let (e1_addr, e1_shutdown) = spawn_identified_stub("engine-v1").await?;
    let (e2_addr, e2_shutdown) = spawn_identified_stub("engine-v2").await?;
    let (e3_addr, e3_shutdown) = spawn_identified_stub("engine-v3").await?;

    for (id, addr, version) in [
        ("e1", e1_addr.as_str(), "1.0.0"),
        ("e2", e2_addr.as_str(), "1.5.0"),
        ("e3", e3_addr.as_str(), "2.0.0"),
    ] {
        register_test_module(&mut mgr, id, addr, "range-ns", "range-service", version).await?;
    }

    let table = synced_routing_table(&mgr_addr).await?;
    let proxy = start_proxy(table).await?;

    // ^1 should pick the highest 1.x, which is 1.5.0
    let (s, body) = proxy_get(proxy, "range-ns", "range-service", Some("^1")).await?;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body, "engine-v2", "^1 should route to highest 1.x (1.5.0)");

    // >=1.5.0 should pick 2.0.0 (highest satisfying)
    let (s, body) = proxy_get(proxy, "range-ns", "range-service", Some(">=1.5.0")).await?;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(
        body, "engine-v3",
        ">=1.5.0 should route to highest satisfying (2.0.0)"
    );

    let _ = e1_shutdown.send(());
    let _ = e2_shutdown.send(());
    let _ = e3_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn test_proxy_returns_503_for_unsatisfiable_range() -> Result<()> {
    let (_pool, mgr_addr, mut mgr) = manager_trio().await?;

    let (e1_addr, _stub) = spawn_identified_stub("engine-v1").await?;
    register_test_module(
        &mut mgr,
        "e1",
        &e1_addr,
        "range-503-ns",
        "range-503-service",
        "1.0.0",
    )
    .await?;

    let table = synced_routing_table(&mgr_addr).await?;
    let proxy = start_proxy(table).await?;

    let (s, _) = proxy_get(proxy, "range-503-ns", "range-503-service", Some("^3")).await?;
    assert_eq!(
        s,
        StatusCode::SERVICE_UNAVAILABLE,
        "unsatisfiable range → 503"
    );

    Ok(())
}

#[tokio::test]
async fn test_proxy_load_balances_across_instances() -> Result<()> {
    let (_pool, mgr_addr, mut mgr) = manager_trio().await?;

    // Two engines both hosting the same (module, version).
    let (e1_addr, e1_shutdown) = spawn_identified_stub("engine-a").await?;
    let (e2_addr, e2_shutdown) = spawn_identified_stub("engine-b").await?;

    register_test_module(&mut mgr, "ea", &e1_addr, "lb-ns", "lb-service", "1.0.0").await?;
    register_test_module(&mut mgr, "eb", &e2_addr, "lb-ns", "lb-service", "1.0.0").await?;

    let table = synced_routing_table(&mgr_addr).await?;
    let proxy = start_proxy(table).await?;

    let mut saw_a = false;
    let mut saw_b = false;
    for _ in 0..10 {
        let (s, body) = proxy_get(proxy, "lb-ns", "lb-service", Some("1.0.0")).await?;
        assert_eq!(s, StatusCode::OK);
        saw_a |= body == "engine-a";
        saw_b |= body == "engine-b";
    }

    assert!(saw_a, "engine-a should have received at least one request");
    assert!(saw_b, "engine-b should have received at least one request");

    let _ = e1_shutdown.send(());
    let _ = e2_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn test_proxy_failover_after_deregister() -> Result<()> {
    let (_pool, mgr_addr, mut mgr) = manager_trio().await?;

    let (e1_addr, e1_shutdown) = spawn_identified_stub("engine-a").await?;
    let (e2_addr, e2_shutdown) = spawn_identified_stub("engine-b").await?;

    register_test_module(
        &mut mgr,
        "ea",
        &e1_addr,
        "fo-ns",
        "failover-service",
        "1.0.0",
    )
    .await?;
    register_test_module(
        &mut mgr,
        "eb",
        &e2_addr,
        "fo-ns",
        "failover-service",
        "1.0.0",
    )
    .await?;

    let table = synced_routing_table(&mgr_addr).await?;
    let proxy = start_proxy(table.clone()).await?;

    // Both instances should be reachable before failover.
    let mut saw_a = false;
    let mut saw_b = false;
    for _ in 0..4 {
        let (_, body) = proxy_get(proxy, "fo-ns", "failover-service", Some("1.0.0")).await?;
        saw_a |= body == "engine-a";
        saw_b |= body == "engine-b";
    }
    assert!(saw_a, "engine-a should be reachable before failover");
    assert!(saw_b, "engine-b should be reachable before failover");

    // Deregister engine-a; its rule is immediately marked unhealthy.
    mgr.deregister_engine(DeregisterEngineRequest {
        engine_id: "ea".into(),
    })
    .await?;
    sync_table(&mgr_addr, &table).await?;

    // All subsequent traffic must go to engine-b.
    for _ in 0..4 {
        let (s, body) = proxy_get(proxy, "fo-ns", "failover-service", Some("1.0.0")).await?;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(
            body, "engine-b",
            "after failover all traffic should go to engine-b"
        );
    }

    let _ = e1_shutdown.send(());
    let _ = e2_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn test_proxy_503_when_all_instances_unhealthy() -> Result<()> {
    let (_pool, mgr_addr, mut mgr) = manager_trio().await?;

    let (e1_addr, _stub) = spawn_identified_stub("engine-a").await?;
    register_test_module(&mut mgr, "ea", &e1_addr, "gone-ns", "gone-service", "1.0.0").await?;

    let table = synced_routing_table(&mgr_addr).await?;
    let proxy = start_proxy(table.clone()).await?;

    let (s, _) = proxy_get(proxy, "gone-ns", "gone-service", Some("1.0.0")).await?;
    assert_eq!(s, StatusCode::OK, "should be reachable before deregister");

    mgr.deregister_engine(DeregisterEngineRequest {
        engine_id: "ea".into(),
    })
    .await?;
    sync_table(&mgr_addr, &table).await?;

    let (s, _) = proxy_get(proxy, "gone-ns", "gone-service", Some("1.0.0")).await?;
    assert_eq!(s, StatusCode::SERVICE_UNAVAILABLE, "all unhealthy → 503");

    Ok(())
}
