#[allow(dead_code, unused_imports)]
mod helpers;
use helpers::*;

use wr_common::wruntime::{
    EngineRegistration, HeartbeatRequest, ListManagersRequest, ModuleDescriptor,
    RegisterEngineRequest,
};

// ── Multi-manager integration tests ──────────────────────────────────────────
//
// These tests verify DB-based health monitoring across multiple managers
// sharing the same Postgres. Chitchat is used only for manager liveness.

/// Engine heartbeats to manager-1; manager-2 sees the engine as healthy
/// immediately via shared Postgres.
#[tokio::test]
async fn test_heartbeat_visible_across_managers() {
    let pool = manager_pool().await;
    let managers = start_manager_cluster(pool.clone(), 2, 30).await.unwrap();

    // Register engine + routing rule via manager-1
    let mut c1 = manager_client(&managers[0].addr).await.unwrap();
    register_test_module(
        &mut c1,
        "engine-1",
        "http://127.0.0.1:19100",
        "ns",
        "svc",
        "1.0.0",
    )
    .await
    .unwrap();

    // Send heartbeat to manager-1
    c1.heartbeat(wr_common::wruntime::HeartbeatRequest {
        engine_id: "engine-1".into(),
        healthy_modules: vec![],
    })
    .await
    .unwrap();

    // No gossip wait needed — DB writes are immediately visible.
    // Wait for a monitor tick (200ms interval + padding).
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Manager-2 can see the healthy rule via the shared DB.
    let mut c2 = manager_client(&managers[1].addr).await.unwrap();
    let (healthy, _) = get_rule_health(&mut c2, "svc").await.unwrap();
    assert!(
        healthy,
        "rule should be healthy (heartbeat written to shared Postgres)"
    );
}

/// Engine heartbeats to manager-1; manager-2 can also verify health via
/// the routing table (rule stays healthy).
#[tokio::test]
async fn test_health_preserved_across_managers() {
    let pool = manager_pool().await;
    let managers = start_manager_cluster(pool.clone(), 2, 2).await.unwrap();

    let mut c1 = manager_client(&managers[0].addr).await.unwrap();
    register_test_module(
        &mut c1,
        "engine-2",
        "http://127.0.0.1:19200",
        "ns",
        "svc2",
        "1.0.0",
    )
    .await
    .unwrap();

    // Heartbeat to manager-1
    c1.heartbeat(wr_common::wruntime::HeartbeatRequest {
        engine_id: "engine-2".into(),
        healthy_modules: vec![],
    })
    .await
    .unwrap();

    // Wait for monitor cycle
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Check via manager-2 that the rule is still healthy
    let mut c2 = manager_client(&managers[1].addr).await.unwrap();
    let (healthy, _) = get_rule_health(&mut c2, "svc2").await.unwrap();
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
    register_test_module(
        &mut c1,
        "engine-3",
        "http://127.0.0.1:19300",
        "ns",
        "svc3",
        "1.0.0",
    )
    .await
    .unwrap();

    // One heartbeat to establish the engine
    c1.heartbeat(wr_common::wruntime::HeartbeatRequest {
        engine_id: "engine-3".into(),
        healthy_modules: vec![],
    })
    .await
    .unwrap();

    // Verify healthy via manager-2
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let mut c2 = manager_client(&managers[1].addr).await.unwrap();
    let (healthy, _) = get_rule_health(&mut c2, "svc3").await.unwrap();
    assert!(healthy, "should be healthy after heartbeat");

    // Stop heartbeating — wait for timeout + monitor cycle
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Either manager should have marked the rule unhealthy
    let (healthy, _) = get_rule_health(&mut c2, "svc3").await.unwrap();
    assert!(!healthy, "should be unhealthy after missed heartbeat");
}

/// A one-manager cluster can register a module, process heartbeats, and keep routes healthy.
#[tokio::test]
async fn test_single_manager_cluster() {
    let pool = manager_pool().await;
    let managers = start_manager_cluster(pool.clone(), 1, 30).await.unwrap();

    let mut c = manager_client(&managers[0].addr).await.unwrap();
    register_test_module(
        &mut c,
        "engine-solo",
        "http://127.0.0.1:19400",
        "ns",
        "solo",
        "1.0.0",
    )
    .await
    .unwrap();

    c.heartbeat(wr_common::wruntime::HeartbeatRequest {
        engine_id: "engine-solo".into(),
        healthy_modules: vec![],
    })
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let (healthy, _) = get_rule_health(&mut c, "solo").await.unwrap();
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
        .query(
            "SELECT manager_id, grpc_address, gossip_address FROM wr_managers",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2, "two managers should be registered");

    // Both should have non-empty addresses
    for row in &rows {
        let grpc: String = row.get(1);
        let gossip: String = row.get(2);
        assert!(grpc.starts_with("http://"), "grpc_address should be a URL");
        assert!(!gossip.is_empty(), "gossip_address should be non-empty");
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
            proxy_address: String::new(),
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
        }),
    })
    .await
    .unwrap();

    // Heartbeat only mm-a to manager-1 for longer than the 1s timeout.
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
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Manager-2 sees the shared outcome.
    let mut c2 = manager_client(&managers[1].addr).await.unwrap();
    let (a_healthy, _) = get_rule_health(&mut c2, "mm-a").await.unwrap();
    let (b_healthy, _) = get_rule_health(&mut c2, "mm-b").await.unwrap();
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
    assert!(!infos[0].gossip_address.is_empty());
}

#[tokio::test]
async fn test_dead_peer_excluded_from_list_managers() {
    let pool = manager_pool().await;
    let managers = start_manager_cluster_fast_death(pool.clone(), 2, 30)
        .await
        .unwrap();
    let survivor = &managers[0];
    let victim = &managers[1];
    let mut c = manager_client(&survivor.addr).await.unwrap();

    // Wait for gossip to converge: survivor reports both managers.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let n = c
            .list_managers(ListManagersRequest {})
            .await
            .unwrap()
            .into_inner()
            .managers
            .len();
        if n == 2 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "gossip did not converge"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    // Kill the victim's gossip (its DB row stays fresh, well inside 60s).
    victim.cluster.initiate_shutdown().unwrap();

    // Survivor must drop the victim via the chitchat-dead path, faster than 60s.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        let ids: Vec<String> = c
            .list_managers(ListManagersRequest {})
            .await
            .unwrap()
            .into_inner()
            .managers
            .into_iter()
            .map(|m| m.manager_id)
            .collect();
        if !ids.contains(&victim.manager_id) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "victim not excluded after chitchat death"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}
