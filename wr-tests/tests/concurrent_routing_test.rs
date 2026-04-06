#[allow(dead_code, unused_imports)]
mod helpers;
use helpers::*;

use anyhow::Result;

use wr_common::wruntime::{
    DeleteRoutingRuleRequest, DeregisterEngineRequest, EngineRegistration, GetRoutingTableRequest,
    ModuleDescriptor, RegisterEngineRequest, RoutingRule,
};

/// Build a routing rule with the given ID and destination module.
fn make_rule(rule_id: &str, dest_module: &str, engine_id: &str) -> RoutingRule {
    RoutingRule {
        rule_id: rule_id.into(),
        source_module: String::new(),
        source_namespace: String::new(),
        destination_module: dest_module.into(),
        destination_namespace: "concurrent-ns".into(),
        destination_version: "1.0.0".into(),
        engine_id: engine_id.into(),
        engine_address: "http://127.0.0.1:9999".into(),
        proxy_address: String::new(),
        peer_address: String::new(),
        healthy: true,
    }
}

// ── Lock contention: upsert retries and succeeds ────────────────────────────

/// Hold the lock briefly — upsert's built-in retry succeeds after the lock is released.
#[tokio::test]
async fn test_upsert_retries_on_contention() -> Result<()> {
    let pool = manager_pool().await;

    // Hold the lock for 50ms, then release. The upsert's retry loop should
    // succeed on a subsequent attempt.
    let pool2 = pool.clone();
    let blocker = tokio::spawn(async move {
        let mut client = pool2.get().await.unwrap();
        let txn = client.transaction().await.unwrap();
        txn.query_one(
            "SELECT version FROM wr_manager_lock WHERE id = 1 FOR UPDATE",
            &[],
        )
        .await
        .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        txn.commit().await.unwrap();
    });

    // Give the blocker time to acquire the lock.
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    // Upsert should succeed after internal retries.
    wr_manager::db::upsert_routing_rule(&pool, &make_rule("retry-r1", "retry-svc", "e1")).await?;

    blocker.await?;

    // Verify the rule was actually written.
    let table = wr_manager::db::get_routing_table(&pool, 0)
        .await?
        .expect("table should exist");
    assert!(
        table.rules.iter().any(|r| r.rule_id == "retry-r1"),
        "upserted rule should be in the routing table",
    );

    Ok(())
}

// ── Lock contention: delete retries and succeeds ────────────────────────────

/// Same pattern for delete — retry loop handles brief contention.
#[tokio::test]
async fn test_delete_retries_on_contention() -> Result<()> {
    let pool = manager_pool().await;

    // Insert a rule first.
    wr_manager::db::upsert_routing_rule(&pool, &make_rule("del-retry-r1", "del-svc", "e1")).await?;

    // Hold the lock briefly.
    let pool2 = pool.clone();
    let blocker = tokio::spawn(async move {
        let mut client = pool2.get().await.unwrap();
        let txn = client.transaction().await.unwrap();
        txn.query_one(
            "SELECT version FROM wr_manager_lock WHERE id = 1 FOR UPDATE",
            &[],
        )
        .await
        .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        txn.commit().await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    // Delete should succeed after retries.
    let deleted = wr_manager::db::delete_routing_rule(&pool, "del-retry-r1").await?;
    assert!(deleted, "rule should have been deleted");

    blocker.await?;

    Ok(())
}

// ── Deregister uses blocking lock — waits then succeeds ─────────────────────

/// deregister_engine uses FOR UPDATE (blocking). Verify it succeeds after
/// the other transaction commits, rather than returning Aborted.
#[tokio::test]
async fn test_deregister_waits_for_lock() -> Result<()> {
    let pool = manager_pool().await;
    let addr = start_manager(pool.clone()).await?;
    let mut c = manager_client(&addr).await?;

    // Register an engine with a module so deregister has rules to mark unhealthy.
    c.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "dereg-e1".into(),
            address: "http://127.0.0.1:9400".into(),
            proxy_address: String::new(),
            peer_address: String::new(),
            modules: vec![ModuleDescriptor {
                name: "dereg-svc".into(),
                namespace: "dereg-ns".into(),
                version: "1.0.0".into(),
                proto_schema: minimal_file_descriptor_set(),
            }],
            secrets: vec![],
        }),
    })
    .await?;
    c.upsert_routing_rule(make_rule("dereg-r1", "dereg-svc", "dereg-e1"))
        .await?;

    // Hold the lock briefly, then release.
    let pool2 = pool.clone();
    let blocker = tokio::spawn(async move {
        let mut client = pool2.get().await.unwrap();
        let txn = client.transaction().await.unwrap();
        txn.query_one(
            "SELECT version FROM wr_manager_lock WHERE id = 1 FOR UPDATE",
            &[],
        )
        .await
        .unwrap();
        // Hold lock for 100ms then release.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        txn.commit().await.unwrap();
    });

    // Give the blocker a moment to acquire the lock.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    // Deregister should block until the lock is released, then succeed.
    c.deregister_engine(DeregisterEngineRequest {
        engine_id: "dereg-e1".into(),
    })
    .await?;

    blocker.await?;

    // Engine should be gone.
    let list = c
        .list_engines(wr_common::wruntime::ListEnginesRequest {})
        .await?
        .into_inner()
        .engines;
    assert!(list.is_empty(), "engine should be deregistered");

    Ok(())
}

// ── Sequential upserts bump version monotonically ───────────────────────────

#[tokio::test]
async fn test_sequential_upserts_bump_version() -> Result<()> {
    let pool = manager_pool().await;
    let addr = start_manager(pool).await?;
    let mut c = manager_client(&addr).await?;

    let v0 = get_routing_table_version(&mut c).await?;

    c.upsert_routing_rule(make_rule("seq-r1", "svc-1", "e1"))
        .await?;
    let v1 = get_routing_table_version(&mut c).await?;

    c.upsert_routing_rule(make_rule("seq-r2", "svc-2", "e2"))
        .await?;
    let v2 = get_routing_table_version(&mut c).await?;

    c.upsert_routing_rule(make_rule("seq-r3", "svc-3", "e3"))
        .await?;
    let v3 = get_routing_table_version(&mut c).await?;

    assert_eq!(v1, v0 + 1);
    assert_eq!(v2, v0 + 2);
    assert_eq!(v3, v0 + 3);

    Ok(())
}

// ── Parallel upserts all eventually succeed and each bumps version ──────────

#[tokio::test]
async fn test_parallel_upserts_all_version_bumps() -> Result<()> {
    let pool = manager_pool().await;
    let addr = start_manager(pool).await?;

    let v_before = {
        let mut c = manager_client(&addr).await?;
        get_routing_table_version(&mut c).await?
    };

    // Launch N concurrent upserts. The db layer retries lock contention internally.
    let n = 5u64;
    let mut handles = Vec::new();
    for i in 0..n {
        let addr = addr.clone();
        handles.push(tokio::spawn(async move {
            let mut c = manager_client(&addr).await.unwrap();
            let rule = make_rule(
                &format!("par-r{i}"),
                &format!("par-svc-{i}"),
                &format!("par-e{i}"),
            );
            c.upsert_routing_rule(rule).await.unwrap();
        }));
    }
    for h in handles {
        h.await?;
    }

    let v_after = {
        let mut c = manager_client(&addr).await?;
        get_routing_table_version(&mut c).await?
    };

    // Each upsert bumps version by 1.
    assert_eq!(
        v_after,
        v_before + n,
        "each upsert should bump version by 1"
    );

    // All rules should be present.
    let mut c = manager_client(&addr).await?;
    let table = c
        .get_routing_table(GetRoutingTableRequest { known_version: 0 })
        .await?
        .into_inner()
        .table
        .unwrap();
    assert_eq!(table.rules.len(), n as usize);

    Ok(())
}

// ── Delete non-existent rule does not bump version ──────────────────────────

#[tokio::test]
async fn test_delete_nonexistent_rule_no_version_bump() -> Result<()> {
    let pool = manager_pool().await;
    let addr = start_manager(pool).await?;
    let mut c = manager_client(&addr).await?;

    // Insert a rule so version > 0.
    c.upsert_routing_rule(make_rule("noop-r1", "noop-svc", "e1"))
        .await?;
    let v_before = get_routing_table_version(&mut c).await?;

    // Delete a rule that doesn't exist.
    c.delete_routing_rule(DeleteRoutingRuleRequest {
        rule_id: "does-not-exist".into(),
    })
    .await?;

    let v_after = get_routing_table_version(&mut c).await?;
    assert_eq!(
        v_after, v_before,
        "version should not change when deleting nonexistent rule",
    );

    Ok(())
}

// ── Delete existing rule bumps version ──────────────────────────────────────

#[tokio::test]
async fn test_delete_existing_rule_bumps_version() -> Result<()> {
    let pool = manager_pool().await;
    let addr = start_manager(pool).await?;
    let mut c = manager_client(&addr).await?;

    c.upsert_routing_rule(make_rule("delv-r1", "delv-svc", "e1"))
        .await?;
    let v_before = get_routing_table_version(&mut c).await?;

    c.delete_routing_rule(DeleteRoutingRuleRequest {
        rule_id: "delv-r1".into(),
    })
    .await?;

    let v_after = get_routing_table_version(&mut c).await?;
    assert_eq!(v_after, v_before + 1);

    // Rule should be gone.
    let table = c
        .get_routing_table(GetRoutingTableRequest { known_version: 0 })
        .await?
        .into_inner()
        .table
        .unwrap();
    assert!(
        table.rules.iter().all(|r| r.rule_id != "delv-r1"),
        "deleted rule should not appear in routing table",
    );

    Ok(())
}

// ── Deregister with no healthy rules does not bump version ──────────────────

#[tokio::test]
async fn test_deregister_no_rules_no_version_bump() -> Result<()> {
    let pool = manager_pool().await;
    let addr = start_manager(pool).await?;
    let mut c = manager_client(&addr).await?;

    // Register engine without any routing rules.
    c.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "norule-e1".into(),
            address: "http://127.0.0.1:9500".into(),
            proxy_address: String::new(),
            peer_address: String::new(),
            modules: vec![],
            secrets: vec![],
        }),
    })
    .await?;

    let v_before = get_routing_table_version(&mut c).await?;

    c.deregister_engine(DeregisterEngineRequest {
        engine_id: "norule-e1".into(),
    })
    .await?;

    let v_after = get_routing_table_version(&mut c).await?;
    assert_eq!(
        v_after, v_before,
        "deregistering engine with no healthy rules should not bump version",
    );

    Ok(())
}

// ── Deregister with healthy rules bumps version ─────────────────────────────

#[tokio::test]
async fn test_deregister_with_rules_bumps_version() -> Result<()> {
    let pool = manager_pool().await;
    let addr = start_manager(pool).await?;
    let mut c = manager_client(&addr).await?;

    c.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "withrule-e1".into(),
            address: "http://127.0.0.1:9501".into(),
            proxy_address: String::new(),
            peer_address: String::new(),
            modules: vec![ModuleDescriptor {
                name: "wr-svc".into(),
                namespace: "wr-ns".into(),
                version: "1.0.0".into(),
                proto_schema: minimal_file_descriptor_set(),
            }],
            secrets: vec![],
        }),
    })
    .await?;
    c.upsert_routing_rule(make_rule("wr-r1", "wr-svc", "withrule-e1"))
        .await?;

    let v_before = get_routing_table_version(&mut c).await?;

    c.deregister_engine(DeregisterEngineRequest {
        engine_id: "withrule-e1".into(),
    })
    .await?;

    let v_after = get_routing_table_version(&mut c).await?;
    assert_eq!(v_after, v_before + 1);

    Ok(())
}

// ── Health monitor + upsert concurrent access ───────────────────────────────

/// The background health monitor and an upsert both try to bump the version.
/// The monitor uses blocking FOR UPDATE; the upsert uses NOWAIT.
/// When contention happens the upsert gets Aborted but succeeds on retry.
#[tokio::test]
async fn test_health_monitor_and_upsert_coexist() -> Result<()> {
    let pool = manager_pool().await;
    let mgr_addr = start_manager_with_monitor(pool.clone(), 1).await?;
    let mut c = manager_client(&mgr_addr).await?;

    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;
    register_module(
        &mut c,
        EngineSpec {
            id: "hm-e1",
            addr: &engine_addr,
            proxy_address: "",
        },
        ModuleSpec {
            namespace: "hm-ns",
            name: "hm-svc",
            version: "1.0.0",
            schema: minimal_file_descriptor_set(),
        },
    )
    .await?;

    // Backdate heartbeat so the monitor will flip health and acquire the lock.
    backdate_engine_heartbeat(&pool, "hm-e1", 60).await;

    // While the monitor is running (200ms ticks), do upserts.
    // The db layer's built-in retry handles any contention with the monitor.
    for i in 0..3 {
        let rule = make_rule(
            &format!("hm-r{i}"),
            &format!("hm-extra-{i}"),
            &format!("hm-extra-e{i}"),
        );
        c.upsert_routing_rule(rule).await?;
    }

    // All 3 extra rules should be present.
    let table = c
        .get_routing_table(GetRoutingTableRequest { known_version: 0 })
        .await?
        .into_inner()
        .table
        .unwrap();

    let extra_count = table
        .rules
        .iter()
        .filter(|r| r.rule_id.starts_with("hm-r"))
        .count();
    assert_eq!(extra_count, 3, "all upserted rules should be present");

    let _ = engine_shutdown.send(());
    Ok(())
}

// ── get_routing_table returns None when version matches ─────────────────────

#[tokio::test]
async fn test_get_routing_table_returns_none_when_up_to_date() -> Result<()> {
    let pool = manager_pool().await;
    let addr = start_manager(pool).await?;
    let mut c = manager_client(&addr).await?;

    c.upsert_routing_rule(make_rule("uptodate-r1", "uptodate-svc", "e1"))
        .await?;

    let current_version = get_routing_table_version(&mut c).await?;

    // Request with the current version — should get empty response (no table).
    let resp = c
        .get_routing_table(GetRoutingTableRequest {
            known_version: current_version,
        })
        .await?
        .into_inner();

    assert!(
        resp.table.is_none(),
        "should return None when client version matches",
    );

    Ok(())
}
