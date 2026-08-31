mod helpers;
use helpers::{
    db::manager_pool,
    manager::{
        manager_trio, register_test_module_raw, register_test_module_ready, start_manager_cluster,
        sync_table, synced_routing_table,
    },
    proxy::{http_client, proxy_get, start_proxy},
    stubs::spawn_stub_engine,
    wasm::{invalid_protobuf, minimal_file_descriptor_set},
};

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use http::{Request, StatusCode};
use http_body_util::Full;

use wr_common::discovery::ManagerDiscovery;
use wr_common::wruntime::node_service_server::NodeService; // brings register_engine into scope
use wr_common::wruntime::{
    BeginEngineDrainRequest, EngineRegistration, GetRoutingTableRequest, HeartbeatRequest,
    ModuleDescriptor, RegisterEngineRequest,
};
use wr_proxy::node_service::NodeAgent;

#[tokio::test]
async fn test_proxy_routes_to_engine() -> Result<()> {
    let (pool, mgr_addr, mut mgr_c) = manager_trio().await?;

    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;

    register_test_module_ready(
        &pool,
        &mut mgr_c,
        "stub-engine",
        &engine_addr,
        "store",
        "inventory-service",
        "1.0.0",
    )
    .await?;

    let table = synced_routing_table(&mgr_addr).await?;
    let proxy = start_proxy(table).await?;

    let (status, body) = proxy_get(proxy, "store", "inventory-service", Some("1.0.0")).await?;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("/Ping"),
        "expected stub to echo request path, got: {body}"
    );

    let _ = engine_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn test_internal_proxy_trusts_protobuf_body() -> Result<()> {
    let (pool, mgr_addr, mut mgr_c) = manager_trio().await?;
    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;
    register_test_module_ready(
        &pool,
        &mut mgr_c,
        "trusted-engine",
        &engine_addr,
        "store",
        "inventory-service",
        "1.0.0",
    )
    .await?;

    let proxy = start_proxy(synced_routing_table(&mgr_addr).await?).await?;
    let response = http_client()
        .request(
            Request::builder()
                .method("POST")
                .uri(format!("http://{proxy}/test.PingService/Ping"))
                .header(
                    "x-wr-destination",
                    "http://store.inventory-service/test.PingService/Ping",
                )
                .body(Full::new(invalid_protobuf()))?,
        )
        .await?;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "trusted internal traffic must reach the engine without proxy schema validation"
    );

    let _ = engine_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn test_proxy_excludes_raw_registration_until_healthy() -> Result<()> {
    let (pool, mgr_addr, mut mgr_c) = manager_trio().await?;
    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;

    register_test_module_raw(
        &mut mgr_c,
        "proxy-ready-e1",
        &engine_addr,
        "store",
        "readiness-service",
        "1.0.0",
    )
    .await?;

    let table = synced_routing_table(&mgr_addr).await?;
    let proxy = start_proxy(table.clone()).await?;

    let (status, _) = proxy_get(proxy, "store", "readiness-service", Some("1.0.0")).await?;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

    mgr_c
        .heartbeat(HeartbeatRequest {
            engine_id: "proxy-ready-e1".into(),
            healthy_modules: vec![ModuleDescriptor {
                name: "readiness-service".into(),
                namespace: "store".into(),
                version: "1.0.0".into(),
                proto_schema: vec![],
            }],
        })
        .await?;
    wr_manager::db::update_route_health(&pool, 30.0, 30.0)
        .await
        .map_err(|status| anyhow::anyhow!("update_route_health failed: {status}"))?;
    sync_table(&mgr_addr, &table).await?;

    let (status, body) = proxy_get(proxy, "store", "readiness-service", Some("1.0.0")).await?;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("/Ping"));

    let _ = engine_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn test_proxy_register_engine_forwards_without_creating_rules() -> Result<()> {
    let (pool, mgr_addr, mut mgr_c) = manager_trio().await?;

    // ManagerDiscovery resolves managers from wr_managers — register the test
    // manager (plaintext, no TLS) so the NodeAgent can forward to it.
    wr_manager::db::register_manager(&pool, "proxy-test-mgr", &mgr_addr, "127.0.0.1:0").await?;
    let discovery = Arc::new(ManagerDiscovery::new(pool.clone(), None));
    discovery.refresh().await;

    let routing = wr_proxy::routing::new_routing_table(
        wr_proxy::config::CircuitBreakerConfig::default(),
        "https://127.0.0.1:9443",
    );
    let agent = Arc::new(NodeAgent::new(discovery, routing));

    let resp = agent
        .register_engine(tonic::Request::new(RegisterEngineRequest {
            registration: Some(EngineRegistration {
                engine_id: "proxy-e1".into(),
                address: "http://127.0.0.1:9700".into(),
                proxy_address: "https://test-node:9443".into(),
                peer_address: "https://127.0.0.1:9443".into(),
                modules: vec![ModuleDescriptor {
                    name: "inventory".into(),
                    namespace: "store".into(),
                    version: "1.0.0".into(),
                    proto_schema: minimal_file_descriptor_set(),
                }],
                secrets: vec![],
                db_namespaces: vec![],
                deployment: None,
            }),
        }))
        .await?
        .into_inner();
    assert!(resp.accepted);

    let table = mgr_c
        .get_routing_table(GetRoutingTableRequest { known_version: 0 })
        .await?
        .into_inner()
        .table
        .unwrap();
    assert_eq!(
        table.rules.len(),
        1,
        "proxy must forward only; the single rule is the manager-created default",
    );
    assert_eq!(table.rules[0].rule_id, "proxy-e1/store/inventory/1.0.0");
    assert!(
        !table.rules[0].healthy,
        "forwarded manager-created default starts unhealthy"
    );

    let readiness = agent
        .heartbeat(tonic::Request::new(HeartbeatRequest {
            engine_id: "proxy-e1".into(),
            healthy_modules: vec![ModuleDescriptor {
                name: "inventory".into(),
                namespace: "store".into(),
                version: "1.0.0".into(),
                proto_schema: vec![],
            }],
        }))
        .await?
        .into_inner();
    assert!(
        readiness.proxy_routing_table_version >= readiness.manager_routing_table_version,
        "first heartbeat must synchronously converge the local proxy"
    );

    let mut tasks = wr_common::task_group::TaskGroup::new();
    let heartbeat_agent = Arc::clone(&agent);
    tasks.spawn("test-heartbeat-loop", move |cancellation| {
        heartbeat_agent.run_heartbeat_loop(Duration::from_millis(20), cancellation)
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let heartbeat_count: i64 = pool
        .get()
        .await?
        .query_one(
            "SELECT COUNT(*) FROM wr_module_heartbeats WHERE engine_id = $1",
            &[&"proxy-e1"],
        )
        .await?
        .get(0);
    assert_eq!(heartbeat_count, 1, "ready heartbeat must reach the manager");

    let withdrawal = agent
        .begin_engine_drain(tonic::Request::new(BeginEngineDrainRequest {
            engine_id: "proxy-e1".into(),
        }))
        .await?
        .into_inner();
    assert!(
        withdrawal.proxy_routing_table_version >= withdrawal.manager_routing_table_version,
        "drain must synchronously converge route withdrawal"
    );
    let stale = match agent
        .heartbeat(tonic::Request::new(HeartbeatRequest {
            engine_id: "proxy-e1".into(),
            healthy_modules: vec![],
        }))
        .await
    {
        Ok(_) => anyhow::bail!("draining engine heartbeat was not fenced"),
        Err(status) => status,
    };
    assert_eq!(stale.code(), tonic::Code::FailedPrecondition);
    let heartbeat_before: f64 = pool
        .get()
        .await?
        .query_one(
            "SELECT EXTRACT(EPOCH FROM last_healthy)::double precision
             FROM wr_module_heartbeats WHERE engine_id = $1",
            &[&"proxy-e1"],
        )
        .await?
        .get(0);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let heartbeat_after: f64 = pool
        .get()
        .await?
        .query_one(
            "SELECT EXTRACT(EPOCH FROM last_healthy)::double precision
             FROM wr_module_heartbeats WHERE engine_id = $1",
            &[&"proxy-e1"],
        )
        .await?
        .get(0);
    assert_eq!(
        heartbeat_after, heartbeat_before,
        "a stale periodic snapshot must not refresh heartbeat state after drain"
    );

    let report = tasks
        .shutdown(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert!(report.is_clean(), "{report:?}");
    Ok(())
}

#[tokio::test]
async fn test_discovery_refreshes_via_list_managers() -> Result<()> {
    let pool = manager_pool().await;
    // A real manager (holds a ClusterHandle + published metadata) registered in wr_managers.
    let managers = start_manager_cluster(pool.clone(), 1, 30).await?;

    let discovery = Arc::new(ManagerDiscovery::new(pool.clone(), None));
    discovery.refresh().await; // cold-start DB seed → ListManagers RPC → cache

    // A client can be obtained, i.e. the cache was populated with a reachable addr.
    let client = discovery.get_client().await;
    assert!(client.is_ok(), "discovery should have a reachable manager");
    let _ = managers;
    Ok(())
}

#[tokio::test]
async fn test_discovery_falls_back_to_db_when_no_manager_reachable() -> Result<()> {
    let pool = manager_pool().await;
    // A fresh wr_managers row whose grpc_address has no server behind it.
    wr_manager::db::register_manager(
        &pool,
        "unreachable-mgr",
        "http://127.0.0.1:1",
        "127.0.0.1:0",
    )
    .await?;

    let discovery = Arc::new(ManagerDiscovery::new(pool.clone(), None));
    discovery.refresh().await; // cold-start DB seed; ListManagers unreachable → DB fallback keeps the row

    assert!(discovery.get_client().await.is_err());
    Ok(())
}
