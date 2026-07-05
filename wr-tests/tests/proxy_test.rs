#[allow(dead_code, unused_imports)]
mod helpers;
use helpers::*;

use std::sync::Arc;

use anyhow::Result;
use http::StatusCode;

use wr_common::discovery::ManagerDiscovery;
use wr_common::wruntime::node_service_server::NodeService; // brings register_engine into scope
use wr_common::wruntime::{
    EngineRegistration, GetRoutingTableRequest, ModuleDescriptor, RegisterEngineRequest,
};
use wr_proxy::node_service::NodeAgent;

#[tokio::test]
async fn test_proxy_routes_to_engine() -> Result<()> {
    let (_pool, mgr_addr, mut mgr_c) = manager_trio().await?;

    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;

    register_test_module(
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
async fn test_proxy_register_engine_forwards_without_creating_rules() -> Result<()> {
    let (pool, mgr_addr, mut mgr_c) = manager_trio().await?;

    // ManagerDiscovery resolves managers from wr_managers — register the test
    // manager (plaintext, no TLS) so the NodeAgent can forward to it.
    wr_manager::db::register_manager(&pool, "proxy-test-mgr", &mgr_addr, "127.0.0.1:0").await?;
    let discovery = Arc::new(ManagerDiscovery::new(pool.clone(), None));
    discovery.refresh().await;

    let agent = NodeAgent::new(discovery);

    let resp = agent
        .register_engine(tonic::Request::new(RegisterEngineRequest {
            registration: Some(EngineRegistration {
                engine_id: "proxy-e1".into(),
                address: "http://127.0.0.1:9700".into(),
                proxy_address: String::new(),
                peer_address: "https://127.0.0.1:9443".into(),
                modules: vec![ModuleDescriptor {
                    name: "inventory".into(),
                    namespace: "store".into(),
                    version: "1.0.0".into(),
                    proto_schema: minimal_file_descriptor_set(),
                }],
                secrets: vec![],
                db_namespaces: vec![],
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

    // No manager is actually reachable, so get_client errors (fallback path exercised, no panic).
    let client = discovery.get_client().await;
    assert!(client.is_err(), "no manager should be reachable");
    Ok(())
}
