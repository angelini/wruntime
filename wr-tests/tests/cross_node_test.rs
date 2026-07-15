mod helpers;
use helpers::{
    manager::{manager_trio, register_test_module_ready_with_peer, sync_table},
    proxy::{proxy_get, start_node, EngineSpec, ModuleSpec},
    stubs::spawn_identified_stub,
    wasm::minimal_file_descriptor_set,
};

use anyhow::Result;
use http::StatusCode;

/// Two proxies simulate two separate nodes on 127.0.0.1.
/// A request entering node A must be forwarded to node B's proxy, which then
/// dispatches it to the engine registered on node B.
#[tokio::test]
async fn test_cross_node_routing() -> Result<()> {
    let (pool, mgr_addr, mut mgr) = manager_trio().await?;

    let (engine_b_addr, engine_b_shutdown) = spawn_identified_stub("engine-b").await?;

    // Start node B first to obtain its proxy address, then register the engine
    // under that address and re-sync so node B's routing table sees the rule.
    let node_b = start_node(&mgr_addr).await?;
    register_test_module_ready_with_peer(
        &pool,
        &mut mgr,
        EngineSpec {
            id: "engine-b-id",
            addr: &engine_b_addr,
            peer_address: &node_b.proxy_address,
        },
        ModuleSpec {
            namespace: "store",
            name: "cross-node-service",
            version: "1.0.0",
            schema: minimal_file_descriptor_set(),
        },
    )
    .await?;
    sync_table(&mgr_addr, &node_b.table).await?;

    // Start node A after registration so its initial sync picks up engine B's rule.
    // Since node A's self peer address ≠ node B's peer address, node A will forward cross-node.
    let node_a = start_node(&mgr_addr).await?;

    let (status, body) =
        proxy_get(node_a.addr, "store", "cross-node-service", Some("1.0.0")).await?;
    assert_eq!(status, StatusCode::OK, "cross-node request should succeed");
    assert_eq!(body, "engine-b", "request should reach engine-b via node B");

    let _ = engine_b_shutdown.send(());
    let _ = node_a.proxy_shutdown.send(());
    let _ = node_b.proxy_shutdown.send(());
    Ok(())
}
