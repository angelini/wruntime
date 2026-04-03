#[allow(dead_code, unused_imports)]
mod helpers;
use helpers::*;

use anyhow::Result;
use http::StatusCode;

#[tokio::test]
async fn test_proxy_routes_to_engine() -> Result<()> {
    let pool = manager_pool().await;
    let mgr_addr = start_manager(pool).await?;
    let mut mgr_c = manager_client(&mgr_addr).await?;

    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;

    register_module(
        &mut mgr_c,
        EngineSpec {
            id: "stub-engine",
            addr: &engine_addr,
            proxy_address: "",
        },
        ModuleSpec {
            namespace: "store",
            name: "inventory-service",
            version: "1.0.0",
            schema: minimal_file_descriptor_set(),
        },
    )
    .await?;

    let table = wr_proxy::routing::new_routing_table();
    sync_table(&mgr_addr, &table).await?;
    let proxy = start_proxy(table).await?;

    let (status, body) = proxy_get(proxy, "store", "inventory-service", Some("1.0.0")).await?;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("/store.inventory-service"),
        "expected stub to echo request path, got: {body}"
    );

    let _ = engine_shutdown.send(());
    Ok(())
}
