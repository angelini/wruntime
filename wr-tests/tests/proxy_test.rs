#[allow(dead_code, unused_imports)]
mod helpers;
use helpers::*;

use anyhow::Result;
use http::StatusCode;

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
        body.contains("/store.inventory-service"),
        "expected stub to echo request path, got: {body}"
    );

    let _ = engine_shutdown.send(());
    Ok(())
}
