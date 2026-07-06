mod helpers;
use helpers::{
    manager::{manager_trio, register_test_module, synced_routing_table},
    proxy::{http_client, proxy_get, start_egress_proxy, EgressConfig},
    stubs::{spawn_http1_stub, spawn_stub_engine},
};

use anyhow::Result;
use bytes::Bytes;
use http::{Request, StatusCode};
use http_body_util::{BodyExt, Full};

/// Allowed domain: proxy forwards the request to the external stub and returns
/// the stub's 200 response to the caller.
#[tokio::test]
async fn test_egress_allowed_domain() -> Result<()> {
    let (ext_base, ext_shutdown) = spawn_http1_stub().await?;

    let table = wr_proxy::routing::new_routing_table();
    let proxy_addr = start_egress_proxy(
        Some(EgressConfig {
            allowed_domains: vec!["127.0.0.1".into()],
        }),
        table,
    )
    .await?;

    let req = Request::builder()
        .method("GET")
        .uri(format!("http://{proxy_addr}/hello"))
        .header("x-wr-destination", format!("{ext_base}/hello"))
        .header("x-wr-source", "test-module")
        .body(Full::new(Bytes::new()))?;

    let resp = http_client().request(req).await?;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await?.to_bytes();
    assert_eq!(
        String::from_utf8_lossy(&body),
        "egress:/hello",
        "stub should echo the request path"
    );

    let _ = ext_shutdown.send(());
    Ok(())
}

/// Blocked domain: routing layer rejects with 503 because the host is not
/// in the egress allowlist and has no internal route.
#[tokio::test]
async fn test_egress_blocked_domain() -> Result<()> {
    let table = wr_proxy::routing::new_routing_table();
    let proxy_addr = start_egress_proxy(
        Some(EgressConfig {
            allowed_domains: vec!["127.0.0.1".into()],
        }),
        table,
    )
    .await?;

    let req = Request::builder()
        .method("GET")
        .uri(format!("http://{proxy_addr}/test"))
        .header(
            "x-wr-destination",
            "http://blocked.notallowed.example.com/test",
        )
        .header("x-wr-source", "test-module")
        .body(Full::new(Bytes::new()))?;

    let resp = http_client().request(req).await?;
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);

    Ok(())
}

/// Internal module calls must still route correctly when egress is configured.
#[tokio::test]
async fn test_egress_internal_module_passthrough() -> Result<()> {
    let (_pool, mgr_addr, mut mgr_c) = manager_trio().await?;

    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;
    register_test_module(
        &mut mgr_c,
        "stub-engine",
        &engine_addr,
        "store",
        "inventory",
        "1.0.0",
    )
    .await?;

    let table = synced_routing_table(&mgr_addr).await?;

    // Egress is enabled but the destination is a registered internal module.
    let proxy_addr = start_egress_proxy(
        Some(EgressConfig {
            allowed_domains: vec!["external.example.com".into()],
        }),
        table,
    )
    .await?;

    let (status, body) = proxy_get(proxy_addr, "store", "inventory", None).await?;
    assert_eq!(
        status,
        StatusCode::OK,
        "internal module call should succeed"
    );
    assert!(
        body.contains("/Ping"),
        "stub should echo the request path, got: {body}"
    );

    let _ = engine_shutdown.send(());
    Ok(())
}
