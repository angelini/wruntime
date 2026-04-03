#[allow(dead_code, unused_imports)]
mod helpers;
use helpers::*;

use anyhow::Result;
use http::{Request, StatusCode};
use http_body_util::{BodyExt, Full};

/// Spin up a manager + stub engine registered as `namespace.module`, then start
/// an ingress proxy with the given `routes`.  Returns `(ingress_addr, engine_shutdown)`.
async fn ingress_fixture(
    module: &str,
    namespace: &str,
    routes: Vec<ExternalRoute>,
) -> Result<(std::net::SocketAddr, tokio::sync::oneshot::Sender<()>)> {
    let pool = manager_pool().await;
    let mgr_addr = start_manager(pool).await?;
    let mut mgr_c = manager_client(&mgr_addr).await?;

    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;
    register_module(
        &mut mgr_c,
        EngineSpec {
            id: "e1",
            addr: &engine_addr,
            proxy_address: "",
        },
        ModuleSpec {
            namespace,
            name: module,
            version: "1.0.0",
            schema: minimal_file_descriptor_set(),
        },
    )
    .await?;

    let table = wr_proxy::routing::new_routing_table();
    sync_table(&mgr_addr, &table).await?;

    let ingress_addr = start_ingress_proxy(table, routes).await?;
    Ok((ingress_addr, engine_shutdown))
}

/// Send a plain HTTP request directly to `addr` (no wruntime headers).
async fn external_get(addr: std::net::SocketAddr, path: &str) -> Result<(StatusCode, String)> {
    external_request(addr, "GET", path, &[]).await
}

async fn external_request(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    extra_headers: &[(&str, &str)],
) -> Result<(StatusCode, String)> {
    let mut builder = Request::builder()
        .method(method)
        .uri(format!("http://{addr}{path}"));
    for (k, v) in extra_headers {
        builder = builder.header(*k, *v);
    }
    let resp = http_client()
        .request(builder.body(Full::new(bytes::Bytes::new()))?)
        .await?;
    let status = resp.status();
    let body = resp.into_body().collect().await?.to_bytes();
    Ok((status, String::from_utf8_lossy(&body).into_owned()))
}

#[tokio::test]
async fn test_external_route_dispatches_to_engine() -> Result<()> {
    let routes = vec![ExternalRoute {
        path: "/items".into(),
        methods: vec![],
        module: "inventory".into(),
        namespace: "ecommerce".into(),
    }];
    let (addr, _shutdown) = ingress_fixture("inventory", "ecommerce", routes).await?;

    let (status, body) = external_get(addr, "/items").await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "/items", "stub engine should echo the request path");
    Ok(())
}

#[tokio::test]
async fn test_external_route_wildcard_segment() -> Result<()> {
    let routes = vec![ExternalRoute {
        path: "/items/{id}".into(),
        methods: vec![],
        module: "inventory".into(),
        namespace: "ecommerce".into(),
    }];
    let (addr, _shutdown) = ingress_fixture("inventory", "ecommerce", routes).await?;

    let (status, body) = external_get(addr, "/items/42").await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "/items/42");
    Ok(())
}

#[tokio::test]
async fn test_external_route_unmatched_path_returns_404() -> Result<()> {
    let routes = vec![ExternalRoute {
        path: "/items".into(),
        methods: vec![],
        module: "inventory".into(),
        namespace: "ecommerce".into(),
    }];
    let (addr, _shutdown) = ingress_fixture("inventory", "ecommerce", routes).await?;

    let (status, _) = external_get(addr, "/orders").await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    Ok(())
}

#[tokio::test]
async fn test_external_route_method_filter() -> Result<()> {
    let routes = vec![ExternalRoute {
        path: "/items".into(),
        methods: vec!["GET".into()],
        module: "inventory".into(),
        namespace: "ecommerce".into(),
    }];
    let (addr, _shutdown) = ingress_fixture("inventory", "ecommerce", routes).await?;

    let (get_status, _) = external_request(addr, "GET", "/items", &[]).await?;
    assert_eq!(get_status, StatusCode::OK, "GET should be allowed");

    let (post_status, _) = external_request(addr, "POST", "/items", &[]).await?;
    assert_eq!(post_status, StatusCode::NOT_FOUND, "POST should be blocked");
    Ok(())
}

#[tokio::test]
async fn test_external_route_strips_spoofed_internal_headers() -> Result<()> {
    // Route /items → ecommerce.inventory.
    // A malicious caller also sends x-wr-destination pointing to a non-existent
    // module.  The ingress layer must strip it so routing uses the configured
    // destination, not the spoofed one.
    let routes = vec![ExternalRoute {
        path: "/items".into(),
        methods: vec![],
        module: "inventory".into(),
        namespace: "ecommerce".into(),
    }];
    let (addr, _shutdown) = ingress_fixture("inventory", "ecommerce", routes).await?;

    let (status, _) = external_request(
        addr,
        "GET",
        "/items",
        &[("x-wr-destination", "http://nonexistent.other/items")],
    )
    .await?;
    // If the spoofed header survived, routing would fail (no rule for nonexistent.other)
    // and the proxy would return 503.  Getting 200 proves it was stripped.
    assert_eq!(
        status,
        StatusCode::OK,
        "spoofed x-wr-destination must be overwritten by ingress layer"
    );
    Ok(())
}
