mod helpers;
use helpers::{
    manager::{manager_trio, register_test_module_ready, synced_routing_table},
    proxy::{http_client, start_ingress_proxy, ExternalRoute},
    stubs::{spawn_capturing_stub, spawn_stub_engine, CapturedRequest},
    wasm::{invalid_protobuf, minimal_file_descriptor_set, valid_ping_request},
};

use std::sync::Arc;

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
    let (pool, mgr_addr, mut mgr_c) = manager_trio().await?;

    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;
    register_test_module_ready(
        &pool,
        &mut mgr_c,
        "e1",
        &engine_addr,
        namespace,
        module,
        "1.0.0",
    )
    .await?;

    let table = synced_routing_table(&mgr_addr).await?;
    let schema_cache = Arc::new(wr_proxy::schema::SchemaCache::default());
    schema_cache
        .insert(namespace, module, "1.0.0", &minimal_file_descriptor_set())
        .await?;

    let ingress_addr = start_ingress_proxy(table, schema_cache, routes).await?;
    Ok((ingress_addr, engine_shutdown))
}

async fn capturing_ingress_fixture(
    module: &str,
    namespace: &str,
    routes: Vec<ExternalRoute>,
) -> Result<(
    std::net::SocketAddr,
    tokio::sync::oneshot::Sender<()>,
    tokio::sync::mpsc::Receiver<CapturedRequest>,
)> {
    let (pool, mgr_addr, mut mgr_c) = manager_trio().await?;
    let (engine_addr, engine_shutdown, captures) = spawn_capturing_stub().await?;
    register_test_module_ready(
        &pool,
        &mut mgr_c,
        "e1",
        &engine_addr,
        namespace,
        module,
        "1.0.0",
    )
    .await?;

    let table = synced_routing_table(&mgr_addr).await?;
    let schema_cache = Arc::new(wr_proxy::schema::SchemaCache::default());
    schema_cache
        .insert(namespace, module, "1.0.0", &minimal_file_descriptor_set())
        .await?;
    let ingress_addr = start_ingress_proxy(table, schema_cache, routes).await?;
    Ok((ingress_addr, engine_shutdown, captures))
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

fn route(path: &str, methods: &[&str]) -> ExternalRoute {
    ExternalRoute::new(
        path,
        "/test.PingService/Ping",
        methods.iter().map(|method| (*method).to_string()).collect(),
        "inventory",
        "ecommerce",
    )
    .expect("test route must be valid")
}

#[tokio::test]
async fn test_external_route_dispatches_to_engine() -> Result<()> {
    let routes = vec![route("/items", &[])];
    let (addr, _shutdown) = ingress_fixture("inventory", "ecommerce", routes).await?;

    let (status, body) = external_get(addr, "/items").await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body, "/test.PingService/Ping",
        "ingress should rewrite the public alias to its canonical RPC path"
    );
    Ok(())
}

#[tokio::test]
async fn test_external_route_wildcard_segment() -> Result<()> {
    let routes = vec![route("/items/{id}", &[])];
    let (addr, _shutdown) = ingress_fixture("inventory", "ecommerce", routes).await?;

    let (status, body) = external_get(addr, "/items/42").await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "/test.PingService/Ping");
    Ok(())
}

#[tokio::test]
async fn test_external_route_rejects_invalid_protobuf() -> Result<()> {
    let routes = vec![route("/items", &["POST"])];
    let (addr, _shutdown) = ingress_fixture("inventory", "ecommerce", routes).await?;

    let response = http_client()
        .request(
            Request::builder()
                .method("POST")
                .uri(format!("http://{addr}/items"))
                .header("content-type", "application/x-protobuf")
                .body(Full::new(invalid_protobuf()))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response.into_body().collect().await?.to_bytes();
    assert!(String::from_utf8_lossy(&body).contains("protobuf schema validation"));
    Ok(())
}

#[tokio::test]
async fn test_external_route_rejects_oversized_body() -> Result<()> {
    let routes = vec![route("/items", &["POST"])];
    let (addr, _shutdown) = ingress_fixture("inventory", "ecommerce", routes).await?;

    let response = http_client()
        .request(
            Request::builder()
                .method("POST")
                .uri(format!("http://{addr}/items"))
                .header("content-type", "application/x-protobuf")
                .body(Full::new(bytes::Bytes::from(vec![0; 1025])))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    Ok(())
}

#[tokio::test]
async fn test_external_json_is_transcoded_and_response_is_unchanged() -> Result<()> {
    let routes = vec![route("/items", &["POST"])];
    let (addr, _shutdown, mut captures) =
        capturing_ingress_fixture("inventory", "ecommerce", routes).await?;

    let response = http_client()
        .request(
            Request::builder()
                .method("POST")
                .uri(format!("http://{addr}/items"))
                .header("content-type", "application/json; charset=UTF-8")
                .header("content-encoding", "identity")
                .header("content-digest", "sha-256=:invalid-after-transcoding:")
                .body(Full::new(bytes::Bytes::from_static(
                    br#"{"message":"hello"}"#,
                )))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/vnd.stub+json"
    );
    assert_eq!(
        response.headers().get("set-cookie").unwrap(),
        "stub=true; HttpOnly"
    );
    assert_eq!(response.headers().get("location").unwrap(), "/created/1");
    let response_body = response.into_body().collect().await?.to_bytes();
    assert_eq!(response_body, "stub-response");

    let captured = captures
        .recv()
        .await
        .expect("engine should receive request");
    assert_eq!(captured.path, "/test.PingService/Ping");
    assert_eq!(
        captured.content_type.as_deref(),
        Some("application/x-protobuf")
    );
    assert_eq!(captured.content_length.as_deref(), Some("7"));
    assert_eq!(captured.content_encoding, None);
    assert_eq!(captured.content_digest, None);
    assert_eq!(captured.body, valid_ping_request());
    Ok(())
}

#[tokio::test]
async fn test_external_form_is_transcoded_to_protobuf() -> Result<()> {
    let routes = vec![route("/items", &["POST"])];
    let (addr, _shutdown, mut captures) =
        capturing_ingress_fixture("inventory", "ecommerce", routes).await?;

    let response = http_client()
        .request(
            Request::builder()
                .method("POST")
                .uri(format!("http://{addr}/items"))
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Full::new(bytes::Bytes::from_static(b"message=hello+world")))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::CREATED);

    let captured = captures
        .recv()
        .await
        .expect("engine should receive request");
    assert_eq!(
        captured.content_type.as_deref(),
        Some("application/x-protobuf")
    );
    assert_eq!(
        captured.body,
        bytes::Bytes::from_static(b"\x0a\x0bhello world")
    );
    Ok(())
}

#[tokio::test]
async fn test_external_nonempty_body_requires_supported_content_type() -> Result<()> {
    let routes = vec![route("/items", &["POST"])];
    let (addr, _shutdown, mut captures) =
        capturing_ingress_fixture("inventory", "ecommerce", routes).await?;

    for content_type in [None, Some("text/plain")] {
        let mut request = Request::builder()
            .method("POST")
            .uri(format!("http://{addr}/items"));
        if let Some(content_type) = content_type {
            request = request.header("content-type", content_type);
        }
        let response = http_client()
            .request(request.body(Full::new(valid_ping_request()))?)
            .await?;
        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }
    assert!(
        captures.try_recv().is_err(),
        "rejected bodies must not be forwarded"
    );
    Ok(())
}

#[tokio::test]
async fn test_external_route_unmatched_path_returns_404() -> Result<()> {
    let routes = vec![route("/items", &[])];
    let (addr, _shutdown) = ingress_fixture("inventory", "ecommerce", routes).await?;

    let (status, _) = external_get(addr, "/orders").await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    Ok(())
}

#[tokio::test]
async fn test_external_route_method_filter() -> Result<()> {
    let routes = vec![route("/items", &["get"])];
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
    let routes = vec![route("/items", &[])];
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
