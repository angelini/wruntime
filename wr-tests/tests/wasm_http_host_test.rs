mod helpers;

use anyhow::Result;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use prost::Message;

use helpers::{
    db::{ModuleServices, ModuleState},
    manager::{manager_trio, register_test_module, synced_routing_table},
    proto,
    proxy::{
        http_client, http_pool, start_egress_proxy, start_ingress_proxy, EgressConfig,
        ExternalRoute,
    },
    stubs::spawn_http1_stub,
    wasm::{spawn_wasm_stub_engine, GuestHarness, TestGuest},
};

#[tokio::test]
async fn wasm_http_egress() -> Result<()> {
    let Some(harness) = GuestHarness::load(TestGuest::Http).await? else {
        return Ok(());
    };

    // External HTTP/1.1 stub (stands in for example.com).
    let (ext_addr, _ext_shutdown) = spawn_http1_stub().await?;
    let ext_uri: http::Uri = ext_addr.parse()?;
    let ext_authority = ext_uri.authority().unwrap().to_string();

    // Egress proxy with 127.0.0.1 in the allowlist.
    let table = wr_proxy::routing::new_routing_table();
    let egress_cfg = EgressConfig {
        allowed_domains: vec!["127.0.0.1".into()],
    };
    let proxy_addr = start_egress_proxy(Some(egress_cfg), table).await?;
    let proxy_uri: hyper::Uri = format!("http://{proxy_addr}").parse()?;

    // Small outbound-body cap so the over-cap case is cheap.
    let cap = 1024usize;

    // Under the cap: succeeds and returns the stub's echo body.
    let under_state = ModuleState::new(
        "http-test".into(),
        "test-ns".into(),
        proxy_uri.clone(),
        http_pool(),
        ModuleServices {
            max_outbound_body_bytes: cap,
            ..ModuleServices::default()
        },
    )?;
    let under_req = proto::EgressRequest {
        authority: ext_authority.clone(),
        path: "/hello-egress".into(),
        body: vec![b'x'; 16],
    };
    let resp = harness.dispatch(under_state, "/Egress", under_req).await?;
    assert_eq!(resp.status(), 200);
    let body = proto::EgressResponse::decode(resp.into_body())?;
    assert_eq!(body.status, 200);
    assert_eq!(body.body, "egress:/hello-egress");

    // Over the cap: the outbound body exceeds `cap`, so send_request returns
    // HttpRequestBodySize; the guest maps the failed http_rpc to a 500.
    let over_state = ModuleState::new(
        "http-test".into(),
        "test-ns".into(),
        proxy_uri.clone(),
        http_pool(),
        ModuleServices {
            max_outbound_body_bytes: cap,
            ..ModuleServices::default()
        },
    )?;
    let over_req = proto::EgressRequest {
        authority: ext_authority.clone(),
        path: "/hello-egress".into(),
        body: vec![b'x'; 4096],
    };
    let resp = harness.dispatch(over_state, "/Egress", over_req).await?;
    assert_eq!(resp.status(), 500);
    let err_body = String::from_utf8_lossy(resp.into_body().as_ref()).into_owned();
    assert!(
        err_body.contains("egress call failed"),
        "unexpected error body: {err_body}"
    );

    Ok(())
}

#[tokio::test]
async fn wasm_http_ingress() -> Result<()> {
    let Some(harness) = GuestHarness::load(TestGuest::Http).await? else {
        return Ok(());
    };

    let (engine, pre) = harness.engine_pre();

    // WASM-backed HTTP/2 engine.
    let (engine_addr, _engine_shutdown) =
        spawn_wasm_stub_engine(engine, pre, "http://127.0.0.1:9001", "http-svc", "test-ns").await?;

    // Manager + registration.
    let (_pool, mgr_addr, mut client) = manager_trio().await?;
    register_test_module(
        &mut client,
        "wasm-engine-1",
        &engine_addr,
        "test-ns",
        "http-svc",
        "1.0.0",
    )
    .await?;

    // Ingress proxy with a public route for Echo.
    let table = synced_routing_table(&mgr_addr).await?;
    let ingress_addr = start_ingress_proxy(
        table,
        vec![ExternalRoute {
            path: "/test.HttpTestService/Echo".into(),
            methods: vec!["POST".into()],
            module: "http-svc".into(),
            namespace: "test-ns".into(),
        }],
    )
    .await?;

    // Plain HTTP request — no x-wr-* headers — simulates external caller.
    let req_body = proto::EchoRequest {
        message: "hello from outside".into(),
    };
    let resp = http_client()
        .request(
            http::Request::builder()
                .method("POST")
                .uri(format!("http://{ingress_addr}/test.HttpTestService/Echo"))
                .body(Full::new(Bytes::from(req_body.encode_to_vec())))?,
        )
        .await?;

    assert_eq!(resp.status(), 200);
    let body_bytes = resp.into_body().collect().await?.to_bytes();
    let echo_resp = proto::EchoResponse::decode(body_bytes)?;
    assert_eq!(echo_resp.message, "echo:hello from outside");
    Ok(())
}
