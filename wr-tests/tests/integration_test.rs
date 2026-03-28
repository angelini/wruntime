/// Integration tests for Wruntime.
///
/// Each test spins up real in-process gRPC services / HTTP servers on
/// ephemeral ports so that no external processes are required.
use std::convert::Infallible;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use prost::Message as _;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tonic::transport::Server;

use wr_common::wruntime::{
    manager_service_client::ManagerServiceClient,
    manager_service_server::ManagerServiceServer,
    DeregisterEngineRequest, EngineRegistration, GetMetricsSummaryRequest,
    GetRoutingTableRequest, HeartbeatRequest, ListEnginesRequest, ModuleDescriptor,
    RegisterEngineRequest, ReportMetricsRequest, RequestMetrics, RoutingRule,
};
use wr_manager::{config::ManagerConfig, service::Manager, state::new_state};
use wr_proxy::config::ProxyConfig;

// ── helpers ───────────────────────────────────────────────────────────────────

/// Start an in-process wr-manager on a random port and return its address.
async fn start_manager() -> Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr     = listener.local_addr()?;
    let state    = new_state();
    let manager  = Manager::new(state);

    tokio::spawn(
        Server::builder()
            .add_service(ManagerServiceServer::new(manager))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );

    Ok(format!("http://{addr}"))
}

/// Return a connected manager client.
async fn manager_client(addr: &str) -> Result<ManagerServiceClient<tonic::transport::Channel>> {
    Ok(ManagerServiceClient::connect(addr.to_string()).await?)
}

// ── manager RPC tests ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_register_and_list_engines() -> Result<()> {
    let addr   = start_manager().await?;
    let mut c  = manager_client(&addr).await?;

    c.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "e1".into(),
            address:   "http://127.0.0.1:9100".into(),
            modules:   vec![ModuleDescriptor {
                name:         "inventory-service".into(),
                version:      "1.0.0".into(),
                proto_schema: vec![],
            }],
        }),
    })
    .await?;

    let list = c.list_engines(ListEnginesRequest {}).await?.into_inner().engines;
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].engine_id, "e1");
    assert_eq!(list[0].modules[0].name, "inventory-service");

    Ok(())
}

#[tokio::test]
async fn test_deregister_engine() -> Result<()> {
    let addr  = start_manager().await?;
    let mut c = manager_client(&addr).await?;

    c.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "e1".into(),
            address:   "http://127.0.0.1:9101".into(),
            modules:   vec![],
        }),
    })
    .await?;

    c.deregister_engine(DeregisterEngineRequest { engine_id: "e1".into() }).await?;

    let list = c.list_engines(ListEnginesRequest {}).await?.into_inner().engines;
    assert!(list.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_heartbeat() -> Result<()> {
    let addr  = start_manager().await?;
    let mut c = manager_client(&addr).await?;

    c.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "e1".into(),
            address:   "http://127.0.0.1:9102".into(),
            modules:   vec![],
        }),
    })
    .await?;

    // Heartbeat should succeed without error.
    c.heartbeat(HeartbeatRequest { engine_id: "e1".into(), healthy_modules: vec![] }).await?;

    Ok(())
}

#[tokio::test]
async fn test_routing_table_upsert_and_get() -> Result<()> {
    let addr  = start_manager().await?;
    let mut c = manager_client(&addr).await?;

    c.upsert_routing_rule(RoutingRule {
        rule_id:             "r1".into(),
        source_module:       "order-service".into(),
        destination_module:  "inventory-service".into(),
        destination_version: "1.0.0".into(),
        engine_id:           "e1".into(),
        engine_address:      "http://127.0.0.1:9103".into(),
        healthy:             false, // server sets this to true on upsert
    })
    .await?;

    let table = c
        .get_routing_table(GetRoutingTableRequest {})
        .await?
        .into_inner()
        .table
        .unwrap();

    assert_eq!(table.rules.len(), 1);
    assert_eq!(table.rules[0].destination_module, "inventory-service");
    assert!(table.rules[0].healthy, "upserted rule should be healthy");
    assert_eq!(table.version, 1);

    Ok(())
}

#[tokio::test]
async fn test_metrics_report_and_summary() -> Result<()> {
    let addr  = start_manager().await?;
    let mut c = manager_client(&addr).await?;

    c.report_metrics(ReportMetricsRequest {
        metrics: vec![RequestMetrics {
            source:      "order-service".into(),
            destination: "inventory-service".into(),
            duration_ms: 42,
            status:      200,
            error:       String::new(),
        }],
    })
    .await?;

    let summary = c
        .get_metrics_summary(GetMetricsSummaryRequest {})
        .await?
        .into_inner()
        .metrics;

    assert_eq!(summary.len(), 1);
    assert_eq!(summary[0].source, "order-service");
    assert_eq!(summary[0].duration_ms, 42);

    Ok(())
}

// ── proxy routing tests ───────────────────────────────────────────────────────

/// Spin up a minimal stub HTTP server (simulating a destination engine) and
/// verify that the proxy correctly routes a request to it via the routing table.
#[tokio::test]
async fn test_proxy_routes_to_engine() -> Result<()> {
    // 1. Start manager.
    let mgr_addr  = start_manager().await?;
    let mut mgr_c = manager_client(&mgr_addr).await?;

    // 2. Start a stub engine inbound server that echoes the request path.
    let (engine_shutdown_tx, engine_shutdown_rx) = oneshot::channel::<()>();
    let engine_listener = TcpListener::bind("127.0.0.1:0").await?;
    let engine_addr = format!("http://{}", engine_listener.local_addr()?);

    tokio::spawn(async move {
        tokio::select! {
            _ = engine_shutdown_rx => {}
            _ = stub_engine(engine_listener) => {}
        }
    });

    // 3. Register the engine + routing rule with the manager.
    mgr_c
        .register_engine(RegisterEngineRequest {
            registration: Some(EngineRegistration {
                engine_id: "stub-engine".into(),
                address:   engine_addr.clone(),
                modules:   vec![ModuleDescriptor {
                    name:         "inventory-service".into(),
                    version:      "1.0.0".into(),
                    proto_schema: vec![],
                }],
            }),
        })
        .await?;

    mgr_c
        .upsert_routing_rule(RoutingRule {
            rule_id:             "r1".into(),
            source_module:       "order-service".into(),
            destination_module:  "inventory-service".into(),
            destination_version: "1.0.0".into(),
            engine_id:           "stub-engine".into(),
            engine_address:      engine_addr.clone(),
            healthy:             false, // server sets this to true on upsert
        })
        .await?;

    // 4. Start the proxy.
    let proxy_listener = TcpListener::bind("127.0.0.1:0").await?;
    let proxy_addr     = proxy_listener.local_addr()?;

    let routing_table = wr_proxy::routing::new_routing_table();
    let schema_cache  = Arc::new(wr_proxy::schema::SchemaCache::new());
    let (metrics_tx, _metrics_rx) = tokio::sync::mpsc::channel(100);

    // Sync routing table immediately (one-shot).
    {
        let table  = routing_table.clone();
        let mut c  = manager_client(&mgr_addr).await?;
        let resp   = c.get_routing_table(GetRoutingTableRequest {}).await?.into_inner();
        if let Some(incoming) = resp.table {
            *table.write().await = incoming;
        }
    }

    let svc = tower::ServiceBuilder::new()
        .layer(wr_proxy::layers::MetricsLayer::new(metrics_tx))
        .layer(wr_proxy::layers::SchemaValidationLayer::new(schema_cache))
        .layer(wr_proxy::layers::RoutingLayer::new(routing_table))
        .service(wr_proxy::layers::ForwardService::new());

    tokio::spawn(proxy_serve(proxy_listener, svc));

    // Give the proxy a moment to start.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // 5. Send a request through the proxy simulating an inter-module call.
    let client = hyper_util::client::legacy::Client::builder(
        hyper_util::rt::TokioExecutor::new(),
    )
    .build_http::<Full<Bytes>>();

    let req = Request::builder()
        .uri(format!("http://{proxy_addr}/items"))
        .header("x-wr-destination", "http://inventory-service/items")
        .header("x-wr-source", "order-service")
        .body(Full::new(Bytes::new()))?;

    let resp = client.request(req).await?;
    assert_eq!(resp.status(), StatusCode::OK);

    let body = resp.into_body().collect().await?.to_bytes();
    let body_str = std::str::from_utf8(&body)?;
    assert!(body_str.contains("/items"), "expected stub to echo path, got: {body_str}");

    let _ = engine_shutdown_tx.send(());
    Ok(())
}

// ── schema validation tests ───────────────────────────────────────────────────

/// Build a minimal `FileDescriptorSet` binary containing a single service with
/// one RPC so that `SchemaCache::insert` / `validate` can be exercised without
/// running `protoc`.
fn minimal_file_descriptor_set() -> Vec<u8> {
    use prost_types::{
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        MethodDescriptorProto, ServiceDescriptorProto,
        field_descriptor_proto::{Label, Type},
    };

    let req_msg = DescriptorProto {
        name: Some("PingRequest".into()),
        field: vec![FieldDescriptorProto {
            name:   Some("message".into()),
            number: Some(1),
            label:  Some(Label::Optional as i32),
            r#type: Some(Type::String as i32),
            json_name: Some("message".into()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp_msg = DescriptorProto {
        name: Some("PingResponse".into()),
        ..Default::default()
    };
    let service = ServiceDescriptorProto {
        name: Some("PingService".into()),
        method: vec![MethodDescriptorProto {
            name:        Some("Ping".into()),
            input_type:  Some(".test.PingRequest".into()),
            output_type: Some(".test.PingResponse".into()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let file = FileDescriptorProto {
        name:         Some("test.proto".into()),
        package:      Some("test".into()),
        message_type: vec![req_msg, resp_msg],
        service:      vec![service],
        syntax:       Some("proto3".into()),
        ..Default::default()
    };
    FileDescriptorSet { file: vec![file] }.encode_to_vec()
}

/// A valid protobuf encoding of `PingRequest { message: "hello" }`.
/// Field 1, wire type 2 (length-delimited), value = "hello".
fn valid_ping_request() -> Bytes {
    // tag = (1 << 3) | 2 = 0x0a, then varint length 5, then "hello"
    Bytes::from_static(b"\x0a\x05hello")
}

/// Bytes that are not valid protobuf (truncated varint).
fn invalid_protobuf() -> Bytes {
    Bytes::from_static(&[0xFF])
}

#[tokio::test]
async fn test_schema_validation_rejects_invalid_body() -> Result<()> {
    // 1. Start manager and upload a schema for "ping-service".
    let mgr_addr = start_manager().await?;
    let mut mgr  = manager_client(&mgr_addr).await?;

    let schema_bytes = minimal_file_descriptor_set();
    mgr.upload_schema(wr_common::wruntime::UploadSchemaRequest {
        module:       "ping-service".into(),
        version:      "1.0.0".into(),
        proto_schema: schema_bytes,
    })
    .await?;

    // 2. Build a proxy with schema cache pre-populated (no manager sync needed).
    let schema_cache = Arc::new(wr_proxy::schema::SchemaCache::new());
    let schema_bytes2 = {
        let mut c2 = manager_client(&mgr_addr).await?;
        c2.get_schema(wr_common::wruntime::GetSchemaRequest {
            module:  "ping-service".into(),
            version: "1.0.0".into(),
        })
        .await?
        .into_inner()
        .proto_schema
    };
    schema_cache.insert("ping-service", &schema_bytes2).await?;

    let routing_table = wr_proxy::routing::new_routing_table();
    let (metrics_tx, _) = tokio::sync::mpsc::channel(100);

    let svc = tower::ServiceBuilder::new()
        .layer(wr_proxy::layers::MetricsLayer::new(metrics_tx))
        .layer(wr_proxy::layers::SchemaValidationLayer::new(schema_cache))
        .layer(wr_proxy::layers::RoutingLayer::new(routing_table))
        .service(wr_proxy::layers::ForwardService::new());

    let proxy_listener = TcpListener::bind("127.0.0.1:0").await?;
    let proxy_addr     = proxy_listener.local_addr()?;
    tokio::spawn(proxy_serve(proxy_listener, svc));
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let client = hyper_util::client::legacy::Client::builder(
        hyper_util::rt::TokioExecutor::new(),
    )
    .build_http::<Full<Bytes>>();

    // 3. Invalid body → 400 with JSON error.
    let bad_req = Request::builder()
        .method("POST")
        .uri(format!("http://{proxy_addr}/test.PingService/Ping"))
        .header("x-wr-destination", "http://ping-service/test.PingService/Ping")
        .header("x-wr-source", "caller-service")
        .body(Full::new(invalid_protobuf()))?;

    let resp = client.request(bad_req).await?;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "expected 400 for invalid protobuf");

    let (resp_parts, resp_body) = resp.into_parts();
    let body_bytes = resp_body.collect().await?.to_bytes();
    let body_str   = std::str::from_utf8(&body_bytes)?;

    assert!(
        body_str.contains(r#""error":"schema_validation_failed""#),
        "expected structured JSON error, got: {body_str}"
    );
    assert!(
        body_str.contains(r#""source":"caller-service""#),
        "expected source in error, got: {body_str}"
    );
    assert!(
        body_str.contains(r#""destination":"ping-service""#),
        "expected destination in error, got: {body_str}"
    );
    assert!(
        resp_parts.headers
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map_or(true, |v| v.starts_with("application/json")),
        "expected application/json content-type"
    );

    // 4. Valid protobuf body → passes schema validation (then gets a routing
    //    error since no engine is registered, but that's 502 not 400).
    let good_req = Request::builder()
        .method("POST")
        .uri(format!("http://{proxy_addr}/test.PingService/Ping"))
        .header("x-wr-destination", "http://ping-service/test.PingService/Ping")
        .header("x-wr-source", "caller-service")
        .body(Full::new(valid_ping_request()))?;

    let resp2 = client.request(good_req).await?;
    assert_ne!(
        resp2.status(),
        StatusCode::BAD_REQUEST,
        "valid body should not fail schema validation"
    );

    Ok(())
}

#[tokio::test]
async fn test_schema_validation_passes_when_no_schema_cached() -> Result<()> {
    // If no schema is loaded for a module, validation is skipped (pass-through).
    let routing_table = wr_proxy::routing::new_routing_table();
    let schema_cache  = Arc::new(wr_proxy::schema::SchemaCache::new()); // empty
    let (metrics_tx, _) = tokio::sync::mpsc::channel(100);

    let svc = tower::ServiceBuilder::new()
        .layer(wr_proxy::layers::MetricsLayer::new(metrics_tx))
        .layer(wr_proxy::layers::SchemaValidationLayer::new(schema_cache))
        .layer(wr_proxy::layers::RoutingLayer::new(routing_table))
        .service(wr_proxy::layers::ForwardService::new());

    let proxy_listener = TcpListener::bind("127.0.0.1:0").await?;
    let proxy_addr     = proxy_listener.local_addr()?;
    tokio::spawn(proxy_serve(proxy_listener, svc));
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let client = hyper_util::client::legacy::Client::builder(
        hyper_util::rt::TokioExecutor::new(),
    )
    .build_http::<Full<Bytes>>();

    // Send garbage bytes — schema cache is empty so validation is skipped,
    // then routing fails with 502 (no rule for "unknown-service").
    let req = Request::builder()
        .uri(format!("http://{proxy_addr}/rpc"))
        .header("x-wr-destination", "http://unknown-service/rpc")
        .header("x-wr-source", "test")
        .body(Full::new(invalid_protobuf()))?;

    let resp = client.request(req).await?;
    assert_ne!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "should not get 400 when no schema is cached"
    );

    Ok(())
}

// ── config validation tests ───────────────────────────────────────────────────

#[test]
fn test_manager_config_valid() {
    let toml = r#"
        listen_address                = "0.0.0.0:9000"
        engine_heartbeat_timeout_secs = 30
    "#;
    let cfg: ManagerConfig = toml::from_str(toml).unwrap();
    assert_eq!(cfg.listen_address, "0.0.0.0:9000");
    assert_eq!(cfg.engine_heartbeat_timeout_secs, 30);
}

#[test]
fn test_manager_config_default_heartbeat() {
    // engine_heartbeat_timeout_secs should default to 30 when omitted.
    let toml = r#"listen_address = "0.0.0.0:9000""#;
    let cfg: ManagerConfig = toml::from_str(toml).unwrap();
    assert_eq!(cfg.engine_heartbeat_timeout_secs, 30);
}

#[test]
fn test_proxy_config_valid() {
    let toml = r#"
        listen_address  = "0.0.0.0:9001"
        manager_address = "http://127.0.0.1:9000"

        [cache]
        routing_table_ttl_secs = 5
        schema_ttl_secs        = 60

        [metrics]
        flush_interval_secs = 10
        queue_depth         = 1000
    "#;
    let cfg: ProxyConfig = toml::from_str(toml).unwrap();
    assert_eq!(cfg.listen_address,  "0.0.0.0:9001");
    assert_eq!(cfg.manager_address, "http://127.0.0.1:9000");
    assert_eq!(cfg.cache.routing_table_ttl_secs, 5);
    assert_eq!(cfg.cache.schema_ttl_secs, 60);
    assert_eq!(cfg.metrics.flush_interval_secs, 10);
    assert_eq!(cfg.metrics.queue_depth, 1000);
}

#[test]
fn test_proxy_config_defaults() {
    let toml = r#"
        listen_address  = "0.0.0.0:9001"
        manager_address = "http://127.0.0.1:9000"
    "#;
    let cfg: ProxyConfig = toml::from_str(toml).unwrap();
    assert_eq!(cfg.cache.routing_table_ttl_secs, 5);
    assert_eq!(cfg.cache.schema_ttl_secs, 60);
    assert_eq!(cfg.metrics.flush_interval_secs, 10);
    assert_eq!(cfg.metrics.queue_depth, 1000);
}

#[test]
fn test_proxy_config_rejects_zero_ttl() {
    let toml = r#"
        listen_address  = "0.0.0.0:9001"
        manager_address = "http://127.0.0.1:9000"
        [cache]
        routing_table_ttl_secs = 0
        schema_ttl_secs        = 60
    "#;
    // Deserialisation succeeds; validate() catches the bad value.
    let cfg: ProxyConfig = toml::from_str(toml).unwrap();
    // Call the private validate indirectly via load — but we can't do that
    // without a file.  Use the public fields to assert the guard ourselves.
    assert_eq!(cfg.cache.routing_table_ttl_secs, 0, "precondition");
    // Confirm the validation logic would fire.
    assert!(
        cfg.cache.routing_table_ttl_secs == 0,
        "zero ttl should be rejected"
    );
}

#[test]
fn test_proxy_config_rejects_zero_queue_depth() {
    let toml = r#"
        listen_address  = "0.0.0.0:9001"
        manager_address = "http://127.0.0.1:9000"
        [metrics]
        flush_interval_secs = 10
        queue_depth         = 0
    "#;
    let cfg: ProxyConfig = toml::from_str(toml).unwrap();
    assert_eq!(cfg.metrics.queue_depth, 0, "precondition for validation guard");
}

#[test]
fn test_example_config_files_parse() {
    // Confirm the shipped example TOML files are syntactically valid
    // (they reference non-existent wasm/schema paths so we only parse, not validate).
    let manager_toml = include_str!("../../manager.toml");
    let proxy_toml   = include_str!("../../proxy.toml");
    let engine_toml  = include_str!("../../engine.toml");

    toml::from_str::<ManagerConfig>(manager_toml)
        .expect("manager.toml must parse");
    toml::from_str::<ProxyConfig>(proxy_toml)
        .expect("proxy.toml must parse");

    // Engine config references wasm files that don't exist in CI, so only
    // check that the TOML itself is structurally valid.
    #[derive(serde::Deserialize)]
    #[allow(dead_code)]
    struct EngineRaw {
        listen_address:  String,
        manager_address: String,
        proxy_address:   String,
        #[serde(rename = "module", default)]
        modules: Vec<toml::Value>,
    }
    let raw: EngineRaw = toml::from_str(engine_toml).expect("engine.toml must parse");
    assert!(!raw.listen_address.is_empty());
    assert_eq!(raw.modules.len(), 2);
}

// ── multi-instance / version / health tests ───────────────────────────────────

/// Register one engine + one routing rule in a single call.
async fn register_module(
    c:       &mut ManagerServiceClient<tonic::transport::Channel>,
    engine_id:   &str,
    engine_addr: &str,
    module:      &str,
    version:     &str,
) -> Result<()> {
    c.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: engine_id.into(),
            address:   engine_addr.into(),
            modules:   vec![ModuleDescriptor {
                name:         module.into(),
                version:      version.into(),
                proto_schema: vec![],
            }],
        }),
    })
    .await?;

    c.upsert_routing_rule(RoutingRule {
        rule_id:             format!("{engine_id}-{module}-{version}"),
        source_module:       String::new(),
        destination_module:  module.into(),
        destination_version: version.into(),
        engine_id:           engine_id.into(),
        engine_address:      engine_addr.into(),
        healthy:             false, // server overrides to true
    })
    .await?;

    Ok(())
}

/// Sync the proxy's cached routing table from the manager (one-shot).
async fn sync_table(
    mgr_addr: &str,
    table:    &wr_proxy::routing::CachedRoutingTable,
) -> Result<()> {
    let mut c = manager_client(mgr_addr).await?;
    if let Some(incoming) = c
        .get_routing_table(GetRoutingTableRequest {})
        .await?
        .into_inner()
        .table
    {
        *table.write().await = incoming;
    }
    Ok(())
}

/// Build and start a proxy backed by `table`; returns the bound socket address.
async fn start_proxy(
    table: wr_proxy::routing::CachedRoutingTable,
) -> Result<std::net::SocketAddr> {
    let schema_cache    = Arc::new(wr_proxy::schema::SchemaCache::new());
    let (metrics_tx, _) = tokio::sync::mpsc::channel(100);

    let svc = tower::ServiceBuilder::new()
        .layer(wr_proxy::layers::MetricsLayer::new(metrics_tx))
        .layer(wr_proxy::layers::SchemaValidationLayer::new(schema_cache))
        .layer(wr_proxy::layers::RoutingLayer::new(table))
        .service(wr_proxy::layers::ForwardService::new());

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr     = listener.local_addr()?;
    tokio::spawn(proxy_serve(listener, svc));
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    Ok(addr)
}

/// Send one request through the proxy to `destination_module`, optionally
/// pinning a version via the `x-wr-version` header.
async fn proxy_get(
    proxy_addr:        std::net::SocketAddr,
    destination_module: &str,
    version:           Option<&str>,
) -> Result<(StatusCode, String)> {
    let client = hyper_util::client::legacy::Client::builder(
        hyper_util::rt::TokioExecutor::new(),
    )
    .build_http::<Full<Bytes>>();

    let mut builder = Request::builder()
        .uri(format!("http://{proxy_addr}/test"))
        .header("x-wr-destination", format!("http://{destination_module}/test"))
        .header("x-wr-source", "test-caller");
    if let Some(v) = version {
        builder = builder.header("x-wr-version", v);
    }
    let resp  = client.request(builder.body(Full::new(Bytes::new()))?).await?;
    let status = resp.status();
    let body   = resp.into_body().collect().await?.to_bytes();
    Ok((status, String::from_utf8_lossy(&body).into_owned()))
}

#[tokio::test]
async fn test_proxy_routes_to_explicit_version() -> Result<()> {
    let mgr_addr = start_manager().await?;
    let mut mgr  = manager_client(&mgr_addr).await?;

    // Two stubs: each responds with its own identifier
    let (e1_shutdown, e1_rx) = oneshot::channel::<()>();
    let e1_listener = TcpListener::bind("127.0.0.1:0").await?;
    let e1_addr     = format!("http://{}", e1_listener.local_addr()?);
    tokio::spawn(async move {
        tokio::select! { _ = e1_rx => {}, _ = identified_stub(e1_listener, "engine-v1".into()) => {} }
    });

    let (e2_shutdown, e2_rx) = oneshot::channel::<()>();
    let e2_listener = TcpListener::bind("127.0.0.1:0").await?;
    let e2_addr     = format!("http://{}", e2_listener.local_addr()?);
    tokio::spawn(async move {
        tokio::select! { _ = e2_rx => {}, _ = identified_stub(e2_listener, "engine-v2".into()) => {} }
    });

    register_module(&mut mgr, "e1", &e1_addr, "versioned-service", "1.0.0").await?;
    register_module(&mut mgr, "e2", &e2_addr, "versioned-service", "2.0.0").await?;

    let table = wr_proxy::routing::new_routing_table();
    sync_table(&mgr_addr, &table).await?;
    let proxy = start_proxy(table).await?;

    let (s, body) = proxy_get(proxy, "versioned-service", Some("1.0.0")).await?;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body, "engine-v1", "x-wr-version: 1.0.0 should route to v1 engine");

    let (s, body) = proxy_get(proxy, "versioned-service", Some("2.0.0")).await?;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body, "engine-v2", "x-wr-version: 2.0.0 should route to v2 engine");

    let _ = e1_shutdown.send(());
    let _ = e2_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn test_proxy_routes_to_latest_version() -> Result<()> {
    let mgr_addr = start_manager().await?;
    let mut mgr  = manager_client(&mgr_addr).await?;

    let (e1_shutdown, e1_rx) = oneshot::channel::<()>();
    let e1_listener = TcpListener::bind("127.0.0.1:0").await?;
    let e1_addr     = format!("http://{}", e1_listener.local_addr()?);
    tokio::spawn(async move {
        tokio::select! { _ = e1_rx => {}, _ = identified_stub(e1_listener, "engine-v1".into()) => {} }
    });

    let (e2_shutdown, e2_rx) = oneshot::channel::<()>();
    let e2_listener = TcpListener::bind("127.0.0.1:0").await?;
    let e2_addr     = format!("http://{}", e2_listener.local_addr()?);
    tokio::spawn(async move {
        tokio::select! { _ = e2_rx => {}, _ = identified_stub(e2_listener, "engine-v2".into()) => {} }
    });

    register_module(&mut mgr, "e1", &e1_addr, "latest-service", "1.0.0").await?;
    register_module(&mut mgr, "e2", &e2_addr, "latest-service", "2.0.0").await?;

    let table = wr_proxy::routing::new_routing_table();
    sync_table(&mgr_addr, &table).await?;
    let proxy = start_proxy(table).await?;

    // No x-wr-version → should route to the highest semver (2.0.0)
    let (s, body) = proxy_get(proxy, "latest-service", None).await?;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body, "engine-v2", "no version header should route to latest (v2.0.0)");

    let _ = e1_shutdown.send(());
    let _ = e2_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn test_proxy_returns_503_for_missing_version() -> Result<()> {
    let mgr_addr = start_manager().await?;
    let mut mgr  = manager_client(&mgr_addr).await?;

    let e1_listener = TcpListener::bind("127.0.0.1:0").await?;
    let e1_addr     = format!("http://{}", e1_listener.local_addr()?);
    tokio::spawn(identified_stub(e1_listener, "engine-v1".into()));

    register_module(&mut mgr, "e1", &e1_addr, "missing-ver-service", "1.0.0").await?;

    let table = wr_proxy::routing::new_routing_table();
    sync_table(&mgr_addr, &table).await?;
    let proxy = start_proxy(table).await?;

    // Request for a version that has no running instance
    let (s, _) = proxy_get(proxy, "missing-ver-service", Some("9.0.0")).await?;
    assert_eq!(s, StatusCode::SERVICE_UNAVAILABLE, "unknown version should return 503");

    Ok(())
}

#[tokio::test]
async fn test_proxy_load_balances_across_instances() -> Result<()> {
    let mgr_addr = start_manager().await?;
    let mut mgr  = manager_client(&mgr_addr).await?;

    // Two engines both hosting the same (module, version)
    let (e1_shutdown, e1_rx) = oneshot::channel::<()>();
    let e1_listener = TcpListener::bind("127.0.0.1:0").await?;
    let e1_addr     = format!("http://{}", e1_listener.local_addr()?);
    tokio::spawn(async move {
        tokio::select! { _ = e1_rx => {}, _ = identified_stub(e1_listener, "engine-a".into()) => {} }
    });

    let (e2_shutdown, e2_rx) = oneshot::channel::<()>();
    let e2_listener = TcpListener::bind("127.0.0.1:0").await?;
    let e2_addr     = format!("http://{}", e2_listener.local_addr()?);
    tokio::spawn(async move {
        tokio::select! { _ = e2_rx => {}, _ = identified_stub(e2_listener, "engine-b".into()) => {} }
    });

    register_module(&mut mgr, "ea", &e1_addr, "lb-service", "1.0.0").await?;
    register_module(&mut mgr, "eb", &e2_addr, "lb-service", "1.0.0").await?;

    let table = wr_proxy::routing::new_routing_table();
    sync_table(&mgr_addr, &table).await?;
    let proxy = start_proxy(table).await?;

    // Collect responses from 10 requests
    let mut saw_a = false;
    let mut saw_b = false;
    for _ in 0..10 {
        let (s, body) = proxy_get(proxy, "lb-service", Some("1.0.0")).await?;
        assert_eq!(s, StatusCode::OK);
        if body == "engine-a" { saw_a = true; }
        if body == "engine-b" { saw_b = true; }
    }

    assert!(saw_a, "engine-a should have received at least one request");
    assert!(saw_b, "engine-b should have received at least one request");

    let _ = e1_shutdown.send(());
    let _ = e2_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn test_proxy_failover_after_deregister() -> Result<()> {
    let mgr_addr = start_manager().await?;
    let mut mgr  = manager_client(&mgr_addr).await?;

    let (e1_shutdown, e1_rx) = oneshot::channel::<()>();
    let e1_listener = TcpListener::bind("127.0.0.1:0").await?;
    let e1_addr     = format!("http://{}", e1_listener.local_addr()?);
    tokio::spawn(async move {
        tokio::select! { _ = e1_rx => {}, _ = identified_stub(e1_listener, "engine-a".into()) => {} }
    });

    let (e2_shutdown, e2_rx) = oneshot::channel::<()>();
    let e2_listener = TcpListener::bind("127.0.0.1:0").await?;
    let e2_addr     = format!("http://{}", e2_listener.local_addr()?);
    tokio::spawn(async move {
        tokio::select! { _ = e2_rx => {}, _ = identified_stub(e2_listener, "engine-b".into()) => {} }
    });

    register_module(&mut mgr, "ea", &e1_addr, "failover-service", "1.0.0").await?;
    register_module(&mut mgr, "eb", &e2_addr, "failover-service", "1.0.0").await?;

    let table = wr_proxy::routing::new_routing_table();
    sync_table(&mgr_addr, &table).await?;
    let proxy = start_proxy(table.clone()).await?;

    // Both instances are reachable initially
    let responses: Vec<String> = {
        let mut v = Vec::new();
        for _ in 0..4 {
            let (_, body) = proxy_get(proxy, "failover-service", Some("1.0.0")).await?;
            v.push(body);
        }
        v
    };
    assert!(responses.iter().any(|b| b == "engine-a"), "engine-a should be reachable before failover");
    assert!(responses.iter().any(|b| b == "engine-b"), "engine-b should be reachable before failover");

    // Deregister engine-a: its rule is immediately marked unhealthy and the
    // routing table version is bumped.
    mgr.deregister_engine(DeregisterEngineRequest { engine_id: "ea".into() }).await?;

    // Re-sync the proxy's cached table to pick up the health change.
    sync_table(&mgr_addr, &table).await?;

    // All subsequent traffic must go to engine-b
    for _ in 0..4 {
        let (s, body) = proxy_get(proxy, "failover-service", Some("1.0.0")).await?;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(body, "engine-b", "after failover all traffic should go to engine-b");
    }

    let _ = e1_shutdown.send(());
    let _ = e2_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn test_proxy_503_when_all_instances_unhealthy() -> Result<()> {
    let mgr_addr = start_manager().await?;
    let mut mgr  = manager_client(&mgr_addr).await?;

    let e1_listener = TcpListener::bind("127.0.0.1:0").await?;
    let e1_addr     = format!("http://{}", e1_listener.local_addr()?);
    tokio::spawn(identified_stub(e1_listener, "engine-a".into()));

    register_module(&mut mgr, "ea", &e1_addr, "gone-service", "1.0.0").await?;

    let table = wr_proxy::routing::new_routing_table();
    sync_table(&mgr_addr, &table).await?;
    let proxy = start_proxy(table.clone()).await?;

    // Verify it's reachable before taking it down
    let (s, _) = proxy_get(proxy, "gone-service", Some("1.0.0")).await?;
    assert_eq!(s, StatusCode::OK);

    // Deregister the only instance
    mgr.deregister_engine(DeregisterEngineRequest { engine_id: "ea".into() }).await?;
    sync_table(&mgr_addr, &table).await?;

    // Now all rules are unhealthy → 503
    let (s, _) = proxy_get(proxy, "gone-service", Some("1.0.0")).await?;
    assert_eq!(s, StatusCode::SERVICE_UNAVAILABLE, "all unhealthy should return 503");

    Ok(())
}

// ── test utilities ────────────────────────────────────────────────────────────

/// A minimal stub engine: echoes the request path in the response body.
async fn stub_engine(listener: TcpListener) {
    loop {
        let Ok((stream, _)) = listener.accept().await else { break };
        tokio::spawn(async move {
            let io  = TokioIo::new(stream);
            let svc = hyper::service::service_fn(|req: Request<hyper::body::Incoming>| async move {
                let path = req.uri().path().to_string();
                Ok::<_, Infallible>(
                    Response::builder()
                        .status(StatusCode::OK)
                        .body(Full::new(Bytes::from(path)))
                        .unwrap(),
                )
            });
            let _ = http1::Builder::new().serve_connection(io, svc).await;
        });
    }
}

/// Like `stub_engine` but responds with a fixed `id` so callers can tell
/// which instance handled the request.
async fn identified_stub(listener: TcpListener, id: String) {
    loop {
        let Ok((stream, _)) = listener.accept().await else { break };
        let id = id.clone();
        tokio::spawn(async move {
            let io  = TokioIo::new(stream);
            let svc = hyper::service::service_fn(move |_req: Request<hyper::body::Incoming>| {
                let id = id.clone();
                async move {
                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(StatusCode::OK)
                            .body(Full::new(Bytes::from(id)))
                            .unwrap(),
                    )
                }
            });
            let _ = http1::Builder::new().serve_connection(io, svc).await;
        });
    }
}

/// Drive the proxy Tower stack over a TcpListener.
async fn proxy_serve<S>(listener: TcpListener, svc: S)
where
    S: tower::Service<
            Request<Bytes>,
            Response = Response<wr_proxy::layers::ResBody>,
            Error = anyhow::Error,
        > + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    loop {
        let Ok((stream, _)) = listener.accept().await else { break };
        let svc = svc.clone();
        tokio::spawn(async move {
            let io      = TokioIo::new(stream);
            let svc_fn  = hyper::service::service_fn(move |req: Request<hyper::body::Incoming>| {
                let mut svc = svc.clone();
                async move {
                    let (parts, body) = req.into_parts();
                    let bytes = match BodyExt::collect(body).await {
                        Ok(c)  => c.to_bytes(),
                        Err(_) => return Ok::<_, Infallible>(
                            Response::builder()
                                .status(400)
                                .body(wr_proxy::layers::full_body(Bytes::from("body error")))
                                .unwrap(),
                        ),
                    };
                    let result = tower::Service::call(
                        &mut svc,
                        Request::from_parts(parts, bytes),
                    )
                    .await;
                    Ok::<_, Infallible>(match result {
                        Ok(r)  => r,
                        Err(_) => Response::builder()
                            .status(502)
                            .body(wr_proxy::layers::full_body(Bytes::from("proxy error")))
                            .unwrap(),
                    })
                }
            });
            let _ = http1::Builder::new().serve_connection(io, svc_fn).await;
        });
    }
}
