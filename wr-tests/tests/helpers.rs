/// Shared helpers for Wruntime integration tests.
///
/// Infrastructure helpers (manager, proxy, stubs) start real in-process
/// services on ephemeral ports.  Fixture helpers (schema, DB) build the
/// minimal test data each suite needs.
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use prost::Message as _;
use prost_types::{
    field_descriptor_proto::{Label, Type},
    DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    MethodDescriptorProto, ServiceDescriptorProto,
};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tonic::transport::Server;

use wr_common::wruntime::{
    manager_service_client::ManagerServiceClient, manager_service_server::ManagerServiceServer,
    EngineRegistration, GetRoutingTableRequest, ModuleDescriptor, RegisterEngineRequest, RoutingRule,
};
use wr_manager::{service::Manager, state::new_state};

// Re-export DB types so tests using `helpers::*` need no local `use` statements.
pub use wr_engine::db::wruntime::db::database::{DbError, Host as DbHost, PgValue};
pub use wr_engine::state::ModuleState;

// ── Manager ───────────────────────────────────────────────────────────────────

/// Start an in-process wr-manager on a random port; returns its gRPC address.
pub async fn start_manager() -> Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(
        Server::builder()
            .add_service(ManagerServiceServer::new(Manager::new(new_state())))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    Ok(format!("http://{addr}"))
}

/// Return a connected manager gRPC client.
pub async fn manager_client(addr: &str) -> Result<ManagerServiceClient<tonic::transport::Channel>> {
    Ok(ManagerServiceClient::connect(addr.to_string()).await?)
}

// ── Proxy ─────────────────────────────────────────────────────────────────────

/// Register one engine and one routing rule in a single call.
pub async fn register_module(
    c: &mut ManagerServiceClient<tonic::transport::Channel>,
    engine_id: &str,
    engine_addr: &str,
    namespace: &str,
    module: &str,
    version: &str,
) -> Result<()> {
    c.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: engine_id.into(),
            address: engine_addr.into(),
            modules: vec![ModuleDescriptor {
                name: module.into(),
                namespace: namespace.into(),
                version: version.into(),
                proto_schema: vec![],
            }],
        }),
    })
    .await?;
    c.upsert_routing_rule(RoutingRule {
        rule_id: format!("{engine_id}-{namespace}-{module}-{version}"),
        source_module: String::new(),
        source_namespace: String::new(),
        destination_module: module.into(),
        destination_namespace: namespace.into(),
        destination_version: version.into(),
        engine_id: engine_id.into(),
        engine_address: engine_addr.into(),
        healthy: false, // manager overrides to true on upsert
    })
    .await?;
    Ok(())
}

/// Pull the routing table from the manager and write it into `table` (one-shot).
pub async fn sync_table(
    mgr_addr: &str,
    table: &wr_proxy::routing::CachedRoutingTable,
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

/// Build and start a proxy with an **empty** schema cache; returns the bound address.
pub async fn start_proxy(
    table: wr_proxy::routing::CachedRoutingTable,
) -> Result<SocketAddr> {
    start_proxy_with_schema(table, Arc::new(wr_proxy::schema::SchemaCache::new())).await
}

/// Build and start a proxy with a **pre-populated** schema cache; returns the bound address.
pub async fn start_proxy_with_schema(
    table: wr_proxy::routing::CachedRoutingTable,
    schema_cache: Arc<wr_proxy::schema::SchemaCache>,
) -> Result<SocketAddr> {
    let (metrics_tx, _) = tokio::sync::mpsc::channel(100);
    let svc = tower::ServiceBuilder::new()
        .layer(wr_proxy::layers::MetricsLayer::new(metrics_tx))
        .layer(wr_proxy::layers::SchemaValidationLayer::new(schema_cache))
        .layer(wr_proxy::layers::RoutingLayer::new(table))
        .service(wr_proxy::layers::ForwardService::new());
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(proxy_serve(listener, svc));
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    Ok(addr)
}

/// Send a GET request through the proxy to `destination_module` in `namespace`,
/// optionally pinning a version via `x-wr-version`.  Returns `(status, body_string)`.
pub async fn proxy_get(
    proxy_addr: SocketAddr,
    namespace: &str,
    destination_module: &str,
    version: Option<&str>,
) -> Result<(StatusCode, String)> {
    let mut builder = Request::builder()
        .uri(format!("http://{proxy_addr}/test"))
        .header(
            "x-wr-destination",
            format!("http://{destination_module}.{namespace}/test"),
        )
        .header("x-wr-source", "test-caller");
    if let Some(v) = version {
        builder = builder.header("x-wr-version", v);
    }
    let resp = http_client()
        .request(builder.body(Full::new(Bytes::new()))?)
        .await?;
    let status = resp.status();
    let body = resp.into_body().collect().await?.to_bytes();
    Ok((status, String::from_utf8_lossy(&body).into_owned()))
}

/// Drive the proxy Tower stack over a TCP listener (accepts until closed).
pub async fn proxy_serve<S>(listener: TcpListener, svc: S)
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
        let Ok((stream, _)) = listener.accept().await else {
            break;
        };
        let svc = svc.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc_fn =
                hyper::service::service_fn(move |req: Request<hyper::body::Incoming>| {
                    let mut svc = svc.clone();
                    async move {
                        let (parts, body) = req.into_parts();
                        let bytes = match BodyExt::collect(body).await {
                            Ok(c) => c.to_bytes(),
                            Err(_) => {
                                return Ok::<_, Infallible>(
                                    Response::builder()
                                        .status(400)
                                        .body(wr_proxy::layers::full_body(Bytes::from(
                                            "body error",
                                        )))
                                        .unwrap(),
                                )
                            }
                        };
                        let result = tower::Service::call(
                            &mut svc,
                            Request::from_parts(parts, bytes),
                        )
                        .await;
                        Ok::<_, Infallible>(match result {
                            Ok(r) => r,
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

/// Build a legacy HTTP/1.1 client for sending test requests through the proxy.
pub fn http_client() -> hyper_util::client::legacy::Client<
    hyper_util::client::legacy::connect::HttpConnector,
    Full<Bytes>,
> {
    hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
        .build_http::<Full<Bytes>>()
}

// ── Stub engines ──────────────────────────────────────────────────────────────

/// A minimal stub engine: echoes the request path in the response body.
pub async fn stub_engine(listener: TcpListener) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            break;
        };
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc =
                hyper::service::service_fn(|req: Request<hyper::body::Incoming>| async move {
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

/// Like `stub_engine` but responds with a fixed `id` string so callers can
/// tell which instance handled a request.
pub async fn identified_stub(listener: TcpListener, id: String) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            break;
        };
        let id = id.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc = hyper::service::service_fn(
                move |_req: Request<hyper::body::Incoming>| {
                    let id = id.clone();
                    async move {
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .body(Full::new(Bytes::from(id)))
                                .unwrap(),
                        )
                    }
                },
            );
            let _ = http1::Builder::new().serve_connection(io, svc).await;
        });
    }
}

/// Spawn a `stub_engine` task; returns the engine's HTTP address and a
/// shutdown sender.  Send or drop the sender to stop the stub.
pub async fn spawn_stub_engine() -> Result<(String, oneshot::Sender<()>)> {
    let (tx, rx) = oneshot::channel::<()>();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = format!("http://{}", listener.local_addr()?);
    tokio::spawn(async move {
        tokio::select! {
            _ = rx => {}
            _ = stub_engine(listener) => {}
        }
    });
    Ok((addr, tx))
}

/// Spawn an `identified_stub` task; returns the engine's HTTP address and a
/// shutdown sender.  Send or drop the sender to stop the stub.
pub async fn spawn_identified_stub(id: &str) -> Result<(String, oneshot::Sender<()>)> {
    let (tx, rx) = oneshot::channel::<()>();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = format!("http://{}", listener.local_addr()?);
    let id = id.to_string();
    tokio::spawn(async move {
        tokio::select! {
            _ = rx => {}
            _ = identified_stub(listener, id) => {}
        }
    });
    Ok((addr, tx))
}

// ── Schema fixtures ───────────────────────────────────────────────────────────

/// Build a minimal `FileDescriptorSet` binary containing a `PingService` with
/// one RPC so schema validation can be exercised without running `protoc`.
pub fn minimal_file_descriptor_set() -> Vec<u8> {
    let req_msg = DescriptorProto {
        name: Some("PingRequest".into()),
        field: vec![FieldDescriptorProto {
            name: Some("message".into()),
            number: Some(1),
            label: Some(Label::Optional as i32),
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
            name: Some("Ping".into()),
            input_type: Some(".test.PingRequest".into()),
            output_type: Some(".test.PingResponse".into()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let file = FileDescriptorProto {
        name: Some("test.proto".into()),
        package: Some("test".into()),
        message_type: vec![req_msg, resp_msg],
        service: vec![service],
        syntax: Some("proto3".into()),
        ..Default::default()
    };
    FileDescriptorSet { file: vec![file] }.encode_to_vec()
}

/// A valid protobuf encoding of `PingRequest { message: "hello" }`.
/// Field 1, wire type 2 (length-delimited), value = "hello".
pub fn valid_ping_request() -> Bytes {
    // tag = (1 << 3) | 2 = 0x0a, varint length 5, then "hello"
    Bytes::from_static(b"\x0a\x05hello")
}

/// Bytes that are not valid protobuf (truncated varint).
pub fn invalid_protobuf() -> Bytes {
    Bytes::from_static(&[0xFF])
}

// ── Database ──────────────────────────────────────────────────────────────────

/// Build a `ModuleState` backed by a connection pool at `WRUNTIME_TEST_DB_URL`.
///
/// Returns `None` when the environment variable is absent so the calling test
/// can return early and skip all DB-dependent assertions — making the test
/// suite pass in CI without a running Postgres instance.
///
/// Use `pool_size = 1` when the test creates `TEMP TABLE`s, which are
/// connection-local: a pool of one guarantees every `pool.get()` call reuses
/// the same underlying connection.
pub fn db_state(pool_size: usize) -> Option<ModuleState> {
    let url = std::env::var("WRUNTIME_TEST_DB_URL").ok()?;
    let pool = Arc::new(wr_engine::pool::build_pool(&url, pool_size).expect("build_pool"));
    Some(ModuleState::new(
        "test".into(),
        "test-ns".into(),
        "http://127.0.0.1:9001".parse().unwrap(),
        Some(pool),
    ))
}
