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
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Full};
use hyper::server::conn::http2;
use hyper_util::rt::{TokioExecutor, TokioIo};
use prost::Message as _;
use prost_types::{
    field_descriptor_proto::{Label, Type},
    DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    MethodDescriptorProto, ServiceDescriptorProto,
};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tonic::transport::Server;
use wasmtime::component::{Component, Linker};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi_http::p2::{
    bindings::http::types::{ErrorCode, Scheme},
    bindings::ProxyPre,
    body::{HyperIncomingBody, HyperOutgoingBody},
    WasiHttpView as _,
};

use wr_common::wruntime::{
    manager_service_client::ManagerServiceClient, manager_service_server::ManagerServiceServer,
    EngineRegistration, GetRoutingTableRequest, ModuleDescriptor, RegisterEngineRequest,
    RoutingRule,
};
use wr_engine::blobstore::BlobstoreRuntime;
use wr_engine::config::BlobstoreConfig;
use wr_manager::{service::Manager, state::new_state};

// Re-export DB types so tests using `helpers::*` need no local `use` statements.
pub use wr_engine::db::wruntime::db::database::{DbError, Host as DbHost, PgValue};
pub use wr_engine::state::{ModuleServices, ModuleState};
pub use wr_proxy::config::{EgressConfig, ExternalRoute};

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

/// Identifies an engine instance and the node it belongs to.
pub struct EngineSpec<'a> {
    pub id: &'a str,
    pub addr: &'a str,
    /// Node proxy address for local-vs-remote dispatch.  Use `""` in
    /// single-node tests where cross-node routing is not exercised.
    pub proxy_address: &'a str,
}

/// Identifies a single WASM module hosted by an engine.
pub struct ModuleSpec<'a> {
    pub namespace: &'a str,
    pub name: &'a str,
    pub version: &'a str,
    /// Serialised `FileDescriptorSet`.  Use [`minimal_file_descriptor_set`]
    /// when a real schema is not needed.
    pub schema: Vec<u8>,
}

/// Register one engine and one routing rule in a single call.
pub async fn register_module(
    c: &mut ManagerServiceClient<tonic::transport::Channel>,
    engine: EngineSpec<'_>,
    module: ModuleSpec<'_>,
) -> Result<()> {
    c.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: engine.id.into(),
            address: engine.addr.into(),
            proxy_address: engine.proxy_address.into(),
            modules: vec![ModuleDescriptor {
                name: module.name.into(),
                namespace: module.namespace.into(),
                version: module.version.into(),
                proto_schema: module.schema,
            }],
        }),
    })
    .await?;
    c.upsert_routing_rule(RoutingRule {
        rule_id: format!(
            "{}-{}-{}-{}",
            engine.id, module.namespace, module.name, module.version
        ),
        source_module: String::new(),
        source_namespace: String::new(),
        destination_module: module.name.into(),
        destination_namespace: module.namespace.into(),
        destination_version: module.version.into(),
        engine_id: engine.id.into(),
        engine_address: engine.addr.into(),
        proxy_address: engine.proxy_address.into(),
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

/// Build and start a proxy; returns the bound address.
pub async fn start_proxy(table: wr_proxy::routing::CachedRoutingTable) -> Result<SocketAddr> {
    start_proxy_on(table, "").await
}

/// Build and start a proxy with a custom `self_proxy_address`; returns the bound address.
pub async fn start_proxy_on(
    table: wr_proxy::routing::CachedRoutingTable,
    self_proxy_address: &str,
) -> Result<SocketAddr> {
    let svc = tower::ServiceBuilder::new()
        .layer(wr_proxy::layers::RoutingLayer::new(
            table,
            self_proxy_address,
        ))
        .service(wr_proxy::layers::ForwardService::new(std::sync::Arc::new(
            wr_proxy::circuit_breaker::CircuitBreakerRegistry::new(Default::default()),
        )));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(proxy_serve(listener, svc));
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    Ok(addr)
}

/// Build and start an external-facing ingress proxy on an ephemeral port.
pub async fn start_ingress_proxy(
    table: wr_proxy::routing::CachedRoutingTable,
    routes: Vec<ExternalRoute>,
) -> Result<SocketAddr> {
    let svc = tower::ServiceBuilder::new()
        .layer(wr_proxy::layers::IngressLayer::new(routes))
        .layer(wr_proxy::layers::RoutingLayer::new(table, ""))
        .service(wr_proxy::layers::ForwardService::new(std::sync::Arc::new(
            wr_proxy::circuit_breaker::CircuitBreakerRegistry::new(Default::default()),
        )));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(proxy_serve(listener, svc));
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    Ok(addr)
}

/// Build and start a proxy with [`EgressLayer`] configured; returns the bound address.
pub async fn start_egress_proxy(
    egress_cfg: Option<EgressConfig>,
    table: wr_proxy::routing::CachedRoutingTable,
) -> Result<SocketAddr> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let egress_enabled = egress_cfg.is_some();
    let svc = tower::ServiceBuilder::new()
        .layer(wr_proxy::layers::RoutingLayer::new(table, "").with_egress(egress_enabled))
        .layer(wr_proxy::layers::EgressLayer::new(egress_cfg))
        .service(wr_proxy::layers::ForwardService::new(Arc::new(
            wr_proxy::circuit_breaker::CircuitBreakerRegistry::new(Default::default()),
        )));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(proxy_serve(listener, svc));
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    Ok(addr)
}

/// Spawn a minimal HTTP/1.1 stub server.
///
/// The stub responds 200 OK with a body of `"egress:<path>"` so the caller can
/// verify that the request arrived and the correct path was preserved.
/// Returns the base URL (`http://127.0.0.1:<port>`) and a shutdown sender.
pub async fn spawn_http1_stub() -> Result<(String, oneshot::Sender<()>)> {
    let (tx, rx) = oneshot::channel::<()>();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = format!("http://{}", listener.local_addr()?);
    tokio::spawn(async move {
        tokio::select! {
            _ = rx => {}
            _ = http1_stub(listener) => {}
        }
    });
    Ok((addr, tx))
}

async fn http1_stub(listener: TcpListener) {
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
                            .body(Full::new(Bytes::from(format!("egress:{path}"))))
                            .unwrap(),
                    )
                });
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, svc)
                .await;
        });
    }
}

/// A running proxy node.
pub struct Node {
    /// TCP address this node listens on — pass to [`proxy_get`] and friends.
    pub addr: std::net::SocketAddr,
    /// `"http://127.0.0.1:{port}"` — store in routing rules for engines on this node.
    pub proxy_address: String,
    /// Shared routing table — call [`sync_table`] on it after registering new engines.
    pub table: wr_proxy::routing::CachedRoutingTable,
    /// Drop or send to shut the proxy down.
    pub proxy_shutdown: oneshot::Sender<()>,
}

/// Spin up a proxy node on an ephemeral port.
pub async fn start_node(mgr_addr: &str) -> Result<Node> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let proxy_address = format!("http://{addr}");

    let table = wr_proxy::routing::new_routing_table();
    sync_table(mgr_addr, &table).await?;

    let svc = tower::ServiceBuilder::new()
        .layer(wr_proxy::layers::RoutingLayer::new(
            table.clone(),
            &proxy_address,
        ))
        .service(wr_proxy::layers::ForwardService::new(std::sync::Arc::new(
            wr_proxy::circuit_breaker::CircuitBreakerRegistry::new(Default::default()),
        )));

    let (tx, rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        tokio::select! {
            _ = rx => {}
            _ = proxy_serve(listener, svc) => {}
        }
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    Ok(Node {
        addr,
        proxy_address,
        table,
        proxy_shutdown: tx,
    })
}

/// Send a GET request through the proxy to `destination_module` in `namespace`,
/// optionally pinning a version via `x-wr-version`.  Returns `(status, body_string)`.
pub async fn proxy_get(
    proxy_addr: SocketAddr,
    namespace: &str,
    destination_module: &str,
    version: Option<&str>,
) -> Result<(StatusCode, String)> {
    // Path uses {namespace}.{module}/Method format, consistent with the HTTP hostname.
    let path = format!("/{namespace}.{destination_module}/Ping");
    let mut builder = Request::builder()
        .uri(format!("http://{proxy_addr}{path}"))
        .header(
            "x-wr-destination",
            format!("http://{namespace}.{destination_module}{path}"),
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
            Request<wr_proxy::layers::ProxyBody>,
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
            let svc_fn = hyper::service::service_fn(move |req: Request<hyper::body::Incoming>| {
                let mut svc = svc.clone();
                async move {
                    let req = req.map(wr_proxy::layers::ProxyBody::streaming);
                    let result = tower::Service::call(&mut svc, req).await;
                    Ok::<_, Infallible>(match result {
                        Ok(r) => r,
                        Err(_) => Response::builder()
                            .status(502)
                            .body(wr_proxy::layers::full_body(Bytes::from("proxy error")))
                            .unwrap(),
                    })
                }
            });
            let _ = http2::Builder::new(TokioExecutor::new())
                .serve_connection(io, svc_fn)
                .await;
        });
    }
}

/// Build an HTTP/2 client for sending test requests through the proxy.
pub fn http_client() -> hyper_util::client::legacy::Client<
    hyper_util::client::legacy::connect::HttpConnector,
    Full<Bytes>,
> {
    hyper_util::client::legacy::Client::builder(TokioExecutor::new())
        .http2_only(true)
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
            let _ = http2::Builder::new(TokioExecutor::new())
                .serve_connection(io, svc)
                .await;
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
            let _ = http2::Builder::new(TokioExecutor::new())
                .serve_connection(io, svc)
                .await;
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

// ── Configurable stub engines ────────────────────────────────────────────────

/// A stub engine that always responds with a fixed HTTP status code.
pub async fn status_stub(listener: TcpListener, status_code: StatusCode) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            break;
        };
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc = hyper::service::service_fn(move |_req: Request<hyper::body::Incoming>| {
                let status = status_code;
                async move {
                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(status)
                            .body(Full::new(Bytes::from(format!("{}", status.as_u16()))))
                            .unwrap(),
                    )
                }
            });
            let _ = http2::Builder::new(TokioExecutor::new())
                .serve_connection(io, svc)
                .await;
        });
    }
}

/// Spawn a stub engine that always responds with `status_code`.
pub async fn spawn_status_stub(status_code: StatusCode) -> Result<(String, oneshot::Sender<()>)> {
    let (tx, rx) = oneshot::channel::<()>();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = format!("http://{}", listener.local_addr()?);
    tokio::spawn(async move {
        tokio::select! {
            _ = rx => {}
            _ = status_stub(listener, status_code) => {}
        }
    });
    Ok((addr, tx))
}

/// A stub engine whose response status can be switched at runtime via an
/// `Arc<std::sync::atomic::AtomicU16>`.
pub async fn switchable_stub(listener: TcpListener, status: Arc<std::sync::atomic::AtomicU16>) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            break;
        };
        let status = status.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc = hyper::service::service_fn(move |_req: Request<hyper::body::Incoming>| {
                let code = status.load(std::sync::atomic::Ordering::Relaxed);
                async move {
                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(code)
                            .body(Full::new(Bytes::from(format!("{code}"))))
                            .unwrap(),
                    )
                }
            });
            let _ = http2::Builder::new(TokioExecutor::new())
                .serve_connection(io, svc)
                .await;
        });
    }
}

/// Spawn a stub engine whose status can be switched at runtime.
/// Returns the engine address, a shutdown sender, and the status control.
pub async fn spawn_switchable_stub(
    initial_status: u16,
) -> Result<(
    String,
    oneshot::Sender<()>,
    Arc<std::sync::atomic::AtomicU16>,
)> {
    let (tx, rx) = oneshot::channel::<()>();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = format!("http://{}", listener.local_addr()?);
    let status = Arc::new(std::sync::atomic::AtomicU16::new(initial_status));
    let s = status.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = rx => {}
            _ = switchable_stub(listener, s) => {}
        }
    });
    Ok((addr, tx, status))
}

// ── Proxy with custom circuit breaker ────────────────────────────────────────

/// Build and start a proxy with a custom [`CircuitBreakerConfig`]; returns the bound address.
pub async fn start_proxy_with_cb(
    table: wr_proxy::routing::CachedRoutingTable,
    cb_config: wr_proxy::config::CircuitBreakerConfig,
) -> Result<SocketAddr> {
    let svc = tower::ServiceBuilder::new()
        .layer(wr_proxy::layers::RoutingLayer::new(table, ""))
        .service(wr_proxy::layers::ForwardService::new(Arc::new(
            wr_proxy::circuit_breaker::CircuitBreakerRegistry::new(cb_config),
        )));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(proxy_serve(listener, svc));
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    Ok(addr)
}

// ── Manager with heartbeat monitor ──────────────────────────────────────────

/// Start an in-process wr-manager that also runs the heartbeat monitor background
/// task.  `timeout_secs` controls how long before a module is marked unhealthy.
/// Returns the gRPC address and the shared state handle for assertions.
pub async fn start_manager_with_monitor(
    timeout_secs: u64,
) -> Result<(String, wr_manager::state::SharedState)> {
    let state = new_state();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(
        Server::builder()
            .add_service(ManagerServiceServer::new(Manager::new(state.clone())))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    tokio::spawn(wr_manager::state::monitor_heartbeats(
        state.clone(),
        timeout_secs,
    ));
    Ok((format!("http://{addr}"), state))
}

// ── Schema fixtures ───────────────────────────────────────────────────────────

/// Build a minimal `FileDescriptorSet` binary containing a `PingService` with
/// one RPC so engine registration can include a schema.
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
pub fn db_state(pool_size: usize) -> Option<ModuleState> {
    let url = std::env::var("WRUNTIME_TEST_DB_URL").ok()?;
    let pool = Arc::new(wr_engine::pool::build_pool(&url, pool_size).expect("build_pool"));
    Some(
        ModuleState::new(
            "test".into(),
            "test-ns".into(),
            "http://127.0.0.1:9001".parse().unwrap(),
            http_client(),
            ModuleServices {
                db_pool: Some(pool),
                ..Default::default()
            },
        )
        .expect("ModuleState"),
    )
}

/// Build a `ModuleState` for a specific `(namespace, name)` pair, provisioning
/// the module's Postgres schema (`wr__{namespace}__{name}`) if it does not
/// already exist.
pub async fn db_state_for_module(
    pool_size: usize,
    namespace: &str,
    name: &str,
) -> Option<ModuleState> {
    let url = std::env::var("WRUNTIME_TEST_DB_URL").ok()?;
    let schema = wr_engine::pool::module_schema(namespace, name);
    let pool = Arc::new(wr_engine::pool::build_pool(&url, pool_size).expect("build_pool"));
    let client = pool
        .get()
        .await
        .expect("get connection for schema provisioning");
    client
        .simple_query(&format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\""))
        .await
        .expect("provision schema");
    drop(client);
    Some(
        ModuleState::new(
            name.into(),
            namespace.into(),
            "http://127.0.0.1:9001".parse().unwrap(),
            http_client(),
            ModuleServices {
                db_pool: Some(pool),
                db_schema: Some(schema),
                ..Default::default()
            },
        )
        .expect("ModuleState"),
    )
}

// ── WASM guest dispatch ──────────────────────────────────────────────────────

/// Set up a wasmtime `Engine` + `ProxyPre` from a compiled WASM component path.
pub fn wasm_module_pre(wasm_path: &str) -> Result<(Arc<Engine>, Arc<ProxyPre<ModuleState>>)> {
    let mut wt_config = Config::new();
    wt_config.wasm_component_model(true);
    let engine = Engine::new(&wt_config)?;
    let component = Component::from_file(&engine, wasm_path)?;

    let mut linker: Linker<ModuleState> = Linker::new(&engine);
    wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
    wasmtime_wasi_http::p2::add_only_http_to_linker_async(&mut linker)?;
    wr_engine::db::wruntime::db::database::add_to_linker::<
        ModuleState,
        wasmtime::component::HasSelf<ModuleState>,
    >(&mut linker, |s| s)?;
    wr_engine::tracing::add_to_linker::<ModuleState, wasmtime::component::HasSelf<ModuleState>>(
        &mut linker,
        |s| s,
    )?;
    wr_engine::blobstore::add_to_linker::<ModuleState, wasmtime::component::HasSelf<ModuleState>>(
        &mut linker,
        |s| s,
    )?;

    let pre = ProxyPre::new(linker.instantiate_pre(&component)?)?;
    Ok((Arc::new(engine), Arc::new(pre)))
}

/// Dispatch a single HTTP request through a WASM component, returning the response.
pub async fn dispatch_to_wasm(
    engine: &Engine,
    pre: &ProxyPre<ModuleState>,
    state: ModuleState,
    request: http::Request<Bytes>,
) -> Result<http::Response<Bytes>> {
    let mut store = Store::new(engine, state);
    let proxy = pre.instantiate_async(&mut store).await?;

    let (req_parts, req_body) = request.into_parts();
    let hyper_body: HyperIncomingBody = UnsyncBoxBody::new(
        Full::new(req_body).map_err(|_: Infallible| ErrorCode::InternalError(None)),
    );
    let hyper_req = hyper::Request::from_parts(req_parts, hyper_body);
    let req_resource = store
        .data_mut()
        .http()
        .new_incoming_request(Scheme::Http, hyper_req)?;

    let (resp_tx, resp_rx) =
        tokio::sync::oneshot::channel::<Result<hyper::Response<HyperOutgoingBody>, ErrorCode>>();
    let out_resource = store.data_mut().http().new_response_outparam(resp_tx)?;

    proxy
        .wasi_http_incoming_handler()
        .call_handle(&mut store, req_resource, out_resource)
        .await?;

    match resp_rx.await {
        Ok(Ok(wasm_resp)) => {
            let (rp, rb) = wasm_resp.into_parts();
            let bytes = rb
                .collect()
                .await
                .map_err(|e| anyhow::anyhow!("collecting WASM response body: {e:?}"))?
                .to_bytes();
            Ok(http::Response::from_parts(rp, bytes))
        }
        Ok(Err(e)) => anyhow::bail!("WASM handler returned ErrorCode: {e:?}"),
        Err(_) => anyhow::bail!("WASM handler dropped the response outparam"),
    }
}

/// Build a `BlobstoreRuntime` from `WRUNTIME_TEST_S3_*` environment variables.
pub fn blobstore_client() -> Option<Arc<BlobstoreRuntime>> {
    let endpoint = std::env::var("WRUNTIME_TEST_S3_ENDPOINT").ok()?;
    let access_key = std::env::var("WRUNTIME_TEST_S3_ACCESS_KEY").ok()?;
    let secret_key = std::env::var("WRUNTIME_TEST_S3_SECRET_KEY").ok()?;
    let config = BlobstoreConfig {
        endpoint,
        access_key_id: access_key,
        secret_access_key: secret_key,
        region: "us-east-1".into(),
    };
    Some(Arc::new(
        BlobstoreRuntime::new(&config).expect("BlobstoreRuntime"),
    ))
}

/// Build a `ModuleState` with a blobstore client for WASM guest tests.
pub fn blobstore_state(blobstore: Arc<BlobstoreRuntime>) -> ModuleState {
    ModuleState::new(
        "blobstore-test".into(),
        "test-ns".into(),
        "http://127.0.0.1:9001".parse().unwrap(),
        http_client(),
        ModuleServices {
            blobstore: Some(blobstore),
            ..Default::default()
        },
    )
    .expect("ModuleState")
}

/// Build a `ModuleState` with no services (tracing tests only need WASI + HTTP).
pub fn tracing_state() -> ModuleState {
    ModuleState::new(
        "tracing-test".into(),
        "test-ns".into(),
        "http://127.0.0.1:9001".parse().unwrap(),
        http_client(),
        ModuleServices::default(),
    )
    .expect("ModuleState")
}
