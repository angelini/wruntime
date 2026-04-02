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

/// Return the test DB URL, panicking if `WRT_TEST_DB_URL` is not set.
pub fn require_db_url() -> String {
    std::env::var("WRT_TEST_DB_URL").expect("WRT_TEST_DB_URL must be set for this test")
}

/// Build a `deadpool_postgres::Pool` for the manager in an isolated Postgres
/// schema so parallel tests don't collide.  Runs migrations inside the schema,
/// returns a pool whose connections have `search_path` permanently set.
///
/// The first call in a test run drops all leftover `mgr_test_*` schemas from
/// previous runs (including failed ones), so schemas don't accumulate.
pub async fn manager_pool() -> deadpool_postgres::Pool {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    static CLEANED: AtomicBool = AtomicBool::new(false);
    static CLEANUP_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    let base_url = require_db_url();
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let schema = format!("mgr_test_{n}");

    // Create the schema using a one-shot connection to the base DB.
    let setup_pool =
        wr_manager::pool::build_pool(&base_url, 1).expect("failed to build setup pool");
    let client = setup_pool.get().await.expect("setup connection");

    // On the first call only, drop all leftover mgr_test_* schemas from
    // previous (possibly failed) test runs.
    if !CLEANED.load(Ordering::SeqCst) {
        let _guard = CLEANUP_LOCK.lock().await;
        if !CLEANED.load(Ordering::SeqCst) {
            let rows = client
                .query(
                    "SELECT schema_name FROM information_schema.schemata
                     WHERE schema_name LIKE 'mgr_test_%'",
                    &[],
                )
                .await
                .expect("list mgr_test schemas");
            for row in &rows {
                let name: &str = row.get(0);
                client
                    .batch_execute(&format!("DROP SCHEMA \"{name}\" CASCADE"))
                    .await
                    .expect("drop leftover schema");
            }
            CLEANED.store(true, Ordering::SeqCst);
        }
    }

    client
        .batch_execute(&format!("CREATE SCHEMA \"{schema}\""))
        .await
        .expect("create schema");
    drop(client);
    drop(setup_pool);

    // Build the real pool with search_path pinned to the new schema.
    let sep = if base_url.contains('?') { "&" } else { "?" };
    let url = format!("{base_url}{sep}options=-csearch_path%3D{schema}");
    let pool = wr_manager::pool::build_pool(&url, 5).expect("failed to build manager test pool");

    let client = pool.get().await.expect("migration connection");
    wr_manager::migrate::run_migrations(&client)
        .await
        .expect("manager migrations failed");
    drop(client);

    pool
}

/// Start an in-process wr-manager on a random port; returns its gRPC address.
pub async fn start_manager(pool: deadpool_postgres::Pool) -> Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    // Use a fixed test key (32 bytes = 64 hex chars)
    let crypto = std::sync::Arc::new(
        wr_manager::crypto::SecretCrypto::from_hex(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        )
        .expect("test encryption key"),
    );
    tokio::spawn(
        Server::builder()
            .add_service(ManagerServiceServer::new(Manager::new(
                new_state(),
                pool,
                crypto,
            )))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    Ok(format!("http://{addr}"))
}

/// Return a connected manager gRPC client.
pub async fn manager_client(addr: &str) -> Result<ManagerServiceClient<tonic::transport::Channel>> {
    Ok(ManagerServiceClient::connect(addr.to_string()).await?)
}

/// Query the routing table via gRPC and find a rule by destination module name.
/// Returns `(healthy, version)`.
pub async fn get_rule_health(
    mgr: &mut ManagerServiceClient<tonic::transport::Channel>,
    destination_module: &str,
) -> Result<(bool, u64)> {
    let table = mgr
        .get_routing_table(GetRoutingTableRequest {})
        .await?
        .into_inner()
        .table
        .expect("routing table present");
    let rule = table
        .rules
        .iter()
        .find(|r| r.destination_module == destination_module)
        .unwrap_or_else(|| panic!("no rule for destination_module={destination_module}"));
    Ok((rule.healthy, table.version))
}

/// Query the routing table version via gRPC.
pub async fn get_routing_table_version(
    mgr: &mut ManagerServiceClient<tonic::transport::Channel>,
) -> Result<u64> {
    let table = mgr
        .get_routing_table(GetRoutingTableRequest {})
        .await?
        .into_inner()
        .table
        .expect("routing table present");
    Ok(table.version)
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
            secrets: vec![],
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
    pool: deadpool_postgres::Pool,
    timeout_secs: u64,
) -> Result<(String, wr_manager::state::SharedState)> {
    let state = new_state();
    let crypto = std::sync::Arc::new(
        wr_manager::crypto::SecretCrypto::from_hex(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        )
        .expect("test encryption key"),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(
        Server::builder()
            .add_service(ManagerServiceServer::new(Manager::new(
                state.clone(),
                pool.clone(),
                crypto,
            )))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    tokio::spawn(wr_manager::state::monitor_heartbeats(
        state.clone(),
        pool,
        timeout_secs,
        std::time::Duration::from_millis(200),
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

/// Build a `ModuleState` backed by a connection pool at `WRT_TEST_DB_URL`.
/// Panics if the env var is not set.
pub fn db_state(pool_size: usize) -> ModuleState {
    let url = require_db_url();
    let pool = Arc::new(wr_engine::pool::build_pool(&url, pool_size).expect("build_pool"));
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
    .expect("ModuleState")
}

/// Build a `ModuleState` for a specific `(namespace, name)` pair, provisioning
/// the module's Postgres schema (`wr__{namespace}__{name}`) if it does not
/// already exist. Panics if `WRT_TEST_DB_URL` is not set.
pub async fn db_state_for_module(pool_size: usize, namespace: &str, name: &str) -> ModuleState {
    let url = require_db_url();
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
    .expect("ModuleState")
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
    wr_engine::llm::add_to_linker::<ModuleState, wasmtime::component::HasSelf<ModuleState>>(
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

/// Spawn a WASM-backed HTTP/2 engine on an ephemeral port.
///
/// Each incoming request is dispatched through a fresh `ModuleState` + `Store`
/// using the provided pre-compiled WASM component.  Returns the engine base URL
/// and a shutdown sender.
pub async fn spawn_wasm_stub_engine(
    engine: Arc<Engine>,
    pre: Arc<ProxyPre<ModuleState>>,
    proxy_uri: &str,
    module_name: &str,
    module_namespace: &str,
) -> Result<(String, oneshot::Sender<()>)> {
    let (tx, rx) = oneshot::channel::<()>();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = format!("http://{}", listener.local_addr()?);
    let proxy_uri: hyper::Uri = proxy_uri.parse()?;
    let module_name = module_name.to_string();
    let module_namespace = module_namespace.to_string();

    tokio::spawn(async move {
        tokio::select! {
            _ = rx => {}
            _ = wasm_engine_serve(listener, engine, pre, proxy_uri, module_name, module_namespace) => {}
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    Ok((addr, tx))
}

async fn wasm_engine_serve(
    listener: TcpListener,
    engine: Arc<Engine>,
    pre: Arc<ProxyPre<ModuleState>>,
    proxy_uri: hyper::Uri,
    module_name: String,
    module_namespace: String,
) {
    let client = http_client();
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            break;
        };
        let engine = engine.clone();
        let pre = pre.clone();
        let proxy_uri = proxy_uri.clone();
        let module_name = module_name.clone();
        let module_namespace = module_namespace.clone();
        let client = client.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc = hyper::service::service_fn(move |req: Request<hyper::body::Incoming>| {
                let engine = engine.clone();
                let pre = pre.clone();
                let proxy_uri = proxy_uri.clone();
                let module_name = module_name.clone();
                let module_namespace = module_namespace.clone();
                let client = client.clone();
                async move {
                    let (parts, body) = req.into_parts();
                    let body_bytes = body
                        .collect()
                        .await
                        .map(|c| c.to_bytes())
                        .unwrap_or_default();
                    let request = Request::from_parts(parts, body_bytes);

                    let state = ModuleState::new(
                        module_name,
                        module_namespace,
                        proxy_uri,
                        client,
                        ModuleServices::default(),
                    )
                    .expect("ModuleState");

                    match dispatch_to_wasm(&engine, &pre, state, request).await {
                        Ok(resp) => {
                            let (parts, body) = resp.into_parts();
                            Ok::<_, Infallible>(Response::from_parts(parts, Full::new(body)))
                        }
                        Err(e) => Ok(Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body(Full::new(Bytes::from(format!("WASM error: {e}"))))
                            .unwrap()),
                    }
                }
            });
            let _ = http2::Builder::new(TokioExecutor::new())
                .serve_connection(io, svc)
                .await;
        });
    }
}

/// Build a `BlobstoreRuntime` from `WRT_TEST_S3_*` environment variables.
/// Panics if the env vars are not set.
pub fn blobstore_client() -> Arc<BlobstoreRuntime> {
    let endpoint = std::env::var("WRT_TEST_S3_ENDPOINT")
        .expect("WRT_TEST_S3_ENDPOINT must be set for this test");
    let access_key = std::env::var("WRT_TEST_S3_ACCESS_KEY")
        .expect("WRT_TEST_S3_ACCESS_KEY must be set for this test");
    let secret_key = std::env::var("WRT_TEST_S3_SECRET_KEY")
        .expect("WRT_TEST_S3_SECRET_KEY must be set for this test");
    let config = BlobstoreConfig {
        endpoint,
        access_key_id: access_key,
        secret_access_key: secret_key,
        region: "us-east-1".into(),
    };
    Arc::new(BlobstoreRuntime::new(&config).expect("BlobstoreRuntime"))
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

// ── LLM mock helpers ────────────────────────────────────────────────────────

use wr_engine::llm::LlmRuntime;

/// The mock response mode determines what the mock Claude API server returns.
#[derive(Clone)]
pub enum MockLlmMode {
    /// Return a simple text completion.
    Text {
        text: String,
        input_tokens: u32,
        output_tokens: u32,
    },
    /// Return a tool_use response.
    ToolUse {
        tool_id: String,
        tool_name: String,
        tool_input: String,
    },
    /// Return an HTTP error status.
    Error { status: u16, body: String },
    /// Return a streaming SSE response with the given text chunks.
    Stream { chunks: Vec<String> },
}

/// Spawn a mock Claude API HTTP server that returns canned responses.
/// Returns the base URL (e.g. "http://127.0.0.1:PORT") and a shutdown handle.
pub async fn spawn_mock_llm_server(
    mode: MockLlmMode,
) -> Result<(String, tokio::sync::oneshot::Sender<()>)> {
    use hyper::service::service_fn;
    use hyper_util::rt::TokioIo;
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        loop {
            let mode = mode.clone();
            tokio::select! {
                accepted = listener.accept() => {
                    let (stream, _) = match accepted {
                        Ok(v) => v,
                        Err(_) => continue,
                    };
                    let io = TokioIo::new(stream);
                    let mode = mode.clone();
                    tokio::spawn(async move {
                        let _ = hyper::server::conn::http1::Builder::new()
                            .serve_connection(
                                io,
                                service_fn(move |req| {
                                    let mode = mode.clone();
                                    async move {
                                        handle_mock_llm_request(req, mode).await
                                    }
                                }),
                            )
                            .await;
                    });
                }
                _ = &mut shutdown_rx => break,
            }
        }
    });

    Ok((format!("http://127.0.0.1:{}", addr.port()), shutdown_tx))
}

async fn handle_mock_llm_request(
    _req: hyper::Request<hyper::body::Incoming>,
    mode: MockLlmMode,
) -> Result<hyper::Response<http_body_util::Full<Bytes>>, std::convert::Infallible> {
    match mode {
        MockLlmMode::Text {
            text,
            input_tokens,
            output_tokens,
        } => {
            let body = serde_json::json!({
                "id": "msg_mock_001",
                "type": "message",
                "role": "assistant",
                "content": [{"type": "text", "text": text}],
                "model": "claude-sonnet-4-6",
                "stop_reason": "end_turn",
                "usage": {
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens
                }
            });
            Ok(hyper::Response::builder()
                .status(200)
                .header("content-type", "application/json")
                .body(Full::new(Bytes::from(serde_json::to_vec(&body).unwrap())))
                .unwrap())
        }
        MockLlmMode::ToolUse {
            tool_id,
            tool_name,
            tool_input,
        } => {
            let input_value: serde_json::Value =
                serde_json::from_str(&tool_input).unwrap_or(serde_json::json!({}));
            let body = serde_json::json!({
                "id": "msg_mock_002",
                "type": "message",
                "role": "assistant",
                "content": [{
                    "type": "tool_use",
                    "id": tool_id,
                    "name": tool_name,
                    "input": input_value
                }],
                "model": "claude-sonnet-4-6",
                "stop_reason": "tool_use",
                "usage": {"input_tokens": 50, "output_tokens": 30}
            });
            Ok(hyper::Response::builder()
                .status(200)
                .header("content-type", "application/json")
                .body(Full::new(Bytes::from(serde_json::to_vec(&body).unwrap())))
                .unwrap())
        }
        MockLlmMode::Error { status, body } => Ok(hyper::Response::builder()
            .status(status)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(body)))
            .unwrap()),
        MockLlmMode::Stream { chunks } => {
            let mut sse = String::new();
            // message_start
            sse.push_str("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_mock_003\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"claude-sonnet-4-6\",\"stop_reason\":null,\"usage\":{\"input_tokens\":25,\"output_tokens\":0}}}\n\n");
            // content_block_start
            sse.push_str("event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n");
            // content_block_delta for each chunk
            for chunk in &chunks {
                let escaped = chunk.replace('\\', "\\\\").replace('"', "\\\"");
                sse.push_str(&format!(
                    "event: content_block_delta\ndata: {{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{{\"type\":\"text_delta\",\"text\":\"{escaped}\"}}}}\n\n"
                ));
            }
            // content_block_stop
            sse.push_str("event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n");
            // message_delta with usage
            let output_tokens = chunks.iter().map(|c| c.len() as u32).sum::<u32>();
            sse.push_str(&format!(
                "event: message_delta\ndata: {{\"type\":\"message_delta\",\"delta\":{{\"stop_reason\":\"end_turn\"}},\"usage\":{{\"output_tokens\":{output_tokens}}}}}\n\n"
            ));
            // message_stop
            sse.push_str("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n");

            Ok(hyper::Response::builder()
                .status(200)
                .header("content-type", "text/event-stream")
                .body(Full::new(Bytes::from(sse)))
                .unwrap())
        }
    }
}

/// Build an `LlmRuntime` pointing at the given mock base URL.
pub fn mock_llm_runtime(base_url: &str) -> Arc<LlmRuntime> {
    use wr_engine::config::LlmConfig;
    // Set a temp env var for the API key
    std::env::set_var("WRT_TEST_LLM_KEY", "mock-key");
    let config = LlmConfig {
        provider: "anthropic".into(),
        api_key_env: "WRT_TEST_LLM_KEY".into(),
        base_url: base_url.into(),
        max_tokens_limit: 8192,
    };
    Arc::new(LlmRuntime::new(&config).expect("LlmRuntime"))
}

/// Build a `ModuleState` with an LLM runtime for WASM guest tests.
pub fn llm_state(llm: Arc<LlmRuntime>) -> ModuleState {
    ModuleState::new(
        "llm-test".into(),
        "test-ns".into(),
        "http://127.0.0.1:9001".parse().unwrap(),
        http_client(),
        ModuleServices {
            llm: Some(llm),
            ..Default::default()
        },
    )
    .expect("ModuleState")
}
