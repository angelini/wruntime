/// Shared helpers for Wruntime integration tests.
///
/// Infrastructure helpers (manager, proxy, stubs) start real in-process
/// services on ephemeral ports.  Fixture helpers (schema, DB) build the
/// minimal test data each suite needs.
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};

use anyhow::Result;
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
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
use wasmtime::component::Component;
use wasmtime::Engine;
use wasmtime_wasi_http::p2::bindings::ProxyPre;

use wr_common::wruntime::{
    manager_service_client::ManagerServiceClient, manager_service_server::ManagerServiceServer,
    EngineRegistration, GetRoutingTableRequest, ModuleDescriptor, RegisterEngineRequest,
    RoutingRule,
};
use wr_engine::blobstore::BlobstoreRuntime;
use wr_engine::config::BlobstoreConfig;
use wr_manager::service::Manager;

// Re-export DB types so tests using `helpers::*` need no local `use` statements.
pub use wr_engine::db::wruntime::db::database::{DbError, Host as DbHost, PgValue};
pub use wr_engine::state::{ModuleServices, ModuleState};
pub use wr_proxy::config::{EgressConfig, ExternalRoute};

// ── Test PKI ─────────────────────────────────────────────────────────────────

/// In-memory PKI for tests — generated once per test binary.
pub struct TestPki {
    pub ca_cert_der: Vec<rustls::pki_types::CertificateDer<'static>>,
    pub node_cert_der: Vec<rustls::pki_types::CertificateDer<'static>>,
    pub node_key_der: rustls::pki_types::PrivateKeyDer<'static>,
}

/// Generate a CA + node cert entirely in memory. No files on disk.
pub fn generate_test_pki() -> TestPki {
    use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair, SanType};
    use std::net::IpAddr;

    // CA
    let mut ca_params = CertificateParams::new(vec![]).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "test-ca");
    let ca_key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let ca_issuer = rcgen::Issuer::from_params(&ca_params, ca_key);

    // Node cert signed by CA
    let mut node_params = CertificateParams::new(vec![]).unwrap();
    node_params.subject_alt_names = vec![
        SanType::DnsName("localhost".try_into().unwrap()),
        SanType::IpAddress(IpAddr::from([127, 0, 0, 1])),
    ];
    node_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "test-node");
    let node_key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let node_cert = node_params.signed_by(&node_key, &ca_issuer).unwrap();

    TestPki {
        ca_cert_der: vec![ca_cert.der().clone()],
        node_cert_der: vec![node_cert.der().clone()],
        node_key_der: rustls::pki_types::PrivateKeyDer::Pkcs8(node_key.serialize_der().into()),
    }
}

/// Lazily-initialized shared PKI — cert gen happens once per test binary.
/// Also installs the rustls crypto provider if not already set.
pub fn shared_test_pki() -> &'static TestPki {
    let _ = rustls::crypto::ring::default_provider().install_default();
    static PKI: OnceLock<TestPki> = OnceLock::new();
    PKI.get_or_init(generate_test_pki)
}

/// Build an HttpsClientPool from the shared test PKI.
pub fn test_mtls_pool() -> wr_common::tls::HttpsClientPool<wr_proxy::layers::ProxyBody> {
    let pki = shared_test_pki();
    let config = wr_common::tls::build_client_config_from_der(
        pki.node_cert_der.clone(),
        pki.node_key_der.clone_key(),
        &pki.ca_cert_der,
    )
    .unwrap();
    wr_common::tls::HttpsClientPool::new(2, config)
}

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

    // Create the schema using a one-shot connection to the base DB (no search_path override).
    let setup_pool = wr_common::pool::build_pool(&base_url, 1).expect("failed to build setup pool");
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
            // Ensure wr_system schema exists before any migrations run.
            // Done once under the lock to avoid races between parallel tests.
            client
                .batch_execute("CREATE SCHEMA IF NOT EXISTS wr_system")
                .await
                .expect("create wr_system schema");
            CLEANED.store(true, Ordering::SeqCst);
        }
    }

    client
        .batch_execute(&format!("CREATE SCHEMA \"{schema}\""))
        .await
        .expect("create schema");
    drop(client);
    drop(setup_pool);

    // Build the real pool with search_path pinned to the test schema.
    let pool = wr_common::pool::build_pool_with_search_path(&base_url, 5, &schema)
        .expect("failed to build manager test pool");

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
            .add_service(ManagerServiceServer::new(Manager::new(pool, crypto)))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    Ok(format!("http://{addr}"))
}

/// Return a connected manager gRPC client.
pub async fn manager_client(addr: &str) -> Result<ManagerServiceClient<tonic::transport::Channel>> {
    Ok(ManagerServiceClient::connect(addr.to_string()).await?)
}

/// Set up an in-process manager: pool + gRPC server + connected client.
pub async fn manager_trio() -> Result<(
    deadpool_postgres::Pool,
    String,
    ManagerServiceClient<tonic::transport::Channel>,
)> {
    let pool = manager_pool().await;
    let addr = start_manager(pool.clone()).await?;
    let client = manager_client(&addr).await?;
    Ok((pool, addr, client))
}

/// Like [`manager_trio`] but also spawns the heartbeat monitor background task.
pub async fn manager_trio_with_monitor(
    timeout_secs: u64,
) -> Result<(
    deadpool_postgres::Pool,
    String,
    ManagerServiceClient<tonic::transport::Channel>,
)> {
    let pool = manager_pool().await;
    let addr = start_manager_with_monitor(pool.clone(), timeout_secs).await?;
    let client = manager_client(&addr).await?;
    Ok((pool, addr, client))
}

/// Register a module with sensible test defaults (empty proxy_address, minimal schema).
pub async fn register_test_module(
    c: &mut ManagerServiceClient<tonic::transport::Channel>,
    engine_id: &str,
    engine_addr: &str,
    namespace: &str,
    name: &str,
    version: &str,
) -> Result<()> {
    register_module(
        c,
        EngineSpec {
            id: engine_id,
            addr: engine_addr,
            proxy_address: "",
        },
        ModuleSpec {
            namespace,
            name,
            version,
            schema: minimal_file_descriptor_set(),
        },
    )
    .await
}

/// Create a routing table and sync it from the manager in one step.
pub async fn synced_routing_table(mgr_addr: &str) -> Result<wr_proxy::routing::CachedRoutingTable> {
    let table = wr_proxy::routing::new_routing_table();
    sync_table(mgr_addr, &table).await?;
    Ok(table)
}

/// Query the routing table via gRPC and find a rule by destination module name.
/// Returns `(healthy, version)`.
pub async fn get_rule_health(
    mgr: &mut ManagerServiceClient<tonic::transport::Channel>,
    destination_module: &str,
) -> Result<(bool, u64)> {
    let table = mgr
        .get_routing_table(GetRoutingTableRequest { known_version: 0 })
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
        .get_routing_table(GetRoutingTableRequest { known_version: 0 })
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
            peer_address: engine.proxy_address.into(),
            modules: vec![ModuleDescriptor {
                name: module.name.into(),
                namespace: module.namespace.into(),
                version: module.version.into(),
                proto_schema: module.schema,
            }],
            secrets: vec![],
            db_namespaces: vec![],
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
        peer_address: engine.proxy_address.into(),
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
        .get_routing_table(GetRoutingTableRequest { known_version: 0 })
        .await?
        .into_inner()
        .table
    {
        *table.write().await =
            wr_proxy::indexed_routing::IndexedRoutingTable::from_proto(&incoming);
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
        .service(wr_proxy::layers::ForwardService::new(
            std::sync::Arc::new(wr_proxy::circuit_breaker::CircuitBreakerRegistry::new(
                Default::default(),
            )),
            test_mtls_pool(),
        ));
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
        .service(wr_proxy::layers::ForwardService::new(
            std::sync::Arc::new(wr_proxy::circuit_breaker::CircuitBreakerRegistry::new(
                Default::default(),
            )),
            test_mtls_pool(),
        ));
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
    let egress_domains = egress_cfg
        .as_ref()
        .map(|e| e.allowed_domains.clone())
        .unwrap_or_default();
    let svc = tower::ServiceBuilder::new()
        .layer(wr_proxy::layers::RoutingLayer::new(table, "").with_egress(egress_domains))
        .layer(wr_proxy::layers::EgressLayer::new(egress_cfg))
        .service(wr_proxy::layers::ForwardService::new(
            Arc::new(wr_proxy::circuit_breaker::CircuitBreakerRegistry::new(
                Default::default(),
            )),
            test_mtls_pool(),
        ));
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

/// Spin up a proxy node with both a plain HTTP listener (for local engine
/// traffic / test requests) and a TLS listener (for cross-node peer traffic).
/// The `proxy_address` returned in `Node` is the `https://` TLS address —
/// this is what gets stored in routing rules for remote proxy forwarding.
pub async fn start_node(mgr_addr: &str) -> Result<Node> {
    // Plain HTTP listener (test requests + local engine forwarding)
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    // TLS listener (cross-node peer traffic)
    let tls_listener = TcpListener::bind("127.0.0.1:0").await?;
    let tls_addr = tls_listener.local_addr()?;
    let proxy_address = format!("https://127.0.0.1:{}", tls_addr.port());

    let pki = shared_test_pki();
    let server_config = wr_common::tls::build_server_config_from_der(
        pki.node_cert_der.clone(),
        pki.node_key_der.clone_key(),
        &pki.ca_cert_der,
    )?;
    let tls_acceptor = tokio_rustls::TlsAcceptor::from(server_config);

    let table = wr_proxy::routing::new_routing_table();
    sync_table(mgr_addr, &table).await?;

    let svc = tower::ServiceBuilder::new()
        .layer(wr_proxy::layers::RoutingLayer::new(
            table.clone(),
            &proxy_address,
        ))
        .service(wr_proxy::layers::ForwardService::new(
            std::sync::Arc::new(wr_proxy::circuit_breaker::CircuitBreakerRegistry::new(
                Default::default(),
            )),
            test_mtls_pool(),
        ));

    let (tx, rx) = oneshot::channel::<()>();
    let svc_clone = svc.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = rx => {}
            _ = async {
                // Run both listeners concurrently
                tokio::join!(
                    proxy_serve(listener, svc.clone()),
                    tls_proxy_serve(tls_listener, tls_acceptor, svc_clone),
                );
            } => {}
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
    let path = "/Ping";
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

/// Drive the proxy Tower stack over a TLS-wrapped TCP listener (mTLS peer traffic).
pub async fn tls_proxy_serve<S>(listener: TcpListener, acceptor: tokio_rustls::TlsAcceptor, svc: S)
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
        let acceptor = acceptor.clone();
        let svc = svc.clone();
        tokio::spawn(async move {
            let tls_stream = match acceptor.accept(stream).await {
                Ok(s) => s,
                Err(_) => return,
            };
            let io = TokioIo::new(tls_stream);
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

/// Pool of HTTP/2 clients for WASM module outbound requests.
pub fn http_pool() -> wr_common::http_pool::HttpClientPool<Full<Bytes>> {
    wr_common::http_pool::HttpClientPool::new(wr_common::http_pool::DEFAULT_POOL_SIZE)
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
        .service(wr_proxy::layers::ForwardService::new(
            Arc::new(wr_proxy::circuit_breaker::CircuitBreakerRegistry::new(
                cb_config,
            )),
            test_mtls_pool(),
        ));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(proxy_serve(listener, svc));
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    Ok(addr)
}

// ── Manager with heartbeat monitor ──────────────────────────────────────────

/// Start an in-process wr-manager that also runs the heartbeat monitor background
/// task.  `timeout_secs` controls how long before an engine is marked unhealthy.
/// Returns the gRPC address.
pub async fn start_manager_with_monitor(
    pool: deadpool_postgres::Pool,
    timeout_secs: u64,
) -> Result<String> {
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
                pool.clone(),
                crypto,
            )))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    tokio::spawn(wr_manager::state::monitor_heartbeats(
        pool,
        timeout_secs,
        std::time::Duration::from_millis(200),
    ));
    Ok(format!("http://{addr}"))
}

/// Backdate an engine's heartbeat in the database for testing health timeout.
pub async fn backdate_engine_heartbeat(
    pool: &deadpool_postgres::Pool,
    engine_id: &str,
    secs_ago: i64,
) {
    let client = pool.get().await.unwrap();
    client
        .execute(
            "UPDATE wr_engines SET last_heartbeat = NOW() - make_interval(secs => $1::double precision) WHERE engine_id = $2",
            &[&(secs_ago as f64), &engine_id],
        )
        .await
        .unwrap();
}

// ── Manager cluster ──────────────────────────────────────────────────────────

/// A running manager instance in a cluster.
pub struct ClusteredManager {
    /// gRPC address of this manager.
    pub addr: String,
}

/// Start `count` managers with chitchat gossip, all sharing the same Postgres.
/// Chitchat is used only for manager liveness — engine heartbeats are in Postgres.
pub async fn start_manager_cluster(
    pool: deadpool_postgres::Pool,
    count: usize,
    heartbeat_timeout_secs: u64,
) -> Result<Vec<ClusteredManager>> {
    let mut managers = Vec::with_capacity(count);
    let mut gossip_addrs: Vec<String> = Vec::new();

    for _ in 0..count {
        let manager_id = uuid::Uuid::new_v4().to_string();

        // Bind gRPC listener
        let grpc_listener = TcpListener::bind("127.0.0.1:0").await?;
        let grpc_addr = grpc_listener.local_addr()?;
        let grpc_url = format!("http://{grpc_addr}");

        // Bind gossip UDP port (pick a free TCP port and use it for UDP)
        let gossip_port = {
            let tmp = TcpListener::bind("127.0.0.1:0").await?;
            tmp.local_addr()?.port()
        };
        let gossip_listen: std::net::SocketAddr = format!("127.0.0.1:{gossip_port}").parse()?;
        let gossip_addr_str = gossip_listen.to_string();

        // Register in wr_managers
        wr_manager::db::register_manager(&pool, &manager_id, &grpc_url, &gossip_addr_str)
            .await
            .map_err(|e| anyhow::anyhow!("register_manager: {e}"))?;

        // Bootstrap chitchat for manager liveness (no application keys)
        let _cluster = Arc::new(
            wr_manager::cluster::ClusterHandle::new(
                &manager_id,
                "test-cluster",
                gossip_listen,
                gossip_addrs.clone(),
                std::time::Duration::from_millis(100),
            )
            .await?,
        );

        gossip_addrs.push(gossip_addr_str);

        let crypto = Arc::new(
            wr_manager::crypto::SecretCrypto::from_hex(
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            )
            .expect("test encryption key"),
        );

        let manager = Manager::new(pool.clone(), crypto);

        // Start gRPC server
        tokio::spawn(
            Server::builder()
                .add_service(ManagerServiceServer::new(manager))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                    grpc_listener,
                )),
        );

        // Start heartbeat monitor (reads Postgres, no gossip)
        tokio::spawn(wr_manager::state::monitor_heartbeats(
            pool.clone(),
            heartbeat_timeout_secs,
            std::time::Duration::from_millis(200),
        ));

        managers.push(ClusteredManager { addr: grpc_url });
    }

    Ok(managers)
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
        http_pool(),
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
    if let Err(e) = client
        .simple_query(&format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\""))
        .await
    {
        // Ignore unique_violation (23505) — a concurrent test may have created
        // the schema between our IF NOT EXISTS check and the actual CREATE.
        let is_duplicate = e
            .as_db_error()
            .is_some_and(|db| db.code().code() == "23505");
        if !is_duplicate {
            panic!("provision schema: {e}");
        }
    }
    drop(client);
    ModuleState::new(
        name.into(),
        namespace.into(),
        "http://127.0.0.1:9001".parse().unwrap(),
        http_pool(),
        ModuleServices {
            db_pool: Some(pool),
            db_schema: Some(Arc::from(schema)),
            ..Default::default()
        },
    )
    .expect("ModuleState")
}

/// Same schema-provisioning body as `db_state_for_module`, but with `limits`.
pub async fn db_state_for_module_with_limits(
    pool_size: usize,
    namespace: &str,
    name: &str,
    limits: wr_engine::config::ResourceLimits,
) -> ModuleState {
    let url = require_db_url();
    let schema = wr_engine::pool::module_schema(namespace, name);
    let pool = Arc::new(wr_engine::pool::build_pool(&url, pool_size).expect("build_pool"));
    let client = pool
        .get()
        .await
        .expect("get connection for schema provisioning");
    if let Err(e) = client
        .simple_query(&format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\""))
        .await
    {
        let is_duplicate = e
            .as_db_error()
            .is_some_and(|db| db.code().code() == "23505");
        if !is_duplicate {
            panic!("provision schema: {e}");
        }
    }
    drop(client);
    ModuleState::new(
        name.into(),
        namespace.into(),
        "http://127.0.0.1:9001".parse().unwrap(),
        http_pool(),
        ModuleServices {
            db_pool: Some(pool),
            db_schema: Some(Arc::from(schema)),
            limits,
            ..Default::default()
        },
    )
    .expect("ModuleState")
}

// ── WASM guest dispatch ──────────────────────────────────────────────────────

/// Fixed pool config for the test harness. Preserves the harness's historical
/// hardcoded limits: 100 component instances (which `build_engine` also uses for
/// `total_memories`/`total_tables`) and a 10 MiB per-instance memory cap — so the
/// runtime extraction does not silently change test capacity.
fn test_pool_config() -> wr_engine::config::PoolConfig {
    wr_engine::config::PoolConfig {
        total_component_instances: 100,
        max_memory_size: 10 * 1024 * 1024,
        epoch_tick_interval_ms: 10,
    }
}

/// Set up a wasmtime `Engine` + `ProxyPre` from a compiled WASM component path.
///
/// Configures the pooling instance allocator (matching production) so that
/// concurrent instantiations reuse pre-allocated memory slots instead of
/// issuing per-request mmap/mprotect syscalls.
pub fn wasm_module_pre(wasm_path: &str) -> Result<(Arc<Engine>, Arc<ProxyPre<ModuleState>>)> {
    let engine = wr_engine::runtime::build_engine(&test_pool_config())?;
    {
        let e = engine.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(10));
            loop {
                interval.tick().await;
                e.increment_epoch();
            }
        });
    }
    let component = Component::from_file(&engine, wasm_path)?;
    let linker = wr_engine::runtime::configure_linker(&engine)?;
    let pre = wr_engine::runtime::instantiate_pre(&engine, &linker, &component)?;
    Ok((Arc::new(engine), Arc::new(pre)))
}

/// Dispatch a single HTTP request through a WASM component, returning the response.
pub async fn dispatch_to_wasm(
    engine: &Engine,
    pre: &ProxyPre<ModuleState>,
    state: ModuleState,
    request: http::Request<Bytes>,
) -> Result<http::Response<Bytes>> {
    wr_engine::runtime::run_incoming_handler(engine, pre, state, request).await
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
    let pool = http_pool();
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            break;
        };
        let engine = engine.clone();
        let pre = pre.clone();
        let proxy_uri = proxy_uri.clone();
        let module_name = module_name.clone();
        let module_namespace = module_namespace.clone();
        let pool = pool.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc = hyper::service::service_fn(move |req: Request<hyper::body::Incoming>| {
                let engine = engine.clone();
                let pre = pre.clone();
                let proxy_uri = proxy_uri.clone();
                let module_name = module_name.clone();
                let module_namespace = module_namespace.clone();
                let pool = pool.clone();
                async move {
                    // Collect the body on this stream, then spawn the
                    // CPU-heavy WASM work onto a separate tokio task so
                    // hyper's HTTP/2 serve_connection can drive other
                    // streams concurrently.
                    let (parts, body) = req.into_parts();
                    let body_bytes = body
                        .collect()
                        .await
                        .map(|c| c.to_bytes())
                        .unwrap_or_default();
                    let request = Request::from_parts(parts, body_bytes);

                    let handle = tokio::spawn(async move {
                        let state = ModuleState::new(
                            module_name.into(),
                            module_namespace.into(),
                            proxy_uri,
                            pool,
                            ModuleServices::default(),
                        )
                        .expect("ModuleState");

                        dispatch_to_wasm(&engine, &pre, state, request).await
                    });

                    match handle.await.expect("wasm task panicked") {
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
    let _ = rustls::crypto::ring::default_provider().install_default();
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
        max_object_size: 16 * 1024 * 1024,
        max_list_objects: 1000,
    };
    Arc::new(BlobstoreRuntime::new(&config).expect("BlobstoreRuntime"))
}

/// Build a `ModuleState` with a blobstore client for WASM guest tests.
pub fn blobstore_state(blobstore: Arc<BlobstoreRuntime>) -> ModuleState {
    ModuleState::new(
        "blobstore-test".into(),
        "test-ns".into(),
        "http://127.0.0.1:9001".parse().unwrap(),
        http_pool(),
        ModuleServices {
            blobstore: Some(blobstore),
            ..Default::default()
        },
    )
    .expect("ModuleState")
}

/// Build a `ModuleState` with a blobstore client and explicit size/list limits.
pub fn blobstore_state_with_limits(
    blobstore: Arc<BlobstoreRuntime>,
    blob_limits: wr_engine::config::BlobstoreLimits,
) -> ModuleState {
    ModuleState::new(
        "blobstore-test".into(),
        "test-ns".into(),
        "http://127.0.0.1:9001".parse().unwrap(),
        http_pool(),
        ModuleServices {
            blobstore: Some(blobstore),
            blob_limits,
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
        http_pool(),
        ModuleServices::default(),
    )
    .expect("ModuleState")
}

pub fn tracing_state_with_limits(limits: wr_engine::config::ResourceLimits) -> ModuleState {
    ModuleState::new(
        "tracing-test".into(),
        "test-ns".into(),
        "http://127.0.0.1:9001".parse().unwrap(),
        http_pool(),
        ModuleServices {
            limits,
            ..Default::default()
        },
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
    /// Return a streaming SSE response that emits partial text then a stream-level `error` event.
    StreamError,
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
            // message_start — CRLF line endings (exercises CRLF normalization). Carries input_tokens.
            sse.push_str("event: message_start\r\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_mock_003\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"claude-sonnet-4-6\",\"stop_reason\":null,\"usage\":{\"input_tokens\":25,\"output_tokens\":0}}}\r\n\r\n");
            // content_block_start
            sse.push_str("event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n");
            // ping — CRLF, must be skipped (no guest event)
            sse.push_str("event: ping\r\ndata: {\"type\":\"ping\"}\r\n\r\n");
            // content_block_delta per chunk, JSON split across two data: lines (multiline accumulation)
            for chunk in &chunks {
                let escaped = chunk.replace('\\', "\\\\").replace('"', "\\\"");
                sse.push_str(&format!(
                    "event: content_block_delta\ndata: {{\"type\":\"content_block_delta\",\"index\":0,\ndata: \"delta\":{{\"type\":\"text_delta\",\"text\":\"{escaped}\"}}}}\n\n"
                ));
            }
            // content_block_stop
            sse.push_str("event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n");
            // message_delta with stop_reason + cumulative output_tokens
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
        MockLlmMode::StreamError => {
            let mut sse = String::new();
            sse.push_str("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_mock_004\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"claude-sonnet-4-6\",\"stop_reason\":null,\"usage\":{\"input_tokens\":10,\"output_tokens\":0}}}\n\n");
            sse.push_str("event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n");
            sse.push_str("event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"partial\"}}\n\n");
            sse.push_str("event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"server overloaded\"}}\n\n");

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
    let _ = rustls::crypto::ring::default_provider().install_default();
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
        http_pool(),
        ModuleServices {
            llm: Some(llm),
            ..Default::default()
        },
    )
    .expect("ModuleState")
}

pub fn llm_state_with_limits(
    llm: Arc<LlmRuntime>,
    limits: wr_engine::config::ResourceLimits,
) -> ModuleState {
    ModuleState::new(
        "llm-test".into(),
        "test-ns".into(),
        "http://127.0.0.1:9001".parse().unwrap(),
        http_pool(),
        ModuleServices {
            llm: Some(llm),
            limits,
            ..Default::default()
        },
    )
    .expect("ModuleState")
}

// ── Worker helpers ───────────────────────────────────────────────────────────

/// Build a `deadpool_postgres::Pool` for worker integration tests and provision
/// the `wr__jobs` schema exactly once. Does NOT clean the jobs table — use unique
/// namespaces for test isolation.
pub async fn worker_pool() -> deadpool_postgres::Pool {
    use tokio::sync::OnceCell;
    static PROVISIONED: OnceCell<()> = OnceCell::const_new();

    let url = require_db_url();
    let pool = wr_engine::pool::build_pool(&url, 2).expect("build pool");

    PROVISIONED
        .get_or_init(|| async {
            wr_engine::worker::provision_job_schema(&pool)
                .await
                .expect("provision wr__jobs");
        })
        .await;

    pool
}

/// Spawn a stub engine that processes worker job requests.
///
/// For each inbound request, the stub reads the path (job_type) and body (payload),
/// then responds with 200 OK and the body `"processed:{path}"`.
/// If the path contains "fail", responds with 500 instead.
pub async fn spawn_worker_stub_engine() -> Result<(String, oneshot::Sender<()>)> {
    let (tx, rx) = oneshot::channel::<()>();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = format!("http://{}", listener.local_addr()?);
    tokio::spawn(async move {
        tokio::select! {
            _ = rx => {}
            _ = worker_stub_engine(listener) => {}
        }
    });
    Ok((addr, tx))
}

async fn worker_stub_engine(listener: TcpListener) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            break;
        };
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc =
                hyper::service::service_fn(|req: Request<hyper::body::Incoming>| async move {
                    let path = req.uri().path().to_string();
                    let status = if path.contains("fail") { 500 } else { 200 };
                    let body_bytes = BodyExt::collect(req.into_body())
                        .await
                        .map(|c| c.to_bytes())
                        .unwrap_or_default();
                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(status)
                            .body(Full::new(Bytes::from(format!(
                                "processed:{}:{}",
                                path,
                                body_bytes.len()
                            ))))
                            .unwrap(),
                    )
                });
            let _ = http2::Builder::new(TokioExecutor::new())
                .serve_connection(io, svc)
                .await;
        });
    }
}
