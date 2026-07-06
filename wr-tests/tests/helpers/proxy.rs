use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::server::conn::http2;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use wr_common::wruntime::{
    manager_service_client::ManagerServiceClient, EngineRegistration, ModuleDescriptor,
    RegisterEngineRequest, RoutingRule,
};

pub use wr_proxy::config::{EgressConfig, ExternalRoute};

use super::manager::sync_table;
use super::pki::{shared_test_pki, test_mtls_pool};

pub const TEST_SELF_PEER: &str = "http://test-node";

/// Identifies an engine instance and the node it belongs to.
pub struct EngineSpec<'a> {
    pub id: &'a str,
    pub addr: &'a str,
    /// Peer address for local-vs-remote dispatch.  Use [`TEST_SELF_PEER`] in
    /// single-node tests so `peer == self` yields local dispatch.
    pub peer_address: &'a str,
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
            proxy_address: engine.peer_address.into(),
            peer_address: engine.peer_address.into(),
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
        peer_address: engine.peer_address.into(),
        healthy: false, // manager overrides to true on upsert
    })
    .await?;
    Ok(())
}

pub async fn start_proxy(table: wr_proxy::routing::CachedRoutingTable) -> Result<SocketAddr> {
    start_proxy_on(table, TEST_SELF_PEER).await
}

/// Build and start a proxy with a custom `self_peer_address`; returns the bound address.
pub async fn start_proxy_on(
    table: wr_proxy::routing::CachedRoutingTable,
    self_peer_address: &str,
) -> Result<SocketAddr> {
    let svc = tower::ServiceBuilder::new()
        .layer(wr_proxy::layers::RoutingLayer::new(
            table,
            self_peer_address,
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
    Ok(addr)
}

/// Build and start an external-facing ingress proxy on an ephemeral port.
pub async fn start_ingress_proxy(
    table: wr_proxy::routing::CachedRoutingTable,
    routes: Vec<ExternalRoute>,
) -> Result<SocketAddr> {
    let svc = tower::ServiceBuilder::new()
        .layer(wr_proxy::layers::IngressLayer::new(routes))
        .layer(wr_proxy::layers::RoutingLayer::new(table, TEST_SELF_PEER))
        .service(wr_proxy::layers::ForwardService::new(
            std::sync::Arc::new(wr_proxy::circuit_breaker::CircuitBreakerRegistry::new(
                Default::default(),
            )),
            test_mtls_pool(),
        ));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(proxy_serve(listener, svc));
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
        .layer(
            wr_proxy::layers::RoutingLayer::new(table, TEST_SELF_PEER).with_egress(egress_domains),
        )
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
    Ok(addr)
}

pub struct Node {
    /// TCP address this node listens on — pass to [`proxy_get`] and friends.
    pub addr: std::net::SocketAddr,
    /// TLS peer URL stored in routing rules for engines on this node.
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

pub async fn start_proxy_with_cb(
    table: wr_proxy::routing::CachedRoutingTable,
    cb_config: wr_proxy::config::CircuitBreakerConfig,
) -> Result<SocketAddr> {
    let svc = tower::ServiceBuilder::new()
        .layer(wr_proxy::layers::RoutingLayer::new(table, TEST_SELF_PEER))
        .service(wr_proxy::layers::ForwardService::new(
            Arc::new(wr_proxy::circuit_breaker::CircuitBreakerRegistry::new(
                cb_config,
            )),
            test_mtls_pool(),
        ));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(proxy_serve(listener, svc));
    Ok(addr)
}
