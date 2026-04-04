mod circuit_breaker;
pub mod config;
pub mod indexed_routing;
mod layers;
pub mod node_service;
pub mod routing;

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use http::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use tokio::net::TcpListener;
use tonic::transport::Server;
use tower::{Service, ServiceBuilder};

use layers::{
    EgressLayer, ForwardService, IngressLayer, ProxyBody, ResBody, RoutingLayer, TracingLayer,
};
use tracing::{error, info, warn};
use wr_common::discovery::ManagerDiscovery;
use wr_common::wruntime::node_service_server::NodeServiceServer;

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    let _telemetry = wr_common::telemetry::init("wr-proxy")?;
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "proxy.toml".to_string());
    let config = config::ProxyConfig::load(&config_path)?;

    // ── Shared state ──────────────────────────────────────────────────────
    let routing_table = routing::new_routing_table();
    let cb_registry = Arc::new(circuit_breaker::CircuitBreakerRegistry::new(
        config.circuit_breaker.clone(),
    ));

    // ── Manager discovery via Postgres ────────────────────────────────────
    let db_pool = {
        let pg_config = deadpool_postgres::Config {
            url: Some(config.database.url.clone()),
            pool: Some(deadpool_postgres::PoolConfig {
                max_size: config.database.max_connections,
                ..Default::default()
            }),
            ..Default::default()
        };
        pg_config
            .create_pool(
                Some(deadpool_postgres::Runtime::Tokio1),
                tokio_postgres::NoTls,
            )
            .context("failed to create discovery pool")?
    };
    let discovery = Arc::new(ManagerDiscovery::new(db_pool));
    discovery.refresh().await;
    discovery.spawn_refresh_task();
    info!("manager discovery initialized");

    // ── Initial routing table sync (blocks until first fetch succeeds) ──
    {
        let mut client = discovery
            .get_client()
            .await
            .map_err(|e| anyhow::anyhow!("initial manager connect failed: {e}"))?;
        routing::sync_once(&mut client, &routing_table, &cb_registry)
            .await
            .context("initial routing table sync failed")?;
        info!("initial routing table sync complete");
    }

    // ── Background tasks ──────────────────────────────────────────────────
    tokio::spawn(routing::sync_routing_table(
        discovery.clone(),
        routing_table.clone(),
        config.cache.routing_table_ttl_secs,
        cb_registry.clone(),
    ));

    // ── NodeService gRPC control plane ───────────────────────────────────
    let node_agent = Arc::new(node_service::NodeAgent::new(discovery));
    node_agent.spawn_heartbeat_loop(Duration::from_secs(3));

    let control_addr = config
        .control_address
        .parse()
        .context("invalid control_address")?;
    tokio::spawn(
        Server::builder()
            .add_service(NodeServiceServer::from_arc(node_agent))
            .serve(control_addr),
    );
    info!(address = %config.control_address, "proxy control plane listening");

    // ── Internal Tower service stack ──────────────────────────────────────
    //
    //   TracingLayer               ← root OTel span per request
    //     └─ RoutingLayer          ← single routing table read; sets ExternalEgress
    //          └─ EgressLayer      ← handles ExternalEgress; passes internal to forward
    //               └─ ForwardService
    //
    let egress_domains = config
        .egress
        .as_ref()
        .map(|e| e.allowed_domains.clone())
        .unwrap_or_default();
    let internal_svc = ServiceBuilder::new()
        .layer(TracingLayer)
        .layer(
            RoutingLayer::new(routing_table.clone(), config.node.proxy_address.clone())
                .with_egress(egress_domains),
        )
        .layer(EgressLayer::new(config.egress.clone()))
        .service(ForwardService::new(cb_registry.clone()));

    let internal_listener = TcpListener::bind(&config.listen_address).await?;
    info!(address = %config.listen_address, "proxy listening (internal)");
    tokio::spawn(accept_loop(internal_listener, internal_svc));

    // ── External Tower service stack (optional) ───────────────────────────
    //
    //   IngressLayer          ← strips x-wr-* headers, matches public routes,
    //     |                     injects x-wr-destination + x-wr-source: external
    //     └─ TracingLayer
    //          └─ RoutingLayer
    //               └─ ForwardService
    //
    if let Some(ext) = &config.external {
        let external_svc = ServiceBuilder::new()
            .layer(IngressLayer::new(ext.routes.clone()))
            .layer(TracingLayer)
            .layer(RoutingLayer::new(
                routing_table,
                config.node.proxy_address.clone(),
            ))
            .service(ForwardService::new(cb_registry.clone()));

        let external_listener = TcpListener::bind(&ext.listen_address).await?;
        info!(address = %ext.listen_address, "proxy listening (external)");
        tokio::spawn(accept_loop(external_listener, external_svc));
    }

    // ── Wait for shutdown signal ──────────────────────────────────────────
    wr_common::signal::shutdown_signal().await;
    Ok(())
}

/// Accepts connections on `listener` and spawns a task per connection that
/// drives `svc` for each HTTP/2 request.  Request bodies are streamed through
/// as [`ProxyBody`] — no buffering occurs at the accept layer.
async fn accept_loop<S>(listener: TcpListener, svc: S)
where
    S: Service<Request<ProxyBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Error: std::fmt::Display + Send + 'static,
    S::Future: Send + 'static,
{
    loop {
        let (stream, peer_addr) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "accept error");
                continue;
            }
        };
        let svc = svc.clone();

        tokio::spawn(async move {
            let io = TokioIo::new(stream);

            let hyper_svc =
                hyper::service::service_fn(move |req: Request<hyper::body::Incoming>| {
                    let mut svc = svc.clone();
                    async move {
                        // Wrap the streaming Incoming body into ProxyBody — no buffering.
                        let req = req.map(ProxyBody::streaming);

                        match svc.call(req).await {
                            Ok(resp) => Ok::<_, Infallible>(resp),
                            Err(e) => {
                                error!(error = %e, "service error");
                                Ok(layers::error_response(
                                    StatusCode::BAD_GATEWAY,
                                    "internal proxy error",
                                ))
                            }
                        }
                    }
                });

            if let Err(e) = auto::Builder::new(TokioExecutor::new())
                .serve_connection(io, hyper_svc)
                .await
            {
                warn!(peer = %peer_addr, error = %e, "connection error");
            }
        });
    }
}
