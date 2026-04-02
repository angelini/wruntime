mod circuit_breaker;
mod config;
mod layers;
mod routing;

use std::convert::Infallible;
use std::sync::Arc;

use anyhow::{Context, Result};
use http::{Request, Response, StatusCode};
use hyper::server::conn::http2;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;
use tower::{Service, ServiceBuilder};

use layers::{
    EgressLayer, ForwardService, IngressLayer, ProxyBody, ResBody, RoutingLayer, TracingLayer,
};
use tracing::{error, info, warn};
use wr_common::wruntime::manager_service_client::ManagerServiceClient;

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

    // ── Connect to wr-manager ─────────────────────────────────────────────
    let manager_client = ManagerServiceClient::connect(config.manager_address.clone()).await?;
    info!(address = %config.manager_address, "connected to manager");

    // ── Initial routing table sync (blocks until first fetch succeeds) ──
    {
        let mut client = manager_client.clone();
        routing::sync_once(&mut client, &routing_table, &cb_registry)
            .await
            .context("initial routing table sync failed")?;
        info!("initial routing table sync complete");
    }

    // ── Background tasks ──────────────────────────────────────────────────
    tokio::spawn(routing::sync_routing_table(
        manager_client.clone(),
        routing_table.clone(),
        config.cache.routing_table_ttl_secs,
        cb_registry.clone(),
    ));

    // ── Internal Tower service stack ──────────────────────────────────────
    //
    //   TracingLayer               ← root OTel span per request
    //     └─ RoutingLayer          ← single routing table read; sets ExternalEgress
    //          └─ EgressLayer      ← handles ExternalEgress; passes internal to forward
    //               └─ ForwardService
    //
    let egress_enabled = config.egress.is_some();
    let internal_svc = ServiceBuilder::new()
        .layer(TracingLayer)
        .layer(
            RoutingLayer::new(routing_table.clone(), config.node.proxy_address.clone())
                .with_egress(egress_enabled),
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
    use tokio::signal::unix::{signal, SignalKind};
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;

    tokio::select! {
        _ = sigint.recv()  => {},
        _ = sigterm.recv() => {},
    }
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

            if let Err(e) = http2::Builder::new(TokioExecutor::new())
                .serve_connection(io, hyper_svc)
                .await
            {
                warn!(peer = %peer_addr, error = %e, "connection error");
            }
        });
    }
}
