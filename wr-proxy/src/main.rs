mod circuit_breaker;
mod config;
mod layers;
mod routing;
mod schema;

use std::convert::Infallible;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use hyper::server::conn::http2;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;
use tower::{Service, ServiceBuilder};

use layers::{
    EgressLayer, ForwardService, IngressLayer, ResBody, RoutingLayer, SchemaValidationLayer,
    TracingLayer,
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
    let schema_cache = Arc::new(schema::SchemaCache::new());
    let cb_registry = Arc::new(circuit_breaker::CircuitBreakerRegistry::new(
        config.circuit_breaker.clone(),
    ));

    // ── Connect to wr-manager ─────────────────────────────────────────────
    let manager_client = ManagerServiceClient::connect(config.manager_address.clone()).await?;
    info!(address = %config.manager_address, "connected to manager");

    // ── Background tasks ──────────────────────────────────────────────────
    let schema_trigger = Arc::new(tokio::sync::Notify::new());

    tokio::spawn(routing::sync_routing_table(
        manager_client.clone(),
        routing_table.clone(),
        config.cache.routing_table_ttl_secs,
        schema_trigger.clone(),
        cb_registry.clone(),
    ));
    tokio::spawn(schema::sync_schemas(
        manager_client.clone(),
        routing_table.clone(),
        schema_cache.clone(),
        config.cache.schema_ttl_secs,
        schema_trigger,
    ));
    // ── Internal Tower service stack ──────────────────────────────────────
    //
    //   TracingLayer               ← root OTel span per request
    //     └─ SchemaValidationLayer ← validates body; passes through uncached when
    //          |                     egress is enabled (external host, not a missing sync)
    //          └─ RoutingLayer     ← single routing table read; sets ExternalEgress
    //               └─ EgressLayer ← handles ExternalEgress; passes internal to forward
    //                    └─ ForwardService
    //
    let egress_enabled = config.egress.is_some();
    let internal_svc = ServiceBuilder::new()
        .layer(TracingLayer)
        .layer(SchemaValidationLayer::new(schema_cache.clone()).with_egress(egress_enabled))
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
    //          └─ SchemaValidationLayer
    //               └─ RoutingLayer
    //                    └─ ForwardService
    //
    if let Some(ext) = &config.external {
        // External traffic is plain HTTP, not protobuf-over-proxy, so schema
        // validation is omitted from this stack.
        let external_svc = ServiceBuilder::new()
            .layer(IngressLayer::new(ext.routes.clone(), schema_cache))
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
/// drives `svc` for each HTTP/1.1 request.
async fn accept_loop<S>(listener: TcpListener, svc: S)
where
    S: Service<Request<Bytes>, Response = Response<ResBody>> + Clone + Send + 'static,
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
                        let (parts, body) = req.into_parts();
                        let bytes = match http_body_util::BodyExt::collect(body).await {
                            Ok(c) => c.to_bytes(),
                            Err(e) => {
                                warn!(error = %e, "body read error");
                                return Ok::<_, Infallible>(layers::error_response(
                                    StatusCode::BAD_REQUEST,
                                    "failed to read body",
                                ));
                            }
                        };

                        match svc.call(Request::from_parts(parts, bytes)).await {
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
