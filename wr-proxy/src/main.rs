mod config;
mod layers;
mod metrics;
mod routing;
mod schema;

use std::convert::Infallible;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::{combinators::BoxBody, Full};
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tower::{Service, ServiceBuilder};

use layers::{ForwardService, MetricsLayer, ResBody, RoutingLayer, SchemaValidationLayer};
use tracing::{error, info, warn};
use wr_common::wruntime::manager_service_client::ManagerServiceClient;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "proxy.toml".to_string());
    let config = config::ProxyConfig::load(&config_path)?;

    // ── Shared state ──────────────────────────────────────────────────────
    let routing_table = routing::new_routing_table();
    let schema_cache = Arc::new(schema::SchemaCache::new());
    let (metrics_tx, metrics_rx) = mpsc::channel(config.metrics.queue_depth);

    // ── Connect to wr-manager ─────────────────────────────────────────────
    let manager_client = ManagerServiceClient::connect(config.manager_address.clone()).await?;
    info!(address = %config.manager_address, "connected to manager");

    // ── Background tasks ──────────────────────────────────────────────────
    tokio::spawn(routing::sync_routing_table(
        manager_client.clone(),
        routing_table.clone(),
        config.cache.routing_table_ttl_secs,
    ));
    tokio::spawn(schema::sync_schemas(
        manager_client.clone(),
        routing_table.clone(),
        schema_cache.clone(),
        config.cache.schema_ttl_secs,
    ));
    tokio::spawn(metrics::flush_metrics(
        manager_client,
        metrics_rx,
        config.metrics.flush_interval_secs,
    ));

    // ── Tower service stack ───────────────────────────────────────────────
    //
    //   MetricsLayer
    //     └─ SchemaValidationLayer
    //          └─ RoutingLayer
    //               └─ ForwardService
    //
    let svc = ServiceBuilder::new()
        .layer(MetricsLayer::new(metrics_tx))
        .layer(SchemaValidationLayer::new(schema_cache))
        .layer(RoutingLayer::new(routing_table))
        .service(ForwardService::new());

    // ── TCP listener ──────────────────────────────────────────────────────
    let listener = TcpListener::bind(&config.listen_address).await?;
    info!(address = %config.listen_address, "proxy listening");

    loop {
        let (stream, peer_addr) = listener.accept().await?;
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
                                return Ok::<_, Infallible>(bad_request("failed to read body"));
                            }
                        };

                        match svc.call(Request::from_parts(parts, bytes)).await {
                            Ok(resp) => Ok::<_, Infallible>(resp),
                            Err(e) => {
                                error!(error = %e, "service error");
                                Ok(gateway_error("internal proxy error"))
                            }
                        }
                    }
                });

            if let Err(e) = http1::Builder::new().serve_connection(io, hyper_svc).await {
                warn!(peer = %peer_addr, error = %e, "connection error");
            }
        });
    }
}

fn bad_request(msg: &str) -> Response<ResBody> {
    Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .body(BoxBody::new(Full::new(Bytes::from(msg.to_string()))))
        .unwrap()
}

fn gateway_error(msg: &str) -> Response<ResBody> {
    Response::builder()
        .status(StatusCode::BAD_GATEWAY)
        .body(BoxBody::new(Full::new(Bytes::from(msg.to_string()))))
        .unwrap()
}
