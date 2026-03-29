use std::convert::Infallible;

use anyhow::Result;
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tracing::{info, info_span, warn, Instrument};

use crate::registry::{InboundRequest, ModuleRegistry};

/// Start the engine's inbound HTTP server.  The proxy forwards module-to-module
/// requests here; we route each request to the appropriate WASM module task
/// via the registry.
pub async fn serve(addr: &str, registry: ModuleRegistry) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!(address = %addr, "inbound server listening");

    loop {
        let (stream, _peer) = listener.accept().await?;
        let registry = registry.clone();

        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc = hyper::service::service_fn(move |req: Request<hyper::body::Incoming>| {
                let registry = registry.clone();
                async move {
                    let namespace = header_owned(req.headers(), "x-wr-namespace");
                    let module = header_owned(req.headers(), "x-wr-module");
                    let version = header_owned(req.headers(), "x-wr-version");
                    let method = req.method().to_string();
                    let path = req.uri().path().to_string();

                    let span = info_span!(
                        "engine.dispatch",
                        otel.name                 = format!("{method} {module}.{namespace}"),
                        wr.namespace              = %namespace,
                        wr.module                 = %module,
                        wr.version                = %version,
                        http.request.method       = %method,
                        url.path                  = %path,
                        http.response.status_code = tracing::field::Empty,
                        otel.status_code          = tracing::field::Empty,
                    );
                    wr_common::telemetry::set_parent_from_headers(&span, req.headers());

                    let resp = handle(req, registry).instrument(span.clone()).await;

                    let status = resp.status().as_u16();
                    span.record("http.response.status_code", status);
                    span.record(
                        "otel.status_code",
                        if status >= 400 { "ERROR" } else { "OK" },
                    );

                    Ok::<_, Infallible>(resp)
                }
            });
            if let Err(e) = http1::Builder::new().serve_connection(io, svc).await {
                warn!(error = %e, "inbound connection error");
            }
        });
    }
}

async fn handle(
    req: Request<hyper::body::Incoming>,
    registry: ModuleRegistry,
) -> Response<Full<Bytes>> {
    // The proxy injects x-wr-namespace, x-wr-module, and x-wr-version so we
    // know which module instance to dispatch to.
    let namespace = match req
        .headers()
        .get("x-wr-namespace")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
    {
        Some(n) => n,
        None => return err(StatusCode::BAD_REQUEST, "missing x-wr-namespace header"),
    };

    let module = match req
        .headers()
        .get("x-wr-module")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
    {
        Some(m) => m,
        None => return err(StatusCode::BAD_REQUEST, "missing x-wr-module header"),
    };

    let version = match req
        .headers()
        .get("x-wr-version")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
    {
        Some(v) => v,
        None => return err(StatusCode::BAD_REQUEST, "missing x-wr-version header"),
    };

    // Buffer body.
    let (parts, body) = req.into_parts();
    let bytes = match BodyExt::collect(body).await {
        Ok(c) => c.to_bytes(),
        Err(e) => {
            warn!(error = %e, "body read error");
            return err(StatusCode::INTERNAL_SERVER_ERROR, "failed to read body");
        }
    };

    let sender = match registry.next_sender(&namespace, &module, &version).await {
        Some(s) => s,
        None => {
            return err(
                StatusCode::NOT_FOUND,
                &format!("module '{module}.{namespace}@{version}' not loaded"),
            )
        }
    };

    let (resp_tx, resp_rx) = oneshot::channel();
    let inbound = InboundRequest {
        request: Request::from_parts(parts, bytes),
        response_tx: resp_tx,
        span: tracing::Span::current(),
    };

    use tokio::sync::mpsc::error::TrySendError;
    match sender.try_send(inbound) {
        Ok(()) => {}
        Err(TrySendError::Full(_)) => {
            return err(StatusCode::TOO_MANY_REQUESTS, "engine at capacity");
        }
        Err(TrySendError::Closed(_)) => {
            return err(StatusCode::SERVICE_UNAVAILABLE, "module channel closed");
        }
    }

    match resp_rx.await {
        Ok(resp) => {
            let (rp, rb) = resp.into_parts();
            Response::from_parts(rp, Full::new(rb))
        }
        Err(_) => err(StatusCode::INTERNAL_SERVER_ERROR, "module did not respond"),
    }
}

fn header_owned(headers: &http::HeaderMap, name: &str) -> String {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown")
        .to_owned()
}

fn err(status: StatusCode, msg: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .body(Full::new(Bytes::from(msg.to_string())))
        .unwrap()
}
