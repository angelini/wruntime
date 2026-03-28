use std::convert::Infallible;

use anyhow::Result;
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::registry::{InboundRequest, ModuleRegistry};

/// Start the engine's inbound HTTP server.  The proxy forwards module-to-module
/// requests here; we route each request to the appropriate WASM module task
/// via the registry.
pub async fn serve(addr: &str, registry: ModuleRegistry) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    println!("[engine] inbound server listening on {addr}");

    loop {
        let (stream, _peer) = listener.accept().await?;
        let registry = registry.clone();

        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc = hyper::service::service_fn(
                move |req: Request<hyper::body::Incoming>| {
                    let registry = registry.clone();
                    async move { Ok::<_, Infallible>(handle(req, registry).await) }
                },
            );
            if let Err(e) = http1::Builder::new().serve_connection(io, svc).await {
                eprintln!("[engine] inbound connection error: {e}");
            }
        });
    }
}

async fn handle(
    req: Request<hyper::body::Incoming>,
    registry: ModuleRegistry,
) -> Response<Full<Bytes>> {
    // The proxy injects x-wr-module so we know which module to dispatch to.
    let module = match req
        .headers()
        .get("x-wr-module")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
    {
        Some(m) => m,
        None => return err(StatusCode::BAD_REQUEST, "missing x-wr-module header"),
    };

    // Buffer body.
    let (parts, body) = req.into_parts();
    let bytes = match BodyExt::collect(body).await {
        Ok(c)  => c.to_bytes(),
        Err(e) => {
            eprintln!("[engine] body read error: {e}");
            return err(StatusCode::INTERNAL_SERVER_ERROR, "failed to read body");
        }
    };

    let sender = match registry.sender(&module).await {
        Some(s) => s,
        None => return err(StatusCode::NOT_FOUND, &format!("module '{module}' not loaded")),
    };

    let (resp_tx, resp_rx) = oneshot::channel();
    let inbound = InboundRequest {
        request:     Request::from_parts(parts, bytes),
        response_tx: resp_tx,
    };

    if sender.send(inbound).await.is_err() {
        return err(StatusCode::SERVICE_UNAVAILABLE, "module channel closed");
    }

    match resp_rx.await {
        Ok(resp) => {
            let (rp, rb) = resp.into_parts();
            Response::from_parts(rp, Full::new(rb))
        }
        Err(_) => err(StatusCode::INTERNAL_SERVER_ERROR, "module did not respond"),
    }
}

fn err(status: StatusCode, msg: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .body(Full::new(Bytes::from(msg.to_string())))
        .unwrap()
}
