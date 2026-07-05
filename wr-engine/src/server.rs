use std::convert::Infallible;
use std::sync::Arc;

use serde_json::json;

use anyhow::Result;
use bytes::Bytes;
use deadpool_postgres::Pool;
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::server::conn::http2;
use hyper_util::rt::{TokioExecutor, TokioIo};
use prost::Message;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tracing::{info, info_span, warn, Instrument};

use crate::registry::{InboundRequest, ModuleRegistry};
use wr_common::wruntime::{
    GetJobStatusRequest, GetJobStatusResponse, SubmitJobRequest, SubmitJobResponse,
};

/// Start the engine's inbound HTTP server.  The proxy forwards module-to-module
/// requests here; we route each request to the appropriate WASM module task
/// via the registry.
pub async fn serve(addr: &str, registry: ModuleRegistry, db_pool: Option<Arc<Pool>>) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!(address = %addr, "inbound server listening");

    loop {
        let (stream, _peer) = listener.accept().await?;
        let registry = registry.clone();
        let db_pool = db_pool.clone();

        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc = hyper::service::service_fn(move |req: Request<hyper::body::Incoming>| {
                let registry = registry.clone();
                let db_pool = db_pool.clone();
                async move {
                    let namespace = header_owned(req.headers(), WR_NAMESPACE);
                    let module = header_owned(req.headers(), WR_MODULE);
                    let version = header_owned(req.headers(), WR_VERSION);
                    let method = req.method().to_string();
                    let path = req.uri().path().to_string();

                    let span = info_span!(
                        "engine.dispatch",
                        otel.name                 = format!("{method} {namespace}.{module}"),
                        wr.namespace              = %namespace,
                        wr.module                 = %module,
                        wr.version                = %version,
                        http.request.method       = %method,
                        url.path                  = %path,
                        http.response.status_code = tracing::field::Empty,
                        otel.status_code          = tracing::field::Empty,
                    );
                    wr_common::telemetry::set_parent_from_headers(&span, req.headers());

                    let resp = handle(req, registry, db_pool)
                        .instrument(span.clone())
                        .await;

                    let status = resp.status().as_u16();
                    span.record("http.response.status_code", status);
                    span.record(
                        "otel.status_code",
                        if status >= 400 { "ERROR" } else { "OK" },
                    );

                    Ok::<_, Infallible>(resp)
                }
            });
            if let Err(e) = http2::Builder::new(TokioExecutor::new())
                .serve_connection(io, svc)
                .await
            {
                warn!(error = %e, "inbound connection error");
            }
        });
    }
}

async fn handle(
    req: Request<hyper::body::Incoming>,
    registry: ModuleRegistry,
    db_pool: Option<Arc<Pool>>,
) -> Response<Full<Bytes>> {
    // ── Health check — no headers required ────────────────────────────────
    if req.uri().path() == "/healthz" {
        return Response::builder()
            .status(StatusCode::OK)
            .body(Full::new(Bytes::from("ok")))
            .unwrap();
    }

    // ── Worker job queue gRPC endpoints ──────────────────────────────────
    if req.uri().path() == "/SubmitJob" || req.uri().path() == "/GetJobStatus" {
        let path = req.uri().path().to_owned();
        return handle_worker_grpc(req, &path, db_pool).await;
    }

    // The proxy injects x-wr-namespace, x-wr-module, and x-wr-version so we
    // know which module instance to dispatch to.
    let namespace = match req
        .headers()
        .get(WR_NAMESPACE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
    {
        Some(n) => n,
        None => return err(StatusCode::BAD_REQUEST, "missing x-wr-namespace header"),
    };

    let module = match req
        .headers()
        .get(WR_MODULE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
    {
        Some(m) => m,
        None => return err(StatusCode::BAD_REQUEST, "missing x-wr-module header"),
    };

    let version = match req
        .headers()
        .get(WR_VERSION)
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
            return too_many_requests(&module);
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

use wr_common::http_headers::{header_owned, WR_MODULE, WR_NAMESPACE, WR_VERSION};

fn too_many_requests(module: &str) -> Response<Full<Bytes>> {
    let body = json!({
        "error": "too_many_requests",
        "reason": "module channel at capacity",
        "module": module,
    });
    Response::builder()
        .status(StatusCode::TOO_MANY_REQUESTS)
        .header(http::header::CONTENT_TYPE, "application/json")
        .header("Retry-After", "1")
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap()
}

fn err(status: StatusCode, msg: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .body(Full::new(Bytes::from(msg.to_string())))
        .unwrap()
}

async fn handle_worker_grpc(
    req: Request<hyper::body::Incoming>,
    path: &str,
    db_pool: Option<Arc<Pool>>,
) -> Response<Full<Bytes>> {
    let pool = match db_pool {
        Some(p) => p,
        None => return err(StatusCode::SERVICE_UNAVAILABLE, "no database configured"),
    };

    let body = match BodyExt::collect(req.into_body()).await {
        Ok(c) => c.to_bytes(),
        Err(e) => {
            warn!(error = %e, "worker grpc body read error");
            return err(StatusCode::BAD_REQUEST, "failed to read body");
        }
    };

    match path {
        "/SubmitJob" => handle_submit_job(&pool, &body).await,
        "/GetJobStatus" => handle_get_job_status(&pool, &body).await,
        _ => err(StatusCode::NOT_FOUND, "unknown worker endpoint"),
    }
}

async fn handle_submit_job(pool: &Pool, body: &[u8]) -> Response<Full<Bytes>> {
    let req = match SubmitJobRequest::decode(body) {
        Ok(r) => r,
        Err(e) => return err(StatusCode::BAD_REQUEST, &format!("decode: {e}")),
    };

    match wr_engine::worker::insert_job(
        pool,
        &req.worker_namespace,
        &req.worker_name,
        &req.worker_version,
        &req.job_type,
        &req.payload,
        req.timeout_secs,
        req.max_attempts,
        "", // source_namespace (not available on this path)
        "", // source_module (not available on this path)
    )
    .await
    {
        Ok(job_id) => {
            let resp = SubmitJobResponse { job_id };
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/x-protobuf")
                .body(Full::new(Bytes::from(resp.encode_to_vec())))
                .unwrap()
        }
        Err(e) => {
            warn!(error = %e, "submit job failed");
            err(StatusCode::INTERNAL_SERVER_ERROR, &format!("insert: {e}"))
        }
    }
}

async fn handle_get_job_status(pool: &Pool, body: &[u8]) -> Response<Full<Bytes>> {
    let req = match GetJobStatusRequest::decode(body) {
        Ok(r) => r,
        Err(e) => return err(StatusCode::BAD_REQUEST, &format!("decode: {e}")),
    };

    match wr_engine::worker::get_job_status(pool, &req.job_id).await {
        Ok(Some(status)) => {
            let resp = GetJobStatusResponse {
                job_id: status.job_id,
                status: status.status,
                result: status.result,
                error_message: status.error_message,
                attempt: status.attempt,
                max_attempts: status.max_attempts,
            };
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/x-protobuf")
                .body(Full::new(Bytes::from(resp.encode_to_vec())))
                .unwrap()
        }
        Ok(None) => err(StatusCode::NOT_FOUND, "job not found"),
        Err(e) => {
            warn!(error = %e, "get job status failed");
            err(StatusCode::INTERNAL_SERVER_ERROR, &format!("query: {e}"))
        }
    }
}
