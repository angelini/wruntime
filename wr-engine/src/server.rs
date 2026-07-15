use std::collections::HashMap;
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

const WORKER_SERVICE_PREFIX: &str = "/wruntime.WorkerService/";
const SUBMIT_JOB_PATH: &str = "/wruntime.WorkerService/SubmitJob";
const GET_JOB_STATUS_PATH: &str = "/wruntime.WorkerService/GetJobStatus";
const SHORT_SUBMIT_JOB_PATH: &str = "/SubmitJob";
const SHORT_GET_JOB_STATUS_PATH: &str = "/GetJobStatus";

fn canonical_worker_path(path: &str) -> Option<&'static str> {
    match path {
        SUBMIT_JOB_PATH | SHORT_SUBMIT_JOB_PATH => Some(SUBMIT_JOB_PATH),
        GET_JOB_STATUS_PATH | SHORT_GET_JOB_STATUS_PATH => Some(GET_JOB_STATUS_PATH),
        _ => None,
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct WorkerDefaults {
    max_attempts: HashMap<(String, String, String), i32>,
}

impl WorkerDefaults {
    pub(crate) fn from_modules(modules: &[wr_engine::config::ModuleConfig]) -> Self {
        let mut max_attempts = HashMap::new();
        for module in modules {
            if module.mode == wr_engine::config::ModuleMode::Worker {
                max_attempts.insert(
                    (
                        module.namespace.clone(),
                        module.name.clone(),
                        module.version.clone(),
                    ),
                    module.worker_max_attempts,
                );
            }
        }
        Self { max_attempts }
    }

    fn max_attempts_for(&self, namespace: &str, name: &str, version: &str) -> i32 {
        self.max_attempts
            .get(&(namespace.to_owned(), name.to_owned(), version.to_owned()))
            .copied()
            .filter(|attempts| *attempts > 0)
            .unwrap_or(3)
    }
}

/// Start the engine's inbound HTTP server.  The proxy forwards module-to-module
/// requests here; we route each request to the appropriate WASM module task
/// via the registry.
pub async fn serve(
    addr: &str,
    registry: ModuleRegistry,
    db_pool: Option<Arc<Pool>>,
    worker_defaults: Arc<WorkerDefaults>,
) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!(address = %addr, "inbound server listening");

    loop {
        let (stream, _peer) = listener.accept().await?;
        let registry = registry.clone();
        let db_pool = db_pool.clone();
        let worker_defaults = worker_defaults.clone();

        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc = hyper::service::service_fn(move |req: Request<hyper::body::Incoming>| {
                let registry = registry.clone();
                let db_pool = db_pool.clone();
                let worker_defaults = worker_defaults.clone();
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

                    let resp = handle(req, registry, db_pool, worker_defaults)
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
    worker_defaults: Arc<WorkerDefaults>,
) -> Response<Full<Bytes>> {
    // ── Health check — no headers required ────────────────────────────────
    if req.uri().path() == "/healthz" {
        return Response::builder()
            .status(StatusCode::OK)
            .body(Full::new(Bytes::from("ok")))
            .unwrap();
    }

    // ── Worker job queue gRPC endpoints ──────────────────────────────────
    let request_path = req.uri().path();
    if request_path.starts_with(WORKER_SERVICE_PREFIX)
        || canonical_worker_path(request_path).is_some()
    {
        let path = request_path.to_owned();
        return handle_worker_grpc(req, &path, db_pool, worker_defaults).await;
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

fn worker_err(status: StatusCode, msg: &str) -> Response<Full<Bytes>> {
    let body = json!({ "error": msg });
    Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap()
}

async fn handle_worker_grpc(
    req: Request<hyper::body::Incoming>,
    path: &str,
    db_pool: Option<Arc<Pool>>,
    worker_defaults: Arc<WorkerDefaults>,
) -> Response<Full<Bytes>> {
    let routed_namespace = req
        .headers()
        .get(WR_NAMESPACE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let routed_module = req
        .headers()
        .get(WR_MODULE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let routed_version = req
        .headers()
        .get(WR_VERSION)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let body = match BodyExt::collect(req.into_body()).await {
        Ok(c) => c.to_bytes(),
        Err(e) => {
            warn!(error = %e, "worker grpc body read error");
            return worker_err(StatusCode::BAD_REQUEST, "failed to read body");
        }
    };

    handle_worker_grpc_bytes(
        path,
        body,
        db_pool,
        routed_namespace.as_deref(),
        routed_module.as_deref(),
        routed_version.as_deref(),
        worker_defaults.as_ref(),
    )
    .await
}

async fn handle_worker_grpc_bytes(
    path: &str,
    body: Bytes,
    db_pool: Option<Arc<Pool>>,
    routed_namespace: Option<&str>,
    routed_module: Option<&str>,
    routed_version: Option<&str>,
    worker_defaults: &WorkerDefaults,
) -> Response<Full<Bytes>> {
    let path = match canonical_worker_path(path) {
        Some(path) => path,
        None => return worker_err(StatusCode::NOT_FOUND, "unknown worker endpoint"),
    };

    let pool = match db_pool {
        Some(p) => p,
        None => return worker_err(StatusCode::SERVICE_UNAVAILABLE, "no database configured"),
    };

    match path {
        SUBMIT_JOB_PATH => {
            handle_submit_job(
                &pool,
                &body,
                routed_namespace,
                routed_module,
                routed_version,
                worker_defaults,
            )
            .await
        }
        GET_JOB_STATUS_PATH => handle_get_job_status(&pool, &body).await,
        _ => unreachable!("worker endpoint path checked above"),
    }
}

async fn handle_submit_job(
    pool: &Pool,
    body: &[u8],
    routed_namespace: Option<&str>,
    routed_module: Option<&str>,
    routed_version: Option<&str>,
    worker_defaults: &WorkerDefaults,
) -> Response<Full<Bytes>> {
    let req = match SubmitJobRequest::decode(body) {
        Ok(r) => r,
        Err(e) => return worker_err(StatusCode::BAD_REQUEST, &format!("decode: {e}")),
    };

    let (routed_namespace, routed_module) = match (routed_namespace, routed_module) {
        (Some(namespace), Some(module)) => (namespace, module),
        _ => {
            return worker_err(
                StatusCode::BAD_REQUEST,
                "missing routed worker identity headers",
            )
        }
    };
    if req.worker_namespace != routed_namespace || req.worker_name != routed_module {
        return worker_err(
            StatusCode::BAD_REQUEST,
            "SubmitJobRequest worker identity does not match routed destination",
        );
    }

    if !req.worker_version.is_empty() {
        if let Some(routed_version) = routed_version {
            if routed_version != req.worker_version {
                return worker_err(
                    StatusCode::BAD_REQUEST,
                    "x-wr-version does not match SubmitJobRequest.worker_version",
                );
            }
        }
    }

    let defaults_version = if req.worker_version.is_empty() {
        routed_version.unwrap_or_default()
    } else {
        &req.worker_version
    };

    let max_attempts = if req.max_attempts > 0 {
        req.max_attempts
    } else {
        worker_defaults.max_attempts_for(&req.worker_namespace, &req.worker_name, defaults_version)
    };

    match wr_engine::worker::insert_job(
        pool,
        &req.worker_namespace,
        &req.worker_name,
        &req.worker_version,
        &req.job_type,
        &req.payload,
        req.timeout_secs,
        max_attempts,
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
            worker_err(StatusCode::INTERNAL_SERVER_ERROR, &format!("insert: {e}"))
        }
    }
}

async fn handle_get_job_status(pool: &Pool, body: &[u8]) -> Response<Full<Bytes>> {
    let req = match GetJobStatusRequest::decode(body) {
        Ok(r) => r,
        Err(e) => return worker_err(StatusCode::BAD_REQUEST, &format!("decode: {e}")),
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
        Ok(None) => worker_err(StatusCode::NOT_FOUND, "job not found"),
        Err(e) => {
            warn!(error = %e, "get job status failed");
            worker_err(StatusCode::INTERNAL_SERVER_ERROR, &format!("query: {e}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn db_url() -> Option<String> {
        std::env::var("WRT_TEST_DB_URL").ok()
    }

    async fn test_pool() -> Option<Arc<Pool>> {
        use tokio::sync::OnceCell;
        static PROVISIONED: OnceCell<()> = OnceCell::const_new();
        let url = db_url()?;
        let pool = wr_engine::pool::build_pool(&url, 2).expect("build pool");
        PROVISIONED
            .get_or_init(|| async {
                wr_engine::worker::provision_job_schema(&pool)
                    .await
                    .expect("provision schema");
            })
            .await;
        Some(Arc::new(pool))
    }

    fn unique_prefix() -> String {
        format!(
            "srv_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )
    }

    fn submit_body(namespace: &str, name: &str, version: &str, max_attempts: i32) -> Bytes {
        Bytes::from(
            SubmitJobRequest {
                worker_namespace: namespace.into(),
                worker_name: name.into(),
                worker_version: version.into(),
                job_type: "/test/Run".into(),
                payload: b"payload".to_vec(),
                timeout_secs: 0,
                max_attempts,
            }
            .encode_to_vec(),
        )
    }

    async fn response_json(resp: Response<Full<Bytes>>) -> (StatusCode, serde_json::Value) {
        let status = resp.status();
        assert_eq!(
            resp.headers().get(http::header::CONTENT_TYPE).unwrap(),
            "application/json"
        );
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&body).unwrap())
    }

    #[tokio::test]
    async fn worker_err_returns_json() {
        let (status, value) =
            response_json(worker_err(StatusCode::BAD_REQUEST, "decode: \"bad\"")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(value, json!({"error": "decode: \"bad\""}));
    }

    #[tokio::test]
    async fn unknown_worker_endpoint_is_json_404() {
        let (status, value) = response_json(
            handle_worker_grpc_bytes(
                "/wruntime.WorkerService/Nope",
                Bytes::new(),
                None,
                None,
                None,
                None,
                &WorkerDefaults::default(),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(value, json!({"error": "unknown worker endpoint"}));
    }

    #[tokio::test]
    async fn canonical_and_short_worker_paths_require_a_database() {
        for path in [
            SUBMIT_JOB_PATH,
            GET_JOB_STATUS_PATH,
            SHORT_SUBMIT_JOB_PATH,
            SHORT_GET_JOB_STATUS_PATH,
        ] {
            let (status, value) = response_json(
                handle_worker_grpc_bytes(
                    path,
                    Bytes::new(),
                    None,
                    None,
                    None,
                    None,
                    &WorkerDefaults::default(),
                )
                .await,
            )
            .await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{path}");
            assert_eq!(value, json!({"error": "no database configured"}), "{path}");
        }
    }

    #[tokio::test]
    async fn submit_accepts_empty_body_worker_version() {
        let Some(pool) = test_pool().await else {
            eprintln!("skipping (no WRT_TEST_DB_URL)");
            return;
        };
        let ns = unique_prefix();
        let defaults = WorkerDefaults {
            max_attempts: HashMap::from([(
                (ns.clone(), "mod".to_string(), "1.0.0".to_string()),
                7,
            )]),
        };
        let resp = handle_worker_grpc_bytes(
            SUBMIT_JOB_PATH,
            submit_body(&ns, "mod", "", 0),
            Some(pool.clone()),
            Some(&ns),
            Some("mod"),
            Some("1.0.0"),
            &defaults,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let job_id = SubmitJobResponse::decode(&body[..]).unwrap().job_id;
        let client = pool.get().await.unwrap();
        let row = client
            .query_one(
                "SELECT worker_version, max_attempts FROM wr__jobs.jobs WHERE job_id = $1",
                &[&job_id],
            )
            .await
            .unwrap();
        assert_eq!(row.get::<_, String>(0), "");
        assert_eq!(row.get::<_, i32>(1), 7);
    }

    #[tokio::test]
    async fn submit_requires_matching_routed_worker_identity() {
        let Some(pool) = test_pool().await else {
            eprintln!("skipping (no WRT_TEST_DB_URL)");
            return;
        };
        let ns = unique_prefix();

        let (status, value) = response_json(
            handle_worker_grpc_bytes(
                SUBMIT_JOB_PATH,
                submit_body(&ns, "mod", "", 0),
                Some(pool.clone()),
                None,
                None,
                Some("1.0.0"),
                &WorkerDefaults::default(),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            value,
            json!({"error": "missing routed worker identity headers"})
        );

        let (status, value) = response_json(
            handle_worker_grpc_bytes(
                SUBMIT_JOB_PATH,
                submit_body("other", "mod", "", 0),
                Some(pool),
                Some(&ns),
                Some("mod"),
                Some("1.0.0"),
                &WorkerDefaults::default(),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            value,
            json!({"error": "SubmitJobRequest worker identity does not match routed destination"})
        );
    }

    #[tokio::test]
    async fn submit_rejects_header_body_version_mismatch() {
        let Some(pool) = test_pool().await else {
            eprintln!("skipping (no WRT_TEST_DB_URL)");
            return;
        };
        let ns = unique_prefix();
        let (status, value) = response_json(
            handle_worker_grpc_bytes(
                SUBMIT_JOB_PATH,
                submit_body(&ns, "mod", "1.0.0", 0),
                Some(pool),
                Some(&ns),
                Some("mod"),
                Some("2.0.0"),
                &WorkerDefaults::default(),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            value,
            json!({"error": "x-wr-version does not match SubmitJobRequest.worker_version"})
        );
    }

    #[tokio::test]
    async fn submit_uses_configured_worker_max_attempts_when_request_zero() {
        let Some(pool) = test_pool().await else {
            eprintln!("skipping (no WRT_TEST_DB_URL)");
            return;
        };
        let ns = unique_prefix();
        let defaults = WorkerDefaults {
            max_attempts: HashMap::from([(
                (ns.clone(), "mod".to_string(), "1.0.0".to_string()),
                7,
            )]),
        };

        let resp = handle_worker_grpc_bytes(
            SUBMIT_JOB_PATH,
            submit_body(&ns, "mod", "1.0.0", 0),
            Some(pool.clone()),
            Some(&ns),
            Some("mod"),
            Some("1.0.0"),
            &defaults,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let job_id = SubmitJobResponse::decode(&body[..]).unwrap().job_id;
        let status = wr_engine::worker::get_job_status(&pool, &job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.max_attempts, 7);

        let resp = handle_worker_grpc_bytes(
            SUBMIT_JOB_PATH,
            submit_body(&ns, "mod", "1.0.0", 4),
            Some(pool.clone()),
            Some(&ns),
            Some("mod"),
            Some("1.0.0"),
            &defaults,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let job_id = SubmitJobResponse::decode(&body[..]).unwrap().job_id;
        let status = wr_engine::worker::get_job_status(&pool, &job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.max_attempts, 4);
    }

    #[test]
    fn short_worker_paths_are_compatibility_aliases() {
        assert_eq!(
            canonical_worker_path(SHORT_SUBMIT_JOB_PATH),
            Some(SUBMIT_JOB_PATH)
        );
        assert_eq!(
            canonical_worker_path(SHORT_GET_JOB_STATUS_PATH),
            Some(GET_JOB_STATUS_PATH)
        );
    }
}
