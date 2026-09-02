use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use deadpool_postgres::Pool;
use http::{Request, Response, StatusCode};
use http_body::Body;
use http_body_util::{BodyExt as _, Limited};
use prost::Message as _;
use serde_json::json;
use tracing::warn;

use wr_common::http_headers::{WR_MODULE, WR_NAMESPACE, WR_VERSION};
use wr_common::lifecycle::MAX_JOB_SUBMIT_MESSAGE_BYTES;
use wr_common::wruntime::{
    GetJobStatusRequest, GetJobStatusResponse, SubmitJobRequest, SubmitJobResponse,
};

pub const SUBMIT_JOB_PATH: &str = "/wruntime.WorkerService/SubmitJob";
pub const GET_JOB_STATUS_PATH: &str = "/wruntime.WorkerService/GetJobStatus";
const SHORT_SUBMIT_JOB_PATH: &str = "/SubmitJob";
const SHORT_GET_JOB_STATUS_PATH: &str = "/GetJobStatus";

pub fn canonical_worker_path(path: &str) -> Option<&'static str> {
    match path {
        SUBMIT_JOB_PATH | SHORT_SUBMIT_JOB_PATH => Some(SUBMIT_JOB_PATH),
        GET_JOB_STATUS_PATH | SHORT_GET_JOB_STATUS_PATH => Some(GET_JOB_STATUS_PATH),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug)]
struct WorkerPolicy {
    max_attempts: u32,
    timeout_secs: u32,
}

#[derive(Clone, Debug, Default)]
pub struct WorkerDefaults {
    policies: HashMap<wr_common::identity::ModuleId, WorkerPolicy>,
}

impl WorkerDefaults {
    pub fn from_modules(modules: &[crate::config::ModuleConfig]) -> anyhow::Result<Self> {
        let mut policies = HashMap::new();
        for module in modules {
            if module.mode == crate::config::ModuleMode::Worker {
                let id = wr_common::identity::ModuleId::parse(
                    &module.namespace,
                    &module.name,
                    &module.version,
                )?;
                policies.insert(
                    id,
                    WorkerPolicy {
                        max_attempts: module.worker_max_attempts,
                        timeout_secs: u32::try_from(module.worker_job_timeout_secs)
                            .unwrap_or(u32::MAX),
                    },
                );
            }
        }
        Ok(Self { policies })
    }

    #[doc(hidden)]
    pub fn with_policy(
        id: wr_common::identity::ModuleId,
        max_attempts: u32,
        timeout_secs: u32,
    ) -> Self {
        Self {
            policies: HashMap::from([(
                id,
                WorkerPolicy {
                    max_attempts,
                    timeout_secs,
                },
            )]),
        }
    }

    fn policy_for(&self, id: &wr_common::identity::ModuleId) -> WorkerPolicy {
        self.policies.get(id).copied().unwrap_or(WorkerPolicy {
            max_attempts: 3,
            timeout_secs: 300,
        })
    }
}

pub fn worker_error(status: StatusCode, message: &str) -> crate::EngineResponse {
    let body = json!({ "error": message });
    Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(crate::response_full(Bytes::from(body.to_string())))
        .unwrap()
}

/// Handle the production worker HTTP request path, including its encoded-body
/// limit, protobuf decoding, routed identity checks, and queue persistence.
pub async fn handle_worker_http_request<B>(
    request: Request<B>,
    db_pool: Option<Arc<Pool>>,
    worker_defaults: Arc<WorkerDefaults>,
) -> crate::EngineResponse
where
    B: Body<Data = Bytes>,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let path = request.uri().path().to_string();
    let header = |name: &str, missing: &str| {
        request
            .headers()
            .get(name)
            .ok_or_else(|| missing.to_string())?
            .to_str()
            .map_err(|_| format!("invalid {name} header"))
    };
    let routed = (|| {
        let namespace = header(WR_NAMESPACE, "missing x-wr-namespace header")?;
        let module = header(WR_MODULE, "missing x-wr-module header")?;
        let version = header(WR_VERSION, "missing x-wr-version header")?;
        wr_common::identity::ModuleId::parse(namespace, module, version)
            .map_err(|error| format!("invalid routed identity: {error}"))
    })()
    .ok();
    let routed_version = routed.as_ref().map(|id| id.version.to_string());
    let routed_namespace = routed.as_ref().map(|id| id.route.namespace.as_str());
    let routed_module = routed.as_ref().map(|id| id.route.module.as_str());
    let body = match Limited::new(request.into_body(), MAX_JOB_SUBMIT_MESSAGE_BYTES)
        .collect()
        .await
    {
        Ok(collected) => collected.to_bytes(),
        Err(error) => {
            warn!(error = %error, "worker grpc body read error");
            return worker_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "worker request body is malformed or exceeds the configured job limit",
            );
        }
    };

    handle_worker_grpc_bytes(
        &path,
        body,
        db_pool,
        routed_namespace,
        routed_module,
        routed_version.as_deref(),
        worker_defaults.as_ref(),
    )
    .await
}

pub async fn handle_worker_grpc_bytes(
    path: &str,
    body: Bytes,
    db_pool: Option<Arc<Pool>>,
    routed_namespace: Option<&str>,
    routed_module: Option<&str>,
    routed_version: Option<&str>,
    worker_defaults: &WorkerDefaults,
) -> crate::EngineResponse {
    let path = match canonical_worker_path(path) {
        Some(path) => path,
        None => return worker_error(StatusCode::NOT_FOUND, "unknown worker endpoint"),
    };

    let pool = match db_pool {
        Some(pool) => pool,
        None => return worker_error(StatusCode::SERVICE_UNAVAILABLE, "no database configured"),
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
) -> crate::EngineResponse {
    let request = match SubmitJobRequest::decode(body) {
        Ok(request) => request,
        Err(error) => return worker_error(StatusCode::BAD_REQUEST, &format!("decode: {error}")),
    };

    let (routed_namespace, routed_module) = match (routed_namespace, routed_module) {
        (Some(namespace), Some(module)) => (namespace, module),
        _ => {
            return worker_error(
                StatusCode::BAD_REQUEST,
                "missing routed worker identity headers",
            )
        }
    };
    if request.worker_namespace != routed_namespace || request.worker_name != routed_module {
        return worker_error(
            StatusCode::BAD_REQUEST,
            "SubmitJobRequest worker identity does not match routed destination",
        );
    }

    if !request.worker_version.is_empty() {
        if let Some(routed_version) = routed_version {
            if routed_version != request.worker_version {
                return worker_error(
                    StatusCode::BAD_REQUEST,
                    "x-wr-version does not match SubmitJobRequest.worker_version",
                );
            }
        }
    }

    let defaults_version = if request.worker_version.is_empty() {
        routed_version.unwrap_or_default()
    } else {
        &request.worker_version
    };
    let policy = wr_common::identity::ModuleId::parse(
        &request.worker_namespace,
        &request.worker_name,
        defaults_version,
    )
    .ok()
    .map(|id| worker_defaults.policy_for(&id))
    .unwrap_or(WorkerPolicy {
        max_attempts: 3,
        timeout_secs: 300,
    });
    let max_attempts = if request.max_attempts > 0 {
        request.max_attempts
    } else {
        policy.max_attempts
    };
    let timeout_secs = if request.timeout_secs > 0 {
        request.timeout_secs
    } else {
        policy.timeout_secs
    };

    match crate::worker::insert_job(
        pool,
        &request.worker_namespace,
        &request.worker_name,
        &request.worker_version,
        &request.job_type,
        &request.payload,
        timeout_secs,
        max_attempts,
        "",
        "",
    )
    .await
    {
        Ok(job_id) => {
            let response = SubmitJobResponse { job_id };
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/x-protobuf")
                .body(crate::response_full(Bytes::from(response.encode_to_vec())))
                .unwrap()
        }
        Err(error) => {
            warn!(error = %error, "submit job failed");
            worker_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("insert: {error}"),
            )
        }
    }
}

async fn handle_get_job_status(pool: &Pool, body: &[u8]) -> crate::EngineResponse {
    let request = match GetJobStatusRequest::decode(body) {
        Ok(request) => request,
        Err(error) => return worker_error(StatusCode::BAD_REQUEST, &format!("decode: {error}")),
    };

    match crate::worker::get_job_status(pool, &request.job_id).await {
        Ok(Some(status)) => {
            let response = GetJobStatusResponse {
                job_id: status.job_id,
                status: match status.status {
                    wr_common::lifecycle::JobState::Pending => {
                        wr_common::wruntime::JobState::Pending as i32
                    }
                    wr_common::lifecycle::JobState::Running => {
                        wr_common::wruntime::JobState::Running as i32
                    }
                    wr_common::lifecycle::JobState::Complete => {
                        wr_common::wruntime::JobState::Complete as i32
                    }
                    wr_common::lifecycle::JobState::Dead => {
                        wr_common::wruntime::JobState::Dead as i32
                    }
                },
                result: status.result,
                error_message: status.error_message,
                attempt: status.attempt.get(),
                max_attempts: status.max_attempts.get(),
            };
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/x-protobuf")
                .body(crate::response_full(Bytes::from(response.encode_to_vec())))
                .unwrap()
        }
        Ok(None) => worker_error(StatusCode::NOT_FOUND, "job not found"),
        Err(error) => {
            warn!(error = %error, "get job status failed");
            worker_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("query: {error}"),
            )
        }
    }
}
