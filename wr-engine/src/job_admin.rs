use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use prost_types::Timestamp;
use tonic::{Request, Response, Status};

use wr_common::lifecycle::JobState as DomainJobState;
use wr_common::lifecycle_service::{AdmissionGate, AdmissionGuard};
use wr_common::wruntime::engine_job_admin_service_server::EngineJobAdminService;
use wr_common::wruntime::{
    CheckJobQueueRequest, CheckJobQueueResponse, GetJobQueueSummaryRequest,
    GetJobQueueSummaryResponse, GetJobRequest, GetJobResponse, JobDetail, JobFilter,
    JobQueueSummary, JobState, JobSummary, ListJobsRequest, ListJobsResponse, RetryJobRequest,
    RetryJobResponse,
};

use crate::job_admin_queue::{self, JobAdminError};

#[derive(Clone)]
pub struct EngineJobAdminApi {
    queue_id: String,
    pool: Arc<Pool>,
    ready: Arc<AtomicBool>,
    admission: AdmissionGate,
}

impl EngineJobAdminApi {
    pub fn new(
        queue_id: String,
        pool: Arc<Pool>,
        ready: Arc<AtomicBool>,
        admission: AdmissionGate,
    ) -> Self {
        Self {
            queue_id,
            pool,
            ready,
            admission,
        }
    }

    fn require_queue(&self, queue_id: &str) -> Result<AdmissionGuard, Status> {
        if queue_id != self.queue_id {
            return Err(Status::invalid_argument(
                "job_queue_id does not match this engine's configured queue",
            ));
        }
        if !self.ready.load(Ordering::Acquire) {
            return Err(Status::unavailable("job queue migrations are not complete"));
        }
        self.admission
            .try_enter()
            .ok_or_else(|| Status::unavailable("job administration is stopping"))
    }
}

fn domain_state(value: i32) -> Result<DomainJobState, Status> {
    match JobState::try_from(value) {
        Ok(JobState::Pending) => Ok(DomainJobState::Pending),
        Ok(JobState::Running) => Ok(DomainJobState::Running),
        Ok(JobState::Complete) => Ok(DomainJobState::Complete),
        Ok(JobState::Dead) => Ok(DomainJobState::Dead),
        _ => Err(Status::invalid_argument("unsupported job status")),
    }
}

fn wire_state(value: DomainJobState) -> i32 {
    match value {
        DomainJobState::Pending => JobState::Pending as i32,
        DomainJobState::Running => JobState::Running as i32,
        DomainJobState::Complete => JobState::Complete as i32,
        DomainJobState::Dead => JobState::Dead as i32,
    }
}

fn from_timestamp(value: Timestamp, field: &str) -> Result<DateTime<Utc>, Status> {
    let nanos = u32::try_from(value.nanos)
        .ok()
        .filter(|value| *value < 1_000_000_000)
        .ok_or_else(|| Status::invalid_argument(format!("{field} is malformed")))?;
    DateTime::from_timestamp(value.seconds, nanos)
        .ok_or_else(|| Status::invalid_argument(format!("{field} is out of range")))
}

fn to_timestamp(value: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: value.timestamp(),
        nanos: value.timestamp_subsec_nanos() as i32,
    }
}

fn convert_filter(filter: Option<JobFilter>) -> Result<job_admin_queue::JobFilter, Status> {
    let filter = filter.unwrap_or_default();
    let nonempty = |value: String| (!value.is_empty()).then_some(value);
    Ok(job_admin_queue::JobFilter {
        worker_namespace: nonempty(filter.worker_namespace),
        worker_name: nonempty(filter.worker_name),
        worker_version: nonempty(filter.worker_version),
        job_type: nonempty(filter.job_type),
        source_namespace: nonempty(filter.source_namespace),
        source_module: nonempty(filter.source_module),
        created_at_from: filter
            .created_at_from
            .map(|value| from_timestamp(value, "created_at_from"))
            .transpose()?,
        created_at_before: filter
            .created_at_before
            .map(|value| from_timestamp(value, "created_at_before"))
            .transpose()?,
    })
}

fn map_error(error: JobAdminError) -> Status {
    match error {
        JobAdminError::Invalid(message) => Status::invalid_argument(message),
        JobAdminError::NotFound => Status::not_found("job not found"),
        JobAdminError::FailedPrecondition(message) => Status::failed_precondition(message),
        JobAdminError::Database(error) => {
            tracing::error!(%error, "job administration database operation failed");
            Status::internal("job queue database operation failed")
        }
    }
}

fn job_summary(value: job_admin_queue::JobListItem) -> JobSummary {
    JobSummary {
        job_id: value.job_id,
        worker_namespace: value.worker_namespace,
        worker_name: value.worker_name,
        worker_version: value.worker_version,
        job_type: value.job_type,
        status: wire_state(value.status),
        attempt: value.attempt.get(),
        max_attempts: value.max_attempts.get(),
        source_namespace: value.source_namespace,
        source_module: value.source_module,
        created_at: Some(to_timestamp(value.created_at)),
        updated_at: Some(to_timestamp(value.updated_at)),
    }
}

fn job_detail(value: job_admin_queue::JobDetail) -> JobDetail {
    let summary = value.summary;
    JobDetail {
        job_id: summary.job_id,
        worker_namespace: summary.worker_namespace,
        worker_name: summary.worker_name,
        worker_version: summary.worker_version,
        job_type: summary.job_type,
        status: wire_state(summary.status),
        payload: value.payload,
        result: value.result,
        last_error: value.last_error,
        attempt: summary.attempt.get(),
        max_attempts: summary.max_attempts.get(),
        timeout_secs: value.timeout.get(),
        source_namespace: summary.source_namespace,
        source_module: summary.source_module,
        created_at: Some(to_timestamp(summary.created_at)),
        updated_at: Some(to_timestamp(summary.updated_at)),
        claimed_at: value.claimed_at.map(to_timestamp),
        lease_expires_at: value.lease_expires_at.map(to_timestamp),
        completed_at: value.completed_at.map(to_timestamp),
        claimed_by: value.claimed_by,
    }
}

#[tonic::async_trait]
impl EngineJobAdminService for EngineJobAdminApi {
    async fn check_job_queue(
        &self,
        request: Request<CheckJobQueueRequest>,
    ) -> Result<Response<CheckJobQueueResponse>, Status> {
        let _admission = self.require_queue(&request.into_inner().job_queue_id)?;
        Ok(Response::new(CheckJobQueueResponse { ready: true }))
    }

    async fn list_jobs(
        &self,
        request: Request<ListJobsRequest>,
    ) -> Result<Response<ListJobsResponse>, Status> {
        let request = request.into_inner();
        let _admission = self.require_queue(&request.job_queue_id)?;
        let filter = convert_filter(request.filter)?;
        let status = request.status.map(domain_state).transpose()?;
        let page = job_admin_queue::list_jobs(
            &self.pool,
            &filter,
            status,
            request.page_size,
            &request.cursor,
        )
        .await
        .map_err(map_error)?;
        Ok(Response::new(ListJobsResponse {
            jobs: page.jobs.into_iter().map(job_summary).collect(),
            next_cursor: page.next_cursor.unwrap_or_default(),
        }))
    }

    async fn get_job_queue_summary(
        &self,
        request: Request<GetJobQueueSummaryRequest>,
    ) -> Result<Response<GetJobQueueSummaryResponse>, Status> {
        let request = request.into_inner();
        let _admission = self.require_queue(&request.job_queue_id)?;
        let filter = convert_filter(request.filter)?;
        let summary = job_admin_queue::summarize_jobs(&self.pool, &filter)
            .await
            .map_err(map_error)?;
        Ok(Response::new(GetJobQueueSummaryResponse {
            summary: Some(JobQueueSummary {
                observed_at: Some(to_timestamp(summary.observed_at)),
                pending: summary.pending,
                running: summary.running,
                complete: summary.complete,
                dead: summary.dead,
                total: summary.total,
                depth: summary.pending,
                oldest_pending_at: summary.oldest_pending_at.map(to_timestamp),
            }),
        }))
    }

    async fn get_job(
        &self,
        request: Request<GetJobRequest>,
    ) -> Result<Response<GetJobResponse>, Status> {
        let request = request.into_inner();
        let _admission = self.require_queue(&request.job_queue_id)?;
        let job = job_admin_queue::get_job(&self.pool, &request.job_id)
            .await
            .map_err(map_error)?;
        Ok(Response::new(GetJobResponse {
            job: Some(job_detail(job)),
        }))
    }

    async fn retry_job(
        &self,
        request: Request<RetryJobRequest>,
    ) -> Result<Response<RetryJobResponse>, Status> {
        let request = request.into_inner();
        let _admission = self.require_queue(&request.job_queue_id)?;
        let job = job_admin_queue::retry_dead_job(&self.pool, &request.job_id)
            .await
            .map_err(map_error)?;
        Ok(Response::new(RetryJobResponse {
            job: Some(job_detail(job)),
        }))
    }
}
