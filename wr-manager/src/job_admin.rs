use std::collections::BTreeMap;
use std::time::Duration;

use deadpool_postgres::Pool;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};

use wr_common::identity::JobQueueId;
use wr_common::lifecycle_service::{AdmissionGate, AdmissionGuard};
use wr_common::node::TlsConfig;
use wr_common::wruntime::engine_job_admin_service_client::EngineJobAdminServiceClient;
use wr_common::wruntime::job_admin_service_server::JobAdminService;
use wr_common::wruntime::{
    CheckJobQueueRequest, GetJobQueueSummaryRequest, GetJobQueueSummaryResponse, GetJobRequest,
    GetJobResponse, JobQueueAvailability, JobQueueInfo, ListJobQueuesRequest,
    ListJobQueuesResponse, ListJobsRequest, ListJobsResponse, RetryJobRequest, RetryJobResponse,
};

use crate::db::{self, JobAdminDelegate};

#[derive(Clone)]
pub struct JobAdminApi {
    pool: Pool,
    heartbeat_timeout_secs: u64,
    delegation_tls: TlsConfig,
    admission: AdmissionGate,
}

impl JobAdminApi {
    pub fn new(
        pool: Pool,
        heartbeat_timeout_secs: u64,
        delegation_tls: TlsConfig,
        admission: AdmissionGate,
    ) -> Self {
        Self {
            pool,
            heartbeat_timeout_secs,
            delegation_tls,
            admission,
        }
    }

    fn require_admission(&self) -> Result<AdmissionGuard, Status> {
        self.admission
            .try_enter()
            .ok_or_else(|| Status::unavailable("manager is stopping"))
    }

    async fn all_delegates(&self) -> Result<Vec<JobAdminDelegate>, Status> {
        db::list_job_admin_delegates(&self.pool, self.heartbeat_timeout_secs).await
    }

    async fn fresh_delegates(&self, queue_id: &str) -> Result<Vec<JobAdminDelegate>, Status> {
        JobQueueId::parse(queue_id).map_err(|error| Status::invalid_argument(error.to_string()))?;
        let delegates = self.all_delegates().await?;
        let known = delegates
            .iter()
            .any(|delegate| delegate.job_queue_id == queue_id);
        if !known {
            return Err(Status::not_found("job queue is not registered"));
        }
        let fresh = delegates
            .into_iter()
            .filter(|delegate| delegate.job_queue_id == queue_id && delegate.fresh)
            .collect::<Vec<_>>();
        if fresh.is_empty() {
            return Err(Status::unavailable("job queue has no fresh delegates"));
        }
        Ok(fresh)
    }

    async fn connect(
        &self,
        delegate: &JobAdminDelegate,
    ) -> anyhow::Result<EngineJobAdminServiceClient<Channel>> {
        let tls = wr_common::tls::build_tonic_client_tls(&self.delegation_tls)?;
        let channel = Endpoint::from_shared(delegate.address.clone())?
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .tls_config(tls)?
            .connect()
            .await?;
        Ok(EngineJobAdminServiceClient::new(channel)
            .max_decoding_message_size(wr_common::lifecycle::MAX_JOB_ADMIN_MESSAGE_BYTES)
            .max_encoding_message_size(wr_common::lifecycle::MAX_JOB_ADMIN_MESSAGE_BYTES))
    }

    async fn connect_qualified(
        &self,
        delegate: &JobAdminDelegate,
        queue_id: &str,
    ) -> anyhow::Result<EngineJobAdminServiceClient<Channel>> {
        let mut client = self.connect(delegate).await?;
        let response = client
            .check_job_queue(CheckJobQueueRequest {
                job_queue_id: queue_id.to_string(),
            })
            .await?
            .into_inner();
        anyhow::ensure!(response.ready, "delegate reported queue not ready");
        Ok(client)
    }

    async fn connect_read(
        &self,
        queue_id: &str,
    ) -> Result<EngineJobAdminServiceClient<Channel>, Status> {
        let delegates = self.fresh_delegates(queue_id).await?;
        let mut last_error = None;
        for delegate in delegates {
            match self.connect_qualified(&delegate, queue_id).await {
                Ok(client) => return Ok(client),
                Err(error) => {
                    tracing::warn!(
                        queue_id,
                        engine_id = %delegate.engine_id,
                        %error,
                        "job administration delegate qualification failed"
                    );
                    last_error = Some(error);
                }
            }
        }
        Err(Status::unavailable(format!(
            "all fresh job queue delegates were unreachable or not ready{}",
            last_error
                .map(|_| "")
                .unwrap_or("; no connection attempt was made")
        )))
    }

    async fn connect_mutation(
        &self,
        queue_id: &str,
    ) -> Result<EngineJobAdminServiceClient<Channel>, Status> {
        let delegate = self
            .fresh_delegates(queue_id)
            .await?
            .into_iter()
            .next()
            .expect("fresh delegates are non-empty");
        self.connect_qualified(&delegate, queue_id)
            .await
            .map_err(|error| {
                tracing::warn!(queue_id, engine_id = %delegate.engine_id, %error, "job retry delegate qualification failed");
                Status::unavailable(
                    "selected job queue delegate is unreachable or not ready; retry was not dispatched",
                )
            })
    }
}

#[tonic::async_trait]
impl JobAdminService for JobAdminApi {
    async fn list_job_queues(
        &self,
        _request: Request<ListJobQueuesRequest>,
    ) -> Result<Response<ListJobQueuesResponse>, Status> {
        let _admission = self.require_admission()?;
        let mut grouped: BTreeMap<String, (u32, u32)> = BTreeMap::new();
        for delegate in self.all_delegates().await? {
            JobQueueId::parse(delegate.job_queue_id.clone())
                .map_err(|_| Status::internal("stored job queue identity is invalid"))?;
            let counts = grouped.entry(delegate.job_queue_id).or_default();
            counts.1 = counts.1.saturating_add(1);
            if delegate.fresh {
                counts.0 = counts.0.saturating_add(1);
            }
            if grouped.len() > wr_common::lifecycle::MAX_JOB_QUEUE_DISCOVERY_ENTRIES {
                return Err(Status::resource_exhausted(
                    "job queue discovery exceeds the configured entry limit",
                ));
            }
        }
        Ok(Response::new(ListJobQueuesResponse {
            queues: grouped
                .into_iter()
                .map(
                    |(job_queue_id, (fresh_delegates, total_delegates))| JobQueueInfo {
                        job_queue_id,
                        availability: if fresh_delegates > 0 {
                            JobQueueAvailability::Available as i32
                        } else {
                            JobQueueAvailability::Unavailable as i32
                        },
                        fresh_delegates,
                        total_delegates,
                    },
                )
                .collect(),
        }))
    }

    async fn list_jobs(
        &self,
        request: Request<ListJobsRequest>,
    ) -> Result<Response<ListJobsResponse>, Status> {
        let _admission = self.require_admission()?;
        let request = request.into_inner();
        let mut client = self.connect_read(&request.job_queue_id).await?;
        client.list_jobs(request).await
    }

    async fn get_job_queue_summary(
        &self,
        request: Request<GetJobQueueSummaryRequest>,
    ) -> Result<Response<GetJobQueueSummaryResponse>, Status> {
        let _admission = self.require_admission()?;
        let request = request.into_inner();
        let mut client = self.connect_read(&request.job_queue_id).await?;
        client.get_job_queue_summary(request).await
    }

    async fn get_job(
        &self,
        request: Request<GetJobRequest>,
    ) -> Result<Response<GetJobResponse>, Status> {
        let _admission = self.require_admission()?;
        let request = request.into_inner();
        let mut client = self.connect_read(&request.job_queue_id).await?;
        client.get_job(request).await
    }

    async fn retry_job(
        &self,
        request: Request<RetryJobRequest>,
    ) -> Result<Response<RetryJobResponse>, Status> {
        let _admission = self.require_admission()?;
        let request = request.into_inner();
        let mut client = self.connect_mutation(&request.job_queue_id).await?;
        match client.retry_job(request).await {
            Ok(response) => Ok(response),
            Err(status)
                if matches!(
                    status.code(),
                    tonic::Code::Cancelled
                        | tonic::Code::DeadlineExceeded
                        | tonic::Code::Unavailable
                        | tonic::Code::Unknown
                ) =>
            {
                Err(Status::unavailable(
                    "retry outcome unknown; inspect the job before retrying again",
                ))
            }
            Err(status) => Err(status),
        }
    }
}
