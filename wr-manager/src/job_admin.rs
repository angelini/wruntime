use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use deadpool_postgres::Pool;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};

use wr_common::identity::JobQueueId;
use wr_common::lifecycle_service::{AdmissionGate, AdmissionGuard};
use wr_common::node::ClientTlsConfig;
use wr_common::wruntime::engine_job_admin_service_client::EngineJobAdminServiceClient;
use wr_common::wruntime::job_service_server::JobService;
use wr_common::wruntime::{
    CheckJobQueueRequest, GetJobQueueSummaryRequest, GetJobQueueSummaryResponse, GetJobRequest,
    GetJobResponse, JobQueueAvailability, JobQueueInfo, ListJobQueuesRequest,
    ListJobQueuesResponse, ListJobsRequest, ListJobsResponse, RetryJobRequest, RetryJobResponse,
};

use crate::db::{self, JobAdminDelegate};

/// Phase-2 inbound JobService authorization façade. It deliberately applies
/// human/service-account queue roles before delegate discovery; manager
/// workload identity is checked only by the outbound engine boundary.
#[derive(Clone)]
pub struct JobServiceAuthorizationFacade {
    inner: crate::service::AuthAwareServiceFacade,
}

impl JobServiceAuthorizationFacade {
    pub fn new(policy: crate::auth::PrincipalPolicy) -> Self {
        Self {
            inner: crate::service::AuthAwareServiceFacade::new("wruntime.JobService", policy)
                .expect("JobService is in the checked manager service registry"),
        }
    }

    pub fn authorize_queue(
        &self,
        method: &str,
        queue_id: &str,
        evidence: &wr_common::tls::LeafEvidence,
    ) -> Result<crate::auth::AuthorizedPrincipal, Status> {
        self.inner.authorize(
            method,
            evidence,
            &crate::auth::AuthorizationResource {
                job_queue_id: Some(queue_id),
                ..Default::default()
            },
        )
    }
}

#[derive(Clone)]
pub struct JobAdminApi {
    pool: Pool,
    heartbeat_timeout_secs: u64,
    delegation_tls: ClientTlsConfig,
    admission: AdmissionGate,
}

impl JobAdminApi {
    pub fn new(
        pool: Pool,
        heartbeat_timeout_secs: u64,
        delegation_tls: ClientTlsConfig,
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

impl JobAdminApi {
    pub async fn list_job_queues(
        &self,
        request: Request<ListJobQueuesRequest>,
    ) -> Result<Response<ListJobQueuesResponse>, Status> {
        self.list_job_queues_filtered(request, None).await
    }

    async fn list_job_queues_filtered(
        &self,
        _request: Request<ListJobQueuesRequest>,
        allowed_queues: Option<&std::collections::BTreeSet<String>>,
    ) -> Result<Response<ListJobQueuesResponse>, Status> {
        let _admission = self.require_admission()?;
        let mut grouped: BTreeMap<String, (u32, u32)> = BTreeMap::new();
        for delegate in self.all_delegates().await? {
            if allowed_queues.is_some_and(|allowed| !allowed.contains(&delegate.job_queue_id)) {
                continue;
            }
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

    pub async fn list_jobs(
        &self,
        request: Request<ListJobsRequest>,
    ) -> Result<Response<ListJobsResponse>, Status> {
        let _admission = self.require_admission()?;
        let request = request.into_inner();
        let mut client = self.connect_read(&request.job_queue_id).await?;
        client.list_jobs(request).await
    }

    pub async fn get_job_queue_summary(
        &self,
        request: Request<GetJobQueueSummaryRequest>,
    ) -> Result<Response<GetJobQueueSummaryResponse>, Status> {
        let _admission = self.require_admission()?;
        let request = request.into_inner();
        let mut client = self.connect_read(&request.job_queue_id).await?;
        client.get_job_queue_summary(request).await
    }

    pub async fn get_job(
        &self,
        request: Request<GetJobRequest>,
    ) -> Result<Response<GetJobResponse>, Status> {
        let _admission = self.require_admission()?;
        let request = request.into_inner();
        let mut client = self.connect_read(&request.job_queue_id).await?;
        client.get_job(request).await
    }

    pub async fn retry_job(
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

#[derive(Clone)]
pub struct AuthorizedJobService {
    inner: JobAdminApi,
    authorizer: Arc<crate::auth::ManagerAuthorizer>,
}

impl AuthorizedJobService {
    pub fn new(inner: JobAdminApi, authorizer: Arc<crate::auth::ManagerAuthorizer>) -> Self {
        Self { inner, authorizer }
    }

    fn authorize<T>(
        &self,
        request: &mut Request<T>,
        method: &'static str,
        queue: Option<&str>,
    ) -> Result<(), Status> {
        self.authorizer
            .authorize(
                request,
                "wruntime.JobService",
                method,
                &crate::auth::AuthorizationResource {
                    job_queue_id: queue,
                    ..Default::default()
                },
            )?
            .verify("wruntime.JobService", method)
    }
}

#[tonic::async_trait]
impl JobService for AuthorizedJobService {
    async fn list_job_queues(
        &self,
        mut request: Request<ListJobQueuesRequest>,
    ) -> Result<Response<ListJobQueuesResponse>, Status> {
        let call = self.authorizer.authorize(
            &mut request,
            "wruntime.JobService",
            "ListJobQueues",
            &Default::default(),
        )?;
        call.verify("wruntime.JobService", "ListJobQueues")?;
        self.inner
            .list_job_queues_filtered(request, call.job_queue_filter())
            .await
    }
    async fn list_jobs(
        &self,
        mut request: Request<ListJobsRequest>,
    ) -> Result<Response<ListJobsResponse>, Status> {
        let queue = request.get_ref().job_queue_id.clone();
        self.authorize(&mut request, "ListJobs", Some(&queue))?;
        self.inner.list_jobs(request).await
    }
    async fn get_job_queue_summary(
        &self,
        mut request: Request<GetJobQueueSummaryRequest>,
    ) -> Result<Response<GetJobQueueSummaryResponse>, Status> {
        let queue = request.get_ref().job_queue_id.clone();
        self.authorize(&mut request, "GetJobQueueSummary", Some(&queue))?;
        self.inner.get_job_queue_summary(request).await
    }
    async fn get_job(
        &self,
        mut request: Request<GetJobRequest>,
    ) -> Result<Response<GetJobResponse>, Status> {
        let queue = request.get_ref().job_queue_id.clone();
        self.authorize(&mut request, "GetJob", Some(&queue))?;
        self.inner.get_job(request).await
    }
    async fn retry_job(
        &self,
        mut request: Request<RetryJobRequest>,
    ) -> Result<Response<RetryJobResponse>, Status> {
        let queue = request.get_ref().job_queue_id.clone();
        self.authorize(&mut request, "RetryJob", Some(&queue))?;
        self.inner.retry_job(request).await
    }
}
