mod helpers;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use prost::Message as _;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::Request;
use wr_common::lifecycle_service::AdmissionGate;
use wr_common::manager_client::ManagerClient as JobAdminServiceClient;
use wr_common::wruntime::cluster_service_server::ClusterServiceServer;
use wr_common::wruntime::engine_job_admin_service_client::EngineJobAdminServiceClient;
use wr_common::wruntime::engine_job_admin_service_server::{
    EngineJobAdminService, EngineJobAdminServiceServer,
};
use wr_common::wruntime::job_service_server::JobServiceServer;
type ManagerServiceClient<T> = wr_common::manager_client::ManagerClient<T>;
use wr_common::wruntime::{
    CheckJobQueueRequest, CheckJobQueueResponse, EngineRegistration, GetJobQueueSummaryRequest,
    GetJobQueueSummaryResponse, GetJobRequest, GetJobResponse, ListEnginesRequest,
    ListJobQueuesRequest, ListJobsRequest, ListJobsResponse, RetryJobRequest, RetryJobResponse,
    WorkloadProjectionKind,
};

#[derive(Clone)]
struct RetryProbe {
    calls: Arc<AtomicUsize>,
    check_calls: Arc<AtomicUsize>,
    ready: bool,
    unavailable: bool,
}

#[tonic::async_trait]
impl EngineJobAdminService for RetryProbe {
    async fn check_job_queue(
        &self,
        _request: Request<CheckJobQueueRequest>,
    ) -> Result<tonic::Response<CheckJobQueueResponse>, tonic::Status> {
        self.check_calls.fetch_add(1, Ordering::SeqCst);
        if self.ready {
            Ok(tonic::Response::new(CheckJobQueueResponse { ready: true }))
        } else {
            Err(tonic::Status::unavailable("queue not ready"))
        }
    }

    async fn list_jobs(
        &self,
        _request: Request<ListJobsRequest>,
    ) -> Result<tonic::Response<ListJobsResponse>, tonic::Status> {
        Ok(tonic::Response::new(ListJobsResponse::default()))
    }

    async fn get_job_queue_summary(
        &self,
        _request: Request<GetJobQueueSummaryRequest>,
    ) -> Result<tonic::Response<GetJobQueueSummaryResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused in retry probe"))
    }

    async fn get_job(
        &self,
        _request: Request<GetJobRequest>,
    ) -> Result<tonic::Response<GetJobResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused in retry probe"))
    }

    async fn retry_job(
        &self,
        _request: Request<RetryJobRequest>,
    ) -> Result<tonic::Response<RetryJobResponse>, tonic::Status> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.unavailable {
            Err(tonic::Status::unavailable("response lost"))
        } else {
            Ok(tonic::Response::new(RetryJobResponse::default()))
        }
    }
}

fn open_admission() -> AdmissionGate {
    let admission = AdmissionGate::closed();
    admission.open();
    admission
}

fn fresh_manager_policy(
) -> Arc<tokio::sync::Mutex<wr_common::snapshot_consumer::SnapshotConsumerState>> {
    let policy = wr_common::authorization_policy::ValidatedPolicy::load(
        br#"
schema_version=1
generation=1
cluster_id="cluster-a"
assignments=[]
proxy_enrollments=[]
node_agent_enrollments=[]
manager_enrollments=[{principal="urn:wruntime:cluster-a:manager:manager-a",manager_id="manager-a",endpoint="https://127.0.0.1:9000/"}]
revoked_leaf_fingerprints=[]
principals=[{uri="urn:wruntime:cluster-a:manager:manager-a",kind="manager"}]
"#,
    )
    .expect("test manager policy");
    let wall = std::time::SystemTime::now();
    let bytes = wr_common::snapshot_consumer::build_snapshot(
        &policy,
        WorkloadProjectionKind::EngineJobAdminV1,
        wall,
    )
    .expect("test engine snapshot");
    let mut state = wr_common::snapshot_consumer::SnapshotConsumerState::Empty;
    wr_common::snapshot_consumer::consume(
        &mut state,
        Ok(&bytes),
        "cluster-a",
        WorkloadProjectionKind::EngineJobAdminV1,
        wall,
        std::time::Instant::now(),
        true,
    );
    assert!(state.is_fresh(std::time::Instant::now()));
    Arc::new(tokio::sync::Mutex::new(state))
}

async fn mtls_channel(address: &str, tls: &impl wr_common::tls::ClientTlsPaths) -> Result<Channel> {
    Ok(Endpoint::from_shared(address.to_string())?
        .tls_config(wr_common::tls::build_tonic_client_tls(tls)?)?
        .connect()
        .await?)
}

async fn start_authorized_engine_api(
    queue_id: &str,
    pool: Arc<deadpool_postgres::Pool>,
    ready: Arc<AtomicBool>,
    admission: AdmissionGate,
) -> Result<(
    EngineJobAdminServiceClient<Channel>,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
)> {
    let pki = helpers::pki::generate_test_pki_files("authorized-engine-api");
    let api = wr_engine::job_admin::EngineJobAdminApi::new_authorized(
        queue_id.into(),
        pool,
        ready,
        admission,
        fresh_manager_policy(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = format!("https://{}", listener.local_addr()?);
    let server = tokio::spawn(
        Server::builder()
            .tls_config(wr_common::tls::build_tonic_server_tls(&pki.server_tls)?)?
            .add_service(EngineJobAdminServiceServer::new(api))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let client = EngineJobAdminServiceClient::new(mtls_channel(&address, &pki.client_tls).await?);
    Ok((client, server))
}

async fn submit_boundary_job_through_engine_http(
    pool: Arc<deadpool_postgres::Pool>,
    payload: Vec<u8>,
) -> Result<String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let defaults = Arc::new(wr_engine::worker_http::WorkerDefaults::default());
        let service = hyper::service::service_fn(move |request| {
            let pool = Arc::clone(&pool);
            let defaults = Arc::clone(&defaults);
            async move {
                let response = wr_engine::worker_http::handle_worker_http_request(
                    request,
                    Some(pool),
                    defaults,
                )
                .await;
                Ok::<_, std::convert::Infallible>(response)
            }
        });
        hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
            .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
            .await?;
        Ok::<_, anyhow::Error>(())
    });
    let request = wr_common::wruntime::SubmitJobRequest {
        worker_namespace: "job-admin-boundary".into(),
        worker_name: "worker".into(),
        worker_version: "1.0.0".into(),
        job_type: "/jobs.Run/Boundary".into(),
        payload,
        timeout_secs: 60,
        max_attempts: 1,
    };
    let response = helpers::proxy::http_client()
        .request(
            http::Request::post(format!("http://{address}/wruntime.WorkerService/SubmitJob"))
                .header("x-wr-namespace", "job-admin-boundary")
                .header("x-wr-module", "worker")
                .header("x-wr-version", "1.0.0")
                .body(Full::new(Bytes::from(request.encode_to_vec())))?,
        )
        .await?;
    anyhow::ensure!(
        response.status() == http::StatusCode::OK,
        "production engine submission returned {}",
        response.status()
    );
    let body = response.into_body().collect().await?.to_bytes();
    let job_id = wr_common::wruntime::SubmitJobResponse::decode(body)?.job_id;
    server.await??;
    Ok(job_id)
}

#[test]
fn job_admin_test_overlapping_trust_roots_fail_validation() {
    let runtime = helpers::pki::generate_test_pki_files("overlapping-runtime");
    let overlap = wr_common::tls::ensure_disjoint_ca_roots(&[
        ("runtime", &runtime.server_tls),
        ("operator", &runtime.server_tls),
    ])
    .expect_err("reusing one CA across trust domains must fail startup validation");
    assert!(overlap.to_string().contains("share a CA certificate"));
}

#[tokio::test]
async fn job_admin_test_single_manager_listener_authorizes_all_mounted_services() -> Result<()> {
    if helpers::db::skip_without_db(
        "job_admin_test_single_manager_listener_authorizes_all_mounted_services",
    ) {
        return Ok(());
    }
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut runtime = helpers::pki::generate_test_pki_files("runtime");
    let operator = helpers::pki::generate_test_pki_files("operator-admin");
    runtime.client_tls.server_ca_cert_path = operator.client_tls.server_ca_cert_path.clone();
    let delegation = helpers::pki::generate_test_pki_files("delegation");
    let roots_dir = tempfile::tempdir()?;
    let client_roots = roots_dir.path().join("manager-client-roots.pem");
    std::fs::write(
        &client_roots,
        format!(
            "{}\n{}",
            std::fs::read_to_string(&operator.server_tls.client_ca_cert_path)?,
            std::fs::read_to_string(&runtime.server_tls.client_ca_cert_path)?,
        ),
    )?;
    let mut manager_server_tls = operator.server_tls.clone();
    manager_server_tls.client_ca_cert_path = client_roots.to_string_lossy().into_owned();
    let manager_pool = helpers::db::manager_pool().await;
    let admission = AdmissionGate::closed();
    admission.open();
    let manager_api = wr_manager::job_admin::JobAdminApi::new(
        manager_pool.clone(),
        30,
        delegation.client_tls.clone(),
        admission.clone(),
    );
    let policy = wr_manager::auth::PrincipalPolicy::new(Arc::new(
        wr_common::authorization_policy::ValidatedPolicy::load(
            br#"schema_version=1
generation=1
cluster_id="cluster-a"
revoked_leaf_fingerprints=[]
principals=[{uri="urn:wruntime:cluster-a:human:operator-admin",kind="human"},{uri="urn:wruntime:cluster-a:proxy:proxy-a",kind="proxy"}]
assignments=[{principal="urn:wruntime:cluster-a:human:operator-admin",role="admin",scope={}}]
manager_enrollments=[]
proxy_enrollments=[{principal="urn:wruntime:cluster-a:proxy:proxy-a",node_id="node-a"}]
node_agent_enrollments=[]
"#,
        )?,
    ));
    let authorizer = Arc::new(wr_manager::auth::ManagerAuthorizer::new(policy, admission));
    let manager_api =
        wr_manager::job_admin::AuthorizedJobService::new(manager_api, authorizer.clone());
    let secret_key = wr_manager::crypto::SecretCrypto::generate_random_password();
    let manager = wr_manager::service::Manager::new(
        manager_pool.clone(),
        Arc::new(wr_manager::crypto::SecretCrypto::from_hex(&secret_key)?),
    );
    let cluster_api = wr_manager::service::AuthorizedClusterService::new(manager, authorizer);
    let manager_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let manager_address = format!("https://{}", manager_listener.local_addr()?);
    let manager_server = tokio::spawn(
        Server::builder()
            .tls_config(wr_common::tls::build_tonic_server_tls(&manager_server_tls)?)?
            .add_service(ClusterServiceServer::new(cluster_api))
            .add_service(
                JobServiceServer::new(manager_api)
                    .max_encoding_message_size(wr_common::lifecycle::MAX_JOB_ADMIN_MESSAGE_BYTES),
            )
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                manager_listener,
            )),
    );

    let channel = mtls_channel(&manager_address, &operator.client_tls).await?;
    let queues = JobAdminServiceClient::from_test_channels(channel.clone(), channel.clone())
        .list_job_queues(ListJobQueuesRequest {})
        .await?
        .into_inner();
    assert!(queues.queues.is_empty());
    let engines = ManagerServiceClient::from_test_channels(channel.clone(), channel)
        .list_engines(ListEnginesRequest {})
        .await?
        .into_inner();
    assert!(engines.engines.is_empty());

    let channel = mtls_channel(&manager_address, &runtime.client_tls).await?;
    let denial = JobAdminServiceClient::from_test_channels(channel.clone(), channel)
        .list_job_queues(ListJobQueuesRequest {})
        .await
        .expect_err("proxy identity must not inherit human job authorization");
    assert_eq!(denial.code(), tonic::Code::PermissionDenied);

    let engine_pool = Arc::new(helpers::worker::worker_pool().await);
    let engine_policy = fresh_manager_policy();
    let engine_api = wr_engine::job_admin::EngineJobAdminApi::new_authorized(
        "test-jobs".into(),
        Arc::clone(&engine_pool),
        Arc::new(AtomicBool::new(true)),
        open_admission(),
        Arc::clone(&engine_policy),
    );
    let engine_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let engine_address = format!("https://{}", engine_listener.local_addr()?);
    let engine_server = tokio::spawn(
        Server::builder()
            .tls_config(wr_common::tls::build_tonic_server_tls(
                &delegation.server_tls,
            )?)?
            .add_service(
                EngineJobAdminServiceServer::new(engine_api)
                    .max_encoding_message_size(wr_common::lifecycle::MAX_JOB_ADMIN_MESSAGE_BYTES),
            )
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                engine_listener,
            )),
    );

    let delegation_channel = mtls_channel(&engine_address, &delegation.client_tls).await?;
    assert!(
        EngineJobAdminServiceClient::new(delegation_channel)
            .check_job_queue(CheckJobQueueRequest {
                job_queue_id: "test-jobs".into(),
            })
            .await?
            .into_inner()
            .ready
    );
    let operator_attempt = mtls_channel(&engine_address, &operator.client_tls).await;
    if let Ok(channel) = operator_attempt {
        assert!(EngineJobAdminServiceClient::new(channel)
            .check_job_queue(CheckJobQueueRequest {
                job_queue_id: "test-jobs".into(),
            })
            .await
            .is_err());
    }

    let boundary_payload = vec![9_u8; wr_common::lifecycle::MAX_JOB_BLOB_BYTES];
    let boundary_job =
        submit_boundary_job_through_engine_http(Arc::clone(&engine_pool), boundary_payload.clone())
            .await?;
    let boundary_claim = wr_engine::worker::claim_job(
        &engine_pool,
        "job-admin-boundary",
        "worker",
        "1.0.0",
        "boundary-engine",
    )
    .await?
    .context("boundary job was not claimable")?;
    let boundary_result = vec![8_u8; wr_common::lifecycle::MAX_JOB_BLOB_BYTES];
    wr_engine::worker::complete_job(
        &engine_pool,
        &boundary_job,
        boundary_claim.claim_id,
        &boundary_result,
    )
    .await?;
    register_delegate(
        &manager_pool,
        "boundary-engine",
        "test-jobs",
        &engine_address,
    )
    .await?;
    let channel = mtls_channel(&manager_address, &operator.client_tls).await?;
    let detail = JobAdminServiceClient::from_test_channels(channel.clone(), channel)
        .get_job(GetJobRequest {
            job_queue_id: "test-jobs".into(),
            job_id: boundary_job,
        })
        .await?
        .into_inner()
        .job
        .context("boundary job detail missing")?;
    assert_eq!(detail.payload.len(), boundary_payload.len());
    assert_eq!(detail.result.len(), boundary_result.len());

    {
        let mut state = engine_policy.lock().await;
        state.expire(std::time::Instant::now() + std::time::Duration::from_secs(31));
    }
    let stale_denial = EngineJobAdminServiceClient::new(
        mtls_channel(&engine_address, &delegation.client_tls).await?,
    )
    .check_job_queue(CheckJobQueueRequest {
        job_queue_id: "test-jobs".into(),
    })
    .await
    .expect_err("expired manager policy snapshot must deny the next RPC");
    assert_eq!(stale_denial.code(), tonic::Code::PermissionDenied);

    manager_server.abort();
    engine_server.abort();
    Ok(())
}

async fn register_delegate(
    pool: &deadpool_postgres::Pool,
    engine_id: &str,
    queue_id: &str,
    address: &str,
) -> Result<()> {
    let registration = EngineRegistration {
        engine_id: engine_id.into(),
        address: "http://127.0.0.1:9100".into(),
        proxy_address: "http://127.0.0.1:9001".into(),
        peer_address: "https://127.0.0.1:9443".into(),
        job_queue_id: queue_id.into(),
        job_admin_address: address.into(),
        ..Default::default()
    };
    let client = pool.get().await?;
    client.execute("INSERT INTO wr_engines (engine_id,address,proxy_address,peer_address,registration,job_queue_id,job_admin_address) VALUES ($1,$2,$3,$4,$5,$6,$7)", &[&registration.engine_id,&registration.address,&registration.proxy_address,&registration.peer_address,&registration.encode_to_vec(),&registration.job_queue_id,&registration.job_admin_address]).await?;
    Ok(())
}

async fn start_retry_probe(
    probe: RetryProbe,
    tls: &impl wr_common::tls::ServerTlsPaths,
) -> Result<(
    String,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = format!("https://{}", listener.local_addr()?);
    let server = tokio::spawn(
        Server::builder()
            .tls_config(wr_common::tls::build_tonic_server_tls(tls)?)?
            .add_service(EngineJobAdminServiceServer::new(probe))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    Ok((address, server))
}

#[tokio::test]
async fn job_admin_test_queue_discovery_deduplicates_and_reports_freshness() -> Result<()> {
    if helpers::db::skip_without_db(
        "job_admin_test_queue_discovery_deduplicates_and_reports_freshness",
    ) {
        return Ok(());
    }
    let delegation = helpers::pki::generate_test_pki_files("discovery-delegation");
    let manager_pool = helpers::db::manager_pool().await;
    register_delegate(
        &manager_pool,
        "a-fresh",
        "shared-jobs",
        "https://127.0.0.1:19151",
    )
    .await?;
    register_delegate(
        &manager_pool,
        "b-draining",
        "shared-jobs",
        "https://127.0.0.1:19152",
    )
    .await?;
    register_delegate(
        &manager_pool,
        "c-stale",
        "stale-jobs",
        "https://127.0.0.1:19153",
    )
    .await?;
    manager_pool
        .get()
        .await?
        .execute(
            "UPDATE wr_engines SET draining = (engine_id = 'b-draining'), \
             last_heartbeat = CASE WHEN engine_id = 'c-stale' THEN now() - interval '1 hour' ELSE last_heartbeat END",
            &[],
        )
        .await?;

    let admission = AdmissionGate::closed();
    admission.open();
    let api =
        wr_manager::job_admin::JobAdminApi::new(manager_pool, 30, delegation.client_tls, admission);
    let queues = api
        .list_job_queues(Request::new(ListJobQueuesRequest {}))
        .await?
        .into_inner()
        .queues;
    assert_eq!(queues.len(), 2);
    assert_eq!(queues[0].job_queue_id, "shared-jobs");
    assert_eq!(queues[0].fresh_delegates, 1);
    assert_eq!(queues[0].total_delegates, 2);
    assert_eq!(
        queues[0].availability(),
        wr_common::wruntime::JobQueueAvailability::Available
    );
    assert_eq!(queues[1].job_queue_id, "stale-jobs");
    assert_eq!(queues[1].fresh_delegates, 0);
    assert_eq!(queues[1].total_delegates, 1);
    assert_eq!(
        queues[1].availability(),
        wr_common::wruntime::JobQueueAvailability::Unavailable
    );
    let invalid = api
        .list_jobs(Request::new(ListJobsRequest {
            job_queue_id: "Invalid_Queue".into(),
            ..Default::default()
        }))
        .await
        .expect_err("invalid queue ID must fail before delegation");
    assert_eq!(invalid.code(), tonic::Code::InvalidArgument);
    let unknown = api
        .list_jobs(Request::new(ListJobsRequest {
            job_queue_id: "unknown-jobs".into(),
            ..Default::default()
        }))
        .await
        .expect_err("unknown queue must fail before delegation");
    assert_eq!(unknown.code(), tonic::Code::NotFound);
    let unavailable = api
        .list_jobs(Request::new(ListJobsRequest {
            job_queue_id: "stale-jobs".into(),
            ..Default::default()
        }))
        .await
        .expect_err("stale queue must fail before delegation");
    assert_eq!(unavailable.code(), tonic::Code::Unavailable);
    Ok(())
}

#[tokio::test]
async fn job_admin_test_manager_read_failover_is_pre_dispatch_only() -> Result<()> {
    if helpers::db::skip_without_db("job_admin_test_manager_read_failover_is_pre_dispatch_only") {
        return Ok(());
    }
    let _ = rustls::crypto::ring::default_provider().install_default();
    let delegation = helpers::pki::generate_test_pki_files("read-delegation");
    let queue_pool = Arc::new(helpers::worker::worker_pool().await);
    let engine_api = wr_engine::job_admin::EngineJobAdminApi::new_authorized(
        "shared-jobs".into(),
        Arc::clone(&queue_pool),
        Arc::new(AtomicBool::new(true)),
        open_admission(),
        fresh_manager_policy(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let reachable_address = format!("https://{}", listener.local_addr()?);
    let engine_server = tokio::spawn(
        Server::builder()
            .tls_config(wr_common::tls::build_tonic_server_tls(
                &delegation.server_tls,
            )?)?
            .add_service(EngineJobAdminServiceServer::new(engine_api))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let wrong_api = wr_engine::job_admin::EngineJobAdminApi::new_authorized(
        "different-jobs".into(),
        queue_pool,
        Arc::new(AtomicBool::new(true)),
        open_admission(),
        fresh_manager_policy(),
    );
    let wrong_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let wrong_address = format!("https://{}", wrong_listener.local_addr()?);
    let wrong_server = tokio::spawn(
        Server::builder()
            .tls_config(wr_common::tls::build_tonic_server_tls(
                &delegation.server_tls,
            )?)?
            .add_service(EngineJobAdminServiceServer::new(wrong_api))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                wrong_listener,
            )),
    );
    let unavailable = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let unavailable_address = format!("https://{}", unavailable.local_addr()?);
    drop(unavailable);

    let manager_pool = helpers::db::manager_pool().await;
    register_delegate(
        &manager_pool,
        "a-wrong-queue",
        "shared-jobs",
        &wrong_address,
    )
    .await?;
    register_delegate(
        &manager_pool,
        "b-unreachable",
        "shared-jobs",
        &unavailable_address,
    )
    .await?;
    register_delegate(
        &manager_pool,
        "c-reachable",
        "shared-jobs",
        &reachable_address,
    )
    .await?;
    let admission = AdmissionGate::closed();
    admission.open();
    let api = wr_manager::job_admin::JobAdminApi::new(
        manager_pool,
        30,
        delegation.client_tls.clone(),
        admission,
    );
    let response = api
        .list_jobs(Request::new(ListJobsRequest {
            job_queue_id: "shared-jobs".into(),
            filter: Some(wr_common::wruntime::JobFilter {
                worker_namespace: "no-such-worker-namespace".into(),
                ..Default::default()
            }),
            page_size: 1,
            ..Default::default()
        }))
        .await?
        .into_inner();
    assert!(response.jobs.is_empty());
    let missing = api
        .get_job(Request::new(GetJobRequest {
            job_queue_id: "shared-jobs".into(),
            job_id: "missing-job".into(),
        }))
        .await
        .expect_err("engine application status must pass through the manager");
    assert_eq!(missing.code(), tonic::Code::NotFound);

    wrong_server.abort();
    engine_server.abort();
    Ok(())
}

#[tokio::test]
async fn job_admin_test_retry_qualifies_only_one_deterministic_delegate() -> Result<()> {
    if helpers::db::skip_without_db(
        "job_admin_test_retry_qualifies_only_one_deterministic_delegate",
    ) {
        return Ok(());
    }
    let _ = rustls::crypto::ring::default_provider().install_default();
    let delegation = helpers::pki::generate_test_pki_files("retry-qualification");
    let first_calls = Arc::new(AtomicUsize::new(0));
    let second_calls = Arc::new(AtomicUsize::new(0));
    let first_checks = Arc::new(AtomicUsize::new(0));
    let second_checks = Arc::new(AtomicUsize::new(0));
    let (first_address, first_server) = start_retry_probe(
        RetryProbe {
            calls: Arc::clone(&first_calls),
            check_calls: Arc::clone(&first_checks),
            ready: false,
            unavailable: false,
        },
        &delegation.server_tls,
    )
    .await?;
    let (second_address, second_server) = start_retry_probe(
        RetryProbe {
            calls: Arc::clone(&second_calls),
            check_calls: Arc::clone(&second_checks),
            ready: true,
            unavailable: false,
        },
        &delegation.server_tls,
    )
    .await?;

    let manager_pool = helpers::db::manager_pool().await;
    register_delegate(&manager_pool, "a-first", "retry-jobs", &first_address).await?;
    register_delegate(&manager_pool, "b-second", "retry-jobs", &second_address).await?;
    let admission = AdmissionGate::closed();
    admission.open();
    let api =
        wr_manager::job_admin::JobAdminApi::new(manager_pool, 30, delegation.client_tls, admission);
    let status = api
        .retry_job(Request::new(RetryJobRequest {
            job_queue_id: "retry-jobs".into(),
            job_id: "dead-job".into(),
        }))
        .await
        .expect_err("an unready selected delegate must prevent dispatch");
    assert_eq!(status.code(), tonic::Code::Unavailable);
    assert!(status.message().contains("not dispatched"));
    assert_eq!(first_checks.load(Ordering::SeqCst), 1);
    assert_eq!(second_checks.load(Ordering::SeqCst), 0);
    assert_eq!(first_calls.load(Ordering::SeqCst), 0);
    assert_eq!(second_calls.load(Ordering::SeqCst), 0);

    first_server.abort();
    second_server.abort();
    Ok(())
}

#[tokio::test]
async fn job_admin_test_manager_never_replays_retry_after_dispatch() -> Result<()> {
    if helpers::db::skip_without_db("job_admin_test_manager_never_replays_retry_after_dispatch") {
        return Ok(());
    }
    let _ = rustls::crypto::ring::default_provider().install_default();
    let delegation = helpers::pki::generate_test_pki_files("retry-delegation");
    let first_calls = Arc::new(AtomicUsize::new(0));
    let second_calls = Arc::new(AtomicUsize::new(0));
    let first_checks = Arc::new(AtomicUsize::new(0));
    let second_checks = Arc::new(AtomicUsize::new(0));
    let (first_address, first_server) = start_retry_probe(
        RetryProbe {
            calls: Arc::clone(&first_calls),
            check_calls: Arc::clone(&first_checks),
            ready: true,
            unavailable: true,
        },
        &delegation.server_tls,
    )
    .await?;
    let (second_address, second_server) = start_retry_probe(
        RetryProbe {
            calls: Arc::clone(&second_calls),
            check_calls: Arc::clone(&second_checks),
            ready: true,
            unavailable: false,
        },
        &delegation.server_tls,
    )
    .await?;

    let manager_pool = helpers::db::manager_pool().await;
    register_delegate(&manager_pool, "a-first", "retry-jobs", &first_address).await?;
    register_delegate(&manager_pool, "b-second", "retry-jobs", &second_address).await?;
    let admission = AdmissionGate::closed();
    admission.open();
    let api = wr_manager::job_admin::JobAdminApi::new(
        manager_pool,
        30,
        delegation.client_tls.clone(),
        admission,
    );
    let status = api
        .retry_job(Request::new(RetryJobRequest {
            job_queue_id: "retry-jobs".into(),
            job_id: "dead-job".into(),
        }))
        .await
        .expect_err("lost retry response must be reported as unknown");
    assert_eq!(status.code(), tonic::Code::Unavailable);
    assert!(status.message().contains("outcome unknown"));
    assert_eq!(first_checks.load(Ordering::SeqCst), 1);
    assert_eq!(second_checks.load(Ordering::SeqCst), 0);
    assert_eq!(first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(second_calls.load(Ordering::SeqCst), 0);

    first_server.abort();
    second_server.abort();
    Ok(())
}

#[tokio::test]
async fn job_admin_test_engine_enforces_queue_scope_and_migration_readiness() -> Result<()> {
    if helpers::db::skip_without_db(
        "job_admin_test_engine_enforces_queue_scope_and_migration_readiness",
    ) {
        return Ok(());
    }
    let pool = Arc::new(helpers::worker::worker_pool().await);
    let ready = Arc::new(AtomicBool::new(false));
    let admission = open_admission();
    let (mut api, server) = start_authorized_engine_api(
        "test-jobs",
        Arc::clone(&pool),
        Arc::clone(&ready),
        admission.clone(),
    )
    .await?;

    let mismatch = api
        .check_job_queue(Request::new(CheckJobQueueRequest {
            job_queue_id: "other-jobs".into(),
        }))
        .await
        .expect_err("wrong queue must fail");
    assert_eq!(mismatch.code(), tonic::Code::InvalidArgument);

    let unavailable = api
        .check_job_queue(Request::new(CheckJobQueueRequest {
            job_queue_id: "test-jobs".into(),
        }))
        .await
        .expect_err("queue is unavailable before migrations are declared ready");
    assert_eq!(unavailable.code(), tonic::Code::Unavailable);

    ready.store(true, Ordering::Release);
    assert!(
        api.check_job_queue(Request::new(CheckJobQueueRequest {
            job_queue_id: "test-jobs".into(),
        }))
        .await?
        .into_inner()
        .ready
    );
    admission.close();
    let stopping = api
        .check_job_queue(Request::new(CheckJobQueueRequest {
            job_queue_id: "test-jobs".into(),
        }))
        .await
        .expect_err("closed admission must fence new job-admin requests");
    assert_eq!(stopping.code(), tonic::Code::Unavailable);
    server.abort();
    Ok(())
}

#[tokio::test]
async fn job_admin_test_engine_maps_list_get_and_retry_statuses() -> Result<()> {
    if helpers::db::skip_without_db("job_admin_test_engine_maps_list_get_and_retry_statuses") {
        return Ok(());
    }
    let pool = Arc::new(helpers::worker::worker_pool().await);
    let namespace = helpers::worker::unique_worker_namespace("job-admin-api");
    let job_id = wr_engine::worker::insert_job(
        &pool,
        &namespace,
        "worker",
        "1.0.0",
        "/jobs.Run/Execute",
        b"payload",
        60,
        1,
        "source",
        "caller",
    )
    .await?;
    let ready = Arc::new(AtomicBool::new(true));
    let (mut api, server) =
        start_authorized_engine_api("test-jobs", Arc::clone(&pool), ready, open_admission())
            .await?;

    let page = api
        .list_jobs(Request::new(ListJobsRequest {
            job_queue_id: "test-jobs".into(),
            filter: Some(wr_common::wruntime::JobFilter {
                worker_namespace: namespace.clone(),
                ..Default::default()
            }),
            status: None,
            page_size: 1,
            cursor: String::new(),
        }))
        .await?
        .into_inner();
    assert_eq!(page.jobs.len(), 1);
    assert_eq!(page.jobs[0].job_id, job_id);

    let detail = api
        .get_job(Request::new(GetJobRequest {
            job_queue_id: "test-jobs".into(),
            job_id: job_id.clone(),
        }))
        .await?
        .into_inner()
        .job
        .expect("job detail");
    assert_eq!(detail.payload, b"payload");

    let pending_retry = api
        .retry_job(Request::new(RetryJobRequest {
            job_queue_id: "test-jobs".into(),
            job_id: job_id.clone(),
        }))
        .await
        .expect_err("pending job retry must conflict");
    assert_eq!(pending_retry.code(), tonic::Code::FailedPrecondition);

    let missing = api
        .get_job(Request::new(GetJobRequest {
            job_queue_id: "test-jobs".into(),
            job_id: "missing-job".into(),
        }))
        .await
        .expect_err("missing job must map to not found");
    assert_eq!(missing.code(), tonic::Code::NotFound);
    server.abort();
    Ok(())
}
