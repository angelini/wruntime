mod helpers;
use helpers::{proxy::http_client, worker::WorkerPoolHarness};

use bytes::Bytes;
use http::{Request, StatusCode};
use http_body_util::{BodyExt, Full};
use std::time::Duration;
use wr_common::config::Validatable;

#[tokio::test]
async fn test_worker_config_parsing() {
    let toml_str = r#"
        listen_address  = "127.0.0.1:9100"
        [node]
        proxy_address   = "http://127.0.0.1:9001"
        control_address = "http://127.0.0.1:9002"
        [node.tls]
        cert_path    = "c.crt"
        key_path     = "c.key"
        ca_cert_path = "ca.crt"

        [database]
        url             = "postgres://localhost/test"
        max_connections = 5

        [[module]]
        name      = "my-worker"
        namespace = "test"
        version   = "1.0.0"
        wasm_path = "wr-tests/guests/tracing-guest/target/wasm32-wasip2/debug/tracing_guest.wasm"
        mode      = "worker"
        database  = true
        worker_concurrency      = 8
        worker_poll_interval_secs = 5
        worker_job_timeout_secs = 600
        worker_max_attempts     = 5
    "#;
    let config: wr_engine::config::EngineConfig = toml::from_str(toml_str).unwrap();
    let m = &config.modules[0];
    assert_eq!(m.mode, wr_engine::config::ModuleMode::Worker);
    assert_eq!(m.worker_concurrency, 8);
    assert_eq!(m.worker_poll_interval_secs, 5);
    assert_eq!(m.worker_job_timeout_secs, 600);
    assert_eq!(m.worker_max_attempts, 5);
}

#[tokio::test]
async fn test_worker_config_defaults() {
    let toml_str = r#"
        listen_address  = "127.0.0.1:9100"
        [node]
        proxy_address   = "http://127.0.0.1:9001"
        control_address = "http://127.0.0.1:9002"
        [node.tls]
        cert_path    = "c.crt"
        key_path     = "c.key"
        ca_cert_path = "ca.crt"

        [database]
        url = "postgres://localhost/test"

        [[module]]
        name      = "my-worker"
        namespace = "test"
        version   = "1.0.0"
        wasm_path = "wr-tests/guests/tracing-guest/target/wasm32-wasip2/debug/tracing_guest.wasm"
        mode      = "worker"
        database  = true
    "#;
    let config: wr_engine::config::EngineConfig = toml::from_str(toml_str).unwrap();
    let m = &config.modules[0];
    assert_eq!(m.worker_concurrency, 4);
    assert_eq!(m.worker_poll_interval_secs, 2);
    assert_eq!(m.worker_job_timeout_secs, 300);
    assert_eq!(m.worker_max_attempts, 3);
}

#[tokio::test]
async fn test_worker_mode_service_default() {
    let toml_str = r#"
        listen_address  = "127.0.0.1:9100"
        [node]
        proxy_address   = "http://127.0.0.1:9001"
        control_address = "http://127.0.0.1:9002"
        [node.tls]
        cert_path    = "c.crt"
        key_path     = "c.key"
        ca_cert_path = "ca.crt"

        [[module]]
        name      = "svc"
        namespace = "test"
        version   = "1.0.0"
        wasm_path = "wr-tests/guests/tracing-guest/target/wasm32-wasip2/debug/tracing_guest.wasm"
    "#;
    let config: wr_engine::config::EngineConfig = toml::from_str(toml_str).unwrap();
    assert_eq!(
        config.modules[0].mode,
        wr_engine::config::ModuleMode::Service
    );
}

// ── worker job queue integration tests (require DB) ──────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn test_worker_job_submission_and_status_via_grpc() {
    let Some(harness) =
        WorkerPoolHarness::new("test_worker_job_submission_and_status_via_grpc", "grpc-mod").await
    else {
        return;
    };
    let pool = harness.pool.clone();

    // Start a minimal HTTP/2 engine server with worker gRPC endpoints.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let pool_clone = pool.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let pool = pool_clone.clone();
            tokio::spawn(async move {
                let io = hyper_util::rt::TokioIo::new(stream);
                let svc =
                    hyper::service::service_fn(move |req: http::Request<hyper::body::Incoming>| {
                        let pool = pool.clone();
                        async move {
                            let path = req.uri().path().to_owned();
                            let body = http_body_util::BodyExt::collect(req.into_body())
                                .await
                                .map(|c| c.to_bytes())
                                .unwrap_or_default();

                            let resp = match path.as_str() {
                                "/wruntime.WorkerService/SubmitJob" => {
                                    use prost::Message;
                                    let req =
                                        wr_common::wruntime::SubmitJobRequest::decode(&body[..])
                                            .unwrap();
                                    let max_attempts = if req.max_attempts > 0 {
                                        req.max_attempts
                                    } else {
                                        5
                                    };
                                    let job_id = wr_engine::worker::insert_job(
                                        &pool,
                                        &req.worker_namespace,
                                        &req.worker_name,
                                        &req.worker_version,
                                        &req.job_type,
                                        &req.payload,
                                        req.timeout_secs,
                                        max_attempts,
                                        "",
                                        "",
                                    )
                                    .await
                                    .unwrap();
                                    let resp = wr_common::wruntime::SubmitJobResponse { job_id };
                                    http::Response::builder()
                                        .status(200)
                                        .body(http_body_util::Full::new(bytes::Bytes::from(
                                            resp.encode_to_vec(),
                                        )))
                                        .unwrap()
                                }
                                "/wruntime.WorkerService/GetJobStatus" => {
                                    use prost::Message;
                                    let req =
                                        wr_common::wruntime::GetJobStatusRequest::decode(&body[..])
                                            .unwrap();
                                    let status =
                                        wr_engine::worker::get_job_status(&pool, &req.job_id)
                                            .await
                                            .unwrap();
                                    let resp = match status {
                                        Some(s) => wr_common::wruntime::GetJobStatusResponse {
                                            job_id: s.job_id,
                                            status: s.status,
                                            result: s.result,
                                            error_message: s.error_message,
                                            attempt: s.attempt,
                                            max_attempts: s.max_attempts,
                                        },
                                        None => wr_common::wruntime::GetJobStatusResponse {
                                            ..Default::default()
                                        },
                                    };
                                    http::Response::builder()
                                        .status(200)
                                        .body(http_body_util::Full::new(bytes::Bytes::from(
                                            resp.encode_to_vec(),
                                        )))
                                        .unwrap()
                                }
                                _ => http::Response::builder()
                                    .status(404)
                                    .body(http_body_util::Full::new(bytes::Bytes::from(
                                        "not found",
                                    )))
                                    .unwrap(),
                            };
                            Ok::<_, std::convert::Infallible>(resp)
                        }
                    });
                let _ =
                    hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
                        .serve_connection(io, svc)
                        .await;
            });
        }
    });

    // Submit a job via gRPC.
    use prost::Message;
    let submit_req = wr_common::wruntime::SubmitJobRequest {
        worker_namespace: "test-ns".into(),
        worker_name: "test-mod".into(),
        worker_version: "1.0.0".into(),
        job_type: "/test/Process".into(),
        payload: b"test-payload".to_vec(),
        timeout_secs: 60,
        max_attempts: 0,
    };
    let client = http_client();
    let resp = client
        .request(
            Request::builder()
                .method("POST")
                .uri(format!("http://{addr}/wruntime.WorkerService/SubmitJob"))
                .header("content-type", "application/x-protobuf")
                .body(Full::new(Bytes::from(submit_req.encode_to_vec())))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let submit_resp = wr_common::wruntime::SubmitJobResponse::decode(&body[..]).unwrap();
    assert!(!submit_resp.job_id.is_empty());

    // Get job status.
    let status_req = wr_common::wruntime::GetJobStatusRequest {
        job_id: submit_resp.job_id.clone(),
    };
    let resp = client
        .request(
            Request::builder()
                .method("POST")
                .uri(format!("http://{addr}/wruntime.WorkerService/GetJobStatus"))
                .header("content-type", "application/x-protobuf")
                .body(Full::new(Bytes::from(status_req.encode_to_vec())))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let status_resp = wr_common::wruntime::GetJobStatusResponse::decode(&body[..]).unwrap();
    assert_eq!(status_resp.job_id, submit_resp.job_id);
    assert_eq!(status_resp.status, "pending");
    assert_eq!(status_resp.max_attempts, 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_worker_grpc_submit_accepts_empty_body_worker_version() {
    let Some(harness) = WorkerPoolHarness::new(
        "test_worker_grpc_submit_accepts_empty_body_worker_version",
        "grpc-version-mod",
    )
    .await
    else {
        return;
    };
    let pool = harness.pool.clone();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let pool_clone = pool.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let pool = pool_clone.clone();
            tokio::spawn(async move {
                let io = hyper_util::rt::TokioIo::new(stream);
                let svc =
                    hyper::service::service_fn(move |req: http::Request<hyper::body::Incoming>| {
                        let pool = pool.clone();
                        async move {
                            let path = req.uri().path().to_owned();
                            let body = http_body_util::BodyExt::collect(req.into_body())
                                .await
                                .map(|c| c.to_bytes())
                                .unwrap_or_default();

                            let resp = match path.as_str() {
                                "/wruntime.WorkerService/SubmitJob" => {
                                    use prost::Message;
                                    let req =
                                        wr_common::wruntime::SubmitJobRequest::decode(&body[..])
                                            .unwrap();
                                    let job_id = wr_engine::worker::insert_job(
                                        &pool,
                                        &req.worker_namespace,
                                        &req.worker_name,
                                        &req.worker_version,
                                        &req.job_type,
                                        &req.payload,
                                        req.timeout_secs,
                                        req.max_attempts,
                                        "",
                                        "",
                                    )
                                    .await
                                    .unwrap();
                                    http::Response::builder()
                                        .status(200)
                                        .body(http_body_util::Full::new(bytes::Bytes::from(
                                            wr_common::wruntime::SubmitJobResponse { job_id }
                                                .encode_to_vec(),
                                        )))
                                        .unwrap()
                                }
                                _ => http::Response::builder()
                                    .status(404)
                                    .body(http_body_util::Full::new(bytes::Bytes::from(
                                        "not found",
                                    )))
                                    .unwrap(),
                            };
                            Ok::<_, std::convert::Infallible>(resp)
                        }
                    });
                let _ =
                    hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
                        .serve_connection(io, svc)
                        .await;
            });
        }
    });

    use prost::Message;
    let submit_req = wr_common::wruntime::SubmitJobRequest {
        worker_namespace: "test-ns".into(),
        worker_name: "test-mod".into(),
        worker_version: "".into(),
        job_type: "/test/Process".into(),
        payload: b"test-payload".to_vec(),
        timeout_secs: 60,
        max_attempts: 0,
    };
    let resp = http_client()
        .request(
            Request::builder()
                .method("POST")
                .uri(format!("http://{addr}/wruntime.WorkerService/SubmitJob"))
                .header("content-type", "application/x-protobuf")
                .header("x-wr-version", "1.0.0")
                .body(Full::new(Bytes::from(submit_req.encode_to_vec())))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let job_id = wr_common::wruntime::SubmitJobResponse::decode(&body[..])
        .unwrap()
        .job_id;
    let client = pool.get().await.unwrap();
    let version: String = client
        .query_one(
            "SELECT worker_version FROM wr__jobs.jobs WHERE job_id = $1",
            &[&job_id],
        )
        .await
        .unwrap()
        .get(0);
    assert!(version.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_worker_pool_dispatches_job_as_http() {
    let Some(mut harness) =
        WorkerPoolHarness::new("test_worker_pool_dispatches_job_as_http", "wpool-mod").await
    else {
        return;
    };
    let job_id = harness
        .insert_job("/test.svc/DoWork", b"job-payload", 60, 3)
        .await
        .unwrap();
    harness.spawn(1, Duration::from_millis(100), Duration::from_secs(10));

    // Wait for the worker to pick up the job and send it as an InboundRequest.
    let inbound = harness.recv_dispatch(Duration::from_secs(5)).await.unwrap();

    // Verify the request shape.
    assert_eq!(inbound.request.method(), "POST");
    assert_eq!(inbound.request.uri().path(), "/test.svc/DoWork");
    assert_eq!(
        inbound.request.headers().get("x-wr-job-id").unwrap(),
        &job_id
    );
    assert_eq!(inbound.request.body().as_ref(), b"job-payload");

    // Respond with 200 OK.
    WorkerPoolHarness::respond(inbound, 200, "done");

    let status = harness
        .wait_for_status(&job_id, "complete", Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(status.status, "complete");
    assert_eq!(status.result, b"done");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_worker_pool_retries_on_failure() {
    let Some(mut harness) =
        WorkerPoolHarness::new("test_worker_pool_retries_on_failure", "retry-mod").await
    else {
        return;
    };
    let job_id = harness.insert_job("/test/Retry", b"", 60, 2).await.unwrap();
    harness.spawn(1, Duration::from_millis(100), Duration::from_secs(10));

    // First dispatch: respond with 500 → should fail and retry.
    let inbound = harness.recv_dispatch(Duration::from_secs(5)).await.unwrap();
    WorkerPoolHarness::respond(inbound, 500, "error");

    // Wait for retry — the job should be reset to pending and re-dispatched.
    let inbound2 = harness.recv_dispatch(Duration::from_secs(5)).await.unwrap();
    assert_eq!(inbound2.request.uri().path(), "/test/Retry");

    // Second attempt: respond with 200.
    WorkerPoolHarness::respond(inbound2, 200, "ok");

    let status = harness
        .wait_for_status(&job_id, "complete", Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(status.status, "complete");
    assert_eq!(status.attempt, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_worker_pool_marks_dead_after_max_attempts() {
    let Some(mut harness) =
        WorkerPoolHarness::new("test_worker_pool_marks_dead_after_max_attempts", "dead-mod").await
    else {
        return;
    };
    // max_attempts = 1 — first failure should mark it dead.
    let job_id = harness.insert_job("/test/Die", b"", 60, 1).await.unwrap();
    harness.spawn(1, Duration::from_millis(100), Duration::from_secs(10));

    let inbound = harness.recv_dispatch(Duration::from_secs(5)).await.unwrap();
    WorkerPoolHarness::respond(inbound, 500, "fatal");

    let status = harness
        .wait_for_status(&job_id, "dead", Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(status.status, "dead");
    assert!(status.error_message.contains("HTTP 500"));

    // Verify no retry — channel should be empty.
    harness
        .expect_no_dispatch(Duration::from_millis(500))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_worker_pool_handles_dropped_response() {
    let Some(mut harness) =
        WorkerPoolHarness::new("test_worker_pool_handles_dropped_response", "drop-mod").await
    else {
        return;
    };
    let job_id = harness.insert_job("/test/Drop", b"", 60, 2).await.unwrap();
    harness.spawn(1, Duration::from_millis(100), Duration::from_secs(10));

    // Receive the dispatch but drop the response_tx without sending — simulates
    // a module crash.
    let inbound = harness.recv_dispatch(Duration::from_secs(5)).await.unwrap();
    drop(inbound.response_tx);

    let status = harness
        .wait_for_status_matching(
            &job_id,
            "dropped worker response records retryable status",
            Duration::from_secs(5),
            |status| {
                (status.status == "pending" || status.status == "running")
                    && status.error_message.contains("module dropped response")
            },
        )
        .await
        .unwrap();
    // Should be pending again (retryable) with error message.
    assert!(
        status.status == "pending" || status.status == "running",
        "expected pending or running for retry, got: {}",
        status.status
    );
    assert!(status.error_message.contains("module dropped response"));
}

// ── Worker pool timeout, claiming, notification, and recovery tests ─────────

#[tokio::test(flavor = "multi_thread")]
async fn test_worker_pool_job_timeout() {
    let Some(mut harness) =
        WorkerPoolHarness::new("test_worker_pool_job_timeout", "timeout-mod").await
    else {
        return;
    };
    let job_id = harness
        .insert_job("/test/Slow", b"data", 60, 1)
        .await
        .unwrap();
    harness.spawn(1, Duration::from_millis(100), Duration::from_millis(200));

    // Receive the dispatch but never respond — let the timeout fire.
    let inbound = harness.recv_dispatch(Duration::from_secs(5)).await.unwrap();
    // Hold onto response_tx without sending — the worker's timeout will fire.
    let _hold = inbound.response_tx;

    let status = harness
        .wait_for_status(&job_id, "dead", Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(
        status.status, "dead",
        "max_attempts=1 so timeout should mark dead"
    );
    assert!(
        status.error_message.contains("job timed out"),
        "expected timeout error, got: {}",
        status.error_message,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_worker_concurrent_claim_across_engines() {
    let Some(harness) =
        WorkerPoolHarness::new("test_worker_concurrent_claim_across_engines", "cc-mod").await
    else {
        return;
    };

    // Insert two jobs.
    let id1 = harness.insert_job("/test/A", b"", 60, 3).await.unwrap();
    // Intentional ordering control for created_at; not readiness/convergence.
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    let id2 = harness.insert_job("/test/B", b"", 60, 3).await.unwrap();

    // Two engines claim concurrently — each should get a different job.
    let (claim1, claim2) = tokio::join!(
        wr_engine::worker::claim_job(
            &harness.pool,
            &harness.namespace,
            &harness.name,
            &harness.version,
            "engine-a",
        ),
        wr_engine::worker::claim_job(
            &harness.pool,
            &harness.namespace,
            &harness.name,
            &harness.version,
            "engine-b",
        ),
    );

    let c1 = claim1.unwrap().expect("engine-a should claim a job");
    let c2 = claim2.unwrap().expect("engine-b should claim a job");
    assert_ne!(
        c1.job_id, c2.job_id,
        "each engine must claim a different job"
    );

    // Together they should have claimed both jobs.
    let mut claimed_ids = vec![c1.job_id, c2.job_id];
    claimed_ids.sort();
    let mut expected = vec![id1, id2];
    expected.sort();
    assert_eq!(claimed_ids, expected);

    // No more jobs to claim.
    let c3 = wr_engine::worker::claim_job(
        &harness.pool,
        &harness.namespace,
        &harness.name,
        &harness.version,
        "engine-c",
    )
    .await
    .unwrap();
    assert!(c3.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_worker_pool_multiple_concurrent_workers() {
    let Some(mut harness) =
        WorkerPoolHarness::new("test_worker_pool_multiple_concurrent_workers", "batch-mod").await
    else {
        return;
    };

    // Insert 4 jobs.
    let mut job_ids = Vec::new();
    for i in 0..4 {
        let id = harness
            .insert_job(
                &format!("/test/Job{i}"),
                format!("payload-{i}").as_bytes(),
                60,
                3,
            )
            .await
            .unwrap();
        job_ids.push(id);
    }

    harness.spawn(3, Duration::from_millis(100), Duration::from_secs(10));

    // Receive and respond to all 4 jobs.
    let mut received_ids = Vec::new();
    for _ in 0..4 {
        let inbound = harness.recv_dispatch(Duration::from_secs(5)).await.unwrap();
        let jid = inbound
            .request
            .headers()
            .get("x-wr-job-id")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        received_ids.push(jid);
        WorkerPoolHarness::respond(inbound, 200, "ok");
    }

    // All 4 jobs should have been dispatched.
    received_ids.sort();
    let mut expected = job_ids.clone();
    expected.sort();
    assert_eq!(received_ids, expected, "all jobs should be dispatched");

    for id in &job_ids {
        let status = harness
            .wait_for_status(id, "complete", Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(status.status, "complete", "job {id} should be complete");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_worker_listen_notify_immediate_wake() {
    let Some(mut harness) =
        WorkerPoolHarness::new("test_worker_listen_notify_immediate_wake", "notify-mod").await
    else {
        return;
    };

    // Start the worker pool FIRST with a very long poll interval.
    // If LISTEN/NOTIFY works, the job will be picked up much sooner.
    harness.spawn(1, Duration::from_secs(60), Duration::from_secs(10));
    harness
        .wait_for_listener(Duration::from_secs(5))
        .await
        .unwrap();

    // NOW insert a job — NOTIFY should wake the worker immediately.
    let _job_id = harness
        .insert_job("/test/Wake", b"ping", 60, 3)
        .await
        .unwrap();

    // Should be dispatched well within 5 seconds (would take 60s without NOTIFY).
    let inbound = harness.recv_dispatch(Duration::from_secs(5)).await.unwrap();

    assert_eq!(inbound.request.uri().path(), "/test/Wake");

    WorkerPoolHarness::respond(inbound, 200, "ok");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_worker_listen_notify_wakes_for_unversioned_job() {
    let Some(mut harness) = WorkerPoolHarness::new(
        "test_worker_listen_notify_wakes_for_unversioned_job",
        "notify-any-version-mod",
    )
    .await
    else {
        return;
    };

    harness.spawn(1, Duration::from_secs(60), Duration::from_secs(10));
    harness
        .wait_for_listener(Duration::from_secs(5))
        .await
        .unwrap();

    wr_engine::worker::insert_job(
        &harness.pool,
        &harness.namespace,
        &harness.name,
        "",
        "/test/WakeAnyVersion",
        b"ping",
        60,
        3,
        "",
        "",
    )
    .await
    .unwrap();

    let inbound = harness.recv_dispatch(Duration::from_secs(5)).await.unwrap();
    assert_eq!(inbound.request.uri().path(), "/test/WakeAnyVersion");
    WorkerPoolHarness::respond(inbound, 200, "ok");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_worker_listen_notify_is_version_scoped() {
    let Some(mut v1) = WorkerPoolHarness::new(
        "test_worker_listen_notify_is_version_scoped_v1",
        "notify-version-mod",
    )
    .await
    else {
        return;
    };
    let Some(mut v2) = WorkerPoolHarness::new(
        "test_worker_listen_notify_is_version_scoped_v2",
        "notify-version-mod",
    )
    .await
    else {
        return;
    };
    v2.namespace.clone_from(&v1.namespace);
    v2.version = "2.0.0".to_string();

    v1.spawn(1, Duration::from_secs(60), Duration::from_secs(10));
    v2.spawn(1, Duration::from_secs(60), Duration::from_secs(10));
    v1.wait_for_listener(Duration::from_secs(5)).await.unwrap();
    v2.wait_for_listener(Duration::from_secs(5)).await.unwrap();

    let _job_id = v2.insert_job("/test/WakeV2", b"ping", 60, 3).await.unwrap();

    let inbound_v2 = v2.recv_dispatch(Duration::from_secs(5)).await.unwrap();
    assert_eq!(inbound_v2.request.uri().path(), "/test/WakeV2");
    v1.expect_no_dispatch(Duration::from_millis(500))
        .await
        .unwrap();

    WorkerPoolHarness::respond(inbound_v2, 200, "ok");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_worker_stale_recovery_marks_dead_when_exhausted() {
    let Some(harness) = WorkerPoolHarness::new(
        "test_worker_stale_recovery_marks_dead_when_exhausted",
        "stale-mod",
    )
    .await
    else {
        return;
    };

    // Insert with max_attempts=1, timeout_secs=1.
    let id = harness.insert_job("/test/Stale", b"", 1, 1).await.unwrap();

    // Claim it (attempt becomes 1, which equals max_attempts).
    let _ = wr_engine::worker::claim_job(
        &harness.pool,
        &harness.namespace,
        &harness.name,
        &harness.version,
        "engine-1",
    )
    .await
    .unwrap();

    // Backdate claimed_at so it appears stale.
    let client = harness.pool.get().await.unwrap();
    client
        .execute(
            "UPDATE wr__jobs.jobs SET claimed_at = now() - interval '10 seconds' WHERE job_id = $1",
            &[&id],
        )
        .await
        .unwrap();
    drop(client);

    // Another test's worker pool stale-recovery task may have already recovered
    // this job, so don't assert on the count — just ensure recovery ran and
    // verify the final job state.
    let _ = wr_engine::worker::recover_stale_jobs(&harness.pool)
        .await
        .unwrap();

    let status = wr_engine::worker::get_job_status(&harness.pool, &id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.status, "dead", "exhausted stale job should be dead");
    assert!(status.error_message.contains("[stale recovery]"));
}

#[tokio::test]
async fn test_worker_config_validation_requires_database() {
    // Use Cargo.toml as a placeholder for wasm_path — any existing file passes the
    // path check. We only care about the worker-requires-database validation.
    let toml_str = r#"
        listen_address  = "127.0.0.1:9100"
        [node]
        proxy_address   = "http://127.0.0.1:9001"
        control_address = "http://127.0.0.1:9002"
        [node.tls]
        cert_path    = "c.crt"
        key_path     = "c.key"
        ca_cert_path = "ca.crt"

        [[module]]
        name      = "my-worker"
        namespace = "test"
        version   = "1.0.0"
        wasm_path = "Cargo.toml"
        mode      = "worker"
        database  = false
    "#;
    let config: wr_engine::config::EngineConfig = toml::from_str(toml_str).unwrap();
    let err = config.validate().unwrap_err();
    assert!(
        format!("{err:#}").contains("job queue requires database"),
        "unexpected error: {err:#}",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_worker_source_metadata_stored() {
    let Some(mut harness) =
        WorkerPoolHarness::new("test_worker_source_metadata_stored", "meta-mod").await
    else {
        return;
    };
    harness.version = "2.0.0".to_string();

    let id = harness
        .insert_job_with_source("/test/Meta", b"data", 120, 5, "caller-ns", "caller-mod")
        .await
        .unwrap();

    // Verify the full row including source metadata and custom values.
    let client = harness.pool.get().await.unwrap();
    let row = client
        .query_one(
            "SELECT worker_namespace, worker_name, worker_version, job_type, \
             payload, timeout_secs, max_attempts, source_namespace, source_module \
             FROM wr__jobs.jobs WHERE job_id = $1",
            &[&id],
        )
        .await
        .unwrap();

    assert_eq!(row.get::<_, String>(0), harness.namespace);
    assert_eq!(row.get::<_, String>(1), "meta-mod");
    assert_eq!(row.get::<_, String>(2), "2.0.0");
    assert_eq!(row.get::<_, String>(3), "/test/Meta");
    assert_eq!(row.get::<_, Vec<u8>>(4), b"data");
    assert_eq!(row.get::<_, i32>(5), 120);
    assert_eq!(row.get::<_, i32>(6), 5);
    assert_eq!(row.get::<_, String>(7), "caller-ns");
    assert_eq!(row.get::<_, String>(8), "caller-mod");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_worker_grpc_get_status_not_found() {
    let Some(harness) =
        WorkerPoolHarness::new("test_worker_grpc_get_status_not_found", "grpc-status-mod").await
    else {
        return;
    };
    let pool = harness.pool.clone();

    // Start a minimal HTTP/2 server with the GetJobStatus endpoint.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let pool_clone = pool.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let pool = pool_clone.clone();
            tokio::spawn(async move {
                let io = hyper_util::rt::TokioIo::new(stream);
                let svc =
                    hyper::service::service_fn(move |req: http::Request<hyper::body::Incoming>| {
                        let pool = pool.clone();
                        async move {
                            let body = http_body_util::BodyExt::collect(req.into_body())
                                .await
                                .map(|c| c.to_bytes())
                                .unwrap_or_default();

                            use prost::Message;
                            let req = wr_common::wruntime::GetJobStatusRequest::decode(&body[..])
                                .unwrap();
                            let status =
                                wr_engine::worker::get_job_status(&pool, &req.job_id).await;

                            let resp = match status {
                                Ok(Some(s)) => http::Response::builder()
                                    .status(200)
                                    .body(http_body_util::Full::new(bytes::Bytes::from(
                                        wr_common::wruntime::GetJobStatusResponse {
                                            job_id: s.job_id,
                                            status: s.status,
                                            result: s.result,
                                            error_message: s.error_message,
                                            attempt: s.attempt,
                                            max_attempts: s.max_attempts,
                                        }
                                        .encode_to_vec(),
                                    )))
                                    .unwrap(),
                                Ok(None) => http::Response::builder()
                                    .status(404)
                                    .body(http_body_util::Full::new(bytes::Bytes::from(
                                        "job not found",
                                    )))
                                    .unwrap(),
                                Err(_) => http::Response::builder()
                                    .status(500)
                                    .body(http_body_util::Full::new(bytes::Bytes::from(
                                        "internal error",
                                    )))
                                    .unwrap(),
                            };
                            Ok::<_, std::convert::Infallible>(resp)
                        }
                    });
                let _ =
                    hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
                        .serve_connection(io, svc)
                        .await;
            });
        }
    });

    let client = http_client();

    // Query a nonexistent job — should get 404.
    use prost::Message;
    let req = wr_common::wruntime::GetJobStatusRequest {
        job_id: "does-not-exist-12345".into(),
    };
    let resp = client
        .request(
            Request::builder()
                .method("POST")
                .uri(format!("http://{addr}/wruntime.WorkerService/GetJobStatus"))
                .header("content-type", "application/x-protobuf")
                .body(Full::new(Bytes::from(req.encode_to_vec())))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_worker_pool_preserves_payload_and_job_type() {
    let Some(mut harness) = WorkerPoolHarness::new(
        "test_worker_pool_preserves_payload_and_job_type",
        "payload-mod",
    )
    .await
    else {
        return;
    };

    // Use a non-trivial payload and multi-segment path.
    let payload = vec![0u8, 1, 2, 255, 254, 128];
    let job_type = "/api/v2/process/heavy";

    let job_id = harness.insert_job(job_type, &payload, 60, 3).await.unwrap();
    harness.spawn(1, Duration::from_millis(100), Duration::from_secs(10));

    let inbound = harness.recv_dispatch(Duration::from_secs(5)).await.unwrap();

    // Verify exact path and binary payload survive the round-trip.
    assert_eq!(inbound.request.uri().path(), job_type);
    assert_eq!(inbound.request.body().as_ref(), &payload);
    assert_eq!(
        inbound
            .request
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "application/x-protobuf",
    );
    assert_eq!(
        inbound
            .request
            .headers()
            .get("x-wr-job-id")
            .unwrap()
            .to_str()
            .unwrap(),
        job_id,
    );

    WorkerPoolHarness::respond(inbound, 200, "ok");
}
