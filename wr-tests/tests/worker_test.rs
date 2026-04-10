#[allow(dead_code, unused_imports)]
mod helpers;
use helpers::*;

use bytes::Bytes;
use http::{Request, StatusCode};
use http_body_util::{BodyExt, Full};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use wr_common::config::Validatable;

static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Unique namespace prefix so parallel tests don't collide.
fn unique_ns() -> String {
    let n = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("it{n}_{ts}")
}

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
    if std::env::var("WRT_TEST_DB_URL").is_err() {
        eprintln!("skipping test_worker_job_submission_and_status_via_grpc (no WRT_TEST_DB_URL)");
        return;
    }
    let pool = worker_pool().await;
    let pool = Arc::new(pool);

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
        max_attempts: 3,
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
    assert_eq!(status_resp.max_attempts, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_worker_pool_dispatches_job_as_http() {
    if std::env::var("WRT_TEST_DB_URL").is_err() {
        eprintln!("skipping test_worker_pool_dispatches_job_as_http (no WRT_TEST_DB_URL)");
        return;
    }
    let pool = worker_pool().await;
    let pool = Arc::new(pool);

    // Create a channel that the worker pool will send InboundRequests into.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<wr_engine::InboundRequest>(16);

    // Insert a job.
    let job_id = wr_engine::worker::insert_job(
        &pool,
        "wpool-ns",
        "wpool-mod",
        "1.0.0",
        "/test.svc/DoWork",
        b"job-payload",
        60,
        3,
        "",
        "",
    )
    .await
    .unwrap();

    // Spawn the worker pool.
    let db_url = std::env::var("WRT_TEST_DB_URL").unwrap();
    wr_engine::worker::spawn_worker_pool(
        pool.clone(),
        wr_engine::worker::WorkerPoolConfig {
            namespace: "wpool-ns".into(),
            name: "wpool-mod".into(),
            version: "1.0.0".into(),
            engine_id: "test-engine".into(),
            concurrency: 1,
            poll_interval: std::time::Duration::from_millis(100),
            job_timeout: std::time::Duration::from_secs(10),
            database_url: db_url,
        },
        tx,
    );

    // Wait for the worker to pick up the job and send it as an InboundRequest.
    let inbound = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("timeout waiting for worker dispatch")
        .expect("channel closed");

    // Verify the request shape.
    assert_eq!(inbound.request.method(), "POST");
    assert_eq!(inbound.request.uri().path(), "/test.svc/DoWork");
    assert_eq!(
        inbound.request.headers().get("x-wr-job-id").unwrap(),
        &job_id
    );
    assert_eq!(inbound.request.body().as_ref(), b"job-payload");

    // Respond with 200 OK.
    let response = http::Response::builder()
        .status(200)
        .body(Bytes::from("done"))
        .unwrap();
    inbound.response_tx.send(response).unwrap();

    // Wait for the worker to update the job status.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let status = wr_engine::worker::get_job_status(&pool, &job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.status, "complete");
    assert_eq!(status.result, b"done");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_worker_pool_retries_on_failure() {
    if std::env::var("WRT_TEST_DB_URL").is_err() {
        eprintln!("skipping test_worker_pool_retries_on_failure (no WRT_TEST_DB_URL)");
        return;
    }
    let pool = worker_pool().await;
    let pool = Arc::new(pool);

    let (tx, mut rx) = tokio::sync::mpsc::channel::<wr_engine::InboundRequest>(16);

    let job_id = wr_engine::worker::insert_job(
        &pool,
        "retry-ns",
        "retry-mod",
        "1.0.0",
        "/test/Retry",
        b"",
        60,
        2,
        "",
        "",
    )
    .await
    .unwrap();

    let db_url = std::env::var("WRT_TEST_DB_URL").unwrap();
    wr_engine::worker::spawn_worker_pool(
        pool.clone(),
        wr_engine::worker::WorkerPoolConfig {
            namespace: "retry-ns".into(),
            name: "retry-mod".into(),
            version: "1.0.0".into(),
            engine_id: "test-engine".into(),
            concurrency: 1,
            poll_interval: std::time::Duration::from_millis(100),
            job_timeout: std::time::Duration::from_secs(10),
            database_url: db_url,
        },
        tx,
    );

    // First dispatch: respond with 500 → should fail and retry.
    let inbound = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("timeout")
        .expect("closed");
    let response = http::Response::builder()
        .status(500)
        .body(Bytes::from("error"))
        .unwrap();
    inbound.response_tx.send(response).unwrap();

    // Wait for retry — the job should be reset to pending and re-dispatched.
    let inbound2 = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("timeout on retry")
        .expect("closed");
    assert_eq!(inbound2.request.uri().path(), "/test/Retry");

    // Second attempt: respond with 200.
    let response = http::Response::builder()
        .status(200)
        .body(Bytes::from("ok"))
        .unwrap();
    inbound2.response_tx.send(response).unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let status = wr_engine::worker::get_job_status(&pool, &job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.status, "complete");
    assert_eq!(status.attempt, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_worker_pool_marks_dead_after_max_attempts() {
    if std::env::var("WRT_TEST_DB_URL").is_err() {
        eprintln!("skipping test_worker_pool_marks_dead_after_max_attempts (no WRT_TEST_DB_URL)");
        return;
    }
    let pool = worker_pool().await;
    let pool = Arc::new(pool);

    let (tx, mut rx) = tokio::sync::mpsc::channel::<wr_engine::InboundRequest>(16);

    // max_attempts = 1 — first failure should mark it dead.
    let job_id = wr_engine::worker::insert_job(
        &pool,
        "dead-ns",
        "dead-mod",
        "1.0.0",
        "/test/Die",
        b"",
        60,
        1,
        "",
        "",
    )
    .await
    .unwrap();

    let db_url = std::env::var("WRT_TEST_DB_URL").unwrap();
    wr_engine::worker::spawn_worker_pool(
        pool.clone(),
        wr_engine::worker::WorkerPoolConfig {
            namespace: "dead-ns".into(),
            name: "dead-mod".into(),
            version: "1.0.0".into(),
            engine_id: "test-engine".into(),
            concurrency: 1,
            poll_interval: std::time::Duration::from_millis(100),
            job_timeout: std::time::Duration::from_secs(10),
            database_url: db_url,
        },
        tx,
    );

    let inbound = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("timeout")
        .expect("closed");
    let response = http::Response::builder()
        .status(500)
        .body(Bytes::from("fatal"))
        .unwrap();
    inbound.response_tx.send(response).unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let status = wr_engine::worker::get_job_status(&pool, &job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.status, "dead");
    assert!(status.error_message.contains("HTTP 500"));

    // Verify no retry — channel should be empty.
    let result = tokio::time::timeout(std::time::Duration::from_millis(500), rx.recv()).await;
    assert!(result.is_err(), "should not dispatch dead job again");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_worker_pool_handles_dropped_response() {
    if std::env::var("WRT_TEST_DB_URL").is_err() {
        eprintln!("skipping test_worker_pool_handles_dropped_response (no WRT_TEST_DB_URL)");
        return;
    }
    let pool = worker_pool().await;
    let pool = Arc::new(pool);

    let (tx, mut rx) = tokio::sync::mpsc::channel::<wr_engine::InboundRequest>(16);

    let job_id = wr_engine::worker::insert_job(
        &pool,
        "drop-ns",
        "drop-mod",
        "1.0.0",
        "/test/Drop",
        b"",
        60,
        2,
        "",
        "",
    )
    .await
    .unwrap();

    let db_url = std::env::var("WRT_TEST_DB_URL").unwrap();
    wr_engine::worker::spawn_worker_pool(
        pool.clone(),
        wr_engine::worker::WorkerPoolConfig {
            namespace: "drop-ns".into(),
            name: "drop-mod".into(),
            version: "1.0.0".into(),
            engine_id: "test-engine".into(),
            concurrency: 1,
            poll_interval: std::time::Duration::from_millis(100),
            job_timeout: std::time::Duration::from_secs(10),
            database_url: db_url,
        },
        tx,
    );

    // Receive the dispatch but drop the response_tx without sending — simulates
    // a module crash.
    let inbound = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("timeout")
        .expect("closed");
    drop(inbound.response_tx);

    // Wait for failure handling + retry.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let status = wr_engine::worker::get_job_status(&pool, &job_id)
        .await
        .unwrap()
        .unwrap();
    // Should be pending again (retryable) with error message.
    assert!(
        status.status == "pending" || status.status == "running",
        "expected pending or running for retry, got: {}",
        status.status
    );
    assert!(status.error_message.contains("module dropped response"));
}

// ── New integration tests ───────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn test_worker_pool_job_timeout() {
    if std::env::var("WRT_TEST_DB_URL").is_err() {
        eprintln!("skipping test_worker_pool_job_timeout (no WRT_TEST_DB_URL)");
        return;
    }
    let pool = worker_pool().await;
    let pool = Arc::new(pool);
    let ns = unique_ns();

    let (tx, mut rx) = tokio::sync::mpsc::channel::<wr_engine::InboundRequest>(16);

    let job_id = wr_engine::worker::insert_job(
        &pool,
        &ns,
        "timeout-mod",
        "1.0.0",
        "/test/Slow",
        b"data",
        60,
        1,
        "",
        "",
    )
    .await
    .unwrap();

    let db_url = std::env::var("WRT_TEST_DB_URL").unwrap();
    wr_engine::worker::spawn_worker_pool(
        pool.clone(),
        wr_engine::worker::WorkerPoolConfig {
            namespace: ns.clone(),
            name: "timeout-mod".into(),
            version: "1.0.0".into(),
            engine_id: "test-engine".into(),
            concurrency: 1,
            poll_interval: std::time::Duration::from_millis(100),
            // Very short timeout so the test doesn't take long.
            job_timeout: std::time::Duration::from_millis(200),
            database_url: db_url,
        },
        tx,
    );

    // Receive the dispatch but never respond — let the timeout fire.
    let inbound = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("timeout waiting for dispatch")
        .expect("channel closed");
    // Hold onto response_tx without sending — the worker's timeout will fire.
    let _hold = inbound.response_tx;

    // Wait for the timeout + DB update.
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    let status = wr_engine::worker::get_job_status(&pool, &job_id)
        .await
        .unwrap()
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
    if std::env::var("WRT_TEST_DB_URL").is_err() {
        eprintln!("skipping test_worker_concurrent_claim_across_engines (no WRT_TEST_DB_URL)");
        return;
    }
    let pool = worker_pool().await;
    let pool = Arc::new(pool);
    let ns = unique_ns();

    // Insert two jobs.
    let id1 =
        wr_engine::worker::insert_job(&pool, &ns, "cc-mod", "1.0.0", "/test/A", b"", 60, 3, "", "")
            .await
            .unwrap();
    // Small delay so created_at ordering is deterministic.
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    let id2 =
        wr_engine::worker::insert_job(&pool, &ns, "cc-mod", "1.0.0", "/test/B", b"", 60, 3, "", "")
            .await
            .unwrap();

    // Two engines claim concurrently — each should get a different job.
    let (claim1, claim2) = tokio::join!(
        wr_engine::worker::claim_job(&pool, &ns, "cc-mod", "engine-a"),
        wr_engine::worker::claim_job(&pool, &ns, "cc-mod", "engine-b"),
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
    let c3 = wr_engine::worker::claim_job(&pool, &ns, "cc-mod", "engine-c")
        .await
        .unwrap();
    assert!(c3.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_worker_pool_multiple_concurrent_workers() {
    if std::env::var("WRT_TEST_DB_URL").is_err() {
        eprintln!("skipping test_worker_pool_multiple_concurrent_workers (no WRT_TEST_DB_URL)");
        return;
    }
    let pool = worker_pool().await;
    let pool = Arc::new(pool);
    let ns = unique_ns();

    let (tx, mut rx) = tokio::sync::mpsc::channel::<wr_engine::InboundRequest>(32);

    // Insert 4 jobs.
    let mut job_ids = Vec::new();
    for i in 0..4 {
        let id = wr_engine::worker::insert_job(
            &pool,
            &ns,
            "batch-mod",
            "1.0.0",
            &format!("/test/Job{i}"),
            format!("payload-{i}").as_bytes(),
            60,
            3,
            "",
            "",
        )
        .await
        .unwrap();
        job_ids.push(id);
    }

    let db_url = std::env::var("WRT_TEST_DB_URL").unwrap();
    wr_engine::worker::spawn_worker_pool(
        pool.clone(),
        wr_engine::worker::WorkerPoolConfig {
            namespace: ns.clone(),
            name: "batch-mod".into(),
            version: "1.0.0".into(),
            engine_id: "test-engine".into(),
            concurrency: 3,
            poll_interval: std::time::Duration::from_millis(100),
            job_timeout: std::time::Duration::from_secs(10),
            database_url: db_url,
        },
        tx,
    );

    // Receive and respond to all 4 jobs.
    let mut received_ids = Vec::new();
    for _ in 0..4 {
        let inbound = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .expect("timeout waiting for job dispatch")
            .expect("channel closed");
        let jid = inbound
            .request
            .headers()
            .get("x-wr-job-id")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        received_ids.push(jid);
        let response = http::Response::builder()
            .status(200)
            .body(Bytes::from("ok"))
            .unwrap();
        inbound.response_tx.send(response).unwrap();
    }

    // All 4 jobs should have been dispatched.
    received_ids.sort();
    let mut expected = job_ids.clone();
    expected.sort();
    assert_eq!(received_ids, expected, "all jobs should be dispatched");

    // Wait for DB updates.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    for id in &job_ids {
        let status = wr_engine::worker::get_job_status(&pool, id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.status, "complete", "job {id} should be complete");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_worker_listen_notify_immediate_wake() {
    if std::env::var("WRT_TEST_DB_URL").is_err() {
        eprintln!("skipping test_worker_listen_notify_immediate_wake (no WRT_TEST_DB_URL)");
        return;
    }
    let pool = worker_pool().await;
    let pool = Arc::new(pool);
    let ns = unique_ns();

    let (tx, mut rx) = tokio::sync::mpsc::channel::<wr_engine::InboundRequest>(16);

    // Start the worker pool FIRST with a very long poll interval.
    // If LISTEN/NOTIFY works, the job will be picked up much sooner.
    let db_url = std::env::var("WRT_TEST_DB_URL").unwrap();
    wr_engine::worker::spawn_worker_pool(
        pool.clone(),
        wr_engine::worker::WorkerPoolConfig {
            namespace: ns.clone(),
            name: "notify-mod".into(),
            version: "1.0.0".into(),
            engine_id: "test-engine".into(),
            concurrency: 1,
            // Long poll interval — if NOTIFY doesn't work, test would time out.
            poll_interval: std::time::Duration::from_secs(60),
            job_timeout: std::time::Duration::from_secs(10),
            database_url: db_url,
        },
        tx,
    );

    // Give the LISTEN connection time to establish.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // NOW insert a job — NOTIFY should wake the worker immediately.
    let _job_id = wr_engine::worker::insert_job(
        &pool,
        &ns,
        "notify-mod",
        "1.0.0",
        "/test/Wake",
        b"ping",
        60,
        3,
        "",
        "",
    )
    .await
    .unwrap();

    // Should be dispatched well within 5 seconds (would take 60s without NOTIFY).
    let inbound = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("LISTEN/NOTIFY failed — job not dispatched within 5s")
        .expect("channel closed");

    assert_eq!(inbound.request.uri().path(), "/test/Wake");

    let response = http::Response::builder()
        .status(200)
        .body(Bytes::from("ok"))
        .unwrap();
    inbound.response_tx.send(response).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_worker_stale_recovery_marks_dead_when_exhausted() {
    if std::env::var("WRT_TEST_DB_URL").is_err() {
        eprintln!(
            "skipping test_worker_stale_recovery_marks_dead_when_exhausted (no WRT_TEST_DB_URL)"
        );
        return;
    }
    let pool = worker_pool().await;
    let ns = unique_ns();

    // Insert with max_attempts=1, timeout_secs=1.
    let id = wr_engine::worker::insert_job(
        &pool,
        &ns,
        "stale-mod",
        "1.0.0",
        "/test/Stale",
        b"",
        1,
        1,
        "",
        "",
    )
    .await
    .unwrap();

    // Claim it (attempt becomes 1, which equals max_attempts).
    let _ = wr_engine::worker::claim_job(&pool, &ns, "stale-mod", "engine-1")
        .await
        .unwrap();

    // Backdate claimed_at so it appears stale.
    let client = pool.get().await.unwrap();
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
    let _ = wr_engine::worker::recover_stale_jobs(&pool).await.unwrap();

    let status = wr_engine::worker::get_job_status(&pool, &id)
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
    if std::env::var("WRT_TEST_DB_URL").is_err() {
        eprintln!("skipping test_worker_source_metadata_stored (no WRT_TEST_DB_URL)");
        return;
    }
    let pool = worker_pool().await;
    let ns = unique_ns();

    let id = wr_engine::worker::insert_job(
        &pool,
        &ns,
        "meta-mod",
        "2.0.0",
        "/test/Meta",
        b"data",
        120,
        5,
        "caller-ns",
        "caller-mod",
    )
    .await
    .unwrap();

    // Verify the full row including source metadata and custom values.
    let client = pool.get().await.unwrap();
    let row = client
        .query_one(
            "SELECT worker_namespace, worker_name, worker_version, job_type, \
             payload, timeout_secs, max_attempts, source_namespace, source_module \
             FROM wr__jobs.jobs WHERE job_id = $1",
            &[&id],
        )
        .await
        .unwrap();

    assert_eq!(row.get::<_, String>(0), ns);
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
    if std::env::var("WRT_TEST_DB_URL").is_err() {
        eprintln!("skipping test_worker_grpc_get_status_not_found (no WRT_TEST_DB_URL)");
        return;
    }
    let pool = worker_pool().await;
    let pool = Arc::new(pool);

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
    if std::env::var("WRT_TEST_DB_URL").is_err() {
        eprintln!("skipping test_worker_pool_preserves_payload_and_job_type (no WRT_TEST_DB_URL)");
        return;
    }
    let pool = worker_pool().await;
    let pool = Arc::new(pool);
    let ns = unique_ns();

    let (tx, mut rx) = tokio::sync::mpsc::channel::<wr_engine::InboundRequest>(16);

    // Use a non-trivial payload and multi-segment path.
    let payload = vec![0u8, 1, 2, 255, 254, 128];
    let job_type = "/api/v2/process/heavy";

    let job_id = wr_engine::worker::insert_job(
        &pool,
        &ns,
        "payload-mod",
        "1.0.0",
        job_type,
        &payload,
        60,
        3,
        "",
        "",
    )
    .await
    .unwrap();

    let db_url = std::env::var("WRT_TEST_DB_URL").unwrap();
    wr_engine::worker::spawn_worker_pool(
        pool.clone(),
        wr_engine::worker::WorkerPoolConfig {
            namespace: ns.clone(),
            name: "payload-mod".into(),
            version: "1.0.0".into(),
            engine_id: "test-engine".into(),
            concurrency: 1,
            poll_interval: std::time::Duration::from_millis(100),
            job_timeout: std::time::Duration::from_secs(10),
            database_url: db_url,
        },
        tx,
    );

    let inbound = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("timeout")
        .expect("closed");

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

    let response = http::Response::builder()
        .status(200)
        .body(Bytes::from("ok"))
        .unwrap();
    inbound.response_tx.send(response).unwrap();
}
