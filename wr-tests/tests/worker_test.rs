#[allow(dead_code, unused_imports)]
mod helpers;
use helpers::*;

use bytes::Bytes;
use http::{Request, StatusCode};
use http_body_util::{BodyExt, Full};
use std::sync::Arc;

#[tokio::test]
async fn test_worker_config_parsing() {
    let toml_str = r#"
        listen_address  = "127.0.0.1:9100"
        [node]
        proxy_address   = "http://127.0.0.1:9001"
        control_address = "http://127.0.0.1:9002"

        [database]
        url             = "postgres://localhost/test"
        max_connections = 5

        [[module]]
        name      = "my-worker"
        namespace = "test"
        version   = "1.0.0"
        wasm_path = "wr-tests/guests/tracing-guest/target/wasm32-wasip2/release/tracing_guest.wasm"
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

        [database]
        url = "postgres://localhost/test"

        [[module]]
        name      = "my-worker"
        namespace = "test"
        version   = "1.0.0"
        wasm_path = "wr-tests/guests/tracing-guest/target/wasm32-wasip2/release/tracing_guest.wasm"
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

        [[module]]
        name      = "svc"
        namespace = "test"
        version   = "1.0.0"
        wasm_path = "wr-tests/guests/tracing-guest/target/wasm32-wasip2/release/tracing_guest.wasm"
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
    let (tx, mut rx) = tokio::sync::mpsc::channel::<wr_engine::worker::InboundRequest>(16);

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

    let (tx, mut rx) = tokio::sync::mpsc::channel::<wr_engine::worker::InboundRequest>(16);

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

    let (tx, mut rx) = tokio::sync::mpsc::channel::<wr_engine::worker::InboundRequest>(16);

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

    let (tx, mut rx) = tokio::sync::mpsc::channel::<wr_engine::worker::InboundRequest>(16);

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
