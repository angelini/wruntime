mod helpers;
use helpers::{
    manager::{
        manager_trio, register_test_module_ready, register_test_module_ready_with_peer,
        synced_routing_table,
    },
    proxy::{http_client, proxy_get, start_proxy_with_cb, EngineSpec, ModuleSpec},
    stubs::{spawn_status_stub, spawn_stub_engine, spawn_switchable_stub},
    wasm::minimal_file_descriptor_set,
};

use anyhow::Result;
use bytes::Bytes;
use http::{Request, StatusCode};
use http_body_util::Full;

use wr_proxy::config::CircuitBreakerConfig;

/// After `failure_threshold` consecutive 500s the circuit opens and subsequent
/// requests are rejected with 503 + `Retry-After` without reaching the engine.
#[tokio::test]
async fn test_circuit_breaker_opens_after_consecutive_failures() -> Result<()> {
    let (pool, mgr_addr, mut mgr) = manager_trio().await?;

    // Stub engine that always returns 500.
    let (engine_addr, engine_shutdown) =
        spawn_status_stub(StatusCode::INTERNAL_SERVER_ERROR).await?;

    register_test_module_ready(
        &pool,
        &mut mgr,
        "cb-e1",
        &engine_addr,
        "cb-ns",
        "failing-svc",
        "1.0.0",
    )
    .await?;

    let table = synced_routing_table(&mgr_addr).await?;

    // threshold=3 so we can test quickly; open_duration_secs=2 for recovery test.
    let proxy = start_proxy_with_cb(
        table,
        CircuitBreakerConfig {
            failure_threshold: 3,
            open_duration_secs: 2,
        },
    )
    .await?;

    // First 3 requests hit the engine and get 500 passed through (counted as failure).
    for _ in 0..3 {
        let (status, _) = proxy_get(proxy, "cb-ns", "failing-svc", Some("1.0.0")).await?;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    // 4th request: circuit is now OPEN — rejected without reaching engine.
    let (status, body) = proxy_get(proxy, "cb-ns", "failing-svc", Some("1.0.0")).await?;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        body.contains("circuit open"),
        "expected circuit open body, got: {body}"
    );

    let _ = engine_shutdown.send(());
    Ok(())
}

/// The circuit breaker also protects the RemoteProxy forwarding path: after
/// `failure_threshold` consecutive failures forwarding to an unreachable peer
/// proxy, the circuit opens and rejects with 503 without attempting a forward.
#[tokio::test]
async fn test_circuit_breaker_opens_for_remote_peer() -> Result<()> {
    let (pool, mgr_addr, mut mgr) = manager_trio().await?;

    // peer_address ("https://127.0.0.1:1") differs from the proxy's self peer
    // address (TEST_SELF_PEER) → make_destination yields RemoteProxy; the peer
    // is unreachable so every forward fails.
    register_test_module_ready_with_peer(
        &pool,
        &mut mgr,
        EngineSpec {
            id: "cb-remote-e1",
            addr: "http://127.0.0.1:1",
            peer_address: "https://127.0.0.1:1",
        },
        ModuleSpec {
            namespace: "cb-remote-ns",
            name: "remote-svc",
            version: "1.0.0",
            schema: minimal_file_descriptor_set(),
        },
    )
    .await?;

    let table = synced_routing_table(&mgr_addr).await?;

    let proxy = start_proxy_with_cb(
        table,
        CircuitBreakerConfig {
            failure_threshold: 3,
            open_duration_secs: 2,
        },
    )
    .await?;

    // First 3 requests attempt the remote forward and fail (counted as failures).
    for _ in 0..3 {
        let (status, _) = proxy_get(proxy, "cb-remote-ns", "remote-svc", Some("1.0.0")).await?;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    // 4th request: circuit is now OPEN — rejected as "circuit open".
    let (status, body) = proxy_get(proxy, "cb-remote-ns", "remote-svc", Some("1.0.0")).await?;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        body.contains("circuit open"),
        "expected circuit open body, got: {body}"
    );

    Ok(())
}

/// Verify the 503 response includes a `Retry-After` header matching the
/// configured `open_duration_secs`.
#[tokio::test]
async fn test_circuit_breaker_retry_after_header() -> Result<()> {
    let (pool, mgr_addr, mut mgr) = manager_trio().await?;

    let (engine_addr, engine_shutdown) =
        spawn_status_stub(StatusCode::INTERNAL_SERVER_ERROR).await?;

    register_test_module_ready(
        &pool,
        &mut mgr,
        "cb-retry-e1",
        &engine_addr,
        "cb-retry-ns",
        "retry-svc",
        "1.0.0",
    )
    .await?;

    let table = synced_routing_table(&mgr_addr).await?;

    let proxy = start_proxy_with_cb(
        table,
        CircuitBreakerConfig {
            failure_threshold: 2,
            open_duration_secs: 7,
        },
    )
    .await?;

    // Trip the circuit.
    for _ in 0..2 {
        proxy_get(proxy, "cb-retry-ns", "retry-svc", Some("1.0.0")).await?;
    }

    // Next request is rejected — check the raw response for Retry-After.
    let path = "/cb-retry-ns.retry-svc/Ping";
    let req = Request::builder()
        .uri(format!("http://{proxy}{path}"))
        .header(
            "x-wr-destination",
            format!("http://cb-retry-ns.retry-svc{path}"),
        )
        .header("x-wr-source", "test-caller")
        .body(Full::new(Bytes::new()))?;
    let resp = http_client().request(req).await?;

    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let retry_after = resp
        .headers()
        .get(http::header::RETRY_AFTER)
        .expect("Retry-After header missing");
    assert_eq!(retry_after.to_str()?, "7");

    let _ = engine_shutdown.send(());
    Ok(())
}

/// 429 Too Many Requests counts as a failure and can trip the circuit.
#[tokio::test]
async fn test_circuit_breaker_429_counts_as_failure() -> Result<()> {
    let (pool, mgr_addr, mut mgr) = manager_trio().await?;

    let (engine_addr, engine_shutdown) = spawn_status_stub(StatusCode::TOO_MANY_REQUESTS).await?;

    register_test_module_ready(
        &pool,
        &mut mgr,
        "cb-429-e1",
        &engine_addr,
        "cb-429-ns",
        "rate-svc",
        "1.0.0",
    )
    .await?;

    let table = synced_routing_table(&mgr_addr).await?;

    let proxy = start_proxy_with_cb(
        table,
        CircuitBreakerConfig {
            failure_threshold: 2,
            open_duration_secs: 2,
        },
    )
    .await?;

    // Trip the circuit with 429s.
    for _ in 0..2 {
        proxy_get(proxy, "cb-429-ns", "rate-svc", Some("1.0.0")).await?;
    }

    // Circuit should be open.
    let (status, body) = proxy_get(proxy, "cb-429-ns", "rate-svc", Some("1.0.0")).await?;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        body.contains("circuit open"),
        "expected circuit open, got: {body}"
    );

    let _ = engine_shutdown.send(());
    Ok(())
}

/// Successful responses keep the circuit closed — no spurious opens.
#[tokio::test]
async fn test_circuit_breaker_stays_closed_on_success() -> Result<()> {
    let (pool, mgr_addr, mut mgr) = manager_trio().await?;

    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;

    register_test_module_ready(
        &pool,
        &mut mgr,
        "cb-ok-e1",
        &engine_addr,
        "cb-ok-ns",
        "ok-svc",
        "1.0.0",
    )
    .await?;

    let table = synced_routing_table(&mgr_addr).await?;

    let proxy = start_proxy_with_cb(
        table,
        CircuitBreakerConfig {
            failure_threshold: 2,
            open_duration_secs: 2,
        },
    )
    .await?;

    // 10 successful requests — all should return 200.
    for _ in 0..10 {
        let (status, _) = proxy_get(proxy, "cb-ok-ns", "ok-svc", Some("1.0.0")).await?;
        assert_eq!(status, StatusCode::OK);
    }

    let _ = engine_shutdown.send(());
    Ok(())
}

/// After the open duration elapses the circuit enters half-open: a successful
/// probe closes the circuit and restores normal traffic.
#[tokio::test]
async fn test_circuit_breaker_half_open_recovery() -> Result<()> {
    let (pool, mgr_addr, mut mgr) = manager_trio().await?;

    // Start with a switchable stub returning 500.
    let (engine_addr, engine_shutdown, status_ctl) = spawn_switchable_stub(500).await?;

    register_test_module_ready(
        &pool,
        &mut mgr,
        "cb-ho-e1",
        &engine_addr,
        "cb-ho-ns",
        "recover-svc",
        "1.0.0",
    )
    .await?;

    let table = synced_routing_table(&mgr_addr).await?;

    let proxy = start_proxy_with_cb(
        table,
        CircuitBreakerConfig {
            failure_threshold: 2,
            open_duration_secs: 1,
        },
    )
    .await?;

    // Trip the circuit.
    for _ in 0..2 {
        proxy_get(proxy, "cb-ho-ns", "recover-svc", Some("1.0.0")).await?;
    }

    // Confirm it's open.
    let (status, body) = proxy_get(proxy, "cb-ho-ns", "recover-svc", Some("1.0.0")).await?;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.contains("circuit open"));

    // Switch the stub to return 200 and wait for the open duration to elapse.
    status_ctl.store(200, std::sync::atomic::Ordering::Relaxed);
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // The circuit should now be half-open and the next request should succeed,
    // transitioning back to closed.
    let (status, _) = proxy_get(proxy, "cb-ho-ns", "recover-svc", Some("1.0.0")).await?;
    assert_eq!(status, StatusCode::OK);

    // Subsequent requests should also succeed (fully closed again).
    let (status, _) = proxy_get(proxy, "cb-ho-ns", "recover-svc", Some("1.0.0")).await?;
    assert_eq!(status, StatusCode::OK);

    let _ = engine_shutdown.send(());
    Ok(())
}

/// Circuit breakers are per-engine: one failing engine doesn't affect another.
#[tokio::test]
async fn test_circuit_breaker_per_engine_isolation() -> Result<()> {
    let (pool, mgr_addr, mut mgr) = manager_trio().await?;

    // Engine A: always fails.
    let (engine_a_addr, engine_a_shutdown) =
        spawn_status_stub(StatusCode::INTERNAL_SERVER_ERROR).await?;
    // Engine B: always succeeds (different module in same namespace).
    let (engine_b_addr, engine_b_shutdown) = spawn_stub_engine().await?;

    register_test_module_ready(
        &pool,
        &mut mgr,
        "cb-iso-ea",
        &engine_a_addr,
        "cb-iso-ns",
        "bad-svc",
        "1.0.0",
    )
    .await?;
    register_test_module_ready(
        &pool,
        &mut mgr,
        "cb-iso-eb",
        &engine_b_addr,
        "cb-iso-ns",
        "good-svc",
        "1.0.0",
    )
    .await?;

    let table = synced_routing_table(&mgr_addr).await?;

    let proxy = start_proxy_with_cb(
        table,
        CircuitBreakerConfig {
            failure_threshold: 2,
            open_duration_secs: 30,
        },
    )
    .await?;

    // Trip engine A's circuit.
    for _ in 0..3 {
        proxy_get(proxy, "cb-iso-ns", "bad-svc", Some("1.0.0")).await?;
    }

    // Engine B should be unaffected.
    let (status, _) = proxy_get(proxy, "cb-iso-ns", "good-svc", Some("1.0.0")).await?;
    assert_eq!(status, StatusCode::OK);

    let _ = engine_a_shutdown.send(());
    let _ = engine_b_shutdown.send(());
    Ok(())
}
