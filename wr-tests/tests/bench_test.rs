/// Benchmark test — measures the hot request path:
///   External request → WASM guest A (Egress) → proxy → WASM guest B (Echo) → response
///
/// Exercises the full inter-module call path including wasmtime instantiation,
/// WASI HTTP interception, proxy header-based routing, and protobuf ser/deser.
///
/// Run:  just test-one bench_hot_path
/// Or:   cargo test -p wr-tests --test bench_test --release -- --nocapture
mod helpers;
use helpers::{
    manager::{manager_trio, register_test_module_ready, sync_table, synced_routing_table},
    proxy::{proxy_get, start_proxy},
    stubs::spawn_stub_engine,
    wasm::{spawn_wasm_stub_engine, wasm_module_pre},
};

use std::time::{Duration, Instant};

use anyhow::Result;
use bytes::Bytes;
use http_body_util::Full;
use prost::Message;

#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/test.rs"));
}

const HTTP_GUEST_WASM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/guests/http-guest/target/wasm32-wasip2/debug/http_guest.wasm"
);

fn skip_if_no_wasm() -> bool {
    if !std::path::Path::new(HTTP_GUEST_WASM).exists() {
        eprintln!("SKIP: http-guest WASM not built — run `just build-test-guests`");
        return true;
    }
    false
}

/// Percentile from a sorted slice.
fn percentile(sorted: &[Duration], p: f64) -> Duration {
    let idx = ((sorted.len() as f64) * p / 100.0).ceil() as usize;
    sorted[idx.min(sorted.len()) - 1]
}

fn print_latency_stats(label: &str, latencies: &mut [Duration]) {
    latencies.sort();
    let n = latencies.len() as f64;
    let total: Duration = latencies.iter().sum();
    let mean = total.as_secs_f64() / n;
    eprintln!("\n=== {label} ({} iterations) ===", latencies.len());
    eprintln!("  throughput: {:>10.1} req/s", n / total.as_secs_f64());
    eprintln!("  mean:       {:>10.3} ms", mean * 1000.0);
    eprintln!(
        "  p50:        {:>10.3} ms",
        percentile(latencies, 50.0).as_secs_f64() * 1000.0
    );
    eprintln!(
        "  p90:        {:>10.3} ms",
        percentile(latencies, 90.0).as_secs_f64() * 1000.0
    );
    eprintln!(
        "  p99:        {:>10.3} ms",
        percentile(latencies, 99.0).as_secs_f64() * 1000.0
    );
    eprintln!(
        "  min:        {:>10.3} ms",
        latencies.first().unwrap().as_secs_f64() * 1000.0
    );
    eprintln!(
        "  max:        {:>10.3} ms",
        latencies.last().unwrap().as_secs_f64() * 1000.0
    );
}

fn print_concurrent_stats(
    label: &str,
    latencies: &mut [Duration],
    wall_time: Duration,
    sequential_rps: f64,
) {
    latencies.sort();
    let n = latencies.len() as f64;
    let total: Duration = latencies.iter().sum();
    let mean = total.as_secs_f64() / n;
    let wall_rps = n / wall_time.as_secs_f64();
    let speedup = wall_rps / sequential_rps;
    eprintln!("\n=== {label} ({} iterations) ===", latencies.len());
    eprintln!("  throughput: {wall_rps:>10.1} req/s  ({speedup:.1}x vs sequential)");
    eprintln!(
        "  wall time:  {:>10.3} ms",
        wall_time.as_secs_f64() * 1000.0
    );
    eprintln!("  mean:       {:>10.3} ms", mean * 1000.0);
    eprintln!(
        "  p50:        {:>10.3} ms",
        percentile(latencies, 50.0).as_secs_f64() * 1000.0
    );
    eprintln!(
        "  p90:        {:>10.3} ms",
        percentile(latencies, 90.0).as_secs_f64() * 1000.0
    );
    eprintln!(
        "  p99:        {:>10.3} ms",
        percentile(latencies, 99.0).as_secs_f64() * 1000.0
    );
    eprintln!(
        "  min:        {:>10.3} ms",
        latencies.first().unwrap().as_secs_f64() * 1000.0
    );
    eprintln!(
        "  max:        {:>10.3} ms",
        latencies.last().unwrap().as_secs_f64() * 1000.0
    );
}

/// Full WASM-to-WASM hot path benchmark:
///   client HTTP request → WASM engine A (Egress handler)
///     → proxy (header-based route)
///       → WASM engine B (Echo handler) → response
///
/// This exercises the complete inter-module call path including:
/// - wasmtime component instantiation per request
/// - WASI HTTP outgoing-handler interception + header injection
/// - proxy routing table lookup + request forwarding
/// - protobuf encode/decode in both guest modules
#[tokio::test(flavor = "multi_thread")]
async fn bench_hot_path() -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    if skip_if_no_wasm() {
        return Ok(());
    }

    let iterations: usize = std::env::var("BENCH_ITERATIONS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200);
    let warmup: usize = std::env::var("BENCH_WARMUP")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);

    // ── Infrastructure ───────────────────────────────────────────────────────
    let (pool, mgr_addr, mut mgr) = manager_trio().await?;

    let table = wr_proxy::routing::new_routing_table();
    let proxy_addr = start_proxy(table.clone()).await?;
    let proxy_uri = format!("http://{proxy_addr}");

    // ── Echo engine (destination — handles the Echo RPC) ─────────────────────
    let (echo_engine, echo_pre) = wasm_module_pre(HTTP_GUEST_WASM)?;
    let (echo_addr, _echo_shutdown) =
        spawn_wasm_stub_engine(echo_engine, echo_pre, &proxy_uri, "echo-svc", "bench-ns").await?;

    register_test_module_ready(
        &pool,
        &mut mgr,
        "echo-engine",
        &echo_addr,
        "bench-ns",
        "echo-svc",
        "1.0.0",
    )
    .await?;

    // ── Caller engine (source — Egress handler calls echo-svc via proxy) ─────
    //
    // The caller engine is NOT registered with the proxy — we send requests
    // directly to it.  Its Egress handler makes an outbound HTTP call to
    // bench-ns.echo-svc, which WasiHttpView intercepts and routes through
    // the proxy to the echo engine.  That second hop is the hot path.
    let (caller_engine, caller_pre) = wasm_module_pre(HTTP_GUEST_WASM)?;
    let (caller_addr, _caller_shutdown) = spawn_wasm_stub_engine(
        caller_engine,
        caller_pre,
        &proxy_uri,
        "caller-svc",
        "bench-ns",
    )
    .await?;

    sync_table(&mgr_addr, &table).await?;

    // ── Build the request payload ────────────────────────────────────────────
    let egress_req = proto::EgressRequest {
        authority: "bench-ns.echo-svc".into(),
        path: "/test.HttpTestService/Echo".into(),
        body: Vec::new(),
    };
    let egress_body = egress_req.encode_to_vec();

    let pool = wr_common::http_pool::HttpClientPool::<Full<Bytes>>::new(
        wr_common::http_pool::DEFAULT_POOL_SIZE,
    );

    // ── Helper to send one request directly to the caller engine ─────────────
    // The caller's Egress handler makes the inter-module call through the
    // proxy, so we measure the full WASM→proxy→WASM round-trip.
    let send = |pool: wr_common::http_pool::HttpClientPool<Full<Bytes>>,
                caller_addr: String,
                body: Vec<u8>| async move {
        let req = http::Request::builder()
            .method("POST")
            .uri(format!("{caller_addr}/test.HttpTestService/Egress"))
            .body(Full::new(Bytes::from(body)))
            .unwrap();
        pool.get().request(req).await
    };

    // ── Warmup ───────────────────────────────────────────────────────────────
    for _ in 0..warmup {
        let resp = send(pool.clone(), caller_addr.clone(), egress_body.clone()).await?;
        assert_eq!(resp.status(), 200, "warmup request failed");
    }

    // ── Measure sequential latencies ─────────────────────────────────────────
    let mut latencies = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let start = Instant::now();
        let resp = send(pool.clone(), caller_addr.clone(), egress_body.clone()).await?;
        let elapsed = start.elapsed();
        assert_eq!(resp.status(), 200);
        latencies.push(elapsed);
    }

    print_latency_stats("WASM→proxy→WASM (sequential)", &mut latencies);
    let sequential_rps = latencies.len() as f64 / latencies.iter().sum::<Duration>().as_secs_f64();

    // ── Measure concurrent throughput ────────────────────────────────────────
    let concurrency: usize = std::env::var("BENCH_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20);

    let start = Instant::now();
    let mut handles = Vec::with_capacity(iterations);
    let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(concurrency));

    for _ in 0..iterations {
        let permit = sem.clone().acquire_owned().await?;
        let pool = pool.clone();
        let body = egress_body.clone();
        let addr = caller_addr.clone();
        handles.push(tokio::spawn(async move {
            let t0 = Instant::now();
            let resp = send(pool, addr, body).await;
            let lat = t0.elapsed();
            drop(permit);
            (resp, lat)
        }));
    }

    let mut concurrent_latencies = Vec::with_capacity(iterations);
    for h in handles {
        let (resp, lat) = h.await?;
        assert_eq!(resp?.status(), 200);
        concurrent_latencies.push(lat);
    }
    let wall_time = start.elapsed();
    print_concurrent_stats(
        &format!("WASM→proxy→WASM (concurrent, {concurrency} in-flight)"),
        &mut concurrent_latencies,
        wall_time,
        sequential_rps,
    );

    Ok(())
}

/// Proxy-only benchmark (stub engines, no WASM overhead).
/// Isolates the proxy routing + forwarding cost from wasmtime instantiation.
#[tokio::test(flavor = "multi_thread")]
async fn bench_proxy_only() -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let iterations: usize = std::env::var("BENCH_ITERATIONS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(500);

    let (pool, mgr_addr, mut mgr) = manager_trio().await?;

    let (engine_addr, _shutdown) = spawn_stub_engine().await?;

    register_test_module_ready(
        &pool,
        &mut mgr,
        "stub-engine",
        &engine_addr,
        "bench-ns",
        "target-svc",
        "1.0.0",
    )
    .await?;

    let table = synced_routing_table(&mgr_addr).await?;
    let proxy_addr = start_proxy(table).await?;

    // Warmup
    for _ in 0..10 {
        let (status, _) = proxy_get(proxy_addr, "bench-ns", "target-svc", Some("1.0.0")).await?;
        assert_eq!(status, 200);
    }

    // Sequential
    let mut latencies = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let start = Instant::now();
        let (status, _) = proxy_get(proxy_addr, "bench-ns", "target-svc", Some("1.0.0")).await?;
        let elapsed = start.elapsed();
        assert_eq!(status, 200);
        latencies.push(elapsed);
    }

    print_latency_stats("proxy→stub (sequential)", &mut latencies);

    Ok(())
}
