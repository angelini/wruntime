mod helpers;

use std::time::{Duration, Instant};

use anyhow::Result;
use helpers::db::require_db_url;

fn percentile(mut samples: Vec<Duration>, percentile: usize) -> Duration {
    samples.sort_unstable();
    let index = (samples.len() - 1) * percentile / 100;
    samples[index]
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual release-mode DB simplification metrics; requires an isolated WRT_TEST_DB_URL"]
async fn report_db_simplification_metrics() -> Result<()> {
    let base_url = require_db_url();
    let pool = wr_engine::pool::build_pool(&base_url, 3)?;

    let mut held = Vec::new();
    let mut backend_pids = std::collections::BTreeSet::new();
    for _ in 0..3 {
        let client = pool.get().await?;
        backend_pids.insert(
            client
                .query_one("SELECT pg_backend_pid()", &[])
                .await?
                .get::<_, i32>(0),
        );
        held.push(client);
    }
    let physical_connections = backend_pids.len();

    let wait_started = Instant::now();
    let wait_timed_out = tokio::time::timeout(Duration::from_millis(100), pool.get())
        .await
        .is_err();
    let held_pool_wait = wait_started.elapsed();
    drop(held);

    let client = pool.get().await?;
    let mut raw_query_samples = Vec::with_capacity(100);
    for _ in 0..10 {
        client.query_one("SELECT $1::int4", &[&7_i32]).await?;
    }
    for _ in 0..100 {
        let started = Instant::now();
        client.query_one("SELECT $1::int4", &[&7_i32]).await?;
        raw_query_samples.push(started.elapsed());
    }

    println!(
        "commit={}",
        option_env!("GIT_COMMIT").unwrap_or("unavailable")
    );
    println!(
        "postgres_version={}",
        client
            .query_one("SHOW server_version", &[])
            .await?
            .get::<_, String>(0)
    );
    println!("warmups=10 samples=100");
    println!("admin_pool_max=3 physical_connections={physical_connections}");
    println!(
        "held_pool_wait_ms={} timed_out={wait_timed_out}",
        held_pool_wait.as_millis()
    );
    println!(
        "raw_query_p50_us={}",
        percentile(raw_query_samples.clone(), 50).as_micros()
    );
    println!(
        "raw_query_p95_us={}",
        percentile(raw_query_samples, 95).as_micros()
    );
    println!(
        "controlled_rtt_ms={}",
        std::env::var("WRT_METRICS_RTT_MS").unwrap_or_else(|_| "unavailable".into())
    );

    assert_eq!(physical_connections, 3);
    assert!(
        wait_timed_out,
        "held pool should demonstrate bounded waiting"
    );
    Ok(())
}
