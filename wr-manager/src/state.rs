use std::time::Duration;

use deadpool_postgres::Pool;
use tracing::{info, warn};

use crate::db;

/// Background task: checks engine health by querying Postgres for stale heartbeats.
/// Marks routing rules unhealthy when their engine's heartbeat exceeds the timeout,
/// and recovers rules when the engine starts heartbeating again.
pub async fn monitor_heartbeats(pool: Pool, timeout_secs: u64, interval: Duration) {
    let timeout = timeout_secs as f64;
    let mut tick = tokio::time::interval(interval);

    loop {
        tick.tick().await;

        match db::update_rule_health_from_heartbeats(&pool, timeout).await {
            Ok((stale, recovered)) => {
                for rule_id in &stale {
                    warn!(rule_id, "module marked unhealthy");
                }
                for rule_id in &recovered {
                    info!(rule_id, "module recovered");
                }
            }
            Err(e) => {
                warn!(error = %e, "monitor: failed to update rule health");
            }
        }
    }
}
