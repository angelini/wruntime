use std::time::Duration;

use deadpool_postgres::Pool;
use tracing::{info, warn};

use crate::db;

/// Background task: recomputes routing-rule health from engine AND per-module
/// heartbeats stored in Postgres. Marks a rule unhealthy when either its engine
/// heartbeat exceeds `engine_timeout_secs` or its specific module's heartbeat
/// exceeds `module_timeout_secs`, and recovers a rule once both signals are
/// fresh again.
pub async fn monitor_heartbeats(
    pool: Pool,
    engine_timeout_secs: u64,
    module_timeout_secs: u64,
    interval: Duration,
) {
    let engine_timeout = engine_timeout_secs as f64;
    let module_timeout = module_timeout_secs as f64;
    let mut tick = tokio::time::interval(interval);

    loop {
        tick.tick().await;

        match db::update_route_health(&pool, engine_timeout, module_timeout).await {
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
