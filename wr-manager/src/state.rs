use std::time::Duration;

use deadpool_postgres::Pool;
use tracing::{info, warn};

use crate::db;
use wr_common::task_group::{TaskCancellation, TaskExit};

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
        update_health(&pool, engine_timeout, module_timeout).await;
    }
}

pub async fn monitor_heartbeats_owned(
    pool: Pool,
    engine_timeout_secs: u64,
    module_timeout_secs: u64,
    interval: Duration,
    mut cancellation: TaskCancellation,
) -> anyhow::Result<TaskExit> {
    let engine_timeout = engine_timeout_secs as f64;
    let module_timeout = module_timeout_secs as f64;
    let mut tick = tokio::time::interval(interval);

    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return Ok(TaskExit::Cancelled),
            _ = tick.tick() => {}
        }

        update_health(&pool, engine_timeout, module_timeout).await;
    }
}

async fn update_health(pool: &Pool, engine_timeout: f64, module_timeout: f64) {
    match db::update_route_health(pool, engine_timeout, module_timeout).await {
        Ok((stale, recovered)) => {
            for rule_id in &stale {
                warn!(rule_id, "module marked unhealthy");
            }
            for rule_id in &recovered {
                info!(rule_id, "module recovered");
            }
        }
        Err(error) => {
            warn!(%error, "monitor: failed to update rule health");
        }
    }
}
