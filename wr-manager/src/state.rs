use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use deadpool_postgres::Pool;
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::db;

/// Ephemeral in-memory state — heartbeats and module health timestamps.
/// Persisted state (engines, routing table, schemas) lives in Postgres.
pub struct ManagerState {
    /// Last heartbeat timestamp per engine_id
    pub heartbeats: HashMap<String, Instant>,
    /// Last healthy-heartbeat timestamp per (engine_id, namespace, module_name, version)
    pub module_health: HashMap<(String, String, String, String), Instant>,
}

impl ManagerState {
    fn new() -> Self {
        Self {
            heartbeats: HashMap::new(),
            module_health: HashMap::new(),
        }
    }
}

pub type SharedState = Arc<RwLock<ManagerState>>;

pub fn new_state() -> SharedState {
    Arc::new(RwLock::new(ManagerState::new()))
}

/// Background task: checks engine and module-level health on a regular interval.
/// Reads the routing table from Postgres, computes health changes from
/// in-memory timestamps, and writes updates back to Postgres.
pub async fn monitor_heartbeats(
    state: SharedState,
    pool: Pool,
    timeout_secs: u64,
    interval: Duration,
) {
    let timeout = Duration::from_secs(timeout_secs);
    let mut tick = tokio::time::interval(interval);

    loop {
        tick.tick().await;
        let now = Instant::now();

        // Read current routing table from DB
        let table = match db::get_routing_table(&pool).await {
            Ok(t) => t,
            Err(e) => {
                warn!(error = %e, "monitor: failed to read routing table");
                continue;
            }
        };

        let state = state.read().await;

        // Log warnings for engines that have gone silent
        for (engine_id, &last_hb) in &state.heartbeats {
            let elapsed = now.duration_since(last_hb);
            if elapsed > timeout {
                warn!(
                    engine_id,
                    elapsed_secs = elapsed.as_secs(),
                    threshold_secs = timeout_secs,
                    "engine unhealthy — missed heartbeat",
                );
            }
        }

        // Compute health changes
        let mut updates: Vec<(String, bool)> = Vec::new();
        for rule in &table.rules {
            let key = (
                rule.engine_id.clone(),
                rule.destination_namespace.clone(),
                rule.destination_module.clone(),
                rule.destination_version.clone(),
            );
            let is_healthy = state
                .module_health
                .get(&key)
                .map(|&last| now.duration_since(last) <= timeout)
                .unwrap_or(true); // startup grace: never-seen = assume healthy

            if rule.healthy != is_healthy {
                updates.push((rule.rule_id.clone(), is_healthy));
            }
        }

        drop(state);

        // Write health changes to DB
        for (rule_id, is_healthy) in updates {
            match db::set_rule_health(&pool, &rule_id, is_healthy).await {
                Ok(()) => {
                    if !is_healthy {
                        warn!(rule_id, "module marked unhealthy");
                    } else {
                        info!(rule_id, "module recovered");
                    }
                }
                Err(e) => {
                    // NOWAIT lock contention — skip this tick, retry next
                    if e.code() == tonic::Code::Aborted {
                        warn!(rule_id, "monitor: lock contention, will retry");
                    } else {
                        warn!(rule_id, error = %e, "monitor: failed to update rule health");
                    }
                }
            }
        }
    }
}
