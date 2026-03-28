use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;

use wr_common::wruntime::{EngineRegistration, RequestMetrics, RoutingTable};

pub struct ManagerState {
    /// Registered engines, keyed by engine_id
    pub engines:       HashMap<String, EngineRegistration>,
    /// Last heartbeat timestamp per engine_id
    pub heartbeats:    HashMap<String, Instant>,
    /// Last healthy-heartbeat timestamp per (engine_id, module_name, version)
    pub module_health: HashMap<(String, String, String), Instant>,
    /// Versioned routing table; version is incremented on every write
    pub routing_table: RoutingTable,
    /// Module schemas: (module_name, version) -> FileDescriptorSet bytes
    pub schemas:       HashMap<(String, String), Vec<u8>>,
    /// Rolling buffer of request metrics reported by proxies
    pub metrics:       VecDeque<RequestMetrics>,
}

impl ManagerState {
    fn new() -> Self {
        Self {
            engines:       HashMap::new(),
            heartbeats:    HashMap::new(),
            module_health: HashMap::new(),
            routing_table: RoutingTable { rules: vec![], version: 0 },
            schemas:       HashMap::new(),
            metrics:       VecDeque::new(),
        }
    }
}

pub type SharedState = Arc<RwLock<ManagerState>>;

pub fn new_state() -> SharedState {
    Arc::new(RwLock::new(ManagerState::new()))
}

/// Background task: checks engine and module-level health every 10 seconds.
/// Marks routing rules unhealthy when their module exceeds the timeout and
/// increments the routing table version so proxies pick up the change.
pub async fn monitor_heartbeats(state: SharedState, timeout_secs: u64) {
    let timeout  = Duration::from_secs(timeout_secs);
    let mut tick = tokio::time::interval(Duration::from_secs(10));

    loop {
        tick.tick().await;
        let now        = Instant::now();
        let mut state  = state.write().await;
        let mut version_bumped = false;

        // Log warnings for engines that have gone silent
        for (engine_id, &last_hb) in &state.heartbeats {
            let elapsed = now.duration_since(last_hb);
            if elapsed > timeout {
                eprintln!(
                    "[manager] engine '{engine_id}' is unhealthy — \
                     no heartbeat for {}s (threshold {}s)",
                    elapsed.as_secs(),
                    timeout_secs,
                );
            }
        }

        // Compute new health status for each routing rule
        let updates: Vec<(usize, bool)> = state.routing_table.rules
            .iter()
            .enumerate()
            .map(|(i, rule)| {
                let key = (
                    rule.engine_id.clone(),
                    rule.destination_module.clone(),
                    rule.destination_version.clone(),
                );
                let is_healthy = state.module_health
                    .get(&key)
                    .map(|&last| now.duration_since(last) <= timeout)
                    .unwrap_or(true); // startup grace: never-seen = assume healthy
                (i, is_healthy)
            })
            .collect();

        // Apply updates and bump version if anything changed
        for (i, is_healthy) in updates {
            let rule = &mut state.routing_table.rules[i];
            if rule.healthy != is_healthy {
                rule.healthy = is_healthy;
                version_bumped = true;
                if !is_healthy {
                    eprintln!(
                        "[manager] module '{}@{}' on engine '{}' marked unhealthy",
                        rule.destination_module, rule.destination_version, rule.engine_id,
                    );
                } else {
                    println!(
                        "[manager] module '{}@{}' on engine '{}' recovered",
                        rule.destination_module, rule.destination_version, rule.engine_id,
                    );
                }
            }
        }

        if version_bumped {
            state.routing_table.version += 1;
        }
    }
}
