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

/// Background task: logs a warning when an engine has not sent a heartbeat
/// within `timeout_secs`.
pub async fn monitor_heartbeats(state: SharedState, timeout_secs: u64) {
    let timeout  = Duration::from_secs(timeout_secs);
    let mut tick = tokio::time::interval(Duration::from_secs(10));

    loop {
        tick.tick().await;
        let now   = Instant::now();
        let state = state.read().await;

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
    }
}
