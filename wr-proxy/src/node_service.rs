use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_retry::strategy::FixedInterval;
use tokio_retry::Retry;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use wr_common::discovery::ManagerDiscovery;
use wr_common::task_group::{TaskCancellation, TaskExit};
use wr_common::wruntime::{
    node_service_server::NodeService, BeginEngineDrainRequest, BeginEngineDrainResponse,
    DeregisterEngineRequest, DeregisterEngineResponse, HeartbeatRequest, HeartbeatResponse,
    ModuleDescriptor, RegisterEngineRequest, RegisterEngineResponse,
};

use crate::routing::{self, CachedRoutingTable};

const CONVERGENCE_BUDGET: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EnginePhase {
    Registered,
    Ready,
    Draining,
    Tombstoned,
}

struct EngineState {
    generation: u64,
    healthy_modules: Vec<ModuleDescriptor>,
    phase: EnginePhase,
}

struct EngineSlotState {
    state: Mutex<EngineState>,
    forward: Mutex<()>,
}

type EngineSlot = Arc<EngineSlotState>;

/// Node-local engine lifecycle owner. The map lock is held only long enough to
/// locate a slot; each slot is the forwarding fence for one engine.
pub struct NodeAgent {
    discovery: Arc<ManagerDiscovery>,
    routing: CachedRoutingTable,
    engines: Mutex<HashMap<String, EngineSlot>>,
}

impl NodeAgent {
    pub fn new(discovery: Arc<ManagerDiscovery>, routing: CachedRoutingTable) -> Self {
        Self {
            discovery,
            routing,
            engines: Mutex::new(HashMap::new()),
        }
    }

    pub async fn run_heartbeat_loop(
        self: Arc<Self>,
        interval: Duration,
        mut cancellation: TaskCancellation,
    ) -> anyhow::Result<TaskExit> {
        let mut tick = tokio::time::interval(interval);
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => return Ok(TaskExit::Cancelled),
                _ = tick.tick() => self.flush_heartbeats().await,
            }
        }
    }

    async fn flush_heartbeats(self: &Arc<Self>) {
        let engines = {
            let engines = self.engines.lock().await;
            engines
                .iter()
                .map(|(engine_id, state)| (engine_id.clone(), Arc::clone(state)))
                .collect::<Vec<_>>()
        };
        let mut forwards = JoinSet::new();
        for (engine_id, state) in engines {
            let agent = Arc::clone(self);
            forwards.spawn(async move {
                agent.flush_engine_heartbeat(engine_id, state).await;
            });
        }
        while let Some(result) = forwards.join_next().await {
            if let Err(error) = result {
                warn!(%error, "engine heartbeat forwarding task panicked");
            }
        }
    }

    async fn flush_engine_heartbeat(&self, engine_id: String, slot: EngineSlot) {
        let (generation, request) = {
            let state = slot.state.lock().await;
            if state.phase != EnginePhase::Ready {
                return;
            }
            (
                state.generation,
                HeartbeatRequest {
                    engine_id: engine_id.clone(),
                    healthy_modules: state.healthy_modules.clone(),
                },
            )
        };

        let _forward = slot.forward.lock().await;
        let state = slot.state.lock().await;
        if state.phase != EnginePhase::Ready || state.generation != generation {
            return;
        }
        let strategy = FixedInterval::from_millis(50).take(2);
        let result = Retry::start(strategy, || {
            let discovery = Arc::clone(&self.discovery);
            let request = request.clone();
            async move {
                let mut client = discovery.get_client().await?;
                client.heartbeat(request).await
            }
        })
        .await;
        if let Err(error) = result {
            warn!(engine_id, generation, %error, "heartbeat forward failed after retries");
            self.discovery.clear_affinity().await;
        }
    }

    async fn engine_slot(&self, engine_id: &str) -> Result<EngineSlot, Status> {
        self.engines
            .lock()
            .await
            .get(engine_id)
            .cloned()
            .ok_or_else(|| Status::not_found("engine is not registered with this proxy"))
    }

    async fn engine_slot_or_insert(&self, engine_id: &str) -> EngineSlot {
        Arc::clone(
            self.engines
                .lock()
                .await
                .entry(engine_id.to_string())
                .or_insert_with(|| {
                    Arc::new(EngineSlotState {
                        state: Mutex::new(EngineState {
                            generation: 0,
                            healthy_modules: Vec::new(),
                            phase: EnginePhase::Tombstoned,
                        }),
                        forward: Mutex::new(()),
                    })
                }),
        )
    }

    async fn converge(&self, manager_version: u64) -> Result<u64, Status> {
        routing::converge_to_version(
            &self.discovery,
            &self.routing,
            manager_version,
            Instant::now() + CONVERGENCE_BUDGET,
        )
        .await
    }
}

#[tonic::async_trait]
impl NodeService for NodeAgent {
    async fn register_engine(
        &self,
        request: Request<RegisterEngineRequest>,
    ) -> Result<Response<RegisterEngineResponse>, Status> {
        let request = request.into_inner();
        let registration = request
            .registration
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("registration is required"))?;
        let engine_id = registration.engine_id.clone();

        let slot = self.engine_slot_or_insert(&engine_id).await;
        let _forward = slot.forward.lock().await;
        let mut state = slot.state.lock().await;
        let generation = state.generation.saturating_add(1);
        let mut client = self.discovery.get_client().await?;
        let response = client.register_engine(request).await?.into_inner();
        *state = EngineState {
            generation,
            healthy_modules: Vec::new(),
            phase: EnginePhase::Registered,
        };

        info!(engine_id, generation, "engine registered via proxy");
        Ok(Response::new(response))
    }

    async fn deregister_engine(
        &self,
        request: Request<DeregisterEngineRequest>,
    ) -> Result<Response<DeregisterEngineResponse>, Status> {
        let request = request.into_inner();
        let engine_id = request.engine_id.clone();
        let slot = self.engine_slot_or_insert(&engine_id).await;
        let _forward = slot.forward.lock().await;
        let mut state = slot.state.lock().await;
        state.generation = state.generation.saturating_add(1);
        state.healthy_modules.clear();
        state.phase = EnginePhase::Tombstoned;
        let generation = state.generation;

        let mut client = self.discovery.get_client().await?;
        let response = client.deregister_engine(request).await?.into_inner();
        info!(engine_id, generation, "engine deregistered via proxy");
        Ok(Response::new(response))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let request = request.into_inner();
        let engine_id = request.engine_id.clone();
        let slot = self.engine_slot(&engine_id).await?;
        let _forward = slot.forward.lock().await;
        let mut state = slot.state.lock().await;
        if matches!(state.phase, EnginePhase::Draining | EnginePhase::Tombstoned) {
            return Err(Status::failed_precondition("engine is draining"));
        }
        state.generation = state.generation.saturating_add(1);
        state.healthy_modules = request.healthy_modules.clone();

        if state.phase == EnginePhase::Ready {
            return Ok(Response::new(HeartbeatResponse {
                manager_routing_table_version: 0,
                proxy_routing_table_version: self.routing.version().await,
            }));
        }

        let mut client = self.discovery.get_client().await?;
        let manager_version = client
            .heartbeat(request)
            .await?
            .into_inner()
            .manager_routing_table_version;
        let proxy_version = self.converge(manager_version).await?;
        state.phase = EnginePhase::Ready;
        info!(
            engine_id,
            manager_version, proxy_version, "engine readiness converged"
        );
        Ok(Response::new(HeartbeatResponse {
            manager_routing_table_version: manager_version,
            proxy_routing_table_version: proxy_version,
        }))
    }

    async fn begin_engine_drain(
        &self,
        request: Request<BeginEngineDrainRequest>,
    ) -> Result<Response<BeginEngineDrainResponse>, Status> {
        let request = request.into_inner();
        let engine_id = request.engine_id.clone();
        let slot = self.engine_slot(&engine_id).await?;
        let _forward = slot.forward.lock().await;
        let mut state = slot.state.lock().await;
        if state.phase == EnginePhase::Tombstoned {
            return Err(Status::failed_precondition("engine is deregistered"));
        }
        state.generation = state.generation.saturating_add(1);
        state.healthy_modules.clear();
        state.phase = EnginePhase::Draining;
        let generation = state.generation;

        let mut client = self.discovery.get_client().await?;
        let manager_version = client
            .begin_engine_drain(request)
            .await?
            .into_inner()
            .manager_routing_table_version;
        let proxy_version = self.converge(manager_version).await?;
        info!(
            engine_id,
            generation, manager_version, proxy_version, "engine drain converged"
        );
        Ok(Response::new(BeginEngineDrainResponse {
            manager_routing_table_version: manager_version,
            proxy_routing_table_version: proxy_version,
        }))
    }
}
