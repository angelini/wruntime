use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use wr_common::discovery::ManagerDiscovery;
use wr_common::process_lifecycle::LifecycleSnapshotHandle;
use wr_common::task_group::{TaskCancellation, TaskExit};
use wr_common::wruntime::{
    proxy_node_control_service_server::ProxyNodeControlService, BeginEngineDrainRequest,
    BeginEngineDrainResponse, DeregisterEngineRequest, DeregisterEngineResponse,
    EngineOwnershipFence, GetProxyRoutingStatusRequest, GetProxyRoutingStatusResponse,
    HeartbeatRequest, HeartbeatResponse, ModuleDescriptor, RegisterEngineRequest,
    RegisterEngineResponse,
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
    engine_id: String,
    fence: Option<EngineOwnershipFence>,
    healthy_modules: Vec<ModuleDescriptor>,
    serialized_snapshot: Vec<u8>,
    phase: EnginePhase,
}

struct EngineSlotState {
    state: Mutex<EngineState>,
    manager_epoch: Mutex<Option<wr_common::manager_client::ManagerEpoch>>,
    forward: Mutex<()>,
}

type EngineSlot = Arc<EngineSlotState>;

/// Node-local engine lifecycle owner. The map lock is held only long enough to
/// locate a slot; each slot is the forwarding fence for one engine.
pub struct NodeAgent {
    discovery: Arc<ManagerDiscovery>,
    routing: CachedRoutingTable,
    lifecycle: LifecycleSnapshotHandle,
    engines: Mutex<HashMap<String, EngineSlot>>,
}

impl NodeAgent {
    pub fn new(
        discovery: Arc<ManagerDiscovery>,
        routing: CachedRoutingTable,
        lifecycle: LifecycleSnapshotHandle,
    ) -> Self {
        Self {
            discovery,
            routing,
            lifecycle,
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
            engines.values().map(Arc::clone).collect::<Vec<_>>()
        };
        let mut forwards = JoinSet::new();
        for state in engines {
            let agent = Arc::clone(self);
            forwards.spawn(async move {
                let engine_id = state.state.lock().await.engine_id.clone();
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
        let (fence, request) = {
            let state = slot.state.lock().await;
            if state.phase != EnginePhase::Ready {
                return;
            }
            let Some(fence) = state.fence.clone() else {
                return;
            };
            (
                fence.clone(),
                HeartbeatRequest {
                    engine_id: engine_id.clone(),
                    healthy_modules: state.healthy_modules.clone(),
                    fence: Some(fence),
                },
            )
        };

        let _forward = slot.forward.lock().await;
        let state = slot.state.lock().await;
        if state.phase != EnginePhase::Ready || state.fence.as_ref() != Some(&fence) {
            return;
        }
        let mut retained = slot.manager_epoch.lock().await;
        if retained.is_none() {
            *retained = self
                .discovery
                .pin(wr_common::manager_client::RetryClass::FreshRegistration)
                .await
                .ok();
        }
        let Some(epoch) = retained.as_mut() else {
            warn!(
                engine_id,
                generation = fence.slot_generation,
                "heartbeat forward could not pin a manager epoch"
            );
            return;
        };
        let response = match epoch.heartbeat(request.clone()).await {
            Ok(response) => Some(response.into_inner()),
            Err(error) if routing::is_transport_failure(&error) => {
                match self.discovery.repin(epoch).await {
                    Ok(replacement) => {
                        *epoch = replacement;
                        match epoch.heartbeat(request).await {
                            Ok(response) => Some(response.into_inner()),
                            Err(retry_error) => {
                                warn!(engine_id,generation=fence.slot_generation,%retry_error,"heartbeat replay failed after deterministic repin");
                                None
                            }
                        }
                    }
                    Err(repin_error) => {
                        warn!(engine_id,generation=fence.slot_generation,%repin_error,"heartbeat forward could not repin manager epoch");
                        None
                    }
                }
            }
            Err(error) => {
                warn!(engine_id,generation=fence.slot_generation,%error,"heartbeat forward failed");
                None
            }
        };
        if let Some(response) = response {
            if response.accepted_fence.as_ref() == Some(&fence) {
                drop(state);
                let mut state = slot.state.lock().await;
                if state.fence.as_ref() == Some(&fence) {
                    state.serialized_snapshot = response.serialized_snapshot;
                }
            }
        }
    }

    async fn engine_slot(&self, engine_id: &str) -> Result<EngineSlot, Status> {
        let slots = self
            .engines
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for slot in slots {
            if slot.state.lock().await.engine_id == engine_id {
                return Ok(slot);
            }
        }
        Err(Status::not_found(
            "engine is not registered with this proxy",
        ))
    }

    async fn engine_slot_or_insert(&self, slot_key: &str) -> EngineSlot {
        Arc::clone(
            self.engines
                .lock()
                .await
                .entry(slot_key.to_string())
                .or_insert_with(|| {
                    Arc::new(EngineSlotState {
                        state: Mutex::new(EngineState {
                            engine_id: String::new(),
                            fence: None,
                            healthy_modules: Vec::new(),
                            serialized_snapshot: Vec::new(),
                            phase: EnginePhase::Tombstoned,
                        }),
                        manager_epoch: Mutex::new(None),
                        forward: Mutex::new(()),
                    })
                }),
        )
    }

    async fn converge(
        &self,
        epoch: &mut wr_common::manager_client::ManagerEpoch,
        manager_version: u64,
    ) -> Result<u64, Status> {
        routing::converge_to_version(
            &self.discovery,
            epoch,
            &self.routing,
            manager_version,
            Instant::now() + CONVERGENCE_BUDGET,
        )
        .await
    }
}

#[tonic::async_trait]
impl ProxyNodeControlService for NodeAgent {
    async fn get_proxy_routing_status(
        &self,
        _request: Request<GetProxyRoutingStatusRequest>,
    ) -> Result<Response<GetProxyRoutingStatusResponse>, Status> {
        Ok(Response::new(GetProxyRoutingStatusResponse {
            process_instance_id: self.lifecycle.current().process_instance_id,
            installed_routing_table_version: self.routing.version().await,
        }))
    }

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
        let activation_id = request.activation_id.clone();
        let metadata = registration.deployment.clone().ok_or_else(|| {
            Status::failed_precondition("managed deployment metadata is required")
        })?;

        let slot_key = format!("{}/{}", metadata.node_id, metadata.engine_slot);
        let slot = self.engine_slot_or_insert(&slot_key).await;
        let _forward = slot.forward.lock().await;
        let mut state = slot.state.lock().await;
        let mut epoch = self
            .discovery
            .pin(wr_common::manager_client::RetryClass::FreshRegistration)
            .await?;
        let response = match epoch.register_engine(request.clone()).await {
            Ok(response) => response,
            Err(error) if routing::is_transport_failure(&error) => {
                epoch = self.discovery.repin(&epoch).await?;
                epoch.register_engine(request).await?
            }
            Err(error) => return Err(error),
        }
        .into_inner();
        let fence = response.fence.clone().ok_or_else(|| {
            Status::data_loss("manager registration response omitted ownership fence")
        })?;
        if fence.node_id != metadata.node_id
            || fence.slot != metadata.engine_slot
            || fence.revision_digest != metadata.revision_digest
            || fence.activation_id != activation_id
            || fence.slot_generation == 0
        {
            return Err(Status::permission_denied(
                "manager returned a mismatched ownership fence",
            ));
        }
        if let Some(current) = state.fence.as_ref() {
            if fence.slot_generation < current.slot_generation
                || (fence.slot_generation == current.slot_generation && fence != *current)
            {
                return Err(Status::permission_denied(
                    "manager returned stale ownership generation",
                ));
            }
        }
        *slot.manager_epoch.lock().await = Some(epoch);
        *state = EngineState {
            engine_id: engine_id.clone(),
            fence: Some(fence.clone()),
            healthy_modules: Vec::new(),
            serialized_snapshot: response.serialized_snapshot.clone(),
            phase: EnginePhase::Registered,
        };

        info!(
            engine_id,
            generation = fence.slot_generation,
            "engine registered via proxy"
        );
        Ok(Response::new(response))
    }

    async fn deregister_engine(
        &self,
        request: Request<DeregisterEngineRequest>,
    ) -> Result<Response<DeregisterEngineResponse>, Status> {
        let request = request.into_inner();
        let engine_id = request.engine_id.clone();
        let slot = self.engine_slot(&engine_id).await?;
        let _forward = slot.forward.lock().await;
        let mut state = slot.state.lock().await;
        let expected = state
            .fence
            .as_ref()
            .ok_or_else(|| Status::permission_denied("engine has no ownership fence"))?;
        if request.fence.as_ref() != Some(expected) {
            return Err(Status::permission_denied(
                "ownership fence does not match proxy cache",
            ));
        }
        let generation = expected.slot_generation;
        state.healthy_modules.clear();
        state.phase = EnginePhase::Tombstoned;

        let mut epoch = self
            .discovery
            .pin(wr_common::manager_client::RetryClass::NoReplayMutation)
            .await?;
        let response = epoch.deregister_engine(request).await?.into_inner();
        *slot.manager_epoch.lock().await = None;
        info!(engine_id, generation, "engine deregistered via proxy");
        Ok(Response::new(response))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let request = request.into_inner();
        let engine_id = request.engine_id.clone();
        let fence = request
            .fence
            .clone()
            .ok_or_else(|| Status::permission_denied("ownership fence is required"))?;
        let slot = match self.engine_slot(&engine_id).await {
            Ok(slot) => slot,
            Err(error) if error.code() == tonic::Code::NotFound => {
                let slot_key = format!("{}/{}", fence.node_id, fence.slot);
                self.engine_slot_or_insert(&slot_key).await
            }
            Err(error) => return Err(error),
        };
        let _forward = slot.forward.lock().await;
        let mut state = slot.state.lock().await;
        let recovering_after_proxy_restart = state.engine_id.is_empty();
        if !recovering_after_proxy_restart {
            if state.engine_id != engine_id {
                return Err(Status::permission_denied(
                    "engine identity does not match proxy cache",
                ));
            }
            if matches!(state.phase, EnginePhase::Draining | EnginePhase::Tombstoned) {
                return Err(Status::failed_precondition("engine is draining"));
            }
            if request.fence.as_ref() != state.fence.as_ref() {
                return Err(Status::permission_denied(
                    "ownership fence does not match proxy cache",
                ));
            }
            state.healthy_modules = request.healthy_modules.clone();

            if state.phase == EnginePhase::Ready {
                return Ok(Response::new(HeartbeatResponse {
                    manager_routing_table_version: 0,
                    proxy_routing_table_version: self.routing.version().await,
                    accepted_fence: state.fence.clone(),
                    serialized_snapshot: state.serialized_snapshot.clone(),
                }));
            }
        }

        let mut retained = slot.manager_epoch.lock().await;
        if retained.is_none() {
            *retained = Some(
                self.discovery
                    .pin(wr_common::manager_client::RetryClass::FreshRegistration)
                    .await?,
            );
        }
        let epoch = retained.as_mut().expect("manager epoch was installed");
        let response = match epoch.heartbeat(request.clone()).await {
            Ok(response) => response,
            Err(error) if routing::is_transport_failure(&error) => {
                *epoch = self.discovery.repin(epoch).await?;
                epoch.heartbeat(request.clone()).await?
            }
            Err(error) => return Err(error),
        };
        let manager_response = response.into_inner();
        if manager_response.accepted_fence.as_ref() != Some(&fence) {
            return Err(Status::permission_denied(
                "manager heartbeat fence does not match proxy cache",
            ));
        }
        let manager_version = manager_response.manager_routing_table_version;
        let proxy_version = self.converge(epoch, manager_version).await?;
        state.engine_id = engine_id.clone();
        state.fence = Some(fence);
        state.healthy_modules = request.healthy_modules;
        state.serialized_snapshot = manager_response.serialized_snapshot.clone();
        state.phase = EnginePhase::Ready;
        info!(
            engine_id,
            manager_version,
            proxy_version,
            recovered = recovering_after_proxy_restart,
            "engine readiness converged"
        );
        Ok(Response::new(HeartbeatResponse {
            manager_routing_table_version: manager_version,
            proxy_routing_table_version: proxy_version,
            accepted_fence: state.fence.clone(),
            serialized_snapshot: manager_response.serialized_snapshot,
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
        if request.fence.as_ref() != state.fence.as_ref() {
            return Err(Status::permission_denied(
                "ownership fence does not match proxy cache",
            ));
        }
        state.healthy_modules.clear();
        state.phase = EnginePhase::Draining;
        let generation = state
            .fence
            .as_ref()
            .expect("registered fence")
            .slot_generation;

        let mut epoch = self
            .discovery
            .pin(wr_common::manager_client::RetryClass::NoReplayMutation)
            .await?;
        let manager_version = epoch
            .begin_engine_drain(request)
            .await?
            .into_inner()
            .manager_routing_table_version;
        let mut convergence_epoch = self
            .discovery
            .pin(wr_common::manager_client::RetryClass::ReadOnly)
            .await?;
        let proxy_version = self
            .converge(&mut convergence_epoch, manager_version)
            .await?;
        info!(
            engine_id,
            generation, manager_version, proxy_version, "engine drain converged"
        );
        Ok(Response::new(BeginEngineDrainResponse {
            manager_routing_table_version: manager_version,
            proxy_routing_table_version: proxy_version,
            accepted_fence: state.fence.clone(),
        }))
    }
}

#[cfg(test)]
mod workflow_class_tests {
    #[test]
    fn proxy_manager_workflows_declare_safe_retry_classes() {
        let source = include_str!("node_service.rs");
        assert!(source.contains("RetryClass::FreshRegistration"));
        assert!(source.contains("RetryClass::NoReplayMutation"));
        assert!(!source.contains(concat!("get_", "client()")));
    }
}
