use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use tokio_retry::strategy::FixedInterval;
use tokio_retry::Retry;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use wr_common::discovery::ManagerDiscovery;
use wr_common::wruntime::{
    node_service_server::NodeService, DeregisterEngineRequest, DeregisterEngineResponse,
    HeartbeatRequest, HeartbeatResponse, ModuleDescriptor, RegisterEngineRequest,
    RegisterEngineResponse, RoutingRule,
};

/// Cached heartbeat state for a single engine.
struct EngineState {
    engine_id: String,
    healthy_modules: Vec<ModuleDescriptor>,
}

/// Node-local gRPC service that engines use instead of connecting to managers.
/// Forwards registration/deregistration to the manager and aggregates heartbeats.
pub struct NodeAgent {
    discovery: Arc<ManagerDiscovery>,
    engines: RwLock<HashMap<String, EngineState>>,
}

impl NodeAgent {
    pub fn new(discovery: Arc<ManagerDiscovery>) -> Self {
        Self {
            discovery,
            engines: RwLock::new(HashMap::new()),
        }
    }

    /// Spawn the background heartbeat aggregation loop.
    /// Sends cached heartbeats to a random manager every `interval`.
    pub fn spawn_heartbeat_loop(self: &Arc<Self>, interval: Duration) {
        let agent = Arc::clone(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            loop {
                tick.tick().await;
                agent.flush_heartbeats().await;
            }
        });
    }

    async fn flush_heartbeats(&self) {
        let engines = self.engines.read().await;
        if engines.is_empty() {
            return;
        }

        let client = match self.discovery.get_client().await {
            Ok(c) => c,
            Err(e) => {
                warn!(error = %e, "heartbeat flush: all managers unreachable");
                return;
            }
        };

        let mut any_failed = false;
        for state in engines.values() {
            let req = wr_common::wruntime::HeartbeatRequest {
                engine_id: state.engine_id.clone(),
                healthy_modules: state.healthy_modules.clone(),
            };
            let strategy = FixedInterval::from_millis(50).take(2);
            let result = Retry::spawn(strategy, || {
                let mut c = client.clone();
                let r = req.clone();
                async move { c.heartbeat(r).await }
            })
            .await;
            if let Err(e) = result {
                warn!(engine_id = %state.engine_id, error = %e, "heartbeat forward failed after retries");
                any_failed = true;
            }
        }

        if any_failed {
            self.discovery.clear_affinity().await;
        }
    }
}

#[tonic::async_trait]
impl NodeService for NodeAgent {
    async fn register_engine(
        &self,
        request: Request<RegisterEngineRequest>,
    ) -> Result<Response<RegisterEngineResponse>, Status> {
        let req = request.into_inner();
        let reg = req
            .registration
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("registration is required"))?;
        let engine_id = reg.engine_id.clone();

        // Forward registration to manager
        let mut client = self.discovery.get_client().await?;
        let response = client.register_engine(req.clone()).await?.into_inner();

        // Forward routing rules for each module with a schema
        for module in &reg.modules {
            if module.proto_schema.is_empty() {
                continue;
            }
            client
                .upsert_routing_rule(RoutingRule {
                    rule_id: format!(
                        "{}/{}/{}/{}",
                        engine_id, module.namespace, module.name, module.version,
                    ),
                    source_module: String::new(),
                    source_namespace: String::new(),
                    destination_module: module.name.clone(),
                    destination_namespace: module.namespace.clone(),
                    destination_version: module.version.clone(),
                    engine_id: engine_id.clone(),
                    engine_address: reg.address.clone(),
                    proxy_address: reg.peer_address.clone(),
                    peer_address: reg.peer_address.clone(),
                    healthy: true,
                })
                .await?;
        }

        // Cache engine for heartbeat aggregation
        {
            let mut engines = self.engines.write().await;
            engines.insert(
                engine_id.clone(),
                EngineState {
                    engine_id: engine_id.clone(),
                    healthy_modules: reg.modules.clone(),
                },
            );
        }

        info!(engine_id, "engine registered via proxy");
        Ok(Response::new(response))
    }

    async fn deregister_engine(
        &self,
        request: Request<DeregisterEngineRequest>,
    ) -> Result<Response<DeregisterEngineResponse>, Status> {
        let req = request.into_inner();
        let engine_id = req.engine_id.clone();

        // Forward to manager
        let mut client = self.discovery.get_client().await?;
        let response = client.deregister_engine(req).await?.into_inner();

        // Remove from local cache
        self.engines.write().await.remove(&engine_id);

        info!(engine_id, "engine deregistered via proxy");
        Ok(Response::new(response))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        let engine_id = req.engine_id.clone();

        // Update local cache — don't forward to manager (aggregation loop handles that)
        let mut engines = self.engines.write().await;
        if let Some(state) = engines.get_mut(&engine_id) {
            state.healthy_modules = req.healthy_modules;
        }

        Ok(Response::new(HeartbeatResponse {}))
    }
}
