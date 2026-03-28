use std::time::Instant;

use tonic::{Request, Response, Status};

use wr_common::wruntime::{
    manager_service_server::ManagerService,
    DeregisterEngineRequest, DeregisterEngineResponse,
    DeleteRoutingRuleRequest, DeleteRoutingRuleResponse,
    GetMetricsSummaryRequest, GetMetricsSummaryResponse,
    GetRoutingTableRequest, GetRoutingTableResponse,
    GetSchemaRequest, GetSchemaResponse,
    HeartbeatRequest, HeartbeatResponse,
    ListEnginesRequest, ListEnginesResponse,
    RegisterEngineRequest, RegisterEngineResponse,
    ReportMetricsRequest, ReportMetricsResponse,
    RoutingRule,
    UploadSchemaRequest, UploadSchemaResponse,
    UpsertRoutingRuleResponse,
};

use crate::state::SharedState;

/// Maximum number of `RequestMetrics` entries held in memory.
const MAX_METRICS: usize = 10_000;

pub struct Manager {
    state: SharedState,
}

impl Manager {
    pub fn new(state: SharedState) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl ManagerService for Manager {
    // ── Engine lifecycle ──────────────────────────────────────────────────

    async fn register_engine(
        &self,
        request: Request<RegisterEngineRequest>,
    ) -> Result<Response<RegisterEngineResponse>, Status> {
        let reg = request
            .into_inner()
            .registration
            .ok_or_else(|| Status::invalid_argument("registration field is required"))?;

        if reg.engine_id.is_empty() {
            return Err(Status::invalid_argument("engine_id is required"));
        }

        let engine_id = reg.engine_id.clone();
        let mut state = self.state.write().await;

        // Store the FileDescriptorSet bytes from each module's descriptor
        for module in &reg.modules {
            if !module.proto_schema.is_empty() {
                state.schemas.insert(
                    (module.name.clone(), module.version.clone()),
                    module.proto_schema.clone(),
                );
            }
        }

        // Initialise module health so the monitor doesn't immediately mark
        // modules as unhealthy before their first heartbeat arrives.
        for module in &reg.modules {
            state.module_health.insert(
                (engine_id.clone(), module.name.clone(), module.version.clone()),
                Instant::now(),
            );
        }

        state.engines.insert(engine_id.clone(), reg);
        state.heartbeats.insert(engine_id.clone(), Instant::now());

        println!("[manager] engine registered: {engine_id}");
        Ok(Response::new(RegisterEngineResponse { accepted: true }))
    }

    async fn deregister_engine(
        &self,
        request: Request<DeregisterEngineRequest>,
    ) -> Result<Response<DeregisterEngineResponse>, Status> {
        let engine_id = request.into_inner().engine_id;
        let mut state = self.state.write().await;

        state.engines.remove(&engine_id);
        state.heartbeats.remove(&engine_id);
        state.module_health.retain(|(eid, _, _), _| eid != &engine_id);

        // Mark all routing rules for this engine as unhealthy so proxies
        // stop sending traffic before the next routing table sync.
        let mut changed = false;
        for rule in &mut state.routing_table.rules {
            if rule.engine_id == engine_id && rule.healthy {
                rule.healthy = false;
                changed = true;
            }
        }
        if changed {
            state.routing_table.version += 1;
        }

        println!("[manager] engine deregistered: {engine_id}");
        Ok(Response::new(DeregisterEngineResponse {}))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req       = request.into_inner();
        let engine_id = req.engine_id;
        let now       = Instant::now();
        let mut state = self.state.write().await;

        state.heartbeats.insert(engine_id.clone(), now);

        for module in &req.healthy_modules {
            state.module_health.insert(
                (engine_id.clone(), module.name.clone(), module.version.clone()),
                now,
            );
        }

        Ok(Response::new(HeartbeatResponse {}))
    }

    async fn list_engines(
        &self,
        _request: Request<ListEnginesRequest>,
    ) -> Result<Response<ListEnginesResponse>, Status> {
        let state   = self.state.read().await;
        let engines = state.engines.values().cloned().collect();
        Ok(Response::new(ListEnginesResponse { engines }))
    }

    // ── Routing table ─────────────────────────────────────────────────────

    async fn get_routing_table(
        &self,
        _request: Request<GetRoutingTableRequest>,
    ) -> Result<Response<GetRoutingTableResponse>, Status> {
        let state = self.state.read().await;
        Ok(Response::new(GetRoutingTableResponse {
            table: Some(state.routing_table.clone()),
        }))
    }

    async fn upsert_routing_rule(
        &self,
        request: Request<RoutingRule>,
    ) -> Result<Response<UpsertRoutingRuleResponse>, Status> {
        let mut rule = request.into_inner();
        rule.healthy = true; // explicitly upserted rules are always healthy

        if rule.rule_id.is_empty() {
            return Err(Status::invalid_argument("rule_id is required"));
        }

        let mut state = self.state.write().await;
        let rules     = &mut state.routing_table.rules;

        match rules.iter_mut().find(|r| r.rule_id == rule.rule_id) {
            Some(existing) => {
                println!("[manager] routing rule updated: {}", rule.rule_id);
                *existing = rule;
            }
            None => {
                println!(
                    "[manager] routing rule added: {} → {}@{} (engine {})",
                    rule.source_module, rule.destination_module,
                    rule.destination_version, rule.engine_id
                );
                rules.push(rule);
            }
        }
        state.routing_table.version += 1;

        Ok(Response::new(UpsertRoutingRuleResponse {}))
    }

    async fn delete_routing_rule(
        &self,
        request: Request<DeleteRoutingRuleRequest>,
    ) -> Result<Response<DeleteRoutingRuleResponse>, Status> {
        let rule_id   = request.into_inner().rule_id;
        let mut state = self.state.write().await;
        let before    = state.routing_table.rules.len();

        state.routing_table.rules.retain(|r| r.rule_id != rule_id);

        if state.routing_table.rules.len() < before {
            state.routing_table.version += 1;
            println!("[manager] routing rule deleted: {rule_id}");
        }

        Ok(Response::new(DeleteRoutingRuleResponse {}))
    }

    // ── Schemas ───────────────────────────────────────────────────────────

    async fn get_schema(
        &self,
        request: Request<GetSchemaRequest>,
    ) -> Result<Response<GetSchemaResponse>, Status> {
        let req   = request.into_inner();
        let state = self.state.read().await;

        let schema = state
            .schemas
            .get(&(req.module.clone(), req.version.clone()))
            .cloned()
            .ok_or_else(|| {
                Status::not_found(format!("no schema for {}/{}", req.module, req.version))
            })?;

        Ok(Response::new(GetSchemaResponse { proto_schema: schema }))
    }

    async fn upload_schema(
        &self,
        request: Request<UploadSchemaRequest>,
    ) -> Result<Response<UploadSchemaResponse>, Status> {
        let req = request.into_inner();

        if req.module.is_empty() || req.version.is_empty() {
            return Err(Status::invalid_argument("module and version are required"));
        }

        self.state
            .write()
            .await
            .schemas
            .insert((req.module.clone(), req.version.clone()), req.proto_schema);

        println!("[manager] schema stored: {}/{}", req.module, req.version);
        Ok(Response::new(UploadSchemaResponse {}))
    }

    // ── Metrics ───────────────────────────────────────────────────────────

    async fn report_metrics(
        &self,
        request: Request<ReportMetricsRequest>,
    ) -> Result<Response<ReportMetricsResponse>, Status> {
        let incoming  = request.into_inner().metrics;
        let mut state = self.state.write().await;

        for metric in incoming {
            if state.metrics.len() >= MAX_METRICS {
                state.metrics.pop_front(); // evict oldest
            }
            state.metrics.push_back(metric);
        }

        Ok(Response::new(ReportMetricsResponse {}))
    }

    async fn get_metrics_summary(
        &self,
        _request: Request<GetMetricsSummaryRequest>,
    ) -> Result<Response<GetMetricsSummaryResponse>, Status> {
        let state   = self.state.read().await;
        let metrics = state.metrics.iter().cloned().collect();
        Ok(Response::new(GetMetricsSummaryResponse { metrics }))
    }
}
