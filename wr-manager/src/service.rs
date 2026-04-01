use std::time::Instant;

use deadpool_postgres::Pool;
use tonic::{Request, Response, Status};
use tracing::info;

use wr_common::wruntime::{
    manager_service_server::ManagerService, DeleteRoutingRuleRequest, DeleteRoutingRuleResponse,
    DeregisterEngineRequest, DeregisterEngineResponse, GetRoutingTableRequest,
    GetRoutingTableResponse, GetSchemaRequest, GetSchemaResponse, HeartbeatRequest,
    HeartbeatResponse, ListEnginesRequest, ListEnginesResponse, RegisterEngineRequest,
    RegisterEngineResponse, RoutingRule, UploadSchemaRequest, UploadSchemaResponse,
    UpsertRoutingRuleResponse,
};

use crate::db;
use crate::state::SharedState;

pub struct Manager {
    state: SharedState,
    pool: Pool,
}

impl Manager {
    pub fn new(state: SharedState, pool: Pool) -> Self {
        Self { state, pool }
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

        // Validate modules
        for module in &reg.modules {
            if module.namespace.is_empty() {
                return Err(Status::invalid_argument(format!(
                    "module '{}' is missing a namespace",
                    module.name
                )));
            }
            if module.proto_schema.is_empty() {
                return Err(Status::invalid_argument(format!(
                    "module '{}' in namespace '{}' has no schema — proto_schema is required",
                    module.name, module.namespace
                )));
            }
        }

        let engine_id = reg.engine_id.clone();

        // Persist to DB
        db::upsert_engine_and_schemas(&self.pool, &reg).await?;

        // Update ephemeral state
        let mut state = self.state.write().await;
        for module in &reg.modules {
            state.module_health.insert(
                (
                    engine_id.clone(),
                    module.namespace.clone(),
                    module.name.clone(),
                    module.version.clone(),
                ),
                Instant::now(),
            );
        }
        state.heartbeats.insert(engine_id.clone(), Instant::now());

        info!(engine_id, "engine registered");
        Ok(Response::new(RegisterEngineResponse { accepted: true }))
    }

    async fn deregister_engine(
        &self,
        request: Request<DeregisterEngineRequest>,
    ) -> Result<Response<DeregisterEngineResponse>, Status> {
        let engine_id = request.into_inner().engine_id;

        // Persist to DB (marks rules unhealthy, deletes engine)
        db::deregister_engine(&self.pool, &engine_id).await?;

        // Clean up ephemeral state
        let mut state = self.state.write().await;
        state.heartbeats.remove(&engine_id);
        state
            .module_health
            .retain(|(eid, _, _, _), _| eid != &engine_id);

        info!(engine_id, "engine deregistered");
        Ok(Response::new(DeregisterEngineResponse {}))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        let engine_id = req.engine_id;
        let now = Instant::now();
        let mut state = self.state.write().await;

        state.heartbeats.insert(engine_id.clone(), now);

        for module in &req.healthy_modules {
            state.module_health.insert(
                (
                    engine_id.clone(),
                    module.namespace.clone(),
                    module.name.clone(),
                    module.version.clone(),
                ),
                now,
            );
        }

        Ok(Response::new(HeartbeatResponse {}))
    }

    async fn list_engines(
        &self,
        _request: Request<ListEnginesRequest>,
    ) -> Result<Response<ListEnginesResponse>, Status> {
        let engines = db::list_engines(&self.pool).await?;
        Ok(Response::new(ListEnginesResponse { engines }))
    }

    // ── Routing table ─────────────────────────────────────────────────────

    async fn get_routing_table(
        &self,
        _request: Request<GetRoutingTableRequest>,
    ) -> Result<Response<GetRoutingTableResponse>, Status> {
        let table = db::get_routing_table(&self.pool).await?;
        Ok(Response::new(GetRoutingTableResponse {
            table: Some(table),
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

        info!(
            rule_id              = %rule.rule_id,
            source               = %rule.source_module,
            source_namespace     = %rule.source_namespace,
            destination          = %rule.destination_module,
            destination_namespace = %rule.destination_namespace,
            version              = %rule.destination_version,
            engine_id            = %rule.engine_id,
            "routing rule upserted",
        );

        db::upsert_routing_rule(&self.pool, &rule).await?;
        Ok(Response::new(UpsertRoutingRuleResponse {}))
    }

    async fn delete_routing_rule(
        &self,
        request: Request<DeleteRoutingRuleRequest>,
    ) -> Result<Response<DeleteRoutingRuleResponse>, Status> {
        let rule_id = request.into_inner().rule_id;

        if db::delete_routing_rule(&self.pool, &rule_id).await? {
            info!(rule_id, "routing rule deleted");
        }

        Ok(Response::new(DeleteRoutingRuleResponse {}))
    }

    // ── Schemas ───────────────────────────────────────────────────────────

    async fn get_schema(
        &self,
        request: Request<GetSchemaRequest>,
    ) -> Result<Response<GetSchemaResponse>, Status> {
        let req = request.into_inner();

        if req.namespace.is_empty() {
            return Err(Status::invalid_argument("namespace is required"));
        }

        let proto_schema =
            db::get_schema(&self.pool, &req.namespace, &req.module, &req.version).await?;
        Ok(Response::new(GetSchemaResponse { proto_schema }))
    }

    async fn upload_schema(
        &self,
        request: Request<UploadSchemaRequest>,
    ) -> Result<Response<UploadSchemaResponse>, Status> {
        let req = request.into_inner();

        if req.module.is_empty() || req.version.is_empty() || req.namespace.is_empty() {
            return Err(Status::invalid_argument(
                "namespace, module, and version are required",
            ));
        }

        db::upsert_schema(
            &self.pool,
            &req.namespace,
            &req.module,
            &req.version,
            &req.proto_schema,
        )
        .await?;

        info!(namespace = %req.namespace, module = %req.module, version = %req.version, "schema stored");
        Ok(Response::new(UploadSchemaResponse {}))
    }
}
