use std::collections::HashMap;
use std::sync::Arc;

use deadpool_postgres::Pool;
use tonic::{Request, Response, Status};
use tracing::info;

use wr_common::wruntime::{
    manager_service_server::ManagerService, DeleteRoutingRuleRequest, DeleteRoutingRuleResponse,
    DeleteSecretRequest, DeleteSecretResponse, DeregisterEngineRequest, DeregisterEngineResponse,
    GetRoutingTableRequest, GetRoutingTableResponse, GetSchemaRequest, GetSchemaResponse,
    HeartbeatRequest, HeartbeatResponse, ListEnginesRequest, ListEnginesResponse,
    ListSecretsRequest, ListSecretsResponse, NamespaceSecrets, RegisterEngineRequest,
    RegisterEngineResponse, RoutingRule, SecretEntry, SetSecretRequest, SetSecretResponse,
    UpsertRoutingRuleResponse,
};

use crate::crypto::SecretCrypto;
use crate::db;

pub struct Manager {
    pool: Pool,
    crypto: Arc<SecretCrypto>,
}

impl Manager {
    pub fn new(pool: Pool, crypto: Arc<SecretCrypto>) -> Self {
        Self { pool, crypto }
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

        // Persist to DB (sets last_heartbeat = NOW() via column default / ON CONFLICT)
        db::upsert_engine_and_schemas(&self.pool, &reg).await?;

        // Resolve requested secrets
        let secrets = if reg.secrets.is_empty() {
            vec![]
        } else {
            let requests: Vec<(String, String)> = reg
                .secrets
                .iter()
                .map(|s| (s.namespace.clone(), s.key.clone()))
                .collect();
            let encrypted = db::get_secrets(&self.pool, &requests).await?;

            // Check for missing secrets
            let found: std::collections::HashSet<(String, String)> = encrypted
                .iter()
                .map(|(ns, key, _, _)| (ns.clone(), key.clone()))
                .collect();
            let missing: Vec<String> = requests
                .iter()
                .filter(|r| !found.contains(r))
                .map(|(ns, key)| format!("{ns}/{key}"))
                .collect();
            if !missing.is_empty() {
                return Err(Status::not_found(format!(
                    "missing secrets: {}",
                    missing.join(", ")
                )));
            }

            // Decrypt and group by namespace
            let mut by_namespace: HashMap<String, HashMap<String, String>> = HashMap::new();
            for (ns, key, ciphertext, nonce) in &encrypted {
                let plaintext = self
                    .crypto
                    .decrypt(ciphertext, nonce)
                    .map_err(|e| Status::internal(format!("failed to decrypt secret: {e}")))?;
                by_namespace
                    .entry(ns.clone())
                    .or_default()
                    .insert(key.clone(), plaintext);
            }

            by_namespace
                .into_iter()
                .map(|(namespace, secrets)| NamespaceSecrets { namespace, secrets })
                .collect()
        };

        info!(engine_id, "engine registered");
        Ok(Response::new(RegisterEngineResponse {
            accepted: true,
            secrets,
        }))
    }

    async fn deregister_engine(
        &self,
        request: Request<DeregisterEngineRequest>,
    ) -> Result<Response<DeregisterEngineResponse>, Status> {
        let engine_id = request.into_inner().engine_id;

        // Persist to DB (marks rules unhealthy, deletes engine)
        db::deregister_engine(&self.pool, &engine_id).await?;

        info!(engine_id, "engine deregistered");
        Ok(Response::new(DeregisterEngineResponse {}))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let engine_id = request.into_inner().engine_id;
        db::heartbeat_engine(&self.pool, &engine_id).await?;
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
        request: Request<GetRoutingTableRequest>,
    ) -> Result<Response<GetRoutingTableResponse>, Status> {
        let known_version = request.into_inner().known_version;
        let table = db::get_routing_table(&self.pool, known_version).await?;
        Ok(Response::new(GetRoutingTableResponse { table }))
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

    // ── Secrets ──────────────────────────────────────────────────────────

    async fn set_secret(
        &self,
        request: Request<SetSecretRequest>,
    ) -> Result<Response<SetSecretResponse>, Status> {
        let req = request.into_inner();
        if req.namespace.is_empty() || req.key.is_empty() {
            return Err(Status::invalid_argument("namespace and key are required"));
        }

        let (ciphertext, nonce) = self
            .crypto
            .encrypt(&req.value)
            .map_err(|e| Status::internal(format!("encryption failed: {e}")))?;

        db::upsert_secret(&self.pool, &req.namespace, &req.key, &ciphertext, &nonce).await?;
        info!(namespace = %req.namespace, key = %req.key, "secret stored");
        Ok(Response::new(SetSecretResponse {}))
    }

    async fn delete_secret(
        &self,
        request: Request<DeleteSecretRequest>,
    ) -> Result<Response<DeleteSecretResponse>, Status> {
        let req = request.into_inner();
        if req.namespace.is_empty() || req.key.is_empty() {
            return Err(Status::invalid_argument("namespace and key are required"));
        }

        db::delete_secret(&self.pool, &req.namespace, &req.key).await?;
        info!(namespace = %req.namespace, key = %req.key, "secret deleted");
        Ok(Response::new(DeleteSecretResponse {}))
    }

    async fn list_secrets(
        &self,
        request: Request<ListSecretsRequest>,
    ) -> Result<Response<ListSecretsResponse>, Status> {
        let req = request.into_inner();
        let entries = db::list_secrets(&self.pool, &req.namespace).await?;
        let secrets = entries
            .into_iter()
            .map(|(namespace, key)| SecretEntry { namespace, key })
            .collect();
        Ok(Response::new(ListSecretsResponse { secrets }))
    }
}
