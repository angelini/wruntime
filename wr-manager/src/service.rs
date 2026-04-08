use std::collections::HashMap;
use std::sync::Arc;

use deadpool_postgres::Pool;
use tonic::{Request, Response, Status};
use tracing::info;

use wr_common::naming::namespace_role;
use wr_common::wruntime::{
    manager_service_server::ManagerService, DeleteRoutingRuleRequest, DeleteRoutingRuleResponse,
    DeleteScheduleRequest, DeleteScheduleResponse, DeleteSecretRequest, DeleteSecretResponse,
    DeregisterEngineRequest, DeregisterEngineResponse, GetRoutingTableRequest,
    GetRoutingTableResponse, GetSchemaRequest, GetSchemaResponse, HeartbeatRequest,
    HeartbeatResponse, ListEnginesRequest, ListEnginesResponse, ListManagersRequest,
    ListManagersResponse, ListSchedulesRequest, ListSchedulesResponse, ListSecretsRequest,
    ListSecretsResponse, ManagerInfo, NamespaceDbCredential, NamespaceSecrets,
    RegisterEngineRequest, RegisterEngineResponse, RoutingRule, Schedule, SecretEntry,
    SetSecretRequest, SetSecretResponse, UpsertRoutingRuleResponse, UpsertScheduleRequest,
    UpsertScheduleResponse,
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

    /// Ensure a DB password exists for the given namespace, creating one if not.
    /// Returns the plaintext password.
    async fn ensure_db_password(&self, namespace: &str) -> Result<String, Status> {
        let key = "__db_password";
        let existing =
            db::get_secrets(&self.pool, &[(namespace.to_string(), key.to_string())]).await?;

        if let Some((_, _, ciphertext, nonce)) = existing.into_iter().next() {
            return self
                .crypto
                .decrypt(&ciphertext, &nonce)
                .map_err(|e| Status::internal(format!("failed to decrypt db password: {e}")));
        }

        // Generate and store a new random password
        let password = SecretCrypto::generate_random_password();
        let (ciphertext, nonce) = self
            .crypto
            .encrypt(&password)
            .map_err(|e| Status::internal(format!("encryption failed: {e}")))?;
        db::upsert_secret(&self.pool, namespace, key, &ciphertext, &nonce).await?;
        Ok(password)
    }

    /// Resolve DB credentials for each namespace that needs database access.
    async fn resolve_db_credentials(
        &self,
        db_namespaces: &[String],
    ) -> Result<Vec<NamespaceDbCredential>, Status> {
        let mut credentials = Vec::with_capacity(db_namespaces.len());
        // Deduplicate namespaces
        let unique: std::collections::HashSet<&str> =
            db_namespaces.iter().map(|s| s.as_str()).collect();
        for namespace in unique {
            let role = namespace_role(namespace);
            let password = self.ensure_db_password(namespace).await?;
            credentials.push(NamespaceDbCredential {
                namespace: namespace.to_string(),
                role,
                password,
            });
        }
        Ok(credentials)
    }

    /// Fetch, validate, decrypt, and group secrets by namespace.
    async fn resolve_secrets(
        &self,
        requests: &[wr_common::wruntime::SecretRequest],
    ) -> Result<Vec<NamespaceSecrets>, Status> {
        if requests.is_empty() {
            return Ok(vec![]);
        }

        // Block reserved key prefix
        for req in requests {
            if req.key.starts_with("__") {
                return Err(Status::invalid_argument(format!(
                    "secret key '{}' uses reserved prefix '__'",
                    req.key
                )));
            }
        }

        let pairs: Vec<(String, String)> = requests
            .iter()
            .map(|s| (s.namespace.clone(), s.key.clone()))
            .collect();
        let encrypted = db::get_secrets(&self.pool, &pairs).await?;

        // Check for missing secrets
        let found: std::collections::HashSet<(String, String)> = encrypted
            .iter()
            .map(|(ns, key, _, _)| (ns.clone(), key.clone()))
            .collect();
        let missing: Vec<String> = pairs
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

        Ok(by_namespace
            .into_iter()
            .map(|(namespace, secrets)| NamespaceSecrets { namespace, secrets })
            .collect())
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

        // Validate modules — proto_schema is only required on the first
        // descriptor for a given (namespace, name, version) tuple; additional
        // entries represent extra instances on the same engine.
        {
            let mut seen = std::collections::HashSet::new();
            for module in &reg.modules {
                if module.namespace.is_empty() {
                    return Err(Status::invalid_argument(format!(
                        "module '{}' is missing a namespace",
                        module.name
                    )));
                }
                let first =
                    seen.insert((&module.namespace, &module.name, &module.version));
                if first && module.proto_schema.is_empty() {
                    return Err(Status::invalid_argument(format!(
                        "module '{}' in namespace '{}' has no schema — proto_schema is required",
                        module.name, module.namespace
                    )));
                }
            }
        }

        let engine_id = reg.engine_id.clone();

        // Persist to DB (sets last_heartbeat = NOW() via column default / ON CONFLICT)
        db::upsert_engine_and_schemas(&self.pool, &reg).await?;

        // Resolve requested secrets
        let secrets = self.resolve_secrets(&reg.secrets).await?;

        // Resolve DB credentials for namespaces that need database access
        let db_credentials = self.resolve_db_credentials(&reg.db_namespaces).await?;

        info!(engine_id, "engine registered");
        Ok(Response::new(RegisterEngineResponse {
            accepted: true,
            secrets,
            db_credentials,
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

    // ── Manager discovery ─────────────────────────────────────────────────

    async fn list_managers(
        &self,
        _request: Request<ListManagersRequest>,
    ) -> Result<Response<ListManagersResponse>, Status> {
        let records = db::list_managers(&self.pool).await?;
        let managers = records
            .into_iter()
            .map(|r| ManagerInfo {
                manager_id: r.manager_id,
                grpc_address: r.grpc_address,
            })
            .collect();
        Ok(Response::new(ListManagersResponse { managers }))
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
        if req.key.starts_with("__") {
            return Err(Status::invalid_argument(
                "secret keys starting with '__' are reserved for internal use",
            ));
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
            .filter(|(_, key)| !key.starts_with("__"))
            .map(|(namespace, key)| SecretEntry { namespace, key })
            .collect();
        Ok(Response::new(ListSecretsResponse { secrets }))
    }

    // ── Schedules ──────────────────────────────────────────────────────────

    async fn upsert_schedule(
        &self,
        request: Request<UpsertScheduleRequest>,
    ) -> Result<Response<UpsertScheduleResponse>, Status> {
        let req = request.into_inner();
        if req.worker_namespace.is_empty()
            || req.worker_name.is_empty()
            || req.worker_version.is_empty()
            || req.job_type.is_empty()
        {
            return Err(Status::invalid_argument(
                "worker_namespace, worker_name, worker_version, and job_type are required",
            ));
        }
        if req.interval_secs <= 0 {
            return Err(Status::invalid_argument("interval_secs must be > 0"));
        }

        let schedule_id = db::upsert_schedule(
            &self.pool,
            &req.worker_namespace,
            &req.worker_name,
            &req.worker_version,
            &req.job_type,
            req.interval_secs,
            req.immediate,
            &req.payload,
            req.timeout_secs,
            req.max_attempts,
        )
        .await?;

        info!(
            schedule_id,
            worker = %format!("{}/{}/{}", req.worker_namespace, req.worker_name, req.worker_version),
            job_type = %req.job_type,
            "schedule upserted"
        );
        Ok(Response::new(UpsertScheduleResponse { schedule_id }))
    }

    async fn delete_schedule(
        &self,
        request: Request<DeleteScheduleRequest>,
    ) -> Result<Response<DeleteScheduleResponse>, Status> {
        let req = request.into_inner();
        if req.worker_namespace.is_empty()
            || req.worker_name.is_empty()
            || req.worker_version.is_empty()
            || req.job_type.is_empty()
        {
            return Err(Status::invalid_argument(
                "worker_namespace, worker_name, worker_version, and job_type are required",
            ));
        }

        db::delete_schedule(
            &self.pool,
            &req.worker_namespace,
            &req.worker_name,
            &req.worker_version,
            &req.job_type,
        )
        .await?;

        info!(
            worker_namespace = %req.worker_namespace,
            job_type = %req.job_type,
            "schedule deleted"
        );
        Ok(Response::new(DeleteScheduleResponse {}))
    }

    async fn list_schedules(
        &self,
        request: Request<ListSchedulesRequest>,
    ) -> Result<Response<ListSchedulesResponse>, Status> {
        let req = request.into_inner();
        let rows = db::list_schedules(&self.pool, &req.worker_namespace).await?;
        let schedules = rows
            .into_iter()
            .map(|r| Schedule {
                schedule_id: r.schedule_id,
                worker_namespace: r.worker_namespace,
                worker_name: r.worker_name,
                worker_version: r.worker_version,
                job_type: r.job_type,
                interval_secs: r.interval_secs,
                immediate: r.immediate,
                payload: r.payload,
                timeout_secs: r.timeout_secs,
                max_attempts: r.max_attempts,
                enabled: r.enabled,
                last_fired_at: r.last_fired_at.map(|t| t.to_rfc3339()).unwrap_or_default(),
            })
            .collect();
        Ok(Response::new(ListSchedulesResponse { schedules }))
    }
}
