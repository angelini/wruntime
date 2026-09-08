use std::collections::HashSet;
use std::sync::Arc;

use deadpool_postgres::Pool;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use wr_common::identity::{
    EngineHttpUrl, EngineId, JobQueueId, ModuleId, Namespace, NamespaceFilter, PeerHttpsUrl,
    ProxyHttpUrl, RouteKey, RuleId,
};
use wr_common::lifecycle_service::{AdmissionGate, AdmissionGuard, ManagerLifecycleState};
use wr_common::wruntime::{
    cluster_service_server::ClusterService, infrastructure_service_server::InfrastructureService,
    lifecycle_service_server::LifecycleService, node_service_server::NodeService,
    policy_service_server::PolicyService, AbandonDeploymentRequest, AbandonDeploymentResponse,
    AdvanceManagerRolloutRequest, AdvanceManagerRolloutResponse, AttestNodeAgentRequest,
    AttestNodeAgentResponse, BeginDeploymentRequest, BeginDeploymentResponse,
    BeginEngineDrainRequest, BeginEngineDrainResponse, BeginManagerRolloutRequest,
    BeginManagerRolloutResponse, BeginRollbackRequest, BeginRollbackResponse,
    CancelOperationRequest, CancelOperationResponse, ClaimNodeCleanupRequest,
    ClaimNodeCleanupResponse, ClaimOperationRequest, ClaimOperationResponse,
    DeleteRoutingRuleRequest, DeleteRoutingRuleResponse, DeleteScheduleRequest,
    DeleteScheduleResponse, DeleteSecretRequest, DeleteSecretResponse, DeploymentCondition,
    DeregisterEngineRequest, DeregisterEngineResponse, FinalizeDeploymentRequest,
    FinalizeDeploymentResponse, GetClusterStatusRequest, GetClusterStatusResponse,
    GetLifecycleStatusRequest, GetLifecycleStatusResponse, GetManagerRolloutRequest,
    GetManagerRolloutResponse, GetNodeCleanupStatusRequest, GetNodeCleanupStatusResponse,
    GetOperationRequest, GetOperationResponse, GetOperatorStatusRequest, GetOperatorStatusResponse,
    GetPolicyStatusRequest, GetPolicyStatusResponse, GetRoutingTableRequest,
    GetRoutingTableResponse, GetSchemaRequest, GetSchemaResponse, GetWorkloadSnapshotRequest,
    GetWorkloadSnapshotResponse, HeartbeatRequest, HeartbeatResponse, LeaseManagerRolloutRequest,
    LeaseManagerRolloutResponse, ListEnginesRequest, ListEnginesResponse, ListManagersRequest,
    ListManagersResponse, ListOperationsRequest, ListOperationsResponse, ListSchedulesRequest,
    ListSchedulesResponse, ListSecretsRequest, ListSecretsResponse, ManagerInfo,
    NodeOperationAction, PolicyCapHeadroom, PutNodeAgentPolicyRequest, PutNodeAgentPolicyResponse,
    RegisterEngineRequest, RegisterEngineResponse, RenewNodeCleanupLeaseRequest,
    RenewNodeCleanupLeaseResponse, RenewOperationLeaseRequest, RenewOperationLeaseResponse,
    ReportNodeCleanupResultRequest, ReportNodeCleanupResultResponse, ReportNodeObservationRequest,
    ReportNodeObservationResponse, ReportStepResultRequest, ReportStepResultResponse,
    ResumeOperationRequest, ResumeOperationResponse, RetryNodeCleanupRequest,
    RetryNodeCleanupResponse, RoutingRule, Schedule, SecretEntry, SetSecretRequest,
    SetSecretResponse, SlotAuthorityStatus, SubmitOperationRequest, SubmitOperationResponse,
    UpsertRoutingRuleResponse, UpsertScheduleRequest, UpsertScheduleResponse,
    VerifyDeploymentRequest, VerifyDeploymentResponse,
};

use crate::auth::PrincipalPolicy;
use crate::crypto::SecretCrypto;
use crate::db;

fn proto_timestamp(value: chrono::DateTime<chrono::Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: value.timestamp(),
        nanos: value.timestamp_subsec_nanos() as i32,
    }
}

fn deployment_condition(code: String, detail: String) -> DeploymentCondition {
    DeploymentCondition {
        code,
        detail,
        severity: wr_common::wruntime::StatusSeverity::Unhealthy as i32,
        affected_identity: String::new(),
        desired: String::new(),
        actual: String::new(),
    }
}

/// Project the database-fresh manager lease rows into the public contract.
/// The query already applies the configured freshness threshold with PostgreSQL
/// `NOW()` and returns deterministic manager-id order.
pub fn reconcile_managers(db_records: &[db::ManagerRecord]) -> Vec<ManagerInfo> {
    db_records
        .iter()
        .map(|record| ManagerInfo {
            manager_id: record.manager_id.clone(),
            grpc_address: record.grpc_address.clone(),
        })
        .collect()
}

/// Strict, mount-ready authorization seam for the six manager service
/// contracts. Phase 3 mounts these façades on the consolidated listener; Phase
/// 2 keeps the current production router topology unchanged.
#[derive(Clone)]
pub struct AuthAwareServiceFacade {
    service: &'static str,
    policy: PrincipalPolicy,
}

impl AuthAwareServiceFacade {
    pub fn new(service: &'static str, policy: PrincipalPolicy) -> Result<Self, Status> {
        if !crate::auth::MANAGER_SERVICE_SET.contains(&service) {
            return Err(Status::permission_denied(
                "service is outside the manager authorization registry",
            ));
        }
        Ok(Self { service, policy })
    }

    pub fn authorize(
        &self,
        method: &str,
        evidence: &wr_common::tls::LeafEvidence,
        resource: &crate::auth::AuthorizationResource<'_>,
    ) -> Result<crate::auth::AuthorizedPrincipal, Status> {
        self.policy
            .authorize_row_evidence(self.service, method, evidence, resource)
    }
}

#[derive(Clone)]
pub struct Manager {
    pool: Pool,
    crypto: Arc<SecretCrypto>,
    manager_liveness_threshold_secs: u64,
    engine_heartbeat_timeout_secs: f64,
    module_heartbeat_timeout_secs: f64,
    admission: AdmissionGate,
    workload_policy: Option<Arc<wr_common::authorization_policy::ValidatedPolicy>>,
}

impl Manager {
    pub fn new(pool: Pool, crypto: Arc<SecretCrypto>) -> Self {
        let admission = AdmissionGate::closed();
        admission.open();
        Self::with_admission(
            pool,
            crypto,
            wr_common::DEFAULT_MANAGER_LIVENESS_THRESHOLD_SECS,
            admission,
        )
    }

    pub fn with_admission(
        pool: Pool,
        crypto: Arc<SecretCrypto>,
        manager_liveness_threshold_secs: u64,
        admission: AdmissionGate,
    ) -> Self {
        Self::with_heartbeat_timeouts(
            pool,
            crypto,
            manager_liveness_threshold_secs,
            10.0,
            10.0,
            admission,
        )
    }

    pub fn with_heartbeat_timeouts(
        pool: Pool,
        crypto: Arc<SecretCrypto>,
        manager_liveness_threshold_secs: u64,
        engine_heartbeat_timeout_secs: f64,
        module_heartbeat_timeout_secs: f64,
        admission: AdmissionGate,
    ) -> Self {
        Self {
            pool,
            crypto,
            manager_liveness_threshold_secs,
            engine_heartbeat_timeout_secs,
            module_heartbeat_timeout_secs,
            admission,
            workload_policy: None,
        }
    }

    pub fn with_workload_policy(
        mut self,
        policy: Arc<wr_common::authorization_policy::ValidatedPolicy>,
    ) -> Self {
        self.workload_policy = Some(policy);
        self
    }

    fn require_admission(&self) -> Result<AdmissionGuard, Status> {
        self.admission
            .try_enter()
            .ok_or_else(|| Status::unavailable("manager is draining"))
    }

    fn valid_deployment_token(value: &str) -> bool {
        !value.is_empty()
            && value.len() <= 128
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    }

    fn valid_bundle_digest(value: &str) -> bool {
        value.len() == 71
            && value.starts_with("sha256:")
            && value[7..]
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    }

    fn canonicalize_deployment_request(request: &mut BeginDeploymentRequest) -> Result<(), Status> {
        let inventory = request
            .inventory
            .take()
            .ok_or_else(|| Status::invalid_argument("deployment inventory is required"))?;
        request.inventory = Some(
            wr_common::deployment_contract::canonicalize_inventory(inventory)
                .map_err(|error| Status::invalid_argument(error.to_string()))?,
        );
        Ok(())
    }

    fn validate_deployment_request(request: &BeginDeploymentRequest) -> Result<(), Status> {
        Namespace::parse(&request.node_id)
            .map_err(|_| Status::invalid_argument("node_id must be a valid stable identity"))?;
        if !Self::valid_deployment_token(&request.attempt_token) {
            return Err(Status::invalid_argument(
                "attempt_token must be 1..=128 URL-safe characters",
            ));
        }
        if !Self::valid_bundle_digest(&request.bundle_digest) {
            return Err(Status::invalid_argument(
                "bundle_digest must be sha256:<lowercase hex>",
            ));
        }
        wr_common::deployment_contract::canonicalize_inventory(
            request
                .inventory
                .clone()
                .ok_or_else(|| Status::invalid_argument("deployment inventory is required"))?,
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        Ok(())
    }
}

impl Manager {
    // ── Engine lifecycle ──────────────────────────────────────────────────

    async fn register_engine(
        &self,
        request: Request<RegisterEngineRequest>,
    ) -> Result<Response<RegisterEngineResponse>, Status> {
        let _admission = self.require_admission()?;
        let request = request.into_inner();
        let activation_id = request.activation_id;
        let reg = request
            .registration
            .ok_or_else(|| Status::invalid_argument("registration field is required"))?;

        EngineId::parse(&reg.engine_id)
            .and_then(|_| EngineHttpUrl::parse(&reg.address))
            .and_then(|_| ProxyHttpUrl::parse(&reg.proxy_address))
            .and_then(|_| PeerHttpsUrl::parse(&reg.peer_address))
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        match (
            reg.job_queue_id.is_empty(),
            reg.job_admin_address.is_empty(),
        ) {
            (true, true) => {}
            (false, false) => {
                JobQueueId::parse(&reg.job_queue_id)
                    .and_then(|_| PeerHttpsUrl::parse(&reg.job_admin_address))
                    .map_err(|error| Status::invalid_argument(error.to_string()))?;
                let admin_uri: http::Uri = reg.job_admin_address.parse().map_err(|error| {
                    Status::invalid_argument(format!("invalid job admin address: {error}"))
                })?;
                if matches!(admin_uri.host(), Some("0.0.0.0" | "::" | "[::]")) {
                    return Err(Status::invalid_argument(
                        "job_admin_address must not advertise an unspecified host",
                    ));
                }
            }
            _ => {
                return Err(Status::invalid_argument(
                    "job_queue_id and job_admin_address must be provided together",
                ));
            }
        }

        // Validate modules — proto_schema is only required on the first
        // descriptor for a given (namespace, name, version) tuple; additional
        // entries represent extra instances on the same engine.
        {
            for module in &reg.modules {
                ModuleId::parse(&module.namespace, &module.name, &module.version)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?;
            }
        }

        for namespace in reg
            .db_namespaces
            .iter()
            .chain(reg.secrets.iter().map(|s| &s.namespace))
        {
            Namespace::parse(namespace)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        }

        if let Some(metadata) = &reg.deployment {
            Namespace::parse(&metadata.node_id)
                .map_err(|_| Status::invalid_argument("deployment.node_id is invalid"))?;
            if metadata.revision == 0
                || metadata.revision > i64::MAX as u64
                || !Self::valid_bundle_digest(&metadata.bundle_digest)
                || !Self::valid_deployment_token(&metadata.engine_slot)
            {
                return Err(Status::invalid_argument(
                    "deployment metadata requires a non-zero revision, sha256 digest, and valid engine slot",
                ));
            }
        }

        let engine_id = reg.engine_id.clone();

        // Desired state, ownership, credential lookup/creation, engine state, and
        // initially-unhealthy routes commit in one transaction. No plaintext or
        // fence escapes when any domain statement fails.
        let committed =
            db::register_engine_and_routes(&self.pool, &self.crypto, &reg, &activation_id).await?;
        let fence = committed.fence;
        let secrets = committed.secrets;
        let db_credentials = committed.db_credentials;
        let serialized_snapshot = self
            .workload_policy
            .as_ref()
            .map(|policy| {
                wr_common::snapshot_consumer::build_snapshot(
                    policy,
                    wr_common::wruntime::WorkloadProjectionKind::EngineJobAdminV1,
                    std::time::SystemTime::now(),
                )
            })
            .transpose()
            .map_err(|error| Status::internal(error.to_string()))?
            .unwrap_or_default();

        info!(
            engine_id,
            generation = fence.slot_generation,
            "engine registered"
        );
        Ok(Response::new(RegisterEngineResponse {
            accepted: true,
            secrets,
            db_credentials,
            fence: Some(fence),
            serialized_snapshot,
        }))
    }

    async fn deregister_engine(
        &self,
        request: Request<DeregisterEngineRequest>,
    ) -> Result<Response<DeregisterEngineResponse>, Status> {
        let request = request.into_inner();
        let engine_id = request.engine_id;
        let fence = request
            .fence
            .ok_or_else(|| Status::permission_denied("ownership fence is required"))?;
        db::deregister_engine(&self.pool, &engine_id, &fence).await?;
        info!(
            engine_id,
            generation = fence.slot_generation,
            "engine deregistered"
        );
        Ok(Response::new(DeregisterEngineResponse {
            accepted_fence: Some(fence),
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let HeartbeatRequest {
            engine_id,
            healthy_modules,
            fence,
        } = request.into_inner();
        let fence =
            fence.ok_or_else(|| Status::permission_denied("ownership fence is required"))?;

        // Validate each reported module independently; skip and log invalid
        // entries rather than rejecting the whole heartbeat.
        let mut valid = Vec::with_capacity(healthy_modules.len());
        for m in healthy_modules {
            if ModuleId::parse(&m.namespace, &m.name, &m.version).is_err() {
                warn!(
                    engine_id = %engine_id,
                    namespace = %m.namespace,
                    module = %m.name,
                    version = %m.version,
                    "skipping malformed module heartbeat entry",
                );
                continue;
            }
            valid.push(m);
        }

        let routing_version =
            db::publish_engine_readiness(&self.pool, &engine_id, &valid, &fence).await?;
        let serialized_snapshot = self
            .workload_policy
            .as_ref()
            .map(|policy| {
                wr_common::snapshot_consumer::build_snapshot(
                    policy,
                    wr_common::wruntime::WorkloadProjectionKind::EngineJobAdminV1,
                    std::time::SystemTime::now(),
                )
            })
            .transpose()
            .map_err(|error| Status::internal(error.to_string()))?
            .unwrap_or_default();
        Ok(Response::new(HeartbeatResponse {
            manager_routing_table_version: routing_version,
            proxy_routing_table_version: 0,
            accepted_fence: Some(fence),
            serialized_snapshot,
        }))
    }

    async fn begin_engine_drain(
        &self,
        request: Request<BeginEngineDrainRequest>,
    ) -> Result<Response<BeginEngineDrainResponse>, Status> {
        let request = request.into_inner();
        let engine_id = request.engine_id;
        let fence = request
            .fence
            .ok_or_else(|| Status::permission_denied("ownership fence is required"))?;
        EngineId::parse(&engine_id).map_err(|error| Status::invalid_argument(error.to_string()))?;
        let routing_version = db::begin_engine_drain(&self.pool, &engine_id, &fence).await?;
        info!(engine_id, routing_version, "engine routes withdrawn");
        Ok(Response::new(BeginEngineDrainResponse {
            manager_routing_table_version: routing_version,
            proxy_routing_table_version: 0,
            accepted_fence: Some(fence),
        }))
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
        let db_records =
            db::list_managers(&self.pool, self.manager_liveness_threshold_secs).await?;
        Ok(Response::new(ListManagersResponse {
            managers: reconcile_managers(&db_records),
        }))
    }

    async fn get_cluster_status(
        &self,
        _request: Request<GetClusterStatusRequest>,
    ) -> Result<Response<GetClusterStatusResponse>, Status> {
        let snapshot = db::get_cluster_status_snapshot(&self.pool).await?;
        let response = crate::status::compose(
            snapshot,
            self.manager_liveness_threshold_secs as f64,
            self.engine_heartbeat_timeout_secs,
            self.module_heartbeat_timeout_secs,
        )?;
        Ok(Response::new(response))
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
        let _admission = self.require_admission()?;
        let mut rule = request.into_inner();
        rule.healthy = true; // explicitly upserted rules are always healthy

        RuleId::parse(&rule.rule_id)
            .and_then(|_| EngineId::parse(&rule.engine_id))
            .and_then(|_| {
                ModuleId::parse(
                    &rule.destination_namespace,
                    &rule.destination_module,
                    &rule.destination_version,
                )
            })
            .and_then(|_| EngineHttpUrl::parse(&rule.engine_address))
            .and_then(|_| PeerHttpsUrl::parse(&rule.peer_address))
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if !rule.source_namespace.is_empty() || !rule.source_module.is_empty() {
            RouteKey::parse(&rule.source_namespace, &rule.source_module)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
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
        let _admission = self.require_admission()?;
        let req = request.into_inner();
        Namespace::parse(&req.namespace)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if req.key.is_empty() {
            return Err(Status::invalid_argument("key is required"));
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
        let _admission = self.require_admission()?;
        let req = request.into_inner();
        Namespace::parse(&req.namespace)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if req.key.is_empty() {
            return Err(Status::invalid_argument("key is required"));
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
        let filter = NamespaceFilter::from_wire(&req.namespace)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let entries = db::list_secrets(&self.pool, &filter).await?;
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
        let _admission = self.require_admission()?;
        let req = request.into_inner();
        ModuleId::parse(&req.worker_namespace, &req.worker_name, &req.worker_version)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if req.job_type.is_empty() {
            return Err(Status::invalid_argument("job_type is required"));
        }
        wr_common::lifecycle::ScheduleIntervalSecs::new(req.interval_secs)
            .and_then(|_| wr_common::lifecycle::JobTimeoutSecs::new(req.timeout_secs))
            .and_then(|_| wr_common::lifecycle::MaxAttempts::new(req.max_attempts))
            .map_err(|error| Status::invalid_argument(error.to_string()))?;

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
        let _admission = self.require_admission()?;
        let req = request.into_inner();
        ModuleId::parse(&req.worker_namespace, &req.worker_name, &req.worker_version)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if req.job_type.is_empty() {
            return Err(Status::invalid_argument("job_type is required"));
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
        let filter = NamespaceFilter::from_wire(&req.worker_namespace)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let rows = db::list_schedules(&self.pool, &filter).await?;
        let schedules = rows
            .into_iter()
            .map(|r| Schedule {
                schedule_id: r.schedule_id,
                worker_namespace: r.worker_namespace,
                worker_name: r.worker_name,
                worker_version: r.worker_version,
                job_type: r.job_type,
                interval_secs: u32::try_from(r.interval_secs)
                    .expect("schedule interval DB constraint"),
                immediate: r.immediate,
                payload: r.payload,
                timeout_secs: u32::try_from(r.timeout_secs)
                    .expect("schedule timeout DB constraint"),
                max_attempts: u32::try_from(r.max_attempts)
                    .expect("schedule attempts DB constraint"),
                enabled: r.enabled,
                last_fired_at: r.last_fired_at.map(proto_timestamp),
                next_fire_at: r.next_fire_at.map(proto_timestamp),
                last_error: r.last_error.unwrap_or_default(),
                consecutive_failures: wr_common::lifecycle::FailureCount::new(
                    u32::try_from(r.consecutive_failures)
                        .expect("schedule failure count DB constraint"),
                )
                .get(),
            })
            .collect();
        Ok(Response::new(ListSchedulesResponse { schedules }))
    }
}

/// Role-gated durable operator API. It composes existing status evidence and
/// delegates every mutation to one transactional operation state machine.
#[derive(Clone)]
pub struct OperatorApi {
    pool: Pool,
    policy: PrincipalPolicy,
    admission: AdmissionGate,
    manager_liveness_threshold_secs: f64,
    engine_heartbeat_timeout_secs: f64,
    module_heartbeat_timeout_secs: f64,
}

impl OperatorApi {
    pub fn new(
        pool: Pool,
        policy: PrincipalPolicy,
        manager_liveness_threshold_secs: f64,
        engine_heartbeat_timeout_secs: f64,
        module_heartbeat_timeout_secs: f64,
    ) -> Self {
        let admission = AdmissionGate::closed();
        admission.open();
        Self::with_admission(
            pool,
            policy,
            admission,
            manager_liveness_threshold_secs,
            engine_heartbeat_timeout_secs,
            module_heartbeat_timeout_secs,
        )
    }

    pub fn with_admission(
        pool: Pool,
        policy: PrincipalPolicy,
        admission: AdmissionGate,
        manager_liveness_threshold_secs: f64,
        engine_heartbeat_timeout_secs: f64,
        module_heartbeat_timeout_secs: f64,
    ) -> Self {
        Self {
            pool,
            policy,
            admission,
            manager_liveness_threshold_secs,
            engine_heartbeat_timeout_secs,
            module_heartbeat_timeout_secs,
        }
    }

    fn require_admission(&self) -> Result<AdmissionGuard, Status> {
        self.admission
            .try_enter()
            .ok_or_else(|| Status::unavailable("privileged manager admission is closed"))
    }

    fn validate_submission(request: &mut SubmitOperationRequest) -> Result<(), Status> {
        Namespace::parse(&request.node_id)
            .map_err(|_| Status::invalid_argument("node_id must be a valid stable identity"))?;
        if !Manager::valid_deployment_token(&request.request_token) {
            return Err(Status::invalid_argument(
                "request_token must be 1..=128 URL-safe characters",
            ));
        }
        let action = NodeOperationAction::try_from(request.action)
            .unwrap_or(NodeOperationAction::Unspecified);
        if action == NodeOperationAction::Unspecified {
            return Err(Status::invalid_argument("operation action is required"));
        }
        request.engine_slots.sort();
        let permits_empty_inventory = action == NodeOperationAction::Scale;
        if (!permits_empty_inventory && request.engine_slots.is_empty())
            || request
                .engine_slots
                .iter()
                .any(|slot| !Manager::valid_deployment_token(slot))
            || request
                .engine_slots
                .windows(2)
                .any(|pair| pair[0] == pair[1])
        {
            return Err(Status::invalid_argument(
                "engine_slots must contain unique URL-safe stable slot identities",
            ));
        }
        let deployment_action = matches!(
            action,
            NodeOperationAction::InitialApply
                | NodeOperationAction::RollingUpgrade
                | NodeOperationAction::Scale
                | NodeOperationAction::Rollback
        );
        if deployment_action
            && (request.target_revision == 0
                || !Manager::valid_bundle_digest(&request.bundle_digest)
                || !Manager::valid_bundle_digest(&request.resolved_release_digest))
        {
            return Err(Status::invalid_argument(
                "deployment operations require target_revision, source bundle digest, and resolved release digest",
            ));
        }
        let policy = request
            .policy
            .get_or_insert_with(|| wr_common::wruntime::RolloutPolicy {
                max_unavailable: 1,
                canary_slot: request.engine_slots.first().cloned().unwrap_or_default(),
                pause_after_canary: false,
                allow_downtime: false,
                deadline_seconds: match action {
                    NodeOperationAction::Drain => 120,
                    NodeOperationAction::Restart => 300,
                    _ => 1800,
                },
            });
        if policy.max_unavailable == 0
            || (!request.engine_slots.is_empty()
                && policy.max_unavailable as usize > request.engine_slots.len())
            || policy.deadline_seconds == 0
        {
            return Err(Status::invalid_argument(
                "policy requires a positive bounded max_unavailable and deadline_seconds",
            ));
        }
        if !policy.canary_slot.is_empty() && !request.engine_slots.contains(&policy.canary_slot) {
            return Err(Status::invalid_argument(
                "canary_slot is not in engine_slots",
            ));
        }
        if action == NodeOperationAction::Scale
            && request.engine_slots.is_empty()
            && !policy.allow_downtime
        {
            return Err(Status::failed_precondition(
                "scale-to-zero requires explicit allow_downtime",
            ));
        }
        Ok(())
    }
}

impl OperatorApi {
    async fn begin_deployment(
        &self,
        mut request: Request<BeginDeploymentRequest>,
    ) -> Result<Response<BeginDeploymentResponse>, Status> {
        let _admission = self.require_admission()?;
        let principal = self.policy.authorize_operator(&mut request)?;
        let mut request = request.into_inner();
        Manager::validate_deployment_request(&request)?;
        Manager::canonicalize_deployment_request(&mut request)?;
        let deployment = db::begin_deployment(&self.pool, &request, &principal.name)
            .await?
            .record;
        Ok(Response::new(BeginDeploymentResponse {
            deployment: Some(deployment),
        }))
    }

    async fn verify_deployment(
        &self,
        mut request: Request<VerifyDeploymentRequest>,
    ) -> Result<Response<VerifyDeploymentResponse>, Status> {
        let _admission = self.require_admission()?;
        self.policy.authorize_read(&mut request)?;
        let request = request.into_inner();
        if request.node_id.is_empty() || request.revision == 0 {
            return Err(Status::invalid_argument(
                "node_id and non-zero revision are required",
            ));
        }
        let deployment = db::get_deployment(&self.pool, &request.node_id, request.revision)
            .await?
            .record;
        let conditions = db::deployment_conditions(
            &self.pool,
            &deployment,
            self.engine_heartbeat_timeout_secs,
            self.module_heartbeat_timeout_secs,
        )
        .await?
        .into_iter()
        .map(|(code, detail)| deployment_condition(code, detail))
        .collect::<Vec<_>>();
        Ok(Response::new(VerifyDeploymentResponse {
            deployment: Some(deployment),
            ready: conditions.is_empty(),
            conditions,
        }))
    }

    async fn begin_rollback(
        &self,
        mut request: Request<BeginRollbackRequest>,
    ) -> Result<Response<BeginRollbackResponse>, Status> {
        let _admission = self.require_admission()?;
        let principal = self.policy.authorize_operator(&mut request)?;
        let request = request.into_inner();
        Namespace::parse(&request.node_id)
            .map_err(|_| Status::invalid_argument("node_id must be a valid stable identity"))?;
        if !Manager::valid_deployment_token(&request.attempt_token) {
            return Err(Status::invalid_argument(
                "attempt_token must be 1..=128 URL-safe characters",
            ));
        }
        let deployment = db::begin_rollback(
            &self.pool,
            &request.node_id,
            request.to_revision,
            &request.attempt_token,
            &principal.name,
        )
        .await?
        .record;
        Ok(Response::new(BeginRollbackResponse {
            deployment: Some(deployment),
        }))
    }

    async fn get_status(
        &self,
        mut request: Request<GetOperatorStatusRequest>,
    ) -> Result<Response<GetOperatorStatusResponse>, Status> {
        let _admission = self.require_admission()?;
        let requested_node =
            (!request.get_ref().node_id.is_empty()).then(|| request.get_ref().node_id.clone());
        let principal = self
            .policy
            .authorize_infrastructure_read(&mut request, requested_node.as_deref())?;
        let filter = request.into_inner();
        let snapshot = db::get_cluster_status_snapshot(&self.pool).await?;
        let mut active_operations = snapshot.active_operations.clone();
        let mut observations = snapshot.observations.clone();
        let mut slot_authorities = snapshot
            .slot_authorities
            .iter()
            .map(|authority| SlotAuthorityStatus {
                node_id: authority.node_id.clone(),
                engine_slot: authority.engine_slot.clone(),
                revision: authority.revision,
                bundle_digest: authority.bundle_digest.clone(),
                resolved_release_digest: authority.resolved_release_digest.clone(),
            })
            .collect::<Vec<_>>();
        let mut agent_attestations = snapshot.agent_attestations.clone();
        let mut cluster = crate::status::compose(
            snapshot,
            self.manager_liveness_threshold_secs,
            self.engine_heartbeat_timeout_secs,
            self.module_heartbeat_timeout_secs,
        )?;
        cluster.nodes.retain(|node| {
            (filter.node_id.is_empty() || node.node_id == filter.node_id)
                && self
                    .policy
                    .allows_infrastructure_read(&principal, &node.node_id)
        });
        cluster.engines.retain(|engine| {
            engine.deployment.as_ref().is_some_and(|deployment| {
                (filter.node_id.is_empty() || deployment.node_id == filter.node_id)
                    && (filter.engine_slot.is_empty()
                        || deployment.engine_slot == filter.engine_slot)
                    && self
                        .policy
                        .allows_infrastructure_read(&principal, &deployment.node_id)
            })
        });
        if !filter.node_id.is_empty() && cluster.nodes.is_empty() {
            return Err(Status::not_found("selected node or slot was not found"));
        }
        if !filter.engine_slot.is_empty()
            && !cluster.engines.iter().any(|engine| {
                engine.deployment.as_ref().is_some_and(|deployment| {
                    deployment.node_id == filter.node_id
                        && deployment.engine_slot == filter.engine_slot
                })
            })
        {
            return Err(Status::not_found("selected node or slot was not found"));
        }
        active_operations.retain(|operation| {
            (filter.node_id.is_empty() || operation.node_id == filter.node_id)
                && self
                    .policy
                    .allows_infrastructure_read(&principal, &operation.node_id)
        });
        observations.retain(|observation| {
            (filter.node_id.is_empty() || observation.node_id == filter.node_id)
                && (filter.engine_slot.is_empty() || observation.engine_slot == filter.engine_slot)
                && self
                    .policy
                    .allows_infrastructure_read(&principal, &observation.node_id)
        });
        slot_authorities.retain(|authority| {
            (filter.node_id.is_empty() || authority.node_id == filter.node_id)
                && (filter.engine_slot.is_empty() || authority.engine_slot == filter.engine_slot)
                && self
                    .policy
                    .allows_infrastructure_read(&principal, &authority.node_id)
        });
        agent_attestations.retain(|attestation| {
            (filter.node_id.is_empty() || attestation.node_id == filter.node_id)
                && self
                    .policy
                    .allows_infrastructure_read(&principal, &attestation.node_id)
        });
        Ok(Response::new(GetOperatorStatusResponse {
            cluster: Some(cluster),
            active_operations,
            observations,
            slot_authorities,
            agent_attestations,
        }))
    }

    async fn submit_operation(
        &self,
        mut request: Request<SubmitOperationRequest>,
    ) -> Result<Response<SubmitOperationResponse>, Status> {
        let _admission = self.require_admission()?;
        let node_id = request.get_ref().node_id.clone();
        let principal = self
            .policy
            .authorize_infrastructure_write(&mut request, &node_id)?;
        let mut request = request.into_inner();
        Self::validate_submission(&mut request)?;
        let operation = crate::operations::submit(&self.pool, &principal.name, &request).await?;
        Ok(Response::new(SubmitOperationResponse {
            operation: Some(operation),
        }))
    }

    async fn get_operation(
        &self,
        mut request: Request<GetOperationRequest>,
    ) -> Result<Response<GetOperationResponse>, Status> {
        let _admission = self.require_admission()?;
        let operation_id = request.get_ref().operation_id.clone();
        let operation = crate::operations::get(&self.pool, &operation_id).await?;
        self.policy
            .authorize_infrastructure_read(&mut request, Some(&operation.node_id))?;
        let events = crate::operations::events(&self.pool, &operation_id).await?;
        Ok(Response::new(GetOperationResponse {
            operation: Some(operation),
            events,
        }))
    }

    async fn list_operations(
        &self,
        mut request: Request<ListOperationsRequest>,
    ) -> Result<Response<ListOperationsResponse>, Status> {
        let _admission = self.require_admission()?;
        let requested_node =
            (!request.get_ref().node_id.is_empty()).then(|| request.get_ref().node_id.clone());
        let principal = self
            .policy
            .authorize_infrastructure_read(&mut request, requested_node.as_deref())?;
        let request = request.into_inner();
        let mut operations =
            crate::operations::list(&self.pool, &request.node_id, request.include_terminal).await?;
        operations.retain(|operation| {
            self.policy
                .allows_infrastructure_read(&principal, &operation.node_id)
        });
        Ok(Response::new(ListOperationsResponse { operations }))
    }

    async fn resume_operation(
        &self,
        mut request: Request<ResumeOperationRequest>,
    ) -> Result<Response<ResumeOperationResponse>, Status> {
        let _admission = self.require_admission()?;
        let operation_id = request.get_ref().operation_id.clone();
        let existing = crate::operations::get(&self.pool, &operation_id).await?;
        let principal = self
            .policy
            .authorize_infrastructure_write(&mut request, &existing.node_id)?;
        let operation =
            crate::operations::resume(&self.pool, &operation_id, &principal.name).await?;
        Ok(Response::new(ResumeOperationResponse {
            operation: Some(operation),
        }))
    }

    async fn cancel_operation(
        &self,
        mut request: Request<CancelOperationRequest>,
    ) -> Result<Response<CancelOperationResponse>, Status> {
        let _admission = self.require_admission()?;
        let operation_id = request.get_ref().operation_id.clone();
        let existing = crate::operations::get(&self.pool, &operation_id).await?;
        let principal = self
            .policy
            .authorize_infrastructure_write(&mut request, &existing.node_id)?;
        let operation =
            crate::operations::cancel(&self.pool, &operation_id, &principal.name).await?;
        Ok(Response::new(CancelOperationResponse {
            operation: Some(operation),
        }))
    }

    async fn put_node_agent_policy(
        &self,
        mut request: Request<PutNodeAgentPolicyRequest>,
    ) -> Result<Response<PutNodeAgentPolicyResponse>, Status> {
        let _admission = self.require_admission()?;
        let principal = self.policy.authorize_operator(&mut request)?;
        let policy = request
            .into_inner()
            .policy
            .ok_or_else(|| Status::invalid_argument("policy is required"))?;
        let policy =
            crate::operations::put_agent_policy(&self.pool, &principal.name, &policy).await?;
        Ok(Response::new(PutNodeAgentPolicyResponse {
            policy: Some(policy),
        }))
    }

    async fn get_node_cleanup_status(
        &self,
        mut request: Request<GetNodeCleanupStatusRequest>,
    ) -> Result<Response<GetNodeCleanupStatusResponse>, Status> {
        let _admission = self.require_admission()?;
        let node_id = request.get_ref().node_id.clone();
        self.policy
            .authorize_infrastructure_read(&mut request, Some(&node_id))?;
        let cleanup = crate::operations::get_node_cleanup_status(&self.pool, &node_id).await?;
        Ok(Response::new(GetNodeCleanupStatusResponse {
            cleanup: Some(cleanup),
        }))
    }

    async fn retry_node_cleanup(
        &self,
        mut request: Request<RetryNodeCleanupRequest>,
    ) -> Result<Response<RetryNodeCleanupResponse>, Status> {
        let _admission = self.require_admission()?;
        let node_id = request.get_ref().node_id.clone();
        self.policy
            .authorize_infrastructure_write(&mut request, &node_id)?;
        let request = request.into_inner();
        let cleanup = crate::operations::retry_node_cleanup(
            &self.pool,
            &node_id,
            request.observed_generation,
        )
        .await?;
        Ok(Response::new(RetryNodeCleanupResponse {
            cleanup: Some(cleanup),
        }))
    }

    async fn finalize_deployment(
        &self,
        mut request: Request<FinalizeDeploymentRequest>,
    ) -> Result<Response<FinalizeDeploymentResponse>, Status> {
        let _admission = self.require_admission()?;
        let principal = self.policy.authorize_operator(&mut request)?;
        let request = request.into_inner();
        Namespace::parse(&request.node_id)
            .map_err(|_| Status::invalid_argument("node_id must be a valid stable identity"))?;
        if !Manager::valid_deployment_token(&request.attempt_token)
            || request.revision == 0
            || !Manager::valid_bundle_digest(&request.bundle_digest)
            || !Manager::valid_bundle_digest(&request.resolved_release_digest)
        {
            return Err(Status::invalid_argument(
                "finalization requires token, revision, source digest, and resolved digest",
            ));
        }
        let deployment = db::finalize_deployment(&self.pool, &request, &principal.name)
            .await?
            .record;
        Ok(Response::new(FinalizeDeploymentResponse {
            deployment: Some(deployment),
        }))
    }

    async fn abandon_deployment(
        &self,
        mut request: Request<AbandonDeploymentRequest>,
    ) -> Result<Response<AbandonDeploymentResponse>, Status> {
        let _admission = self.require_admission()?;
        let principal = self.policy.authorize_operator(&mut request)?;
        let request = request.into_inner();
        Namespace::parse(&request.node_id)
            .map_err(|_| Status::invalid_argument("node_id must be a valid stable identity"))?;
        if !Manager::valid_deployment_token(&request.attempt_token) {
            return Err(Status::invalid_argument(
                "attempt_token must be 1..=128 URL-safe characters",
            ));
        }
        let deployment = db::abandon_deployment(
            &self.pool,
            &request.node_id,
            &request.attempt_token,
            &principal.name,
        )
        .await?
        .record;
        Ok(Response::new(AbandonDeploymentResponse {
            abandoned: true,
            deployment: Some(deployment),
        }))
    }
}

/// Pull-based node executor protocol. Certificate mapping fixes one agent to
/// one node before any lease or observation is accepted.
#[derive(Clone)]
pub struct NodeAgentApi {
    pool: Pool,
    policy: PrincipalPolicy,
    admission: AdmissionGate,
}

impl NodeAgentApi {
    pub fn new(pool: Pool, policy: PrincipalPolicy) -> Self {
        let admission = AdmissionGate::closed();
        admission.open();
        Self::with_admission(pool, policy, admission)
    }

    pub fn with_admission(pool: Pool, policy: PrincipalPolicy, admission: AdmissionGate) -> Self {
        Self {
            pool,
            policy,
            admission,
        }
    }

    fn require_admission(&self) -> Result<AdmissionGuard, Status> {
        self.admission
            .try_enter()
            .ok_or_else(|| Status::unavailable("privileged manager admission is closed"))
    }
}

impl NodeAgentApi {
    async fn attest(
        &self,
        mut request: Request<AttestNodeAgentRequest>,
    ) -> Result<Response<AttestNodeAgentResponse>, Status> {
        let _admission = self.require_admission()?;
        let node_id = request
            .get_ref()
            .attestation
            .as_ref()
            .map(|value| value.node_id.clone())
            .ok_or_else(|| Status::invalid_argument("attestation is required"))?;
        let principal = self.policy.authorize_agent(&mut request, &node_id)?;
        let attestation = request.into_inner().attestation.expect("validated above");
        let conditions =
            crate::operations::attest(&self.pool, &principal.name, &attestation).await?;
        Ok(Response::new(AttestNodeAgentResponse {
            accepted: conditions.is_empty(),
            conditions,
        }))
    }

    async fn claim_operation(
        &self,
        mut request: Request<ClaimOperationRequest>,
    ) -> Result<Response<ClaimOperationResponse>, Status> {
        let _admission = self.require_admission()?;
        let node_id = request.get_ref().node_id.clone();
        let agent_instance_id = request.get_ref().agent_instance_id.clone();
        let principal = self.policy.authorize_agent(&mut request, &node_id)?;
        let response =
            crate::operations::claim(&self.pool, &node_id, &agent_instance_id, &principal.name)
                .await?
                .unwrap_or(ClaimOperationResponse {
                    instruction: None,
                    lease_seconds: 0,
                });
        Ok(Response::new(response))
    }

    async fn claim_node_cleanup(
        &self,
        mut request: Request<ClaimNodeCleanupRequest>,
    ) -> Result<Response<ClaimNodeCleanupResponse>, Status> {
        let _admission = self.require_admission()?;
        let node_id = request.get_ref().node_id.clone();
        let agent_instance_id = request.get_ref().agent_instance_id.clone();
        let principal = self.policy.authorize_agent(&mut request, &node_id)?;
        let response = crate::operations::claim_node_cleanup(
            &self.pool,
            &node_id,
            &agent_instance_id,
            &principal.name,
        )
        .await?
        .unwrap_or(ClaimNodeCleanupResponse {
            instruction: None,
            lease_seconds: 0,
        });
        Ok(Response::new(response))
    }

    async fn renew_node_cleanup_lease(
        &self,
        mut request: Request<RenewNodeCleanupLeaseRequest>,
    ) -> Result<Response<RenewNodeCleanupLeaseResponse>, Status> {
        let _admission = self.require_admission()?;
        let node_id = request.get_ref().node_id.clone();
        let principal = self.policy.authorize_agent(&mut request, &node_id)?;
        let response =
            crate::operations::renew_node_cleanup(&self.pool, request.get_ref(), &principal.name)
                .await?;
        Ok(Response::new(response))
    }

    async fn report_node_cleanup_result(
        &self,
        mut request: Request<ReportNodeCleanupResultRequest>,
    ) -> Result<Response<ReportNodeCleanupResultResponse>, Status> {
        let _admission = self.require_admission()?;
        let node_id = request.get_ref().node_id.clone();
        let principal = self.policy.authorize_agent(&mut request, &node_id)?;
        let response = crate::operations::report_node_cleanup_result(
            &self.pool,
            request.get_ref(),
            &principal.name,
        )
        .await?;
        Ok(Response::new(response))
    }

    async fn renew_operation_lease(
        &self,
        mut request: Request<RenewOperationLeaseRequest>,
    ) -> Result<Response<RenewOperationLeaseResponse>, Status> {
        let _admission = self.require_admission()?;
        let node_id = request.get_ref().node_id.clone();
        let principal = self.policy.authorize_agent(&mut request, &node_id)?;
        let request = request.into_inner();
        let lease_expires_at = crate::operations::renew(
            &self.pool,
            &request.node_id,
            &request.operation_id,
            request.lease_epoch,
            &request.agent_instance_id,
            &principal.name,
        )
        .await?;
        Ok(Response::new(RenewOperationLeaseResponse {
            lease_expires_at: Some(lease_expires_at),
        }))
    }

    async fn report_observation(
        &self,
        mut request: Request<ReportNodeObservationRequest>,
    ) -> Result<Response<ReportNodeObservationResponse>, Status> {
        let _admission = self.require_admission()?;
        let node_id = request.get_ref().node_id.clone();
        let principal = self.policy.authorize_agent(&mut request, &node_id)?;
        let operation =
            crate::operations::report_observation(&self.pool, request.get_ref(), &principal.name)
                .await?;
        Ok(Response::new(ReportNodeObservationResponse {
            operation: Some(operation),
        }))
    }

    async fn report_step_result(
        &self,
        mut request: Request<ReportStepResultRequest>,
    ) -> Result<Response<ReportStepResultResponse>, Status> {
        let _admission = self.require_admission()?;
        let node_id = request.get_ref().node_id.clone();
        let principal = self.policy.authorize_agent(&mut request, &node_id)?;
        let operation =
            crate::operations::report_step(&self.pool, request.get_ref(), &principal.name).await?;
        Ok(Response::new(ReportStepResultResponse {
            operation: Some(operation),
        }))
    }
}

#[derive(Clone)]
pub struct AuthorizedClusterService {
    inner: Manager,
    authorizer: Arc<crate::auth::ManagerAuthorizer>,
}

impl AuthorizedClusterService {
    pub fn new(inner: Manager, authorizer: Arc<crate::auth::ManagerAuthorizer>) -> Self {
        Self { inner, authorizer }
    }

    fn authorize<T>(
        &self,
        request: &mut Request<T>,
        method: &'static str,
        resource: &crate::auth::AuthorizationResource<'_>,
    ) -> Result<(), Status> {
        self.authorizer
            .authorize(request, "wruntime.ClusterService", method, resource)?
            .verify("wruntime.ClusterService", method)
    }
}

#[tonic::async_trait]
impl ClusterService for AuthorizedClusterService {
    async fn list_engines(
        &self,
        mut request: Request<ListEnginesRequest>,
    ) -> Result<Response<ListEnginesResponse>, Status> {
        let call = self.authorizer.authorize(
            &mut request,
            "wruntime.ClusterService",
            "ListEngines",
            &Default::default(),
        )?;
        call.verify("wruntime.ClusterService", "ListEngines")?;
        let mut response = self.inner.list_engines(request).await?;
        response.get_mut().engines.retain(|engine| {
            let node = engine
                .deployment
                .as_ref()
                .map(|deployment| deployment.node_id.as_str());
            engine.modules.iter().all(|module| {
                call.allows_collection_item(Some(&module.namespace), node, None, None)
            })
        });
        Ok(response)
    }
    async fn get_routing_table(
        &self,
        mut request: Request<GetRoutingTableRequest>,
    ) -> Result<Response<GetRoutingTableResponse>, Status> {
        let call = self.authorizer.authorize(
            &mut request,
            "wruntime.ClusterService",
            "GetRoutingTable",
            &Default::default(),
        )?;
        call.verify("wruntime.ClusterService", "GetRoutingTable")?;
        let mut response = self.inner.get_routing_table(request).await?;
        if let Some(table) = response.get_mut().table.as_mut() {
            table.rules.retain(|rule| {
                call.allows_collection_item(Some(&rule.destination_namespace), None, None, None)
            });
        }
        Ok(response)
    }
    async fn upsert_routing_rule(
        &self,
        mut request: Request<RoutingRule>,
    ) -> Result<Response<UpsertRoutingRuleResponse>, Status> {
        let namespace = request.get_ref().destination_namespace.clone();
        self.authorize(
            &mut request,
            "UpsertRoutingRule",
            &crate::auth::AuthorizationResource {
                namespace_id: Some(&namespace),
                ..Default::default()
            },
        )?;
        self.inner.upsert_routing_rule(request).await
    }
    async fn delete_routing_rule(
        &self,
        mut request: Request<DeleteRoutingRuleRequest>,
    ) -> Result<Response<DeleteRoutingRuleResponse>, Status> {
        let call = self.authorizer.authorize(
            &mut request,
            "wruntime.ClusterService",
            "DeleteRoutingRule",
            &Default::default(),
        )?;
        call.verify("wruntime.ClusterService", "DeleteRoutingRule")?;
        let rule_id = request.get_ref().rule_id.clone();
        let namespace = db::resolve_routing_rule_namespace(&self.inner.pool, &rule_id).await?;
        if let Some(namespace) = namespace {
            if !call.allows_collection_item(Some(&namespace), None, None, None) {
                return Err(Status::permission_denied(
                    "role scope does not cover the routing rule",
                ));
            }
            if db::delete_routing_rule_scoped(&self.inner.pool, &rule_id, &namespace).await? {
                info!(rule_id, "routing rule deleted");
            }
        }
        Ok(Response::new(DeleteRoutingRuleResponse {}))
    }
    async fn list_managers(
        &self,
        mut request: Request<ListManagersRequest>,
    ) -> Result<Response<ListManagersResponse>, Status> {
        let call = self.authorizer.authorize(
            &mut request,
            "wruntime.ClusterService",
            "ListManagers",
            &Default::default(),
        )?;
        call.verify("wruntime.ClusterService", "ListManagers")?;
        let mut response = self.inner.list_managers(request).await?;
        response.get_mut().managers.retain(|manager| {
            call.allows_collection_item(None, None, None, Some(&manager.manager_id))
        });
        Ok(response)
    }
    async fn get_cluster_status(
        &self,
        mut request: Request<GetClusterStatusRequest>,
    ) -> Result<Response<GetClusterStatusResponse>, Status> {
        self.authorize(&mut request, "GetClusterStatus", &Default::default())?;
        self.inner.get_cluster_status(request).await
    }
    async fn get_schema(
        &self,
        mut request: Request<GetSchemaRequest>,
    ) -> Result<Response<GetSchemaResponse>, Status> {
        let namespace = request.get_ref().namespace.clone();
        self.authorize(
            &mut request,
            "GetSchema",
            &crate::auth::AuthorizationResource {
                namespace_id: Some(&namespace),
                ..Default::default()
            },
        )?;
        self.inner.get_schema(request).await
    }
    async fn set_secret(
        &self,
        mut request: Request<SetSecretRequest>,
    ) -> Result<Response<SetSecretResponse>, Status> {
        let namespace = request.get_ref().namespace.clone();
        self.authorize(
            &mut request,
            "SetSecret",
            &crate::auth::AuthorizationResource {
                namespace_id: Some(&namespace),
                ..Default::default()
            },
        )?;
        self.inner.set_secret(request).await
    }
    async fn delete_secret(
        &self,
        mut request: Request<DeleteSecretRequest>,
    ) -> Result<Response<DeleteSecretResponse>, Status> {
        let namespace = request.get_ref().namespace.clone();
        self.authorize(
            &mut request,
            "DeleteSecret",
            &crate::auth::AuthorizationResource {
                namespace_id: Some(&namespace),
                ..Default::default()
            },
        )?;
        self.inner.delete_secret(request).await
    }
    async fn list_secrets(
        &self,
        mut request: Request<ListSecretsRequest>,
    ) -> Result<Response<ListSecretsResponse>, Status> {
        let namespace = request.get_ref().namespace.clone();
        let resource = crate::auth::AuthorizationResource {
            namespace_id: (!namespace.is_empty()).then_some(namespace.as_str()),
            ..Default::default()
        };
        let call = self.authorizer.authorize(
            &mut request,
            "wruntime.ClusterService",
            "ListSecrets",
            &resource,
        )?;
        call.verify("wruntime.ClusterService", "ListSecrets")?;
        let mut response = self.inner.list_secrets(request).await?;
        response.get_mut().secrets.retain(|secret| {
            call.allows_collection_item(Some(&secret.namespace), None, None, None)
        });
        Ok(response)
    }
    async fn upsert_schedule(
        &self,
        mut request: Request<UpsertScheduleRequest>,
    ) -> Result<Response<UpsertScheduleResponse>, Status> {
        let namespace = request.get_ref().worker_namespace.clone();
        self.authorize(
            &mut request,
            "UpsertSchedule",
            &crate::auth::AuthorizationResource {
                namespace_id: Some(&namespace),
                ..Default::default()
            },
        )?;
        self.inner.upsert_schedule(request).await
    }
    async fn delete_schedule(
        &self,
        mut request: Request<DeleteScheduleRequest>,
    ) -> Result<Response<DeleteScheduleResponse>, Status> {
        let namespace = request.get_ref().worker_namespace.clone();
        self.authorize(
            &mut request,
            "DeleteSchedule",
            &crate::auth::AuthorizationResource {
                namespace_id: Some(&namespace),
                ..Default::default()
            },
        )?;
        self.inner.delete_schedule(request).await
    }
    async fn list_schedules(
        &self,
        mut request: Request<ListSchedulesRequest>,
    ) -> Result<Response<ListSchedulesResponse>, Status> {
        let namespace = request.get_ref().worker_namespace.clone();
        let resource = crate::auth::AuthorizationResource {
            namespace_id: (!namespace.is_empty()).then_some(namespace.as_str()),
            ..Default::default()
        };
        let call = self.authorizer.authorize(
            &mut request,
            "wruntime.ClusterService",
            "ListSchedules",
            &resource,
        )?;
        call.verify("wruntime.ClusterService", "ListSchedules")?;
        let mut response = self.inner.list_schedules(request).await?;
        response.get_mut().schedules.retain(|schedule| {
            call.allows_collection_item(Some(&schedule.worker_namespace), None, None, None)
        });
        Ok(response)
    }
}

#[derive(Clone)]
pub struct InfrastructureApi {
    manager: Manager,
    operator: OperatorApi,
}

impl InfrastructureApi {
    pub fn new(manager: Manager, operator: OperatorApi) -> Self {
        Self { manager, operator }
    }

    fn canonical_rollout_request(
        request: &mut BeginManagerRolloutRequest,
    ) -> Result<String, Status> {
        use prost::Message;
        use sha2::{Digest, Sha256};
        if !Manager::valid_deployment_token(&request.client_operation_id) {
            return Err(Status::invalid_argument(
                "client_operation_id must satisfy the bounded operation-token grammar",
            ));
        }
        uuid::Uuid::parse_str(&request.client_operation_id)
            .map_err(|_| Status::invalid_argument("client_operation_id must be a UUID"))?;
        wr_common::identity::ClusterId::parse(&request.cluster_id)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if request.target_generation == 0
            || !Manager::valid_bundle_digest(&request.target_policy_digest)
        {
            return Err(Status::invalid_argument(
                "target generation and sha256 policy digest are required",
            ));
        }
        if request.target_policy_validator_version
            != wr_common::authorization_policy::AUTHORIZATION_POLICY_VALIDATOR_VERSION
        {
            return Err(Status::invalid_argument(
                "target policy validator version 1 is required",
            ));
        }
        wr_common::identity::PrincipalUri::parse(&request.target_deployment_principal_uri)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if !Manager::valid_bundle_digest(&request.target_deployment_leaf_fingerprint) {
            return Err(Status::invalid_argument(
                "target deployment leaf fingerprint must be sha256:<lowercase hex>",
            ));
        }
        if request.expected_targets.is_empty() {
            return Err(Status::invalid_argument(
                "expected target manager set must not be empty",
            ));
        }
        request
            .source_managers
            .sort_by(|left, right| left.manager_id.cmp(&right.manager_id));
        let mut source_managers = HashSet::new();
        for source in &request.source_managers {
            wr_common::identity::ManagerId::parse(&source.manager_id)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            if !source_managers.insert(source.manager_id.as_str())
                || PeerHttpsUrl::parse(&source.endpoint).is_err()
                || !Manager::valid_bundle_digest(&source.host_digest)
                || !Manager::valid_bundle_digest(&source.selector_digest)
            {
                return Err(Status::invalid_argument(
                    "source managers require unique IDs and canonical endpoint/host/selector digests",
                ));
            }
        }
        if !Manager::valid_bundle_digest(&request.manifest_digest)
            || uuid::Uuid::parse_str(&request.executor_id).is_err()
            || request.deployment_certificate.is_empty()
            || request.deployment_certificate.len() > 128
            || request.deployment_certificate.contains('/')
        {
            return Err(Status::invalid_argument(
                "manifest digest, executor UUID, and deployment certificate name are required",
            ));
        }
        request
            .expected_targets
            .sort_by(|left, right| left.manager_id.cmp(&right.manager_id));
        let mut managers = HashSet::new();
        for target in &request.expected_targets {
            wr_common::identity::ManagerId::parse(&target.manager_id)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            if !managers.insert(target.manager_id.as_str()) {
                return Err(Status::invalid_argument(
                    "expected target manager IDs must be unique",
                ));
            }
            if PeerHttpsUrl::parse(&target.endpoint).is_err()
                || !Manager::valid_bundle_digest(&target.host_digest)
                || !Manager::valid_bundle_digest(&target.config_digest)
                || !Manager::valid_bundle_digest(&target.executable_digest)
                || !Manager::valid_bundle_digest(&target.backend_spec_digest)
                || !Manager::valid_bundle_digest(&target.credential_digest)
                || !Manager::valid_bundle_digest(&target.old_selector_digest)
                || !Manager::valid_bundle_digest(&target.new_selector_digest)
                || !matches!(target.backend.as_str(), "systemd" | "compose")
            {
                return Err(Status::invalid_argument(
                    "target endpoint, backend, and canonical artifact/selector digests are required",
                ));
            }
        }
        if !request.recovery_of.is_empty() {
            uuid::Uuid::parse_str(&request.recovery_of)
                .map_err(|_| Status::invalid_argument("recovery_of must be a rollout UUID"))?;
        }
        Ok(format!(
            "sha256:{:x}",
            Sha256::digest(request.encode_to_vec())
        ))
    }

    #[cfg(test)]
    fn reject_revoked_rollout_leaf(
        policy: &PrincipalPolicy,
        evidence: &wr_common::tls::LeafEvidence,
    ) -> Result<(), Status> {
        if policy
            .snapshot()
            .revoked_leaf_fingerprints
            .contains(&evidence.fingerprint)
        {
            return Err(Status::permission_denied(
                "rollout caller certificate leaf is revoked",
            ));
        }
        Ok(())
    }

    fn authorize_rollout_controller<T>(
        &self,
        request: &Request<T>,
        rollout: &wr_common::wruntime::ManagerRollout,
    ) -> Result<(), Status> {
        let principal = request
            .extensions()
            .get::<crate::auth::AuthorizedPrincipal>()
            .ok_or_else(|| Status::permission_denied("authorized caller context is missing"))?;
        if principal.name != rollout.deployment_principal_uri
            || principal.fingerprint != rollout.target_deployment_leaf_fingerprint
        {
            return Err(Status::permission_denied(
                "rollout control requires its recorded deployment identity",
            ));
        }
        Ok(())
    }

    async fn begin_manager_rollout_inner(
        &self,
        request: Request<BeginManagerRolloutRequest>,
    ) -> Result<Response<BeginManagerRolloutResponse>, Status> {
        let principal = request
            .extensions()
            .get::<crate::auth::AuthorizedPrincipal>()
            .cloned()
            .ok_or_else(|| Status::permission_denied("authorized caller context is missing"))?;
        if !matches!(
            principal.kind,
            wr_common::identity::PrincipalKind::Human
                | wr_common::identity::PrincipalKind::ServiceAccount
        ) {
            return Err(Status::permission_denied(
                "manager rollout creation requires a deployment principal",
            ));
        }
        let mut request = request.into_inner();
        if request.target_deployment_principal_uri != principal.name
            || request.target_deployment_leaf_fingerprint != principal.fingerprint
        {
            return Err(Status::permission_denied(
                "target prevalidation claims do not match the authenticated leaf",
            ));
        }
        let manager_ids = request
            .expected_targets
            .iter()
            .map(|target| target.manager_id.clone())
            .collect::<Vec<_>>();
        if self.manager.admission.is_open() {
            if !self
                .operator
                .policy
                .snapshot()
                .authorizes_rollout(&principal.name, &manager_ids)
            {
                return Err(Status::permission_denied(
                    "source policy does not authorize the complete rollout target set",
                ));
            }
        } else {
            let targets = request
                .expected_targets
                .iter()
                .map(|target| wr_common::authorization_policy::RolloutTarget {
                    manager_id: target.manager_id.clone(),
                    endpoint: target.endpoint.clone(),
                })
                .collect::<Vec<_>>();
            let receipt = self
                .operator
                .policy
                .snapshot()
                .prevalidate_rollout_claims(&principal.name, &principal.fingerprint, &targets)
                .map_err(|error| {
                    Status::permission_denied(format!(
                        "closed-startup target validation failed: {error}"
                    ))
                })?;
            if receipt.generation != request.target_generation
                || receipt.digest != request.target_policy_digest
                || receipt.cluster_id != request.cluster_id
                || receipt.validator_version != request.target_policy_validator_version
            {
                return Err(Status::failed_precondition(
                    "closed-startup rollout does not match the loaded target policy",
                ));
            }
        }
        let digest = Self::canonical_rollout_request(&mut request)?;
        let rollout = db::begin_manager_rollout(
            &self.manager.pool,
            &principal.name,
            &principal.fingerprint,
            &request,
            &digest,
            self.manager.admission.is_open(),
        )
        .await?;
        Ok(Response::new(BeginManagerRolloutResponse {
            rollout: Some(rollout),
        }))
    }
}

impl InfrastructureApi {
    async fn get_status(
        &self,
        request: Request<GetOperatorStatusRequest>,
    ) -> Result<Response<GetOperatorStatusResponse>, Status> {
        self.operator.get_status(request).await
    }
    async fn submit_operation(
        &self,
        request: Request<SubmitOperationRequest>,
    ) -> Result<Response<SubmitOperationResponse>, Status> {
        self.operator.submit_operation(request).await
    }
    async fn get_operation(
        &self,
        request: Request<GetOperationRequest>,
    ) -> Result<Response<GetOperationResponse>, Status> {
        self.operator.get_operation(request).await
    }
    async fn list_operations(
        &self,
        request: Request<ListOperationsRequest>,
    ) -> Result<Response<ListOperationsResponse>, Status> {
        self.operator.list_operations(request).await
    }
    async fn resume_operation(
        &self,
        request: Request<ResumeOperationRequest>,
    ) -> Result<Response<ResumeOperationResponse>, Status> {
        self.operator.resume_operation(request).await
    }
    async fn cancel_operation(
        &self,
        request: Request<CancelOperationRequest>,
    ) -> Result<Response<CancelOperationResponse>, Status> {
        self.operator.cancel_operation(request).await
    }
    async fn begin_deployment(
        &self,
        request: Request<BeginDeploymentRequest>,
    ) -> Result<Response<BeginDeploymentResponse>, Status> {
        self.operator.begin_deployment(request).await
    }
    async fn verify_deployment(
        &self,
        request: Request<VerifyDeploymentRequest>,
    ) -> Result<Response<VerifyDeploymentResponse>, Status> {
        self.operator.verify_deployment(request).await
    }
    async fn finalize_deployment(
        &self,
        request: Request<FinalizeDeploymentRequest>,
    ) -> Result<Response<FinalizeDeploymentResponse>, Status> {
        self.operator.finalize_deployment(request).await
    }
    async fn abandon_deployment(
        &self,
        request: Request<AbandonDeploymentRequest>,
    ) -> Result<Response<AbandonDeploymentResponse>, Status> {
        self.operator.abandon_deployment(request).await
    }
    async fn begin_rollback(
        &self,
        request: Request<BeginRollbackRequest>,
    ) -> Result<Response<BeginRollbackResponse>, Status> {
        self.operator.begin_rollback(request).await
    }
    async fn put_node_agent_policy(
        &self,
        request: Request<PutNodeAgentPolicyRequest>,
    ) -> Result<Response<PutNodeAgentPolicyResponse>, Status> {
        self.operator.put_node_agent_policy(request).await
    }
    async fn get_node_cleanup_status(
        &self,
        request: Request<GetNodeCleanupStatusRequest>,
    ) -> Result<Response<GetNodeCleanupStatusResponse>, Status> {
        self.operator.get_node_cleanup_status(request).await
    }
    async fn retry_node_cleanup(
        &self,
        request: Request<RetryNodeCleanupRequest>,
    ) -> Result<Response<RetryNodeCleanupResponse>, Status> {
        self.operator.retry_node_cleanup(request).await
    }
    async fn begin_manager_rollout(
        &self,
        request: Request<BeginManagerRolloutRequest>,
    ) -> Result<Response<BeginManagerRolloutResponse>, Status> {
        self.begin_manager_rollout_inner(request).await
    }
    async fn lease_manager_rollout(
        &self,
        request: Request<LeaseManagerRolloutRequest>,
    ) -> Result<Response<LeaseManagerRolloutResponse>, Status> {
        let current =
            db::get_manager_rollout(&self.manager.pool, &request.get_ref().rollout_id).await?;
        self.authorize_rollout_controller(&request, &current)?;
        let request = request.into_inner();
        let rollout = db::lease_manager_rollout(
            &self.manager.pool,
            &request.rollout_id,
            &request.executor_id,
            request.expected_lease_epoch,
        )
        .await?;
        Ok(Response::new(LeaseManagerRolloutResponse {
            rollout: Some(rollout),
        }))
    }
    async fn advance_manager_rollout(
        &self,
        request: Request<AdvanceManagerRolloutRequest>,
    ) -> Result<Response<AdvanceManagerRolloutResponse>, Status> {
        let current =
            db::get_manager_rollout(&self.manager.pool, &request.get_ref().rollout_id).await?;
        self.authorize_rollout_controller(&request, &current)?;
        let request = request.into_inner();
        let rollout = db::advance_manager_rollout(
            &self.manager.pool,
            &request.rollout_id,
            &request.executor_id,
            request.lease_epoch,
            request.expected_phase,
            request.next_phase,
            &request.member_outcomes,
        )
        .await?;
        Ok(Response::new(AdvanceManagerRolloutResponse {
            rollout: Some(rollout),
        }))
    }
    async fn get_manager_rollout(
        &self,
        request: Request<GetManagerRolloutRequest>,
    ) -> Result<Response<GetManagerRolloutResponse>, Status> {
        let rollout =
            db::get_manager_rollout(&self.manager.pool, &request.get_ref().rollout_id).await?;
        let call = request
            .extensions()
            .get::<crate::auth::AuthorizedCall>()
            .ok_or_else(|| Status::permission_denied("authorized caller context is missing"))?;
        if !rollout
            .expected_targets
            .iter()
            .all(|target| call.allows_collection_item(None, None, None, Some(&target.manager_id)))
        {
            return Err(Status::permission_denied(
                "role scope does not cover the rollout manager set",
            ));
        }
        if !self.manager.admission.is_open() {
            self.authorize_rollout_controller(&request, &rollout)?;
        }
        Ok(Response::new(GetManagerRolloutResponse {
            rollout: Some(rollout),
        }))
    }
}

#[derive(Clone)]
pub struct AuthorizedInfrastructureService {
    inner: InfrastructureApi,
    authorizer: Arc<crate::auth::ManagerAuthorizer>,
}

impl AuthorizedInfrastructureService {
    pub fn new(inner: InfrastructureApi, authorizer: Arc<crate::auth::ManagerAuthorizer>) -> Self {
        Self { inner, authorizer }
    }
    fn authorize<T>(&self, request: &mut Request<T>, method: &'static str) -> Result<(), Status> {
        self.authorize_node(request, method, None)
    }

    fn authorize_node<T>(
        &self,
        request: &mut Request<T>,
        method: &'static str,
        node_id: Option<&str>,
    ) -> Result<(), Status> {
        self.authorizer
            .authorize(
                request,
                "wruntime.InfrastructureService",
                method,
                &crate::auth::AuthorizationResource {
                    node_id,
                    ..Default::default()
                },
            )?
            .verify("wruntime.InfrastructureService", method)
    }
}

#[tonic::async_trait]
impl InfrastructureService for AuthorizedInfrastructureService {
    async fn get_status(
        &self,
        mut r: Request<GetOperatorStatusRequest>,
    ) -> Result<Response<GetOperatorStatusResponse>, Status> {
        let node = r.get_ref().node_id.clone();
        self.authorize_node(
            &mut r,
            "GetStatus",
            (!node.is_empty()).then_some(node.as_str()),
        )?;
        self.inner.get_status(r).await
    }
    async fn submit_operation(
        &self,
        mut r: Request<SubmitOperationRequest>,
    ) -> Result<Response<SubmitOperationResponse>, Status> {
        let node = r.get_ref().node_id.clone();
        self.authorize_node(&mut r, "SubmitOperation", Some(&node))?;
        self.inner.submit_operation(r).await
    }
    async fn get_operation(
        &self,
        mut r: Request<GetOperationRequest>,
    ) -> Result<Response<GetOperationResponse>, Status> {
        self.authorize(&mut r, "GetOperation")?;
        self.inner.get_operation(r).await
    }
    async fn list_operations(
        &self,
        mut r: Request<ListOperationsRequest>,
    ) -> Result<Response<ListOperationsResponse>, Status> {
        let node = r.get_ref().node_id.clone();
        let call = self.authorizer.authorize(
            &mut r,
            "wruntime.InfrastructureService",
            "ListOperations",
            &crate::auth::AuthorizationResource {
                node_id: (!node.is_empty()).then_some(node.as_str()),
                ..Default::default()
            },
        )?;
        call.verify("wruntime.InfrastructureService", "ListOperations")?;
        let mut response = self.inner.list_operations(r).await?;
        response.get_mut().operations.retain(|operation| {
            call.allows_collection_item(None, Some(&operation.node_id), None, None)
        });
        Ok(response)
    }
    async fn resume_operation(
        &self,
        mut r: Request<ResumeOperationRequest>,
    ) -> Result<Response<ResumeOperationResponse>, Status> {
        self.authorize(&mut r, "ResumeOperation")?;
        self.inner.resume_operation(r).await
    }
    async fn cancel_operation(
        &self,
        mut r: Request<CancelOperationRequest>,
    ) -> Result<Response<CancelOperationResponse>, Status> {
        self.authorize(&mut r, "CancelOperation")?;
        self.inner.cancel_operation(r).await
    }
    async fn begin_deployment(
        &self,
        mut r: Request<BeginDeploymentRequest>,
    ) -> Result<Response<BeginDeploymentResponse>, Status> {
        let node = r.get_ref().node_id.clone();
        self.authorize_node(&mut r, "BeginDeployment", Some(&node))?;
        self.inner.begin_deployment(r).await
    }
    async fn verify_deployment(
        &self,
        mut r: Request<VerifyDeploymentRequest>,
    ) -> Result<Response<VerifyDeploymentResponse>, Status> {
        let node = r.get_ref().node_id.clone();
        self.authorize_node(&mut r, "VerifyDeployment", Some(&node))?;
        self.inner.verify_deployment(r).await
    }
    async fn finalize_deployment(
        &self,
        mut r: Request<FinalizeDeploymentRequest>,
    ) -> Result<Response<FinalizeDeploymentResponse>, Status> {
        let node = r.get_ref().node_id.clone();
        self.authorize_node(&mut r, "FinalizeDeployment", Some(&node))?;
        self.inner.finalize_deployment(r).await
    }
    async fn abandon_deployment(
        &self,
        mut r: Request<AbandonDeploymentRequest>,
    ) -> Result<Response<AbandonDeploymentResponse>, Status> {
        let node = r.get_ref().node_id.clone();
        self.authorize_node(&mut r, "AbandonDeployment", Some(&node))?;
        self.inner.abandon_deployment(r).await
    }
    async fn begin_rollback(
        &self,
        mut r: Request<BeginRollbackRequest>,
    ) -> Result<Response<BeginRollbackResponse>, Status> {
        let node = r.get_ref().node_id.clone();
        self.authorize_node(&mut r, "BeginRollback", Some(&node))?;
        self.inner.begin_rollback(r).await
    }
    async fn put_node_agent_policy(
        &self,
        mut r: Request<PutNodeAgentPolicyRequest>,
    ) -> Result<Response<PutNodeAgentPolicyResponse>, Status> {
        let node = r
            .get_ref()
            .policy
            .as_ref()
            .map(|policy| policy.node_id.clone())
            .ok_or_else(|| Status::invalid_argument("policy is required"))?;
        self.authorize_node(&mut r, "PutNodeAgentPolicy", Some(&node))?;
        self.inner.put_node_agent_policy(r).await
    }
    async fn get_node_cleanup_status(
        &self,
        mut r: Request<GetNodeCleanupStatusRequest>,
    ) -> Result<Response<GetNodeCleanupStatusResponse>, Status> {
        let node = r.get_ref().node_id.clone();
        self.authorize_node(&mut r, "GetNodeCleanupStatus", Some(&node))?;
        self.inner.get_node_cleanup_status(r).await
    }
    async fn retry_node_cleanup(
        &self,
        mut r: Request<RetryNodeCleanupRequest>,
    ) -> Result<Response<RetryNodeCleanupResponse>, Status> {
        let node = r.get_ref().node_id.clone();
        self.authorize_node(&mut r, "RetryNodeCleanup", Some(&node))?;
        self.inner.retry_node_cleanup(r).await
    }
    async fn begin_manager_rollout(
        &self,
        mut r: Request<BeginManagerRolloutRequest>,
    ) -> Result<Response<BeginManagerRolloutResponse>, Status> {
        let manager_ids = r
            .get_ref()
            .expected_targets
            .iter()
            .map(|target| target.manager_id.clone())
            .collect::<Vec<_>>();
        self.authorizer
            .authorize(
                &mut r,
                "wruntime.InfrastructureService",
                "BeginManagerRollout",
                &crate::auth::AuthorizationResource {
                    manager_ids: &manager_ids,
                    ..Default::default()
                },
            )?
            .verify("wruntime.InfrastructureService", "BeginManagerRollout")?;
        self.inner.begin_manager_rollout(r).await
    }
    async fn lease_manager_rollout(
        &self,
        mut r: Request<LeaseManagerRolloutRequest>,
    ) -> Result<Response<LeaseManagerRolloutResponse>, Status> {
        self.authorize(&mut r, "LeaseManagerRollout")?;
        self.inner.lease_manager_rollout(r).await
    }
    async fn advance_manager_rollout(
        &self,
        mut r: Request<AdvanceManagerRolloutRequest>,
    ) -> Result<Response<AdvanceManagerRolloutResponse>, Status> {
        self.authorize(&mut r, "AdvanceManagerRollout")?;
        self.inner.advance_manager_rollout(r).await
    }
    async fn get_manager_rollout(
        &self,
        mut r: Request<GetManagerRolloutRequest>,
    ) -> Result<Response<GetManagerRolloutResponse>, Status> {
        self.authorize(&mut r, "GetManagerRollout")?;
        self.inner.get_manager_rollout(r).await
    }
}

#[derive(Clone)]
pub struct AuthenticatedLifecycleApi {
    inner: wr_common::lifecycle_service::LifecycleServiceAdapter,
    policy: PrincipalPolicy,
}

impl AuthenticatedLifecycleApi {
    pub fn new(
        inner: wr_common::lifecycle_service::LifecycleServiceAdapter,
        policy: PrincipalPolicy,
    ) -> Self {
        Self { inner, policy }
    }
}

impl AuthenticatedLifecycleApi {
    async fn get_status(
        &self,
        mut request: Request<GetLifecycleStatusRequest>,
    ) -> Result<Response<GetLifecycleStatusResponse>, Status> {
        self.policy.authorize_enrolled(&mut request)?;
        self.inner.get_status(request).await
    }
}

#[derive(Clone)]
pub struct AuthorizedLifecycleService {
    inner: AuthenticatedLifecycleApi,
    authorizer: Arc<crate::auth::ManagerAuthorizer>,
}
impl AuthorizedLifecycleService {
    pub fn new(
        inner: AuthenticatedLifecycleApi,
        authorizer: Arc<crate::auth::ManagerAuthorizer>,
    ) -> Self {
        Self { inner, authorizer }
    }
}
#[tonic::async_trait]
impl LifecycleService for AuthorizedLifecycleService {
    async fn get_status(
        &self,
        mut request: Request<GetLifecycleStatusRequest>,
    ) -> Result<Response<GetLifecycleStatusResponse>, Status> {
        self.authorizer
            .authorize(
                &mut request,
                "wruntime.LifecycleService",
                "GetStatus",
                &Default::default(),
            )?
            .verify("wruntime.LifecycleService", "GetStatus")?;
        self.inner.get_status(request).await
    }
}

#[derive(Clone)]
pub struct PolicyApi {
    policy: PrincipalPolicy,
    admission: AdmissionGate,
    lifecycle: ManagerLifecycleState,
}

impl PolicyApi {
    pub fn new(
        policy: PrincipalPolicy,
        admission: AdmissionGate,
        lifecycle: ManagerLifecycleState,
    ) -> Self {
        Self {
            policy,
            admission,
            lifecycle,
        }
    }

    fn require_admission(&self) -> Result<AdmissionGuard, Status> {
        self.admission
            .try_enter()
            .ok_or_else(|| Status::unavailable("privileged manager admission is closed"))
    }
}

impl PolicyApi {
    async fn get_policy_status(
        &self,
        mut request: Request<GetPolicyStatusRequest>,
    ) -> Result<Response<GetPolicyStatusResponse>, Status> {
        let _admission = self.require_admission()?;
        self.policy.authorize_enrolled(&mut request)?;
        let snapshot = self.policy.snapshot();
        let headroom = &snapshot.cap_headroom;
        Ok(Response::new(GetPolicyStatusResponse {
            generation: snapshot.generation,
            digest: snapshot.digest.clone(),
            admission: self.lifecycle.admission().as_str_name().into(),
            schema_version: snapshot.schema_version,
            validator_version:
                wr_common::authorization_policy::AUTHORIZATION_POLICY_VALIDATOR_VERSION,
            cap_headroom: Some(PolicyCapHeadroom {
                raw_bytes: headroom.raw_bytes as u64,
                canonical_bytes: headroom.canonical_bytes as u64,
                principals: headroom.principals as u64,
                assignments: headroom.assignments as u64,
                scope_values: headroom.scope_values as u64,
                managers: headroom.managers as u64,
                proxies: headroom.proxies as u64,
                node_agents: headroom.node_agents as u64,
                revoked_fingerprints: headroom.revoked_fingerprints as u64,
            }),
        }))
    }

    async fn get_workload_snapshot(
        &self,
        mut request: Request<GetWorkloadSnapshotRequest>,
    ) -> Result<Response<GetWorkloadSnapshotResponse>, Status> {
        let _admission = self.require_admission()?;
        self.policy.authorize_proxy(&mut request)?;
        let snapshot = self.policy.snapshot();
        Ok(Response::new(GetWorkloadSnapshotResponse {
            generation: snapshot.generation,
            digest: snapshot.digest.clone(),
            serialized_snapshot: wr_common::snapshot_consumer::build_snapshot(
                snapshot,
                wr_common::wruntime::WorkloadProjectionKind::ProxyPeerV1,
                std::time::SystemTime::now(),
            )
            .map_err(|error| Status::internal(error.to_string()))?,
        }))
    }
}

#[derive(Clone)]
pub struct AuthorizedPolicyService {
    inner: PolicyApi,
    authorizer: Arc<crate::auth::ManagerAuthorizer>,
}
impl AuthorizedPolicyService {
    pub fn new(inner: PolicyApi, authorizer: Arc<crate::auth::ManagerAuthorizer>) -> Self {
        Self { inner, authorizer }
    }
}
#[tonic::async_trait]
impl PolicyService for AuthorizedPolicyService {
    async fn get_policy_status(
        &self,
        mut request: Request<GetPolicyStatusRequest>,
    ) -> Result<Response<GetPolicyStatusResponse>, Status> {
        self.authorizer
            .authorize(
                &mut request,
                "wruntime.PolicyService",
                "GetPolicyStatus",
                &Default::default(),
            )?
            .verify("wruntime.PolicyService", "GetPolicyStatus")?;
        self.inner.get_policy_status(request).await
    }
    async fn get_workload_snapshot(
        &self,
        mut request: Request<GetWorkloadSnapshotRequest>,
    ) -> Result<Response<GetWorkloadSnapshotResponse>, Status> {
        self.authorizer
            .authorize(
                &mut request,
                "wruntime.PolicyService",
                "GetWorkloadSnapshot",
                &Default::default(),
            )?
            .verify("wruntime.PolicyService", "GetWorkloadSnapshot")?;
        self.inner.get_workload_snapshot(request).await
    }
}

#[derive(Clone)]
pub struct ManagerNodeApi {
    manager: Manager,
    agent: NodeAgentApi,
}

impl ManagerNodeApi {
    pub fn new(manager: Manager, agent: NodeAgentApi) -> Self {
        Self { manager, agent }
    }
}

impl ManagerNodeApi {
    async fn register_engine(
        &self,
        request: Request<RegisterEngineRequest>,
    ) -> Result<Response<RegisterEngineResponse>, Status> {
        self.manager.register_engine(request).await
    }
    async fn deregister_engine(
        &self,
        request: Request<DeregisterEngineRequest>,
    ) -> Result<Response<DeregisterEngineResponse>, Status> {
        self.manager.deregister_engine(request).await
    }
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        self.manager.heartbeat(request).await
    }
    async fn begin_engine_drain(
        &self,
        request: Request<BeginEngineDrainRequest>,
    ) -> Result<Response<BeginEngineDrainResponse>, Status> {
        self.manager.begin_engine_drain(request).await
    }
    async fn attest(
        &self,
        request: Request<AttestNodeAgentRequest>,
    ) -> Result<Response<AttestNodeAgentResponse>, Status> {
        self.agent.attest(request).await
    }
    async fn claim_operation(
        &self,
        request: Request<ClaimOperationRequest>,
    ) -> Result<Response<ClaimOperationResponse>, Status> {
        self.agent.claim_operation(request).await
    }
    async fn claim_node_cleanup(
        &self,
        request: Request<ClaimNodeCleanupRequest>,
    ) -> Result<Response<ClaimNodeCleanupResponse>, Status> {
        self.agent.claim_node_cleanup(request).await
    }
    async fn renew_node_cleanup_lease(
        &self,
        request: Request<RenewNodeCleanupLeaseRequest>,
    ) -> Result<Response<RenewNodeCleanupLeaseResponse>, Status> {
        self.agent.renew_node_cleanup_lease(request).await
    }
    async fn report_node_cleanup_result(
        &self,
        request: Request<ReportNodeCleanupResultRequest>,
    ) -> Result<Response<ReportNodeCleanupResultResponse>, Status> {
        self.agent.report_node_cleanup_result(request).await
    }
    async fn renew_operation_lease(
        &self,
        request: Request<RenewOperationLeaseRequest>,
    ) -> Result<Response<RenewOperationLeaseResponse>, Status> {
        self.agent.renew_operation_lease(request).await
    }
    async fn report_observation(
        &self,
        request: Request<ReportNodeObservationRequest>,
    ) -> Result<Response<ReportNodeObservationResponse>, Status> {
        self.agent.report_observation(request).await
    }
    async fn report_step_result(
        &self,
        request: Request<ReportStepResultRequest>,
    ) -> Result<Response<ReportStepResultResponse>, Status> {
        self.agent.report_step_result(request).await
    }
}

#[derive(Clone)]
pub struct AuthorizedNodeService {
    inner: ManagerNodeApi,
    authorizer: Arc<crate::auth::ManagerAuthorizer>,
}
impl AuthorizedNodeService {
    pub fn new(inner: ManagerNodeApi, authorizer: Arc<crate::auth::ManagerAuthorizer>) -> Self {
        Self { inner, authorizer }
    }
    fn authorize<T>(
        &self,
        request: &mut Request<T>,
        method: &'static str,
        node_id: Option<&str>,
    ) -> Result<(), Status> {
        self.authorizer
            .authorize(
                request,
                "wruntime.NodeService",
                method,
                &crate::auth::AuthorizationResource {
                    node_id,
                    ..Default::default()
                },
            )?
            .verify("wruntime.NodeService", method)
    }

    async fn authorize_existing_engine<T>(
        &self,
        request: &mut Request<T>,
        method: &'static str,
        engine_id: &str,
    ) -> Result<(), Status> {
        let call = self.authorizer.authorize(
            request,
            "wruntime.NodeService",
            method,
            &Default::default(),
        )?;
        call.verify("wruntime.NodeService", method)?;
        if let Some(node_id) = db::resolve_engine_node(&self.inner.manager.pool, engine_id).await? {
            if call.principal_node_id() != Some(node_id.as_str()) {
                return Err(Status::permission_denied(
                    "enrolled proxy does not own the engine deployment",
                ));
            }
        }
        Ok(())
    }
}
#[tonic::async_trait]
impl NodeService for AuthorizedNodeService {
    async fn register_engine(
        &self,
        mut r: Request<RegisterEngineRequest>,
    ) -> Result<Response<RegisterEngineResponse>, Status> {
        let node = r
            .get_ref()
            .registration
            .as_ref()
            .and_then(|x| x.deployment.as_ref())
            .map(|x| x.node_id.clone());
        self.authorize(&mut r, "RegisterEngine", node.as_deref())?;
        self.inner.register_engine(r).await
    }
    async fn deregister_engine(
        &self,
        mut r: Request<DeregisterEngineRequest>,
    ) -> Result<Response<DeregisterEngineResponse>, Status> {
        let engine_id = r.get_ref().engine_id.clone();
        self.authorize_existing_engine(&mut r, "DeregisterEngine", &engine_id)
            .await?;
        self.inner.deregister_engine(r).await
    }
    async fn heartbeat(
        &self,
        mut r: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let engine_id = r.get_ref().engine_id.clone();
        self.authorize_existing_engine(&mut r, "Heartbeat", &engine_id)
            .await?;
        self.inner.heartbeat(r).await
    }
    async fn begin_engine_drain(
        &self,
        mut r: Request<BeginEngineDrainRequest>,
    ) -> Result<Response<BeginEngineDrainResponse>, Status> {
        let engine_id = r.get_ref().engine_id.clone();
        self.authorize_existing_engine(&mut r, "BeginEngineDrain", &engine_id)
            .await?;
        self.inner.begin_engine_drain(r).await
    }
    async fn attest(
        &self,
        mut r: Request<AttestNodeAgentRequest>,
    ) -> Result<Response<AttestNodeAgentResponse>, Status> {
        let node = r
            .get_ref()
            .attestation
            .as_ref()
            .map(|attestation| attestation.node_id.clone())
            .ok_or_else(|| Status::invalid_argument("attestation is required"))?;
        self.authorize(&mut r, "Attest", Some(&node))?;
        self.inner.attest(r).await
    }
    async fn claim_operation(
        &self,
        mut r: Request<ClaimOperationRequest>,
    ) -> Result<Response<ClaimOperationResponse>, Status> {
        let node = r.get_ref().node_id.clone();
        self.authorize(&mut r, "ClaimOperation", Some(&node))?;
        self.inner.claim_operation(r).await
    }
    async fn claim_node_cleanup(
        &self,
        mut r: Request<ClaimNodeCleanupRequest>,
    ) -> Result<Response<ClaimNodeCleanupResponse>, Status> {
        let node = r.get_ref().node_id.clone();
        self.authorize(&mut r, "ClaimNodeCleanup", Some(&node))?;
        self.inner.claim_node_cleanup(r).await
    }
    async fn renew_node_cleanup_lease(
        &self,
        mut r: Request<RenewNodeCleanupLeaseRequest>,
    ) -> Result<Response<RenewNodeCleanupLeaseResponse>, Status> {
        let node = r.get_ref().node_id.clone();
        self.authorize(&mut r, "RenewNodeCleanupLease", Some(&node))?;
        self.inner.renew_node_cleanup_lease(r).await
    }
    async fn report_node_cleanup_result(
        &self,
        mut r: Request<ReportNodeCleanupResultRequest>,
    ) -> Result<Response<ReportNodeCleanupResultResponse>, Status> {
        let node = r.get_ref().node_id.clone();
        self.authorize(&mut r, "ReportNodeCleanupResult", Some(&node))?;
        self.inner.report_node_cleanup_result(r).await
    }
    async fn renew_operation_lease(
        &self,
        mut r: Request<RenewOperationLeaseRequest>,
    ) -> Result<Response<RenewOperationLeaseResponse>, Status> {
        let node = r.get_ref().node_id.clone();
        self.authorize(&mut r, "RenewOperationLease", Some(&node))?;
        self.inner.renew_operation_lease(r).await
    }
    async fn report_observation(
        &self,
        mut r: Request<ReportNodeObservationRequest>,
    ) -> Result<Response<ReportNodeObservationResponse>, Status> {
        let node = r.get_ref().node_id.clone();
        self.authorize(&mut r, "ReportObservation", Some(&node))?;
        self.inner.report_observation(r).await
    }
    async fn report_step_result(
        &self,
        mut r: Request<ReportStepResultRequest>,
    ) -> Result<Response<ReportStepResultResponse>, Status> {
        let node = r.get_ref().node_id.clone();
        self.authorize(&mut r, "ReportStepResult", Some(&node))?;
        self.inner.report_step_result(r).await
    }
}

#[cfg(test)]
mod reconcile_tests {
    use super::*;

    #[test]
    fn schedule_timestamp_preserves_seconds_and_nanos() {
        let value = chrono::DateTime::from_timestamp(1_700_000_000, 123_456_789).unwrap();
        let timestamp = proto_timestamp(value);
        assert_eq!(timestamp.seconds, 1_700_000_000);
        assert_eq!(timestamp.nanos, 123_456_789);
    }

    #[test]
    fn rollout_creation_and_controller_gate_reject_revoked_leaf() {
        let policy = PrincipalPolicy::new(Arc::new(
            wr_common::authorization_policy::ValidatedPolicy::load(
                br#"schema_version=1
generation=1
cluster_id="cluster-a"
revoked_leaf_fingerprints=["sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
principals=[{uri="urn:wruntime:cluster-a:human:deployer",kind="human"}]
assignments=[{principal="urn:wruntime:cluster-a:human:deployer",role="admin",scope={}}]
manager_enrollments=[]
proxy_enrollments=[]
node_agent_enrollments=[]
"#,
            )
            .unwrap(),
        ));
        let evidence = wr_common::tls::LeafEvidence {
            profile: wr_common::tls::LeafProfile::Client,
            principal: Some(
                wr_common::identity::PrincipalUri::parse("urn:wruntime:cluster-a:human:deployer")
                    .unwrap(),
            ),
            endpoint_dns_names: Vec::new(),
            endpoint_ip_addresses: Vec::new(),
            fingerprint: format!("sha256:{}", "a".repeat(64)),
            serial: "01".into(),
            spki_fingerprint: format!("sha256:{}", "b".repeat(64)),
        };
        assert_eq!(
            InfrastructureApi::reject_revoked_rollout_leaf(&policy, &evidence)
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
    }

    #[test]
    fn manager_rollout_canonicalization_sorts_targets_and_binds_content() {
        let target = |manager_id: &str, digit: char| wr_common::wruntime::ManagerRolloutTarget {
            manager_id: manager_id.into(),
            endpoint: format!("https://{manager_id}:9000"),
            host_digest: format!("sha256:{}", digit.to_string().repeat(64)),
            config_digest: format!("sha256:{}", digit.to_string().repeat(64)),
            backend: "systemd".into(),
            executable_digest: format!("sha256:{}", digit.to_string().repeat(64)),
            backend_spec_digest: format!("sha256:{}", digit.to_string().repeat(64)),
            credential_digest: format!("sha256:{}", digit.to_string().repeat(64)),
            old_selector_digest: format!("sha256:{}", digit.to_string().repeat(64)),
            new_selector_digest: format!("sha256:{}", digit.to_string().repeat(64)),
        };
        let mut first = BeginManagerRolloutRequest {
            client_operation_id: "73c54ac7-fe23-4ddf-96e5-0da2e4b18d7d".into(),
            cluster_id: "cluster-a".into(),
            target_generation: 2,
            target_policy_digest: format!("sha256:{}", "a".repeat(64)),
            expected_targets: vec![target("manager-b", 'b'), target("manager-a", 'a')],
            recovery_of: String::new(),
            target_policy_validator_version: 1,
            target_deployment_principal_uri: "urn:wruntime:cluster-a:human:deployer".into(),
            target_deployment_leaf_fingerprint: format!("sha256:{}", "c".repeat(64)),
            source_managers: vec![],
            manifest_digest: format!("sha256:{}", "d".repeat(64)),
            executor_id: "9ba18521-717d-4c39-bf79-e4bb87f95c55".into(),
            deployment_certificate: "deploy-set-v1".into(),
        };
        first.source_managers = vec![wr_common::wruntime::ManagerRolloutSource {
            manager_id: "manager-source".into(),
            endpoint: "https://manager-source:9000".into(),
            host_digest: format!("sha256:{}", "e".repeat(64)),
            selector_digest: format!("sha256:{}", "f".repeat(64)),
        }];
        let mut reordered = first.clone();
        reordered.expected_targets.reverse();
        let first_digest = InfrastructureApi::canonical_rollout_request(&mut first).unwrap();
        let reordered_digest =
            InfrastructureApi::canonical_rollout_request(&mut reordered).unwrap();
        assert_eq!(first_digest, reordered_digest);
        reordered.expected_targets[0].credential_digest = format!("sha256:{}", "9".repeat(64));
        assert_ne!(
            first_digest,
            InfrastructureApi::canonical_rollout_request(&mut reordered).unwrap()
        );
        let mut changed_source = first.clone();
        changed_source.source_managers[0].selector_digest = format!("sha256:{}", "8".repeat(64));
        assert_ne!(
            first_digest,
            InfrastructureApi::canonical_rollout_request(&mut changed_source).unwrap()
        );
    }

    #[test]
    fn database_records_project_in_query_order() {
        let records = vec![
            db::ManagerRecord {
                manager_id: "manager-a".into(),
                grpc_address: "https://manager-a:9000".into(),
            },
            db::ManagerRecord {
                manager_id: "manager-b".into(),
                grpc_address: "https://manager-b:9000".into(),
            },
        ];

        let managers = reconcile_managers(&records);

        assert_eq!(managers.len(), 2);
        assert_eq!(managers[0].manager_id, "manager-a");
        assert_eq!(managers[1].grpc_address, "https://manager-b:9000");
    }
}
