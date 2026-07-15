use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use deadpool_postgres::Pool;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use wr_common::identity::{
    EngineHttpUrl, EngineId, ModuleId, Namespace, NamespaceFilter, PeerHttpsUrl, ProxyHttpUrl,
    RouteKey, RuleId,
};
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

use crate::cluster::{ClusterHandle, ManagerLiveness};
use crate::crypto::SecretCrypto;
use crate::db;

/// A genuine liveness discrepancy surfaced by reconciliation: a manager the DB
/// still considers fresh that chitchat has affirmatively marked dead.
fn proto_timestamp(value: chrono::DateTime<chrono::Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: value.timestamp(),
        nanos: value.timestamp_subsec_nanos() as i32,
    }
}

pub struct ReconcileWarning {
    pub manager_id: String,
}

/// Per-manager reconciliation of the DB-fresh set against chitchat, keyed on
/// `manager_id`. Pure (no I/O, no clock) so it is unit-testable without gossip
/// timing. `within_window` is the caller's `ClusterHandle::within_convergence_window()`.
pub fn reconcile_managers(
    db_records: &HashMap<String, db::ManagerRecord>,
    live: &HashMap<String, ManagerLiveness>,
    dead: &HashSet<String>,
    within_window: bool,
    self_id: &str,
) -> (Vec<ManagerInfo>, Vec<ReconcileWarning>) {
    let mut managers = Vec::new();
    let mut warnings = Vec::new();

    // Stable, deterministic order over the union of DB and gossip ids.
    let mut ids: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
    ids.extend(db_records.keys().map(String::as_str));
    ids.extend(live.keys().map(String::as_str));

    for id in ids {
        let db = db_records.get(id);

        // Chitchat affirmatively dead → exclude immediately, regardless of DB
        // freshness or cluster size. A DB-fresh row that gossip says is dead is
        // the ONLY genuine discrepancy worth a warning;
        // never warn about self.
        if dead.contains(id) {
            if db.is_some() && id != self_id {
                warnings.push(ReconcileWarning {
                    manager_id: id.to_string(),
                });
            }
            continue;
        }

        // Live in gossip → include, preferring gossip addresses and filling any
        // blank field from the DB record for this manager_id.
        if let Some(l) = live.get(id) {
            let grpc_address = if !l.grpc_address.is_empty() {
                l.grpc_address.clone()
            } else {
                db.map(|d| d.grpc_address.clone()).unwrap_or_default()
            };
            let gossip_address = if !l.gossip_address.is_empty() {
                l.gossip_address.clone()
            } else {
                db.map(|d| d.gossip_address.clone()).unwrap_or_default()
            };
            managers.push(ManagerInfo {
                manager_id: id.to_string(),
                grpc_address,
                gossip_address,
            });
            continue;
        }

        // DB-fresh but never observed in gossip (neither live nor dead):
        // include during the bootstrap window, else trust chitchat and drop
        // (no warn — ordinary post-window state).
        if let Some(d) = db {
            if within_window {
                managers.push(ManagerInfo {
                    manager_id: id.to_string(),
                    grpc_address: d.grpc_address.clone(),
                    gossip_address: d.gossip_address.clone(),
                });
            }
        }
    }

    (managers, warnings)
}

pub struct Manager {
    pool: Pool,
    crypto: Arc<SecretCrypto>,
    cluster: Arc<ClusterHandle>,
}

impl Manager {
    pub fn new(pool: Pool, crypto: Arc<SecretCrypto>, cluster: Arc<ClusterHandle>) -> Self {
        Self {
            pool,
            crypto,
            cluster,
        }
    }

    /// Ensure a DB password exists for the given namespace, creating one if not.
    /// Returns the plaintext password.
    async fn ensure_db_password(&self, namespace: &str) -> Result<String, Status> {
        let key = "__db_password";

        // Fast path: already stored — decrypt and return.
        let existing =
            db::get_secrets(&self.pool, &[(namespace.to_string(), key.to_string())]).await?;
        if let Some((_, _, ciphertext, nonce)) = existing.into_iter().next() {
            return self
                .crypto
                .decrypt(&ciphertext, &nonce)
                .map_err(|e| Status::internal(format!("failed to decrypt db password: {e}")));
        }

        // Miss: generate + encrypt a candidate, then insert only if absent.
        // Concurrent callers race here; ON CONFLICT DO NOTHING lets the DB pick
        // a single winning row.
        let candidate = SecretCrypto::generate_random_password();
        let (ciphertext, nonce) = self
            .crypto
            .encrypt(&candidate)
            .map_err(|e| Status::internal(format!("encryption failed: {e}")))?;
        db::insert_secret_if_absent(&self.pool, namespace, key, &ciphertext, &nonce).await?;

        // Re-read unconditionally and decrypt the STORED value, so a caller
        // whose insert lost the conflict still returns the persisted password.
        let stored =
            db::get_secrets(&self.pool, &[(namespace.to_string(), key.to_string())]).await?;
        let (_, _, ciphertext, nonce) = stored
            .into_iter()
            .next()
            .ok_or_else(|| Status::internal("db password missing immediately after insert"))?;
        self.crypto
            .decrypt(&ciphertext, &nonce)
            .map_err(|e| Status::internal(format!("failed to decrypt db password: {e}")))
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

        EngineId::parse(&reg.engine_id)
            .and_then(|_| EngineHttpUrl::parse(&reg.address))
            .and_then(|_| ProxyHttpUrl::parse(&reg.proxy_address))
            .and_then(|_| PeerHttpsUrl::parse(&reg.peer_address))
            .map_err(|error| Status::invalid_argument(error.to_string()))?;

        // Validate modules — proto_schema is only required on the first
        // descriptor for a given (namespace, name, version) tuple; additional
        // entries represent extra instances on the same engine.
        {
            let mut seen = std::collections::HashSet::new();
            for module in &reg.modules {
                ModuleId::parse(&module.namespace, &module.name, &module.version)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?;
                let first = seen.insert((&module.namespace, &module.name, &module.version));
                if first && module.proto_schema.is_empty() {
                    return Err(Status::invalid_argument(format!(
                        "module '{}' in namespace '{}' has no schema — proto_schema is required",
                        module.name, module.namespace
                    )));
                }
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

        let engine_id = reg.engine_id.clone();

        // Resolve requested secrets (fails before any write).
        let secrets = self.resolve_secrets(&reg.secrets).await?;

        // Resolve DB credentials for namespaces that need database access
        // (fails before any write).
        let db_credentials = self.resolve_db_credentials(&reg.db_namespaces).await?;

        // Persist engine, schemas, and initially-unhealthy default routing rules
        // atomically. Routes are published last, so a failure in either resolver
        // above leaves no engine, schema, or routing-rule rows.
        db::register_engine_and_routes(&self.pool, &reg).await?;

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
        let HeartbeatRequest {
            engine_id,
            healthy_modules,
        } = request.into_inner();

        // Bump engine liveness FIRST and unconditionally. Engine liveness keeps
        // every route on the engine healthy, so a single malformed module
        // descriptor must never starve it (which would flip ALL the engine's
        // routes unhealthy after the timeout).
        db::heartbeat_engine(&self.pool, &engine_id).await?;

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

        db::upsert_module_heartbeats(&self.pool, &engine_id, &valid).await?;

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
        let db_records: HashMap<String, db::ManagerRecord> = db::list_managers(&self.pool)
            .await?
            .into_iter()
            .map(|r| (r.manager_id.clone(), r))
            .collect();

        let live: HashMap<String, ManagerLiveness> = self
            .cluster
            .live_managers()
            .await
            .into_iter()
            .map(|m| (m.manager_id.clone(), m))
            .collect();
        let dead = self.cluster.dead_manager_ids().await;
        let within_window = self.cluster.within_convergence_window();
        let self_id = self.cluster.self_id();

        let (managers, warnings) =
            reconcile_managers(&db_records, &live, &dead, within_window, &self_id);

        for w in warnings {
            warn!(
                manager_id = %w.manager_id,
                "manager is DB-fresh but chitchat reports it dead; excluding from ListManagers",
            );
        }

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

#[cfg(test)]
mod reconcile_tests {
    use super::*;

    fn rec(id: &str, grpc: &str, gossip: &str) -> db::ManagerRecord {
        db::ManagerRecord {
            manager_id: id.to_string(),
            grpc_address: grpc.to_string(),
            gossip_address: gossip.to_string(),
        }
    }
    fn live(id: &str, grpc: &str, gossip: &str) -> ManagerLiveness {
        ManagerLiveness {
            manager_id: id.to_string(),
            grpc_address: grpc.to_string(),
            gossip_address: gossip.to_string(),
        }
    }
    fn map<T>(items: Vec<(&str, T)>) -> HashMap<String, T> {
        items.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
    }

    #[test]
    fn schedule_timestamp_preserves_seconds_and_nanos() {
        let value = chrono::DateTime::from_timestamp(1_700_000_000, 123_456_789).unwrap();
        let timestamp = proto_timestamp(value);
        assert_eq!(timestamp.seconds, 1_700_000_000);
        assert_eq!(timestamp.nanos, 123_456_789);
    }

    // 2-manager: peer is chitchat-dead while still DB-fresh (the N1 case).
    #[test]
    fn dead_peer_excluded_and_warned() {
        let db = map(vec![
            ("self", rec("self", "https://self:9000", "self:9010")),
            ("peer", rec("peer", "https://peer:9000", "peer:9010")),
        ]);
        let live = map(vec![(
            "self",
            live("self", "https://self:9000", "self:9010"),
        )]);
        let dead: HashSet<String> = ["peer".to_string()].into_iter().collect();

        let (managers, warnings) = reconcile_managers(&db, &live, &dead, true, "self");

        let ids: Vec<_> = managers.iter().map(|m| m.manager_id.as_str()).collect();
        assert_eq!(ids, vec!["self"]);
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].manager_id, "peer");
    }

    // Single manager (live = just self, no dead) → self included, no warn.
    #[test]
    fn single_manager_self_only_no_warn() {
        let db = map(vec![(
            "self",
            rec("self", "https://self:9000", "self:9010"),
        )]);
        let live = map(vec![(
            "self",
            live("self", "https://self:9000", "self:9010"),
        )]);
        let dead: HashSet<String> = HashSet::new();

        let (managers, warnings) = reconcile_managers(&db, &live, &dead, true, "self");

        assert_eq!(managers.len(), 1);
        assert_eq!(managers[0].manager_id, "self");
        assert_eq!(managers[0].gossip_address, "self:9010");
        assert!(warnings.is_empty());
    }

    // DB-fresh peer unknown to gossip, within window → included (bootstrap).
    #[test]
    fn db_fresh_unknown_within_window_included() {
        let db = map(vec![
            ("self", rec("self", "https://self:9000", "self:9010")),
            ("peer", rec("peer", "https://peer:9000", "peer:9010")),
        ]);
        let live = map(vec![(
            "self",
            live("self", "https://self:9000", "self:9010"),
        )]);
        let dead: HashSet<String> = HashSet::new();

        let (managers, warnings) = reconcile_managers(&db, &live, &dead, true, "self");

        let ids: Vec<_> = managers.iter().map(|m| m.manager_id.as_str()).collect();
        assert_eq!(ids, vec!["peer", "self"]); // BTreeSet order
        assert!(warnings.is_empty());
    }

    // DB-fresh peer unknown to gossip, window elapsed → excluded, no warn.
    #[test]
    fn db_fresh_unknown_after_window_excluded() {
        let db = map(vec![
            ("self", rec("self", "https://self:9000", "self:9010")),
            ("peer", rec("peer", "https://peer:9000", "peer:9010")),
        ]);
        let live = map(vec![(
            "self",
            live("self", "https://self:9000", "self:9010"),
        )]);
        let dead: HashSet<String> = HashSet::new();

        let (managers, warnings) = reconcile_managers(&db, &live, &dead, false, "self");

        let ids: Vec<_> = managers.iter().map(|m| m.manager_id.as_str()).collect();
        assert_eq!(ids, vec!["self"]);
        assert!(warnings.is_empty());
    }

    // Live peer missing grpc_address in gossip but present in DB → filled from DB.
    #[test]
    fn live_missing_grpc_filled_from_db() {
        let db = map(vec![(
            "peer",
            rec("peer", "https://peer:9000", "peer:9010"),
        )]);
        let live = map(vec![("peer", live("peer", "", ""))]);
        let dead: HashSet<String> = HashSet::new();

        let (managers, _warnings) = reconcile_managers(&db, &live, &dead, false, "self");

        assert_eq!(managers.len(), 1);
        assert_eq!(managers[0].grpc_address, "https://peer:9000");
        assert_eq!(managers[0].gossip_address, "peer:9010");
    }
}
