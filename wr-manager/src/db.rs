use deadpool_postgres::{GenericClient, Pool};
use prost::Message;
use tokio_retry::strategy::ExponentialBackoff;
use tokio_retry::RetryIf;
use tonic::Status;

use wr_common::deployment_contract::{canonicalize_inventory, revision_digest};
use wr_common::identity::NamespaceFilter;
use wr_common::lifecycle_service::{AdmissionGate, ManagerLifecycleState};
use wr_common::naming::namespace_role;
use wr_common::task_group::{TaskCancellation, TaskExit};
use wr_common::wruntime::{
    BeginDeploymentRequest, BeginManagerRolloutRequest, DeploymentInventoryV1, DeploymentRecord,
    DeploymentState, EngineOwnershipFence, EngineRegistration, ManagerRollout, ManagerRolloutPhase,
    ModuleDescriptor, NamespaceDbCredential, NamespaceSecrets, NodeAgentAttestation,
    NodeAgentPolicy, NodeOperation, PrivilegedAdmissionState, RoutingRule, RoutingTable,
    SlotObservation,
};

/// Exponential backoff strategy for NOWAIT lock retries: 10ms, 20ms, 40ms, 80ms.
fn lock_retry_strategy() -> impl Iterator<Item = std::time::Duration> {
    ExponentialBackoff::from_millis(10).take(4)
}

/// Returns true if the error is a NOWAIT lock contention (retryable).
fn is_lock_contention(e: &Status) -> bool {
    e.code() == tonic::Code::Aborted
}

// ── Private helpers ──────────────────────────────────────────────────────────

/// Acquire the global routing-table lock within an existing transaction.
/// Returns the current version. Uses NOWAIT so concurrent writers get an
/// immediate `Status::aborted` instead of blocking.
async fn acquire_global_lock(
    txn: &deadpool_postgres::Transaction<'_>,
    operation: &str,
) -> Result<i64, Status> {
    let row = txn
        .query_one(
            "SELECT version FROM wr_manager_lock WHERE id = 1 FOR UPDATE NOWAIT",
            &[],
        )
        .await
        .map_err(|e| map_lock_err(e, operation))?;
    Ok(row.get(0))
}

/// Increment the routing table version and return the new value.
async fn increment_version(txn: &deadpool_postgres::Transaction<'_>) -> Result<i64, Status> {
    let row = txn
        .query_one(
            "UPDATE wr_manager_lock SET version = version + 1 WHERE id = 1 RETURNING version",
            &[],
        )
        .await
        .map_err(|e| Status::internal(format!("failed to increment version: {e}")))?;
    Ok(row.get(0))
}

/// Map a Postgres error to a tonic Status.
/// `LOCK_NOT_AVAILABLE` (55P03) becomes `Status::aborted` so callers can retry.
fn map_lock_err(e: tokio_postgres::Error, operation: &str) -> Status {
    let code = e.code().map(|c| c.code()).unwrap_or_default();
    if code == "55P03" {
        Status::aborted(format!(
            "concurrent write conflict during {operation} — another routing table update is in progress, retry"
        ))
    } else {
        Status::internal(format!(
            "lock query failed during {operation}: {}",
            wr_common::pool::pg_error_string(&e)
        ))
    }
}

/// Acquire the global routing-table lock, waiting if another transaction holds it.
/// Used by the background monitor which can afford to block briefly.
async fn acquire_global_lock_wait(txn: &deadpool_postgres::Transaction<'_>) -> Result<i64, Status> {
    let row = txn
        .query_one(
            "SELECT version FROM wr_manager_lock WHERE id = 1 FOR UPDATE",
            &[],
        )
        .await
        .map_err(|e| Status::internal(format!("lock query failed: {e}")))?;
    Ok(row.get(0))
}

/// Extension trait to replace `.map_err(|e| Status::internal(e.to_string()))?`
/// with `.internal()?`.
trait IntoInternalStatus<T> {
    fn internal(self) -> Result<T, Status>;
}

impl<T, E: std::fmt::Display> IntoInternalStatus<T> for Result<T, E> {
    fn internal(self) -> Result<T, Status> {
        self.map_err(|e| Status::internal(e.to_string()))
    }
}

// ── Engine operations ────────────────────────────────────────────────────────

/// Register an engine, its module schemas, and one default routing rule per
/// unique schema-bearing module tuple — all in a single transaction under the
/// global routing lock. Default rules are created as initially unhealthy and
/// become routable only after heartbeat-driven health recomputation. Routes are
/// the last statements before commit, so any earlier failure rolls back the whole
/// registration (no partial routes).
fn assigned_slot_generation(current: u64, replay: bool) -> Result<u64, Status> {
    if replay {
        Ok(current)
    } else {
        current
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("engine slot generation exhausted"))
    }
}

pub struct RegistrationCommit {
    pub fence: EngineOwnershipFence,
    pub secrets: Vec<NamespaceSecrets>,
    pub db_credentials: Vec<NamespaceDbCredential>,
}

pub async fn register_engine_and_routes(
    pool: &Pool,
    crypto: &crate::crypto::SecretCrypto,
    reg: &EngineRegistration,
    activation_id: &str,
) -> Result<RegistrationCommit, Status> {
    let mut client = pool.get().await.internal()?;
    let txn = client.transaction().await.internal()?;

    acquire_global_lock_wait(&txn).await?;

    let metadata = reg
        .deployment
        .as_ref()
        .ok_or_else(|| Status::failed_precondition("managed deployment metadata is required"))?;
    let operation_id = uuid::Uuid::parse_str(&metadata.operation_id)
        .map_err(|_| Status::invalid_argument("deployment operation_id must be a UUID"))?;
    let activation_uuid = uuid::Uuid::parse_str(activation_id)
        .map_err(|_| Status::invalid_argument("activation_id must be a UUID"))?;
    txn.query_one(
        "SELECT current_revision, target_revision FROM wr_nodes WHERE node_id=$1 FOR UPDATE",
        &[&metadata.node_id],
    )
    .await
    .internal()?;
    let operation = txn
        .query_opt(
            "SELECT operation.target_revision, operation.bundle_digest,
                operation.target_revision_digest, operation.action, operation.phase,
                slot.source_revision AS slot_source_revision,
                slot.source_digest AS slot_source_digest,
                slot.target_revision AS slot_target_revision,
                slot.target_digest AS slot_target_digest
         FROM wr_node_operations operation
         JOIN wr_node_operation_slots slot
           ON slot.operation_id = operation.operation_id
          AND slot.engine_slot = $3
         WHERE operation.operation_id=$1 AND operation.node_id=$2
           AND operation.state IN ('queued','running','paused')
         FOR UPDATE",
            &[&operation_id, &metadata.node_id, &metadata.engine_slot],
        )
        .await
        .internal()?
        .ok_or_else(|| {
            Status::failed_precondition("deployment operation is not live for this node and slot")
        })?;
    let deployment = txn
        .query_opt(
            "SELECT deployment.expected_inventory, deployment.revision_digest, deployment.state,
                node.current_revision
         FROM wr_node_deployments deployment
         JOIN wr_nodes node ON node.node_id = deployment.node_id
         WHERE deployment.node_id=$1 AND deployment.revision=$2
           AND deployment.bundle_digest=$3
           AND deployment.state IN ('pending','active','succeeded')
         FOR UPDATE",
            &[
                &metadata.node_id,
                &i64::try_from(metadata.revision)
                    .map_err(|_| Status::invalid_argument("deployment revision is too large"))?,
                &metadata.bundle_digest,
            ],
        )
        .await
        .internal()?
        .ok_or_else(|| {
            Status::failed_precondition("deployment metadata does not match desired state")
        })?;
    let stored_digest: String = deployment.get("revision_digest");
    let deployment_state: String = deployment.get("state");
    let is_staged = matches!(deployment_state.as_str(), "pending" | "active");
    let is_committed = deployment_state == "succeeded"
        && deployment.get::<_, i64>("current_revision") == metadata.revision as i64;
    let operation_target_matches = operation.get::<_, i64>("target_revision")
        == metadata.revision as i64
        && operation.get::<_, String>("bundle_digest") == metadata.bundle_digest
        && operation
            .get::<_, Option<String>>("target_revision_digest")
            .as_deref()
            == Some(&stored_digest);
    let restart_target_matches = operation.get::<_, String>("action") == "restart"
        && operation.get::<_, i64>("slot_target_revision") == metadata.revision as i64
        && operation.get::<_, String>("slot_target_digest") == metadata.bundle_digest;
    let restoration_source_matches = operation.get::<_, String>("phase") == "restoring_source"
        && operation.get::<_, i64>("slot_source_revision") == metadata.revision as i64
        && operation.get::<_, String>("slot_source_digest") == metadata.bundle_digest;
    if !((operation_target_matches && is_staged)
        || ((restart_target_matches || restoration_source_matches) && is_committed))
        || metadata.revision_digest != stored_digest
    {
        return Err(Status::failed_precondition(
            "deployment operation/revision digest mismatch",
        ));
    }
    txn.query_opt("SELECT authoritative FROM wr_node_slot_authority WHERE node_id=$1 AND engine_slot=$2 AND revision=$3 FOR UPDATE", &[&metadata.node_id,&metadata.engine_slot,&(metadata.revision as i64)]).await.internal()?;
    let inventory = DeploymentInventoryV1::decode(
        deployment
            .get::<_, Vec<u8>>("expected_inventory")
            .as_slice(),
    )
    .map_err(|e| Status::internal(format!("stored deployment inventory is invalid: {e}")))?;
    let expected = inventory
        .engines
        .iter()
        .find(|e| e.engine_slot == metadata.engine_slot)
        .ok_or_else(|| Status::failed_precondition("slot is not declared by deployment"))?;
    let mut actual_modules: std::collections::BTreeMap<(String, String, String), String> =
        std::collections::BTreeMap::new();
    for module in &reg.modules {
        let key = (
            module.namespace.clone(),
            module.name.clone(),
            module.version.clone(),
        );
        let digest = if module.proto_schema.is_empty() {
            String::new()
        } else {
            wr_common::deployment_contract::schema_digest(&module.proto_schema)
        };
        if digest.is_empty() {
            if actual_modules.contains_key(&key) {
                continue;
            }
            return Err(Status::invalid_argument(
                "module registration requires a protobuf schema",
            ));
        }
        match actual_modules.get(&key) {
            Some(existing) if existing.is_empty() => {
                actual_modules.insert(key, digest);
            }
            Some(existing) if existing != &digest => {
                return Err(Status::failed_precondition(
                    "duplicate module registration has conflicting schemas",
                ));
            }
            Some(_) => {}
            None => {
                actual_modules.insert(key, digest);
            }
        }
    }
    let expected_modules = expected
        .modules
        .iter()
        .map(|m| {
            let i = m
                .identity
                .as_ref()
                .ok_or_else(|| Status::internal("stored expected module identity missing"))?;
            Ok((
                (i.namespace.clone(), i.name.clone(), i.version.clone()),
                m.proto_schema_digest.clone(),
            ))
        })
        .collect::<Result<std::collections::BTreeMap<_, _>, Status>>()?;
    let mut actual_secrets = reg.secrets.clone();
    actual_secrets.sort_by(|a, b| (&a.namespace, &a.key).cmp(&(&b.namespace, &b.key)));
    let mut actual_dbs = reg.db_namespaces.clone();
    actual_dbs.sort();
    if actual_modules != expected_modules
        || actual_secrets != expected.secrets
        || actual_dbs != expected.db_namespaces
        || reg.job_queue_id != expected.job_queue_id
        || reg.job_admin_address != expected.job_admin_address
    {
        return Err(Status::failed_precondition(
            "registration resources do not exactly match declared slot inventory",
        ));
    }
    let owner=txn.query_one("SELECT operation_id,revision,revision_digest,activation_id,engine_id,slot_generation FROM wr_node_slot_owners WHERE node_id=$1 AND engine_slot=$2 FOR UPDATE", &[&metadata.node_id,&metadata.engine_slot]).await.internal()?;
    let generation_bytes: Vec<u8> = owner.get("slot_generation");
    let generation = u64::from_be_bytes(
        generation_bytes
            .try_into()
            .map_err(|_| Status::internal("stored slot generation is malformed"))?,
    );
    let replay = owner.get::<_, Option<uuid::Uuid>>("operation_id") == Some(operation_id)
        && owner.get::<_, Option<i64>>("revision") == Some(metadata.revision as i64)
        && owner.get::<_, Option<String>>("revision_digest").as_deref() == Some(&stored_digest)
        && owner.get::<_, Option<uuid::Uuid>>("activation_id") == Some(activation_uuid);
    let next_generation = assigned_slot_generation(generation, replay)?;
    if next_generation == 0 {
        return Err(Status::internal(
            "manager attempted to assign zero slot generation",
        ));
    }
    // Credential lookup and creation occur only after all desired-state and fence checks,
    // under this same owner transaction. Plaintext is returned only after commit.
    let mut grouped =
        std::collections::BTreeMap::<String, std::collections::HashMap<String, String>>::new();
    for secret in &reg.secrets {
        if secret.key.starts_with("__") {
            return Err(Status::invalid_argument("reserved secret key"));
        }
        let row = txn
            .query_opt(
                "SELECT ciphertext,nonce FROM wr_secrets WHERE namespace=$1 AND key=$2 FOR SHARE",
                &[&secret.namespace, &secret.key],
            )
            .await
            .internal()?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "missing secret: {}/{}",
                    secret.namespace, secret.key
                ))
            })?;
        let value = crypto
            .decrypt(&row.get::<_, Vec<u8>>(0), &row.get::<_, Vec<u8>>(1))
            .map_err(|e| Status::internal(format!("failed to decrypt secret: {e}")))?;
        grouped
            .entry(secret.namespace.clone())
            .or_default()
            .insert(secret.key.clone(), value);
    }
    let mut db_credentials = Vec::new();
    let dbs = reg
        .db_namespaces
        .iter()
        .collect::<std::collections::BTreeSet<_>>();
    for namespace in dbs {
        let key = "__db_password";
        let row = txn
            .query_opt(
                "SELECT ciphertext,nonce FROM wr_secrets WHERE namespace=$1 AND key=$2 FOR UPDATE",
                &[namespace, &key],
            )
            .await
            .internal()?;
        let password = if let Some(row) = row {
            crypto
                .decrypt(&row.get::<_, Vec<u8>>(0), &row.get::<_, Vec<u8>>(1))
                .map_err(|e| Status::internal(format!("failed to decrypt db password: {e}")))?
        } else {
            let candidate = crate::crypto::SecretCrypto::generate_random_password();
            let (ciphertext, nonce) = crypto
                .encrypt(&candidate)
                .map_err(|e| Status::internal(format!("encryption failed: {e}")))?;
            txn.execute(
                "INSERT INTO wr_secrets(namespace,key,ciphertext,nonce) VALUES($1,$2,$3,$4)",
                &[namespace, &key, &ciphertext, &nonce],
            )
            .await
            .internal()?;
            candidate
        };
        db_credentials.push(NamespaceDbCredential {
            namespace: (*namespace).clone(),
            role: namespace_role(namespace),
            password,
        });
    }
    let secrets = grouped
        .into_iter()
        .map(|(namespace, secrets)| NamespaceSecrets { namespace, secrets })
        .collect();
    if !replay {
        if let Some(old_engine) = owner.get::<_, Option<String>>("engine_id") {
            txn.execute("UPDATE wr_routing_rules SET healthy=FALSE,updated_at=NOW() WHERE engine_id=$1 AND healthy", &[&old_engine]).await.internal()?;
            txn.execute(
                "DELETE FROM wr_module_heartbeats WHERE engine_id=$1",
                &[&old_engine],
            )
            .await
            .internal()?;
        }
    }
    let registration_bytes = reg.encode_to_vec();
    let deployment_node_id = reg.deployment.as_ref().map(|value| value.node_id.as_str());
    let deployment_revision = reg
        .deployment
        .as_ref()
        .map(|value| i64::try_from(value.revision))
        .transpose()
        .map_err(|_| Status::invalid_argument("deployment revision is too large"))?;
    let deployment_bundle_digest = reg
        .deployment
        .as_ref()
        .map(|value| value.bundle_digest.as_str());
    let deployment_engine_slot = reg
        .deployment
        .as_ref()
        .map(|value| value.engine_slot.as_str());

    txn.execute(
        "INSERT INTO wr_engines
           (engine_id, address, proxy_address, peer_address, registration,
            deployment_node_id, deployment_revision, deployment_bundle_digest,
            deployment_engine_slot, job_queue_id, job_admin_address, operation_id,
            deployment_revision_digest, activation_id, slot_generation)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULLIF($10, ''), NULLIF($11, ''), $12, $13, $14, $15)
         ON CONFLICT (engine_id) DO UPDATE
           SET address = EXCLUDED.address,
               proxy_address = EXCLUDED.proxy_address,
               peer_address = EXCLUDED.peer_address,
               registration = EXCLUDED.registration,
               deployment_node_id = EXCLUDED.deployment_node_id,
               deployment_revision = EXCLUDED.deployment_revision,
               deployment_bundle_digest = EXCLUDED.deployment_bundle_digest,
               deployment_engine_slot = EXCLUDED.deployment_engine_slot,
               job_queue_id = EXCLUDED.job_queue_id,
               job_admin_address = EXCLUDED.job_admin_address,
               operation_id = EXCLUDED.operation_id,
               deployment_revision_digest = EXCLUDED.deployment_revision_digest,
               activation_id = EXCLUDED.activation_id,
               slot_generation = EXCLUDED.slot_generation,
               updated_at = NOW(),
               last_heartbeat = NOW(),
               draining = FALSE",
        &[
            &reg.engine_id,
            &reg.address,
            &reg.proxy_address,
            &reg.peer_address,
            &registration_bytes,
            &deployment_node_id,
            &deployment_revision,
            &deployment_bundle_digest,
            &deployment_engine_slot,
            &reg.job_queue_id,
            &reg.job_admin_address,
            &operation_id,
            &stored_digest,
            &activation_uuid,
            &&next_generation.to_be_bytes()[..],
        ],
    )
    .await
    .internal()?;

    for module in &reg.modules {
        if module.proto_schema.is_empty() {
            continue;
        }
        txn.execute(
            "INSERT INTO wr_schemas (namespace, module_name, version, proto_schema)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (namespace, module_name, version) DO UPDATE
               SET proto_schema = EXCLUDED.proto_schema,
                   updated_at = NOW()",
            &[
                &module.namespace,
                &module.name,
                &module.version,
                &module.proto_schema,
            ],
        )
        .await
        .internal()?;
    }

    // Publish one default routing rule per unique schema-bearing module tuple.
    // source_* = "", destination_* = module tuple, engine_address = reg.address,
    // peer_address = reg.peer_address, healthy = false.
    let empty = String::new();
    let healthy = false;
    let mut seen = std::collections::HashSet::new();
    let mut seen_advertised = std::collections::HashSet::new();
    let mut desired_rule_ids: Vec<String> = Vec::new();
    let mut advertised_ns: Vec<String> = Vec::new();
    let mut advertised_name: Vec<String> = Vec::new();
    let mut advertised_ver: Vec<String> = Vec::new();
    for module in &reg.modules {
        // Track every advertised tuple (schema-bearing or not) so heartbeat
        // reconciliation only removes modules this engine no longer advertises.
        if seen_advertised.insert((&module.namespace, &module.name, &module.version)) {
            advertised_ns.push(module.namespace.clone());
            advertised_name.push(module.name.clone());
            advertised_ver.push(module.version.clone());
        }
        if module.proto_schema.is_empty() {
            continue;
        }
        if !seen.insert((&module.namespace, &module.name, &module.version)) {
            continue;
        }
        let rule_id = format!(
            "{}/{}/{}/{}",
            reg.engine_id, module.namespace, module.name, module.version
        );
        txn.execute(
            "INSERT INTO wr_routing_rules (
                rule_id, source_namespace, source_module,
                destination_namespace, destination_module, destination_version,
                engine_id, engine_address, peer_address, healthy
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
             ON CONFLICT (rule_id) DO UPDATE SET
                source_namespace = EXCLUDED.source_namespace,
                source_module = EXCLUDED.source_module,
                destination_namespace = EXCLUDED.destination_namespace,
                destination_module = EXCLUDED.destination_module,
                destination_version = EXCLUDED.destination_version,
                engine_id = EXCLUDED.engine_id,
                engine_address = EXCLUDED.engine_address,
                peer_address = EXCLUDED.peer_address,
                healthy = EXCLUDED.healthy,
                updated_at = NOW()",
            &[
                &rule_id,
                &empty,
                &empty,
                &module.namespace,
                &module.name,
                &module.version,
                &reg.engine_id,
                &reg.address,
                &reg.peer_address,
                &healthy,
            ],
        )
        .await
        .internal()?;
        desired_rule_ids.push(rule_id);
    }
    let rules_written = desired_rule_ids.len() as u64;

    // Clear readiness for every tuple advertised by this registration. A
    // re-registering engine must not inherit module readiness from a previous
    // process before the new process has loaded and health-checked modules.
    txn.execute(
        "DELETE FROM wr_module_heartbeats
         WHERE engine_id = $1
           AND (namespace, module_name, version) IN (
             SELECT n, m, v FROM unnest($2::text[], $3::text[], $4::text[]) AS t(n, m, v)
           )",
        &[
            &reg.engine_id,
            &advertised_ns,
            &advertised_name,
            &advertised_ver,
        ],
    )
    .await
    .internal()?;

    // Reconcile: remove engine-owned DEFAULT rules this registration no longer
    // advertises. Only rows with this engine's canonical default prefix
    // "{engine_id}/" are touched; admin rules from UpsertRoutingRule use a
    // different rule_id shape and are left intact. When desired_rule_ids is empty
    // (no schema-bearing modules), `<> ALL('{}')` is true for every prefixed row,
    // so all of this engine's default rules are removed — the authoritative result.
    let engine_prefix = format!("{}/", reg.engine_id);
    let deleted_rules = txn
        .execute(
            "DELETE FROM wr_routing_rules
             WHERE engine_id = $1
               AND starts_with(rule_id, $2)
               AND rule_id <> ALL($3::text[])",
            &[&reg.engine_id, &engine_prefix, &desired_rule_ids],
        )
        .await
        .internal()?;

    // Drop per-module heartbeats for modules this engine no longer advertises;
    // retained/incoming tuples were already cleared above. Keep-set is every
    // advertised tuple; NOT IN over an empty unnest deletes all of this engine's
    // heartbeat rows (zero-module re-registration).
    txn.execute(
        "DELETE FROM wr_module_heartbeats
         WHERE engine_id = $1
           AND (namespace, module_name, version) NOT IN (
             SELECT n, m, v FROM unnest($2::text[], $3::text[], $4::text[]) AS t(n, m, v)
           )",
        &[
            &reg.engine_id,
            &advertised_ns,
            &advertised_name,
            &advertised_ver,
        ],
    )
    .await
    .internal()?;

    if rules_written > 0 || deleted_rules > 0 {
        increment_version(&txn).await?;
    }

    if let Some(metadata) = &reg.deployment {
        txn.execute(
            "UPDATE wr_node_deployments d
             SET state = 'active', activated_at = COALESCE(d.activated_at, NOW())
             FROM wr_nodes n
             WHERE d.node_id = $1 AND d.revision = $2 AND d.bundle_digest = $3
               AND d.state = 'pending' AND n.node_id = d.node_id
               AND (n.current_revision = d.revision OR n.target_revision = d.revision)",
            &[
                &metadata.node_id,
                &i64::try_from(metadata.revision)
                    .map_err(|_| Status::invalid_argument("deployment revision is too large"))?,
                &metadata.bundle_digest,
            ],
        )
        .await
        .internal()?;
    }

    txn.execute("UPDATE wr_node_slot_owners SET operation_id=$3,revision=$4,revision_digest=$5,activation_id=$6,engine_id=$7,slot_generation=$8,route_authority=TRUE,lifecycle_authority=TRUE,updated_at=NOW() WHERE node_id=$1 AND engine_slot=$2", &[&metadata.node_id,&metadata.engine_slot,&operation_id,&(metadata.revision as i64),&stored_digest,&activation_uuid,&reg.engine_id,&&next_generation.to_be_bytes()[..]]).await.internal()?;
    txn.commit().await.internal()?;
    Ok(RegistrationCommit {
        fence: EngineOwnershipFence {
            node_id: metadata.node_id.clone(),
            slot: metadata.engine_slot.clone(),
            revision_digest: stored_digest,
            activation_id: activation_id.to_string(),
            slot_generation: next_generation,
        },
        secrets,
        db_credentials,
    })
}

async fn require_owner_fence(
    txn: &deadpool_postgres::Transaction<'_>,
    engine_id: &str,
    fence: &EngineOwnershipFence,
) -> Result<(), Status> {
    if fence.node_id.is_empty()
        || fence.slot.is_empty()
        || fence.revision_digest.is_empty()
        || fence.activation_id.is_empty()
        || fence.slot_generation == 0
    {
        return Err(Status::permission_denied(
            "complete non-zero ownership fence is required",
        ));
    }
    let generation = fence.slot_generation.to_be_bytes();
    let row=txn.query_opt("SELECT 1 FROM wr_node_slot_owners WHERE node_id=$1 AND engine_slot=$2 AND revision_digest=$3 AND activation_id=$4 AND engine_id=$5 AND slot_generation=$6 AND lifecycle_authority FOR UPDATE", &[&fence.node_id,&fence.slot,&fence.revision_digest,&uuid::Uuid::parse_str(&fence.activation_id).map_err(|_|Status::permission_denied("ownership activation is invalid"))?,&engine_id,&&generation[..]]).await.internal()?;
    if row.is_none() {
        return Err(Status::permission_denied(
            "ownership fence is stale or mismatched",
        ));
    }
    Ok(())
}

/// Deregister an engine: mark its routing rules unhealthy, delete the engine
/// row, and bump the routing table version.
pub async fn deregister_engine(
    pool: &Pool,
    engine_id: &str,
    fence: &EngineOwnershipFence,
) -> Result<(), Status> {
    let mut client = pool.get().await.internal()?;
    let txn = client.transaction().await.internal()?;

    acquire_global_lock_wait(&txn).await?;
    require_owner_fence(&txn, engine_id, fence).await?;

    let changed = txn
        .execute(
            "UPDATE wr_routing_rules SET healthy = FALSE, updated_at = NOW()
             WHERE engine_id = $1 AND healthy = TRUE",
            &[&engine_id],
        )
        .await
        .internal()?;

    txn.execute("UPDATE wr_node_slot_owners SET route_authority=FALSE,lifecycle_authority=FALSE,updated_at=NOW() WHERE node_id=$1 AND engine_slot=$2", &[&fence.node_id,&fence.slot]).await.internal()?;
    txn.execute("DELETE FROM wr_engines WHERE engine_id = $1", &[&engine_id])
        .await
        .internal()?;

    txn.execute(
        "DELETE FROM wr_module_heartbeats WHERE engine_id = $1",
        &[&engine_id],
    )
    .await
    .internal()?;

    if changed > 0 {
        increment_version(&txn).await?;
    }

    txn.commit().await.internal()?;
    Ok(())
}

/// Atomically publish engine/module readiness, make matching routes serving,
/// and return the routing-table version that contains the publication.
pub async fn publish_engine_readiness(
    pool: &Pool,
    engine_id: &str,
    modules: &[ModuleDescriptor],
    fence: &EngineOwnershipFence,
) -> Result<u64, Status> {
    let mut client = pool.get().await.internal()?;
    let txn = client.transaction().await.internal()?;
    let mut version = acquire_global_lock_wait(&txn).await?;
    require_owner_fence(&txn, engine_id, fence).await?;

    let engine = txn
        .query_opt(
            "SELECT e.draining,
                    e.deployment_node_id IS NULL
                    OR (
                        EXISTS (
                            SELECT 1 FROM wr_node_slot_owners owner
                            WHERE owner.node_id = e.deployment_node_id
                              AND owner.engine_slot = e.deployment_engine_slot
                              AND owner.engine_id = e.engine_id
                              AND owner.route_authority
                        )
                        AND (
                            EXISTS (
                                SELECT 1 FROM wr_node_slot_authority a
                                WHERE a.node_id = e.deployment_node_id
                                  AND a.engine_slot = e.deployment_engine_slot
                                  AND a.revision = e.deployment_revision
                                  AND a.authoritative
                            )
                            OR (
                                e.deployment_revision = n.current_revision
                                AND NOT EXISTS (
                                    SELECT 1 FROM wr_node_slot_authority selected
                                    WHERE selected.node_id = e.deployment_node_id
                                      AND selected.engine_slot = e.deployment_engine_slot
                                      AND selected.authoritative
                                )
                            )
                        )
                    ) AS authoritative
             FROM wr_engines e
             LEFT JOIN wr_nodes n ON n.node_id = e.deployment_node_id
             WHERE e.engine_id = $1",
            &[&engine_id],
        )
        .await
        .internal()?
        .ok_or_else(|| {
            Status::failed_precondition(format!("engine {engine_id} is not registered"))
        })?;
    if engine.get::<_, bool>("draining") {
        return Err(Status::failed_precondition(format!(
            "engine {engine_id} is draining"
        )));
    }
    let authoritative: bool = engine.get("authoritative");
    txn.execute(
        "UPDATE wr_engines SET last_heartbeat = NOW(), updated_at = NOW() WHERE engine_id = $1",
        &[&engine_id],
    )
    .await
    .internal()?;

    let mut namespaces = Vec::with_capacity(modules.len());
    let mut names = Vec::with_capacity(modules.len());
    let mut versions = Vec::with_capacity(modules.len());
    for module in modules {
        txn.execute(
            "INSERT INTO wr_module_heartbeats (engine_id, namespace, module_name, version)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (engine_id, namespace, module_name, version)
             DO UPDATE SET last_healthy = NOW()",
            &[&engine_id, &module.namespace, &module.name, &module.version],
        )
        .await
        .internal()?;
        namespaces.push(module.namespace.clone());
        names.push(module.name.clone());
        versions.push(module.version.clone());
    }

    let changed = if authoritative {
        txn.execute(
            "UPDATE wr_routing_rules r
             SET healthy = TRUE, updated_at = NOW()
             WHERE r.engine_id = $1 AND r.healthy = FALSE
               AND (r.destination_namespace, r.destination_module, r.destination_version) IN (
                 SELECT namespace, name, version
                 FROM unnest($2::text[], $3::text[], $4::text[])
                   AS healthy(namespace, name, version)
               )",
            &[&engine_id, &namespaces, &names, &versions],
        )
        .await
        .internal()?
    } else {
        txn.execute(
            "UPDATE wr_routing_rules SET healthy = FALSE, updated_at = NOW()
             WHERE engine_id = $1 AND healthy = TRUE",
            &[&engine_id],
        )
        .await
        .internal()?
    };
    if changed > 0 {
        version = increment_version(&txn).await?;
    }

    txn.commit().await.internal()?;
    u64::try_from(version).map_err(|_| Status::internal("routing version is negative"))
}

/// Idempotently fence an engine from future readiness publication and make
/// every route for it non-serving without deleting its registration.
pub async fn begin_engine_drain(
    pool: &Pool,
    engine_id: &str,
    fence: &EngineOwnershipFence,
) -> Result<u64, Status> {
    let mut client = pool.get().await.internal()?;
    let txn = client.transaction().await.internal()?;
    let mut version = acquire_global_lock_wait(&txn).await?;
    require_owner_fence(&txn, engine_id, fence).await?;

    let exists = txn
        .query_opt(
            "UPDATE wr_engines
             SET draining = TRUE, updated_at = NOW()
             WHERE engine_id = $1
             RETURNING engine_id",
            &[&engine_id],
        )
        .await
        .internal()?;
    if exists.is_none() {
        return Err(Status::not_found(format!(
            "engine {engine_id} is not registered"
        )));
    }

    let changed = txn
        .execute(
            "UPDATE wr_routing_rules SET healthy = FALSE, updated_at = NOW()
             WHERE engine_id = $1 AND healthy = TRUE",
            &[&engine_id],
        )
        .await
        .internal()?;
    if changed > 0 {
        version = increment_version(&txn).await?;
    }

    txn.commit().await.internal()?;
    u64::try_from(version).map_err(|_| Status::internal("routing version is negative"))
}

/// Recompute routing-rule health from BOTH engine and per-module heartbeats.
///
/// A rule is healthy iff its engine's heartbeat is fresh (within
/// `engine_timeout_secs`) AND a matching `wr_module_heartbeats` row for the
/// rule's destination `(namespace, module, version)` on that engine is fresh
/// (within `module_timeout_secs`). A stale engine fails all its rules; a stale
/// or missing module takes only its own routes out of rotation.
///
/// Health recomputation and its routing generation update share the global
/// routing/evidence lock so operation reconciliation cannot commit from a
/// snapshot that predates a completed health publication.
///
/// Returns `(stale_rule_ids, recovered_rule_ids)`.
pub async fn update_route_health(
    pool: &Pool,
    engine_timeout_secs: f64,
    module_timeout_secs: f64,
) -> Result<(Vec<String>, Vec<String>), Status> {
    let mut client = pool.get().await.internal()?;
    let txn = client.transaction().await.internal()?;
    acquire_global_lock_wait(&txn).await?;

    // Mark unhealthy: currently healthy but no longer backed by BOTH a fresh
    // engine heartbeat and a fresh matching module heartbeat. Once any exact
    // slot authority exists it overrides the committed-revision fallback.
    let authority_predicate = "(
        e.deployment_node_id IS NULL
        OR (
            EXISTS (
                SELECT 1 FROM wr_node_slot_owners owner
                WHERE owner.node_id = e.deployment_node_id
                  AND owner.engine_slot = e.deployment_engine_slot
                  AND owner.engine_id = e.engine_id
                  AND owner.route_authority
            )
            AND (
                EXISTS (
                    SELECT 1 FROM wr_node_slot_authority a
                    WHERE a.node_id = e.deployment_node_id
                      AND a.engine_slot = e.deployment_engine_slot
                      AND a.revision = e.deployment_revision
                      AND a.authoritative
                )
                OR (
                    e.deployment_revision = n.current_revision
                    AND NOT EXISTS (
                        SELECT 1 FROM wr_node_slot_authority selected
                        WHERE selected.node_id = e.deployment_node_id
                          AND selected.engine_slot = e.deployment_engine_slot
                          AND selected.authoritative
                    )
                )
            )
        )
    )";
    let stale_sql = format!(
        "UPDATE wr_routing_rules r SET healthy = FALSE, updated_at = NOW()
         WHERE r.healthy = TRUE
           AND NOT EXISTS (
             SELECT 1 FROM wr_engines e
             LEFT JOIN wr_nodes n ON n.node_id = e.deployment_node_id
             JOIN wr_module_heartbeats m
               ON m.engine_id = e.engine_id
              AND m.namespace = r.destination_namespace
              AND m.module_name = r.destination_module
              AND m.version = r.destination_version
             WHERE e.engine_id = r.engine_id
               AND e.draining = FALSE
               AND {authority_predicate}
               AND e.last_heartbeat >= NOW() - make_interval(secs => $1::double precision)
               AND m.last_healthy >= NOW() - make_interval(secs => $2::double precision)
           )
         RETURNING rule_id"
    );
    let stale_rows = txn
        .query(&stale_sql, &[&engine_timeout_secs, &module_timeout_secs])
        .await
        .internal()?;

    // Mark healthy: currently unhealthy but now backed by BOTH fresh signals.
    let recovered_sql = format!(
        "UPDATE wr_routing_rules r SET healthy = TRUE, updated_at = NOW()
         WHERE r.healthy = FALSE
           AND EXISTS (
             SELECT 1 FROM wr_engines e
             LEFT JOIN wr_nodes n ON n.node_id = e.deployment_node_id
             JOIN wr_module_heartbeats m
               ON m.engine_id = e.engine_id
              AND m.namespace = r.destination_namespace
              AND m.module_name = r.destination_module
              AND m.version = r.destination_version
             WHERE e.engine_id = r.engine_id
               AND e.draining = FALSE
               AND {authority_predicate}
               AND e.last_heartbeat >= NOW() - make_interval(secs => $1::double precision)
               AND m.last_healthy >= NOW() - make_interval(secs => $2::double precision)
           )
         RETURNING rule_id"
    );
    let recovered_rows = txn
        .query(
            &recovered_sql,
            &[&engine_timeout_secs, &module_timeout_secs],
        )
        .await
        .internal()?;

    let stale: Vec<String> = stale_rows.iter().map(|r| r.get(0)).collect();
    let recovered: Vec<String> = recovered_rows.iter().map(|r| r.get(0)).collect();

    if !stale.is_empty() || !recovered.is_empty() {
        increment_version(&txn).await?;
    }
    txn.commit().await.internal()?;

    Ok((stale, recovered))
}

/// List all registered engines (decoded from protobuf BYTEA).
pub async fn list_engines(pool: &Pool) -> Result<Vec<EngineRegistration>, Status> {
    let client = pool.get().await.internal()?;
    let rows = client
        .query("SELECT registration FROM wr_engines", &[])
        .await
        .internal()?;

    rows.iter()
        .map(|row| {
            let bytes: Vec<u8> = row.get(0);
            EngineRegistration::decode(bytes.as_slice())
                .map_err(|e| Status::internal(format!("failed to decode registration: {e}")))
        })
        .collect()
}

#[derive(Clone, Debug)]
pub struct JobAdminDelegate {
    pub job_queue_id: String,
    pub engine_id: String,
    pub address: String,
    pub fresh: bool,
}

/// List all queue delegates, including stale registrations, in deterministic
/// queue/engine order. Freshness is computed by PostgreSQL at one observation.
pub async fn list_job_admin_delegates(
    pool: &Pool,
    heartbeat_timeout_secs: u64,
) -> Result<Vec<JobAdminDelegate>, Status> {
    let timeout = i64::try_from(heartbeat_timeout_secs)
        .map_err(|_| Status::internal("heartbeat timeout is too large"))?;
    let client = pool.get().await.internal()?;
    let rows = client
        .query(
            "SELECT job_queue_id, engine_id, job_admin_address, \
                    (NOT draining AND last_heartbeat >= statement_timestamp() - $1::bigint * interval '1 second') AS fresh \
             FROM wr_engines \
             WHERE job_queue_id IS NOT NULL AND job_admin_address IS NOT NULL \
             ORDER BY job_queue_id, engine_id",
            &[&timeout],
        )
        .await
        .internal()?;
    Ok(rows
        .into_iter()
        .map(|row| JobAdminDelegate {
            job_queue_id: row.get(0),
            engine_id: row.get(1),
            address: row.get(2),
            fresh: row.get(3),
        })
        .collect())
}

// ── Desired node deployment operations ──────────────────────────────────────

#[derive(Clone, Debug)]
pub struct StatusDeploymentRecord {
    pub record: DeploymentRecord,
    pub current_revision: u64,
    pub target_revision: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct StatusEngineRecord {
    pub registration: EngineRegistration,
    pub registered_at: chrono::DateTime<chrono::Utc>,
    pub last_heartbeat: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone, Debug)]
pub struct StatusModuleHeartbeat {
    pub engine_id: String,
    pub namespace: String,
    pub module_name: String,
    pub version: String,
    pub last_healthy: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone, Debug)]
pub struct StatusRouteRecord {
    pub rule: RoutingRule,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone, Debug)]
pub struct StatusManagerRecord {
    pub manager_id: String,
    pub grpc_address: String,
    pub registered_at: chrono::DateTime<chrono::Utc>,
    pub last_heartbeat: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone, Debug)]
pub struct StatusSlotAuthority {
    pub node_id: String,
    pub engine_slot: String,
    pub revision: u64,
    pub bundle_digest: String,
    pub resolved_release_digest: String,
}

#[derive(Clone, Debug)]
pub struct ClusterStatusSnapshot {
    pub observed_at: chrono::DateTime<chrono::Utc>,
    pub routing_version: u64,
    pub deployments: Vec<StatusDeploymentRecord>,
    pub engines: Vec<StatusEngineRecord>,
    pub module_heartbeats: Vec<StatusModuleHeartbeat>,
    pub routes: Vec<StatusRouteRecord>,
    pub managers: Vec<StatusManagerRecord>,
    pub slot_authorities: Vec<StatusSlotAuthority>,
    pub active_operations: Vec<NodeOperation>,
    pub observations: Vec<SlotObservation>,
    pub agent_attestations: Vec<NodeAgentAttestation>,
    pub agent_policies: Vec<NodeAgentPolicy>,
}

fn deployment_revision(value: u64) -> Result<i64, Status> {
    i64::try_from(value).map_err(|_| Status::invalid_argument("deployment revision is too large"))
}

#[derive(Clone, Debug)]
pub struct DeploymentRow {
    pub record: DeploymentRecord,
}

fn deployment_state(value: &str) -> Result<i32, Status> {
    match value {
        "pending" => Ok(DeploymentState::Pending as i32),
        "active" => Ok(DeploymentState::Active as i32),
        "succeeded" => Ok(DeploymentState::Succeeded as i32),
        "failed" => Ok(DeploymentState::Failed as i32),
        _ => Err(Status::internal(format!(
            "unknown deployment state '{value}'"
        ))),
    }
}

fn deployment_row(row: &tokio_postgres::Row) -> Result<DeploymentRow, Status> {
    let inventory: Vec<u8> = row.get("expected_inventory");
    let snapshot = DeploymentInventoryV1::decode(inventory.as_slice())
        .map_err(|e| Status::internal(format!("failed to decode deployment inventory: {e}")))?;
    let created_at: chrono::DateTime<chrono::Utc> = row.get("created_at");
    let activated_at: Option<chrono::DateTime<chrono::Utc>> = row.get("activated_at");
    let completed_at: Option<chrono::DateTime<chrono::Utc>> = row.get("completed_at");
    Ok(DeploymentRow {
        record: DeploymentRecord {
            node_id: row.get("node_id"),
            revision: row.get::<_, i64>("revision") as u64,
            attempt_token: row.get("attempt_token"),
            bundle_digest: row.get("bundle_digest"),
            inventory: Some(snapshot),
            state: deployment_state(row.get::<_, String>("state").as_str())?,
            created_at: Some(prost_types::Timestamp {
                seconds: created_at.timestamp(),
                nanos: created_at.timestamp_subsec_nanos() as i32,
            }),
            completed_at: completed_at.map(|time| prost_types::Timestamp {
                seconds: time.timestamp(),
                nanos: time.timestamp_subsec_nanos() as i32,
            }),
            failure_detail: row.get("failure_detail"),
            source_revision: row.get::<_, i64>("source_revision") as u64,
            activated_at: activated_at.map(|time| prost_types::Timestamp {
                seconds: time.timestamp(),
                nanos: time.timestamp_subsec_nanos() as i32,
            }),
            resolved_release_digest: row.get("resolved_release_digest"),
            finalized_at: row
                .get::<_, Option<chrono::DateTime<chrono::Utc>>>("finalized_at")
                .map(|time| prost_types::Timestamp {
                    seconds: time.timestamp(),
                    nanos: time.timestamp_subsec_nanos() as i32,
                }),
            inventory_schema_version: row.get::<_, i32>("inventory_schema_version") as u32,
            revision_digest: row.get("revision_digest"),
        },
    })
}

async fn get_deployment_in_transaction(
    txn: &deadpool_postgres::Transaction<'_>,
    node_id: &str,
    revision: i64,
) -> Result<Option<DeploymentRow>, Status> {
    txn.query_opt(
        "SELECT node_id, revision, attempt_token, bundle_digest, expected_inventory,
                inventory_schema_version, revision_digest, state, failure_detail,
                source_revision, resolved_release_digest, finalized_at, created_at,
                activated_at, completed_at
         FROM wr_node_deployments WHERE node_id = $1 AND revision = $2",
        &[&node_id, &revision],
    )
    .await
    .internal()?
    .as_ref()
    .map(deployment_row)
    .transpose()
}

/// Allocate a per-node revision. The node row lock makes independently running
/// managers serialize only attempts for the same stable node identity.
pub async fn begin_deployment(
    pool: &Pool,
    request: &BeginDeploymentRequest,
    actor: &str,
) -> Result<DeploymentRow, Status> {
    let inventory = canonicalize_inventory(
        request
            .inventory
            .clone()
            .ok_or_else(|| Status::invalid_argument("deployment inventory is required"))?,
    )
    .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let mut client = pool.get().await.internal()?;
    let txn = client.transaction().await.internal()?;

    txn.execute(
        "INSERT INTO wr_nodes (node_id) VALUES ($1) ON CONFLICT (node_id) DO NOTHING",
        &[&request.node_id],
    )
    .await
    .internal()?;

    // An attempt token is idempotent. Check again after acquiring the node lock
    // so a concurrent retry cannot allocate a second revision.
    let node = txn
        .query_one(
            "SELECT current_revision, target_revision FROM wr_nodes WHERE node_id = $1 FOR UPDATE",
            &[&request.node_id],
        )
        .await
        .internal()?;
    if let Some(row) = txn
        .query_opt(
            "SELECT node_id, revision, attempt_token, bundle_digest, expected_inventory,
                    inventory_schema_version, revision_digest, state, failure_detail,
                    source_revision, resolved_release_digest, finalized_at, created_at,
                    activated_at, completed_at, allocated_by
             FROM wr_node_deployments WHERE node_id = $1 AND attempt_token = $2",
            &[&request.node_id, &request.attempt_token],
        )
        .await
        .internal()?
    {
        let existing = deployment_row(&row)?;
        if row.get::<_, String>("allocated_by") != actor
            || existing.record.bundle_digest != request.bundle_digest
            || existing.record.inventory.as_ref() != Some(&inventory)
            || existing.record.source_revision != 0
        {
            return Err(Status::already_exists(
                "attempt_token was already used with different deployment content",
            ));
        }
        txn.commit().await.internal()?;
        return Ok(existing);
    }
    if let Some(target_revision) = node.get::<_, Option<i64>>("target_revision") {
        return Err(Status::failed_precondition(format!(
            "node '{}' already has staged target revision {target_revision}",
            request.node_id
        )));
    }

    let revision: i64 = txn
        .query_one(
            "SELECT COALESCE(MAX(revision), 0) + 1
             FROM wr_node_deployments WHERE node_id = $1",
            &[&request.node_id],
        )
        .await
        .internal()?
        .get(0);
    txn.execute(
        "UPDATE wr_nodes SET target_revision = $2, updated_at = NOW() WHERE node_id = $1",
        &[&request.node_id, &revision],
    )
    .await
    .internal()?;
    let digest = revision_digest(
        &request.node_id,
        revision as u64,
        &request.bundle_digest,
        &inventory,
    )
    .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let snapshot = inventory.encode_to_vec();
    txn.execute(
        "INSERT INTO wr_node_deployments
           (node_id, revision, attempt_token, bundle_digest, expected_inventory,
            inventory_schema_version, revision_digest, state, allocated_by)
         VALUES ($1, $2, $3, $4, $5, 1, $6, 'pending', $7)",
        &[
            &request.node_id,
            &revision,
            &request.attempt_token,
            &request.bundle_digest,
            &snapshot,
            &digest,
            &actor,
        ],
    )
    .await
    .internal()?;
    for engine in &inventory.engines {
        txn.execute(
            "INSERT INTO wr_node_slot_owners (node_id, engine_slot, slot_generation)
             VALUES ($1, $2, $3) ON CONFLICT (node_id, engine_slot) DO NOTHING",
            &[
                &request.node_id,
                &engine.engine_slot,
                &&0u64.to_be_bytes()[..],
            ],
        )
        .await
        .internal()?;
    }
    let deployment = get_deployment_in_transaction(&txn, &request.node_id, revision)
        .await?
        .expect("deployment inserted in this transaction");
    txn.commit().await.internal()?;
    Ok(deployment)
}

/// Bind the exact post-template release identity once before operation
/// submission. The source bundle identity remains independently immutable.
pub async fn finalize_deployment(
    pool: &Pool,
    request: &wr_common::wruntime::FinalizeDeploymentRequest,
    actor: &str,
) -> Result<DeploymentRow, Status> {
    let mut client = pool.get().await.internal()?;
    let txn = client.transaction().await.internal()?;
    let revision = deployment_revision(request.revision)?;
    let row = txn
        .query_one(
            "SELECT bundle_digest, resolved_release_digest, abandoned_at, operation_id, allocated_by
             FROM wr_node_deployments
             WHERE node_id = $1 AND attempt_token = $2 AND revision = $3 FOR UPDATE",
            &[&request.node_id, &request.attempt_token, &revision],
        )
        .await
        .internal()?;
    if row.get::<_, String>("allocated_by") != actor {
        return Err(Status::permission_denied(
            "deployment allocation belongs to a different authenticated actor",
        ));
    }
    if row.get::<_, String>("bundle_digest") != request.bundle_digest {
        return Err(Status::already_exists(
            "finalization source identity conflicts with the allocated deployment",
        ));
    }
    if row
        .get::<_, Option<chrono::DateTime<chrono::Utc>>>("abandoned_at")
        .is_some()
    {
        return Err(Status::failed_precondition(
            "staged allocation was abandoned",
        ));
    }
    let existing: String = row.get("resolved_release_digest");
    if !existing.is_empty() && existing != request.resolved_release_digest {
        return Err(Status::already_exists(
            "deployment was already finalized with a different resolved release digest",
        ));
    }
    if existing.is_empty() {
        if row.get::<_, Option<uuid::Uuid>>("operation_id").is_some() {
            return Err(Status::failed_precondition(
                "submitted deployment cannot be finalized",
            ));
        }
        txn.execute(
            "UPDATE wr_node_deployments
             SET resolved_release_digest = $4, finalized_at = NOW(), finalized_by = $5
             WHERE node_id = $1 AND attempt_token = $2 AND revision = $3",
            &[
                &request.node_id,
                &request.attempt_token,
                &revision,
                &request.resolved_release_digest,
                &actor,
            ],
        )
        .await
        .internal()?;
    }
    let deployment = get_deployment_in_transaction(&txn, &request.node_id, revision)
        .await?
        .expect("finalized deployment remains present");
    txn.commit().await.internal()?;
    Ok(deployment)
}

/// Abandon an unbound staged allocation only while manager evidence proves it
/// has no operation, registration, or serving authority. Exact retries are
/// idempotent; submitted allocations must be cancelled through the operation.
pub async fn abandon_deployment(
    pool: &Pool,
    node_id: &str,
    attempt_token: &str,
    actor: &str,
) -> Result<DeploymentRow, Status> {
    let mut client = pool.get().await.internal()?;
    let txn = client.transaction().await.internal()?;
    let row = txn
        .query_opt(
            "SELECT revision, operation_id, abandoned_at, allocated_by
             FROM wr_node_deployments
             WHERE node_id = $1 AND attempt_token = $2 FOR UPDATE",
            &[&node_id, &attempt_token],
        )
        .await
        .internal()?
        .ok_or_else(|| Status::not_found("staged allocation not found"))?;
    if row.get::<_, String>("allocated_by") != actor {
        return Err(Status::permission_denied(
            "deployment allocation belongs to a different authenticated actor",
        ));
    }
    if row
        .get::<_, Option<chrono::DateTime<chrono::Utc>>>("abandoned_at")
        .is_some()
    {
        let deployment = get_deployment_in_transaction(&txn, node_id, row.get("revision"))
            .await?
            .expect("abandoned deployment remains present");
        txn.commit().await.internal()?;
        return Ok(deployment);
    }
    if row.get::<_, Option<uuid::Uuid>>("operation_id").is_some() {
        return Err(Status::failed_precondition(
            "a submitted allocation must be cancelled through its operation",
        ));
    }
    let revision: i64 = row.get("revision");
    let has_effect: bool = txn
        .query_one(
            "SELECT EXISTS(
                SELECT 1 FROM wr_engines
                WHERE deployment_node_id = $1 AND deployment_revision = $2
             ) OR EXISTS(
                SELECT 1 FROM wr_node_slot_authority
                WHERE node_id = $1 AND revision = $2 AND authoritative
             )",
            &[&node_id, &revision],
        )
        .await
        .internal()?
        .get(0);
    if has_effect {
        return Err(Status::failed_precondition(
            "staged allocation has activation or authority evidence and cannot be abandoned",
        ));
    }
    txn.execute(
        "UPDATE wr_node_deployments
         SET state = 'failed', failure_detail = 'staged allocation abandoned',
             completed_at = NOW(), abandoned_at = NOW(), abandoned_by = $3
         WHERE node_id = $1 AND revision = $2",
        &[&node_id, &revision, &actor],
    )
    .await
    .internal()?;
    txn.execute(
        "UPDATE wr_nodes SET target_revision = NULL, updated_at = NOW()
         WHERE node_id = $1 AND target_revision = $2",
        &[&node_id, &revision],
    )
    .await
    .internal()?;
    let deployment = get_deployment_in_transaction(&txn, node_id, revision)
        .await?
        .expect("abandoned deployment remains present");
    txn.commit().await.internal()?;
    Ok(deployment)
}

pub async fn get_deployment(
    pool: &Pool,
    node_id: &str,
    revision: u64,
) -> Result<DeploymentRow, Status> {
    let client = pool.get().await.internal()?;
    let row = client
        .query_opt(
            "SELECT node_id, revision, attempt_token, bundle_digest, expected_inventory,
                    inventory_schema_version, revision_digest, state, failure_detail,
                    source_revision, resolved_release_digest, finalized_at, created_at,
                    activated_at, completed_at
             FROM wr_node_deployments WHERE node_id = $1 AND revision = $2",
            &[&node_id, &deployment_revision(revision)?],
        )
        .await
        .internal()?
        .ok_or_else(|| Status::not_found(format!("deployment {node_id}/{revision} not found")))?;
    deployment_row(&row)
}

pub async fn complete_deployment(
    pool: &Pool,
    node_id: &str,
    revision: u64,
    succeeded: bool,
    failure_detail: &str,
) -> Result<DeploymentRow, Status> {
    let mut client = pool.get().await.internal()?;
    let txn = client.transaction().await.internal()?;
    let state = if succeeded { "succeeded" } else { "failed" };
    let database_revision = deployment_revision(revision)?;
    let updated = txn
        .execute(
            "UPDATE wr_node_deployments
             SET state = $3, failure_detail = $4, completed_at = NOW()
             WHERE node_id = $1 AND revision = $2 AND state IN ('pending', 'active')",
            &[&node_id, &database_revision, &state, &failure_detail],
        )
        .await
        .internal()?;
    if updated == 0 {
        return Err(Status::failed_precondition(format!(
            "deployment {node_id}/{revision} is not pending or active"
        )));
    }
    if succeeded {
        txn.execute(
            "UPDATE wr_nodes SET current_revision = $2, target_revision = NULL, updated_at = NOW()
             WHERE node_id = $1 AND target_revision = $2",
            &[&node_id, &database_revision],
        )
        .await
        .internal()?;
    } else {
        // The committed serving revision never moved at begin; failure only
        // clears this exact staged target after its slots have been reconciled.
        txn.execute(
            "UPDATE wr_nodes SET target_revision = NULL, updated_at = NOW()
             WHERE node_id = $1 AND target_revision = $2",
            &[&node_id, &database_revision],
        )
        .await
        .internal()?;
    }
    let deployment = get_deployment_in_transaction(&txn, node_id, database_revision)
        .await?
        .ok_or_else(|| Status::internal("completed deployment disappeared"))?;
    txn.commit().await.internal()?;
    Ok(deployment)
}

/// Capture all DB-backed cluster and operation evidence through one transaction.
/// Callers select read-only or mutation semantics before invoking this helper.
pub(crate) async fn capture_cluster_status_snapshot<C>(
    txn: &C,
) -> Result<ClusterStatusSnapshot, Status>
where
    C: GenericClient + Sync,
{
    let observed_at: chrono::DateTime<chrono::Utc> =
        txn.query_one("SELECT NOW()", &[]).await.internal()?.get(0);
    let routing_version = txn
        .query_one("SELECT version FROM wr_manager_lock WHERE id = 1", &[])
        .await
        .internal()?
        .get::<_, i64>(0) as u64;

    let deployment_rows = txn
        .query(
            "SELECT d.node_id, d.revision, d.attempt_token, d.bundle_digest,
                    d.expected_inventory, d.inventory_schema_version, d.revision_digest,
                    d.state, d.failure_detail, d.source_revision,
                    d.resolved_release_digest, d.finalized_at, d.created_at,
                    d.activated_at, d.completed_at, n.current_revision,
                    n.target_revision
             FROM wr_node_deployments d
             JOIN wr_nodes n ON n.node_id = d.node_id
             ORDER BY d.node_id, d.revision DESC",
            &[],
        )
        .await
        .internal()?;
    let deployments = deployment_rows
        .iter()
        .map(|row| {
            Ok(StatusDeploymentRecord {
                record: deployment_row(row)?.record,
                current_revision: row.get::<_, i64>("current_revision") as u64,
                target_revision: row
                    .get::<_, Option<i64>>("target_revision")
                    .map(|revision| revision as u64),
            })
        })
        .collect::<Result<Vec<_>, Status>>()?;

    let engine_rows = txn
        .query(
            "SELECT registration, registered_at, last_heartbeat
             FROM wr_engines ORDER BY engine_id",
            &[],
        )
        .await
        .internal()?;
    let engines = engine_rows
        .iter()
        .map(|row| {
            let bytes: Vec<u8> = row.get("registration");
            Ok(StatusEngineRecord {
                registration: EngineRegistration::decode(bytes.as_slice()).map_err(|error| {
                    Status::internal(format!("failed to decode engine registration: {error}"))
                })?,
                registered_at: row.get("registered_at"),
                last_heartbeat: row.get("last_heartbeat"),
            })
        })
        .collect::<Result<Vec<_>, Status>>()?;

    let module_heartbeats = txn
        .query(
            "SELECT engine_id, namespace, module_name, version, last_healthy
             FROM wr_module_heartbeats
             ORDER BY engine_id, namespace, module_name, version",
            &[],
        )
        .await
        .internal()?
        .iter()
        .map(|row| StatusModuleHeartbeat {
            engine_id: row.get("engine_id"),
            namespace: row.get("namespace"),
            module_name: row.get("module_name"),
            version: row.get("version"),
            last_healthy: row.get("last_healthy"),
        })
        .collect();

    let routes = txn
        .query(
            "SELECT rule_id, source_namespace, source_module, destination_namespace,
                    destination_module, destination_version, engine_id, engine_address,
                    healthy, peer_address, updated_at
             FROM wr_routing_rules ORDER BY rule_id",
            &[],
        )
        .await
        .internal()?
        .iter()
        .map(|row| StatusRouteRecord {
            rule: RoutingRule {
                rule_id: row.get("rule_id"),
                source_module: row.get("source_module"),
                destination_module: row.get("destination_module"),
                engine_id: row.get("engine_id"),
                engine_address: row.get("engine_address"),
                destination_version: row.get("destination_version"),
                healthy: row.get("healthy"),
                source_namespace: row.get("source_namespace"),
                destination_namespace: row.get("destination_namespace"),
                peer_address: row.get("peer_address"),
            },
            updated_at: row.get("updated_at"),
        })
        .collect();

    let managers = txn
        .query(
            "SELECT manager_id, grpc_address, registered_at, last_heartbeat
             FROM wr_managers ORDER BY manager_id",
            &[],
        )
        .await
        .internal()?
        .iter()
        .map(|row| StatusManagerRecord {
            manager_id: row.get("manager_id"),
            grpc_address: row.get("grpc_address"),
            registered_at: row.get("registered_at"),
            last_heartbeat: row.get("last_heartbeat"),
        })
        .collect();

    let slot_authorities = txn
        .query(
            "SELECT a.node_id, a.engine_slot, a.revision, d.bundle_digest,
                    a.resolved_release_digest
             FROM wr_node_slot_authority a
             JOIN wr_node_deployments d ON d.node_id = a.node_id AND d.revision = a.revision
             WHERE a.authoritative ORDER BY a.node_id, a.engine_slot",
            &[],
        )
        .await
        .internal()?
        .iter()
        .map(|row| StatusSlotAuthority {
            node_id: row.get("node_id"),
            engine_slot: row.get("engine_slot"),
            revision: row.get::<_, i64>("revision") as u64,
            bundle_digest: row.get("bundle_digest"),
            resolved_release_digest: row.get("resolved_release_digest"),
        })
        .collect();

    let active_operations = crate::operations::list_from_client(txn, "", false).await?;
    let observations = crate::operations::observations_from_client(txn, "", "").await?;
    let agent_attestations = crate::operations::attestations_from_client(txn, "").await?;
    let agent_policies = crate::operations::policies_from_client(txn).await?;
    Ok(ClusterStatusSnapshot {
        observed_at,
        routing_version,
        deployments,
        engines,
        module_heartbeats,
        routes,
        managers,
        slot_authorities,
        active_operations,
        observations,
        agent_attestations,
        agent_policies,
    })
}

/// Capture all DB-backed cluster status evidence under one repeatable-read,
/// read-only transaction. Presentation severity is intentionally not persisted.
pub async fn get_cluster_status_snapshot(pool: &Pool) -> Result<ClusterStatusSnapshot, Status> {
    let mut client = pool.get().await.internal()?;
    let txn = client.transaction().await.internal()?;
    txn.batch_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await
        .internal()?;
    let snapshot = capture_cluster_status_snapshot(&txn).await?;
    txn.commit().await.internal()?;
    Ok(snapshot)
}

fn fresh(
    observed_at: chrono::DateTime<chrono::Utc>,
    timestamp: chrono::DateTime<chrono::Utc>,
    timeout_secs: f64,
) -> bool {
    observed_at
        .signed_duration_since(timestamp)
        .num_milliseconds() as f64
        <= timeout_secs * 1000.0
}

/// Pure Plan 1 verifier over an already captured snapshot. Cluster status and
/// VerifyDeployment therefore use identical desired-revision semantics.
pub fn deployment_conditions_from_snapshot(
    snapshot: &ClusterStatusSnapshot,
    deployment: &DeploymentRecord,
    engine_timeout_secs: f64,
    module_timeout_secs: f64,
) -> Result<Vec<(String, String)>, Status> {
    let current_revision = snapshot
        .deployments
        .iter()
        .find(|candidate| candidate.record.node_id == deployment.node_id)
        .map(|candidate| candidate.current_revision)
        .ok_or_else(|| Status::not_found(format!("node '{}' not found", deployment.node_id)))?;
    let target_revision = snapshot
        .deployments
        .iter()
        .find(|candidate| candidate.record.node_id == deployment.node_id)
        .and_then(|candidate| candidate.target_revision);
    if current_revision != deployment.revision && target_revision != Some(deployment.revision) {
        return Ok(vec![(
            "NON_AUTHORITATIVE_REVISION".into(),
            format!(
                "revision {} is neither committed revision {} nor staged target {:?}",
                deployment.revision, current_revision, target_revision
            ),
        )]);
    }

    let mut conditions = Vec::new();
    let expected_engines = deployment
        .inventory
        .as_ref()
        .map(|inventory| inventory.engines.as_slice())
        .unwrap_or_default();
    for expected in expected_engines {
        let same_slot: Vec<_> = snapshot
            .engines
            .iter()
            .filter(|engine| {
                engine
                    .registration
                    .deployment
                    .as_ref()
                    .is_some_and(|metadata| {
                        metadata.node_id == deployment.node_id
                            && metadata.engine_slot == expected.engine_slot
                    })
            })
            .collect();
        let exact: Vec<_> = same_slot
            .iter()
            .filter(|engine| {
                engine
                    .registration
                    .deployment
                    .as_ref()
                    .is_some_and(|metadata| {
                        metadata.revision == deployment.revision
                            && metadata.bundle_digest == deployment.bundle_digest
                    })
            })
            .copied()
            .collect();
        if exact.is_empty() {
            let code = if same_slot.is_empty() {
                "MISSING_ENGINE"
            } else if same_slot.iter().any(|engine| {
                engine
                    .registration
                    .deployment
                    .as_ref()
                    .is_some_and(|metadata| metadata.revision != deployment.revision)
            }) {
                "REVISION_MISMATCH"
            } else {
                "DIGEST_MISMATCH"
            };
            conditions.push((
                code.into(),
                format!(
                    "engine slot '{}' has no matching activated registration",
                    expected.engine_slot
                ),
            ));
            continue;
        }
        let fresh_engines: Vec<_> = exact
            .into_iter()
            .filter(|engine| {
                fresh(
                    snapshot.observed_at,
                    engine.last_heartbeat,
                    engine_timeout_secs,
                )
            })
            .collect();
        if fresh_engines.is_empty() {
            conditions.push((
                "STALE_ENGINE_HEARTBEAT".into(),
                format!(
                    "engine slot '{}' has no fresh matching registration",
                    expected.engine_slot
                ),
            ));
            continue;
        }
        if fresh_engines.len() != 1 {
            conditions.push((
                "DUPLICATE_ENGINE_SLOT".into(),
                format!(
                    "engine slot '{}' has {} fresh matching registrations",
                    expected.engine_slot,
                    fresh_engines.len()
                ),
            ));
            continue;
        }
        let engine = &fresh_engines[0].registration;
        for module in &expected.modules {
            let Some(module) = module.identity.as_ref() else {
                conditions.push((
                    "INVALID_INVENTORY".into(),
                    "expected module identity is missing".into(),
                ));
                continue;
            };
            if !engine.modules.iter().any(|advertised| {
                advertised.namespace == module.namespace
                    && advertised.name == module.name
                    && advertised.version == module.version
            }) {
                conditions.push((
                    "MISSING_MODULE".into(),
                    format!(
                        "engine slot '{}' does not advertise {}.{}@{}",
                        expected.engine_slot, module.namespace, module.name, module.version
                    ),
                ));
                continue;
            }
            let heartbeat = snapshot.module_heartbeats.iter().find(|heartbeat| {
                heartbeat.engine_id == engine.engine_id
                    && heartbeat.namespace == module.namespace
                    && heartbeat.module_name == module.name
                    && heartbeat.version == module.version
            });
            let Some(heartbeat) = heartbeat else {
                conditions.push((
                    "MISSING_MODULE_HEARTBEAT".into(),
                    format!(
                        "engine slot '{}' module {}.{}@{} has no heartbeat",
                        expected.engine_slot, module.namespace, module.name, module.version
                    ),
                ));
                continue;
            };
            if !fresh(
                snapshot.observed_at,
                heartbeat.last_healthy,
                module_timeout_secs,
            ) {
                conditions.push((
                    "STALE_MODULE_HEARTBEAT".into(),
                    format!(
                        "engine slot '{}' module {}.{}@{} is not fresh",
                        expected.engine_slot, module.namespace, module.name, module.version
                    ),
                ));
                continue;
            }
            let rule_id = format!(
                "{}/{}/{}/{}",
                engine.engine_id, module.namespace, module.name, module.version
            );
            match snapshot.routes.iter().find(|route| {
                route.rule.rule_id == rule_id
                    && route.rule.engine_id == engine.engine_id
                    && route.rule.destination_namespace == module.namespace
                    && route.rule.destination_module == module.name
                    && route.rule.destination_version == module.version
            }) {
                None => conditions.push((
                    "MISSING_ROUTE".into(),
                    format!(
                        "engine slot '{}' module {}.{}@{} has no default route",
                        expected.engine_slot, module.namespace, module.name, module.version
                    ),
                )),
                Some(route) if !route.rule.healthy => conditions.push((
                    "UNHEALTHY_ROUTE".into(),
                    format!(
                        "engine slot '{}' module {}.{}@{} default route is unhealthy",
                        expected.engine_slot, module.namespace, module.name, module.version
                    ),
                )),
                Some(_) => {}
            }
        }
    }
    conditions.sort();
    Ok(conditions)
}

/// Evaluate desired slots against one coherent database snapshot.
pub async fn deployment_conditions(
    pool: &Pool,
    deployment: &DeploymentRecord,
    engine_timeout_secs: f64,
    module_timeout_secs: f64,
) -> Result<Vec<(String, String)>, Status> {
    let snapshot = get_cluster_status_snapshot(pool).await?;
    deployment_conditions_from_snapshot(
        &snapshot,
        deployment,
        engine_timeout_secs,
        module_timeout_secs,
    )
}

pub async fn begin_rollback(
    pool: &Pool,
    node_id: &str,
    to_revision: u64,
    attempt_token: &str,
    actor: &str,
) -> Result<DeploymentRow, Status> {
    let mut client = pool.get().await.internal()?;
    let txn = client.transaction().await.internal()?;
    let node = txn
        .query_opt(
            "SELECT current_revision, target_revision FROM wr_nodes WHERE node_id = $1 FOR UPDATE",
            &[&node_id],
        )
        .await
        .internal()?
        .ok_or_else(|| Status::not_found(format!("node '{node_id}' has no deployment history")))?;
    let current_revision: i64 = node.get("current_revision");

    if let Some(row) = txn
        .query_opt(
            "SELECT node_id, revision, attempt_token, bundle_digest, expected_inventory,
                    inventory_schema_version, revision_digest, state, failure_detail,
                    source_revision, resolved_release_digest, finalized_at, created_at,
                    activated_at, completed_at, allocated_by
             FROM wr_node_deployments WHERE node_id = $1 AND attempt_token = $2",
            &[&node_id, &attempt_token],
        )
        .await
        .internal()?
    {
        let existing = deployment_row(&row)?;
        if row.get::<_, String>("allocated_by") != actor
            || existing.record.source_revision == 0
            || (to_revision != 0 && existing.record.source_revision != to_revision)
        {
            return Err(Status::already_exists(
                "attempt_token was already used for different deployment content",
            ));
        }
        txn.commit().await.internal()?;
        return Ok(existing);
    }
    if let Some(target_revision) = node.get::<_, Option<i64>>("target_revision") {
        return Err(Status::failed_precondition(format!(
            "node '{node_id}' already has staged target revision {target_revision}"
        )));
    }

    let selected_row = if to_revision == 0 {
        txn.query_opt(
            "SELECT node_id, revision, attempt_token, bundle_digest, expected_inventory,
                    inventory_schema_version, revision_digest, state, failure_detail,
                    source_revision, resolved_release_digest, finalized_at, created_at,
                    activated_at, completed_at
             FROM wr_node_deployments
             WHERE node_id = $1 AND state = 'succeeded' AND revision < $2
             ORDER BY revision DESC LIMIT 1",
            &[&node_id, &current_revision],
        )
        .await
        .internal()?
    } else {
        let requested = i64::try_from(to_revision)
            .map_err(|_| Status::invalid_argument("to_revision is too large"))?;
        txn.query_opt(
            "SELECT node_id, revision, attempt_token, bundle_digest, expected_inventory,
                    inventory_schema_version, revision_digest, state, failure_detail,
                    source_revision, resolved_release_digest, finalized_at, created_at,
                    activated_at, completed_at
             FROM wr_node_deployments
             WHERE node_id = $1 AND revision = $2 AND revision < $3 AND state = 'succeeded'",
            &[&node_id, &requested, &current_revision],
        )
        .await
        .internal()?
    }
    .ok_or_else(|| Status::not_found("requested prior successful deployment snapshot not found"))?;
    let selected = deployment_row(&selected_row)?.record;
    let revision: i64 = txn
        .query_one(
            "SELECT COALESCE(MAX(revision), 0) + 1
             FROM wr_node_deployments WHERE node_id = $1",
            &[&node_id],
        )
        .await
        .internal()?
        .get(0);
    txn.execute(
        "UPDATE wr_nodes SET target_revision = $2, updated_at = NOW() WHERE node_id = $1",
        &[&node_id, &revision],
    )
    .await
    .internal()?;
    let inventory = selected
        .inventory
        .ok_or_else(|| Status::internal("selected deployment inventory missing"))?;
    let digest = revision_digest(
        node_id,
        revision as u64,
        &selected.bundle_digest,
        &inventory,
    )
    .map_err(|e| Status::internal(e.to_string()))?;
    let snapshot = inventory.encode_to_vec();
    txn.execute(
        "INSERT INTO wr_node_deployments
           (node_id, revision, attempt_token, bundle_digest, expected_inventory,
            inventory_schema_version, revision_digest, state, source_revision, allocated_by)
         VALUES ($1, $2, $3, $4, $5, 1, $6, 'pending', $7, $8)",
        &[
            &node_id,
            &revision,
            &attempt_token,
            &selected.bundle_digest,
            &snapshot,
            &digest,
            &(selected.revision as i64),
            &actor,
        ],
    )
    .await
    .internal()?;
    let deployment = get_deployment_in_transaction(&txn, node_id, revision)
        .await?
        .expect("rollback deployment inserted in this transaction");
    txn.commit().await.internal()?;
    Ok(deployment)
}

// ── Routing operations ───────────────────────────────────────────────────────

/// Upsert a routing rule. Acquires the global lock and bumps the version.
/// Retries automatically on NOWAIT lock contention with exponential backoff.
pub async fn upsert_routing_rule(pool: &Pool, rule: &RoutingRule) -> Result<(), Status> {
    if rule.peer_address.is_empty() {
        return Err(Status::invalid_argument(
            "routing rule peer_address must be non-empty",
        ));
    }
    RetryIf::start(
        lock_retry_strategy(),
        || upsert_routing_rule_once(pool, rule),
        is_lock_contention,
    )
    .await
}

async fn upsert_routing_rule_once(pool: &Pool, rule: &RoutingRule) -> Result<(), Status> {
    let mut client = pool.get().await.internal()?;
    let txn = client.transaction().await.internal()?;

    acquire_global_lock(&txn, "upsert_routing_rule").await?;

    txn.execute(
        "INSERT INTO wr_routing_rules (
            rule_id, source_namespace, source_module,
            destination_namespace, destination_module, destination_version,
            engine_id, engine_address, peer_address, healthy
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
         ON CONFLICT (rule_id) DO UPDATE SET
            source_namespace = EXCLUDED.source_namespace,
            source_module = EXCLUDED.source_module,
            destination_namespace = EXCLUDED.destination_namespace,
            destination_module = EXCLUDED.destination_module,
            destination_version = EXCLUDED.destination_version,
            engine_id = EXCLUDED.engine_id,
            engine_address = EXCLUDED.engine_address,
            peer_address = EXCLUDED.peer_address,
            healthy = EXCLUDED.healthy,
            updated_at = NOW()",
        &[
            &rule.rule_id,
            &rule.source_namespace,
            &rule.source_module,
            &rule.destination_namespace,
            &rule.destination_module,
            &rule.destination_version,
            &rule.engine_id,
            &rule.engine_address,
            &rule.peer_address,
            &rule.healthy,
        ],
    )
    .await
    .internal()?;

    increment_version(&txn).await?;
    txn.commit().await.internal()?;
    Ok(())
}

pub async fn resolve_engine_node(pool: &Pool, engine_id: &str) -> Result<Option<String>, Status> {
    let client = pool.get().await.internal()?;
    client
        .query_opt(
            "SELECT deployment_node_id FROM wr_engines WHERE engine_id=$1",
            &[&engine_id],
        )
        .await
        .internal()
        .map(|row| row.and_then(|row| row.get(0)))
}

pub async fn resolve_routing_rule_namespace(
    pool: &Pool,
    rule_id: &str,
) -> Result<Option<String>, Status> {
    let client = pool.get().await.internal()?;
    client
        .query_opt(
            "SELECT destination_namespace FROM wr_routing_rules WHERE rule_id=$1",
            &[&rule_id],
        )
        .await
        .internal()
        .map(|row| row.map(|row| row.get(0)))
}

/// Delete a routing rule by ID. Returns true if a rule was actually deleted.
/// Retries automatically on NOWAIT lock contention with exponential backoff.
pub async fn delete_routing_rule_scoped(
    pool: &Pool,
    rule_id: &str,
    expected_namespace: &str,
) -> Result<bool, Status> {
    RetryIf::start(
        lock_retry_strategy(),
        || delete_routing_rule_once(pool, rule_id, Some(expected_namespace)),
        is_lock_contention,
    )
    .await
}

pub async fn delete_routing_rule(pool: &Pool, rule_id: &str) -> Result<bool, Status> {
    RetryIf::start(
        lock_retry_strategy(),
        || delete_routing_rule_once(pool, rule_id, None),
        is_lock_contention,
    )
    .await
}

async fn delete_routing_rule_once(
    pool: &Pool,
    rule_id: &str,
    expected_namespace: Option<&str>,
) -> Result<bool, Status> {
    let mut client = pool.get().await.internal()?;
    let txn = client.transaction().await.internal()?;

    acquire_global_lock(&txn, "delete_routing_rule").await?;

    let deleted = txn
        .execute(
            "DELETE FROM wr_routing_rules WHERE rule_id = $1 AND ($2::text IS NULL OR destination_namespace = $2)",
            &[&rule_id, &expected_namespace],
        )
        .await
        .internal()?;

    if deleted > 0 {
        increment_version(&txn).await?;
    }

    txn.commit().await.internal()?;
    Ok(deleted > 0)
}

/// Read the routing table from the database.
/// If `known_version` is non-zero and matches the current version, returns `None`
/// (the caller's copy is up to date). Otherwise returns the full table.
pub async fn get_routing_table(
    pool: &Pool,
    known_version: u64,
) -> Result<Option<RoutingTable>, Status> {
    let client = pool.get().await.internal()?;

    let version: i64 = client
        .query_one("SELECT version FROM wr_manager_lock WHERE id = 1", &[])
        .await
        .internal()?
        .get(0);

    if known_version != 0 && known_version == version as u64 {
        return Ok(None);
    }

    let rows = client
        .query(
            "SELECT rule_id, source_namespace, source_module,
                    destination_namespace, destination_module, destination_version,
                    engine_id, engine_address, healthy, peer_address
             FROM wr_routing_rules",
            &[],
        )
        .await
        .internal()?;

    let rules = rows
        .iter()
        .map(|row| RoutingRule {
            rule_id: row.get(0),
            source_namespace: row.get(1),
            source_module: row.get(2),
            destination_namespace: row.get(3),
            destination_module: row.get(4),
            destination_version: row.get(5),
            engine_id: row.get(6),
            engine_address: row.get(7),
            healthy: row.get(8),
            peer_address: row.get(9),
        })
        .collect();

    Ok(Some(RoutingTable {
        rules,
        version: version as u64,
    }))
}

// ── Schema operations ────────────────────────────────────────────────────────

// ── Secret operations ────────────────────────────────────────────────────────

/// Insert or update an encrypted secret.
pub async fn upsert_secret(
    pool: &Pool,
    namespace: &str,
    key: &str,
    ciphertext: &[u8],
    nonce: &[u8],
) -> Result<(), Status> {
    let client = pool.get().await.internal()?;
    client
        .execute(
            "INSERT INTO wr_secrets (namespace, key, ciphertext, nonce)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (namespace, key) DO UPDATE
               SET ciphertext = EXCLUDED.ciphertext,
                   nonce = EXCLUDED.nonce,
                   updated_at = NOW()",
            &[&namespace, &key, &ciphertext, &nonce],
        )
        .await
        .internal()?;
    Ok(())
}

/// Insert an encrypted secret only if no row exists for (namespace, key).
/// Uses `ON CONFLICT DO NOTHING` so racing callers converge on a single stored
/// row; the caller must re-read (via `get_secrets`) to obtain the winning value.
pub async fn insert_secret_if_absent(
    pool: &Pool,
    namespace: &str,
    key: &str,
    ciphertext: &[u8],
    nonce: &[u8],
) -> Result<(), Status> {
    let client = pool.get().await.internal()?;
    client
        .execute(
            "INSERT INTO wr_secrets (namespace, key, ciphertext, nonce)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (namespace, key) DO NOTHING",
            &[&namespace, &key, &ciphertext, &nonce],
        )
        .await
        .internal()?;
    Ok(())
}

/// Delete a secret by (namespace, key). Returns true if a row was deleted.
pub async fn delete_secret(pool: &Pool, namespace: &str, key: &str) -> Result<bool, Status> {
    let client = pool.get().await.internal()?;
    let deleted = client
        .execute(
            "DELETE FROM wr_secrets WHERE namespace = $1 AND key = $2",
            &[&namespace, &key],
        )
        .await
        .internal()?;
    Ok(deleted > 0)
}

/// List secret metadata (namespace + key only, no values).
pub async fn list_secrets(
    pool: &Pool,
    filter: &NamespaceFilter,
) -> Result<Vec<(String, String)>, Status> {
    let client = pool.get().await.internal()?;
    let rows = if matches!(filter, NamespaceFilter::All) {
        client
            .query(
                "SELECT namespace, key FROM wr_secrets ORDER BY namespace, key",
                &[],
            )
            .await
            .internal()?
    } else {
        client
            .query(
                "SELECT namespace, key FROM wr_secrets WHERE namespace = $1 ORDER BY key",
                &[&filter.as_db_value()],
            )
            .await
            .internal()?
    };
    Ok(rows.iter().map(|r| (r.get(0), r.get(1))).collect())
}

/// Fetch encrypted secrets for specific (namespace, key) pairs.
pub async fn get_secrets(
    pool: &Pool,
    requests: &[(String, String)],
) -> Result<Vec<(String, String, Vec<u8>, Vec<u8>)>, Status> {
    if requests.is_empty() {
        return Ok(vec![]);
    }
    let client = pool.get().await.internal()?;
    let mut results = Vec::with_capacity(requests.len());
    for (namespace, key) in requests {
        let row = client
            .query_opt(
                "SELECT namespace, key, ciphertext, nonce FROM wr_secrets
                 WHERE namespace = $1 AND key = $2",
                &[namespace, key],
            )
            .await
            .internal()?;
        if let Some(row) = row {
            results.push((row.get(0), row.get(1), row.get(2), row.get(3)));
        }
    }
    Ok(results)
}

// ── Manager registration ────────────────────────────────────────────────────

/// A registered manager in the cluster.
pub struct ManagerRecord {
    pub manager_id: String,
    pub grpc_address: String,
}

/// Register (or re-register) this manager in the cluster.
pub async fn register_manager(
    pool: &Pool,
    manager_id: &str,
    grpc_address: &str,
) -> Result<(), Status> {
    let client = pool.get().await.internal()?;
    client
        .execute(
            "INSERT INTO wr_managers (manager_id, grpc_address)
             VALUES ($1, $2)
             ON CONFLICT (manager_id) DO UPDATE
               SET grpc_address = EXCLUDED.grpc_address,
                   last_heartbeat = NOW()",
            &[&manager_id, &grpc_address],
        )
        .await
        .internal()?;
    Ok(())
}

/// Remove this manager from the cluster.
pub async fn deregister_manager(pool: &Pool, manager_id: &str) -> Result<(), Status> {
    let client = pool.get().await.internal()?;
    client
        .execute(
            "DELETE FROM wr_managers WHERE manager_id = $1",
            &[&manager_id],
        )
        .await
        .internal()?;
    Ok(())
}

/// List all managers that have heartbeated within the given threshold.
pub async fn list_managers(
    pool: &Pool,
    liveness_threshold_secs: u64,
) -> Result<Vec<ManagerRecord>, Status> {
    let client = pool.get().await.internal()?;
    let threshold_secs = liveness_threshold_secs as f64;
    let rows = client
        .query(
            "SELECT manager_id, grpc_address FROM wr_managers
             WHERE last_heartbeat > NOW() - make_interval(secs => $1::double precision)
             ORDER BY manager_id",
            &[&threshold_secs],
        )
        .await
        .internal()?;
    Ok(rows
        .iter()
        .map(|r| ManagerRecord {
            manager_id: r.get(0),
            grpc_address: r.get(1),
        })
        .collect())
}

/// Install the first accepted policy only for a pristine control plane. This
/// is the explicit fresh-database bootstrap path; rollout recovery never calls it.
pub async fn bootstrap_initial_manager_policy_state(
    pool: &Pool,
    generation: u64,
    digest: &str,
) -> Result<(), Status> {
    let client = pool.get().await.internal()?;
    client
        .execute(
            "UPDATE wr_manager_rollout_guard SET accepted_generation=$1, accepted_digest=$2 WHERE singleton AND accepted_generation IS NULL AND accepted_digest IS NULL AND active_rollout_id IS NULL AND NOT EXISTS (SELECT 1 FROM wr_manager_rollouts)",
            &[&(generation as i64), &digest],
        )
        .await
        .internal()?;
    Ok(())
}

pub async fn initialize_manager_policy_state(
    pool: &Pool,
    manager_id: &str,
    generation: u64,
    digest: &str,
) -> Result<bool, Status> {
    let client = pool.get().await.internal()?;
    let row = client.query_one(
        "SELECT accepted_generation, accepted_digest, active_rollout_id FROM wr_manager_rollout_guard WHERE singleton",
        &[],
    ).await.internal()?;
    let accepted_generation: Option<i64> = row.get(0);
    let accepted_digest: Option<String> = row.get(1);
    let active: Option<uuid::Uuid> = row.get(2);
    if accepted_generation.is_some_and(|accepted| generation < accepted as u64)
        || (accepted_generation == Some(generation as i64)
            && accepted_digest.as_deref() != Some(digest))
    {
        client.execute("UPDATE wr_managers SET policy_generation=$2, policy_digest=$3, admission_state='CLOSED_MISMATCH' WHERE manager_id=$1", &[&manager_id, &(generation as i64), &digest]).await.internal()?;
        return Ok(false);
    }
    let open = active.is_none()
        && accepted_generation == Some(generation as i64)
        && accepted_digest.as_deref() == Some(digest);
    let state = if open { "OPEN" } else { "CLOSED_STARTUP" };
    client.execute("UPDATE wr_managers SET policy_generation=$2, policy_digest=$3, admission_state=$4 WHERE manager_id=$1", &[&manager_id, &(generation as i64), &digest, &state]).await.internal()?;
    Ok(open)
}

pub async fn observe_manager_rollout(
    pool: &Pool,
    manager_id: &str,
    generation: u64,
    digest: &str,
    admission: &AdmissionGate,
    lifecycle: &ManagerLifecycleState,
    policy: &wr_common::authorization_policy::ValidatedPolicy,
) -> Result<(), Status> {
    let client = pool.get().await.internal()?;
    let row = client
        .query_one(
            "SELECT g.accepted_generation, g.accepted_digest,
                r.rollout_id, r.phase, r.target_generation, r.target_policy_digest,
                m.member_role, r.lease_epoch, r.expected_target_set_hash,
                r.target_policy_validator_version, r.target_deployment_principal_uri,
                r.target_deployment_leaf_fingerprint, r.cluster_id, r.canonical_request
         FROM wr_manager_rollout_guard g
         LEFT JOIN wr_manager_rollouts r ON r.rollout_id=g.active_rollout_id
         LEFT JOIN wr_manager_rollout_members m ON m.rollout_id=r.rollout_id AND m.manager_id=$1
            AND m.member_role=CASE
                WHEN r.target_generation=$2 AND r.target_policy_digest=$3 THEN 'target'
                ELSE 'source'
            END
         WHERE g.singleton",
            &[&manager_id, &(generation as i64), &digest],
        )
        .await
        .internal()?;
    let accepted_generation: Option<i64> = row.get(0);
    let accepted_digest: Option<String> = row.get(1);
    let rollout_id: Option<uuid::Uuid> = row.get(2);

    let Some(rollout_id) = rollout_id else {
        let accepted = accepted_generation == Some(generation as i64)
            && accepted_digest.as_deref() == Some(digest);
        let mismatch = accepted_generation.is_some_and(|accepted_generation| {
            generation < accepted_generation as u64
                || (generation == accepted_generation as u64
                    && accepted_digest.as_deref() != Some(digest))
        });
        let (state, wire_state) = if accepted {
            ("OPEN", PrivilegedAdmissionState::Open)
        } else if mismatch {
            ("CLOSED_MISMATCH", PrivilegedAdmissionState::ClosedMismatch)
        } else {
            ("CLOSED_STARTUP", PrivilegedAdmissionState::ClosedStartup)
        };
        if accepted {
            admission.open();
        } else {
            admission.close();
        }
        lifecycle.update(
            wire_state,
            "",
            ManagerRolloutPhase::Unspecified as i32,
            "",
            0,
        );
        client.execute(
            "UPDATE wr_managers SET policy_generation=$2,policy_digest=$3,admission_state=$4,rollout_id=NULL,rollout_phase=NULL,rollout_lease_epoch=0,last_heartbeat=NOW() WHERE manager_id=$1",
            &[&manager_id, &(generation as i64), &digest, &state],
        ).await.internal()?;
        return Ok(());
    };

    let phase: i32 = row.get(3);
    let target_generation: i64 = row.get(4);
    let target_digest: String = row.get(5);
    let role: Option<String> = row.get(6);
    let epoch: i64 = row.get(7);
    let expected_set_hash: String = row.get(8);
    let validator_version: i32 = row.get(9);
    let target_principal: String = row.get(10);
    let target_fingerprint: String = row.get(11);
    let cluster_id: String = row.get(12);
    let canonical_request: Vec<u8> = row.get(13);
    let request = BeginManagerRolloutRequest::decode(canonical_request.as_slice())
        .map_err(|_| Status::internal("stored manager rollout request is corrupt"))?;
    let targets = request
        .expected_targets
        .iter()
        .map(|target| wr_common::authorization_policy::RolloutTarget {
            manager_id: target.manager_id.clone(),
            endpoint: target.endpoint.clone(),
        })
        .collect::<Vec<_>>();
    let receipt_matches = policy
        .prevalidate_rollout_claims(&target_principal, &target_fingerprint, &targets)
        .is_ok_and(|receipt| {
            validator_version == receipt.validator_version as i32
                && generation == receipt.generation
                && digest == receipt.digest
                && cluster_id == receipt.cluster_id
                && expected_set_hash == receipt.target_set_hash
        });
    let target_matches =
        generation == target_generation as u64 && digest == target_digest && receipt_matches;
    let (state, wire_state, should_open) =
        match (ManagerRolloutPhase::try_from(phase).ok(), role.as_deref()) {
            (
                Some(ManagerRolloutPhase::Prepared | ManagerRolloutPhase::Staging),
                Some("source"),
            ) => ("OPEN", PrivilegedAdmissionState::Open, true),
            (
                Some(ManagerRolloutPhase::ActivatingTarget | ManagerRolloutPhase::Completed),
                Some("target"),
            ) if target_matches => ("OPEN", PrivilegedAdmissionState::Open, true),
            (_, Some("target")) if !target_matches => (
                "CLOSED_MISMATCH",
                PrivilegedAdmissionState::ClosedMismatch,
                false,
            ),
            (_, Some("source" | "target")) => (
                "CLOSED_ROLLOUT",
                PrivilegedAdmissionState::ClosedRollout,
                false,
            ),
            _ => (
                "CLOSED_ROLLOUT",
                PrivilegedAdmissionState::ClosedRollout,
                false,
            ),
        };
    if should_open {
        admission.open();
    } else {
        admission.close();
    }
    lifecycle.update(
        wire_state,
        rollout_id.to_string(),
        phase,
        expected_set_hash,
        epoch as u64,
    );
    client.execute(
        "UPDATE wr_managers SET policy_generation=$2,policy_digest=$3,admission_state=$4,rollout_id=$5,rollout_phase=$6,rollout_lease_epoch=$7,last_heartbeat=NOW() WHERE manager_id=$1",
        &[&manager_id, &(generation as i64), &digest, &state, &rollout_id, &phase, &epoch],
    ).await.internal()?;
    let process_state = if target_matches || role.as_deref() == Some("source") {
        "READY"
    } else {
        "MISMATCH"
    };
    client.execute(
        "UPDATE wr_manager_rollout_members SET observed_policy_generation=$3,observed_policy_digest=$4,process_state=$5,admission_state=$6,last_acknowledged_at=NOW()
         WHERE rollout_id=$1 AND manager_id=$2 AND member_role=$7",
        &[
            &rollout_id,
            &manager_id,
            &(generation as i64),
            &digest,
            &process_state,
            &state,
            &role,
        ],
    ).await.internal()?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn run_manager_rollout_observer_owned(
    pool: Pool,
    manager_id: String,
    generation: u64,
    digest: String,
    admission: AdmissionGate,
    lifecycle: ManagerLifecycleState,
    policy: std::sync::Arc<wr_common::authorization_policy::ValidatedPolicy>,
    mut cancellation: TaskCancellation,
) -> anyhow::Result<TaskExit> {
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(2));
    loop {
        tokio::select! { _ = cancellation.cancelled() => return Ok(TaskExit::Cancelled), _ = ticker.tick() => {} }
        observe_manager_rollout(
            &pool,
            &manager_id,
            generation,
            &digest,
            &admission,
            &lifecycle,
            &policy,
        )
        .await
        .map_err(|error| anyhow::anyhow!("manager rollout observer failed: {error}"))?;
    }
}

/// Update this manager's heartbeat timestamp.
pub async fn heartbeat_manager(pool: &Pool, manager_id: &str) -> Result<(), Status> {
    let client = pool.get().await.internal()?;
    client
        .execute(
            "UPDATE wr_managers SET last_heartbeat = NOW() WHERE manager_id = $1",
            &[&manager_id],
        )
        .await
        .internal()?;
    Ok(())
}

/// Renew this manager's lease independently of cluster-wide stale-row cleanup.
pub async fn run_manager_heartbeat_owned(
    pool: Pool,
    manager_id: String,
    interval: std::time::Duration,
    admission: AdmissionGate,
    mut cancellation: TaskCancellation,
) -> anyhow::Result<TaskExit> {
    let mut ticker = tokio::time::interval(interval);
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return Ok(TaskExit::Cancelled),
            _ = ticker.tick() => {}
        }
        // Lease freshness is lifecycle evidence and continues while privileged
        // admission is deliberately closed for startup or rollout.
        let _ = &admission;
        tokio::select! {
            _ = cancellation.cancelled() => return Ok(TaskExit::Cancelled),
            result = heartbeat_manager(&pool, &manager_id) => {
                result.map_err(|error| anyhow::anyhow!("manager heartbeat failed: {error}"))?;
            }
        }
    }
}

/// Reap long-stale manager rows on an independently owned, slower cadence.
pub async fn run_stale_manager_reaper_owned(
    pool: Pool,
    stale_threshold_secs: u64,
    interval: std::time::Duration,
    mut cancellation: TaskCancellation,
) -> anyhow::Result<TaskExit> {
    let mut ticker = tokio::time::interval(interval);
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return Ok(TaskExit::Cancelled),
            _ = ticker.tick() => {}
        }
        tokio::select! {
            _ = cancellation.cancelled() => return Ok(TaskExit::Cancelled),
            result = cleanup_stale_managers(&pool, stale_threshold_secs) => {
                result.map_err(|error| anyhow::anyhow!("stale manager cleanup failed: {error}"))?;
            }
        }
    }
}

/// Remove managers that haven't heartbeated within the threshold. Returns count deleted.
pub async fn cleanup_stale_managers(pool: &Pool, stale_threshold_secs: u64) -> Result<u64, Status> {
    let client = pool.get().await.internal()?;
    let deleted = client
        .execute(
            "DELETE FROM wr_managers WHERE last_heartbeat < NOW() - make_interval(secs => $1::double precision)",
            &[&(stale_threshold_secs as f64)],
        )
        .await
        .internal()?;
    Ok(deleted)
}

/// Get a schema by (namespace, module, version).
pub async fn get_schema(
    pool: &Pool,
    namespace: &str,
    module: &str,
    version: &str,
) -> Result<Vec<u8>, Status> {
    let client = pool.get().await.internal()?;
    let row = client
        .query_opt(
            "SELECT proto_schema FROM wr_schemas
             WHERE namespace = $1 AND module_name = $2 AND version = $3",
            &[&namespace, &module, &version],
        )
        .await
        .internal()?
        .ok_or_else(|| {
            Status::not_found(format!("no schema for {namespace}/{module}/{version}"))
        })?;
    Ok(row.get(0))
}

// ── Schedule operations ────────────────────────────────────────────────────

const SCHEDULE_COLUMNS: &str = "schedule_id, worker_namespace, worker_name, worker_version, \
    job_type, interval_secs, immediate, payload, timeout_secs, max_attempts, enabled, \
    last_fired_at, next_fire_at, last_error, consecutive_failures, claim_id::text AS claim_id";

pub struct ScheduleRow {
    pub schedule_id: String,
    pub worker_namespace: String,
    pub worker_name: String,
    pub worker_version: String,
    pub job_type: String,
    pub interval_secs: i32,
    pub immediate: bool,
    pub payload: Vec<u8>,
    pub timeout_secs: i32,
    pub max_attempts: i32,
    pub enabled: bool,
    pub last_fired_at: Option<chrono::DateTime<chrono::Utc>>,
    pub next_fire_at: Option<chrono::DateTime<chrono::Utc>>,
    pub last_error: Option<String>,
    pub consecutive_failures: i32,
    pub claim_id: Option<String>,
}

fn row_to_schedule(row: &tokio_postgres::Row) -> ScheduleRow {
    ScheduleRow {
        schedule_id: row.get(0),
        worker_namespace: row.get(1),
        worker_name: row.get(2),
        worker_version: row.get(3),
        job_type: row.get(4),
        interval_secs: row.get(5),
        immediate: row.get(6),
        payload: row.get(7),
        timeout_secs: row.get(8),
        max_attempts: row.get(9),
        enabled: row.get(10),
        last_fired_at: row.get(11),
        next_fire_at: row.get(12),
        last_error: row.get(13),
        consecutive_failures: row.get(14),
        claim_id: row.get(15),
    }
}

/// Upsert a schedule by its natural key (namespace, name, version, job_type).
/// Returns the schedule_id.
#[allow(clippy::too_many_arguments)]
pub async fn upsert_schedule(
    pool: &Pool,
    worker_namespace: &str,
    worker_name: &str,
    worker_version: &str,
    job_type: &str,
    interval_secs: u32,
    immediate: bool,
    payload: &[u8],
    timeout_secs: u32,
    max_attempts: u32,
) -> Result<String, Status> {
    let interval_secs = i32::try_from(interval_secs)
        .map_err(|_| Status::invalid_argument("interval_secs exceeds PostgreSQL INT"))?;
    let timeout_secs = i32::try_from(timeout_secs)
        .map_err(|_| Status::invalid_argument("timeout_secs exceeds PostgreSQL INT"))?;
    let max_attempts = i32::try_from(max_attempts)
        .map_err(|_| Status::invalid_argument("max_attempts exceeds PostgreSQL INT"))?;
    let client = pool.get().await.internal()?;
    let row = client
        .query_one(
            "INSERT INTO wr_schedules
                (worker_namespace, worker_name, worker_version, job_type,
                 interval_secs, immediate, payload, timeout_secs, max_attempts, enabled, next_fire_at)
             VALUES ($1, $2, $3, $4, $5::int, $6, $7, $8, $9, TRUE,
                 CASE WHEN $6 THEN NOW()
                      ELSE NOW() + make_interval(secs => $5::double precision) END)
             ON CONFLICT (worker_namespace, worker_name, worker_version, job_type)
             DO UPDATE SET
                interval_secs = EXCLUDED.interval_secs,
                immediate     = EXCLUDED.immediate,
                payload       = EXCLUDED.payload,
                timeout_secs  = EXCLUDED.timeout_secs,
                max_attempts  = EXCLUDED.max_attempts,
                enabled       = TRUE,
                next_fire_at  = CASE
                    WHEN wr_schedules.last_fired_at IS NULL AND EXCLUDED.immediate THEN NOW()
                    WHEN wr_schedules.last_fired_at IS NULL
                        THEN NOW() + make_interval(secs => EXCLUDED.interval_secs::double precision)
                    ELSE wr_schedules.last_fired_at + make_interval(secs => EXCLUDED.interval_secs::double precision)
                  END,
                claimed_by    = NULL,
                claimed_until = NULL,
                claim_id      = NULL,
                updated_at    = NOW()
             RETURNING schedule_id",
            &[
                &worker_namespace,
                &worker_name,
                &worker_version,
                &job_type,
                &interval_secs,
                &immediate,
                &payload,
                &timeout_secs,
                &max_attempts,
            ],
        )
        .await
        .internal()?;
    Ok(row.get(0))
}

/// Delete a schedule by natural key. Returns true if a row was deleted.
pub async fn delete_schedule(
    pool: &Pool,
    worker_namespace: &str,
    worker_name: &str,
    worker_version: &str,
    job_type: &str,
) -> Result<bool, Status> {
    let client = pool.get().await.internal()?;
    let deleted = client
        .execute(
            "DELETE FROM wr_schedules
             WHERE worker_namespace = $1 AND worker_name = $2
               AND worker_version = $3 AND job_type = $4",
            &[&worker_namespace, &worker_name, &worker_version, &job_type],
        )
        .await
        .internal()?;
    Ok(deleted > 0)
}

/// List schedules, optionally filtered by namespace. Empty namespace returns all.
pub async fn list_schedules(
    pool: &Pool,
    filter: &NamespaceFilter,
) -> Result<Vec<ScheduleRow>, Status> {
    let client = pool.get().await.internal()?;
    let rows = if matches!(filter, NamespaceFilter::All) {
        let sql = format!(
            "SELECT {SCHEDULE_COLUMNS} FROM wr_schedules
             ORDER BY worker_namespace, worker_name, job_type"
        );
        client.query(&sql, &[]).await.internal()?
    } else {
        let sql = format!(
            "SELECT {SCHEDULE_COLUMNS} FROM wr_schedules
             WHERE worker_namespace = $1
             ORDER BY worker_name, job_type"
        );
        client
            .query(&sql, &[&filter.as_db_value()])
            .await
            .internal()?
    };
    Ok(rows.iter().map(row_to_schedule).collect())
}

/// Claim due, unleased (or lease-expired) schedules atomically, stamping a fresh
/// lease + fencing `claim_id`. Runs in a short transaction the caller commits
/// immediately. `SKIP LOCKED` keeps concurrent managers from double-claiming.
pub async fn claim_due_schedules(
    txn: &deadpool_postgres::Transaction<'_>,
    claimer_id: &str,
    lease_secs: f64,
) -> Result<Vec<ScheduleRow>, Status> {
    let sql = format!(
        "UPDATE wr_schedules
         SET claimed_by      = $1,
             claimed_until   = NOW() + make_interval(secs => $2::double precision),
             claim_id        = gen_random_uuid(),
             last_attempt_at = NOW()
         WHERE schedule_id IN (
             SELECT schedule_id FROM wr_schedules
             WHERE enabled = TRUE
               AND next_fire_at <= NOW()
               AND (claimed_until IS NULL OR claimed_until < NOW())
             FOR UPDATE SKIP LOCKED
         )
         RETURNING {SCHEDULE_COLUMNS}"
    );
    let rows = txn
        .query(&sql, &[&claimer_id, &lease_secs])
        .await
        .internal()?;
    Ok(rows.iter().map(row_to_schedule).collect())
}

/// Fenced success finalize. Advances next_fire_at by one interval, clears the
/// lease/error/claim. The `claim_id` guard drops stale (reclaimed) attempts.
/// Returns rows affected (0 == fenced out).
pub async fn mark_schedule_succeeded(
    pool: &Pool,
    schedule_id: &str,
    claim_id: &str,
) -> Result<u64, Status> {
    let client = pool.get().await.internal()?;
    let n = client
        .execute(
            "UPDATE wr_schedules
             SET last_fired_at        = NOW(),
                 next_fire_at         = NOW() + make_interval(secs => interval_secs::double precision),
                 last_error           = NULL,
                 consecutive_failures = 0,
                 claimed_by           = NULL,
                 claimed_until        = NULL,
                 claim_id             = NULL,
                 updated_at           = NOW()
             WHERE schedule_id = $1 AND claim_id::text = $2",
            &[&schedule_id, &claim_id],
        )
        .await
        .internal()?;
    Ok(n)
}

/// Fenced failure finalize. Records the error, bumps consecutive_failures, sets a
/// backed-off next_fire_at, clears the lease/claim. `claim_id` guard as above.
/// Returns rows affected (0 == fenced out).
pub async fn mark_schedule_failed(
    pool: &Pool,
    schedule_id: &str,
    claim_id: &str,
    error: &str,
    backoff_secs: f64,
) -> Result<u64, Status> {
    let client = pool.get().await.internal()?;
    let n = client
        .execute(
            "UPDATE wr_schedules
             SET last_error           = $3,
                 consecutive_failures = consecutive_failures + 1,
                 next_fire_at         = NOW() + make_interval(secs => $4::double precision),
                 claimed_by           = NULL,
                 claimed_until        = NULL,
                 claim_id             = NULL,
                 updated_at           = NOW()
             WHERE schedule_id = $1 AND claim_id::text = $2",
            &[&schedule_id, &claim_id, &error, &backoff_secs],
        )
        .await
        .internal()?;
    Ok(n)
}

/// Atomically create or recover a manager rollout after a lost response.
pub async fn begin_manager_rollout(
    pool: &Pool,
    deployment_principal_uri: &str,
    deployment_leaf_fingerprint: &str,
    request: &BeginManagerRolloutRequest,
    canonical_request_digest: &str,
    privileged_admission_open: bool,
) -> Result<ManagerRollout, Status> {
    let client_operation_id = uuid::Uuid::parse_str(&request.client_operation_id)
        .map_err(|_| Status::invalid_argument("client_operation_id must be a UUID"))?;
    let mut client = pool.get().await.internal()?;
    let transaction = client.transaction().await.internal()?;
    if let Some(row) = transaction
        .query_opt(
            "SELECT rollout_id, deployment_leaf_fingerprint, canonical_request_digest, canonical_request, phase
             FROM wr_manager_rollouts
             WHERE deployment_principal_uri = $1 AND client_operation_id = $2
             FOR UPDATE",
            &[&deployment_principal_uri, &client_operation_id],
        )
        .await
        .internal()?
    {
        let _original_fingerprint: String = row.get(1);
        let stored_digest: String = row.get(2);
        if stored_digest != canonical_request_digest {
            return Err(Status::already_exists(
                "client_operation_id was already used with different rollout content",
            ));
        }
        let stored: Vec<u8> = row.get(3);
        let stored_request = BeginManagerRolloutRequest::decode(stored.as_slice())
            .map_err(|_| Status::internal("stored manager rollout request is corrupt"))?;
        let rollout = manager_rollout_from_row(
            row.get(0),
            deployment_principal_uri,
            &stored_request,
            stored_digest,
            row.get(4),
        );
        transaction.commit().await.internal()?;
        return Ok(rollout);
    }

    let guard = transaction.query_one(
        "SELECT accepted_generation, accepted_digest, active_rollout_id FROM wr_manager_rollout_guard WHERE singleton FOR UPDATE",
        &[],
    ).await.internal()?;
    let accepted_generation: Option<i64> = guard.get(0);
    let accepted_digest: Option<String> = guard.get(1);
    let active: Option<uuid::Uuid> = guard.get(2);
    let recovery_of = if request.recovery_of.is_empty() {
        None
    } else {
        Some(
            uuid::Uuid::parse_str(&request.recovery_of)
                .map_err(|_| Status::invalid_argument("recovery_of must be a rollout UUID"))?,
        )
    };
    if !privileged_admission_open && accepted_generation.is_some() && recovery_of.is_none() {
        return Err(Status::failed_precondition(
            "closed privileged admission permits only empty-cluster bootstrap or failed-closed recovery",
        ));
    }
    match (active, recovery_of) {
        (None, None) => {}
        (Some(active), Some(predecessor)) if active == predecessor => {
            let predecessor = transaction
                .query_one(
                    "SELECT phase, deployment_principal_uri, target_generation
                 FROM wr_manager_rollouts WHERE rollout_id=$1 FOR UPDATE",
                    &[&predecessor],
                )
                .await
                .internal()?;
            let predecessor_phase: i32 = predecessor.get(0);
            let predecessor_principal: String = predecessor.get(1);
            let predecessor_generation: i64 = predecessor.get(2);
            if predecessor_phase != ManagerRolloutPhase::FailedClosed as i32
                || predecessor_principal != deployment_principal_uri
                || request.target_generation <= predecessor_generation as u64
            {
                return Err(Status::failed_precondition(
                    "failed-closed recovery requires the active predecessor, its recorded principal, and a strictly newer generation",
                ));
            }
        }
        (Some(_), None) => {
            return Err(Status::failed_precondition(
                "another manager rollout is active",
            ))
        }
        _ => {
            return Err(Status::failed_precondition(
                "recovery_of must name the active failed-closed rollout",
            ))
        }
    }
    let live_sources = transaction
        .query(
            "SELECT manager_id, grpc_address FROM wr_managers
             WHERE admission_state='OPEN' AND last_heartbeat > NOW() - INTERVAL '30 seconds'
             ORDER BY manager_id FOR UPDATE",
            &[],
        )
        .await
        .internal()?;
    let declared_sources = request
        .source_managers
        .iter()
        .map(|source| (&source.manager_id, &source.endpoint))
        .collect::<Vec<_>>();
    let observed_sources = live_sources
        .iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect::<Vec<_>>();
    if declared_sources.len() != observed_sources.len()
        || declared_sources.iter().zip(&observed_sources).any(
            |((declared_id, declared_endpoint), (observed_id, observed_endpoint))| {
                declared_id.as_str() != observed_id
                    || declared_endpoint.as_str() != observed_endpoint
            },
        )
    {
        return Err(Status::failed_precondition(
            "manifest source manager set does not match the complete live source set",
        ));
    }
    if accepted_generation.is_some_and(|generation| request.target_generation < generation as u64) {
        return Err(Status::failed_precondition(
            "target policy generation must be strictly newer",
        ));
    }
    if accepted_generation == Some(request.target_generation as i64)
        && accepted_digest.as_deref() != Some(&request.target_policy_digest)
    {
        return Err(Status::failed_precondition(
            "same policy generation has a different digest",
        ));
    }
    if accepted_generation.is_none() {
        let fresh = transaction
            .query(
                "SELECT manager_id, policy_generation, policy_digest, admission_state
             FROM wr_managers WHERE last_heartbeat > NOW() - INTERVAL '30 seconds'
             ORDER BY manager_id FOR UPDATE",
                &[],
            )
            .await
            .internal()?;
        let expected = request
            .expected_targets
            .iter()
            .map(|target| target.manager_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let observed = fresh
            .iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<std::collections::BTreeSet<_>>();
        let matching = !fresh.is_empty()
            && fresh.iter().all(|row| {
                row.get::<_, Option<i64>>(1) == Some(request.target_generation as i64)
                    && row.get::<_, Option<String>>(2).as_deref()
                        == Some(&request.target_policy_digest)
                    && row.get::<_, String>(3) == "CLOSED_STARTUP"
            });
        if !matching
            || observed
                .iter()
                .map(String::as_str)
                .collect::<std::collections::BTreeSet<_>>()
                != expected
        {
            return Err(Status::failed_precondition(
                "empty-cluster rollout requires the complete matching CLOSED_STARTUP manager set",
            ));
        }
    }

    let rollout_id = uuid::Uuid::new_v4();
    let canonical_request = request.encode_to_vec();
    let expected_target_set_hash = manager_target_set_hash(&request.expected_targets);
    transaction
        .execute(
            "INSERT INTO wr_manager_rollouts
             (rollout_id, deployment_principal_uri, deployment_leaf_fingerprint,
              client_operation_id, canonical_request_digest, canonical_request,
              target_policy_validator_version, target_deployment_principal_uri,
              target_deployment_leaf_fingerprint, expected_target_set_hash, cluster_id,
              target_generation, target_policy_digest, recovery_of)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
            &[
                &rollout_id,
                &deployment_principal_uri,
                &deployment_leaf_fingerprint,
                &client_operation_id,
                &canonical_request_digest,
                &canonical_request,
                &(request.target_policy_validator_version as i32),
                &request.target_deployment_principal_uri,
                &request.target_deployment_leaf_fingerprint,
                &expected_target_set_hash,
                &request.cluster_id,
                &(request.target_generation as i64),
                &request.target_policy_digest,
                &recovery_of,
            ],
        )
        .await
        .internal()?;
    transaction
        .execute(
            "UPDATE wr_manager_rollout_guard SET active_rollout_id = $1 WHERE singleton",
            &[&rollout_id],
        )
        .await
        .internal()?;
    transaction.execute(
        "INSERT INTO wr_manager_rollout_members (rollout_id, member_role, manager_id, observed_policy_generation, observed_policy_digest, admission_state)
         SELECT $1, 'source', manager_id, policy_generation, policy_digest, admission_state
         FROM wr_managers
         WHERE admission_state='OPEN' AND last_heartbeat > NOW() - INTERVAL '30 seconds'
         ON CONFLICT DO NOTHING",
        &[&rollout_id],
    ).await.internal()?;
    for target in &request.expected_targets {
        transaction
            .execute(
                "INSERT INTO wr_manager_rollout_members
             (rollout_id, member_role, manager_id, expected_host_digest, expected_config_digest,
              expected_backend, expected_executable_digest, expected_backend_spec_digest,
              expected_credential_digest, expected_old_selector_digest, expected_new_selector_digest)
             VALUES ($1, 'target', $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                &[
                    &rollout_id,
                    &target.manager_id,
                    &target.host_digest,
                    &target.config_digest,
                    &target.backend,
                    &target.executable_digest,
                    &target.backend_spec_digest,
                    &target.credential_digest,
                    &target.old_selector_digest,
                    &target.new_selector_digest,
                ],
            )
            .await
            .internal()?;
    }
    transaction.execute(
        "INSERT INTO wr_manager_rollout_events (rollout_id, event_type, phase) VALUES ($1, 'created', $2)",
        &[&rollout_id, &(ManagerRolloutPhase::Prepared as i32)],
    ).await.internal()?;
    transaction.commit().await.internal()?;
    Ok(manager_rollout_from_row(
        rollout_id,
        deployment_principal_uri,
        request,
        canonical_request_digest.to_string(),
        ManagerRolloutPhase::Prepared as i32,
    ))
}

pub async fn lease_manager_rollout(
    pool: &Pool,
    rollout_id: &str,
    executor_id: &str,
    expected_lease_epoch: u64,
) -> Result<ManagerRollout, Status> {
    let rollout_id = uuid::Uuid::parse_str(rollout_id)
        .map_err(|_| Status::invalid_argument("rollout_id must be a UUID"))?;
    let executor = uuid::Uuid::parse_str(executor_id)
        .map_err(|_| Status::invalid_argument("executor_id must be a UUID"))?;
    let mut client = pool.get().await.internal()?;
    let transaction = client.transaction().await.internal()?;
    let row = transaction.query_opt(
        "SELECT executor_id, lease_epoch, lease_expires_at, phase FROM wr_manager_rollouts WHERE rollout_id=$1 FOR UPDATE",
        &[&rollout_id],
    ).await.internal()?.ok_or_else(|| Status::not_found("manager rollout was not found"))?;
    let owner: Option<uuid::Uuid> = row.get(0);
    let epoch: i64 = row.get(1);
    let expires: Option<chrono::DateTime<chrono::Utc>> = row.get(2);
    let phase: i32 = row.get(3);
    if matches!(
        ManagerRolloutPhase::try_from(phase),
        Ok(ManagerRolloutPhase::Completed
            | ManagerRolloutPhase::FailedPreClose
            | ManagerRolloutPhase::FailedClosed)
    ) {
        return Err(Status::failed_precondition(
            "terminal rollout cannot be leased",
        ));
    }
    if expected_lease_epoch != epoch as u64 {
        return Err(Status::failed_precondition("stale rollout lease epoch"));
    }
    let expired = expires.is_none_or(|deadline| deadline <= chrono::Utc::now());
    let next_epoch = match owner {
        Some(owner) if owner == executor => epoch.max(1),
        Some(_) if !expired => {
            return Err(Status::failed_precondition(
                "rollout lease is owned by another executor",
            ))
        }
        _ => epoch
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("rollout lease epoch exhausted"))?,
    };
    transaction
        .execute(
            "UPDATE wr_manager_rollouts SET executor_id=$2, lease_epoch=$3,
         lease_expires_at=NOW()+INTERVAL '30 seconds', updated_at=NOW() WHERE rollout_id=$1",
            &[&rollout_id, &executor, &next_epoch],
        )
        .await
        .internal()?;
    transaction.commit().await.internal()?;
    get_manager_rollout(pool, &rollout_id.to_string()).await
}

pub async fn advance_manager_rollout(
    pool: &Pool,
    rollout_id: &str,
    executor_id: &str,
    lease_epoch: u64,
    expected_phase: i32,
    next_phase: i32,
    member_outcomes: &[wr_common::wruntime::ManagerRolloutMemberOutcome],
) -> Result<ManagerRollout, Status> {
    let rollout_id = uuid::Uuid::parse_str(rollout_id)
        .map_err(|_| Status::invalid_argument("rollout_id must be a UUID"))?;
    let executor = uuid::Uuid::parse_str(executor_id)
        .map_err(|_| Status::invalid_argument("executor_id must be a UUID"))?;
    let expected = ManagerRolloutPhase::try_from(expected_phase)
        .map_err(|_| Status::invalid_argument("expected rollout phase is invalid"))?;
    let next = ManagerRolloutPhase::try_from(next_phase)
        .map_err(|_| Status::invalid_argument("next rollout phase is invalid"))?;
    let legal = matches!(
        (expected, next),
        (
            ManagerRolloutPhase::Prepared,
            ManagerRolloutPhase::Staging | ManagerRolloutPhase::FailedPreClose
        ) | (
            ManagerRolloutPhase::Staging,
            ManagerRolloutPhase::ClosingOld | ManagerRolloutPhase::FailedPreClose
        ) | (
            ManagerRolloutPhase::ClosingOld,
            ManagerRolloutPhase::OldClosed | ManagerRolloutPhase::FailedClosed
        ) | (
            ManagerRolloutPhase::OldClosed,
            ManagerRolloutPhase::StartingTarget | ManagerRolloutPhase::FailedClosed
        ) | (
            ManagerRolloutPhase::StartingTarget,
            ManagerRolloutPhase::TargetReadyClosed | ManagerRolloutPhase::FailedClosed
        ) | (
            ManagerRolloutPhase::TargetReadyClosed,
            ManagerRolloutPhase::ActivatingTarget | ManagerRolloutPhase::FailedClosed
        ) | (
            ManagerRolloutPhase::ActivatingTarget,
            ManagerRolloutPhase::Completed | ManagerRolloutPhase::FailedClosed
        )
    );
    if !legal {
        return Err(Status::failed_precondition(
            "illegal manager rollout phase transition",
        ));
    }
    let mut client = pool.get().await.internal()?;
    let transaction = client.transaction().await.internal()?;
    let row = transaction.query_opt(
        "SELECT phase, executor_id, lease_epoch, lease_expires_at, target_generation, target_policy_digest
         FROM wr_manager_rollouts WHERE rollout_id=$1 FOR UPDATE",
        &[&rollout_id],
    ).await.internal()?.ok_or_else(|| Status::not_found("manager rollout was not found"))?;
    let current: i32 = row.get(0);
    let owner: Option<uuid::Uuid> = row.get(1);
    let epoch: i64 = row.get(2);
    let expires: Option<chrono::DateTime<chrono::Utc>> = row.get(3);
    if owner != Some(executor)
        || epoch as u64 != lease_epoch
        || expires.is_none_or(|deadline| deadline <= chrono::Utc::now())
    {
        return Err(Status::failed_precondition(
            "rollout fenced lease no longer matches",
        ));
    }
    if current != expected_phase && current != next_phase {
        return Err(Status::failed_precondition(
            "rollout phase no longer matches",
        ));
    }
    let mut outcome_keys = std::collections::BTreeSet::new();
    for outcome in member_outcomes {
        wr_common::identity::ManagerId::parse(&outcome.manager_id)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if !matches!(outcome.member_role.as_str(), "source" | "target")
            || outcome.host_action_outcome.is_empty()
            || outcome.host_action_outcome.len() > 64
            || outcome.error.len() > 1024
            || !outcome_keys.insert((&outcome.member_role, &outcome.manager_id))
        {
            return Err(Status::invalid_argument(
                "manager rollout member outcome is invalid or duplicated",
            ));
        }
        let member = transaction
            .query_opt(
                "SELECT host_action_outcome, error FROM wr_manager_rollout_members
             WHERE rollout_id=$1 AND member_role=$2 AND manager_id=$3 FOR UPDATE",
                &[&rollout_id, &outcome.member_role, &outcome.manager_id],
            )
            .await
            .internal()?
            .ok_or_else(|| {
                Status::invalid_argument("manager rollout outcome names an unexpected member")
            })?;
        let stored_outcome: Option<String> = member.get(0);
        let stored_error: Option<String> = member.get(1);
        if let Some(stored_outcome) = stored_outcome {
            if stored_outcome != outcome.host_action_outcome
                || stored_error.as_deref().unwrap_or_default() != outcome.error
            {
                return Err(Status::already_exists(
                    "conflicting manager rollout member outcome replay",
                ));
            }
        } else if current == next_phase {
            return Err(Status::already_exists(
                "phase replay adds an uncommitted member outcome",
            ));
        } else {
            let admission_override = if outcome.member_role == "source"
                && matches!(
                    outcome.host_action_outcome.as_str(),
                    "STOPPED" | "UNREACHABLE"
                ) {
                Some(outcome.host_action_outcome.as_str())
            } else {
                None
            };
            transaction.execute(
                "UPDATE wr_manager_rollout_members
                 SET host_action_outcome=$4,error=NULLIF($5,''),admission_state=COALESCE($6,admission_state),last_acknowledged_at=NOW()
                 WHERE rollout_id=$1 AND member_role=$2 AND manager_id=$3",
                &[&rollout_id, &outcome.member_role, &outcome.manager_id, &outcome.host_action_outcome, &outcome.error, &admission_override],
            ).await.internal()?;
        }
    }
    if current == next_phase {
        transaction.commit().await.internal()?;
        return get_manager_rollout(pool, &rollout_id.to_string()).await;
    }
    let barrier_sql = match next {
        ManagerRolloutPhase::OldClosed => Some("SELECT COUNT(*) FROM wr_manager_rollout_members WHERE rollout_id=$1 AND member_role='source' AND COALESCE(admission_state,'') NOT IN ('CLOSED_ROLLOUT','STOPPED','UNREACHABLE')"),
        ManagerRolloutPhase::TargetReadyClosed => Some("SELECT
            (SELECT COUNT(*) FROM wr_manager_rollout_members m JOIN wr_manager_rollouts r USING (rollout_id)
             WHERE m.rollout_id=$1 AND m.member_role='target'
               AND (m.process_state!='READY' OR m.admission_state!='CLOSED_ROLLOUT'
                    OR m.observed_policy_generation!=r.target_generation
                    OR m.observed_policy_digest!=r.target_policy_digest))
            + (SELECT COUNT(*) FROM wr_managers
               WHERE last_heartbeat > NOW() - INTERVAL '30 seconds' AND admission_state='OPEN')"),
        ManagerRolloutPhase::Completed => Some("SELECT
            (SELECT COUNT(*) FROM wr_manager_rollout_members
             WHERE rollout_id=$1 AND member_role='target' AND admission_state!='OPEN')
            + (SELECT COUNT(*) FROM wr_managers live
               WHERE live.last_heartbeat > NOW() - INTERVAL '30 seconds' AND live.admission_state='OPEN'
                 AND NOT EXISTS (SELECT 1 FROM wr_manager_rollout_members target
                                 WHERE target.rollout_id=$1 AND target.member_role='target'
                                   AND target.manager_id=live.manager_id))"),
        _ => None,
    };
    if let Some(sql) = barrier_sql {
        let remaining: i64 = transaction
            .query_one(sql, &[&rollout_id])
            .await
            .internal()?
            .get(0);
        if remaining != 0 {
            return Err(Status::failed_precondition(
                "manager rollout barrier is not satisfied",
            ));
        }
    }
    let terminal_failure = match next {
        ManagerRolloutPhase::FailedPreClose => {
            Some("manager rollout failed before privileged admission closed")
        }
        ManagerRolloutPhase::FailedClosed => {
            Some("manager rollout failed after privileged admission closed")
        }
        _ => None,
    };
    transaction.execute(
        "UPDATE wr_manager_rollouts SET phase=$2,failure=COALESCE($3,failure),updated_at=NOW() WHERE rollout_id=$1",
        &[&rollout_id, &next_phase, &terminal_failure],
    ).await.internal()?;
    transaction.execute("INSERT INTO wr_manager_rollout_events (rollout_id,event_type,phase) VALUES ($1,'phase-advanced',$2)", &[&rollout_id, &next_phase]).await.internal()?;
    match next {
        ManagerRolloutPhase::Completed => {
            let generation: i64 = row.get(4);
            let digest: String = row.get(5);
            transaction.execute("UPDATE wr_manager_rollout_guard SET accepted_generation=$2, accepted_digest=$3, active_rollout_id=NULL WHERE singleton AND active_rollout_id=$1", &[&rollout_id, &generation, &digest]).await.internal()?;
        }
        ManagerRolloutPhase::FailedPreClose => {
            transaction.execute("UPDATE wr_manager_rollout_guard SET active_rollout_id=NULL WHERE singleton AND active_rollout_id=$1", &[&rollout_id]).await.internal()?;
        }
        _ => {}
    }
    transaction.commit().await.internal()?;
    get_manager_rollout(pool, &rollout_id.to_string()).await
}

pub async fn get_manager_rollout(pool: &Pool, rollout_id: &str) -> Result<ManagerRollout, Status> {
    let rollout_id = uuid::Uuid::parse_str(rollout_id)
        .map_err(|_| Status::invalid_argument("rollout_id must be a UUID"))?;
    let client = pool.get().await.internal()?;
    let row = client
        .query_opt(
            "SELECT deployment_principal_uri, canonical_request_digest, canonical_request, phase,
                    executor_id::text, lease_epoch, lease_expires_at, failure, expected_target_set_hash
             FROM wr_manager_rollouts WHERE rollout_id = $1",
            &[&rollout_id],
        )
        .await
        .internal()?
        .ok_or_else(|| Status::not_found("manager rollout was not found"))?;
    let principal: String = row.get(0);
    let request_bytes: Vec<u8> = row.get(2);
    let request = BeginManagerRolloutRequest::decode(request_bytes.as_slice())
        .map_err(|_| Status::internal("stored manager rollout request is corrupt"))?;
    let mut rollout =
        manager_rollout_from_row(rollout_id, &principal, &request, row.get(1), row.get(3));
    rollout.executor_id = row.get::<_, Option<String>>(4).unwrap_or_default();
    rollout.lease_epoch = row.get::<_, i64>(5) as u64;
    rollout.lease_expires_at =
        row.get::<_, Option<chrono::DateTime<chrono::Utc>>>(6)
            .map(|value| prost_types::Timestamp {
                seconds: value.timestamp(),
                nanos: value.timestamp_subsec_nanos() as i32,
            });
    rollout.failure = row.get::<_, Option<String>>(7).unwrap_or_default();
    rollout.expected_target_set_hash = row.get(8);
    Ok(rollout)
}

fn manager_rollout_from_row(
    rollout_id: uuid::Uuid,
    deployment_principal_uri: &str,
    request: &BeginManagerRolloutRequest,
    request_digest: String,
    phase: i32,
) -> ManagerRollout {
    ManagerRollout {
        rollout_id: rollout_id.to_string(),
        deployment_principal_uri: deployment_principal_uri.to_string(),
        client_operation_id: request.client_operation_id.clone(),
        request_digest,
        cluster_id: request.cluster_id.clone(),
        target_generation: request.target_generation,
        target_policy_digest: request.target_policy_digest.clone(),
        expected_targets: request.expected_targets.clone(),
        recovery_of: request.recovery_of.clone(),
        phase,
        executor_id: String::new(),
        lease_epoch: 0,
        lease_expires_at: None,
        target_policy_validator_version: request.target_policy_validator_version,
        target_deployment_principal_uri: request.target_deployment_principal_uri.clone(),
        target_deployment_leaf_fingerprint: request.target_deployment_leaf_fingerprint.clone(),
        expected_target_set_hash: manager_target_set_hash(&request.expected_targets),
        failure: String::new(),
        source_managers: request.source_managers.clone(),
        manifest_digest: request.manifest_digest.clone(),
        executor_id_from_manifest: request.executor_id.clone(),
        deployment_certificate: request.deployment_certificate.clone(),
    }
}

fn manager_target_set_hash(targets: &[wr_common::wruntime::ManagerRolloutTarget]) -> String {
    let targets = targets
        .iter()
        .map(|target| wr_common::authorization_policy::RolloutTarget {
            manager_id: target.manager_id.clone(),
            endpoint: target.endpoint.clone(),
        })
        .collect::<Vec<_>>();
    wr_common::authorization_policy::manager_set_hash(&targets)
        .expect("canonical rollout request already validated unique manager targets")
}

#[cfg(test)]
mod fence_generation_tests {
    use super::*;

    #[test]
    fn first_replay_replacement_and_exhaustion_are_checked() {
        assert_eq!(assigned_slot_generation(0, false).unwrap(), 1);
        assert_eq!(assigned_slot_generation(7, true).unwrap(), 7);
        assert_eq!(assigned_slot_generation(u64::MAX, true).unwrap(), u64::MAX);
        assert_eq!(
            assigned_slot_generation(u64::MAX, false)
                .unwrap_err()
                .code(),
            tonic::Code::ResourceExhausted
        );
    }
}
