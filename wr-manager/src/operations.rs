use deadpool_postgres::{GenericClient, Pool};
use prost::Message;
use sha2::{Digest, Sha256};
use tokio_postgres::Row;
use tonic::Status;
use uuid::Uuid;
use wr_common::agent_policy::{
    missing_capabilities, normalize_capabilities, validate_identity, validate_sha256_digest,
    AGENT_PROTOCOL_VERSION,
};
use wr_common::deployment_contract::deployment_operation_id;
use wr_common::wruntime::{
    instruction_target, operation_target_progress, AgentInstruction, BackendKind,
    BackendProcessState, BackendTerminationEvidence, ClaimNodeCleanupResponse,
    ClaimOperationResponse, DeploymentCondition, EngineSlotTargetIdentity, EngineTargetDetails,
    InstructionTarget, InstructionTargetKind, NodeAgentAttestation, NodeAgentPolicy,
    NodeCleanupAuthority, NodeCleanupInstruction, NodeCleanupResultDisposition, NodeCleanupState,
    NodeCleanupSummary, NodeOperation, NodeOperationAction, NodeOperationPhase, NodeOperationState,
    NodeOperationStepKind, NodeSlotTransitionKind, OperationEvent, OperationTargetProgress,
    ProcessLifecycleState, ProxyTargetDetails, ProxyTargetIdentity, ReportNodeCleanupResultRequest,
    ReportNodeCleanupResultResponse, ReportNodeObservationRequest, ReportStepResultRequest,
    RolloutPolicy, ServiceKind, SlotAuthorityStatus, SlotObservation, SubmitOperationRequest,
};

const LEASE_SECONDS: f64 = 15.0;
const EVIDENCE_FRESH_SECONDS: i64 = 15;

fn internal(error: impl std::fmt::Debug) -> Status {
    Status::internal(format!("database operation failed: {error:?}"))
}

pub async fn fence_cleanup_authority<C>(
    client: &C,
    node_id: &str,
    reason: &str,
) -> Result<bool, Status>
where
    C: GenericClient + Sync,
{
    let row = client
        .query_opt(
            "UPDATE wr_node_release_cleanup
             SET generation = generation + 1, state = 'needs_reconcile',
                 authority_payload = NULL, payload_digest = '', candidate_count = 0,
                 agent_instance_id = NULL, claimed_by = NULL, claim_instance = NULL,
                 lease_expires_at = NULL, delivered_at = NULL,
                 next_reconcile_at = NOW(), diagnostic_code = '', diagnostic_detail = '',
                 updated_at = NOW()
             WHERE node_id = $1
             RETURNING generation - 1 AS old_generation, generation",
            &[&node_id],
        )
        .await
        .map_err(internal)?;
    let Some(row) = row else { return Ok(false) };
    let old_generation: i64 = row.get("old_generation");
    let generation: i64 = row.get("generation");
    client
        .execute(
            "UPDATE wr_node_release_cleanup_generations
             SET outcome = 'superseded', completed_at = NOW()
             WHERE node_id = $1 AND generation = $2 AND outcome = 'materialized'",
            &[&node_id, &old_generation],
        )
        .await
        .map_err(internal)?;
    client
        .execute(
            "INSERT INTO wr_node_release_cleanup_events
               (node_id, generation, event_code, detail)
             VALUES ($1, $2, 'AUTHORITY_FENCED', $3)",
            &[&node_id, &generation, &reason],
        )
        .await
        .map_err(internal)?;
    Ok(true)
}

async fn acquire_evidence_lock<C>(client: &C) -> Result<(), Status>
where
    C: GenericClient + Sync,
{
    client
        .query_one(
            "SELECT version FROM wr_manager_lock WHERE id = 1 FOR UPDATE",
            &[],
        )
        .await
        .map_err(internal)?;
    Ok(())
}

fn timestamp(value: chrono::DateTime<chrono::Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: value.timestamp(),
        nanos: value.timestamp_subsec_nanos() as i32,
    }
}

fn action_name(action: NodeOperationAction) -> &'static str {
    match action {
        NodeOperationAction::Deployment => "deployment",
        NodeOperationAction::Restart => "restart",
        NodeOperationAction::Rollback => "rollback",
        NodeOperationAction::Unspecified => "unspecified",
    }
}

fn parse_action(value: &str) -> Result<NodeOperationAction, Status> {
    match value {
        "deployment" => Ok(NodeOperationAction::Deployment),
        "restart" => Ok(NodeOperationAction::Restart),
        "rollback" => Ok(NodeOperationAction::Rollback),
        _ => Err(Status::internal("stored operation has an invalid action")),
    }
}

fn parse_state(value: &str) -> Result<NodeOperationState, Status> {
    match value {
        "queued" => Ok(NodeOperationState::Queued),
        "running" => Ok(NodeOperationState::Running),
        "paused" => Ok(NodeOperationState::Paused),
        "succeeded" => Ok(NodeOperationState::Succeeded),
        "failed" => Ok(NodeOperationState::Failed),
        "cancelled" => Ok(NodeOperationState::Cancelled),
        _ => Err(Status::internal("stored operation has an invalid state")),
    }
}

fn parse_phase(value: &str) -> Result<NodeOperationPhase, Status> {
    match value {
        "forward" => Ok(NodeOperationPhase::Forward),
        "restoring_source" => Ok(NodeOperationPhase::RestoringSource),
        "committing" => Ok(NodeOperationPhase::Committing),
        "complete" => Ok(NodeOperationPhase::Complete),
        _ => Err(Status::internal("stored operation has an invalid phase")),
    }
}

fn step_name(step: NodeOperationStepKind) -> &'static str {
    match step {
        NodeOperationStepKind::VerifyReleaseMetadata => "verify_release_metadata",
        NodeOperationStepKind::VerifyProxy => "verify_proxy",
        NodeOperationStepKind::StopBackend => "stop_backend",
        NodeOperationStepKind::SelectRelease => "select_release",
        NodeOperationStepKind::StartBackend => "start_backend",
        NodeOperationStepKind::VerifyTarget => "verify_target",
        NodeOperationStepKind::SwitchAuthority => "switch_authority",
        NodeOperationStepKind::VerifyServing => "verify_serving",
        NodeOperationStepKind::RestoreSource => "restore_source",
        NodeOperationStepKind::InspectBackend => "inspect_backend",
        NodeOperationStepKind::Unspecified => "complete",
    }
}

fn parse_step(value: &str) -> Result<NodeOperationStepKind, Status> {
    match value {
        "verify_release_metadata" => Ok(NodeOperationStepKind::VerifyReleaseMetadata),
        "verify_proxy" => Ok(NodeOperationStepKind::VerifyProxy),
        "stop_backend" => Ok(NodeOperationStepKind::StopBackend),
        "select_release" => Ok(NodeOperationStepKind::SelectRelease),
        "start_backend" => Ok(NodeOperationStepKind::StartBackend),
        "verify_target" => Ok(NodeOperationStepKind::VerifyTarget),
        "switch_authority" => Ok(NodeOperationStepKind::SwitchAuthority),
        "verify_serving" => Ok(NodeOperationStepKind::VerifyServing),
        "restore_source" => Ok(NodeOperationStepKind::RestoreSource),
        "inspect_backend" => Ok(NodeOperationStepKind::InspectBackend),
        "complete" => Ok(NodeOperationStepKind::Unspecified),
        _ => Err(Status::internal("stored operation has an invalid step")),
    }
}

fn target_step_compatible(kind: InstructionTargetKind, step: NodeOperationStepKind) -> bool {
    use NodeOperationStepKind as Step;
    match kind {
        InstructionTargetKind::Proxy => matches!(
            step,
            Step::InspectBackend
                | Step::VerifyTarget
                | Step::VerifyProxy
                | Step::StopBackend
                | Step::SelectRelease
                | Step::StartBackend
                | Step::RestoreSource
                | Step::Unspecified
        ),
        InstructionTargetKind::EngineSlot => matches!(
            step,
            Step::VerifyReleaseMetadata
                | Step::InspectBackend
                | Step::StopBackend
                | Step::SelectRelease
                | Step::StartBackend
                | Step::VerifyTarget
                | Step::SwitchAuthority
                | Step::VerifyServing
                | Step::RestoreSource
                | Step::Unspecified
        ),
        InstructionTargetKind::Unspecified | InstructionTargetKind::ReleaseCleanup => false,
    }
}

fn backend_name(value: BackendKind) -> Result<&'static str, Status> {
    match value {
        BackendKind::Systemd => Ok("systemd"),
        BackendKind::Docker => Ok("docker"),
        BackendKind::Unspecified => Err(Status::invalid_argument("backend is required")),
    }
}

fn parse_backend(value: &str) -> BackendKind {
    match value {
        "systemd" => BackendKind::Systemd,
        "docker" => BackendKind::Docker,
        _ => BackendKind::Unspecified,
    }
}

fn operation_condition(code: String, detail: String) -> DeploymentCondition {
    DeploymentCondition {
        code,
        detail,
        severity: wr_common::wruntime::StatusSeverity::Unhealthy as i32,
        affected_identity: String::new(),
        desired: String::new(),
        actual: String::new(),
    }
}

fn target_from_row(row: &Row) -> Result<OperationTargetProgress, Status> {
    let kind: String = row.get("target_kind");
    let key: String = row.get("target_key");
    let (kind, identity, details, transition) = match kind.as_str() {
        "proxy" if key == "proxy" => (
            InstructionTargetKind::Proxy,
            operation_target_progress::Identity::Proxy(Default::default()),
            operation_target_progress::Details::ProxyDetails(ProxyTargetDetails {}),
            NodeSlotTransitionKind::Unspecified,
        ),
        "engine_slot" if !key.is_empty() && key != "proxy" => (
            InstructionTargetKind::EngineSlot,
            operation_target_progress::Identity::EngineSlot(EngineSlotTargetIdentity {
                engine_slot: key,
            }),
            operation_target_progress::Details::EngineDetails(EngineTargetDetails {
                rollout_order: row
                    .get::<_, Option<i32>>("rollout_order")
                    .ok_or_else(|| Status::internal("engine target detail is missing"))?
                    as u32,
                authoritative_revision: row
                    .get::<_, Option<i64>>("authoritative_revision")
                    .ok_or_else(|| Status::internal("engine target detail is missing"))?
                    as u64,
                authority_switched: row
                    .get::<_, Option<bool>>("authority_switched")
                    .ok_or_else(|| Status::internal("engine target detail is missing"))?,
                serving_converged: row
                    .get::<_, Option<bool>>("serving_converged")
                    .ok_or_else(|| Status::internal("engine target detail is missing"))?,
            }),
            parse_transition(
                row.get::<_, Option<String>>("transition_kind")
                    .ok_or_else(|| Status::internal("engine target transition is missing"))?
                    .as_str(),
            )?
            .proto(),
        ),
        _ => {
            return Err(Status::internal(
                "stored operation target identity is invalid",
            ))
        }
    };
    let code: String = row.get("condition_code");
    let detail: String = row.get("condition_detail");
    Ok(OperationTargetProgress {
        kind: kind as i32,
        identity: Some(identity),
        next_step: parse_step(row.get::<_, String>("next_step").as_str())? as i32,
        completed_steps: row.get::<_, i32>("completed_steps") as u32,
        complete: row.get("complete"),
        conditions: if code.is_empty() {
            vec![]
        } else {
            vec![operation_condition(code, detail)]
        },
        source_revision: row.get::<_, i64>("source_revision") as u64,
        source_digest: row.get("source_digest"),
        source_resolved_release_digest: row.get("source_resolved_digest"),
        target_revision: row.get::<_, i64>("target_revision") as u64,
        target_digest: row.get("target_digest"),
        target_resolved_release_digest: row.get("target_resolved_digest"),
        pinned_backend_instance_id: row.get("pinned_backend_instance_id"),
        pinned_process_instance_id: row.get("pinned_process_instance_id"),
        changed: row.get("changed"),
        effect_delivered_at: row
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>("effect_delivered_at")
            .map(timestamp),
        effect_reported: row.get("effect_reported"),
        effect_observed_revision: row.get::<_, i64>("effect_observed_revision") as u64,
        effect_observed_digest: row.get("effect_observed_digest"),
        effect_observed_resolved_release_digest: row.get("effect_observed_resolved_digest"),
        effect_backend_instance_id: row.get("effect_backend_instance_id"),
        effect_process_instance_id: row.get("effect_process_instance_id"),
        effect_condition_code: row.get("effect_condition_code"),
        effect_detail: row.get("effect_detail"),
        effect_ambiguous: row.get("effect_ambiguous"),
        details: Some(details),
        transition: transition as i32,
        termination_evidence: decode_termination_evidence(row.get("effect_termination_evidence"))?,
    })
}

fn instruction_target_key(target: &InstructionTarget) -> Result<(&'static str, &str), Status> {
    match (
        InstructionTargetKind::try_from(target.kind).unwrap_or(InstructionTargetKind::Unspecified),
        target.identity.as_ref(),
    ) {
        (InstructionTargetKind::Proxy, Some(instruction_target::Identity::Proxy(_))) => {
            Ok(("proxy", "proxy"))
        }
        (
            InstructionTargetKind::EngineSlot,
            Some(instruction_target::Identity::EngineSlotTarget(identity)),
        ) if !identity.engine_slot.is_empty() && identity.engine_slot != "proxy" => {
            Ok(("engine_slot", &identity.engine_slot))
        }
        _ => Err(Status::invalid_argument(
            "target kind and identity mismatch",
        )),
    }
}

fn target_key(target: &OperationTargetProgress) -> Result<(&'static str, &str), Status> {
    match (
        InstructionTargetKind::try_from(target.kind).unwrap_or(InstructionTargetKind::Unspecified),
        target.identity.as_ref(),
        target.details.as_ref(),
    ) {
        (
            InstructionTargetKind::Proxy,
            Some(operation_target_progress::Identity::Proxy(_)),
            Some(operation_target_progress::Details::ProxyDetails(_)),
        ) => Ok(("proxy", "proxy")),
        (
            InstructionTargetKind::EngineSlot,
            Some(operation_target_progress::Identity::EngineSlot(identity)),
            Some(operation_target_progress::Details::EngineDetails(_)),
        ) if !identity.engine_slot.is_empty() => Ok(("engine_slot", &identity.engine_slot)),
        _ => Err(Status::internal(
            "operation target kind, identity, and details mismatch",
        )),
    }
}

fn engine_slot(target: &OperationTargetProgress) -> Result<&str, Status> {
    let (kind, key) = target_key(target)?;
    if kind != "engine_slot" {
        return Err(Status::internal("expected an engine operation target"));
    }
    Ok(key)
}

fn engine_details(target: &OperationTargetProgress) -> Result<&EngineTargetDetails, Status> {
    match target.details.as_ref() {
        Some(operation_target_progress::Details::EngineDetails(details)) => Ok(details),
        _ => Err(Status::internal("engine target detail is missing")),
    }
}

fn proxy_target(operation: &NodeOperation) -> Result<&OperationTargetProgress, Status> {
    operation
        .targets
        .iter()
        .find(|target| target.kind == InstructionTargetKind::Proxy as i32)
        .ok_or_else(|| Status::internal("operation proxy target is missing"))
}

#[derive(Clone)]
struct OperationSlotProgress {
    engine_slot: String,
    transition: DerivedTransition,
    authoritative_revision: u64,
    next_step: i32,
    complete: bool,
    source_revision: u64,
    source_digest: String,
    target_revision: u64,
    target_digest: String,
    pinned_backend_instance_id: String,
    pinned_process_instance_id: String,

    effect_delivered_at: Option<prost_types::Timestamp>,
    effect_reported: bool,
    effect_backend_instance_id: String,
    effect_process_instance_id: String,
    effect_condition_code: String,
    rollout_order: u32,
    effect_ambiguous: bool,
    source_resolved_release_digest: String,
    target_resolved_release_digest: String,
}

fn operation_slots(operation: &NodeOperation) -> Result<Vec<OperationSlotProgress>, Status> {
    operation
        .targets
        .iter()
        .filter(|target| target.kind == InstructionTargetKind::EngineSlot as i32)
        .map(|target| {
            let details = engine_details(target)?;
            Ok(OperationSlotProgress {
                engine_slot: engine_slot(target)?.to_string(),
                transition: match NodeSlotTransitionKind::try_from(target.transition)
                    .unwrap_or(NodeSlotTransitionKind::Unspecified)
                {
                    NodeSlotTransitionKind::Addition => DerivedTransition::Addition,
                    NodeSlotTransitionKind::Replacement => DerivedTransition::Replacement,
                    NodeSlotTransitionKind::Unchanged => DerivedTransition::Unchanged,
                    NodeSlotTransitionKind::Removal => DerivedTransition::Removal,
                    NodeSlotTransitionKind::Restart => DerivedTransition::Restart,
                    NodeSlotTransitionKind::Unspecified => {
                        return Err(Status::internal("engine target transition is missing"))
                    }
                },
                authoritative_revision: details.authoritative_revision,
                next_step: target.next_step,
                complete: target.complete,
                source_revision: target.source_revision,
                source_digest: target.source_digest.clone(),
                target_revision: target.target_revision,
                target_digest: target.target_digest.clone(),
                pinned_backend_instance_id: target.pinned_backend_instance_id.clone(),
                pinned_process_instance_id: target.pinned_process_instance_id.clone(),

                effect_delivered_at: target.effect_delivered_at,
                effect_reported: target.effect_reported,
                effect_backend_instance_id: target.effect_backend_instance_id.clone(),
                effect_process_instance_id: target.effect_process_instance_id.clone(),
                effect_condition_code: target.effect_condition_code.clone(),
                rollout_order: details.rollout_order,
                effect_ambiguous: target.effect_ambiguous,
                source_resolved_release_digest: target.source_resolved_release_digest.clone(),
                target_resolved_release_digest: target.target_resolved_release_digest.clone(),
            })
        })
        .collect()
}

async fn load_operation<C>(client: &C, operation_id: Uuid) -> Result<NodeOperation, Status>
where
    C: GenericClient + Sync,
{
    let row = client
        .query_opt(
            "SELECT operation_id, node_id, request_token, actor, action, state, policy,
                    source_revision, target_revision, bundle_digest, resolved_release_digest,
                    target_revision_digest, committed, lease_epoch, lease_expires_at,
                    failure_code, failure_detail, created_at, updated_at, phase,
                    forward_deadline, forward_fenced, restoration_requested,
                    agent_instance_id, restoration_terminal_state
             FROM wr_node_operations WHERE operation_id = $1",
            &[&operation_id],
        )
        .await
        .map_err(internal)?
        .ok_or_else(|| Status::not_found("operation not found"))?;
    let targets = client
        .query(
            "SELECT t.target_kind, t.target_key, t.next_step, t.completed_steps, t.complete,
                    t.condition_code, t.condition_detail, t.source_revision, t.source_digest,
                    t.source_resolved_digest, t.target_revision, t.target_digest,
                    t.target_resolved_digest, t.pinned_backend_instance_id,
                    t.pinned_process_instance_id, t.changed, t.effect_ambiguous,
                    t.effect_delivered_at, t.effect_reported, t.effect_observed_revision,
                    t.effect_observed_digest, t.effect_observed_resolved_digest,
                    t.effect_backend_instance_id, t.effect_process_instance_id,
                    t.effect_condition_code, t.effect_detail, t.effect_termination_evidence,
                    d.rollout_order,
                    d.authoritative_revision, d.authority_switched, d.serving_converged,
                    d.transition_kind
             FROM wr_node_operation_targets t
             LEFT JOIN wr_node_operation_engine_target_details d
               ON (d.operation_id, d.target_kind, d.target_key) =
                  (t.operation_id, t.target_kind, t.target_key)
             WHERE t.operation_id = $1
             ORDER BY CASE t.target_kind WHEN 'proxy' THEN 0 ELSE 1 END,
                      d.rollout_order NULLS FIRST, t.target_key",
            &[&operation_id],
        )
        .await
        .map_err(internal)?;
    operation_from_row(&row, &targets)
}

fn operation_from_row(row: &Row, targets: &[Row]) -> Result<NodeOperation, Status> {
    let policy = RolloutPolicy::decode(row.get::<_, Vec<u8>>("policy").as_slice())
        .map_err(|error| Status::internal(format!("stored rollout policy is invalid: {error}")))?;
    let failure_code: String = row.get("failure_code");
    let failure_detail: String = row.get("failure_detail");
    let lease_expires_at: Option<chrono::DateTime<chrono::Utc>> = row.get("lease_expires_at");
    let created_at: chrono::DateTime<chrono::Utc> = row.get("created_at");
    let updated_at: chrono::DateTime<chrono::Utc> = row.get("updated_at");
    let forward_deadline: chrono::DateTime<chrono::Utc> = row.get("forward_deadline");
    let action = parse_action(row.get::<_, String>("action").as_str())?;
    let targets = targets
        .iter()
        .map(target_from_row)
        .collect::<Result<Vec<_>, Status>>()?;
    for target in targets
        .iter()
        .filter(|target| target.kind == InstructionTargetKind::EngineSlot as i32)
    {
        let stored = match NodeSlotTransitionKind::try_from(target.transition)
            .unwrap_or(NodeSlotTransitionKind::Unspecified)
        {
            NodeSlotTransitionKind::Addition => DerivedTransition::Addition,
            NodeSlotTransitionKind::Replacement => DerivedTransition::Replacement,
            NodeSlotTransitionKind::Unchanged => DerivedTransition::Unchanged,
            NodeSlotTransitionKind::Removal => DerivedTransition::Removal,
            NodeSlotTransitionKind::Restart => DerivedTransition::Restart,
            NodeSlotTransitionKind::Unspecified => {
                return Err(Status::internal("engine target transition is missing"))
            }
        };
        validate_stored_transition(action, stored, target)?;
    }
    Ok(NodeOperation {
        operation_id: row.get::<_, Uuid>("operation_id").to_string(),
        node_id: row.get("node_id"),
        request_token: row.get("request_token"),
        actor: row.get("actor"),
        action: action as i32,
        state: parse_state(row.get::<_, String>("state").as_str())? as i32,
        policy: Some(policy),
        source_revision: row.get::<_, i64>("source_revision") as u64,
        target_revision: row.get::<_, i64>("target_revision") as u64,
        bundle_digest: row.get("bundle_digest"),
        revision_digest: row
            .get::<_, Option<String>>("target_revision_digest")
            .unwrap_or_default(),
        targets,
        committed: row.get("committed"),
        lease_epoch: row.get::<_, i64>("lease_epoch") as u64,
        lease_expires_at: lease_expires_at.map(timestamp),
        created_at: Some(timestamp(created_at)),
        updated_at: Some(timestamp(updated_at)),
        conditions: if failure_code.is_empty() {
            vec![]
        } else {
            vec![operation_condition(failure_code, failure_detail)]
        },
        phase: parse_phase(row.get::<_, String>("phase").as_str())? as i32,
        forward_deadline: Some(timestamp(forward_deadline)),
        forward_fenced: row.get("forward_fenced"),
        restoration_requested: row.get("restoration_requested"),
        agent_instance_id: row
            .get::<_, Option<String>>("agent_instance_id")
            .unwrap_or_default(),
        restoration_terminal_state: row
            .get::<_, Option<String>>("restoration_terminal_state")
            .map(|value| parse_state(&value).map(|state| state as i32))
            .transpose()?
            .unwrap_or(NodeOperationState::Unspecified as i32),
        resolved_release_digest: row.get("resolved_release_digest"),
    })
}

fn decode_termination_evidence(
    bytes: Option<Vec<u8>>,
) -> Result<Option<BackendTerminationEvidence>, Status> {
    bytes
        .map(|bytes| BackendTerminationEvidence::decode(bytes.as_slice()))
        .transpose()
        .map_err(|error| {
            Status::internal(format!("stored termination evidence is invalid: {error}"))
        })
}

async fn append_event<C: GenericClient + Sync>(
    client: &C,
    operation_id: Uuid,
    actor: &str,
    code: &str,
    detail: &str,
    epoch: i64,
) -> Result<(), Status> {
    client
        .execute(
            "INSERT INTO wr_node_operation_events
               (operation_id, actor, event_code, detail, lease_epoch)
             VALUES ($1, $2, $3, $4, $5)",
            &[&operation_id, &actor, &code, &detail, &epoch],
        )
        .await
        .map_err(internal)?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DerivedTransition {
    Addition,
    Replacement,
    Unchanged,
    Removal,
    Restart,
}

impl DerivedTransition {
    fn name(self) -> &'static str {
        match self {
            Self::Addition => "addition",
            Self::Replacement => "replacement",
            Self::Unchanged => "unchanged",
            Self::Removal => "removal",
            Self::Restart => "restart",
        }
    }

    fn proto(self) -> NodeSlotTransitionKind {
        match self {
            Self::Addition => NodeSlotTransitionKind::Addition,
            Self::Replacement => NodeSlotTransitionKind::Replacement,
            Self::Unchanged => NodeSlotTransitionKind::Unchanged,
            Self::Removal => NodeSlotTransitionKind::Removal,
            Self::Restart => NodeSlotTransitionKind::Restart,
        }
    }
}

fn parse_transition(value: &str) -> Result<DerivedTransition, Status> {
    match value {
        "addition" => Ok(DerivedTransition::Addition),
        "replacement" => Ok(DerivedTransition::Replacement),
        "unchanged" => Ok(DerivedTransition::Unchanged),
        "removal" => Ok(DerivedTransition::Removal),
        "restart" => Ok(DerivedTransition::Restart),
        _ => Err(Status::internal(
            "stored operation target has an invalid transition kind",
        )),
    }
}

fn derive_transition(
    source_revision: i64,
    source_digest: &str,
    source_resolved_digest: &str,
    target_revision: i64,
    target_digest: &str,
    target_resolved_digest: &str,
) -> DerivedTransition {
    match (source_revision > 0, target_revision > 0) {
        (false, true) => DerivedTransition::Addition,
        (true, false) => DerivedTransition::Removal,
        (true, true)
            if source_revision == target_revision
                && source_digest == target_digest
                && source_resolved_digest == target_resolved_digest =>
        {
            DerivedTransition::Unchanged
        }
        (true, true) => DerivedTransition::Replacement,
        (false, false) => DerivedTransition::Unchanged,
    }
}

fn validate_stored_transition(
    action: NodeOperationAction,
    stored: DerivedTransition,
    target: &OperationTargetProgress,
) -> Result<(), Status> {
    let source_revision = target.source_revision as i64;
    let target_revision = target.target_revision as i64;
    let expected = match action {
        NodeOperationAction::Restart => {
            if source_revision <= 0
                || source_revision != target_revision
                || target.source_digest != target.target_digest
                || target.source_resolved_release_digest != target.target_resolved_release_digest
            {
                return Err(Status::internal(
                    "stored restart transition changes immutable desired identity",
                ));
            }
            DerivedTransition::Restart
        }
        NodeOperationAction::Deployment | NodeOperationAction::Rollback => derive_transition(
            source_revision,
            &target.source_digest,
            &target.source_resolved_release_digest,
            target_revision,
            &target.target_digest,
            &target.target_resolved_release_digest,
        ),
        NodeOperationAction::Unspecified => {
            return Err(Status::internal("stored operation has an invalid action"))
        }
    };
    if stored != expected {
        return Err(Status::internal(
            "stored operation transition conflicts with immutable target snapshots",
        ));
    }
    Ok(())
}

fn first_forward_step(transition: DerivedTransition) -> Option<NodeOperationStepKind> {
    match transition {
        DerivedTransition::Addition | DerivedTransition::Replacement => {
            Some(NodeOperationStepKind::VerifyReleaseMetadata)
        }
        // A read-only source proof pins the exact process/backend identities
        // before any destructive stop instruction may be emitted.
        DerivedTransition::Removal | DerivedTransition::Restart => {
            Some(NodeOperationStepKind::VerifyTarget)
        }
        DerivedTransition::Unchanged => None,
    }
}

fn next_forward_step(
    transition: DerivedTransition,
    step: NodeOperationStepKind,
) -> Option<NodeOperationStepKind> {
    use NodeOperationStepKind as Step;
    match (transition, step) {
        (DerivedTransition::Addition, Step::VerifyReleaseMetadata) => Some(Step::SelectRelease),
        (DerivedTransition::Replacement, Step::VerifyReleaseMetadata) => Some(Step::VerifyTarget),
        (DerivedTransition::Removal, Step::StopBackend) => Some(Step::SwitchAuthority),
        (DerivedTransition::Restart, Step::StopBackend) => Some(Step::StartBackend),
        (DerivedTransition::Replacement, Step::StopBackend) => Some(Step::SelectRelease),
        (DerivedTransition::Addition | DerivedTransition::Replacement, Step::SelectRelease) => {
            Some(Step::StartBackend)
        }
        (DerivedTransition::Addition | DerivedTransition::Replacement, Step::StartBackend)
        | (DerivedTransition::Restart, Step::StartBackend) => Some(Step::VerifyTarget),
        (DerivedTransition::Addition | DerivedTransition::Replacement, Step::SwitchAuthority) => {
            Some(Step::VerifyServing)
        }
        (DerivedTransition::Removal, Step::SwitchAuthority)
        | (DerivedTransition::Addition | DerivedTransition::Replacement, Step::VerifyServing)
        | (DerivedTransition::Restart, Step::VerifyServing) => None,
        _ => None,
    }
}

async fn deployment_inventory<C: GenericClient + Sync>(
    client: &C,
    node_id: &str,
    revision: i64,
) -> Result<(String, String, Vec<String>), Status> {
    if revision == 0 {
        return Ok((String::new(), String::new(), vec![]));
    }
    let row = client
        .query_opt(
            "SELECT bundle_digest, resolved_release_digest, expected_inventory FROM wr_node_deployments
             WHERE node_id = $1 AND revision = $2",
            &[&node_id, &revision],
        )
        .await
        .map_err(internal)?
        .ok_or_else(|| Status::failed_precondition("deployment snapshot is missing"))?;
    let inventory = wr_common::wruntime::DeploymentInventoryV1::decode(
        row.get::<_, Vec<u8>>("expected_inventory").as_slice(),
    )
    .map_err(|error| Status::internal(format!("deployment inventory is invalid: {error}")))?;
    Ok((
        row.get("bundle_digest"),
        row.get("resolved_release_digest"),
        inventory
            .engines
            .into_iter()
            .map(|engine| engine.engine_slot)
            .collect(),
    ))
}

pub async fn submit(
    pool: &Pool,
    actor: &str,
    request: &SubmitOperationRequest,
) -> Result<NodeOperation, Status> {
    let mut client = pool.get().await.map_err(internal)?;
    let transaction = client.transaction().await.map_err(internal)?;
    transaction
        .execute(
            "INSERT INTO wr_nodes (node_id) VALUES ($1) ON CONFLICT (node_id) DO NOTHING",
            &[&request.node_id],
        )
        .await
        .map_err(internal)?;
    let node = transaction
        .query_one(
            "SELECT current_revision, target_revision FROM wr_nodes WHERE node_id = $1 FOR UPDATE",
            &[&request.node_id],
        )
        .await
        .map_err(internal)?;
    let source_revision: i64 = node.get("current_revision");
    let payload = request.encode_to_vec();
    if let Some(existing) = transaction
        .query_opt(
            "SELECT operation_id, request_payload FROM wr_node_operations
             WHERE actor = $1 AND request_token = $2",
            &[&actor, &request.request_token],
        )
        .await
        .map_err(internal)?
    {
        if existing.get::<_, Vec<u8>>("request_payload") != payload {
            return Err(Status::already_exists(
                "request_token was already used by this actor with different operation content",
            ));
        }
        let operation = load_operation(&transaction, existing.get("operation_id")).await?;
        transaction.commit().await.map_err(internal)?;
        return Ok(operation);
    }

    let action =
        NodeOperationAction::try_from(request.action).unwrap_or(NodeOperationAction::Unspecified);
    if action == NodeOperationAction::Unspecified {
        return Err(Status::invalid_argument("operation action is required"));
    }
    let policy = request.policy.unwrap_or(RolloutPolicy {
        max_unavailable: 1,
        allow_downtime: false,
        deadline_seconds: if action == NodeOperationAction::Restart {
            300
        } else {
            1800
        },
    });
    let target_revision = i64::try_from(request.target_revision)
        .map_err(|_| Status::invalid_argument("target_revision is too large"))?;
    let deployment_action = matches!(
        action,
        NodeOperationAction::Deployment | NodeOperationAction::Rollback
    );
    let mut operation_id = (!deployment_action).then(Uuid::new_v4);
    let mut target_revision_digest: Option<String> = None;
    let deadline_seconds = i64::try_from(policy.deadline_seconds)
        .map_err(|_| Status::invalid_argument("deadline_seconds is too large"))?;

    let (source_digest, source_resolved_digest, source_slots) =
        deployment_inventory(&transaction, &request.node_id, source_revision).await?;

    // An exact committed revision is a new idempotent operator request, but it
    // has no rollout effects. Validate it against the committed snapshot before
    // touching staged allocation state, node targets, authority, or cleanup.
    if action == NodeOperationAction::Deployment && target_revision == source_revision {
        let committed = transaction
            .query_opt(
                "SELECT bundle_digest, resolved_release_digest, revision_digest
                 FROM wr_node_deployments
                 WHERE node_id = $1 AND revision = $2 AND state = 'succeeded' FOR SHARE",
                &[&request.node_id, &source_revision],
            )
            .await
            .map_err(internal)?
            .ok_or_else(|| {
                Status::failed_precondition("committed deployment snapshot is missing")
            })?;
        let committed_digest: String = committed.get("bundle_digest");
        let committed_resolved_digest: String = committed.get("resolved_release_digest");
        if request.bundle_digest != committed_digest
            || request.resolved_release_digest != committed_resolved_digest
            || source_digest != committed_digest
            || source_resolved_digest != committed_resolved_digest
        {
            return Err(Status::failed_precondition(
                "operation digest does not match the committed deployment",
            ));
        }
        let mut committed_slots = source_slots.clone();
        committed_slots.sort();
        committed_slots.dedup();

        let operation_id = Uuid::new_v4();
        let revision_digest: String = committed.get("revision_digest");
        transaction
            .execute(
                "INSERT INTO wr_node_operations
                   (operation_id, node_id, request_token, actor, action, state, phase,
                    request_payload, policy, source_revision, target_revision, bundle_digest,
                    resolved_release_digest, target_revision_digest, forward_deadline,
                    committed, committed_at)
                 VALUES ($1, $2, $3, $4, $5, 'succeeded', 'complete', $6, $7, $8, $8,
                         $9, $10, $11, NOW() + make_interval(secs => $12::double precision),
                         TRUE, NOW())",
                &[
                    &operation_id,
                    &request.node_id,
                    &request.request_token,
                    &actor,
                    &action_name(action),
                    &payload,
                    &policy.encode_to_vec(),
                    &source_revision,
                    &committed_digest,
                    &committed_resolved_digest,
                    &revision_digest,
                    &(deadline_seconds as f64),
                ],
            )
            .await
            .map_err(internal)?;
        transaction
            .execute(
                "INSERT INTO wr_node_operation_targets
                   (operation_id, node_id, target_kind, target_key, next_step, complete,
                    source_revision, source_digest, source_resolved_digest,
                    target_revision, target_digest, target_resolved_digest)
                 VALUES ($1, $2, 'proxy', 'proxy', 'complete', TRUE,
                         $3, $4, $5, $3, $4, $5)",
                &[
                    &operation_id,
                    &request.node_id,
                    &source_revision,
                    &committed_digest,
                    &committed_resolved_digest,
                ],
            )
            .await
            .map_err(internal)?;
        for (rollout_order, slot) in committed_slots.into_iter().enumerate() {
            transaction
                .execute(
                    "INSERT INTO wr_node_operation_targets
                       (operation_id, node_id, target_kind, target_key, next_step, complete,
                        source_revision, source_digest, source_resolved_digest,
                        target_revision, target_digest, target_resolved_digest)
                     VALUES ($1, $2, 'engine_slot', $3, 'complete', TRUE,
                             $4, $5, $6, $4, $5, $6)",
                    &[
                        &operation_id,
                        &request.node_id,
                        &slot,
                        &source_revision,
                        &committed_digest,
                        &committed_resolved_digest,
                    ],
                )
                .await
                .map_err(internal)?;
            transaction
                .execute(
                    "INSERT INTO wr_node_operation_engine_target_details
                       (operation_id, target_key, rollout_order, authoritative_revision,
                        serving_converged, transition_kind)
                     VALUES ($1, $2, $3, $4, TRUE, 'unchanged')",
                    &[
                        &operation_id,
                        &slot,
                        &(rollout_order as i32),
                        &source_revision,
                    ],
                )
                .await
                .map_err(internal)?;
        }
        append_event(
            &transaction,
            operation_id,
            actor,
            "OPERATION_NO_EFFECT",
            &format!("exact committed revision {source_revision} is already converged"),
            0,
        )
        .await?;
        let operation = load_operation(&transaction, operation_id).await?;
        transaction.commit().await.map_err(internal)?;
        return Ok(operation);
    }

    let (target_digest, target_resolved_digest, target_slots) = if deployment_action {
        let (digest, resolved_digest, slots) =
            deployment_inventory(&transaction, &request.node_id, target_revision).await?;
        let deployment = transaction
            .query_one(
                "SELECT attempt_token, bundle_digest, resolved_release_digest, finalized_at,
                        abandoned_at, allocated_by, revision_digest
                 FROM wr_node_deployments
                 WHERE node_id = $1 AND revision = $2 AND state IN ('pending', 'active') FOR UPDATE",
                &[&request.node_id, &target_revision],
            )
            .await
            .map_err(internal)?;
        if deployment
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>("abandoned_at")
            .is_some()
        {
            return Err(Status::failed_precondition(
                "staged allocation was abandoned",
            ));
        }
        if deployment.get::<_, String>("allocated_by") != actor {
            return Err(Status::permission_denied(
                "staged allocation belongs to a different authenticated actor",
            ));
        }
        if deployment.get::<_, String>("attempt_token") != request.request_token {
            return Err(Status::failed_precondition(
                "operation request_token must match the staged allocation token",
            ));
        }
        if deployment
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>("finalized_at")
            .is_none()
            || deployment
                .get::<_, String>("resolved_release_digest")
                .is_empty()
        {
            return Err(Status::failed_precondition(
                "staged deployment must be finalized before submission",
            ));
        }
        let revision_digest: String = deployment.get("revision_digest");
        operation_id = Some(
            Uuid::parse_str(&deployment_operation_id(&revision_digest).map_err(internal)?)
                .map_err(internal)?,
        );
        target_revision_digest = Some(revision_digest);
        if deployment.get::<_, String>("bundle_digest") != request.bundle_digest
            || digest != request.bundle_digest
            || deployment.get::<_, String>("resolved_release_digest")
                != request.resolved_release_digest
            || resolved_digest != request.resolved_release_digest
        {
            return Err(Status::failed_precondition(
                "operation source or resolved digest does not match the finalized deployment",
            ));
        }
        (digest, resolved_digest, slots)
    } else {
        (
            source_digest.clone(),
            source_resolved_digest.clone(),
            vec![request.engine_slot.clone()],
        )
    };
    let operation_id = operation_id.expect("all supported actions derive an operation ID");

    if action == NodeOperationAction::Restart
        && (source_slots.is_empty() || !source_slots.contains(&request.engine_slot))
    {
        return Err(Status::failed_precondition(
            "restart requires a slot from the committed deployment",
        ));
    }
    let withdraws_last_source = if deployment_action {
        !source_slots.is_empty() && target_slots.is_empty()
    } else {
        source_slots.len() == 1
    };
    if withdraws_last_source && !policy.allow_downtime {
        return Err(Status::failed_precondition(
            "withdrawing the last healthy source slot requires allow_downtime",
        ));
    }
    let mut slots = if deployment_action {
        source_slots
            .iter()
            .chain(target_slots.iter())
            .cloned()
            .collect::<Vec<_>>()
    } else {
        vec![request.engine_slot.clone()]
    };
    slots.sort();
    slots.dedup();
    if slots.is_empty() && !deployment_action {
        return Err(Status::invalid_argument("operation has no affected slots"));
    }
    let affected_slots = if action == NodeOperationAction::Restart {
        slots.len()
    } else {
        slots
            .iter()
            .filter(|slot| {
                derive_transition(
                    if source_slots.contains(slot) {
                        source_revision
                    } else {
                        0
                    },
                    if source_slots.contains(slot) {
                        source_digest.as_str()
                    } else {
                        ""
                    },
                    if source_slots.contains(slot) {
                        source_resolved_digest.as_str()
                    } else {
                        ""
                    },
                    if target_slots.contains(slot) {
                        target_revision
                    } else {
                        0
                    },
                    if target_slots.contains(slot) {
                        target_digest.as_str()
                    } else {
                        ""
                    },
                    if target_slots.contains(slot) {
                        target_resolved_digest.as_str()
                    } else {
                        ""
                    },
                ) != DerivedTransition::Unchanged
            })
            .count()
    };
    if affected_slots > 0 && policy.max_unavailable as usize > affected_slots {
        return Err(Status::invalid_argument(
            "max_unavailable exceeds the number of affected slots",
        ));
    }
    // New capacity lands first, retained transitions proceed next, and
    // removals happen last. Ordering is derived from immutable inventories,
    // never from the caller's temporary legacy deployment selector.
    slots.sort_by_key(|slot| {
        let source = source_slots.contains(slot);
        let target = target_slots.contains(slot);
        let transition = if action == NodeOperationAction::Restart {
            DerivedTransition::Restart
        } else {
            derive_transition(
                if source { source_revision } else { 0 },
                if source { source_digest.as_str() } else { "" },
                if source {
                    source_resolved_digest.as_str()
                } else {
                    ""
                },
                if target { target_revision } else { 0 },
                if target { target_digest.as_str() } else { "" },
                if target {
                    target_resolved_digest.as_str()
                } else {
                    ""
                },
            )
        };
        let group = match transition {
            DerivedTransition::Addition => 0,
            DerivedTransition::Replacement
            | DerivedTransition::Unchanged
            | DerivedTransition::Restart => 1,
            DerivedTransition::Removal => 2,
        };
        (group, slot.clone())
    });
    if deployment_action {
        transaction
            .execute(
                "UPDATE wr_nodes SET target_revision = $2, updated_at = NOW() WHERE node_id = $1",
                &[&request.node_id, &target_revision],
            )
            .await
            .map_err(internal)?;
    }
    transaction
        .execute(
            "INSERT INTO wr_node_operations
               (operation_id, node_id, request_token, actor, action, state, request_payload,
                policy, source_revision, target_revision, bundle_digest, resolved_release_digest,
                target_revision_digest, forward_deadline)
             VALUES ($1, $2, $3, $4, $5, 'queued', $6, $7, $8, $9, $10, $11, $12,
                     NOW() + make_interval(secs => $13::double precision))",
            &[
                &operation_id,
                &request.node_id,
                &request.request_token,
                &actor,
                &action_name(action),
                &payload,
                &policy.encode_to_vec(),
                &source_revision,
                &target_revision,
                &request.bundle_digest,
                &request.resolved_release_digest,
                &target_revision_digest,
                &(deadline_seconds as f64),
            ],
        )
        .await
        .map_err(|error| {
            if error.code() == Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION) {
                Status::failed_precondition("node already has an active operation")
            } else {
                internal(error)
            }
        })?;
    let proxy_first = if deployment_action {
        if source_revision > 0 {
            // Explicit source proof must produce a result. Reserve inspection
            // for observation-only ambiguity recovery.
            "verify_target"
        } else {
            "select_release"
        }
    } else {
        "complete"
    };
    transaction
        .execute(
            "INSERT INTO wr_node_operation_targets
               (operation_id, node_id, target_kind, target_key, next_step, complete,
                source_revision, source_digest, source_resolved_digest,
                target_revision, target_digest, target_resolved_digest)
             VALUES ($1, $2, 'proxy', 'proxy', $3, $3 = 'complete', $4, $5, $6, $7, $8, $9)",
            &[
                &operation_id,
                &request.node_id,
                &proxy_first,
                &source_revision,
                &source_digest,
                &source_resolved_digest,
                &target_revision,
                &request.bundle_digest,
                &request.resolved_release_digest,
            ],
        )
        .await
        .map_err(internal)?;
    if deployment_action {
        transaction
            .execute(
                "UPDATE wr_node_deployments SET allocation_actor = $3, operation_id = $4
                 WHERE node_id = $1 AND revision = $2",
                &[&request.node_id, &target_revision, &actor, &operation_id],
            )
            .await
            .map_err(internal)?;
    }
    for (rollout_order, slot) in slots.into_iter().enumerate() {
        let source = if source_slots.contains(&slot) {
            source_revision
        } else {
            0
        };
        let target = if deployment_action {
            if target_slots.contains(&slot) {
                target_revision
            } else {
                0
            }
        } else {
            source_revision
        };
        let transition = if action == NodeOperationAction::Restart {
            DerivedTransition::Restart
        } else {
            derive_transition(
                source,
                if source > 0 {
                    source_digest.as_str()
                } else {
                    ""
                },
                if source > 0 {
                    source_resolved_digest.as_str()
                } else {
                    ""
                },
                target,
                if target > 0 {
                    target_digest.as_str()
                } else {
                    ""
                },
                if target > 0 {
                    target_resolved_digest.as_str()
                } else {
                    ""
                },
            )
        };
        let first = first_forward_step(transition);
        let complete = first.is_none();
        transaction
            .execute(
                "INSERT INTO wr_node_operation_targets
                   (operation_id, node_id, target_kind, target_key, next_step, complete,
                    source_revision, source_digest, source_resolved_digest,
                    target_revision, target_digest, target_resolved_digest)
                 VALUES ($1, $2, 'engine_slot', $3, $4, $5, $6, $7, $8, $9, $10, $11)",
                &[
                    &operation_id,
                    &request.node_id,
                    &slot,
                    &first.map(step_name).unwrap_or("complete"),
                    &complete,
                    &source,
                    &if source > 0 {
                        source_digest.as_str()
                    } else {
                        ""
                    },
                    &if source > 0 {
                        source_resolved_digest.as_str()
                    } else {
                        ""
                    },
                    &target,
                    &if target > 0 {
                        target_digest.as_str()
                    } else {
                        ""
                    },
                    &if target > 0 {
                        target_resolved_digest.as_str()
                    } else {
                        ""
                    },
                ],
            )
            .await
            .map_err(internal)?;
        transaction
            .execute(
                "INSERT INTO wr_node_operation_engine_target_details
                   (operation_id, target_key, rollout_order, authoritative_revision,
                    serving_converged, transition_kind)
                 VALUES ($1, $2, $3, $4, $5, $6)",
                &[
                    &operation_id,
                    &slot,
                    &(rollout_order as i32),
                    &source,
                    &complete,
                    &transition.name(),
                ],
            )
            .await
            .map_err(internal)?;
    }
    let transition_detail = if deployment_action {
        let mut additions = 0;
        let mut replacements = 0;
        let mut unchanged = 0;
        let mut removals = 0;
        for slot in source_slots
            .iter()
            .chain(target_slots.iter())
            .collect::<std::collections::BTreeSet<_>>()
        {
            match derive_transition(
                if source_slots.contains(slot) {
                    source_revision
                } else {
                    0
                },
                if source_slots.contains(slot) {
                    source_digest.as_str()
                } else {
                    ""
                },
                if source_slots.contains(slot) {
                    source_resolved_digest.as_str()
                } else {
                    ""
                },
                if target_slots.contains(slot) {
                    target_revision
                } else {
                    0
                },
                if target_slots.contains(slot) {
                    target_digest.as_str()
                } else {
                    ""
                },
                if target_slots.contains(slot) {
                    target_resolved_digest.as_str()
                } else {
                    ""
                },
            ) {
                DerivedTransition::Addition => additions += 1,
                DerivedTransition::Replacement => replacements += 1,
                DerivedTransition::Unchanged => unchanged += 1,
                DerivedTransition::Removal => removals += 1,
                DerivedTransition::Restart => unreachable!("restart is not inventory-derived"),
            }
        }
        format!(
            "desired transitions: additions={additions}, replacements={replacements}, unchanged={unchanged}, removals={removals}"
        )
    } else {
        format!("restart transition: slot={}", request.engine_slot)
    };
    append_event(
        &transaction,
        operation_id,
        actor,
        "OPERATION_SUBMITTED",
        &transition_detail,
        0,
    )
    .await?;
    fence_cleanup_authority(&transaction, &request.node_id, "OPERATION_SUBMITTED").await?;
    let operation = load_operation(&transaction, operation_id).await?;
    transaction.commit().await.map_err(internal)?;
    Ok(operation)
}

pub async fn get(pool: &Pool, operation_id: &str) -> Result<NodeOperation, Status> {
    let id = Uuid::parse_str(operation_id)
        .map_err(|_| Status::invalid_argument("operation_id must be a UUID"))?;
    let client = pool.get().await.map_err(internal)?;
    load_operation(&client, id).await
}

pub async fn events(pool: &Pool, operation_id: &str) -> Result<Vec<OperationEvent>, Status> {
    let id = Uuid::parse_str(operation_id)
        .map_err(|_| Status::invalid_argument("operation_id must be a UUID"))?;
    let client = pool.get().await.map_err(internal)?;
    client
        .query(
            "SELECT sequence, operation_id, actor, event_code, detail, lease_epoch, created_at
             FROM wr_node_operation_events WHERE operation_id = $1 ORDER BY sequence",
            &[&id],
        )
        .await
        .map_err(internal)?
        .into_iter()
        .map(|row| {
            let created_at: chrono::DateTime<chrono::Utc> = row.get("created_at");
            Ok(OperationEvent {
                sequence: row.get::<_, i64>("sequence") as u64,
                operation_id: row.get::<_, Uuid>("operation_id").to_string(),
                actor: row.get("actor"),
                event_code: row.get("event_code"),
                detail: row.get("detail"),
                lease_epoch: row.get::<_, i64>("lease_epoch") as u64,
                created_at: Some(timestamp(created_at)),
            })
        })
        .collect()
}

pub(crate) async fn list_from_client<C>(
    client: &C,
    node_id: &str,
    include_terminal: bool,
) -> Result<Vec<NodeOperation>, Status>
where
    C: GenericClient + Sync,
{
    let rows = client
        .query(
            "SELECT operation_id FROM wr_node_operations
             WHERE ($1 = '' OR node_id = $1)
               AND ($2 OR state IN ('queued', 'running', 'paused'))
             ORDER BY created_at DESC, operation_id",
            &[&node_id, &include_terminal],
        )
        .await
        .map_err(internal)?;
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        result.push(load_operation(client, row.get("operation_id")).await?);
    }
    Ok(result)
}

pub async fn list(
    pool: &Pool,
    node_id: &str,
    include_terminal: bool,
) -> Result<Vec<NodeOperation>, Status> {
    let client = pool.get().await.map_err(internal)?;
    list_from_client(&client, node_id, include_terminal).await
}

async fn enter_restoration<C: GenericClient + Sync>(
    client: &C,
    id: Uuid,
    actor: &str,
    terminal: &str,
    code: &str,
    detail: &str,
) -> Result<(), Status> {
    let row = client
        .query_opt(
            "UPDATE wr_node_operations
             SET state = 'queued', phase = 'restoring_source', forward_fenced = TRUE,
                 restoration_requested = TRUE, restoration_terminal_state = $2,
                 failure_code = $3, failure_detail = $4, lease_expires_at = NULL,
                 claimed_by = NULL, agent_instance_id = NULL, updated_at = NOW()
             WHERE operation_id = $1 AND NOT committed
               AND state IN ('queued', 'running', 'paused')
             RETURNING lease_epoch",
            &[&id, &terminal, &code, &detail],
        )
        .await
        .map_err(internal)?;
    let Some(row) = row else {
        return Err(Status::failed_precondition(
            "only an active uncommitted operation can enter restoration",
        ));
    };
    client
        .execute(
            "UPDATE wr_node_operation_targets
             SET complete = NOT (changed OR effect_ambiguous),
                 next_step = CASE
                     WHEN changed OR effect_ambiguous THEN 'restore_source'
                     ELSE 'complete' END,
                 effect_delivered_at = NULL, effect_reported = FALSE,
                 effect_condition_code = '', effect_detail = '',
                 effect_observed_revision = 0, effect_observed_digest = '',
                 effect_backend_instance_id = '', effect_process_instance_id = '',
                 condition_code = '', condition_detail = '', updated_at = NOW()
             WHERE operation_id = $1",
            &[&id],
        )
        .await
        .map_err(internal)?;
    append_event(client, id, actor, code, detail, row.get("lease_epoch")).await
}

pub async fn resume(pool: &Pool, operation_id: &str, actor: &str) -> Result<NodeOperation, Status> {
    let id = Uuid::parse_str(operation_id)
        .map_err(|_| Status::invalid_argument("operation_id must be a UUID"))?;
    let mut client = pool.get().await.map_err(internal)?;
    let transaction = client.transaction().await.map_err(internal)?;
    let row = transaction
        .query_opt(
            "UPDATE wr_node_operations SET state = 'queued', updated_at = NOW(),
                    lease_expires_at = NULL, claimed_by = NULL, agent_instance_id = NULL,
                    failure_code = '', failure_detail = ''
             WHERE operation_id = $1 AND state = 'paused' RETURNING lease_epoch",
            &[&id],
        )
        .await
        .map_err(internal)?
        .ok_or_else(|| Status::failed_precondition("operation must be paused"))?;
    append_event(
        &transaction,
        id,
        actor,
        "OPERATION_RESUMED",
        "fresh activation and epoch required",
        row.get("lease_epoch"),
    )
    .await?;
    let operation = load_operation(&transaction, id).await?;
    transaction.commit().await.map_err(internal)?;
    Ok(operation)
}

pub async fn cancel(pool: &Pool, operation_id: &str, actor: &str) -> Result<NodeOperation, Status> {
    let id = Uuid::parse_str(operation_id)
        .map_err(|_| Status::invalid_argument("operation_id must be a UUID"))?;
    let mut client = pool.get().await.map_err(internal)?;
    let transaction = client.transaction().await.map_err(internal)?;
    enter_restoration(
        &transaction,
        id,
        actor,
        "cancelled",
        "CANCEL_REQUESTED",
        "source restoration required before cancellation is terminal",
    )
    .await?;
    let operation = load_operation(&transaction, id).await?;
    transaction.commit().await.map_err(internal)?;
    Ok(operation)
}

async fn deadline_expired<C: GenericClient + Sync>(client: &C, id: Uuid) -> Result<bool, Status> {
    let row = client
        .query_one(
            "SELECT phase = 'forward' AND forward_deadline <= NOW() AS expired,
                    forward_fenced FROM wr_node_operations WHERE operation_id = $1 FOR UPDATE",
            &[&id],
        )
        .await
        .map_err(internal)?;
    Ok(row.get::<_, bool>("expired") || row.get::<_, bool>("forward_fenced"))
}

async fn set_slot_condition<C: GenericClient + Sync>(
    client: &C,
    id: Uuid,
    slot: &str,
    code: &str,
    detail: &str,
) -> Result<(), Status> {
    client
        .execute(
            "UPDATE wr_node_operation_targets SET condition_code = $3, condition_detail = $4,
                    updated_at = NOW() WHERE operation_id = $1
                      AND target_kind = 'engine_slot' AND target_key = $2",
            &[&id, &slot, &code, &detail],
        )
        .await
        .map_err(internal)?;
    Ok(())
}

async fn clear_slot_condition<C: GenericClient + Sync>(
    client: &C,
    id: Uuid,
    slot: &str,
) -> Result<(), Status> {
    set_slot_condition(client, id, slot, "", "").await
}

async fn pause_with_condition<C: GenericClient + Sync>(
    client: &C,
    id: Uuid,
    slot: &str,
    code: &str,
    detail: &str,
) -> Result<(), Status> {
    set_slot_condition(client, id, slot, code, detail).await?;
    client
        .execute(
            "UPDATE wr_node_operations SET state = 'paused', failure_code = $2,
                    failure_detail = $3, lease_expires_at = NULL, claimed_by = NULL,
                    agent_instance_id = NULL, updated_at = NOW()
             WHERE operation_id = $1 AND state IN ('queued', 'running')",
            &[&id, &code, &detail],
        )
        .await
        .map_err(internal)?;
    Ok(())
}

fn observation_is_fresh(
    snapshot: &crate::db::ClusterStatusSnapshot,
    observation: &SlotObservation,
) -> bool {
    observation.observed_at.as_ref().is_some_and(|value| {
        chrono::DateTime::from_timestamp(value.seconds, value.nanos as u32).is_some_and(|time| {
            snapshot.observed_at.signed_duration_since(time)
                <= chrono::Duration::seconds(EVIDENCE_FRESH_SECONDS)
        })
    })
}

fn observation_after_delivery(
    observation: &SlotObservation,
    delivered_at: Option<&prost_types::Timestamp>,
) -> bool {
    match (observation.observed_at.as_ref(), delivered_at) {
        (Some(observed), Some(delivered)) => {
            (observed.seconds, observed.nanos) > (delivered.seconds, delivered.nanos)
        }
        _ => false,
    }
}

fn matching_observation<'a>(
    snapshot: &'a crate::db::ClusterStatusSnapshot,
    operation: &NodeOperation,
    slot: &str,
) -> Option<&'a SlotObservation> {
    snapshot.observations.iter().find(|observation| {
        observation.node_id == operation.node_id
            && observation.engine_slot == slot
            && observation.operation_id == operation.operation_id
            && observation.agent_instance_id == operation.agent_instance_id
            && observation.lease_epoch == operation.lease_epoch
            && observation_is_fresh(snapshot, observation)
    })
}

struct ReadyExpectation<'a> {
    service_kind: ServiceKind,
    revision: u64,
    digest: &'a str,
    previous_backend: &'a str,
    previous_process: &'a str,
    reported_backend: &'a str,
    reported_process: &'a str,
}

fn observation_ready(
    snapshot: &crate::db::ClusterStatusSnapshot,
    observation: &SlotObservation,
    expected: &ReadyExpectation<'_>,
) -> bool {
    if !observation_is_fresh(snapshot, observation)
        || observation.backend_state != BackendProcessState::Running as i32
        || observation.backend_instance_id.is_empty()
        || !observation.backend_query_error.is_empty()
        || observation.observed_revision != expected.revision
        || observation.observed_digest != expected.digest
        || (!expected.previous_backend.is_empty()
            && observation.backend_instance_id == expected.previous_backend)
        || (!expected.reported_backend.is_empty()
            && observation.backend_instance_id != expected.reported_backend)
    {
        return false;
    }
    observation.lifecycle.as_ref().is_some_and(|status| {
        status.state == ProcessLifecycleState::Ready as i32
            && status.service_kind == expected.service_kind as i32
            && !status.process_instance_id.is_empty()
            && (expected.previous_process.is_empty()
                || status.process_instance_id != expected.previous_process)
            && (expected.reported_process.is_empty()
                || status.process_instance_id == expected.reported_process)
    })
}

fn exact_registration_count(
    snapshot: &crate::db::ClusterStatusSnapshot,
    node_id: &str,
    slot: &str,
    revision: u64,
    digest: &str,
) -> usize {
    snapshot
        .engines
        .iter()
        .filter(|engine| {
            engine
                .registration
                .deployment
                .as_ref()
                .is_some_and(|metadata| {
                    metadata.node_id == node_id
                        && metadata.engine_slot == slot
                        && metadata.revision == revision
                        && metadata.bundle_digest == digest
                })
                && snapshot
                    .observed_at
                    .signed_duration_since(engine.last_heartbeat)
                    <= chrono::Duration::seconds(EVIDENCE_FRESH_SECONDS)
        })
        .count()
}

fn exact_registration_absent(
    snapshot: &crate::db::ClusterStatusSnapshot,
    node_id: &str,
    slot: &str,
    revision: u64,
) -> bool {
    !snapshot.engines.iter().any(|engine| {
        engine
            .registration
            .deployment
            .as_ref()
            .is_some_and(|metadata| {
                metadata.node_id == node_id
                    && metadata.engine_slot == slot
                    && metadata.revision == revision
            })
    })
}

fn slot_routes_healthy(
    snapshot: &crate::db::ClusterStatusSnapshot,
    node_id: &str,
    slot: &str,
    revision: u64,
    digest: &str,
) -> Result<bool, Status> {
    let Some(candidate) = snapshot.deployments.iter().find(|candidate| {
        candidate.record.node_id == node_id
            && candidate.record.revision == revision
            && candidate.record.bundle_digest == digest
    }) else {
        return Ok(false);
    };
    let mut deployment = candidate.record.clone();
    let Some(inventory) = deployment.inventory.as_mut() else {
        return Ok(false);
    };
    inventory
        .engines
        .retain(|expected| expected.engine_slot == slot);
    if inventory.engines.len() != 1 {
        return Ok(false);
    }
    let selected = snapshot
        .slot_authorities
        .iter()
        .find(|authority| authority.node_id == node_id && authority.engine_slot == slot);
    if selected.is_some_and(|authority| authority.revision != revision) {
        return Ok(false);
    }
    Ok(crate::db::deployment_conditions_from_snapshot(
        snapshot,
        &deployment,
        EVIDENCE_FRESH_SECONDS as f64,
        EVIDENCE_FRESH_SECONDS as f64,
    )?
    .is_empty())
}

fn old_routes_non_serving(
    snapshot: &crate::db::ClusterStatusSnapshot,
    node_id: &str,
    slot: &str,
    revision: u64,
) -> bool {
    let old_engine_ids = snapshot.engines.iter().filter_map(|engine| {
        engine
            .registration
            .deployment
            .as_ref()
            .and_then(|metadata| {
                (metadata.node_id == node_id
                    && metadata.engine_slot == slot
                    && metadata.revision == revision)
                    .then_some(engine.registration.engine_id.as_str())
            })
    });
    let old_engine_ids = old_engine_ids.collect::<std::collections::HashSet<_>>();
    snapshot
        .routes
        .iter()
        .filter(|route| old_engine_ids.contains(route.rule.engine_id.as_str()))
        .all(|route| !route.rule.healthy)
}

fn stop_preserves_availability(
    snapshot: &crate::db::ClusterStatusSnapshot,
    operation: &NodeOperation,
    slot: &str,
    policy: &RolloutPolicy,
) -> Result<bool, Status> {
    let mut authoritative = std::collections::BTreeMap::<String, (u64, String)>::new();
    if let Some(current) = snapshot.deployments.iter().find(|deployment| {
        deployment.record.node_id == operation.node_id
            && deployment.record.revision == deployment.current_revision
    }) {
        for expected in current
            .record
            .inventory
            .as_ref()
            .into_iter()
            .flat_map(|inventory| &inventory.engines)
        {
            authoritative.insert(
                expected.engine_slot.clone(),
                (
                    current.record.revision,
                    current.record.bundle_digest.clone(),
                ),
            );
        }
    }
    for candidate in operation_slots(operation)? {
        if candidate.authoritative_revision == 0 {
            authoritative.remove(&candidate.engine_slot);
        } else {
            let digest = if candidate.authoritative_revision == candidate.source_revision {
                candidate.source_digest.clone()
            } else {
                candidate.target_digest.clone()
            };
            authoritative.insert(
                candidate.engine_slot.clone(),
                (candidate.authoritative_revision, digest),
            );
        }
    }
    let desired = authoritative.len() as i64;
    let mut healthy = 0_i64;
    let mut selected_slot_healthy = false;
    for (candidate_slot, (revision, digest)) in authoritative {
        let slot_healthy = slot_routes_healthy(
            snapshot,
            &operation.node_id,
            &candidate_slot,
            revision,
            &digest,
        )?;
        healthy += i64::from(slot_healthy);
        if candidate_slot == slot {
            selected_slot_healthy = slot_healthy;
        }
    }
    let projected_healthy = healthy - i64::from(selected_slot_healthy);
    let unavailable = desired.saturating_sub(projected_healthy);
    Ok((policy.allow_downtime || projected_healthy > 0)
        && unavailable <= i64::from(policy.max_unavailable))
}

async fn switch_authority<C: GenericClient + Sync>(
    client: &C,
    node_id: &str,
    slot: &str,
    revision: i64,
) -> Result<(), Status> {
    // Serialize with routing publication. The authority row and withdrawal of
    // every old revision's already-healthy routes are one database transaction.
    client
        .query_one(
            "SELECT version FROM wr_manager_lock WHERE id = 1 FOR UPDATE",
            &[],
        )
        .await
        .map_err(internal)?;
    let authority_changed: bool = client
        .query_one(
            "SELECT NOT EXISTS(SELECT 1 FROM wr_node_slot_authority
             WHERE node_id = $1 AND engine_slot = $2 AND authoritative AND revision = $3)",
            &[&node_id, &slot, &revision],
        )
        .await
        .map_err(internal)?
        .get(0);
    client
        .execute(
            "UPDATE wr_node_slot_authority SET authoritative = FALSE, updated_at = NOW()
             WHERE node_id = $1 AND engine_slot = $2 AND authoritative",
            &[&node_id, &slot],
        )
        .await
        .map_err(internal)?;
    if revision > 0 {
        client
            .execute(
                "INSERT INTO wr_node_slot_authority
                    (node_id, engine_slot, revision, authoritative, resolved_release_digest)
                 SELECT $1, $2, $3, TRUE, resolved_release_digest
                 FROM wr_node_deployments WHERE node_id = $1 AND revision = $3
                 ON CONFLICT (node_id, engine_slot, revision) DO UPDATE SET
                   authoritative = TRUE,
                   resolved_release_digest = EXCLUDED.resolved_release_digest,
                   updated_at = NOW()",
                &[&node_id, &slot, &revision],
            )
            .await
            .map_err(internal)?;
    }
    let withdrawn = client
        .execute(
            "UPDATE wr_routing_rules r SET healthy = FALSE, updated_at = NOW()
             FROM wr_engines e
             WHERE r.engine_id = e.engine_id AND r.healthy
               AND e.deployment_node_id = $1 AND e.deployment_engine_slot = $2
               AND e.deployment_revision <> $3",
            &[&node_id, &slot, &revision],
        )
        .await
        .map_err(internal)?;
    if withdrawn > 0 {
        client
            .execute(
                "UPDATE wr_manager_lock SET version = version + 1 WHERE id = 1",
                &[],
            )
            .await
            .map_err(internal)?;
    }
    if authority_changed {
        fence_cleanup_authority(client, node_id, "SLOT_AUTHORITY_CHANGED").await?;
    }
    Ok(())
}

async fn advance_slot<C: GenericClient + Sync>(
    client: &C,
    id: Uuid,
    slot: &str,
    next: Option<NodeOperationStepKind>,
    authoritative_revision: i64,
) -> Result<(), Status> {
    let complete = next.is_none();
    client
        .execute(
            "UPDATE wr_node_operation_targets
             SET completed_steps = completed_steps + 1, next_step = $3, complete = $4,
                 effect_ambiguous = FALSE, effect_delivered_at = NULL, effect_reported = FALSE,
                 effect_observed_revision = 0, effect_observed_digest = '',
                 effect_observed_resolved_digest = '', effect_backend_instance_id = '',
                 effect_process_instance_id = '', condition_code = '', condition_detail = '',
                 updated_at = NOW()
             WHERE operation_id = $1 AND target_kind = 'engine_slot' AND target_key = $2",
            &[
                &id,
                &slot,
                &next.map(step_name).unwrap_or("complete"),
                &complete,
            ],
        )
        .await
        .map_err(internal)?;
    client
        .execute(
            "UPDATE wr_node_operation_engine_target_details
             SET authoritative_revision = $3
             WHERE operation_id = $1 AND target_kind = 'engine_slot' AND target_key = $2",
            &[&id, &slot, &authoritative_revision],
        )
        .await
        .map_err(internal)?;
    Ok(())
}

async fn reconcile_slot<C: GenericClient + Sync>(
    client: &C,
    id: Uuid,
    snapshot: &crate::db::ClusterStatusSnapshot,
    operation: &NodeOperation,
    slot: &OperationSlotProgress,
) -> Result<bool, Status> {
    let transition = slot.transition;
    let phase =
        NodeOperationPhase::try_from(operation.phase).unwrap_or(NodeOperationPhase::Unspecified);
    let step = NodeOperationStepKind::try_from(slot.next_step)
        .unwrap_or(NodeOperationStepKind::Unspecified);
    let observation = matching_observation(snapshot, operation, &slot.engine_slot);

    if phase == NodeOperationPhase::RestoringSource {
        if step != NodeOperationStepKind::RestoreSource {
            return Ok(false);
        }
        if !slot.effect_condition_code.is_empty() {
            pause_with_condition(
                client,
                id,
                &slot.engine_slot,
                &slot.effect_condition_code,
                "restoration effect failed; explicit resume is required",
            )
            .await?;
            return Ok(false);
        }
        let conclusive = observation.is_some_and(|value| {
            value.backend_query_error.is_empty()
                && !value.backend_instance_id.is_empty()
                && matches!(
                    BackendProcessState::try_from(value.backend_state),
                    Ok(BackendProcessState::Running | BackendProcessState::Exited)
                )
        });
        let source_host_proven = if slot.source_revision == 0 {
            observation.is_some_and(|value| {
                value.backend_state == BackendProcessState::Exited as i32
                    && value.observed_revision == 0
                    && value.observed_digest.is_empty()
                    && value.backend_query_error.is_empty()
                    && !value.backend_instance_id.is_empty()
            }) && exact_registration_absent(
                snapshot,
                &operation.node_id,
                &slot.engine_slot,
                slot.target_revision,
            )
        } else {
            observation.is_some_and(|value| {
                observation_ready(
                    snapshot,
                    value,
                    &ReadyExpectation {
                        service_kind: ServiceKind::Engine,
                        revision: slot.source_revision,
                        digest: &slot.source_digest,
                        previous_backend: "",
                        previous_process: "",
                        reported_backend: &slot.effect_backend_instance_id,
                        reported_process: &slot.effect_process_instance_id,
                    },
                )
            }) && exact_registration_count(
                snapshot,
                &operation.node_id,
                &slot.engine_slot,
                slot.source_revision,
                &slot.source_digest,
            ) == 1
        };
        if source_host_proven {
            switch_authority(
                client,
                &operation.node_id,
                &slot.engine_slot,
                slot.source_revision as i64,
            )
            .await?;
            if slot.source_revision > 0
                && !slot_routes_healthy(
                    snapshot,
                    &operation.node_id,
                    &slot.engine_slot,
                    slot.source_revision,
                    &slot.source_digest,
                )?
            {
                set_slot_condition(
                    client,
                    id,
                    &slot.engine_slot,
                    "RESTORATION_ROUTE_PENDING",
                    "source authority is restored but required module routes have not converged",
                )
                .await?;
                return Ok(false);
            }
        } else if slot.effect_ambiguous && conclusive {
            client
                .execute(
                    "UPDATE wr_node_operation_targets
                     SET changed = TRUE, effect_ambiguous = FALSE,
                         effect_delivered_at = NULL, condition_code = '', condition_detail = ''
                     WHERE operation_id = $1 AND target_kind = 'engine_slot' AND target_key = $2",
                    &[&id, &slot.engine_slot],
                )
                .await
                .map_err(internal)?;
            return Ok(true);
        } else {
            set_slot_condition(
                client,
                id,
                &slot.engine_slot,
                "RESTORATION_EVIDENCE_PENDING",
                if slot.effect_ambiguous {
                    "fresh inspection must prove whether the delivered effect changed the slot"
                } else {
                    "fresh source backend, lifecycle, digest, registration, and route evidence are required"
                },
            )
            .await?;
            return Ok(false);
        }
        advance_slot(
            client,
            id,
            &slot.engine_slot,
            None,
            slot.source_revision as i64,
        )
        .await?;
        return Ok(true);
    }

    if !slot.effect_condition_code.is_empty() {
        enter_restoration(
            client,
            id,
            "manager",
            "failed",
            &slot.effect_condition_code,
            "typed effect failed; source restoration is required",
        )
        .await?;
        return Ok(false);
    }
    let next = next_forward_step(transition, step);
    match step {
        NodeOperationStepKind::VerifyReleaseMetadata | NodeOperationStepKind::VerifyProxy => {
            if !slot.effect_reported {
                return Ok(false);
            }
            advance_slot(
                client,
                id,
                &slot.engine_slot,
                next,
                slot.source_revision as i64,
            )
            .await?;
        }
        NodeOperationStepKind::StopBackend => {
            if slot.effect_delivered_at.is_none() {
                return Ok(false);
            }
            let Some(observation) = observation else {
                return Ok(false);
            };
            // The agent reports a completed effect before publishing its
            // matching observation. Ignore older source evidence until that
            // post-delivery observation arrives.
            if !observation_after_delivery(observation, slot.effect_delivered_at.as_ref()) {
                return Ok(false);
            }
            if observation.backend_state == BackendProcessState::Running as i32
                && observation.backend_instance_id == slot.pinned_backend_instance_id
                && observation.backend_query_error.is_empty()
            {
                client
                    .execute(
                        "UPDATE wr_node_operation_targets
                         SET effect_delivered_at = NULL, effect_ambiguous = FALSE
                         WHERE operation_id = $1 AND target_kind = 'engine_slot' AND target_key = $2",
                        &[&id, &slot.engine_slot],
                    )
                    .await
                    .map_err(internal)?;
                return Ok(true);
            }
            if observation.backend_state != BackendProcessState::Exited as i32
                || slot.pinned_backend_instance_id.is_empty()
                || observation.backend_instance_id != slot.pinned_backend_instance_id
                || !observation.backend_query_error.is_empty()
                || !exact_registration_absent(
                    snapshot,
                    &operation.node_id,
                    &slot.engine_slot,
                    slot.source_revision,
                )
                || !old_routes_non_serving(
                    snapshot,
                    &operation.node_id,
                    &slot.engine_slot,
                    slot.source_revision,
                )
            {
                pause_with_condition(
                    client,
                    id,
                    &slot.engine_slot,
                    "STOP_EVIDENCE_PENDING",
                    "the fresh pinned backend exit, registration removal, and non-serving routes are not all proven",
                )
                .await?;
                return Ok(false);
            }
            client
                .execute(
                    "UPDATE wr_node_operation_targets SET changed = TRUE
                     WHERE operation_id = $1 AND target_kind = 'engine_slot' AND target_key = $2",
                    &[&id, &slot.engine_slot],
                )
                .await
                .map_err(internal)?;
            advance_slot(
                client,
                id,
                &slot.engine_slot,
                next,
                slot.source_revision as i64,
            )
            .await?;
        }
        NodeOperationStepKind::SelectRelease => {
            if observation.is_some_and(|value| {
                observation_after_delivery(value, slot.effect_delivered_at.as_ref())
                    && value.observed_revision == slot.source_revision
                    && value.observed_digest == slot.source_digest
                    && value.backend_query_error.is_empty()
            }) {
                client
                    .execute(
                        "UPDATE wr_node_operation_targets
                         SET effect_delivered_at = NULL, effect_ambiguous = FALSE
                         WHERE operation_id = $1 AND target_kind = 'engine_slot' AND target_key = $2",
                        &[&id, &slot.engine_slot],
                    )
                    .await
                    .map_err(internal)?;
                return Ok(true);
            }
            let selected = observation.is_some_and(|value| {
                value.observed_revision == slot.target_revision
                    && value.observed_digest == slot.target_digest
                    && value.backend_query_error.is_empty()
            });
            if !selected {
                return Ok(false);
            }
            if slot.target_revision != slot.source_revision {
                client
                    .execute(
                        "UPDATE wr_node_operation_targets SET changed = TRUE
                         WHERE operation_id = $1 AND target_kind = 'engine_slot' AND target_key = $2",
                        &[&id, &slot.engine_slot],
                    )
                    .await
                    .map_err(internal)?;
            }
            advance_slot(
                client,
                id,
                &slot.engine_slot,
                next,
                slot.source_revision as i64,
            )
            .await?;
        }
        NodeOperationStepKind::StartBackend => {
            let ready = observation.is_some_and(|value| {
                observation_ready(
                    snapshot,
                    value,
                    &ReadyExpectation {
                        service_kind: ServiceKind::Engine,
                        revision: slot.target_revision,
                        digest: &slot.target_digest,
                        previous_backend: &slot.pinned_backend_instance_id,
                        previous_process: &slot.pinned_process_instance_id,
                        reported_backend: &slot.effect_backend_instance_id,
                        reported_process: &slot.effect_process_instance_id,
                    },
                )
            });
            if !ready {
                if observation.is_some_and(|value| {
                    observation_after_delivery(value, slot.effect_delivered_at.as_ref())
                        && value.backend_state == BackendProcessState::Exited as i32
                        && value.observed_revision == slot.target_revision
                        && value.observed_digest == slot.target_digest
                        && value.backend_query_error.is_empty()
                }) {
                    client
                        .execute(
                            "UPDATE wr_node_operation_targets
                             SET effect_delivered_at = NULL, effect_ambiguous = FALSE
                             WHERE operation_id = $1 AND target_kind = 'engine_slot' AND target_key = $2",
                            &[&id, &slot.engine_slot],
                        )
                        .await
                        .map_err(internal)?;
                    return Ok(true);
                }
                return Ok(false);
            }
            advance_slot(
                client,
                id,
                &slot.engine_slot,
                next,
                slot.source_revision as i64,
            )
            .await?;
        }
        NodeOperationStepKind::VerifyTarget => {
            let proving_source =
                slot.source_revision > 0 && slot.pinned_process_instance_id.is_empty();
            let (expected_revision, expected_digest, previous_backend, previous_process) =
                if proving_source {
                    (slot.source_revision, slot.source_digest.as_str(), "", "")
                } else {
                    (
                        slot.target_revision,
                        slot.target_digest.as_str(),
                        slot.pinned_backend_instance_id.as_str(),
                        slot.pinned_process_instance_id.as_str(),
                    )
                };
            let ready = observation.is_some_and(|value| {
                observation_ready(
                    snapshot,
                    value,
                    &ReadyExpectation {
                        service_kind: ServiceKind::Engine,
                        revision: expected_revision,
                        digest: expected_digest,
                        previous_backend,
                        previous_process,
                        reported_backend: &slot.effect_backend_instance_id,
                        reported_process: &slot.effect_process_instance_id,
                    },
                )
            });
            if !ready
                || exact_registration_count(
                    snapshot,
                    &operation.node_id,
                    &slot.engine_slot,
                    expected_revision,
                    expected_digest,
                ) != 1
            {
                let query_failed =
                    observation.is_some_and(|value| !value.backend_query_error.is_empty());
                // The agent durably reports a completed verification before it
                // publishes the matching observation. Do not interpret that
                // intentional ordering as invalid source/target evidence.
                if observation.is_some() && (query_failed || slot.effect_reported) {
                    pause_with_condition(
                        client,
                        id,
                        &slot.engine_slot,
                        if proving_source {
                            "SOURCE_EVIDENCE_INVALID"
                        } else {
                            "TARGET_EVIDENCE_INVALID"
                        },
                        "exact fresh running backend, replacement identities, lifecycle READY, digest, and one registration are required",
                    )
                    .await?;
                }
                return Ok(false);
            }
            let observed = observation.expect("ready evidence is present");
            let lifecycle = observed
                .lifecycle
                .as_ref()
                .expect("ready evidence includes lifecycle status");
            if proving_source {
                let policy = operation
                    .policy
                    .as_ref()
                    .ok_or_else(|| Status::internal("stored rollout policy is missing"))?;
                if !stop_preserves_availability(snapshot, operation, &slot.engine_slot, policy)? {
                    pause_with_condition(
                        client,
                        id,
                        &slot.engine_slot,
                        "AVAILABILITY_POLICY_BLOCKED",
                        "the next withdrawal would exceed max_unavailable or remove the last healthy slot",
                    )
                    .await?;
                    return Ok(false);
                }
                client
                    .execute(
                        "UPDATE wr_node_operation_targets
                         SET pinned_backend_instance_id = $3, pinned_process_instance_id = $4
                         WHERE operation_id = $1 AND target_kind = 'engine_slot' AND target_key = $2",
                        &[
                            &id,
                            &slot.engine_slot,
                            &observed.backend_instance_id,
                            &lifecycle.process_instance_id,
                        ],
                    )
                    .await
                    .map_err(internal)?;
                advance_slot(
                    client,
                    id,
                    &slot.engine_slot,
                    Some(NodeOperationStepKind::StopBackend),
                    slot.source_revision as i64,
                )
                .await?;
                return Ok(true);
            }

            let already_authoritative = snapshot.slot_authorities.iter().any(|authority| {
                authority.node_id == operation.node_id
                    && authority.engine_slot == slot.engine_slot
                    && authority.revision == slot.target_revision
            });
            if already_authoritative && slot.source_revision != slot.target_revision {
                pause_with_condition(
                    client,
                    id,
                    &slot.engine_slot,
                    "PREMATURE_TARGET_AUTHORITY",
                    "target became authoritative before the manager switch transaction",
                )
                .await?;
                return Ok(false);
            }
            client
                .execute(
                    "UPDATE wr_node_operation_targets
                     SET pinned_backend_instance_id = $3, pinned_process_instance_id = $4
                     WHERE operation_id = $1 AND target_kind = 'engine_slot' AND target_key = $2",
                    &[
                        &id,
                        &slot.engine_slot,
                        &observed.backend_instance_id,
                        &lifecycle.process_instance_id,
                    ],
                )
                .await
                .map_err(internal)?;
            let after_target = if slot.source_revision == slot.target_revision {
                Some(NodeOperationStepKind::VerifyServing)
            } else {
                Some(NodeOperationStepKind::SwitchAuthority)
            };
            advance_slot(
                client,
                id,
                &slot.engine_slot,
                after_target,
                slot.source_revision as i64,
            )
            .await?;
        }
        NodeOperationStepKind::SwitchAuthority => {
            switch_authority(
                client,
                &operation.node_id,
                &slot.engine_slot,
                slot.target_revision as i64,
            )
            .await?;
            client
                .execute(
                    "WITH detail AS (
                         UPDATE wr_node_operation_engine_target_details
                         SET authority_switched = TRUE
                         WHERE operation_id = $1 AND target_kind = 'engine_slot' AND target_key = $2
                     )
                     UPDATE wr_node_operation_targets SET changed = TRUE
                     WHERE operation_id = $1 AND target_kind = 'engine_slot' AND target_key = $2",
                    &[&id, &slot.engine_slot],
                )
                .await
                .map_err(internal)?;
            advance_slot(
                client,
                id,
                &slot.engine_slot,
                next,
                slot.target_revision as i64,
            )
            .await?;
        }
        NodeOperationStepKind::VerifyServing => {
            if !slot_routes_healthy(
                snapshot,
                &operation.node_id,
                &slot.engine_slot,
                slot.target_revision,
                &slot.target_digest,
            )? {
                set_slot_condition(
                    client,
                    id,
                    &slot.engine_slot,
                    "SERVING_CONVERGENCE_PENDING",
                    "fresh required module heartbeats and exact healthy default routes have not converged under target authority",
                )
                .await?;
                return Ok(false);
            }
            client
                .execute(
                    "UPDATE wr_node_operation_engine_target_details SET serving_converged = TRUE
                     WHERE operation_id = $1 AND target_kind = 'engine_slot' AND target_key = $2",
                    &[&id, &slot.engine_slot],
                )
                .await
                .map_err(internal)?;
            advance_slot(
                client,
                id,
                &slot.engine_slot,
                None,
                slot.target_revision as i64,
            )
            .await?;
        }
        _ => return Ok(false),
    }
    clear_slot_condition(client, id, &slot.engine_slot).await?;
    Ok(true)
}

async fn reconcile<C: GenericClient + Sync>(
    client: &C,
    id: Uuid,
    actor: &str,
) -> Result<(), Status> {
    // The caller acquires the routing/evidence lock before establishing its
    // repeatable-read snapshot, so this capture cannot predate a completed publisher.
    let snapshot = crate::db::capture_cluster_status_snapshot(client).await?;
    let operation = snapshot
        .active_operations
        .iter()
        .find(|operation| operation.operation_id == id.to_string())
        .ok_or_else(|| Status::failed_precondition("operation is not active"))?;
    let state =
        NodeOperationState::try_from(operation.state).unwrap_or(NodeOperationState::Unspecified);
    if !matches!(
        state,
        NodeOperationState::Queued | NodeOperationState::Running
    ) {
        return Ok(());
    }
    let phase =
        NodeOperationPhase::try_from(operation.phase).unwrap_or(NodeOperationPhase::Unspecified);
    if !proxy_target(operation)?.complete {
        return Ok(());
    }
    let slots = operation_slots(operation)?;
    if let Some(slot) = slots
        .iter()
        .filter(|slot| !slot.complete)
        .min_by_key(|slot| slot.rollout_order)
    {
        if !reconcile_slot(client, id, &snapshot, operation, slot).await? {
            return Ok(());
        }
        return Ok(());
    }
    if phase == NodeOperationPhase::RestoringSource {
        let terminal = NodeOperationState::try_from(operation.restoration_terminal_state)
            .unwrap_or(NodeOperationState::Failed);
        let terminal = match terminal {
            NodeOperationState::Cancelled => "cancelled",
            _ => "failed",
        };
        client
            .execute(
                "UPDATE wr_node_operations SET state = $2, phase = 'complete',
                        lease_expires_at = NULL, claimed_by = NULL, agent_instance_id = NULL,
                        updated_at = NOW() WHERE operation_id = $1",
                &[&id, &terminal],
            )
            .await
            .map_err(internal)?;
        append_event(
            client,
            id,
            actor,
            "SOURCE_RESTORED",
            terminal,
            operation.lease_epoch as i64,
        )
        .await?;
        return Ok(());
    }
    let action =
        NodeOperationAction::try_from(operation.action).unwrap_or(NodeOperationAction::Unspecified);
    let commits = action != NodeOperationAction::Restart && operation.target_revision > 0;
    if commits {
        let target = operation.target_revision as i64;
        client
            .execute(
                "UPDATE wr_nodes SET current_revision = $2, target_revision = NULL,
                        updated_at = NOW() WHERE node_id = $1 AND target_revision = $2",
                &[&operation.node_id, &target],
            )
            .await
            .map_err(internal)?;
        client
            .execute(
                "UPDATE wr_node_deployments SET state = 'succeeded', completed_at = NOW()
                 WHERE node_id = $1 AND revision = $2 AND state IN ('pending', 'active')",
                &[&operation.node_id, &target],
            )
            .await
            .map_err(internal)?;
        fence_cleanup_authority(client, &operation.node_id, "SERVING_COMMIT").await?;
        client
            .execute(
                "UPDATE wr_node_operations SET committed = TRUE, committed_at = NOW(),
                        phase = 'complete', state = 'succeeded', lease_expires_at = NULL,
                        claimed_by = NULL, agent_instance_id = NULL, updated_at = NOW()
                 WHERE operation_id = $1",
                &[&id],
            )
            .await
            .map_err(internal)?;
        append_event(
            client,
            id,
            actor,
            "REVISION_COMMITTED",
            &target.to_string(),
            operation.lease_epoch as i64,
        )
        .await?;
    } else {
        client
            .execute(
                "UPDATE wr_node_operations SET state = 'succeeded', phase = 'complete',
                        lease_expires_at = NULL, claimed_by = NULL, agent_instance_id = NULL,
                        updated_at = NOW() WHERE operation_id = $1",
                &[&id],
            )
            .await
            .map_err(internal)?;
        append_event(
            client,
            id,
            actor,
            "OPERATION_SUCCEEDED",
            "",
            operation.lease_epoch as i64,
        )
        .await?;
    }
    Ok(())
}

struct CleanupPolicySnapshot {
    fingerprint: String,
    known: Vec<wr_common::wruntime::ReleaseInventoryEntry>,
    delete: Vec<wr_common::wruntime::ReleaseInventoryEntry>,
}

async fn manager_cleanup_policy<C>(
    client: &C,
    node_id: &str,
) -> Result<CleanupPolicySnapshot, Status>
where
    C: GenericClient + Sync,
{
    let policy = client
        .query_opt(
            "SELECT retention_count FROM wr_node_agent_policies WHERE node_id = $1",
            &[&node_id],
        )
        .await
        .map_err(internal)?
        .ok_or_else(|| Status::failed_precondition("node-agent retention policy is missing"))?;
    let retention_count = policy.get::<_, i32>("retention_count") as usize;
    let node = client
        .query_one(
            "SELECT current_revision, target_revision FROM wr_nodes WHERE node_id = $1",
            &[&node_id],
        )
        .await
        .map_err(internal)?;
    let current = node.get::<_, i64>("current_revision");
    let staged = node.get::<_, Option<i64>>("target_revision");
    let rows = client
        .query(
            "SELECT revision, bundle_digest, resolved_release_digest, state, abandoned_at
             FROM wr_node_deployments WHERE node_id = $1 ORDER BY revision DESC",
            &[&node_id],
        )
        .await
        .map_err(internal)?;
    let mut known = rows
        .iter()
        .map(|row| {
            (
                row.get::<_, i64>("revision"),
                (
                    row.get::<_, String>("bundle_digest"),
                    row.get::<_, String>("resolved_release_digest"),
                ),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    for deletion in client
        .query(
            "SELECT revision FROM wr_node_release_deletions WHERE node_id = $1",
            &[&node_id],
        )
        .await
        .map_err(internal)?
    {
        known.remove(&deletion.get::<_, i64>("revision"));
    }
    let mut protected = std::collections::BTreeSet::new();
    if current > 0 {
        protected.insert(current);
    }
    if let Some(staged) = staged {
        protected.insert(staged);
    }
    // Retention is explicitly historical: current is protected independently.
    protected.extend(
        rows.iter()
            .filter(|row| {
                row.get::<_, String>("state") == "succeeded"
                    && row.get::<_, i64>("revision") != current
                    && known.contains_key(&row.get::<_, i64>("revision"))
            })
            .take(retention_count)
            .map(|row| row.get::<_, i64>("revision")),
    );
    for row in client
        .query(
            "SELECT source_revision, target_revision FROM wr_node_operations
             WHERE node_id = $1 AND state IN ('queued', 'running', 'paused')",
            &[&node_id],
        )
        .await
        .map_err(internal)?
    {
        for revision in [
            row.get::<_, i64>("source_revision"),
            row.get::<_, i64>("target_revision"),
        ] {
            if revision > 0 {
                protected.insert(revision);
            }
        }
    }
    protected.extend(
        rows.iter()
            .filter(|row| {
                matches!(row.get::<_, String>("state").as_str(), "pending" | "active")
                    && row
                        .get::<_, Option<chrono::DateTime<chrono::Utc>>>("abandoned_at")
                        .is_none()
                    && known.contains_key(&row.get::<_, i64>("revision"))
            })
            .map(|row| row.get::<_, i64>("revision")),
    );
    protected.extend(
        client
            .query(
                "SELECT observed_revision FROM wr_node_slot_observations
                 WHERE node_id = $1 AND observed_revision > 0",
                &[&node_id],
            )
            .await
            .map_err(internal)?
            .into_iter()
            .map(|row| row.get::<_, i64>("observed_revision")),
    );
    protected.extend(
        client
            .query(
                "SELECT revision FROM wr_node_slot_authority
                 WHERE node_id = $1 AND authoritative",
                &[&node_id],
            )
            .await
            .map_err(internal)?
            .into_iter()
            .map(|row| row.get::<_, i64>("revision")),
    );
    let known = known
        .into_iter()
        .map(|(revision, (bundle_digest, resolved_release_digest))| {
            wr_common::wruntime::ReleaseInventoryEntry {
                revision: revision as u64,
                bundle_digest,
                resolved_release_digest,
            }
        })
        .collect::<Vec<_>>();
    let delete = known
        .iter()
        .filter(|release| !protected.contains(&(release.revision as i64)))
        .cloned()
        .collect::<Vec<_>>();
    let mut fingerprint_input = Vec::new();
    fingerprint_input.extend_from_slice(&(retention_count as u64).to_be_bytes());
    for revision in &protected {
        fingerprint_input.extend_from_slice(&revision.to_be_bytes());
    }
    for release in &known {
        fingerprint_input.extend_from_slice(&release.encode_to_vec());
    }
    Ok(CleanupPolicySnapshot {
        fingerprint: format!("sha256:{:x}", Sha256::digest(&fingerprint_input)),
        known,
        delete,
    })
}

pub async fn claim(
    pool: &Pool,
    node_id: &str,
    agent_instance_id: &str,
    agent: &str,
) -> Result<Option<ClaimOperationResponse>, Status> {
    if agent_instance_id.is_empty() {
        return Err(Status::invalid_argument("agent_instance_id is required"));
    }
    let mut client = pool.get().await.map_err(internal)?;
    let transaction = client.transaction().await.map_err(internal)?;
    transaction
        .batch_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .await
        .map_err(internal)?;
    acquire_evidence_lock(&transaction).await?;
    let attested = transaction
        .query_opt(
            "SELECT 1
             FROM wr_node_agent_attestations a
             JOIN wr_node_agent_policies p ON p.node_id = a.node_id
             WHERE a.node_id = $1 AND a.agent_instance_id = $2
               AND a.authenticated_principal = $3
               AND a.protocol_version = p.protocol_version
               AND a.binary_digest = p.binary_digest
               AND a.backend = p.backend
               AND p.capabilities <@ a.capabilities
               AND a.observed_at >= NOW() - INTERVAL '30 seconds'",
            &[&node_id, &agent_instance_id, &agent],
        )
        .await
        .map_err(internal)?
        .is_some();
    if !attested {
        return Err(Status::failed_precondition(
            "fresh authenticated node-agent attestation is required",
        ));
    }
    let expired = transaction
        .query(
            "UPDATE wr_node_operations SET state = 'paused', failure_code = 'LEASE_EXPIRED',
                    failure_detail = 'node-agent lease expired', claimed_by = NULL,
                    agent_instance_id = NULL, lease_expires_at = NULL, updated_at = NOW()
             WHERE node_id = $1 AND state = 'running' AND lease_expires_at <= NOW()
             RETURNING operation_id, lease_epoch",
            &[&node_id],
        )
        .await
        .map_err(internal)?;
    for row in expired {
        append_event(
            &transaction,
            row.get("operation_id"),
            "manager",
            "LEASE_EXPIRED",
            "explicit resume is required",
            row.get("lease_epoch"),
        )
        .await?;
    }
    let row = transaction
        .query_opt(
            "SELECT operation_id, state, lease_epoch, phase, forward_deadline
             FROM wr_node_operations
             WHERE node_id = $1 AND (
                 state = 'queued' OR
                 (state = 'running' AND agent_instance_id = $2 AND lease_expires_at > NOW())
             ) ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1",
            &[&node_id, &agent_instance_id],
        )
        .await
        .map_err(internal)?;
    let Some(row) = row else {
        transaction.commit().await.map_err(internal)?;
        return Ok(None);
    };
    let id: Uuid = row.get("operation_id");
    let phase: String = row.get("phase");
    if phase == "forward" && deadline_expired(&transaction, id).await? {
        enter_restoration(
            &transaction,
            id,
            "manager",
            "failed",
            "FORWARD_DEADLINE_EXPIRED",
            "forward deadline expired; forward execution is permanently fenced",
        )
        .await?;
        transaction.commit().await.map_err(internal)?;
        return Ok(None);
    }
    let state: String = row.get("state");
    let epoch: i64 = if state == "queued" {
        transaction
            .query_one(
                "UPDATE wr_node_operations
                 SET state = 'running', lease_epoch = lease_epoch + 1,
                     lease_expires_at = NOW() + make_interval(secs => $3),
                     claimed_by = $4, agent_instance_id = $2, updated_at = NOW()
                 WHERE operation_id = $1 RETURNING lease_epoch",
                &[&id, &agent_instance_id, &LEASE_SECONDS, &agent],
            )
            .await
            .map_err(internal)?
            .get("lease_epoch")
    } else {
        let epoch: i64 = row.get("lease_epoch");
        transaction
            .execute(
                "UPDATE wr_node_operations
                 SET lease_expires_at = NOW() + make_interval(secs => $2), updated_at = NOW()
                 WHERE operation_id = $1",
                &[&id, &LEASE_SECONDS],
            )
            .await
            .map_err(internal)?;
        epoch
    };
    reconcile(&transaction, id, agent).await?;
    let operation = load_operation(&transaction, id).await?;
    if NodeOperationState::try_from(operation.state).unwrap_or(NodeOperationState::Unspecified)
        != NodeOperationState::Running
    {
        transaction.commit().await.map_err(internal)?;
        return Ok(None);
    }
    let phase =
        NodeOperationPhase::try_from(operation.phase).unwrap_or(NodeOperationPhase::Unspecified);
    let proxy = proxy_target(&operation)?;
    let proxy_step = NodeOperationStepKind::try_from(proxy.next_step)
        .unwrap_or(NodeOperationStepKind::Unspecified);
    let proxy_pending = !proxy.complete;
    let (
        slot_name,
        step,
        revision,
        digest,
        resolved_digest,
        pinned_backend,
        pinned_process,
        delivered,
        ambiguous,
        target_kind,
    ) = if proxy_pending {
        let proving_source = proxy_step == NodeOperationStepKind::VerifyTarget;
        let uses_source = phase == NodeOperationPhase::RestoringSource
            || matches!(
                proxy_step,
                NodeOperationStepKind::InspectBackend | NodeOperationStepKind::StopBackend
            )
            || proving_source;
        (
            String::new(),
            proxy_step,
            if uses_source {
                proxy.source_revision
            } else {
                proxy.target_revision
            },
            if uses_source {
                proxy.source_digest.clone()
            } else {
                proxy.target_digest.clone()
            },
            if uses_source {
                proxy.source_resolved_release_digest.clone()
            } else {
                proxy.target_resolved_release_digest.clone()
            },
            proxy.pinned_backend_instance_id.clone(),
            proxy.pinned_process_instance_id.clone(),
            proxy.effect_delivered_at.is_some(),
            proxy.effect_ambiguous,
            InstructionTargetKind::Proxy,
        )
    } else if let Some(slot) = operation_slots(&operation)?
        .iter()
        .find(|slot| !slot.complete)
    {
        let step = NodeOperationStepKind::try_from(slot.next_step)
            .unwrap_or(NodeOperationStepKind::Unspecified);
        if matches!(
            step,
            NodeOperationStepKind::SwitchAuthority | NodeOperationStepKind::VerifyServing
        ) {
            transaction.commit().await.map_err(internal)?;
            return Ok(None);
        }
        let restoration = phase == NodeOperationPhase::RestoringSource;
        let proving_source = step == NodeOperationStepKind::VerifyTarget
            && slot.source_revision > 0
            && slot.pinned_process_instance_id.is_empty();
        let uses_source =
            restoration || step == NodeOperationStepKind::StopBackend || proving_source;
        (
            slot.engine_slot.clone(),
            step,
            if uses_source {
                slot.source_revision
            } else {
                slot.target_revision
            },
            if uses_source {
                slot.source_digest.clone()
            } else {
                slot.target_digest.clone()
            },
            if uses_source {
                slot.source_resolved_release_digest.clone()
            } else {
                slot.target_resolved_release_digest.clone()
            },
            slot.pinned_backend_instance_id.clone(),
            slot.pinned_process_instance_id.clone(),
            slot.effect_delivered_at.is_some(),
            slot.effect_ambiguous,
            InstructionTargetKind::EngineSlot,
        )
    } else {
        transaction.commit().await.map_err(internal)?;
        return Ok(None);
    };
    let revision_digest = if revision == 0 {
        String::new()
    } else {
        transaction
            .query_opt(
                "SELECT revision_digest FROM wr_node_deployments
                 WHERE node_id = $1 AND revision = $2",
                &[&node_id, &(revision as i64)],
            )
            .await
            .map_err(internal)?
            .ok_or_else(|| Status::failed_precondition("instruction deployment is missing"))?
            .get("revision_digest")
    };
    let mutating_effect = matches!(
        step,
        NodeOperationStepKind::StopBackend
            | NodeOperationStepKind::SelectRelease
            | NodeOperationStepKind::StartBackend
            | NodeOperationStepKind::RestoreSource
    );
    let inspection = (mutating_effect && delivered) || ambiguous;
    let instruction_step = if inspection {
        NodeOperationStepKind::InspectBackend
    } else {
        step
    };
    if !target_step_compatible(target_kind, instruction_step) {
        return Err(Status::internal(
            "stored target kind and step are incompatible",
        ));
    }
    if mutating_effect && !inspection {
        if target_kind == InstructionTargetKind::Proxy {
            transaction
                .execute(
                    "UPDATE wr_node_operation_targets
                     SET effect_delivered_at = NOW(), effect_ambiguous = TRUE,
                         updated_at = NOW()
                     WHERE operation_id = $1 AND target_kind = 'proxy'",
                    &[&id],
                )
                .await
                .map_err(internal)?;
        } else {
            transaction
                .execute(
                    "UPDATE wr_node_operation_targets
                     SET effect_delivered_at = NOW(), effect_ambiguous = TRUE, updated_at = NOW()
                     WHERE operation_id = $1 AND target_kind = 'engine_slot' AND target_key = $2",
                    &[&id, &slot_name],
                )
                .await
                .map_err(internal)?;
        }
    }
    append_event(
        &transaction,
        id,
        agent,
        "LEASE_CLAIMED",
        &format!("{slot_name}:{}", step_name(instruction_step)),
        epoch,
    )
    .await?;
    let deadline = operation.forward_deadline;
    let restoration = phase == NodeOperationPhase::RestoringSource;
    transaction.commit().await.map_err(internal)?;
    Ok(Some(ClaimOperationResponse {
        instruction: Some(AgentInstruction {
            operation_id: id.to_string(),
            node_id: node_id.to_string(),
            lease_epoch: epoch as u64,
            step: instruction_step as i32,
            operation_deadline: if restoration { None } else { deadline },
            agent_instance_id: agent_instance_id.to_string(),
            target: Some(InstructionTarget {
                kind: target_kind as i32,
                revision,
                bundle_digest: digest,
                resolved_release_digest: resolved_digest,
                identity: Some(match target_kind {
                    InstructionTargetKind::Proxy => {
                        instruction_target::Identity::Proxy(ProxyTargetIdentity {})
                    }
                    InstructionTargetKind::EngineSlot => {
                        instruction_target::Identity::EngineSlotTarget(EngineSlotTargetIdentity {
                            engine_slot: slot_name,
                        })
                    }
                    _ => return Err(Status::internal("invalid workload target kind")),
                }),
            }),
            pinned_backend_instance_id: pinned_backend,
            pinned_process_instance_id: pinned_process,
            restoration,
            revision_digest,
        }),
        lease_seconds: LEASE_SECONDS as u64,
    }))
}

pub async fn renew(
    pool: &Pool,
    node_id: &str,
    operation_id: &str,
    epoch: u64,
    agent_instance_id: &str,
    agent: &str,
) -> Result<prost_types::Timestamp, Status> {
    let id = Uuid::parse_str(operation_id)
        .map_err(|_| Status::invalid_argument("operation_id must be a UUID"))?;
    let epoch =
        i64::try_from(epoch).map_err(|_| Status::invalid_argument("lease epoch is too large"))?;
    let mut client = pool.get().await.map_err(internal)?;
    let transaction = client.transaction().await.map_err(internal)?;
    let row = transaction
        .query_opt(
            "SELECT phase, forward_deadline FROM wr_node_operations
             WHERE operation_id = $1 AND node_id = $2 AND state = 'running'
               AND lease_epoch = $3 AND claimed_by = $4 AND agent_instance_id = $5
               AND lease_expires_at > NOW() FOR UPDATE",
            &[&id, &node_id, &epoch, &agent, &agent_instance_id],
        )
        .await
        .map_err(internal)?
        .ok_or_else(|| {
            Status::aborted("operation lease is stale, expired, or activation-mismatched")
        })?;
    if row.get::<_, String>("phase") == "forward" && deadline_expired(&transaction, id).await? {
        enter_restoration(
            &transaction,
            id,
            "manager",
            "failed",
            "FORWARD_DEADLINE_EXPIRED",
            "forward deadline expired; forward execution is permanently fenced",
        )
        .await?;
        transaction.commit().await.map_err(internal)?;
        return Err(Status::deadline_exceeded(
            "forward operation deadline expired",
        ));
    }
    let lease_expires_at: chrono::DateTime<chrono::Utc> = transaction
        .query_one(
            "UPDATE wr_node_operations
             SET lease_expires_at = NOW() + make_interval(secs => $2), updated_at = NOW()
             WHERE operation_id = $1 RETURNING lease_expires_at",
            &[&id, &LEASE_SECONDS],
        )
        .await
        .map_err(internal)?
        .get("lease_expires_at");
    transaction.commit().await.map_err(internal)?;
    Ok(timestamp(lease_expires_at))
}

pub(crate) async fn observations_from_client<C>(
    client: &C,
    node_id: &str,
    engine_slot: &str,
) -> Result<Vec<SlotObservation>, Status>
where
    C: GenericClient + Sync,
{
    client
        .query(
            "SELECT node_id, engine_slot, lifecycle_status, backend_state,
                    backend_instance_id, observed_revision, observed_at, observed_digest,
                    observed_resolved_digest, backend_query_error, operation_id,
                    agent_instance_id, lease_epoch,
                    (SELECT effect_termination_evidence
                       FROM wr_node_operation_targets effects
                      WHERE effects.operation_id = wr_node_slot_observations.operation_id
                        AND effects.target_kind = 'engine_slot'
                        AND effects.target_key = wr_node_slot_observations.engine_slot)
                        AS termination_evidence
             FROM wr_node_slot_observations
             WHERE ($1 = '' OR node_id = $1) AND ($2 = '' OR engine_slot = $2)
             ORDER BY node_id, engine_slot",
            &[&node_id, &engine_slot],
        )
        .await
        .map_err(internal)?
        .into_iter()
        .map(|row| {
            let observed_at: chrono::DateTime<chrono::Utc> = row.get("observed_at");
            let stale = chrono::Utc::now()
                .signed_duration_since(observed_at)
                .num_seconds()
                > EVIDENCE_FRESH_SECONDS;
            let lifecycle = row
                .get::<_, Option<Vec<u8>>>("lifecycle_status")
                .map(|bytes| wr_common::wruntime::LifecycleStatus::decode(bytes.as_slice()))
                .transpose()
                .map_err(|error| {
                    Status::internal(format!("stored lifecycle observation is invalid: {error}"))
                })?;
            let backend_state = if stale {
                BackendProcessState::Unspecified
            } else {
                match row.get::<_, String>("backend_state").as_str() {
                    "running" => BackendProcessState::Running,
                    "exited" => BackendProcessState::Exited,
                    "query_error" => BackendProcessState::QueryError,
                    _ => BackendProcessState::Unspecified,
                }
            };
            Ok(SlotObservation {
                node_id: row.get("node_id"),
                engine_slot: row.get("engine_slot"),
                lifecycle: if stale { None } else { lifecycle },
                backend_state: backend_state as i32,
                backend_instance_id: row.get("backend_instance_id"),
                observed_revision: row.get::<_, i64>("observed_revision") as u64,
                observed_at: Some(timestamp(observed_at)),
                observed_digest: row.get("observed_digest"),
                backend_query_error: row.get("backend_query_error"),
                operation_id: row
                    .get::<_, Option<Uuid>>("operation_id")
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                agent_instance_id: row.get("agent_instance_id"),
                lease_epoch: row.get::<_, i64>("lease_epoch") as u64,
                observed_resolved_release_digest: row.get("observed_resolved_digest"),
                termination_evidence: decode_termination_evidence(row.get("termination_evidence"))?,
            })
        })
        .collect()
}

pub async fn observations(
    pool: &Pool,
    node_id: &str,
    engine_slot: &str,
) -> Result<Vec<SlotObservation>, Status> {
    let client = pool.get().await.map_err(internal)?;
    observations_from_client(&client, node_id, engine_slot).await
}

pub async fn report_observation(
    pool: &Pool,
    request: &ReportNodeObservationRequest,
    agent: &str,
) -> Result<NodeOperation, Status> {
    let id = Uuid::parse_str(&request.operation_id)
        .map_err(|_| Status::invalid_argument("operation_id must be a UUID"))?;
    let epoch = i64::try_from(request.lease_epoch)
        .map_err(|_| Status::invalid_argument("lease epoch is too large"))?;
    // Freshness is manager-received time. A node agent cannot extend evidence
    // lifetime by supplying a future timestamp.
    let observed_at = chrono::Utc::now();
    let lifecycle = request.lifecycle.as_ref().map(Message::encode_to_vec);
    let backend = BackendProcessState::try_from(request.backend_state)
        .unwrap_or(BackendProcessState::Unspecified);
    let backend_state = match backend {
        BackendProcessState::Running => "running",
        BackendProcessState::Exited => "exited",
        BackendProcessState::QueryError => "query_error",
        BackendProcessState::Unspecified => "unknown",
    };
    if backend == BackendProcessState::QueryError && request.backend_query_error.is_empty() {
        return Err(Status::invalid_argument(
            "backend query errors require typed error evidence",
        ));
    }
    let revision = i64::try_from(request.observed_revision)
        .map_err(|_| Status::invalid_argument("observed_revision is too large"))?;
    let mut client = pool.get().await.map_err(internal)?;
    let transaction = client.transaction().await.map_err(internal)?;
    transaction
        .batch_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .await
        .map_err(internal)?;
    acquire_evidence_lock(&transaction).await?;
    let operation = transaction
        .query_opt(
            "SELECT phase FROM wr_node_operations
             WHERE operation_id = $1 AND node_id = $2 AND state = 'running'
               AND lease_epoch = $3 AND claimed_by = $4 AND agent_instance_id = $5
               AND lease_expires_at > NOW() FOR UPDATE",
            &[
                &id,
                &request.node_id,
                &epoch,
                &agent,
                &request.agent_instance_id,
            ],
        )
        .await
        .map_err(internal)?
        .ok_or_else(|| {
            Status::aborted("observation has a stale, expired, or activation-mismatched lease")
        })?;
    let prior_observation = transaction
        .query_opt(
            "SELECT observed_revision, observed_digest, observed_resolved_digest
             FROM wr_node_slot_observations WHERE node_id = $1 AND engine_slot = $2 FOR UPDATE",
            &[&request.node_id, &request.engine_slot],
        )
        .await
        .map_err(internal)?;
    let protection_changed = prior_observation.as_ref().is_none_or(|row| {
        row.get::<_, i64>("observed_revision") != revision
            || row.get::<_, String>("observed_digest") != request.observed_digest
            || row.get::<_, String>("observed_resolved_digest")
                != request.observed_resolved_release_digest
    });
    if operation.get::<_, String>("phase") == "forward"
        && deadline_expired(&transaction, id).await?
    {
        enter_restoration(
            &transaction,
            id,
            "manager",
            "failed",
            "FORWARD_DEADLINE_EXPIRED",
            "forward deadline expired; forward execution is permanently fenced",
        )
        .await?;
        transaction.commit().await.map_err(internal)?;
        return Err(Status::deadline_exceeded(
            "forward operation deadline expired",
        ));
    }
    transaction
        .execute(
            "INSERT INTO wr_node_slot_observations
               (node_id, engine_slot, lifecycle_status, backend_state, backend_instance_id,
                observed_revision, observed_at, observed_digest, observed_resolved_digest,
                backend_query_error, operation_id, agent_instance_id, lease_epoch)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
             ON CONFLICT (node_id, engine_slot) DO UPDATE SET
               lifecycle_status = EXCLUDED.lifecycle_status,
               backend_state = EXCLUDED.backend_state,
               backend_instance_id = EXCLUDED.backend_instance_id,
               observed_revision = EXCLUDED.observed_revision,
               observed_at = EXCLUDED.observed_at,
               observed_digest = EXCLUDED.observed_digest,
               observed_resolved_digest = EXCLUDED.observed_resolved_digest,
               backend_query_error = EXCLUDED.backend_query_error,
               operation_id = EXCLUDED.operation_id,
               agent_instance_id = EXCLUDED.agent_instance_id,
               lease_epoch = EXCLUDED.lease_epoch
             WHERE wr_node_slot_observations.observed_at <= EXCLUDED.observed_at",
            &[
                &request.node_id,
                &request.engine_slot,
                &lifecycle,
                &backend_state,
                &request.backend_instance_id,
                &revision,
                &observed_at,
                &request.observed_digest,
                &request.observed_resolved_release_digest,
                &request.backend_query_error,
                &id,
                &request.agent_instance_id,
                &epoch,
            ],
        )
        .await
        .map_err(internal)?;
    if protection_changed {
        fence_cleanup_authority(&transaction, &request.node_id, "OBSERVATION_CHANGED").await?;
    }
    reconcile(&transaction, id, agent).await?;
    let result = load_operation(&transaction, id).await?;
    transaction.commit().await.map_err(internal)?;
    Ok(result)
}

pub async fn report_step(
    pool: &Pool,
    request: &ReportStepResultRequest,
    agent: &str,
) -> Result<NodeOperation, Status> {
    let id = Uuid::parse_str(&request.operation_id)
        .map_err(|_| Status::invalid_argument("operation_id must be a UUID"))?;
    let epoch = i64::try_from(request.lease_epoch)
        .map_err(|_| Status::invalid_argument("lease epoch is too large"))?;
    let reported_step =
        NodeOperationStepKind::try_from(request.step).unwrap_or(NodeOperationStepKind::Unspecified);
    let report_target = request
        .target
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("result target is required"))?;
    let (target_kind, target_key) = instruction_target_key(report_target)?;
    let target_kind_value = InstructionTargetKind::try_from(report_target.kind)
        .unwrap_or(InstructionTargetKind::Unspecified);
    if !target_step_compatible(target_kind_value, reported_step) {
        return Err(Status::invalid_argument(
            "target kind does not support the reported step",
        ));
    }
    let result_payload = request.encode_to_vec();
    let termination_evidence = request
        .termination_evidence
        .as_ref()
        .map(Message::encode_to_vec);
    let is_stop_result = reported_step == NodeOperationStepKind::StopBackend;
    let mut client = pool.get().await.map_err(internal)?;
    let transaction = client.transaction().await.map_err(internal)?;
    transaction
        .batch_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .await
        .map_err(internal)?;
    acquire_evidence_lock(&transaction).await?;
    if let Some(receipt) = transaction
        .query_opt(
            "SELECT authenticated_principal, result_payload
             FROM wr_node_operation_result_receipts
             WHERE operation_id = $1 AND node_id = $2 AND agent_instance_id = $3
               AND lease_epoch = $4 AND step = $5
               AND target_kind = $6 AND target_key = $7 FOR UPDATE",
            &[
                &id,
                &request.node_id,
                &request.agent_instance_id,
                &epoch,
                &request.step,
                &target_kind,
                &target_key,
            ],
        )
        .await
        .map_err(internal)?
    {
        let receipt_actor: String = receipt.get("authenticated_principal");
        let receipt_payload: Vec<u8> = receipt.get("result_payload");
        if receipt_actor != agent || receipt_payload != result_payload {
            return Err(Status::aborted(
                "result retry conflicts with the accepted instruction receipt",
            ));
        }
        let result = load_operation(&transaction, id).await?;
        transaction.commit().await.map_err(internal)?;
        return Ok(result);
    }
    let operation = transaction
        .query_opt(
            "SELECT o.phase, p.pinned_process_instance_id AS proxy_process_instance_id
             FROM wr_node_operations o
             JOIN wr_node_operation_targets p
               ON p.operation_id = o.operation_id AND p.target_kind = 'proxy'
             WHERE o.operation_id = $1 AND o.node_id = $2 AND o.state = 'running'
               AND o.lease_epoch = $3 AND o.claimed_by = $4 AND o.agent_instance_id = $5
               AND o.lease_expires_at > NOW() FOR UPDATE OF o, p",
            &[
                &id,
                &request.node_id,
                &epoch,
                &agent,
                &request.agent_instance_id,
            ],
        )
        .await
        .map_err(internal)?
        .ok_or_else(|| {
            Status::aborted("result has a stale, expired, or activation-mismatched lease")
        })?;
    let phase: String = operation.get("phase");
    if phase == "forward" && deadline_expired(&transaction, id).await? {
        enter_restoration(
            &transaction,
            id,
            "manager",
            "failed",
            "FORWARD_DEADLINE_EXPIRED",
            "forward deadline expired; forward execution is permanently fenced",
        )
        .await?;
        transaction.commit().await.map_err(internal)?;
        return Err(Status::deadline_exceeded(
            "forward operation deadline expired",
        ));
    }
    if target_kind == "proxy" {
        let proxy = transaction
            .query_one(
                "SELECT next_step, source_revision, source_digest, source_resolved_digest,
                        target_revision, target_digest, target_resolved_digest
                 FROM wr_node_operation_targets
                 WHERE operation_id = $1 AND target_kind = 'proxy' AND target_key = 'proxy'
                   AND NOT complete FOR UPDATE",
                &[&id],
            )
            .await
            .map_err(internal)?;
        let expected = parse_step(proxy.get::<_, String>("next_step").as_str())?;
        if expected == NodeOperationStepKind::Unspecified || expected != reported_step {
            return Err(Status::aborted(format!(
                "stale proxy result: expected {}, received {}",
                step_name(expected),
                step_name(reported_step)
            )));
        }
        let source_revision: i64 = proxy.get("source_revision");
        let target_revision: i64 = proxy.get("target_revision");
        let proving_source = reported_step == NodeOperationStepKind::VerifyTarget;
        let uses_source = matches!(
            reported_step,
            NodeOperationStepKind::InspectBackend
                | NodeOperationStepKind::StopBackend
                | NodeOperationStepKind::RestoreSource
        ) || proving_source;
        let (expected_revision, expected_digest, expected_resolved): (i64, String, String) =
            if uses_source {
                (
                    source_revision,
                    proxy.get("source_digest"),
                    proxy.get("source_resolved_digest"),
                )
            } else {
                (
                    target_revision,
                    proxy.get("target_digest"),
                    proxy.get("target_resolved_digest"),
                )
            };
        let mut condition_code = request.condition_code.clone();
        let mut detail = request.detail.clone();
        if condition_code.is_empty() && !request.backend_query_error.is_empty() {
            condition_code = "BACKEND_QUERY_ERROR".into();
            detail = request.backend_query_error.clone();
        } else if condition_code.is_empty()
            && (request.observed_revision != expected_revision as u64
                || request.observed_digest != expected_digest
                || request.observed_resolved_release_digest != expected_resolved)
        {
            condition_code = "RELEASE_EVIDENCE_MISMATCH".into();
            detail = "proxy evidence does not match the immutable instruction triple".into();
        } else if condition_code.is_empty()
            && matches!(
                reported_step,
                NodeOperationStepKind::VerifyTarget | NodeOperationStepKind::VerifyProxy
            )
            && request.process_instance_id.is_empty()
        {
            condition_code = "PROXY_EVIDENCE_MISSING".into();
            detail = "proxy verification requires a process identity".into();
        }
        if !condition_code.is_empty() {
            transaction
                .execute(
                    "WITH target AS (
                         UPDATE wr_node_operation_targets
                         SET effect_condition_code = $2, effect_detail = $3, updated_at = NOW()
                         WHERE operation_id = $1 AND target_kind = 'proxy'
                     )
                     UPDATE wr_node_operations SET state = 'paused', failure_code = $2,
                            failure_detail = $3, lease_expires_at = NULL,
                            claimed_by = NULL, agent_instance_id = NULL, updated_at = NOW()
                     WHERE operation_id = $1",
                    &[&id, &condition_code, &detail],
                )
                .await
                .map_err(internal)?;
        } else {
            let next = match reported_step {
                NodeOperationStepKind::InspectBackend => NodeOperationStepKind::StopBackend,
                NodeOperationStepKind::StopBackend => NodeOperationStepKind::SelectRelease,
                NodeOperationStepKind::SelectRelease => NodeOperationStepKind::StartBackend,
                NodeOperationStepKind::StartBackend => NodeOperationStepKind::VerifyProxy,
                NodeOperationStepKind::VerifyTarget => NodeOperationStepKind::StopBackend,
                NodeOperationStepKind::VerifyProxy | NodeOperationStepKind::RestoreSource => {
                    NodeOperationStepKind::Unspecified
                }
                _ => return Err(Status::aborted("invalid proxy operation step")),
            };
            let changed = matches!(
                reported_step,
                NodeOperationStepKind::StopBackend
                    | NodeOperationStepKind::SelectRelease
                    | NodeOperationStepKind::StartBackend
                    | NodeOperationStepKind::RestoreSource
            );
            transaction
                .execute(
                    "UPDATE wr_node_operation_targets
                     SET next_step = $2, complete = $2 = 'complete', changed = changed OR $3,
                         effect_ambiguous = FALSE, effect_reported = TRUE,
                         effect_observed_revision = $4, effect_observed_digest = $5,
                         effect_observed_resolved_digest = $6,
                         effect_backend_instance_id = $7, effect_process_instance_id = $8,
                         pinned_backend_instance_id = CASE
                             WHEN $7 <> '' THEN $7 ELSE pinned_backend_instance_id END,
                         pinned_process_instance_id = CASE
                             WHEN $8 <> '' THEN $8 ELSE pinned_process_instance_id END,
                         effect_condition_code = '', effect_detail = '',
                         effect_delivered_at = NULL,
                         effect_termination_evidence = CASE
                             WHEN $9 THEN $10 ELSE effect_termination_evidence END,
                         updated_at = NOW()
                     WHERE operation_id = $1 AND target_kind = 'proxy'",
                    &[
                        &id,
                        &step_name(next),
                        &changed,
                        &i64::try_from(request.observed_revision).map_err(|_| {
                            Status::invalid_argument("observed_revision is too large")
                        })?,
                        &request.observed_digest,
                        &request.observed_resolved_release_digest,
                        &request.backend_instance_id,
                        &request.process_instance_id,
                        &is_stop_result,
                        &termination_evidence,
                    ],
                )
                .await
                .map_err(internal)?;
            reconcile(&transaction, id, agent).await?;
        }
    } else {
        let slot = transaction
            .query_opt(
                "SELECT next_step, source_revision, source_digest, source_resolved_digest,
                        target_revision, target_digest, target_resolved_digest,
                        pinned_backend_instance_id, pinned_process_instance_id
                 FROM wr_node_operation_targets
                 WHERE operation_id = $1 AND target_kind = 'engine_slot'
                   AND target_key = $2 AND NOT complete FOR UPDATE",
                &[&id, &target_key],
            )
            .await
            .map_err(internal)?
            .ok_or_else(|| Status::failed_precondition("operation slot is complete or unknown"))?;
        let expected = parse_step(slot.get::<_, String>("next_step").as_str())?;
        if expected != reported_step {
            return Err(Status::aborted(format!(
                "stale step result: expected {}, received {}",
                step_name(expected),
                step_name(reported_step)
            )));
        }
        let source_revision: i64 = slot.get("source_revision");
        let source_digest: String = slot.get("source_digest");
        let source_resolved_digest: String = slot.get("source_resolved_digest");
        let target_revision: i64 = slot.get("target_revision");
        let target_digest: String = slot.get("target_digest");
        let target_resolved_digest: String = slot.get("target_resolved_digest");
        let pinned_backend: String = slot.get("pinned_backend_instance_id");
        let pinned_process: String = slot.get("pinned_process_instance_id");
        let uses_source = matches!(
            reported_step,
            NodeOperationStepKind::StopBackend | NodeOperationStepKind::RestoreSource
        ) || (reported_step == NodeOperationStepKind::VerifyTarget
            && source_revision > 0
            && pinned_process.is_empty());
        let (expected_revision, expected_digest, expected_resolved_digest) = if uses_source {
            (
                source_revision,
                source_digest.as_str(),
                source_resolved_digest.as_str(),
            )
        } else {
            (
                target_revision,
                target_digest.as_str(),
                target_resolved_digest.as_str(),
            )
        };
        let pinned_proxy: String = operation.get("proxy_process_instance_id");
        let (condition_code, detail) = if !request.condition_code.is_empty() {
            (request.condition_code.as_str(), request.detail.as_str())
        } else if !request.backend_query_error.is_empty() {
            ("BACKEND_QUERY_ERROR", request.backend_query_error.as_str())
        } else if matches!(
            reported_step,
            NodeOperationStepKind::VerifyReleaseMetadata
                | NodeOperationStepKind::StopBackend
                | NodeOperationStepKind::SelectRelease
                | NodeOperationStepKind::StartBackend
                | NodeOperationStepKind::VerifyTarget
                | NodeOperationStepKind::RestoreSource
        ) && (request.observed_revision != expected_revision as u64
            || request.observed_digest != expected_digest
            || request.observed_resolved_release_digest != expected_resolved_digest)
        {
            (
                "RELEASE_EVIDENCE_MISMATCH",
                "reported revision/digest do not match the immutable instruction snapshot",
            )
        } else if reported_step == NodeOperationStepKind::VerifyProxy
            && request.process_instance_id.is_empty()
        {
            (
                "PROXY_EVIDENCE_MISSING",
                "proxy verification requires an activation process identity",
            )
        } else if reported_step == NodeOperationStepKind::VerifyProxy
            && !pinned_proxy.is_empty()
            && request.process_instance_id != pinned_proxy
        {
            (
                "PROXY_IDENTITY_CHANGED",
                "proxy process identity differs from the operation's pinned activation",
            )
        } else if reported_step == NodeOperationStepKind::StopBackend
            && request.backend_instance_id != pinned_backend
        {
            (
                "BACKEND_IDENTITY_MISMATCH",
                "stop result must identify the exact pinned backend instance",
            )
        } else if matches!(
            reported_step,
            NodeOperationStepKind::StopBackend
                | NodeOperationStepKind::StartBackend
                | NodeOperationStepKind::RestoreSource
        ) && request.backend_instance_id.is_empty()
        {
            (
                "BACKEND_IDENTITY_MISSING",
                "typed backend effects require the exact resulting backend instance identity",
            )
        } else if (reported_step == NodeOperationStepKind::StartBackend
            || (reported_step == NodeOperationStepKind::RestoreSource && source_revision > 0))
            && request.process_instance_id.is_empty()
        {
            (
                "PROCESS_IDENTITY_MISSING",
                "start and restoration effects require the exact resulting process identity",
            )
        } else {
            ("", request.detail.as_str())
        };
        transaction
            .execute(
                "UPDATE wr_node_operation_targets
                 SET effect_reported = TRUE, effect_condition_code = $3, effect_detail = $4,
                     effect_observed_revision = $5, effect_observed_digest = $6,
                     effect_observed_resolved_digest = $9,
                     effect_backend_instance_id = $7, effect_process_instance_id = $8,
                     effect_termination_evidence = CASE
                         WHEN $10 THEN $11 ELSE effect_termination_evidence END,
                     updated_at = NOW() WHERE operation_id = $1
                       AND target_kind = 'engine_slot' AND target_key = $2",
                &[
                    &id,
                    &target_key,
                    &condition_code,
                    &detail,
                    &i64::try_from(request.observed_revision)
                        .map_err(|_| Status::invalid_argument("observed_revision is too large"))?,
                    &request.observed_digest,
                    &request.backend_instance_id,
                    &request.process_instance_id,
                    &request.observed_resolved_release_digest,
                    &is_stop_result,
                    &termination_evidence,
                ],
            )
            .await
            .map_err(internal)?;
        if reported_step == NodeOperationStepKind::VerifyProxy && condition_code.is_empty() {
            transaction
                .execute(
                    "UPDATE wr_node_operation_targets
                     SET pinned_process_instance_id = CASE
                         WHEN pinned_process_instance_id = '' THEN $2
                         ELSE pinned_process_instance_id END,
                         updated_at = NOW()
                     WHERE operation_id = $1 AND target_kind = 'proxy'",
                    &[&id, &request.process_instance_id],
                )
                .await
                .map_err(internal)?;
        }
        append_event(
            &transaction,
            id,
            agent,
            "EFFECT_REPORTED",
            &format!("{}:{}", target_key, step_name(reported_step)),
            epoch,
        )
        .await?;
        reconcile(&transaction, id, agent).await?;
    }
    transaction
        .execute(
            "INSERT INTO wr_node_operation_result_receipts
               (operation_id, node_id, agent_instance_id, lease_epoch, step,
                target_kind, target_key, authenticated_principal, result_payload)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            &[
                &id,
                &request.node_id,
                &request.agent_instance_id,
                &epoch,
                &request.step,
                &target_kind,
                &target_key,
                &agent,
                &result_payload,
            ],
        )
        .await
        .map_err(internal)?;
    let result = load_operation(&transaction, id).await?;
    transaction.commit().await.map_err(internal)?;
    Ok(result)
}

fn cleanup_state(value: &str) -> Result<NodeCleanupState, Status> {
    match value {
        "clean" => Ok(NodeCleanupState::Clean),
        "needs_reconcile" => Ok(NodeCleanupState::NeedsReconcile),
        "pending" => Ok(NodeCleanupState::Pending),
        "claimed" => Ok(NodeCleanupState::Claimed),
        "paused" => Ok(NodeCleanupState::Paused),
        _ => Err(Status::internal("stored cleanup state is invalid")),
    }
}

fn cleanup_summary_from_row(row: &Row) -> Result<NodeCleanupSummary, Status> {
    let inventory = row
        .get::<_, Option<Vec<u8>>>("known_inventory")
        .map(|bytes| NodeCleanupAuthority::decode(bytes.as_slice()))
        .transpose()
        .map_err(|error| Status::internal(format!("stored cleanup inventory is invalid: {error}")))?
        .map(|value| value.known_inventory.len() as u32)
        .unwrap_or_default();
    Ok(NodeCleanupSummary {
        node_id: row.get("node_id"),
        state: cleanup_state(row.get::<_, String>("state").as_str())? as i32,
        generation: row.get::<_, i64>("generation") as u64,
        candidate_count: row.get::<_, i32>("candidate_count") as u32,
        last_reconciled_at: row
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>("last_reconciled_at")
            .map(timestamp),
        next_action_at: row
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>("next_reconcile_at")
            .map(timestamp),
        inventory_count: inventory,
        diagnostic_code: row.get("diagnostic_code"),
        diagnostic_detail: row.get("diagnostic_detail"),
    })
}

const CLEANUP_ROW_COLUMNS: &str = "node_id, generation, state, protection_fingerprint, authority_payload, payload_digest, known_inventory, candidate_count, agent_instance_id, claimed_by, claim_instance, lease_epoch, lease_expires_at, delivered_at, last_reconciled_at, next_reconcile_at, last_attempt_at, diagnostic_code, diagnostic_detail";

pub async fn get_node_cleanup_status(
    pool: &Pool,
    node_id: &str,
) -> Result<NodeCleanupSummary, Status> {
    if node_id.is_empty() {
        return Err(Status::invalid_argument("node_id is required"));
    }
    let client = pool.get().await.map_err(internal)?;
    let query =
        format!("SELECT {CLEANUP_ROW_COLUMNS} FROM wr_node_release_cleanup WHERE node_id = $1");
    let row = client
        .query_opt(&query, &[&node_id])
        .await
        .map_err(internal)?
        .ok_or_else(|| Status::not_found("node cleanup state not found"))?;
    cleanup_summary_from_row(&row)
}

pub async fn retry_node_cleanup(
    pool: &Pool,
    node_id: &str,
    observed_generation: u64,
) -> Result<NodeCleanupSummary, Status> {
    let generation = i64::try_from(observed_generation)
        .map_err(|_| Status::invalid_argument("observed generation is too large"))?;
    let mut client = pool.get().await.map_err(internal)?;
    let transaction = client.transaction().await.map_err(internal)?;
    let query = format!(
        "UPDATE wr_node_release_cleanup SET generation = generation + 1,
             state = 'needs_reconcile', authority_payload = NULL, payload_digest = '',
             candidate_count = 0, agent_instance_id = NULL, claimed_by = NULL,
             claim_instance = NULL, lease_expires_at = NULL, delivered_at = NULL,
             next_reconcile_at = NOW(), diagnostic_code = '', diagnostic_detail = '', updated_at = NOW()
         WHERE node_id = $1 AND generation = $2 AND state = 'paused'
         RETURNING {CLEANUP_ROW_COLUMNS}"
    );
    let row = transaction
        .query_opt(&query, &[&node_id, &generation])
        .await
        .map_err(internal)?
        .ok_or_else(|| {
            Status::failed_precondition("cleanup generation changed or is not paused")
        })?;
    transaction.execute(
        "UPDATE wr_node_release_cleanup_generations SET outcome = 'superseded', completed_at = NOW()
         WHERE node_id = $1 AND generation = $2 AND outcome IN ('materialized', 'paused')",
        &[&node_id, &generation],
    ).await.map_err(internal)?;
    let summary = cleanup_summary_from_row(&row)?;
    transaction.commit().await.map_err(internal)?;
    Ok(summary)
}

pub async fn reconcile_node_cleanup_batch(
    pool: &Pool,
    _manager_id: &str,
    batch_size: i64,
) -> Result<u64, Status> {
    let mut client = pool.get().await.map_err(internal)?;
    let transaction = client.transaction().await.map_err(internal)?;
    // SKIP LOCKED scanners must observe rows committed by a concurrent scanner
    // instead of failing the whole periodic pass with SQLSTATE 40001.
    transaction
        .batch_execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        .await
        .map_err(internal)?;
    transaction
        .execute(
            "INSERT INTO wr_node_release_cleanup (node_id)
         SELECT node_id FROM wr_nodes ON CONFLICT (node_id) DO NOTHING",
            &[],
        )
        .await
        .map_err(internal)?;
    let rows = transaction
        .query(
            "SELECT node_id, generation, state, protection_fingerprint, known_inventory, lease_expires_at
         FROM wr_node_release_cleanup
         WHERE state = 'needs_reconcile'
            OR (state <> 'paused' AND (next_reconcile_at IS NULL OR next_reconcile_at <= NOW()))
         ORDER BY last_reconciled_at ASC NULLS FIRST LIMIT $1 FOR UPDATE SKIP LOCKED",
            &[&batch_size],
        )
        .await
        .map_err(internal)?;
    for row in &rows {
        let node_id: String = row.get("node_id");
        let mut generation: i64 = row.get("generation");
        let state: String = row.get("state");
        let policy = match manager_cleanup_policy(&transaction, &node_id).await {
            Ok(policy) => policy,
            Err(error) => {
                transaction.execute(
                    "UPDATE wr_node_release_cleanup SET state = 'paused', diagnostic_code = 'POLICY_QUERY_FAILED',
                     diagnostic_detail = $2, last_reconciled_at = NOW(), next_reconcile_at = NOW() + INTERVAL '30 seconds', updated_at = NOW()
                     WHERE node_id = $1",
                    &[&node_id, &error.message()],
                ).await.map_err(internal)?;
                continue;
            }
        };
        let fingerprint_changed =
            row.get::<_, String>("protection_fingerprint") != policy.fingerprint;
        let lease_expired = state == "claimed"
            && row
                .get::<_, Option<chrono::DateTime<chrono::Utc>>>("lease_expires_at")
                .is_none_or(|lease| lease <= chrono::Utc::now());
        if state != "needs_reconcile" && (fingerprint_changed || lease_expired) {
            transaction.execute(
                "UPDATE wr_node_release_cleanup SET generation = generation + 1,
                 state = 'needs_reconcile', authority_payload = NULL, payload_digest = '', candidate_count = 0,
                 agent_instance_id = NULL, claimed_by = NULL, claim_instance = NULL,
                 lease_expires_at = NULL, delivered_at = NULL, updated_at = NOW() WHERE node_id = $1",
                &[&node_id],
            ).await.map_err(internal)?;
            transaction.execute(
                "UPDATE wr_node_release_cleanup_generations SET outcome = 'superseded', completed_at = NOW()
                 WHERE node_id = $1 AND generation = $2 AND outcome = 'materialized'",
                &[&node_id, &generation],
            ).await.map_err(internal)?;
            generation += 1;
        } else if matches!(state.as_str(), "pending" | "claimed") {
            transaction.execute(
                "UPDATE wr_node_release_cleanup SET last_reconciled_at = NOW(),
                 next_reconcile_at = NOW() + INTERVAL '30 seconds', updated_at = NOW() WHERE node_id = $1",
                &[&node_id],
            ).await.map_err(internal)?;
            continue;
        }
        let prior_inventory = row
            .get::<_, Option<Vec<u8>>>("known_inventory")
            .map(|bytes| NodeCleanupAuthority::decode(bytes.as_slice()))
            .transpose()
            .map_err(|error| {
                Status::internal(format!("stored cleanup inventory is invalid: {error}"))
            })?
            .map(|value| value.known_inventory);
        let delete = match prior_inventory.as_ref() {
            None => Vec::new(),
            Some(inventory) => inventory
                .iter()
                .filter(|item| policy.delete.contains(item))
                .cloned()
                .collect(),
        };
        if prior_inventory.is_some() && delete.is_empty() {
            transaction.execute(
                "UPDATE wr_node_release_cleanup SET state = 'clean', protection_fingerprint = $2,
                 authority_payload = NULL, payload_digest = '', candidate_count = 0,
                 last_reconciled_at = NOW(), next_reconcile_at = NOW() + INTERVAL '30 seconds',
                 diagnostic_code = '', diagnostic_detail = '', updated_at = NOW() WHERE node_id = $1",
                &[&node_id, &policy.fingerprint],
            ).await.map_err(internal)?;
            continue;
        }
        let authority = NodeCleanupAuthority {
            delete_releases: delete,
            known_inventory: prior_inventory.unwrap_or_default(),
        };
        let payload = authority.encode_to_vec();
        let digest = format!("sha256:{:x}", Sha256::digest(&payload));
        transaction.execute(
            "INSERT INTO wr_node_release_cleanup_generations
               (node_id, generation, protection_fingerprint, authority_payload, payload_digest, known_inventory)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (node_id, generation) DO UPDATE SET
               protection_fingerprint = EXCLUDED.protection_fingerprint,
               authority_payload = EXCLUDED.authority_payload, payload_digest = EXCLUDED.payload_digest,
               known_inventory = EXCLUDED.known_inventory, outcome = 'materialized', completed_at = NULL",
            &[&node_id, &generation, &policy.fingerprint, &payload, &digest, &row.get::<_, Option<Vec<u8>>>("known_inventory")],
        ).await.map_err(internal)?;
        transaction.execute(
            "UPDATE wr_node_release_cleanup SET state = 'pending', protection_fingerprint = $2,
             authority_payload = $3, payload_digest = $4, candidate_count = $5,
             agent_instance_id = NULL, claimed_by = NULL, claim_instance = NULL,
             lease_expires_at = NULL, delivered_at = NULL, last_reconciled_at = NOW(),
             next_reconcile_at = NOW() + INTERVAL '30 seconds', diagnostic_code = '', diagnostic_detail = '', updated_at = NOW()
             WHERE node_id = $1 AND generation = $6",
            &[&node_id, &policy.fingerprint, &payload, &digest, &(authority.delete_releases.len() as i32), &generation],
        ).await.map_err(internal)?;
    }
    let count = rows.len() as u64;
    transaction.commit().await.map_err(internal)?;
    Ok(count)
}

async fn cleanup_attested<C: GenericClient + Sync>(
    client: &C,
    node_id: &str,
    agent_instance_id: &str,
    principal: &str,
) -> Result<bool, Status> {
    Ok(client
        .query_opt(
            "SELECT 1 FROM wr_node_agent_attestations a
         JOIN wr_node_agent_policies p ON p.node_id = a.node_id
         WHERE a.node_id = $1 AND a.agent_instance_id = $2 AND a.authenticated_principal = $3
           AND a.protocol_version = p.protocol_version AND a.binary_digest = p.binary_digest
           AND a.backend = p.backend AND p.capabilities <@ a.capabilities
           AND a.observed_at >= NOW() - INTERVAL '30 seconds'",
            &[&node_id, &agent_instance_id, &principal],
        )
        .await
        .map_err(internal)?
        .is_some())
}

pub async fn claim_node_cleanup(
    pool: &Pool,
    node_id: &str,
    agent_instance_id: &str,
    principal: &str,
) -> Result<Option<ClaimNodeCleanupResponse>, Status> {
    if node_id.is_empty() || agent_instance_id.is_empty() {
        return Err(Status::invalid_argument(
            "node_id and agent_instance_id are required",
        ));
    }
    let mut client = pool.get().await.map_err(internal)?;
    let transaction = client.transaction().await.map_err(internal)?;
    transaction
        .batch_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .await
        .map_err(internal)?;
    if !cleanup_attested(&transaction, node_id, agent_instance_id, principal).await? {
        return Err(Status::failed_precondition(
            "fresh authenticated node-agent attestation is required",
        ));
    }
    let query = format!(
        "SELECT {CLEANUP_ROW_COLUMNS} FROM wr_node_release_cleanup WHERE node_id = $1 FOR UPDATE"
    );
    let Some(row) = transaction
        .query_opt(&query, &[&node_id])
        .await
        .map_err(internal)?
    else {
        transaction.commit().await.map_err(internal)?;
        return Ok(None);
    };
    let state: String = row.get("state");
    if state != "pending"
        && !(state == "claimed"
            && row.get::<_, Option<String>>("agent_instance_id").as_deref()
                == Some(agent_instance_id)
            && row.get::<_, Option<String>>("claimed_by").as_deref() == Some(principal)
            && row
                .get::<_, Option<chrono::DateTime<chrono::Utc>>>("lease_expires_at")
                .is_some_and(|time| time > chrono::Utc::now()))
    {
        transaction.commit().await.map_err(internal)?;
        return Ok(None);
    }
    let policy = manager_cleanup_policy(&transaction, node_id).await?;
    if policy.fingerprint != row.get::<_, String>("protection_fingerprint") {
        fence_cleanup_authority(&transaction, node_id, "CLAIM_POLICY_CHANGED").await?;
        transaction.commit().await.map_err(internal)?;
        return Ok(None);
    }
    let authority_bytes: Vec<u8> = row
        .get::<_, Option<Vec<u8>>>("authority_payload")
        .ok_or_else(|| Status::internal("pending cleanup authority is missing"))?;
    let authority = NodeCleanupAuthority::decode(authority_bytes.as_slice()).map_err(|error| {
        Status::internal(format!("stored cleanup authority is invalid: {error}"))
    })?;
    let claim_instance = if state == "claimed" {
        row.get::<_, Option<Uuid>>("claim_instance")
            .expect("claimed cleanup has claim instance")
    } else {
        Uuid::new_v4()
    };
    let epoch: i64 = if state == "claimed" {
        row.get("lease_epoch")
    } else {
        row.get::<_, i64>("lease_epoch") + 1
    };
    transaction.execute(
        "UPDATE wr_node_release_cleanup SET state = 'claimed', agent_instance_id = $2,
         claimed_by = $3, claim_instance = $4, lease_epoch = $5,
         lease_expires_at = NOW() + make_interval(secs => $6), delivered_at = COALESCE(delivered_at, NOW()),
         last_attempt_at = NOW(), updated_at = NOW() WHERE node_id = $1",
        &[&node_id, &agent_instance_id, &principal, &claim_instance, &epoch, &LEASE_SECONDS],
    ).await.map_err(internal)?;
    let generation: i64 = row.get("generation");
    let payload_digest: String = row.get("payload_digest");
    transaction.commit().await.map_err(internal)?;
    Ok(Some(ClaimNodeCleanupResponse {
        instruction: Some(NodeCleanupInstruction {
            node_id: node_id.to_string(),
            agent_instance_id: agent_instance_id.to_string(),
            generation: generation as u64,
            lease_epoch: epoch as u64,
            claim_instance: claim_instance.to_string(),
            payload_digest,
            delete_releases: authority.delete_releases,
            expected_inventory: authority.known_inventory,
            deadline: Some(timestamp(
                chrono::Utc::now() + chrono::Duration::seconds(LEASE_SECONDS as i64),
            )),
        }),
        lease_seconds: LEASE_SECONDS as u64,
    }))
}

pub async fn renew_node_cleanup(
    pool: &Pool,
    request: &wr_common::wruntime::RenewNodeCleanupLeaseRequest,
    principal: &str,
) -> Result<wr_common::wruntime::RenewNodeCleanupLeaseResponse, Status> {
    let generation = i64::try_from(request.generation)
        .map_err(|_| Status::invalid_argument("generation is too large"))?;
    let epoch = i64::try_from(request.lease_epoch)
        .map_err(|_| Status::invalid_argument("lease epoch is too large"))?;
    let claim = Uuid::parse_str(&request.claim_instance)
        .map_err(|_| Status::invalid_argument("claim_instance must be a UUID"))?;
    let client = pool.get().await.map_err(internal)?;
    let row = client.query_opt(
        "UPDATE wr_node_release_cleanup SET lease_expires_at = NOW() + make_interval(secs => $7), updated_at = NOW()
         WHERE node_id = $1 AND generation = $2 AND state = 'claimed' AND lease_epoch = $3
           AND claim_instance = $4 AND agent_instance_id = $5 AND claimed_by = $6 AND lease_expires_at > NOW()
         RETURNING lease_expires_at",
        &[&request.node_id, &generation, &epoch, &claim, &request.agent_instance_id, &principal, &LEASE_SECONDS],
    ).await.map_err(internal)?;
    Ok(wr_common::wruntime::RenewNodeCleanupLeaseResponse {
        lease_expires_at: row
            .as_ref()
            .map(|row| timestamp(row.get("lease_expires_at"))),
        superseded: row.is_none(),
    })
}

fn canonical_releases(
    values: &[wr_common::wruntime::ReleaseInventoryEntry],
) -> Result<std::collections::BTreeMap<u64, (String, String)>, Status> {
    let mut result = std::collections::BTreeMap::new();
    for value in values {
        if value.revision == 0
            || value.bundle_digest.is_empty()
            || value.resolved_release_digest.is_empty()
            || result
                .insert(
                    value.revision,
                    (
                        value.bundle_digest.clone(),
                        value.resolved_release_digest.clone(),
                    ),
                )
                .is_some()
        {
            return Err(Status::failed_precondition(
                "cleanup result contains invalid or duplicate release identity",
            ));
        }
    }
    Ok(result)
}

pub async fn report_node_cleanup_result(
    pool: &Pool,
    request: &ReportNodeCleanupResultRequest,
    principal: &str,
) -> Result<ReportNodeCleanupResultResponse, Status> {
    let generation = i64::try_from(request.generation)
        .map_err(|_| Status::invalid_argument("generation is too large"))?;
    let epoch = i64::try_from(request.lease_epoch)
        .map_err(|_| Status::invalid_argument("lease epoch is too large"))?;
    let claim = Uuid::parse_str(&request.claim_instance)
        .map_err(|_| Status::invalid_argument("claim_instance must be a UUID"))?;
    let payload = request.encode_to_vec();
    let mut client = pool.get().await.map_err(internal)?;
    let transaction = client.transaction().await.map_err(internal)?;
    if let Some(receipt) = transaction.query_opt(
        "SELECT authenticated_principal, result_payload FROM wr_node_release_cleanup_result_receipts
         WHERE node_id = $1 AND generation = $2 AND agent_instance_id = $3 AND lease_epoch = $4 AND claim_instance = $5",
        &[&request.node_id, &generation, &request.agent_instance_id, &epoch, &claim],
    ).await.map_err(internal)? {
        if receipt.get::<_, String>("authenticated_principal") != principal || receipt.get::<_, Vec<u8>>("result_payload") != payload {
            return Err(Status::aborted("cleanup result retry conflicts with stored receipt"));
        }
        let summary = get_cleanup_summary_in(&transaction, &request.node_id).await?;
        transaction.commit().await.map_err(internal)?;
        return Ok(ReportNodeCleanupResultResponse { disposition: NodeCleanupResultDisposition::Accepted as i32, cleanup: Some(summary) });
    }
    let query = format!(
        "SELECT {CLEANUP_ROW_COLUMNS} FROM wr_node_release_cleanup WHERE node_id = $1 FOR UPDATE"
    );
    let row = transaction
        .query_opt(&query, &[&request.node_id])
        .await
        .map_err(internal)?
        .ok_or_else(|| Status::not_found("node cleanup state not found"))?;
    let current = row.get::<_, i64>("generation") == generation
        && row.get::<_, String>("state") == "claimed"
        && row.get::<_, i64>("lease_epoch") == epoch
        && row.get::<_, Option<Uuid>>("claim_instance") == Some(claim)
        && row.get::<_, Option<String>>("agent_instance_id").as_deref()
            == Some(request.agent_instance_id.as_str())
        && row.get::<_, Option<String>>("claimed_by").as_deref() == Some(principal)
        && row
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>("lease_expires_at")
            .is_some_and(|time| time > chrono::Utc::now());
    if !current {
        let summary = cleanup_summary_from_row(&row)?;
        transaction.commit().await.map_err(internal)?;
        return Ok(ReportNodeCleanupResultResponse {
            disposition: NodeCleanupResultDisposition::Superseded as i32,
            cleanup: Some(summary),
        });
    }
    if row.get::<_, String>("payload_digest") != request.payload_digest {
        return Err(Status::failed_precondition(
            "cleanup payload digest does not match issued authority",
        ));
    }
    let authority = NodeCleanupAuthority::decode(
        row.get::<_, Option<Vec<u8>>>("authority_payload")
            .expect("claimed authority")
            .as_slice(),
    )
    .map_err(|error| Status::internal(format!("stored cleanup authority is invalid: {error}")))?;
    if request.condition_code.is_empty() {
        let deleted = canonical_releases(&request.deleted_releases)?;
        let issued = canonical_releases(&authority.delete_releases)?;
        if deleted != issued {
            return Err(Status::failed_precondition(
                "deleted releases do not exactly match issued authority",
            ));
        }
        let resulting = canonical_releases(&request.resulting_inventory)?;
        if authority.known_inventory.is_empty() {
            let policy = manager_cleanup_policy(&transaction, &request.node_id).await?;
            let catalog = canonical_releases(&policy.known)?;
            if resulting
                .iter()
                .any(|(revision, identity)| catalog.get(revision) != Some(identity))
            {
                return Err(Status::failed_precondition(
                    "complete inventory contains an unknown release identity",
                ));
            }
        } else {
            let mut expected = canonical_releases(&authority.known_inventory)?;
            for revision in issued.keys() {
                expected.remove(revision);
            }
            if resulting != expected {
                return Err(Status::failed_precondition("resulting inventory does not equal issued inventory minus authorized deletions"));
            }
        }
        for release in &request.deleted_releases {
            transaction
                .execute(
                    "INSERT INTO wr_node_release_deletions
                   (node_id, revision, bundle_digest, resolved_release_digest, cleanup_generation)
                 VALUES ($1, $2, $3, $4, $5) ON CONFLICT (node_id, revision) DO NOTHING",
                    &[
                        &request.node_id,
                        &(release.revision as i64),
                        &release.bundle_digest,
                        &release.resolved_release_digest,
                        &generation,
                    ],
                )
                .await
                .map_err(internal)?;
        }
        let inventory = NodeCleanupAuthority {
            delete_releases: Vec::new(),
            known_inventory: request.resulting_inventory.clone(),
        }
        .encode_to_vec();
        transaction.execute(
            "UPDATE wr_node_release_cleanup SET state = 'clean', known_inventory = $2,
             authority_payload = NULL, payload_digest = '', candidate_count = 0,
             agent_instance_id = NULL, claimed_by = NULL, claim_instance = NULL,
             lease_expires_at = NULL, diagnostic_code = '', diagnostic_detail = '', updated_at = NOW()
             WHERE node_id = $1",
            &[&request.node_id, &inventory],
        ).await.map_err(internal)?;
        transaction.execute(
            "UPDATE wr_node_release_cleanup_generations SET outcome = 'succeeded', completed_at = NOW() WHERE node_id = $1 AND generation = $2",
            &[&request.node_id, &generation],
        ).await.map_err(internal)?;
    } else {
        transaction
            .execute(
                "UPDATE wr_node_release_cleanup SET state = 'paused', agent_instance_id = NULL,
             claimed_by = NULL, claim_instance = NULL, lease_expires_at = NULL,
             diagnostic_code = $2, diagnostic_detail = $3, updated_at = NOW() WHERE node_id = $1",
                &[&request.node_id, &request.condition_code, &request.detail],
            )
            .await
            .map_err(internal)?;
        transaction.execute(
            "UPDATE wr_node_release_cleanup_generations SET outcome = 'paused', completed_at = NOW() WHERE node_id = $1 AND generation = $2",
            &[&request.node_id, &generation],
        ).await.map_err(internal)?;
    }
    transaction.execute(
        "INSERT INTO wr_node_release_cleanup_result_receipts
           (node_id, generation, agent_instance_id, lease_epoch, claim_instance, authenticated_principal, payload_digest, result_payload)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        &[&request.node_id, &generation, &request.agent_instance_id, &epoch, &claim, &principal, &request.payload_digest, &payload],
    ).await.map_err(internal)?;
    let summary = get_cleanup_summary_in(&transaction, &request.node_id).await?;
    transaction.commit().await.map_err(internal)?;
    Ok(ReportNodeCleanupResultResponse {
        disposition: NodeCleanupResultDisposition::Accepted as i32,
        cleanup: Some(summary),
    })
}

async fn get_cleanup_summary_in<C: GenericClient + Sync>(
    client: &C,
    node_id: &str,
) -> Result<NodeCleanupSummary, Status> {
    let query =
        format!("SELECT {CLEANUP_ROW_COLUMNS} FROM wr_node_release_cleanup WHERE node_id = $1");
    let row = client
        .query_one(&query, &[&node_id])
        .await
        .map_err(internal)?;
    cleanup_summary_from_row(&row)
}

pub(crate) async fn cleanup_summaries_from_client<C: GenericClient + Sync>(
    client: &C,
) -> Result<Vec<NodeCleanupSummary>, Status> {
    let query =
        format!("SELECT {CLEANUP_ROW_COLUMNS} FROM wr_node_release_cleanup ORDER BY node_id");
    client
        .query(&query, &[])
        .await
        .map_err(internal)?
        .iter()
        .map(cleanup_summary_from_row)
        .collect()
}

fn canonical_agent_policy(policy: &NodeAgentPolicy) -> Result<(String, Vec<String>), Status> {
    validate_identity(&policy.node_id, "node_id")
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    if policy.protocol_version != AGENT_PROTOCOL_VERSION {
        return Err(Status::invalid_argument(
            "node-agent protocol version must exactly match this manager",
        ));
    }
    validate_sha256_digest(&policy.binary_digest, "binary_digest")
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let backend =
        backend_name(BackendKind::try_from(policy.backend).unwrap_or(BackendKind::Unspecified))?
            .to_string();
    let capabilities = normalize_capabilities(&policy.capabilities)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    Ok((backend, capabilities))
}

pub async fn put_agent_policy(
    pool: &Pool,
    actor: &str,
    policy: &NodeAgentPolicy,
) -> Result<NodeAgentPolicy, Status> {
    let (backend, capabilities) = canonical_agent_policy(policy)?;
    let explicit_retention = policy
        .retention_count
        .map(|value| {
            if value == 0 {
                Err(Status::invalid_argument("retention_count must be positive"))
            } else {
                i32::try_from(value)
                    .map_err(|_| Status::invalid_argument("retention_count is too large"))
            }
        })
        .transpose()?;
    let mut client = pool.get().await.map_err(internal)?;
    let transaction = client.transaction().await.map_err(internal)?;
    let prior = transaction
        .query_opt(
            "SELECT protocol_version, binary_digest, backend, capabilities, retention_count
             FROM wr_node_agent_policies WHERE node_id = $1 FOR UPDATE",
            &[&policy.node_id],
        )
        .await
        .map_err(internal)?;
    let retention = match (explicit_retention, prior.as_ref()) {
        (Some(value), _) => value,
        (None, Some(row)) => row.get("retention_count"),
        (None, None) => {
            return Err(Status::invalid_argument(
                "retention_count is required when creating a node-agent policy",
            ))
        }
    };
    let policy_changed = prior.as_ref().is_some_and(|row| {
        row.get::<_, String>("protocol_version") != policy.protocol_version
            || row.get::<_, String>("binary_digest") != policy.binary_digest
            || row.get::<_, String>("backend") != backend
            || row.get::<_, Vec<String>>("capabilities") != capabilities
    });
    transaction
        .execute(
            "INSERT INTO wr_nodes(node_id) VALUES ($1) ON CONFLICT(node_id) DO NOTHING",
            &[&policy.node_id],
        )
        .await
        .map_err(internal)?;
    transaction
        .execute(
            "INSERT INTO wr_node_agent_policies
               (node_id, protocol_version, backend, retention_count, actor, binary_digest, capabilities)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT(node_id) DO UPDATE SET protocol_version = EXCLUDED.protocol_version,
               backend = EXCLUDED.backend, retention_count = EXCLUDED.retention_count,
               actor = EXCLUDED.actor, binary_digest = EXCLUDED.binary_digest,
               capabilities = EXCLUDED.capabilities, updated_at = NOW()",
            &[&policy.node_id, &policy.protocol_version, &backend, &retention, &actor,
              &policy.binary_digest, &capabilities],
        )
        .await
        .map_err(internal)?;
    if policy_changed {
        fence_cleanup_authority(&transaction, &policy.node_id, "AGENT_POLICY_UPDATED").await?;
        for row in transaction
            .query(
                "UPDATE wr_node_operations SET state = 'paused', failure_code = 'AGENT_POLICY_UPDATED',
                        failure_detail = 'node-agent policy changed; attest the replacement and explicitly resume',
                        lease_expires_at = NULL, claimed_by = NULL, agent_instance_id = NULL,
                        updated_at = NOW()
                 WHERE node_id = $1 AND state = 'running'
                 RETURNING operation_id, lease_epoch",
                &[&policy.node_id],
            )
            .await
            .map_err(internal)?
        {
            append_event(
                &transaction,
                row.get("operation_id"),
                actor,
                "AGENT_POLICY_UPDATED",
                "active lease fenced; replacement activation requires explicit resume",
                row.get("lease_epoch"),
            )
            .await?;
        }
    }
    transaction.commit().await.map_err(internal)?;
    Ok(NodeAgentPolicy {
        node_id: policy.node_id.clone(),
        protocol_version: policy.protocol_version.clone(),
        backend: policy.backend,
        retention_count: Some(retention as u32),
        binary_digest: policy.binary_digest.clone(),
        capabilities,
    })
}

pub async fn attest(
    pool: &Pool,
    principal: &str,
    attestation: &NodeAgentAttestation,
) -> Result<Vec<DeploymentCondition>, Status> {
    validate_identity(&attestation.node_id, "node_id")
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    validate_identity(&attestation.agent_instance_id, "agent_instance_id")
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    validate_sha256_digest(&attestation.binary_digest, "binary_digest")
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let capabilities = normalize_capabilities(&attestation.capabilities)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let backend = backend_name(
        BackendKind::try_from(attestation.backend).unwrap_or(BackendKind::Unspecified),
    )?;
    let client = pool.get().await.map_err(internal)?;
    let policy = client
        .query_opt(
            "SELECT protocol_version, binary_digest, backend, capabilities
             FROM wr_node_agent_policies WHERE node_id = $1",
            &[&attestation.node_id],
        )
        .await
        .map_err(internal)?;
    let mut conditions = Vec::new();
    if let Some(policy) = policy {
        if policy.get::<_, String>("protocol_version") != attestation.protocol_version {
            conditions.push(operation_condition(
                "PROTOCOL_MISMATCH".into(),
                "agent protocol does not match manager policy".into(),
            ));
        }
        if policy.get::<_, String>("backend") != backend {
            conditions.push(operation_condition(
                "BACKEND_MISMATCH".into(),
                "agent backend does not match manager policy".into(),
            ));
        }
        if policy.get::<_, String>("binary_digest") != attestation.binary_digest {
            conditions.push(operation_condition(
                "BINARY_MISMATCH".into(),
                "agent binary digest does not match manager policy".into(),
            ));
        }
        let missing =
            missing_capabilities(&policy.get::<_, Vec<String>>("capabilities"), &capabilities)
                .map_err(internal)?;
        if !missing.is_empty() {
            conditions.push(operation_condition(
                "CAPABILITY_MISMATCH".into(),
                format!("missing required capabilities: {}", missing.join(", ")),
            ));
        }
    } else {
        conditions.push(operation_condition(
            "AGENT_POLICY_MISSING".into(),
            "manager has no expected policy for this node".into(),
        ));
    }
    client
        .execute(
            "INSERT INTO wr_node_agent_attestations
               (node_id, agent_instance_id, authenticated_principal, protocol_version,
                binary_digest, backend, capabilities, observed_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
             ON CONFLICT(node_id, agent_instance_id) DO UPDATE SET
               authenticated_principal = EXCLUDED.authenticated_principal,
               protocol_version = EXCLUDED.protocol_version, binary_digest = EXCLUDED.binary_digest,
               backend = EXCLUDED.backend, capabilities = EXCLUDED.capabilities, observed_at = NOW()",
            &[&attestation.node_id, &attestation.agent_instance_id, &principal,
              &attestation.protocol_version, &attestation.binary_digest, &backend, &capabilities],
        )
        .await
        .map_err(internal)?;
    Ok(conditions)
}

pub(crate) async fn policies_from_client<C>(client: &C) -> Result<Vec<NodeAgentPolicy>, Status>
where
    C: GenericClient + Sync,
{
    Ok(client
        .query(
            "SELECT node_id, protocol_version, backend, retention_count, binary_digest, capabilities
             FROM wr_node_agent_policies ORDER BY node_id",
            &[],
        )
        .await
        .map_err(internal)?
        .into_iter()
        .map(|row| NodeAgentPolicy {
            node_id: row.get("node_id"),
            protocol_version: row.get("protocol_version"),
            backend: parse_backend(row.get::<_, String>("backend").as_str()) as i32,
            retention_count: Some(row.get::<_, i32>("retention_count") as u32),
            binary_digest: row.get("binary_digest"),
            capabilities: row.get("capabilities"),
        })
        .collect())
}

pub(crate) async fn attestations_from_client<C>(
    client: &C,
    node_id: &str,
) -> Result<Vec<NodeAgentAttestation>, Status>
where
    C: GenericClient + Sync,
{
    Ok(client
        .query(
            "SELECT node_id, agent_instance_id, authenticated_principal, protocol_version,
                    binary_digest, backend, capabilities, observed_at
             FROM wr_node_agent_attestations WHERE ($1 = '' OR node_id = $1)
             ORDER BY node_id, observed_at DESC",
            &[&node_id],
        )
        .await
        .map_err(internal)?
        .into_iter()
        .map(|row| NodeAgentAttestation {
            node_id: row.get("node_id"),
            agent_instance_id: row.get("agent_instance_id"),
            protocol_version: row.get("protocol_version"),
            binary_digest: row.get("binary_digest"),
            backend: parse_backend(row.get::<_, String>("backend").as_str()) as i32,
            capabilities: row.get("capabilities"),
            observed_at: Some(timestamp(row.get("observed_at"))),
            authenticated_principal: row.get("authenticated_principal"),
        })
        .collect())
}

pub async fn attestations(pool: &Pool, node_id: &str) -> Result<Vec<NodeAgentAttestation>, Status> {
    let client = pool.get().await.map_err(internal)?;
    attestations_from_client(&client, node_id).await
}

pub async fn authorities(pool: &Pool, node_id: &str) -> Result<Vec<SlotAuthorityStatus>, Status> {
    let client = pool.get().await.map_err(internal)?;
    Ok(client
        .query(
            "SELECT a.node_id, a.engine_slot, a.revision, d.bundle_digest,
                    a.resolved_release_digest
             FROM wr_node_slot_authority a
             JOIN wr_node_deployments d ON d.node_id = a.node_id AND d.revision = a.revision
             WHERE a.authoritative AND ($1 = '' OR a.node_id = $1)
             ORDER BY a.node_id, a.engine_slot",
            &[&node_id],
        )
        .await
        .map_err(internal)?
        .into_iter()
        .map(|row| SlotAuthorityStatus {
            node_id: row.get("node_id"),
            engine_slot: row.get("engine_slot"),
            revision: row.get::<_, i64>("revision") as u64,
            bundle_digest: row.get("bundle_digest"),
            resolved_release_digest: row.get("resolved_release_digest"),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_inventory_identity_derives_transition_kind() {
        assert_eq!(
            derive_transition(0, "", "", 2, "target", "target-release"),
            DerivedTransition::Addition
        );
        assert_eq!(
            derive_transition(1, "source", "source-release", 0, "", ""),
            DerivedTransition::Removal
        );
        assert_eq!(
            derive_transition(1, "same", "same-release", 1, "same", "same-release"),
            DerivedTransition::Unchanged
        );
        assert_eq!(
            derive_transition(1, "same", "same-release", 2, "same", "same-release"),
            DerivedTransition::Replacement,
            "a new immutable revision is replacement even when content digests match"
        );
    }

    #[test]
    fn corrupt_stored_transition_combinations_fail_closed() {
        let target = |source_revision,
                      source_digest: &str,
                      source_resolved_digest: &str,
                      target_revision,
                      target_digest: &str,
                      target_resolved_digest: &str| OperationTargetProgress {
            source_revision,
            source_digest: source_digest.into(),
            source_resolved_release_digest: source_resolved_digest.into(),
            target_revision,
            target_digest: target_digest.into(),
            target_resolved_release_digest: target_resolved_digest.into(),
            ..Default::default()
        };
        assert!(validate_stored_transition(
            NodeOperationAction::Deployment,
            DerivedTransition::Removal,
            &target(0, "", "", 2, "target", "target-release"),
        )
        .is_err());
        assert!(validate_stored_transition(
            NodeOperationAction::Restart,
            DerivedTransition::Restart,
            &target(1, "source", "source-release", 2, "target", "target-release"),
        )
        .is_err());
        assert!(validate_stored_transition(
            NodeOperationAction::Restart,
            DerivedTransition::Restart,
            &target(1, "same", "same-release", 1, "same", "same-release"),
        )
        .is_ok());
    }

    #[test]
    fn derived_transitions_have_distinct_release_verified_sequences() {
        use NodeOperationStepKind as Step;

        assert_eq!(
            first_forward_step(DerivedTransition::Addition),
            Some(Step::VerifyReleaseMetadata)
        );
        assert_eq!(
            next_forward_step(DerivedTransition::Addition, Step::VerifyReleaseMetadata),
            Some(Step::SelectRelease)
        );
        assert_eq!(
            first_forward_step(DerivedTransition::Replacement),
            Some(Step::VerifyReleaseMetadata)
        );
        assert_eq!(
            next_forward_step(DerivedTransition::Replacement, Step::VerifyReleaseMetadata),
            Some(Step::VerifyTarget)
        );
        assert_eq!(
            next_forward_step(DerivedTransition::Replacement, Step::StopBackend),
            Some(Step::SelectRelease)
        );
        assert_eq!(
            first_forward_step(DerivedTransition::Removal),
            Some(Step::VerifyTarget)
        );
        assert_eq!(
            next_forward_step(DerivedTransition::Removal, Step::StopBackend),
            Some(Step::SwitchAuthority)
        );
        assert_eq!(
            first_forward_step(DerivedTransition::Restart),
            Some(Step::VerifyTarget)
        );
        assert_eq!(
            next_forward_step(DerivedTransition::Restart, Step::StopBackend),
            Some(Step::StartBackend)
        );
        assert_eq!(first_forward_step(DerivedTransition::Unchanged), None);
    }
}
