use deadpool_postgres::{GenericClient, Pool};
use prost::Message;
use tokio_postgres::Row;
use tonic::Status;
use uuid::Uuid;
use wr_common::agent_policy::{AgentPolicy, AgentPolicyBackend};
use wr_common::deployment_contract::deployment_operation_id;
use wr_common::wruntime::{
    AgentInstruction, BackendKind, BackendProcessState, BackendTerminationEvidence,
    ClaimOperationResponse, CleanupReleaseEvidence, DeploymentCondition, InstructionTarget,
    InstructionTargetKind, NodeAgentAttestation, NodeAgentPolicy, NodeOperation,
    NodeOperationAction, NodeOperationPhase, NodeOperationState, NodeOperationStepKind,
    OperationEvent, OperationSlotProgress, ProcessLifecycleState, ReportNodeObservationRequest,
    ReportStepResultRequest, RolloutPolicy, ServiceKind, SlotAuthorityStatus, SlotObservation,
    SubmitOperationRequest,
};

const LEASE_SECONDS: f64 = 15.0;
const EVIDENCE_FRESH_SECONDS: i64 = 15;

fn internal(error: impl std::fmt::Debug) -> Status {
    Status::internal(format!("database operation failed: {error:?}"))
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
        NodeOperationAction::InitialApply => "initial_apply",
        NodeOperationAction::Drain => "drain",
        NodeOperationAction::Restart => "restart",
        NodeOperationAction::RollingUpgrade => "rolling_upgrade",
        NodeOperationAction::Scale => "scale",
        NodeOperationAction::Rollback => "rollback",
        NodeOperationAction::Unspecified => "unspecified",
    }
}

fn parse_action(value: &str) -> Result<NodeOperationAction, Status> {
    match value {
        "initial_apply" => Ok(NodeOperationAction::InitialApply),
        "drain" => Ok(NodeOperationAction::Drain),
        "restart" => Ok(NodeOperationAction::Restart),
        "rolling_upgrade" => Ok(NodeOperationAction::RollingUpgrade),
        "scale" => Ok(NodeOperationAction::Scale),
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
        "committed_cleanup" => Ok(NodeOperationPhase::CommittedCleanup),
        "complete" => Ok(NodeOperationPhase::Complete),
        "superseded" => Ok(NodeOperationPhase::Superseded),
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
        NodeOperationStepKind::CleanupRelease => "cleanup_release",
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
        "cleanup_release" => Ok(NodeOperationStepKind::CleanupRelease),
        "inspect_backend" => Ok(NodeOperationStepKind::InspectBackend),
        "complete" => Ok(NodeOperationStepKind::Unspecified),
        _ => Err(Status::internal("stored operation has an invalid step")),
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
                    agent_instance_id, cleanup_superseded_by, cleanup_evidence,
                    cleanup_delivered_at, cleanup_reported_at, cleanup_backend_query_error,
                    proxy_process_instance_id, restoration_terminal_state, proxy_next_step,
                    proxy_source_revision, proxy_source_digest, proxy_source_resolved_digest,
                    proxy_target_revision, proxy_target_digest, proxy_target_resolved_digest,
                    proxy_backend_instance_id, proxy_changed, proxy_effect_ambiguous,
                    proxy_effect_delivered_at, proxy_effect_reported,
                    proxy_effect_observed_revision, proxy_effect_observed_digest,
                    proxy_effect_observed_resolved_digest, proxy_effect_backend_instance_id,
                    proxy_effect_process_instance_id, proxy_effect_condition_code,
                    proxy_effect_detail, proxy_effect_termination_evidence
             FROM wr_node_operations WHERE operation_id = $1",
            &[&operation_id],
        )
        .await
        .map_err(internal)?
        .ok_or_else(|| Status::not_found("operation not found"))?;
    let slots = client
        .query(
            "SELECT engine_slot, rollout_order, authoritative_revision, next_step, completed_steps, complete,
                    condition_code, condition_detail, source_revision, source_digest,
                    source_resolved_digest, target_revision, target_digest, target_resolved_digest,
                    pinned_backend_instance_id, pinned_process_instance_id, authority_switched,
                    serving_converged, changed, effect_ambiguous, effect_delivered_at,
                    effect_reported, effect_observed_revision, effect_observed_digest,
                    effect_observed_resolved_digest, effect_backend_instance_id,
                    effect_process_instance_id, effect_condition_code, effect_detail,
                    effect_termination_evidence
             FROM wr_node_operation_slots WHERE operation_id = $1 ORDER BY rollout_order",
            &[&operation_id],
        )
        .await
        .map_err(internal)?;
    operation_from_row(&row, &slots)
}

fn operation_from_row(row: &Row, slots: &[Row]) -> Result<NodeOperation, Status> {
    let policy = RolloutPolicy::decode(row.get::<_, Vec<u8>>("policy").as_slice())
        .map_err(|error| Status::internal(format!("stored rollout policy is invalid: {error}")))?;
    let failure_code: String = row.get("failure_code");
    let failure_detail: String = row.get("failure_detail");
    let lease_expires_at: Option<chrono::DateTime<chrono::Utc>> = row.get("lease_expires_at");
    let created_at: chrono::DateTime<chrono::Utc> = row.get("created_at");
    let updated_at: chrono::DateTime<chrono::Utc> = row.get("updated_at");
    let forward_deadline: chrono::DateTime<chrono::Utc> = row.get("forward_deadline");
    let cleanup_superseded_by: Option<Uuid> = row.get("cleanup_superseded_by");
    let cleanup_evidence = row
        .get::<_, Option<Vec<u8>>>("cleanup_evidence")
        .map(|bytes| CleanupReleaseEvidence::decode(bytes.as_slice()))
        .transpose()
        .map_err(|error| {
            Status::internal(format!("stored cleanup evidence is invalid: {error}"))
        })?;
    let cleanup_delivered_at: Option<chrono::DateTime<chrono::Utc>> =
        row.get("cleanup_delivered_at");
    let cleanup_reported_at: Option<chrono::DateTime<chrono::Utc>> = row.get("cleanup_reported_at");
    Ok(NodeOperation {
        operation_id: row.get::<_, Uuid>("operation_id").to_string(),
        node_id: row.get("node_id"),
        request_token: row.get("request_token"),
        actor: row.get("actor"),
        action: parse_action(row.get::<_, String>("action").as_str())? as i32,
        state: parse_state(row.get::<_, String>("state").as_str())? as i32,
        policy: Some(policy),
        source_revision: row.get::<_, i64>("source_revision") as u64,
        target_revision: row.get::<_, i64>("target_revision") as u64,
        bundle_digest: row.get("bundle_digest"),
        revision_digest: row
            .get::<_, Option<String>>("target_revision_digest")
            .unwrap_or_default(),
        slots: slots
            .iter()
            .map(|slot| {
                let code: String = slot.get("condition_code");
                let detail: String = slot.get("condition_detail");
                Ok(OperationSlotProgress {
                    engine_slot: slot.get("engine_slot"),
                    authoritative_revision: slot.get::<_, i64>("authoritative_revision") as u64,
                    next_step: parse_step(slot.get::<_, String>("next_step").as_str())? as i32,
                    completed_steps: slot.get::<_, i32>("completed_steps") as u32,
                    complete: slot.get("complete"),
                    conditions: if code.is_empty() {
                        vec![]
                    } else {
                        vec![operation_condition(code, detail)]
                    },
                    source_revision: slot.get::<_, i64>("source_revision") as u64,
                    source_digest: slot.get("source_digest"),
                    target_revision: slot.get::<_, i64>("target_revision") as u64,
                    target_digest: slot.get("target_digest"),
                    pinned_backend_instance_id: slot.get("pinned_backend_instance_id"),
                    pinned_process_instance_id: slot.get("pinned_process_instance_id"),
                    authority_switched: slot.get("authority_switched"),
                    serving_converged: slot.get("serving_converged"),
                    changed: slot.get("changed"),
                    effect_ambiguous: slot.get("effect_ambiguous"),
                    effect_delivered_at: slot
                        .get::<_, Option<chrono::DateTime<chrono::Utc>>>("effect_delivered_at")
                        .map(timestamp),
                    effect_reported: slot.get("effect_reported"),
                    effect_observed_revision: slot.get::<_, i64>("effect_observed_revision") as u64,
                    effect_observed_digest: slot.get("effect_observed_digest"),
                    effect_backend_instance_id: slot.get("effect_backend_instance_id"),
                    effect_process_instance_id: slot.get("effect_process_instance_id"),
                    effect_condition_code: slot.get("effect_condition_code"),
                    effect_detail: slot.get("effect_detail"),
                    rollout_order: slot.get::<_, i32>("rollout_order") as u32,
                    source_resolved_release_digest: slot.get("source_resolved_digest"),
                    target_resolved_release_digest: slot.get("target_resolved_digest"),
                    effect_observed_resolved_release_digest: slot
                        .get("effect_observed_resolved_digest"),
                    termination_evidence: decode_termination_evidence(
                        slot.get("effect_termination_evidence"),
                    )?,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?,
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
        cleanup_superseded_by: cleanup_superseded_by
            .map(|value| value.to_string())
            .unwrap_or_default(),
        cleanup_evidence,
        cleanup_reported_at: cleanup_reported_at.map(timestamp),
        cleanup_backend_query_error: row.get("cleanup_backend_query_error"),
        restoration_terminal_state: row
            .get::<_, Option<String>>("restoration_terminal_state")
            .map(|value| parse_state(&value).map(|state| state as i32))
            .transpose()?
            .unwrap_or(NodeOperationState::Unspecified as i32),
        cleanup_delivered_at: cleanup_delivered_at.map(timestamp),
        proxy_process_instance_id: row.get("proxy_process_instance_id"),
        resolved_release_digest: row.get("resolved_release_digest"),
        proxy_next_step: parse_step(row.get::<_, String>("proxy_next_step").as_str())? as i32,
        proxy_source_revision: row.get::<_, i64>("proxy_source_revision") as u64,
        proxy_source_digest: row.get("proxy_source_digest"),
        proxy_source_resolved_release_digest: row.get("proxy_source_resolved_digest"),
        proxy_target_revision: row.get::<_, i64>("proxy_target_revision") as u64,
        proxy_target_digest: row.get("proxy_target_digest"),
        proxy_target_resolved_release_digest: row.get("proxy_target_resolved_digest"),
        proxy_backend_instance_id: row.get("proxy_backend_instance_id"),
        proxy_changed: row.get("proxy_changed"),
        proxy_effect_ambiguous: row.get("proxy_effect_ambiguous"),
        proxy_effect_delivered_at: row
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>("proxy_effect_delivered_at")
            .map(timestamp),
        proxy_effect_reported: row.get("proxy_effect_reported"),
        proxy_effect_observed_revision: row.get::<_, i64>("proxy_effect_observed_revision") as u64,
        proxy_effect_observed_digest: row.get("proxy_effect_observed_digest"),
        proxy_effect_observed_resolved_release_digest: row
            .get("proxy_effect_observed_resolved_digest"),
        proxy_effect_backend_instance_id: row.get("proxy_effect_backend_instance_id"),
        proxy_effect_process_instance_id: row.get("proxy_effect_process_instance_id"),
        proxy_effect_condition_code: row.get("proxy_effect_condition_code"),
        proxy_effect_detail: row.get("proxy_effect_detail"),
        termination_evidence: decode_termination_evidence(
            row.get("proxy_effect_termination_evidence"),
        )?,
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

fn first_forward_step(
    action: NodeOperationAction,
    source_revision: i64,
    target_revision: i64,
) -> NodeOperationStepKind {
    match action {
        // A read-only source proof pins the exact process/backend identities
        // before any destructive stop instruction may be emitted.
        NodeOperationAction::Drain | NodeOperationAction::Restart => {
            NodeOperationStepKind::VerifyTarget
        }
        NodeOperationAction::Scale if source_revision > 0 && target_revision == 0 => {
            NodeOperationStepKind::VerifyTarget
        }
        NodeOperationAction::InitialApply
        | NodeOperationAction::RollingUpgrade
        | NodeOperationAction::Scale
        | NodeOperationAction::Rollback => NodeOperationStepKind::VerifyReleaseMetadata,
        NodeOperationAction::Unspecified => NodeOperationStepKind::Unspecified,
    }
}

fn next_forward_step(
    action: NodeOperationAction,
    step: NodeOperationStepKind,
    source_revision: i64,
    target_revision: i64,
) -> Option<NodeOperationStepKind> {
    use NodeOperationStepKind as Step;
    match (action, step) {
        (NodeOperationAction::Drain, Step::StopBackend) => Some(Step::SwitchAuthority),
        (NodeOperationAction::Restart, Step::StopBackend) => Some(Step::StartBackend),
        (NodeOperationAction::Scale, Step::StopBackend) if target_revision == 0 => {
            Some(Step::SwitchAuthority)
        }
        (_, Step::VerifyReleaseMetadata) if source_revision > 0 => Some(Step::VerifyTarget),
        (_, Step::VerifyReleaseMetadata) => Some(Step::SelectRelease),
        (_, Step::StopBackend) => Some(Step::SelectRelease),
        (_, Step::SelectRelease) => Some(Step::StartBackend),
        (_, Step::StartBackend) => Some(Step::VerifyTarget),
        (_, Step::SwitchAuthority) if target_revision == 0 => None,
        (_, Step::SwitchAuthority) => Some(Step::VerifyServing),
        (_, Step::VerifyServing) => None,
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
    let policy = request.policy.clone().unwrap_or(RolloutPolicy {
        max_unavailable: 1,
        canary_slot: String::new(),
        pause_after_canary: false,
        allow_downtime: false,
        deadline_seconds: match action {
            NodeOperationAction::Drain => 120,
            NodeOperationAction::Restart => 300,
            _ => 1800,
        },
    });
    let target_revision = i64::try_from(request.target_revision)
        .map_err(|_| Status::invalid_argument("target_revision is too large"))?;
    let deployment_action = matches!(
        action,
        NodeOperationAction::InitialApply
            | NodeOperationAction::RollingUpgrade
            | NodeOperationAction::Scale
            | NodeOperationAction::Rollback
    );
    let mut operation_id = (!deployment_action).then(Uuid::new_v4);
    let mut target_revision_digest: Option<String> = None;

    let (source_digest, source_resolved_digest, source_slots) =
        deployment_inventory(&transaction, &request.node_id, source_revision).await?;
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
            request.engine_slots.clone(),
        )
    };
    let operation_id = operation_id.expect("deployment actions derive an operation ID");

    // An urgent rollback fences committed cleanup before the new operation is
    // admitted. History/evidence remain, and the old cleanup can issue no effect.
    if action == NodeOperationAction::Rollback {
        if let Some(cleanup) = transaction
            .query_opt(
                "SELECT operation_id FROM wr_node_operations
                 WHERE node_id = $1 AND committed AND phase = 'committed_cleanup'
                   AND state IN ('queued', 'running', 'paused') FOR UPDATE",
                &[&request.node_id],
            )
            .await
            .map_err(internal)?
        {
            let cleanup_id: Uuid = cleanup.get("operation_id");
            transaction
                .execute(
                    "UPDATE wr_node_operations SET phase = 'superseded', state = 'succeeded',
                            lease_expires_at = NULL, claimed_by = NULL, agent_instance_id = NULL,
                            updated_at = NOW() WHERE operation_id = $1",
                    &[&cleanup_id],
                )
                .await
                .map_err(internal)?;
            append_event(
                &transaction,
                cleanup_id,
                actor,
                "CLEANUP_SUPERSEDED",
                &operation_id.to_string(),
                0,
            )
            .await?;
        }
    }

    if action == NodeOperationAction::InitialApply && source_revision != 0 {
        return Err(Status::failed_precondition(
            "initial apply requires a node with no committed revision",
        ));
    }
    if matches!(
        action,
        NodeOperationAction::RollingUpgrade | NodeOperationAction::Scale
    ) && source_revision == 0
    {
        return Err(Status::failed_precondition(
            "upgrade and scale require a committed source revision",
        ));
    }
    if action == NodeOperationAction::RollingUpgrade && source_slots != target_slots {
        return Err(Status::failed_precondition(
            "rolling upgrade cannot change the immutable slot inventory",
        ));
    }
    let mut requested = request.engine_slots.clone();
    requested.sort();
    requested.dedup();
    let mut expected_target = target_slots.clone();
    expected_target.sort();
    expected_target.dedup();
    if deployment_action && requested != expected_target {
        return Err(Status::failed_precondition(
            "operation slots do not match the staged deployment inventory",
        ));
    }
    if matches!(
        action,
        NodeOperationAction::Drain | NodeOperationAction::Restart
    ) && (source_slots.is_empty() || requested.iter().any(|slot| !source_slots.contains(slot)))
    {
        return Err(Status::failed_precondition(
            "drain and restart require slots from the committed deployment",
        ));
    }
    let withdraws_last_source = source_slots.len() == 1
        && match action {
            NodeOperationAction::Drain | NodeOperationAction::Restart => true,
            NodeOperationAction::RollingUpgrade | NodeOperationAction::Rollback => {
                target_slots.len() <= 1
            }
            NodeOperationAction::Scale => target_slots.is_empty(),
            NodeOperationAction::InitialApply | NodeOperationAction::Unspecified => false,
        };
    if withdraws_last_source && !policy.allow_downtime {
        return Err(Status::failed_precondition(
            "withdrawing the last healthy source slot requires allow_downtime",
        ));
    }
    let slots = ordered_operation_slots(
        action,
        &source_slots,
        &target_slots,
        if deployment_action {
            None
        } else {
            Some(requested)
        },
        &policy.canary_slot,
    );
    if slots.is_empty() && action != NodeOperationAction::Scale {
        return Err(Status::invalid_argument("operation has no affected slots"));
    }
    if deployment_action {
        transaction
            .execute(
                "UPDATE wr_nodes SET target_revision = $2, updated_at = NOW() WHERE node_id = $1",
                &[&request.node_id, &target_revision],
            )
            .await
            .map_err(internal)?;
    }
    let deadline_seconds = i64::try_from(policy.deadline_seconds)
        .map_err(|_| Status::invalid_argument("deadline_seconds is too large"))?;
    transaction
        .execute(
            "INSERT INTO wr_node_operations
               (operation_id, node_id, request_token, actor, action, state, request_payload,
                policy, source_revision, target_revision, bundle_digest, resolved_release_digest,
                target_revision_digest, forward_deadline, proxy_next_step,
                proxy_source_revision, proxy_source_digest, proxy_source_resolved_digest,
                proxy_target_revision, proxy_target_digest, proxy_target_resolved_digest)
             VALUES ($1, $2, $3, $4, $5, 'queued', $6, $7, $8, $9, $10, $11, $12,
                     NOW() + make_interval(secs => $13::double precision), $14, $8, $15, $16,
                     $9, $10, $11)",
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
                &if deployment_action {
                    if source_revision > 0 {
                        // Explicit source proof must produce a result. Reserve
                        // inspect_backend for observation-only ambiguity recovery.
                        "verify_target"
                    } else {
                        "select_release"
                    }
                } else {
                    "complete"
                },
                &source_digest,
                &source_resolved_digest,
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
    if action == NodeOperationAction::Rollback {
        transaction
            .execute(
                "UPDATE wr_node_operations SET cleanup_superseded_by = $2
                 WHERE node_id = $1 AND phase = 'superseded'
                   AND cleanup_superseded_by IS NULL",
                &[&request.node_id, &operation_id],
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
        } else if action == NodeOperationAction::Drain {
            0
        } else {
            source_revision
        };
        let first = first_forward_step(action, source, target);
        transaction
            .execute(
                "INSERT INTO wr_node_operation_slots
                   (operation_id, node_id, engine_slot, rollout_order,
                    authoritative_revision, next_step, source_revision, source_digest,
                    source_resolved_digest, target_revision, target_digest, target_resolved_digest)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
                &[
                    &operation_id,
                    &request.node_id,
                    &slot,
                    &(rollout_order as i32),
                    &source,
                    &step_name(first),
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
    }
    append_event(
        &transaction,
        operation_id,
        actor,
        "OPERATION_SUBMITTED",
        action_name(action),
        0,
    )
    .await?;
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
                 proxy_next_step = CASE WHEN proxy_changed OR proxy_effect_ambiguous
                                        THEN 'restore_source' ELSE 'complete' END,
                 proxy_effect_delivered_at = NULL, proxy_effect_reported = FALSE,
                 proxy_effect_condition_code = '', proxy_effect_detail = '',
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
            "UPDATE wr_node_operation_slots
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
                    failure_code = '', failure_detail = '',
                    cleanup_delete_allowlist = CASE
                        WHEN phase = 'committed_cleanup'
                             AND (cleanup_delivered_at IS NULL OR cleanup_reported_at IS NOT NULL)
                        THEN NULL ELSE cleanup_delete_allowlist END,
                    cleanup_delivered_at = CASE
                        WHEN phase = 'committed_cleanup' AND cleanup_reported_at IS NOT NULL
                        THEN NULL ELSE cleanup_delivered_at END,
                    cleanup_evidence = CASE
                        WHEN phase = 'committed_cleanup' AND cleanup_reported_at IS NOT NULL
                        THEN NULL ELSE cleanup_evidence END,
                    cleanup_reported_at = CASE
                        WHEN phase = 'committed_cleanup' AND cleanup_reported_at IS NOT NULL
                        THEN NULL ELSE cleanup_reported_at END,
                    cleanup_backend_query_error = CASE
                        WHEN phase = 'committed_cleanup' AND cleanup_reported_at IS NOT NULL
                        THEN '' ELSE cleanup_backend_query_error END
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
            "UPDATE wr_node_operation_slots SET condition_code = $3, condition_detail = $4,
                    updated_at = NOW() WHERE operation_id = $1 AND engine_slot = $2",
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
    for candidate in &operation.slots {
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
            "UPDATE wr_node_operation_slots
             SET completed_steps = completed_steps + 1, next_step = $3, complete = $4,
                 authoritative_revision = $5, effect_ambiguous = FALSE,
                 effect_delivered_at = NULL, effect_reported = FALSE,
                 effect_observed_revision = 0, effect_observed_digest = '',
                 effect_backend_instance_id = '', effect_process_instance_id = '',
                 condition_code = '', condition_detail = '', updated_at = NOW()
             WHERE operation_id = $1 AND engine_slot = $2",
            &[
                &id,
                &slot,
                &next.map(step_name).unwrap_or("complete"),
                &complete,
                &authoritative_revision,
            ],
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
    let action =
        NodeOperationAction::try_from(operation.action).unwrap_or(NodeOperationAction::Unspecified);
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
                    "UPDATE wr_node_operation_slots
                     SET changed = TRUE, effect_ambiguous = FALSE,
                         effect_delivered_at = NULL, condition_code = '', condition_detail = ''
                     WHERE operation_id = $1 AND engine_slot = $2",
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
    let next = next_forward_step(
        action,
        step,
        slot.source_revision as i64,
        slot.target_revision as i64,
    );
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
                        "UPDATE wr_node_operation_slots
                         SET effect_delivered_at = NULL, effect_ambiguous = FALSE
                         WHERE operation_id = $1 AND engine_slot = $2",
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
                    "UPDATE wr_node_operation_slots SET changed = TRUE
                     WHERE operation_id = $1 AND engine_slot = $2",
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
                        "UPDATE wr_node_operation_slots
                         SET effect_delivered_at = NULL, effect_ambiguous = FALSE
                         WHERE operation_id = $1 AND engine_slot = $2",
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
                        "UPDATE wr_node_operation_slots SET changed = TRUE
                         WHERE operation_id = $1 AND engine_slot = $2",
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
                            "UPDATE wr_node_operation_slots
                             SET effect_delivered_at = NULL, effect_ambiguous = FALSE
                             WHERE operation_id = $1 AND engine_slot = $2",
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
                        "UPDATE wr_node_operation_slots
                         SET pinned_backend_instance_id = $3, pinned_process_instance_id = $4
                         WHERE operation_id = $1 AND engine_slot = $2",
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
                    "UPDATE wr_node_operation_slots
                     SET pinned_backend_instance_id = $3, pinned_process_instance_id = $4
                     WHERE operation_id = $1 AND engine_slot = $2",
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
                    "UPDATE wr_node_operation_slots
                     SET authority_switched = TRUE, changed = TRUE
                     WHERE operation_id = $1 AND engine_slot = $2",
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
                    "UPDATE wr_node_operation_slots SET serving_converged = TRUE
                     WHERE operation_id = $1 AND engine_slot = $2",
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
    if matches!(
        phase,
        NodeOperationPhase::CommittedCleanup | NodeOperationPhase::Superseded
    ) {
        return Ok(());
    }
    if NodeOperationStepKind::try_from(operation.proxy_next_step)
        .unwrap_or(NodeOperationStepKind::Unspecified)
        != NodeOperationStepKind::Unspecified
    {
        return Ok(());
    }
    if let Some(slot) = operation
        .slots
        .iter()
        .filter(|slot| !slot.complete)
        .min_by_key(|slot| slot.rollout_order)
    {
        if !reconcile_slot(client, id, &snapshot, operation, slot).await? {
            return Ok(());
        }
        let updated = load_operation(client, id).await?;
        let updated_slot = updated
            .slots
            .iter()
            .find(|candidate| candidate.engine_slot == slot.engine_slot)
            .expect("operation slot remains durable");
        if phase == NodeOperationPhase::Forward
            && slot.rollout_order == 0
            && updated_slot.complete
            && updated.slots.iter().any(|candidate| !candidate.complete)
            && operation
                .policy
                .as_ref()
                .is_some_and(|policy| policy.pause_after_canary)
        {
            client
                .execute(
                    "UPDATE wr_node_operations SET state = 'paused',
                            failure_code = 'CANARY_PAUSED',
                            failure_detail = 'explicit resume is required after canary',
                            lease_expires_at = NULL, claimed_by = NULL,
                            agent_instance_id = NULL, updated_at = NOW()
                     WHERE operation_id = $1",
                    &[&id],
                )
                .await
                .map_err(internal)?;
            append_event(
                client,
                id,
                actor,
                "CANARY_PAUSED",
                "explicit resume is required after canary",
                operation.lease_epoch as i64,
            )
            .await?;
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
    let commits = matches!(
        action,
        NodeOperationAction::InitialApply
            | NodeOperationAction::RollingUpgrade
            | NodeOperationAction::Scale
            | NodeOperationAction::Rollback
    );
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
        let next_phase = if operation.source_revision == 0 {
            "complete"
        } else {
            "committed_cleanup"
        };
        client
            .execute(
                "UPDATE wr_node_operations SET committed = TRUE, committed_at = NOW(),
                        phase = $2, state = CASE WHEN $2 = 'complete' THEN 'succeeded' ELSE state END,
                        lease_expires_at = CASE WHEN $2 = 'complete' THEN NULL ELSE lease_expires_at END,
                        claimed_by = CASE WHEN $2 = 'complete' THEN NULL ELSE claimed_by END,
                        agent_instance_id = CASE WHEN $2 = 'complete' THEN NULL ELSE agent_instance_id END,
                        updated_at = NOW()
                 WHERE operation_id = $1",
                &[&id, &next_phase],
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

async fn manager_cleanup_delete_allowlist<C>(
    client: &C,
    node_id: &str,
) -> Result<Vec<wr_common::wruntime::ReleaseInventoryEntry>, Status>
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
            "SELECT revision, bundle_digest, resolved_release_digest, state
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
             WHERE node_id = $1 AND (
                 state IN ('queued', 'running', 'paused')
                 OR phase IN ('committed_cleanup', 'superseded')
             )",
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
    Ok(known
        .into_iter()
        .filter(|(revision, _)| !protected.contains(revision))
        .map(|(revision, (bundle_digest, resolved_release_digest))| {
            wr_common::wruntime::ReleaseInventoryEntry {
                revision: revision as u64,
                bundle_digest,
                resolved_release_digest,
            }
        })
        .collect())
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
               AND a.config_digest = p.config_digest
               AND a.backend = p.backend
               AND a.retention_count = p.retention_count
               AND a.capabilities = p.capabilities
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
    let proxy_step = NodeOperationStepKind::try_from(operation.proxy_next_step)
        .unwrap_or(NodeOperationStepKind::Unspecified);
    let proxy_pending =
        proxy_step != NodeOperationStepKind::Unspecified && step_name(proxy_step) != "complete";
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
    ) = if phase == NodeOperationPhase::CommittedCleanup {
        (
            String::new(),
            NodeOperationStepKind::CleanupRelease,
            operation.source_revision,
            operation
                .slots
                .iter()
                .find(|slot| slot.source_revision > 0)
                .map(|slot| slot.source_digest.clone())
                .unwrap_or_default(),
            operation
                .slots
                .iter()
                .find(|slot| slot.source_revision > 0)
                .map(|slot| slot.source_resolved_release_digest.clone())
                .unwrap_or_default(),
            String::new(),
            String::new(),
            operation.cleanup_delivered_at.is_some(),
            false,
            InstructionTargetKind::ReleaseCleanup,
        )
    } else if proxy_pending {
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
                operation.proxy_source_revision
            } else {
                operation.proxy_target_revision
            },
            if uses_source {
                operation.proxy_source_digest.clone()
            } else {
                operation.proxy_target_digest.clone()
            },
            if uses_source {
                operation.proxy_source_resolved_release_digest.clone()
            } else {
                operation.proxy_target_resolved_release_digest.clone()
            },
            operation.proxy_backend_instance_id.clone(),
            operation.proxy_process_instance_id.clone(),
            operation.proxy_effect_delivered_at.is_some(),
            operation.proxy_effect_ambiguous,
            InstructionTargetKind::Proxy,
        )
    } else if let Some(slot) = operation.slots.iter().find(|slot| !slot.complete) {
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
    let cleanup_delete_releases = if step == NodeOperationStepKind::CleanupRelease {
        let stored = transaction
            .query_one(
                "SELECT cleanup_delete_allowlist FROM wr_node_operations WHERE operation_id = $1",
                &[&id],
            )
            .await
            .map_err(internal)?
            .get::<_, Option<Vec<u8>>>("cleanup_delete_allowlist");
        match stored {
            Some(bytes) => {
                CleanupReleaseEvidence::decode(bytes.as_slice())
                    .map_err(|error| {
                        Status::internal(format!("stored cleanup allow-list is invalid: {error}"))
                    })?
                    .retained_releases
            }
            None => manager_cleanup_delete_allowlist(&transaction, node_id).await?,
        }
    } else {
        Vec::new()
    };
    let mutating_effect = matches!(
        step,
        NodeOperationStepKind::StopBackend
            | NodeOperationStepKind::SelectRelease
            | NodeOperationStepKind::StartBackend
            | NodeOperationStepKind::RestoreSource
            | NodeOperationStepKind::CleanupRelease
    );
    // Cleanup deletion is exactly manager-allow-listed and locally idempotent.
    // After activation loss, reissue that same authority so the replacement can
    // prove the resulting inventory; other ambiguous mutations require inspect.
    let inspection =
        (mutating_effect && delivered && step != NodeOperationStepKind::CleanupRelease)
            || ambiguous;
    let instruction_step = if inspection {
        NodeOperationStepKind::InspectBackend
    } else {
        step
    };
    if mutating_effect && !inspection {
        if step == NodeOperationStepKind::CleanupRelease {
            let encoded_allowlist = CleanupReleaseEvidence {
                retained_releases: cleanup_delete_releases.clone(),
            }
            .encode_to_vec();
            transaction
                .execute(
                    "UPDATE wr_node_operations SET cleanup_delivered_at = NOW(),
                            cleanup_delete_allowlist = $2, updated_at = NOW()
                     WHERE operation_id = $1",
                    &[&id, &encoded_allowlist],
                )
                .await
                .map_err(internal)?;
        } else if target_kind == InstructionTargetKind::Proxy {
            transaction
                .execute(
                    "UPDATE wr_node_operations
                     SET proxy_effect_delivered_at = NOW(), proxy_effect_ambiguous = TRUE,
                         updated_at = NOW() WHERE operation_id = $1",
                    &[&id],
                )
                .await
                .map_err(internal)?;
        } else {
            transaction
                .execute(
                    "UPDATE wr_node_operation_slots
                     SET effect_delivered_at = NOW(), effect_ambiguous = TRUE, updated_at = NOW()
                     WHERE operation_id = $1 AND engine_slot = $2",
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
                engine_slot: slot_name,
                revision,
                bundle_digest: digest,
                resolved_release_digest: resolved_digest,
            }),
            pinned_backend_instance_id: pinned_backend,
            pinned_process_instance_id: pinned_process,
            restoration,
            cleanup_delete_releases,
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
                       FROM wr_node_operation_slots effects
                      WHERE effects.operation_id = wr_node_slot_observations.operation_id
                        AND effects.engine_slot = wr_node_slot_observations.engine_slot)
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
    reconcile(&transaction, id, agent).await?;
    let result = load_operation(&transaction, id).await?;
    transaction.commit().await.map_err(internal)?;
    Ok(result)
}

async fn reconcile_cleanup<C: GenericClient + Sync>(
    client: &C,
    id: Uuid,
    actor: &str,
) -> Result<(), Status> {
    let snapshot = crate::db::capture_cluster_status_snapshot(client).await?;
    let operation = snapshot
        .active_operations
        .iter()
        .find(|operation| operation.operation_id == id.to_string())
        .ok_or_else(|| Status::failed_precondition("cleanup operation is not active"))?;
    if NodeOperationPhase::try_from(operation.phase).ok()
        != Some(NodeOperationPhase::CommittedCleanup)
    {
        return Err(Status::failed_precondition(
            "operation is not in committed cleanup",
        ));
    }
    let mut failure = None;
    if !operation.cleanup_backend_query_error.is_empty() {
        failure = Some((
            "CLEANUP_QUERY_ERROR",
            operation.cleanup_backend_query_error.as_str(),
        ));
    }
    let evidence_fresh = operation
        .cleanup_reported_at
        .as_ref()
        .is_some_and(|reported| {
            chrono::DateTime::from_timestamp(reported.seconds, reported.nanos as u32).is_some_and(
                |time| {
                    snapshot.observed_at.signed_duration_since(time)
                        <= chrono::Duration::seconds(EVIDENCE_FRESH_SECONDS)
                },
            )
        });
    if failure.is_none() && (!evidence_fresh || operation.cleanup_evidence.is_none()) {
        failure = Some((
            "CLEANUP_EVIDENCE_MISSING",
            "fresh typed resulting release inventory is required",
        ));
    }
    let mut known = snapshot
        .deployments
        .iter()
        .filter(|deployment| deployment.record.node_id == operation.node_id)
        .map(|deployment| {
            (
                deployment.record.revision,
                (
                    deployment.record.bundle_digest.clone(),
                    deployment.record.resolved_release_digest.clone(),
                ),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    for deletion in client
        .query(
            "SELECT revision FROM wr_node_release_deletions WHERE node_id = $1",
            &[&operation.node_id],
        )
        .await
        .map_err(internal)?
    {
        known.remove(&(deletion.get::<_, i64>("revision") as u64));
    }
    let allowlist_bytes = client
        .query_one(
            "SELECT cleanup_delete_allowlist FROM wr_node_operations WHERE operation_id = $1",
            &[&id],
        )
        .await
        .map_err(internal)?
        .get::<_, Option<Vec<u8>>>("cleanup_delete_allowlist")
        .ok_or_else(|| Status::failed_precondition("cleanup deletion allow-list is missing"))?;
    let allowlist = CleanupReleaseEvidence::decode(allowlist_bytes.as_slice())
        .map_err(|error| {
            Status::internal(format!("stored cleanup allow-list is invalid: {error}"))
        })?
        .retained_releases;
    let mut deletions = std::collections::BTreeMap::new();
    for release in &allowlist {
        if deletions
            .insert(
                release.revision,
                (
                    release.bundle_digest.clone(),
                    release.resolved_release_digest.clone(),
                ),
            )
            .is_some()
            || known.get(&release.revision)
                != Some(&(
                    release.bundle_digest.clone(),
                    release.resolved_release_digest.clone(),
                ))
        {
            failure = Some((
                "CLEANUP_ALLOWLIST_INVALID",
                "persisted cleanup allow-list contains a duplicate or unknown revision/digest",
            ));
            break;
        }
    }
    let mut retained = std::collections::BTreeMap::new();
    if let Some(evidence) = operation.cleanup_evidence.as_ref() {
        for release in &evidence.retained_releases {
            if retained
                .insert(
                    release.revision,
                    (
                        release.bundle_digest.clone(),
                        release.resolved_release_digest.clone(),
                    ),
                )
                .is_some()
                || known.get(&release.revision)
                    != Some(&(
                        release.bundle_digest.clone(),
                        release.resolved_release_digest.clone(),
                    ))
            {
                failure = Some((
                    "CLEANUP_INVENTORY_INVALID",
                    "resulting inventory contains a duplicate or unknown revision/digest",
                ));
                break;
            }
        }
    }
    let expected_retained = known
        .iter()
        .filter(|(revision, _)| !deletions.contains_key(revision))
        .map(|(revision, digest)| (*revision, digest.clone()))
        .collect::<std::collections::BTreeMap<_, _>>();
    if failure.is_none() && retained != expected_retained {
        failure = Some((
            "CLEANUP_INVENTORY_MISMATCH",
            "resulting inventory does not exactly match the manager-authorized deletion set",
        ));
    }
    if let Some((code, detail)) = failure {
        client
            .execute(
                "UPDATE wr_node_operations SET state = 'paused', failure_code = $2,
                        failure_detail = $3, lease_expires_at = NULL, claimed_by = NULL,
                        agent_instance_id = NULL, updated_at = NOW() WHERE operation_id = $1",
                &[&id, &code, &detail],
            )
            .await
            .map_err(internal)?;
        append_event(
            client,
            id,
            actor,
            code,
            detail,
            operation.lease_epoch as i64,
        )
        .await?;
    } else {
        for release in &allowlist {
            client
                .execute(
                    "INSERT INTO wr_node_release_deletions
                       (node_id, revision, bundle_digest, resolved_release_digest, operation_id)
                     VALUES ($1, $2, $3, $4, $5)
                     ON CONFLICT (node_id, revision) DO NOTHING",
                    &[
                        &operation.node_id,
                        &(release.revision as i64),
                        &release.bundle_digest,
                        &release.resolved_release_digest,
                        &id,
                    ],
                )
                .await
                .map_err(internal)?;
            append_event(
                client,
                id,
                actor,
                "RELEASE_DELETED",
                &format!("{}:{}", release.revision, release.bundle_digest),
                operation.lease_epoch as i64,
            )
            .await?;
        }
        client
            .execute(
                "UPDATE wr_node_operations SET state = 'succeeded', phase = 'complete',
                        lease_expires_at = NULL, claimed_by = NULL, agent_instance_id = NULL,
                        failure_code = '', failure_detail = '', updated_at = NOW()
                 WHERE operation_id = $1",
                &[&id],
            )
            .await
            .map_err(internal)?;
        append_event(
            client,
            id,
            actor,
            "CLEANUP_SUCCEEDED",
            "typed release inventory satisfies retention and protection policy",
            operation.lease_epoch as i64,
        )
        .await?;
    }
    Ok(())
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
               AND lease_epoch = $4 AND step = $5 AND engine_slot = $6 FOR UPDATE",
            &[
                &id,
                &request.node_id,
                &request.agent_instance_id,
                &epoch,
                &request.step,
                &request.engine_slot,
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
            "SELECT phase, proxy_process_instance_id, proxy_next_step,
                    proxy_source_revision, proxy_source_digest, proxy_source_resolved_digest,
                    proxy_target_revision, proxy_target_digest, proxy_target_resolved_digest
             FROM wr_node_operations
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
    if phase == "committed_cleanup" {
        if reported_step != NodeOperationStepKind::CleanupRelease {
            return Err(Status::aborted(
                "committed cleanup result has the wrong typed step",
            ));
        }
        if !request.condition_code.is_empty() {
            transaction
                .execute(
                    "UPDATE wr_node_operations SET state = 'paused', failure_code = $2,
                            failure_detail = $3, lease_expires_at = NULL, claimed_by = NULL,
                            agent_instance_id = NULL, updated_at = NOW() WHERE operation_id = $1",
                    &[&id, &request.condition_code, &request.detail],
                )
                .await
                .map_err(internal)?;
            append_event(
                &transaction,
                id,
                agent,
                &request.condition_code,
                &request.detail,
                epoch,
            )
            .await?;
        } else {
            let evidence = request
                .cleanup_evidence
                .as_ref()
                .map(Message::encode_to_vec);
            transaction
                .execute(
                    "UPDATE wr_node_operations
                     SET cleanup_evidence = $2, cleanup_reported_at = NOW(),
                         cleanup_backend_query_error = $3, updated_at = NOW()
                     WHERE operation_id = $1",
                    &[&id, &evidence, &request.backend_query_error],
                )
                .await
                .map_err(internal)?;
            reconcile_cleanup(&transaction, id, agent).await?;
        }
    } else if request.engine_slot.is_empty() {
        let expected = parse_step(operation.get::<_, String>("proxy_next_step").as_str())?;
        if expected == NodeOperationStepKind::Unspecified || expected != reported_step {
            return Err(Status::aborted(format!(
                "stale proxy result: expected {}, received {}",
                step_name(expected),
                step_name(reported_step)
            )));
        }
        let source_revision: i64 = operation.get("proxy_source_revision");
        let target_revision: i64 = operation.get("proxy_target_revision");
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
                    operation.get("proxy_source_digest"),
                    operation.get("proxy_source_resolved_digest"),
                )
            } else {
                (
                    target_revision,
                    operation.get("proxy_target_digest"),
                    operation.get("proxy_target_resolved_digest"),
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
                    "UPDATE wr_node_operations SET state = 'paused', failure_code = $2,
                            failure_detail = $3, proxy_effect_condition_code = $2,
                            proxy_effect_detail = $3, lease_expires_at = NULL,
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
                    "UPDATE wr_node_operations
                     SET proxy_next_step = $2, proxy_changed = proxy_changed OR $3,
                         proxy_effect_ambiguous = FALSE, proxy_effect_reported = TRUE,
                         proxy_effect_observed_revision = $4,
                         proxy_effect_observed_digest = $5,
                         proxy_effect_observed_resolved_digest = $6,
                         proxy_effect_backend_instance_id = $7,
                         proxy_effect_process_instance_id = $8,
                         proxy_backend_instance_id = CASE
                             WHEN $7 <> '' THEN $7 ELSE proxy_backend_instance_id END,
                         proxy_process_instance_id = CASE
                             WHEN $8 <> '' THEN $8 ELSE proxy_process_instance_id END,
                         proxy_effect_condition_code = '', proxy_effect_detail = '',
                         proxy_effect_delivered_at = NULL,
                         proxy_effect_termination_evidence = CASE
                             WHEN $9 THEN $10 ELSE proxy_effect_termination_evidence END,
                         updated_at = NOW()
                     WHERE operation_id = $1",
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
                 FROM wr_node_operation_slots
                 WHERE operation_id = $1 AND engine_slot = $2 AND NOT complete FOR UPDATE",
                &[&id, &request.engine_slot],
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
                "UPDATE wr_node_operation_slots
                 SET effect_reported = TRUE, effect_condition_code = $3, effect_detail = $4,
                     effect_observed_revision = $5, effect_observed_digest = $6,
                     effect_observed_resolved_digest = $9,
                     effect_backend_instance_id = $7, effect_process_instance_id = $8,
                     effect_termination_evidence = CASE
                         WHEN $10 THEN $11 ELSE effect_termination_evidence END,
                     updated_at = NOW() WHERE operation_id = $1 AND engine_slot = $2",
                &[
                    &id,
                    &request.engine_slot,
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
                    "UPDATE wr_node_operations
                     SET proxy_process_instance_id = CASE
                         WHEN proxy_process_instance_id = '' THEN $2
                         ELSE proxy_process_instance_id END,
                         updated_at = NOW()
                     WHERE operation_id = $1",
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
            &format!("{}:{}", request.engine_slot, step_name(reported_step)),
            epoch,
        )
        .await?;
        reconcile(&transaction, id, agent).await?;
    }
    transaction
        .execute(
            "INSERT INTO wr_node_operation_result_receipts
               (operation_id, node_id, agent_instance_id, lease_epoch, step, engine_slot,
                authenticated_principal, result_payload)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            &[
                &id,
                &request.node_id,
                &request.agent_instance_id,
                &epoch,
                &request.step,
                &request.engine_slot,
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

fn canonical_agent_policy(policy: &NodeAgentPolicy) -> Result<AgentPolicy, Status> {
    let backend = match BackendKind::try_from(policy.backend).unwrap_or(BackendKind::Unspecified) {
        BackendKind::Systemd => AgentPolicyBackend::Systemd,
        BackendKind::Docker => AgentPolicyBackend::Docker,
        BackendKind::Unspecified => {
            return Err(Status::invalid_argument("agent backend is required"))
        }
    };
    let canonical = AgentPolicy {
        policy_version: policy.policy_version,
        node_id: policy.node_id.clone(),
        manager_endpoint: policy.manager_endpoint.clone(),
        client_cert_path: policy.client_cert_path.clone(),
        client_key_path: policy.client_key_path.clone(),
        ca_cert_path: policy.ca_cert_path.clone(),
        deployment_root: policy.deployment_root.clone(),
        runtime_dir: policy.runtime_dir.clone(),
        backend,
        compose_project: policy.compose_project.clone(),
        systemctl_path: policy.systemctl_path.clone(),
        docker_path: policy.docker_path.clone(),
        poll_interval_seconds: policy.poll_interval_seconds,
        renew_interval_seconds: policy.renew_interval_seconds,
        retention_count: policy.retention_count,
        protocol_version: policy.protocol_version.clone(),
        capabilities: policy.capabilities.clone(),
    }
    .normalized()
    .map_err(|error| Status::invalid_argument(format!("invalid node-agent policy: {error:#}")))?;
    let expected_digest = canonical.canonical_digest().map_err(|error| {
        Status::invalid_argument(format!("invalid node-agent policy: {error:#}"))
    })?;
    if policy.config_digest != expected_digest {
        return Err(Status::invalid_argument(
            "config_digest does not match the canonical node-agent policy",
        ));
    }
    if !policy.binary_digest.starts_with("sha256:")
        || policy.binary_digest.len() != 71
        || !policy.binary_digest[7..]
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(Status::invalid_argument(
            "binary_digest must be an exact SHA-256 digest",
        ));
    }
    Ok(canonical)
}

fn policy_proto(
    policy: AgentPolicy,
    binary_digest: String,
    config_digest: String,
) -> NodeAgentPolicy {
    NodeAgentPolicy {
        node_id: policy.node_id,
        protocol_version: policy.protocol_version,
        config_digest,
        backend: match policy.backend {
            AgentPolicyBackend::Systemd => BackendKind::Systemd,
            AgentPolicyBackend::Docker => BackendKind::Docker,
        } as i32,
        retention_count: policy.retention_count,
        policy_version: policy.policy_version,
        binary_digest,
        manager_endpoint: policy.manager_endpoint,
        client_cert_path: policy.client_cert_path,
        client_key_path: policy.client_key_path,
        ca_cert_path: policy.ca_cert_path,
        deployment_root: policy.deployment_root,
        runtime_dir: policy.runtime_dir,
        compose_project: policy.compose_project,
        systemctl_path: policy.systemctl_path,
        docker_path: policy.docker_path,
        poll_interval_seconds: policy.poll_interval_seconds,
        renew_interval_seconds: policy.renew_interval_seconds,
        capabilities: policy.capabilities,
    }
}

pub async fn put_agent_policy(
    pool: &Pool,
    actor: &str,
    policy: &NodeAgentPolicy,
) -> Result<NodeAgentPolicy, Status> {
    let canonical = canonical_agent_policy(policy)?;
    let config_digest = canonical.canonical_digest().map_err(|error| {
        Status::invalid_argument(format!("invalid node-agent policy: {error:#}"))
    })?;
    let backend = match canonical.backend {
        AgentPolicyBackend::Systemd => "systemd",
        AgentPolicyBackend::Docker => "docker",
    };
    let retention = i32::try_from(canonical.retention_count)
        .map_err(|_| Status::invalid_argument("retention_count is too large"))?;
    let policy_version = i32::try_from(canonical.policy_version)
        .map_err(|_| Status::invalid_argument("policy_version is too large"))?;
    let poll = i64::try_from(canonical.poll_interval_seconds)
        .map_err(|_| Status::invalid_argument("poll interval is too large"))?;
    let renew = i64::try_from(canonical.renew_interval_seconds)
        .map_err(|_| Status::invalid_argument("renew interval is too large"))?;
    let mut client = pool.get().await.map_err(internal)?;
    let transaction = client.transaction().await.map_err(internal)?;
    let prior = transaction
        .query_opt(
            "SELECT config_digest, binary_digest FROM wr_node_agent_policies WHERE node_id = $1 FOR UPDATE",
            &[&canonical.node_id],
        )
        .await
        .map_err(internal)?;
    let policy_changed = prior.as_ref().is_some_and(|row| {
        row.get::<_, String>("config_digest") != config_digest
            || row.get::<_, String>("binary_digest") != policy.binary_digest
    });
    transaction
        .execute(
            "INSERT INTO wr_nodes(node_id) VALUES ($1) ON CONFLICT(node_id) DO NOTHING",
            &[&canonical.node_id],
        )
        .await
        .map_err(internal)?;
    transaction
        .execute(
            "INSERT INTO wr_node_agent_policies
               (node_id, protocol_version, config_digest, backend, retention_count, actor,
                policy_version, binary_digest, manager_endpoint, client_cert_path, client_key_path,
                ca_cert_path, deployment_root, runtime_dir, compose_project, systemctl_path,
                docker_path, poll_interval_seconds, renew_interval_seconds, capabilities)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
                     $15, $16, $17, $18, $19, $20)
             ON CONFLICT(node_id) DO UPDATE SET protocol_version = EXCLUDED.protocol_version,
               config_digest = EXCLUDED.config_digest, backend = EXCLUDED.backend,
               retention_count = EXCLUDED.retention_count, actor = EXCLUDED.actor,
               policy_version = EXCLUDED.policy_version, binary_digest = EXCLUDED.binary_digest,
               manager_endpoint = EXCLUDED.manager_endpoint, client_cert_path = EXCLUDED.client_cert_path,
               client_key_path = EXCLUDED.client_key_path, ca_cert_path = EXCLUDED.ca_cert_path,
               deployment_root = EXCLUDED.deployment_root, runtime_dir = EXCLUDED.runtime_dir,
               compose_project = EXCLUDED.compose_project, systemctl_path = EXCLUDED.systemctl_path,
               docker_path = EXCLUDED.docker_path, poll_interval_seconds = EXCLUDED.poll_interval_seconds,
               renew_interval_seconds = EXCLUDED.renew_interval_seconds,
               capabilities = EXCLUDED.capabilities, updated_at = NOW()",
            &[
                &canonical.node_id, &canonical.protocol_version, &config_digest, &backend, &retention,
                &actor, &policy_version, &policy.binary_digest, &canonical.manager_endpoint,
                &canonical.client_cert_path, &canonical.client_key_path, &canonical.ca_cert_path,
                &canonical.deployment_root, &canonical.runtime_dir, &canonical.compose_project,
                &canonical.systemctl_path, &canonical.docker_path, &poll, &renew,
                &canonical.capabilities,
            ],
        )
        .await
        .map_err(internal)?;
    if policy_changed {
        transaction
            .execute(
                "UPDATE wr_node_operations SET cleanup_delete_allowlist = NULL, updated_at = NOW()
                 WHERE node_id = $1 AND phase = 'committed_cleanup'
                   AND state IN ('queued', 'running', 'paused')
                   AND (cleanup_delivered_at IS NULL OR cleanup_reported_at IS NOT NULL)",
                &[&canonical.node_id],
            )
            .await
            .map_err(internal)?;
        for row in transaction
            .query(
                "UPDATE wr_node_operations SET state = 'paused', failure_code = 'AGENT_POLICY_UPDATED',
                        failure_detail = 'node-agent policy changed; attest the replacement and explicitly resume',
                        lease_expires_at = NULL, claimed_by = NULL, agent_instance_id = NULL,
                        updated_at = NOW()
                 WHERE node_id = $1 AND state = 'running'
                 RETURNING operation_id, lease_epoch",
                &[&canonical.node_id],
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
    Ok(policy_proto(
        canonical,
        policy.binary_digest.clone(),
        config_digest,
    ))
}

pub async fn attest(
    pool: &Pool,
    principal: &str,
    attestation: &NodeAgentAttestation,
) -> Result<Vec<DeploymentCondition>, Status> {
    if attestation.node_id.is_empty()
        || attestation.agent_instance_id.is_empty()
        || attestation.protocol_version.is_empty()
        || attestation.binary_digest.is_empty()
        || attestation.config_digest.is_empty()
    {
        return Err(Status::invalid_argument(
            "complete attestation identity and digests are required",
        ));
    }
    let backend = backend_name(
        BackendKind::try_from(attestation.backend).unwrap_or(BackendKind::Unspecified),
    )?;
    let client = pool.get().await.map_err(internal)?;
    let policy = client
        .query_opt(
            "SELECT protocol_version, binary_digest, config_digest, backend, retention_count,
                    capabilities FROM wr_node_agent_policies WHERE node_id = $1",
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
        if policy.get::<_, String>("config_digest") != attestation.config_digest {
            conditions.push(operation_condition(
                "CONFIG_MISMATCH".into(),
                "agent config digest does not match manager policy".into(),
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
        if policy.get::<_, i32>("retention_count") as u32 != attestation.retention_count {
            conditions.push(operation_condition(
                "RETENTION_MISMATCH".into(),
                "agent retention value does not match manager policy".into(),
            ));
        }
        if policy.get::<_, Vec<String>>("capabilities") != attestation.capabilities {
            conditions.push(operation_condition(
                "CAPABILITY_MISMATCH".into(),
                "agent capabilities do not match manager policy".into(),
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
                binary_digest, config_digest, backend, capabilities, observed_at, retention_count)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), $9)
             ON CONFLICT(node_id, agent_instance_id) DO UPDATE SET
               authenticated_principal = EXCLUDED.authenticated_principal,
               protocol_version = EXCLUDED.protocol_version, binary_digest = EXCLUDED.binary_digest,
               config_digest = EXCLUDED.config_digest, backend = EXCLUDED.backend,
               capabilities = EXCLUDED.capabilities, observed_at = NOW(),
               retention_count = EXCLUDED.retention_count",
            &[
                &attestation.node_id,
                &attestation.agent_instance_id,
                &principal,
                &attestation.protocol_version,
                &attestation.binary_digest,
                &attestation.config_digest,
                &backend,
                &attestation.capabilities,
                &(attestation.retention_count as i32),
            ],
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
            "SELECT node_id, protocol_version, config_digest, backend, retention_count,
                    policy_version, binary_digest, manager_endpoint, client_cert_path,
                    client_key_path, ca_cert_path, deployment_root, runtime_dir, compose_project,
                    systemctl_path, docker_path, poll_interval_seconds, renew_interval_seconds,
                    capabilities
             FROM wr_node_agent_policies ORDER BY node_id",
            &[],
        )
        .await
        .map_err(internal)?
        .into_iter()
        .map(|row| NodeAgentPolicy {
            node_id: row.get("node_id"),
            protocol_version: row.get("protocol_version"),
            config_digest: row.get("config_digest"),
            backend: parse_backend(row.get::<_, String>("backend").as_str()) as i32,
            retention_count: row.get::<_, i32>("retention_count") as u32,
            policy_version: row.get::<_, i32>("policy_version") as u32,
            binary_digest: row.get("binary_digest"),
            manager_endpoint: row.get("manager_endpoint"),
            client_cert_path: row.get("client_cert_path"),
            client_key_path: row.get("client_key_path"),
            ca_cert_path: row.get("ca_cert_path"),
            deployment_root: row.get("deployment_root"),
            runtime_dir: row.get("runtime_dir"),
            compose_project: row.get("compose_project"),
            systemctl_path: row.get("systemctl_path"),
            docker_path: row.get("docker_path"),
            poll_interval_seconds: row.get::<_, i64>("poll_interval_seconds") as u64,
            renew_interval_seconds: row.get::<_, i64>("renew_interval_seconds") as u64,
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
                    binary_digest, config_digest, backend, capabilities, observed_at,
                    retention_count
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
            config_digest: row.get("config_digest"),
            backend: parse_backend(row.get::<_, String>("backend").as_str()) as i32,
            capabilities: row.get("capabilities"),
            observed_at: Some(timestamp(row.get("observed_at"))),
            authenticated_principal: row.get("authenticated_principal"),
            retention_count: row.get::<_, i32>("retention_count") as u32,
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

fn ordered_operation_slots(
    action: NodeOperationAction,
    source_slots: &[String],
    target_slots: &[String],
    requested: Option<Vec<String>>,
    canary_slot: &str,
) -> Vec<String> {
    let mut slots = requested.unwrap_or_else(|| {
        source_slots
            .iter()
            .chain(target_slots.iter())
            .cloned()
            .collect()
    });
    slots.sort();
    slots.dedup();
    // Scale-out capacity lands first, retained slots roll next, and scale-in
    // removals happen last. The canary is first only within its safety group.
    slots.sort_by_key(|slot| {
        let group = if action == NodeOperationAction::Scale {
            match (source_slots.contains(slot), target_slots.contains(slot)) {
                (false, true) => 0,
                (true, true) => 1,
                (true, false) => 2,
                (false, false) => 3,
            }
        } else {
            0
        };
        (group, slot != canary_slot, slot.clone())
    });
    slots
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scale_orders_add_before_retain_and_retain_before_remove() {
        let one = vec!["engine-1".to_string()];
        let two = vec!["engine-1".to_string(), "engine-2".to_string()];
        assert_eq!(
            ordered_operation_slots(NodeOperationAction::Scale, &one, &two, None, "engine-1"),
            ["engine-2", "engine-1"]
        );
        assert_eq!(
            ordered_operation_slots(NodeOperationAction::Scale, &two, &one, None, "engine-1"),
            ["engine-1", "engine-2"]
        );
    }

    #[test]
    fn equal_slot_upgrade_keeps_stable_inventory_order() {
        let slots = vec!["engine-1".to_string(), "engine-2".to_string()];
        assert_eq!(
            ordered_operation_slots(
                NodeOperationAction::RollingUpgrade,
                &slots,
                &slots,
                None,
                "engine-1",
            ),
            slots
        );
    }

    #[test]
    fn action_specific_sequences_are_not_one_generic_list() {
        assert_eq!(
            next_forward_step(
                NodeOperationAction::Drain,
                NodeOperationStepKind::StopBackend,
                1,
                0,
            ),
            Some(NodeOperationStepKind::SwitchAuthority)
        );
        assert_eq!(
            next_forward_step(
                NodeOperationAction::Drain,
                NodeOperationStepKind::SwitchAuthority,
                1,
                0,
            ),
            None
        );
        assert_eq!(
            next_forward_step(
                NodeOperationAction::Restart,
                NodeOperationStepKind::StopBackend,
                1,
                1,
            ),
            Some(NodeOperationStepKind::StartBackend)
        );
        assert_eq!(
            first_forward_step(NodeOperationAction::Scale, 1, 0),
            NodeOperationStepKind::VerifyTarget
        );
        assert_eq!(
            first_forward_step(NodeOperationAction::InitialApply, 0, 1),
            NodeOperationStepKind::VerifyReleaseMetadata
        );
    }
}
