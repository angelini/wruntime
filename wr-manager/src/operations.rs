use deadpool_postgres::{GenericClient, Pool};
use prost::Message;
use tokio_postgres::Row;
use tonic::Status;
use uuid::Uuid;
use wr_common::wruntime::{
    AgentInstruction, BackendProcessState, ClaimOperationResponse, DeploymentCondition,
    NodeOperation, NodeOperationAction, NodeOperationState, NodeOperationStepKind, OperationEvent,
    OperationSlotProgress, ReportNodeObservationRequest, ReportStepResultRequest, RolloutPolicy,
    SlotObservation, SubmitOperationRequest,
};

const LEASE_SECONDS: f64 = 15.0;

fn internal(error: impl std::fmt::Debug) -> Status {
    Status::internal(format!("database operation failed: {error:?}"))
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

fn step_name(step: NodeOperationStepKind) -> &'static str {
    match step {
        NodeOperationStepKind::VerifyRelease => "verify_release",
        NodeOperationStepKind::StopSlot => "stop_slot",
        NodeOperationStepKind::SelectRelease => "select_release",
        NodeOperationStepKind::StartSlot => "start_slot",
        NodeOperationStepKind::VerifyReady => "verify_ready",
        NodeOperationStepKind::Unspecified => "complete",
    }
}

fn parse_step(value: &str) -> Result<NodeOperationStepKind, Status> {
    match value {
        "verify_release" => Ok(NodeOperationStepKind::VerifyRelease),
        "stop_slot" => Ok(NodeOperationStepKind::StopSlot),
        "select_release" => Ok(NodeOperationStepKind::SelectRelease),
        "start_slot" => Ok(NodeOperationStepKind::StartSlot),
        "verify_ready" => Ok(NodeOperationStepKind::VerifyReady),
        "complete" => Ok(NodeOperationStepKind::Unspecified),
        _ => Err(Status::internal("stored operation has an invalid step")),
    }
}

fn first_step(action: NodeOperationAction) -> NodeOperationStepKind {
    match action {
        NodeOperationAction::Drain | NodeOperationAction::Restart => {
            NodeOperationStepKind::StopSlot
        }
        NodeOperationAction::InitialApply
        | NodeOperationAction::RollingUpgrade
        | NodeOperationAction::Scale
        | NodeOperationAction::Rollback => NodeOperationStepKind::VerifyRelease,
        NodeOperationAction::Unspecified => NodeOperationStepKind::Unspecified,
    }
}

fn next_step(action: NodeOperationAction, completed: i32) -> Option<NodeOperationStepKind> {
    let steps: &[NodeOperationStepKind] = match action {
        NodeOperationAction::Drain => &[NodeOperationStepKind::StopSlot],
        NodeOperationAction::Restart => &[
            NodeOperationStepKind::StopSlot,
            NodeOperationStepKind::StartSlot,
            NodeOperationStepKind::VerifyReady,
        ],
        NodeOperationAction::InitialApply
        | NodeOperationAction::RollingUpgrade
        | NodeOperationAction::Scale
        | NodeOperationAction::Rollback => &[
            NodeOperationStepKind::VerifyRelease,
            NodeOperationStepKind::StopSlot,
            NodeOperationStepKind::SelectRelease,
            NodeOperationStepKind::StartSlot,
            NodeOperationStepKind::VerifyReady,
        ],
        NodeOperationAction::Unspecified => &[],
    };
    steps.get(completed as usize).copied()
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
                    source_revision, target_revision, bundle_digest, committed, lease_epoch,
                    lease_expires_at, failure_code, failure_detail, created_at, updated_at
             FROM wr_node_operations WHERE operation_id = $1",
            &[&operation_id],
        )
        .await
        .map_err(internal)?
        .ok_or_else(|| Status::not_found("operation not found"))?;
    let slots = client
        .query(
            "SELECT engine_slot, authoritative_revision, next_step, completed_steps, complete,
                    condition_code, condition_detail
             FROM wr_node_operation_slots WHERE operation_id = $1 ORDER BY rollout_order",
            &[&operation_id],
        )
        .await
        .map_err(internal)?;
    operation_from_row(&row, &slots)
}

fn operation_from_row(row: &Row, slots: &[Row]) -> Result<NodeOperation, Status> {
    let policy_bytes: Vec<u8> = row.get("policy");
    let policy = RolloutPolicy::decode(policy_bytes.as_slice())
        .map_err(|error| Status::internal(format!("stored rollout policy is invalid: {error}")))?;
    let failure_code: String = row.get("failure_code");
    let failure_detail: String = row.get("failure_detail");
    let lease_expires_at: Option<chrono::DateTime<chrono::Utc>> = row.get("lease_expires_at");
    let created_at: chrono::DateTime<chrono::Utc> = row.get("created_at");
    let updated_at: chrono::DateTime<chrono::Utc> = row.get("updated_at");
    let mut conditions = Vec::new();
    if !failure_code.is_empty() {
        conditions.push(operation_condition(failure_code, failure_detail));
    }
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
                })
            })
            .collect::<Result<Vec<_>, Status>>()?,
        committed: row.get("committed"),
        lease_epoch: row.get::<_, i64>("lease_epoch") as u64,
        lease_expires_at: lease_expires_at.map(timestamp),
        created_at: Some(timestamp(created_at)),
        updated_at: Some(timestamp(updated_at)),
        conditions,
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
        let existing_payload: Vec<u8> = existing.get("request_payload");
        if existing_payload != payload {
            return Err(Status::already_exists(
                "request_token was already used by this actor with different operation content",
            ));
        }
        let id: Uuid = existing.get("operation_id");
        let operation = load_operation(&transaction, id).await?;
        transaction.commit().await.map_err(internal)?;
        return Ok(operation);
    }

    let action =
        NodeOperationAction::try_from(request.action).unwrap_or(NodeOperationAction::Unspecified);
    if action == NodeOperationAction::Unspecified {
        return Err(Status::invalid_argument("operation action is required"));
    }
    let operation_id = Uuid::new_v4();
    let policy = request.policy.clone().unwrap_or(RolloutPolicy {
        max_unavailable: 1,
        canary_slot: String::new(),
        pause_after_canary: false,
        allow_downtime: false,
        deadline_seconds: 300,
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
    if deployment_action {
        let deployment = transaction
            .query_opt(
                "SELECT bundle_digest, expected_inventory FROM wr_node_deployments
                 WHERE node_id = $1 AND revision = $2 AND state IN ('pending', 'active')",
                &[&request.node_id, &target_revision],
            )
            .await
            .map_err(internal)?
            .ok_or_else(|| {
                Status::failed_precondition("target revision is not a staged deployment")
            })?;
        if deployment.get::<_, String>("bundle_digest") != request.bundle_digest {
            return Err(Status::failed_precondition(
                "operation digest does not match the staged deployment",
            ));
        }
        let snapshot = wr_common::wruntime::DeploymentRecord::decode(
            deployment
                .get::<_, Vec<u8>>("expected_inventory")
                .as_slice(),
        )
        .map_err(|error| Status::internal(format!("staged inventory is invalid: {error}")))?;
        let mut expected_slots = snapshot
            .expected_engines
            .into_iter()
            .map(|engine| engine.engine_slot)
            .collect::<Vec<_>>();
        let mut requested_slots = request.engine_slots.clone();
        expected_slots.sort();
        requested_slots.sort();
        if expected_slots != requested_slots {
            return Err(Status::failed_precondition(
                "operation slots do not match the staged deployment inventory",
            ));
        }
    }
    if deployment_action && target_revision > 0 {
        transaction
            .execute(
                "UPDATE wr_nodes SET target_revision = $2, updated_at = NOW()
                 WHERE node_id = $1",
                &[&request.node_id, &target_revision],
            )
            .await
            .map_err(internal)?;
    }
    transaction
        .execute(
            "INSERT INTO wr_node_operations
               (operation_id, node_id, request_token, actor, action, state, request_payload,
                policy, source_revision, target_revision, bundle_digest)
             VALUES ($1, $2, $3, $4, $5, 'queued', $6, $7, $8, $9, $10)",
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
    let mut slots = request.engine_slots.clone();
    slots.sort();
    slots.dedup();
    if !policy.canary_slot.is_empty() {
        if let Some(index) = slots.iter().position(|slot| slot == &policy.canary_slot) {
            let canary = slots.remove(index);
            slots.insert(0, canary);
        }
    }
    let initial_step = step_name(first_step(action));
    for (rollout_order, slot) in slots.into_iter().enumerate() {
        let rollout_order = rollout_order as i32;
        transaction
            .execute(
                "INSERT INTO wr_node_operation_slots
                   (operation_id, node_id, engine_slot, rollout_order,
                    authoritative_revision, next_step)
                 VALUES ($1, $2, $3, $4, $5, $6)",
                &[
                    &operation_id,
                    &request.node_id,
                    &slot,
                    &rollout_order,
                    &source_revision,
                    &initial_step,
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

pub async fn list(
    pool: &Pool,
    node_id: &str,
    include_terminal: bool,
) -> Result<Vec<NodeOperation>, Status> {
    let client = pool.get().await.map_err(internal)?;
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
        result.push(load_operation(&client, row.get("operation_id")).await?);
    }
    Ok(result)
}

pub async fn resume(pool: &Pool, operation_id: &str, actor: &str) -> Result<NodeOperation, Status> {
    transition_admin(
        pool,
        operation_id,
        actor,
        "paused",
        "queued",
        "OPERATION_RESUMED",
    )
    .await
}

pub async fn cancel(pool: &Pool, operation_id: &str, actor: &str) -> Result<NodeOperation, Status> {
    let id = Uuid::parse_str(operation_id)
        .map_err(|_| Status::invalid_argument("operation_id must be a UUID"))?;
    let mut client = pool.get().await.map_err(internal)?;
    let transaction = client.transaction().await.map_err(internal)?;
    let row = transaction
        .query_opt(
            "UPDATE wr_node_operations
             SET state = 'cancelled', updated_at = NOW(), lease_expires_at = NULL, claimed_by = NULL
             WHERE operation_id = $1 AND state IN ('queued', 'paused') AND NOT committed
             RETURNING node_id, source_revision, target_revision, lease_epoch",
            &[&id],
        )
        .await
        .map_err(internal)?
        .ok_or_else(|| {
            Status::failed_precondition(
                "only an uncommitted queued or paused operation can be cancelled",
            )
        })?;
    let node_id: String = row.get("node_id");
    let source_revision: i64 = row.get("source_revision");
    let target_revision: i64 = row.get("target_revision");
    transaction
        .execute(
            "UPDATE wr_node_slot_authority a SET authoritative = FALSE, updated_at = NOW()
             WHERE a.node_id = $1 AND a.engine_slot IN (
               SELECT engine_slot FROM wr_node_operation_slots WHERE operation_id = $2
             )",
            &[&node_id, &id],
        )
        .await
        .map_err(internal)?;
    if source_revision > 0 {
        transaction
            .execute(
                "INSERT INTO wr_node_slot_authority
                   (node_id, engine_slot, revision, authoritative)
                 SELECT $1, engine_slot, $3, TRUE FROM wr_node_operation_slots
                 WHERE operation_id = $2
                 ON CONFLICT (node_id, engine_slot, revision) DO UPDATE SET
                   authoritative = TRUE, updated_at = NOW()",
                &[&node_id, &id, &source_revision],
            )
            .await
            .map_err(internal)?;
    }
    transaction
        .execute(
            "UPDATE wr_nodes SET target_revision = NULL, updated_at = NOW()
             WHERE node_id = $1 AND target_revision = NULLIF($2, 0)",
            &[&node_id, &target_revision],
        )
        .await
        .map_err(internal)?;
    append_event(
        &transaction,
        id,
        actor,
        "OPERATION_CANCELLED",
        "cancelled before commit",
        row.get("lease_epoch"),
    )
    .await?;
    let operation = load_operation(&transaction, id).await?;
    transaction.commit().await.map_err(internal)?;
    Ok(operation)
}

async fn transition_admin(
    pool: &Pool,
    operation_id: &str,
    actor: &str,
    from: &str,
    to: &str,
    event: &str,
) -> Result<NodeOperation, Status> {
    let id = Uuid::parse_str(operation_id)
        .map_err(|_| Status::invalid_argument("operation_id must be a UUID"))?;
    let mut client = pool.get().await.map_err(internal)?;
    let transaction = client.transaction().await.map_err(internal)?;
    let row = transaction
        .query_opt(
            "UPDATE wr_node_operations SET state = $3, updated_at = NOW(),
                    lease_expires_at = NULL, claimed_by = NULL
             WHERE operation_id = $1 AND state = $2 RETURNING lease_epoch",
            &[&id, &from, &to],
        )
        .await
        .map_err(internal)?
        .ok_or_else(|| Status::failed_precondition(format!("operation must be {from}")))?;
    append_event(&transaction, id, actor, event, "", row.get("lease_epoch")).await?;
    let operation = load_operation(&transaction, id).await?;
    transaction.commit().await.map_err(internal)?;
    Ok(operation)
}

pub async fn claim(
    pool: &Pool,
    node_id: &str,
    agent: &str,
) -> Result<Option<ClaimOperationResponse>, Status> {
    let mut client = pool.get().await.map_err(internal)?;
    let transaction = client.transaction().await.map_err(internal)?;
    let expired = transaction
        .query(
            "UPDATE wr_node_operations SET state = 'paused', failure_code = 'LEASE_EXPIRED',
                    failure_detail = 'node-agent lease expired', claimed_by = NULL,
                    lease_expires_at = NULL, updated_at = NOW()
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
            "node-agent lease expired",
            row.get("lease_epoch"),
        )
        .await?;
    }
    let row = transaction
        .query_opt(
            "SELECT operation_id, state, lease_epoch
             FROM wr_node_operations
             WHERE node_id = $1 AND (
                 state = 'queued' OR
                 (state = 'running' AND claimed_by = $2 AND lease_expires_at > NOW())
             )
             ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1",
            &[&node_id, &agent],
        )
        .await
        .map_err(internal)?;
    let Some(row) = row else {
        transaction.commit().await.map_err(internal)?;
        return Ok(None);
    };
    let id: Uuid = row.get("operation_id");
    let state: String = row.get("state");
    let epoch: i64 = if state == "queued" {
        transaction
            .query_one(
                "UPDATE wr_node_operations
                 SET state = 'running', lease_epoch = lease_epoch + 1,
                     lease_expires_at = NOW() + make_interval(secs => $3),
                     claimed_by = $2, updated_at = NOW()
                 WHERE operation_id = $1 RETURNING lease_epoch",
                &[&id, &agent, &LEASE_SECONDS],
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
    let operation = load_operation(&transaction, id).await?;
    let slot = operation
        .slots
        .iter()
        .find(|slot| !slot.complete)
        .ok_or_else(|| Status::internal("active operation has no incomplete slot"))?;
    let step = NodeOperationStepKind::try_from(slot.next_step)
        .unwrap_or(NodeOperationStepKind::Unspecified);
    append_event(
        &transaction,
        id,
        agent,
        "LEASE_CLAIMED",
        &format!("{}:{}", slot.engine_slot, step_name(step)),
        epoch,
    )
    .await?;
    transaction.commit().await.map_err(internal)?;
    Ok(Some(ClaimOperationResponse {
        instruction: Some(AgentInstruction {
            operation_id: id.to_string(),
            node_id: node_id.to_string(),
            engine_slot: slot.engine_slot.clone(),
            lease_epoch: epoch as u64,
            step: step as i32,
            revision: if operation.target_revision == 0 {
                operation.source_revision
            } else {
                operation.target_revision
            },
            bundle_digest: operation.bundle_digest,
            operation_deadline: operation.created_at.and_then(|created| {
                operation
                    .policy
                    .as_ref()
                    .map(|policy| prost_types::Timestamp {
                        seconds: created.seconds + policy.deadline_seconds as i64,
                        nanos: created.nanos,
                    })
            }),
        }),
        lease_seconds: LEASE_SECONDS as u64,
    }))
}

pub async fn renew(
    pool: &Pool,
    node_id: &str,
    operation_id: &str,
    epoch: u64,
    agent: &str,
) -> Result<prost_types::Timestamp, Status> {
    let id = Uuid::parse_str(operation_id)
        .map_err(|_| Status::invalid_argument("operation_id must be a UUID"))?;
    let epoch =
        i64::try_from(epoch).map_err(|_| Status::invalid_argument("lease epoch is too large"))?;
    let client = pool.get().await.map_err(internal)?;
    let row = client
        .query_opt(
            "UPDATE wr_node_operations
             SET lease_expires_at = NOW() + make_interval(secs => $5), updated_at = NOW()
             WHERE operation_id = $1 AND node_id = $2 AND state = 'running'
               AND lease_epoch = $3 AND claimed_by = $4 AND lease_expires_at > NOW()
             RETURNING lease_expires_at",
            &[&id, &node_id, &epoch, &agent, &LEASE_SECONDS],
        )
        .await
        .map_err(internal)?
        .ok_or_else(|| Status::aborted("operation lease is stale or expired"))?;
    Ok(timestamp(row.get("lease_expires_at")))
}

pub async fn observations(
    pool: &Pool,
    node_id: &str,
    engine_slot: &str,
) -> Result<Vec<SlotObservation>, Status> {
    let client = pool.get().await.map_err(internal)?;
    client
        .query(
            "SELECT node_id, engine_slot, lifecycle_status, backend_state,
                    backend_instance_id, observed_revision, observed_at
             FROM wr_node_slot_observations
             WHERE ($1 = '' OR node_id = $1) AND ($2 = '' OR engine_slot = $2)
             ORDER BY node_id, engine_slot",
            &[&node_id, &engine_slot],
        )
        .await
        .map_err(internal)?
        .into_iter()
        .map(|row| {
            let lifecycle = row
                .get::<_, Option<Vec<u8>>>("lifecycle_status")
                .map(|bytes| wr_common::wruntime::LifecycleStatus::decode(bytes.as_slice()))
                .transpose()
                .map_err(|error| {
                    Status::internal(format!("stored lifecycle observation is invalid: {error}"))
                })?;
            let observed_at: chrono::DateTime<chrono::Utc> = row.get("observed_at");
            let stale = chrono::Utc::now()
                .signed_duration_since(observed_at)
                .num_seconds()
                > 15;
            let backend_state = if stale {
                BackendProcessState::Unspecified
            } else {
                match row.get::<_, String>("backend_state").as_str() {
                    "running" => BackendProcessState::Running,
                    "exited" => BackendProcessState::Exited,
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
            })
        })
        .collect()
}

pub async fn report_observation(
    pool: &Pool,
    request: &ReportNodeObservationRequest,
) -> Result<(), Status> {
    let observed_at = request
        .observed_at
        .as_ref()
        .and_then(|value| chrono::DateTime::from_timestamp(value.seconds, value.nanos as u32))
        .unwrap_or_else(chrono::Utc::now);
    let lifecycle = request.lifecycle.as_ref().map(Message::encode_to_vec);
    let backend_state = match BackendProcessState::try_from(request.backend_state)
        .unwrap_or(BackendProcessState::Unspecified)
    {
        BackendProcessState::Running => "running",
        BackendProcessState::Exited => "exited",
        BackendProcessState::Unspecified => "unknown",
    };
    let observed_revision = i64::try_from(request.observed_revision)
        .map_err(|_| Status::invalid_argument("observed_revision is too large"))?;
    let client = pool.get().await.map_err(internal)?;
    client
        .execute(
            "INSERT INTO wr_node_slot_observations
               (node_id, engine_slot, lifecycle_status, backend_state, backend_instance_id,
                observed_revision, observed_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (node_id, engine_slot) DO UPDATE SET
               lifecycle_status = EXCLUDED.lifecycle_status,
               backend_state = EXCLUDED.backend_state,
               backend_instance_id = EXCLUDED.backend_instance_id,
               observed_revision = EXCLUDED.observed_revision,
               observed_at = EXCLUDED.observed_at
             WHERE wr_node_slot_observations.observed_at <= EXCLUDED.observed_at",
            &[
                &request.node_id,
                &request.engine_slot,
                &lifecycle,
                &backend_state,
                &request.backend_instance_id,
                &observed_revision,
                &observed_at,
            ],
        )
        .await
        .map_err(internal)?;
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
    let mut client = pool.get().await.map_err(internal)?;
    let transaction = client.transaction().await.map_err(internal)?;
    let operation_row = transaction
        .query_opt(
            "SELECT action, target_revision, source_revision, policy FROM wr_node_operations
             WHERE operation_id = $1 AND node_id = $2 AND state = 'running'
               AND lease_epoch = $3 AND claimed_by = $4 AND lease_expires_at > NOW()
             FOR UPDATE",
            &[&id, &request.node_id, &epoch, &agent],
        )
        .await
        .map_err(internal)?
        .ok_or_else(|| Status::aborted("operation result has a stale or expired lease"))?;
    let action = parse_action(operation_row.get::<_, String>("action").as_str())?;
    let slot = transaction
        .query_opt(
            "SELECT next_step, completed_steps, rollout_order FROM wr_node_operation_slots
             WHERE operation_id = $1 AND node_id = $2 AND engine_slot = $3 AND NOT complete
             FOR UPDATE",
            &[&id, &request.node_id, &request.engine_slot],
        )
        .await
        .map_err(internal)?
        .ok_or_else(|| {
            Status::failed_precondition("operation slot is already complete or unknown")
        })?;
    let expected_step = parse_step(slot.get::<_, String>("next_step").as_str())?;
    if expected_step != reported_step {
        return Err(Status::aborted(format!(
            "stale step result: expected {}, received {}",
            step_name(expected_step),
            step_name(reported_step)
        )));
    }
    if !request.succeeded {
        let code = if request.condition_code.is_empty() {
            "STEP_FAILED"
        } else {
            request.condition_code.as_str()
        };
        transaction
            .execute(
                "UPDATE wr_node_operations SET state = 'paused', failure_code = $2,
                        failure_detail = $3, lease_expires_at = NULL, claimed_by = NULL,
                        updated_at = NOW() WHERE operation_id = $1",
                &[&id, &code, &request.detail],
            )
            .await
            .map_err(internal)?;
        transaction
            .execute(
                "UPDATE wr_node_operation_slots SET condition_code = $4, condition_detail = $5,
                        updated_at = NOW()
                 WHERE operation_id = $1 AND node_id = $2 AND engine_slot = $3",
                &[
                    &id,
                    &request.node_id,
                    &request.engine_slot,
                    &code,
                    &request.detail,
                ],
            )
            .await
            .map_err(internal)?;
        append_event(&transaction, id, agent, code, &request.detail, epoch).await?;
    } else {
        let completed: i32 = slot.get::<_, i32>("completed_steps") + 1;
        let next = next_step(action, completed);
        let complete = next.is_none();
        let next_name = next.map(step_name).unwrap_or("complete");
        let authoritative_revision: i64 = if complete
            && matches!(
                action,
                NodeOperationAction::InitialApply
                    | NodeOperationAction::RollingUpgrade
                    | NodeOperationAction::Scale
                    | NodeOperationAction::Rollback
            ) {
            operation_row.get("target_revision")
        } else {
            operation_row.get("source_revision")
        };
        if reported_step == NodeOperationStepKind::SelectRelease {
            let target_revision: i64 = operation_row.get("target_revision");
            transaction
                .execute(
                    "UPDATE wr_node_slot_authority SET authoritative = FALSE, updated_at = NOW()
                     WHERE node_id = $1 AND engine_slot = $2 AND authoritative",
                    &[&request.node_id, &request.engine_slot],
                )
                .await
                .map_err(internal)?;
            transaction
                .execute(
                    "INSERT INTO wr_node_slot_authority
                       (node_id, engine_slot, revision, authoritative)
                     VALUES ($1, $2, $3, TRUE)
                     ON CONFLICT (node_id, engine_slot, revision) DO UPDATE SET
                       authoritative = TRUE, updated_at = NOW()",
                    &[&request.node_id, &request.engine_slot, &target_revision],
                )
                .await
                .map_err(internal)?;
        }
        transaction
            .execute(
                "UPDATE wr_node_operation_slots
                 SET completed_steps = $4, next_step = $5, complete = $6,
                     authoritative_revision = $7, condition_code = '', condition_detail = '',
                     updated_at = NOW()
                 WHERE operation_id = $1 AND node_id = $2 AND engine_slot = $3",
                &[
                    &id,
                    &request.node_id,
                    &request.engine_slot,
                    &completed,
                    &next_name,
                    &complete,
                    &authoritative_revision,
                ],
            )
            .await
            .map_err(internal)?;
        append_event(
            &transaction,
            id,
            agent,
            "STEP_SUCCEEDED",
            &format!("{}:{}", request.engine_slot, step_name(reported_step)),
            epoch,
        )
        .await?;
        let remaining: i64 = transaction
            .query_one(
                "SELECT COUNT(*) FROM wr_node_operation_slots WHERE operation_id = $1 AND NOT complete",
                &[&id],
            )
            .await
            .map_err(internal)?
            .get(0);
        if remaining > 0 && complete && slot.get::<_, i32>("rollout_order") == 0 {
            let policy =
                RolloutPolicy::decode(operation_row.get::<_, Vec<u8>>("policy").as_slice())
                    .map_err(|error| {
                        Status::internal(format!("stored rollout policy is invalid: {error}"))
                    })?;
            if policy.pause_after_canary {
                transaction
                    .execute(
                        "UPDATE wr_node_operations SET state = 'paused', lease_expires_at = NULL,
                                claimed_by = NULL, updated_at = NOW() WHERE operation_id = $1",
                        &[&id],
                    )
                    .await
                    .map_err(internal)?;
                append_event(&transaction, id, agent, "CANARY_PAUSED", "", epoch).await?;
            }
        }
        if remaining == 0 {
            let committed = matches!(
                action,
                NodeOperationAction::InitialApply
                    | NodeOperationAction::RollingUpgrade
                    | NodeOperationAction::Scale
                    | NodeOperationAction::Rollback
            );
            transaction
                .execute(
                    "UPDATE wr_node_operations SET state = 'succeeded', committed = $2,
                            lease_expires_at = NULL, claimed_by = NULL, updated_at = NOW()
                     WHERE operation_id = $1",
                    &[&id, &committed],
                )
                .await
                .map_err(internal)?;
            if committed {
                let target_revision: i64 = operation_row.get("target_revision");
                transaction
                    .execute(
                        "UPDATE wr_nodes SET current_revision = $2, target_revision = NULL,
                                updated_at = NOW() WHERE node_id = $1 AND target_revision = $2",
                        &[&request.node_id, &target_revision],
                    )
                    .await
                    .map_err(internal)?;
                transaction
                    .execute(
                        "UPDATE wr_node_deployments SET state = 'succeeded', completed_at = NOW()
                         WHERE node_id = $1 AND revision = $2 AND state IN ('pending', 'active')",
                        &[&request.node_id, &target_revision],
                    )
                    .await
                    .map_err(internal)?;
            }
            append_event(&transaction, id, agent, "OPERATION_SUCCEEDED", "", epoch).await?;
        }
    }
    let operation = load_operation(&transaction, id).await?;
    transaction.commit().await.map_err(internal)?;
    Ok(operation)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drain_and_restart_reuse_stop_composition() {
        assert_eq!(
            next_step(NodeOperationAction::Drain, 0),
            Some(NodeOperationStepKind::StopSlot)
        );
        assert_eq!(next_step(NodeOperationAction::Drain, 1), None);
        assert_eq!(
            (0..3)
                .filter_map(|index| next_step(NodeOperationAction::Restart, index))
                .collect::<Vec<_>>(),
            vec![
                NodeOperationStepKind::StopSlot,
                NodeOperationStepKind::StartSlot,
                NodeOperationStepKind::VerifyReady,
            ]
        );
    }

    #[test]
    fn rolling_actions_have_one_typed_release_sequence() {
        for action in [
            NodeOperationAction::InitialApply,
            NodeOperationAction::RollingUpgrade,
            NodeOperationAction::Scale,
            NodeOperationAction::Rollback,
        ] {
            assert_eq!(
                (0..5)
                    .filter_map(|index| next_step(action, index))
                    .collect::<Vec<_>>(),
                vec![
                    NodeOperationStepKind::VerifyRelease,
                    NodeOperationStepKind::StopSlot,
                    NodeOperationStepKind::SelectRelease,
                    NodeOperationStepKind::StartSlot,
                    NodeOperationStepKind::VerifyReady,
                ]
            );
        }
    }
}
