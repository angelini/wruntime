use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use serde::Serialize;
use wr_common::wruntime::{
    operation_target_progress, BackendKind, BackendStopDisposition, BackendTerminationEvidence,
    CancelOperationRequest, GetOperationRequest, InstructionTargetKind, ListOperationsRequest,
    NodeOperation, NodeOperationState, NodeSlotTransitionKind, OperationTargetProgress,
    ResumeOperationRequest,
};

use crate::client;

#[derive(Args)]
pub struct OperationsArgs {
    #[command(subcommand)]
    pub command: OperationsCommand,
}

#[derive(Subcommand)]
pub enum OperationsCommand {
    /// Show one operation and its append-only event history.
    Get {
        operation_id: String,
        #[arg(long)]
        json: bool,
    },
    /// List active operations, or include durable history.
    List {
        #[arg(long)]
        node_id: Option<String>,
        #[arg(long)]
        include_terminal: bool,
        #[arg(long)]
        json: bool,
    },
    /// Resume a paused operation with a fresh agent lease on next claim.
    Resume {
        operation_id: String,
        #[arg(long)]
        json: bool,
    },
    /// Cancel an uncommitted queued or paused operation.
    Cancel {
        operation_id: String,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Serialize)]
struct OperationSlotDto<'a> {
    engine_slot: &'a str,
    transition: &'static str,
    next_step: &'static str,
    complete: bool,
    conditions: Vec<&'a str>,
}

#[derive(Serialize)]
struct OperationDto<'a> {
    operation_id: &'a str,
    node_id: &'a str,
    request_token: &'a str,
    actor: &'a str,
    action: &'static str,
    state: &'static str,
    committed: bool,
    source_revision: u64,
    target_revision: u64,
    bundle_digest: &'a str,
    resolved_release_digest: &'a str,
    phase: &'static str,
    lease_epoch: u64,
    affected_slots: Vec<&'a str>,
    slots: Vec<OperationSlotDto<'a>>,
    conditions: Vec<&'a str>,
}

#[derive(Serialize)]
struct TerminationEvidenceDto<'a> {
    backend: &'static str,
    backend_instance_id: &'a str,
    process_instance_id: &'a str,
    graceful_termination_requested: bool,
    kill_escalated: bool,
    disposition: &'static str,
    terminal_result: &'a str,
    exit_code: Option<i32>,
    signal: Option<i32>,
}

#[derive(Serialize)]
struct OperationSlotDetailDto<'a> {
    engine_slot: &'a str,
    changed: bool,
    effect_reported: bool,
    pinned_backend_instance_id: &'a str,
    pinned_process_instance_id: &'a str,
    effect_backend_instance_id: &'a str,
    effect_process_instance_id: &'a str,
    termination_evidence: Option<TerminationEvidenceDto<'a>>,
}

#[derive(Serialize)]
struct ProxyOperationDetailDto<'a> {
    changed: bool,
    effect_reported: bool,
    pinned_backend_instance_id: &'a str,
    pinned_process_instance_id: &'a str,
    effect_backend_instance_id: &'a str,
    effect_process_instance_id: &'a str,
    termination_evidence: Option<TerminationEvidenceDto<'a>>,
}

fn action_name(value: i32) -> &'static str {
    use wr_common::wruntime::NodeOperationAction as Action;
    match Action::try_from(value).unwrap_or(Action::Unspecified) {
        Action::Deployment => "deployment",
        Action::Restart => "restart",
        Action::Rollback => "rollback",
        Action::Unspecified => "unspecified",
    }
}

fn transition_name(value: i32) -> &'static str {
    use NodeSlotTransitionKind as Transition;
    match Transition::try_from(value).unwrap_or(Transition::Unspecified) {
        Transition::Addition => "addition",
        Transition::Replacement => "replacement",
        Transition::Unchanged => "unchanged",
        Transition::Removal => "removal",
        Transition::Restart => "restart",
        Transition::Unspecified => "unspecified",
    }
}

fn next_step_name(value: i32) -> &'static str {
    match value {
        1 => "verify-release-metadata",
        2 => "verify-proxy",
        3 => "stop-backend",
        4 => "select-release",
        5 => "start-backend",
        6 => "verify-target",
        7 => "switch-authority",
        8 => "verify-serving",
        9 => "restore-source",
        10 => "cleanup-release",
        11 => "inspect-backend",
        _ => "complete",
    }
}

fn state_name(value: i32) -> &'static str {
    match NodeOperationState::try_from(value).unwrap_or(NodeOperationState::Unspecified) {
        NodeOperationState::Queued => "queued",
        NodeOperationState::Running => "running",
        NodeOperationState::Paused => "paused",
        NodeOperationState::Succeeded => "succeeded",
        NodeOperationState::Failed => "failed",
        NodeOperationState::Cancelled => "cancelled",
        NodeOperationState::Unspecified => "unspecified",
    }
}

fn phase_name(value: i32) -> &'static str {
    use wr_common::wruntime::NodeOperationPhase as Phase;
    match Phase::try_from(value).unwrap_or(Phase::Unspecified) {
        Phase::Forward => "forward",
        Phase::RestoringSource => "restoring-source",
        Phase::Committing => "committing",
        Phase::Complete => "complete",
        Phase::Unspecified => "unspecified",
    }
}

fn backend_name(value: i32) -> &'static str {
    match BackendKind::try_from(value).unwrap_or(BackendKind::Unspecified) {
        BackendKind::Systemd => "systemd",
        BackendKind::Docker => "docker",
        BackendKind::Unspecified => "unknown",
    }
}

fn disposition_name(value: i32) -> &'static str {
    match BackendStopDisposition::try_from(value).unwrap_or(BackendStopDisposition::Unknown) {
        BackendStopDisposition::Graceful => "graceful",
        BackendStopDisposition::Forced => "forced",
        BackendStopDisposition::Unknown => "unknown",
    }
}

fn termination_dto(evidence: &BackendTerminationEvidence) -> TerminationEvidenceDto<'_> {
    TerminationEvidenceDto {
        backend: backend_name(evidence.backend),
        backend_instance_id: &evidence.backend_instance_id,
        process_instance_id: &evidence.process_instance_id,
        graceful_termination_requested: evidence.graceful_termination_requested,
        kill_escalated: evidence.kill_escalated,
        disposition: disposition_name(evidence.disposition),
        terminal_result: &evidence.terminal_result,
        exit_code: evidence.exit_code,
        signal: evidence.signal,
    }
}

fn engine_slot(target: &OperationTargetProgress) -> Result<&str> {
    match (
        target.kind,
        target.identity.as_ref(),
        target.details.as_ref(),
    ) {
        (
            kind,
            Some(operation_target_progress::Identity::EngineSlot(identity)),
            Some(operation_target_progress::Details::EngineDetails(_)),
        ) if kind == InstructionTargetKind::EngineSlot as i32
            && !identity.engine_slot.is_empty() =>
        {
            Ok(identity.engine_slot.as_str())
        }
        _ => bail!("operation engine target kind, identity, and details mismatch"),
    }
}

fn operation_slots(operation: &NodeOperation) -> Result<Vec<OperationSlotDto<'_>>> {
    let mut slots = operation
        .targets
        .iter()
        .filter_map(|target| {
            let kind = InstructionTargetKind::try_from(target.kind)
                .unwrap_or(InstructionTargetKind::Unspecified);
            if kind == InstructionTargetKind::Proxy {
                return None;
            }
            Some(
                match (kind, target.identity.as_ref(), target.details.as_ref()) {
                    (
                        InstructionTargetKind::EngineSlot,
                        Some(operation_target_progress::Identity::EngineSlot(identity)),
                        Some(operation_target_progress::Details::EngineDetails(_)),
                    ) if !identity.engine_slot.is_empty() => Ok(OperationSlotDto {
                        engine_slot: identity.engine_slot.as_str(),
                        transition: transition_name(target.transition),
                        next_step: next_step_name(target.next_step),
                        complete: target.complete,
                        conditions: target
                            .conditions
                            .iter()
                            .map(|condition| condition.code.as_str())
                            .collect(),
                    }),
                    _ => Err(anyhow::anyhow!(
                        "operation engine target kind, identity, and details mismatch"
                    )),
                },
            )
        })
        .collect::<Result<Vec<_>>>()?;
    slots.sort_by_key(|slot| slot.engine_slot);
    Ok(slots)
}

fn slot_detail_dto(target: &OperationTargetProgress) -> Result<OperationSlotDetailDto<'_>> {
    Ok(OperationSlotDetailDto {
        engine_slot: engine_slot(target)?,
        changed: target.changed,
        effect_reported: target.effect_reported,
        pinned_backend_instance_id: &target.pinned_backend_instance_id,
        pinned_process_instance_id: &target.pinned_process_instance_id,
        effect_backend_instance_id: &target.effect_backend_instance_id,
        effect_process_instance_id: &target.effect_process_instance_id,
        termination_evidence: target.termination_evidence.as_ref().map(termination_dto),
    })
}

fn proxy_detail_dto(operation: &NodeOperation) -> Result<ProxyOperationDetailDto<'_>> {
    let target = operation
        .targets
        .iter()
        .find(|target| target.kind == InstructionTargetKind::Proxy as i32)
        .context("operation proxy target is missing")?;
    Ok(ProxyOperationDetailDto {
        changed: target.changed,
        effect_reported: target.effect_reported,
        pinned_backend_instance_id: &target.pinned_backend_instance_id,
        pinned_process_instance_id: &target.pinned_process_instance_id,
        effect_backend_instance_id: &target.effect_backend_instance_id,
        effect_process_instance_id: &target.effect_process_instance_id,
        termination_evidence: target.termination_evidence.as_ref().map(termination_dto),
    })
}

fn slot_line(slot: &OperationSlotDto<'_>) -> String {
    format!(
        "Slot {} transition={} next_step={} complete={}",
        slot.engine_slot, slot.transition, slot.next_step, slot.complete
    )
}

fn dto(operation: &NodeOperation) -> Result<OperationDto<'_>> {
    let slots = operation_slots(operation)?;
    Ok(OperationDto {
        operation_id: &operation.operation_id,
        node_id: &operation.node_id,
        request_token: &operation.request_token,
        actor: &operation.actor,
        action: action_name(operation.action),
        state: state_name(operation.state),
        committed: operation.committed,
        source_revision: operation.source_revision,
        target_revision: operation.target_revision,
        bundle_digest: &operation.bundle_digest,
        resolved_release_digest: &operation.resolved_release_digest,
        phase: phase_name(operation.phase),
        lease_epoch: operation.lease_epoch,
        affected_slots: slots.iter().map(|slot| slot.engine_slot).collect(),
        slots,
        conditions: operation
            .conditions
            .iter()
            .map(|condition| condition.code.as_str())
            .chain(operation.targets.iter().flat_map(|target| {
                target
                    .conditions
                    .iter()
                    .map(|condition| condition.code.as_str())
            }))
            .collect(),
    })
}

fn detail_dto(operation: &NodeOperation) -> Result<serde_json::Value> {
    let mut value = serde_json::to_value(dto(operation)?)?;
    let object = value
        .as_object_mut()
        .context("operation JSON projection must be an object")?;
    object.insert("schema_version".into(), 1_u32.into());
    object.insert(
        "slots".into(),
        serde_json::to_value(
            operation
                .targets
                .iter()
                .filter(|target| target.kind == InstructionTargetKind::EngineSlot as i32)
                .map(slot_detail_dto)
                .collect::<Result<Vec<_>>>()?,
        )?,
    );
    object.insert(
        "proxy".into(),
        serde_json::to_value(proxy_detail_dto(operation)?)?,
    );
    Ok(value)
}

pub(crate) fn render_operation(operation: &NodeOperation, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(&dto(operation)?)?);
    } else {
        println!(
            "Operation {}  {}  {}  phase={}  node={}  committed={}",
            operation.operation_id,
            action_name(operation.action),
            state_name(operation.state),
            phase_name(operation.phase),
            operation.node_id,
            operation.committed
        );
        println!("Request token: {}", operation.request_token);
        for slot in operation_slots(operation)? {
            println!("{}", slot_line(&slot));
            if let Some(target) = operation.targets.iter().find(|target| {
                matches!(
                    target.identity.as_ref(),
                    Some(operation_target_progress::Identity::EngineSlot(identity))
                        if identity.engine_slot == slot.engine_slot
                )
            }) {
                for condition in &target.conditions {
                    println!("  Condition {}: {}", condition.code, condition.detail);
                }
            }
        }
        for condition in &operation.conditions {
            println!("Condition {}: {}", condition.code, condition.detail);
        }
    }
    Ok(())
}

pub(crate) async fn wait_for_terminal(
    manager: &str,
    operation: NodeOperation,
    wait_timeout: Duration,
    json: bool,
) -> Result<()> {
    let operation_id = operation.operation_id.clone();
    let deadline = tokio::time::Instant::now() + wait_timeout;
    let mut latest = operation;
    loop {
        match NodeOperationState::try_from(latest.state).unwrap_or(NodeOperationState::Unspecified)
        {
            NodeOperationState::Succeeded => return render_operation(&latest, json),
            NodeOperationState::Failed
            | NodeOperationState::Cancelled
            | NodeOperationState::Paused => {
                render_operation(&latest, json)?;
                bail!(
                    "operation {} ended in state {}",
                    latest.operation_id,
                    state_name(latest.state)
                );
            }
            _ => {}
        }
        if tokio::time::Instant::now() >= deadline {
            render_operation(&latest, json)?;
            bail!(
                "wait timeout expired; durable operation {} remains {}",
                latest.operation_id,
                state_name(latest.state)
            );
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        latest = client::connect_operator(manager, wr_common::manager_client::RetryClass::ReadOnly)
            .await?
            .get_operation(GetOperationRequest {
                operation_id: operation_id.clone(),
            })
            .await?
            .into_inner()
            .operation
            .context("GetOperation omitted operation")?;
    }
}

pub async fn run(args: OperationsArgs, manager: &str) -> Result<()> {
    let mut operator = client::connect_operator(
        manager,
        wr_common::manager_client::RetryClass::NoReplayMutation,
    )
    .await?;
    match args.command {
        OperationsCommand::Get { operation_id, json } => {
            let response = operator
                .get_operation(GetOperationRequest { operation_id })
                .await?
                .into_inner();
            let operation = response
                .operation
                .context("GetOperation omitted operation")?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&detail_dto(&operation)?)?
                );
            } else {
                render_operation(&operation, false)?;
            }
            if !json {
                for event in response.events {
                    println!(
                        "#{:04} {} {}",
                        event.sequence, event.event_code, event.detail
                    );
                }
            }
            Ok(())
        }
        OperationsCommand::List {
            node_id,
            include_terminal,
            json,
        } => {
            let operations = operator
                .list_operations(ListOperationsRequest {
                    node_id: node_id.unwrap_or_default(),
                    include_terminal,
                })
                .await?
                .into_inner()
                .operations;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(
                        &operations.iter().map(dto).collect::<Result<Vec<_>>>()?
                    )?
                );
            } else if operations.is_empty() {
                println!("No operations.");
            } else {
                for operation in operations {
                    render_operation(&operation, false)?;
                }
            }
            Ok(())
        }
        OperationsCommand::Resume { operation_id, json } => {
            let operation = operator
                .resume_operation(ResumeOperationRequest { operation_id })
                .await?
                .into_inner()
                .operation
                .context("ResumeOperation omitted operation")?;
            render_operation(&operation, json)
        }
        OperationsCommand::Cancel { operation_id, json } => {
            let operation = operator
                .cancel_operation(CancelOperationRequest { operation_id })
                .await?
                .into_inner()
                .operation
                .context("CancelOperation omitted operation")?;
            render_operation(&operation, json)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wr_common::wruntime::{
        operation_target_progress, DeploymentCondition, EngineSlotTargetIdentity,
        EngineTargetDetails, NodeOperationAction, OperationTargetProgress, ProxyTargetDetails,
        ProxyTargetIdentity,
    };

    #[test]
    fn stable_action_transition_and_step_labels_cover_unknown_values() {
        assert_eq!(
            action_name(NodeOperationAction::Deployment as i32),
            "deployment"
        );
        assert_eq!(
            action_name(NodeOperationAction::Rollback as i32),
            "rollback"
        );
        assert_eq!(action_name(NodeOperationAction::Restart as i32), "restart");
        assert_eq!(action_name(i32::MAX), "unspecified");

        let transitions = [
            (NodeSlotTransitionKind::Addition, "addition"),
            (NodeSlotTransitionKind::Replacement, "replacement"),
            (NodeSlotTransitionKind::Unchanged, "unchanged"),
            (NodeSlotTransitionKind::Removal, "removal"),
            (NodeSlotTransitionKind::Restart, "restart"),
            (NodeSlotTransitionKind::Unspecified, "unspecified"),
        ];
        for (value, expected) in transitions {
            assert_eq!(transition_name(value as i32), expected);
        }
        assert_eq!(transition_name(i32::MAX), "unspecified");

        let steps = [
            (1, "verify-release-metadata"),
            (2, "verify-proxy"),
            (3, "stop-backend"),
            (4, "select-release"),
            (5, "start-backend"),
            (6, "verify-target"),
            (7, "switch-authority"),
            (8, "verify-serving"),
            (9, "restore-source"),
            (10, "cleanup-release"),
            (11, "inspect-backend"),
            (0, "complete"),
            (i32::MAX, "complete"),
        ];
        for (value, expected) in steps {
            assert_eq!(next_step_name(value), expected);
        }
    }

    fn evidence(
        backend: BackendKind,
        disposition: BackendStopDisposition,
    ) -> BackendTerminationEvidence {
        BackendTerminationEvidence {
            backend: backend as i32,
            backend_instance_id: "backend-a".into(),
            process_instance_id: "process-a".into(),
            graceful_termination_requested: true,
            kill_escalated: disposition == BackendStopDisposition::Forced,
            disposition: disposition as i32,
            terminal_result: "success".into(),
            exit_code: Some(0),
            signal: None,
        }
    }

    #[test]
    fn operation_dto_retains_affected_slots_and_adds_stable_slot_progress() {
        let operation = NodeOperation {
            action: NodeOperationAction::Deployment as i32,
            targets: vec![OperationTargetProgress {
                kind: InstructionTargetKind::EngineSlot as i32,
                identity: Some(operation_target_progress::Identity::EngineSlot(
                    EngineSlotTargetIdentity {
                        engine_slot: "blue".into(),
                    },
                )),
                details: Some(operation_target_progress::Details::EngineDetails(
                    EngineTargetDetails::default(),
                )),
                transition: NodeSlotTransitionKind::Replacement as i32,
                next_step: 3,
                complete: false,
                conditions: vec![DeploymentCondition {
                    code: "waiting-for-capacity".into(),
                    detail: "one serving target is required".into(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let value = serde_json::to_value(dto(&operation).unwrap()).unwrap();
        assert_eq!(value["action"], "deployment");
        assert_eq!(value["affected_slots"], serde_json::json!(["blue"]));
        assert_eq!(
            value["slots"],
            serde_json::json!([{
                "engine_slot": "blue",
                "transition": "replacement",
                "next_step": "stop-backend",
                "complete": false,
                "conditions": ["waiting-for-capacity"]
            }])
        );
        let slots = operation_slots(&operation).unwrap();
        assert_eq!(
            slot_line(&slots[0]),
            "Slot blue transition=replacement next_step=stop-backend complete=false"
        );
    }

    #[test]
    fn detail_projection_preserves_engine_and_proxy_termination_evidence() {
        let engine_evidence = evidence(BackendKind::Systemd, BackendStopDisposition::Graceful);
        let proxy_evidence = BackendTerminationEvidence {
            backend_instance_id: "proxy-backend".into(),
            process_instance_id: "proxy-process".into(),
            ..evidence(BackendKind::Docker, BackendStopDisposition::Forced)
        };
        let operation = NodeOperation {
            targets: vec![
                OperationTargetProgress {
                    kind: InstructionTargetKind::Proxy as i32,
                    identity: Some(operation_target_progress::Identity::Proxy(
                        ProxyTargetIdentity {},
                    )),
                    details: Some(operation_target_progress::Details::ProxyDetails(
                        ProxyTargetDetails {},
                    )),
                    pinned_backend_instance_id: "proxy-pinned-backend".into(),
                    pinned_process_instance_id: "proxy-pinned-process".into(),
                    effect_backend_instance_id: "proxy-backend".into(),
                    effect_process_instance_id: "proxy-process".into(),
                    termination_evidence: Some(proxy_evidence),
                    ..Default::default()
                },
                OperationTargetProgress {
                    kind: InstructionTargetKind::EngineSlot as i32,
                    identity: Some(operation_target_progress::Identity::EngineSlot(
                        EngineSlotTargetIdentity {
                            engine_slot: "blue".into(),
                        },
                    )),
                    details: Some(operation_target_progress::Details::EngineDetails(
                        EngineTargetDetails::default(),
                    )),
                    termination_evidence: Some(engine_evidence),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let value = detail_dto(&operation).expect("detail JSON");
        assert_eq!(value["schema_version"], 1);
        assert_eq!(
            value["slots"][0]["termination_evidence"]["backend"],
            "systemd"
        );
        assert_eq!(
            value["proxy"]["termination_evidence"]["backend_instance_id"],
            "proxy-backend"
        );
        assert_eq!(value["proxy"]["termination_evidence"]["backend"], "docker");
        assert_eq!(
            value["proxy"]["termination_evidence"]["disposition"],
            "forced"
        );
    }
}
