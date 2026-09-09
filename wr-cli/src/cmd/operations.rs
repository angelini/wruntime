use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use serde::Serialize;
use wr_common::wruntime::{
    BackendKind, BackendStopDisposition, BackendTerminationEvidence, CancelOperationRequest,
    GetOperationRequest, ListOperationsRequest, NodeOperation, NodeOperationState,
    OperationSlotProgress, ResumeOperationRequest,
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

#[derive(Serialize)]
struct OperationDetailDto<'a> {
    schema_version: u32,
    #[serde(flatten)]
    summary: OperationDto<'a>,
    slots: Vec<OperationSlotDetailDto<'a>>,
    proxy: ProxyOperationDetailDto<'a>,
}

fn action_name(value: i32) -> &'static str {
    use wr_common::wruntime::NodeOperationAction as Action;
    match Action::try_from(value).unwrap_or(Action::Unspecified) {
        Action::InitialApply => "initial-apply",
        Action::Drain => "drain",
        Action::Restart => "restart",
        Action::RollingUpgrade => "rolling-upgrade",
        Action::Scale => "scale",
        Action::Rollback => "rollback",
        Action::Unspecified => "unspecified",
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
        Phase::CommittedCleanup => "committed-cleanup",
        Phase::Complete => "complete",
        Phase::Superseded => "superseded",
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

fn slot_detail_dto(slot: &OperationSlotProgress) -> OperationSlotDetailDto<'_> {
    OperationSlotDetailDto {
        engine_slot: &slot.engine_slot,
        changed: slot.changed,
        effect_reported: slot.effect_reported,
        pinned_backend_instance_id: &slot.pinned_backend_instance_id,
        pinned_process_instance_id: &slot.pinned_process_instance_id,
        effect_backend_instance_id: &slot.effect_backend_instance_id,
        effect_process_instance_id: &slot.effect_process_instance_id,
        termination_evidence: slot.termination_evidence.as_ref().map(termination_dto),
    }
}

fn dto(operation: &NodeOperation) -> OperationDto<'_> {
    OperationDto {
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
        affected_slots: operation
            .slots
            .iter()
            .map(|slot| slot.engine_slot.as_str())
            .collect(),
        conditions: operation
            .conditions
            .iter()
            .map(|condition| condition.code.as_str())
            .chain(operation.slots.iter().flat_map(|slot| {
                slot.conditions
                    .iter()
                    .map(|condition| condition.code.as_str())
            }))
            .collect(),
    }
}

fn detail_dto(operation: &NodeOperation) -> OperationDetailDto<'_> {
    OperationDetailDto {
        schema_version: 1,
        summary: dto(operation),
        slots: operation.slots.iter().map(slot_detail_dto).collect(),
        proxy: ProxyOperationDetailDto {
            changed: operation.proxy_changed,
            effect_reported: operation.proxy_effect_reported,
            pinned_backend_instance_id: &operation.proxy_backend_instance_id,
            pinned_process_instance_id: &operation.proxy_process_instance_id,
            effect_backend_instance_id: &operation.proxy_effect_backend_instance_id,
            effect_process_instance_id: &operation.proxy_effect_process_instance_id,
            termination_evidence: operation.termination_evidence.as_ref().map(termination_dto),
        },
    }
}

pub(crate) fn render_operation(operation: &NodeOperation, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(&dto(operation))?);
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
        if !operation.slots.is_empty() {
            println!(
                "Slots: {}",
                operation
                    .slots
                    .iter()
                    .map(|slot| slot.engine_slot.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
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
                println!("{}", serde_json::to_string_pretty(&detail_dto(&operation))?);
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
                    serde_json::to_string_pretty(&operations.iter().map(dto).collect::<Vec<_>>())?
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
    fn detail_projection_preserves_engine_and_proxy_termination_evidence() {
        let operation = NodeOperation {
            operation_id: "operation-a".into(),
            node_id: "node-a".into(),
            request_token: "token-a".into(),
            action: wr_common::wruntime::NodeOperationAction::RollingUpgrade as i32,
            state: NodeOperationState::Succeeded as i32,
            slots: vec![OperationSlotProgress {
                engine_slot: "blue".into(),
                changed: true,
                effect_reported: true,
                pinned_backend_instance_id: "backend-a".into(),
                pinned_process_instance_id: "process-a".into(),
                effect_backend_instance_id: "backend-a".into(),
                effect_process_instance_id: "process-a".into(),
                termination_evidence: Some(evidence(
                    BackendKind::Systemd,
                    BackendStopDisposition::Graceful,
                )),
                ..Default::default()
            }],
            proxy_changed: true,
            proxy_effect_reported: true,
            proxy_backend_instance_id: "proxy-pinned-backend".into(),
            proxy_process_instance_id: "proxy-pinned-process".into(),
            proxy_effect_backend_instance_id: "proxy-backend".into(),
            proxy_effect_process_instance_id: "proxy-process".into(),
            termination_evidence: Some(BackendTerminationEvidence {
                backend_instance_id: "proxy-backend".into(),
                process_instance_id: "proxy-process".into(),
                ..evidence(BackendKind::Docker, BackendStopDisposition::Forced)
            }),
            ..Default::default()
        };
        let value = serde_json::to_value(detail_dto(&operation)).expect("detail JSON");
        assert_eq!(value["schema_version"], 1);
        assert_eq!(
            value["slots"][0]["termination_evidence"]["backend"],
            "systemd"
        );
        assert_eq!(
            value["slots"][0]["termination_evidence"]["disposition"],
            "graceful"
        );
        assert_eq!(
            value["proxy"]["pinned_backend_instance_id"],
            "proxy-pinned-backend"
        );
        assert_eq!(
            value["proxy"]["pinned_process_instance_id"],
            "proxy-pinned-process"
        );
        assert_eq!(
            value["proxy"]["termination_evidence"]["backend_instance_id"],
            "proxy-backend"
        );
        assert_eq!(
            value["proxy"]["termination_evidence"]["process_instance_id"],
            "proxy-process"
        );
        assert_eq!(value["proxy"]["termination_evidence"]["backend"], "docker");
        assert_eq!(
            value["proxy"]["termination_evidence"]["disposition"],
            "forced"
        );
    }

    #[test]
    fn detail_is_explicitly_null_unknown_and_summary_stays_compact() {
        let operation = NodeOperation {
            slots: vec![OperationSlotProgress {
                engine_slot: "blue".into(),
                termination_evidence: Some(evidence(
                    BackendKind::Unspecified,
                    BackendStopDisposition::Unknown,
                )),
                ..Default::default()
            }],
            ..Default::default()
        };
        let detail = serde_json::to_value(detail_dto(&operation)).expect("detail JSON");
        assert_eq!(
            detail["slots"][0]["termination_evidence"]["backend"],
            "unknown"
        );
        assert_eq!(
            detail["slots"][0]["termination_evidence"]["disposition"],
            "unknown"
        );
        assert!(detail["proxy"]["termination_evidence"].is_null());
        let summary = serde_json::to_value(dto(&operation)).expect("summary JSON");
        assert!(summary.get("slots").is_none());
        assert!(summary.get("proxy").is_none());
        assert!(summary.get("schema_version").is_none());
    }
}
