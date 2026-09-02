use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use serde::Serialize;
use wr_common::wruntime::{
    CancelOperationRequest, GetOperationRequest, ListOperationsRequest, NodeOperation,
    NodeOperationState, ResumeOperationRequest,
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
    lease_epoch: u64,
    affected_slots: Vec<&'a str>,
    conditions: Vec<&'a str>,
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

pub(crate) fn render_operation(operation: &NodeOperation, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(&dto(operation))?);
    } else {
        println!(
            "Operation {}  {}  {}  node={}  committed={}",
            operation.operation_id,
            action_name(operation.action),
            state_name(operation.state),
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
        latest = client::connect_operator(manager)
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
    let mut operator = client::connect_operator(manager).await?;
    match args.command {
        OperationsCommand::Get { operation_id, json } => {
            let response = operator
                .get_operation(GetOperationRequest { operation_id })
                .await?
                .into_inner();
            let operation = response
                .operation
                .context("GetOperation omitted operation")?;
            render_operation(&operation, json)?;
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
