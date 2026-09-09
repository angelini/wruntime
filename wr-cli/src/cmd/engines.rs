use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use serde::Serialize;
use tabled::builder::Builder;
use tabled::grid::config::HorizontalLine;
use tabled::settings::{Style, Theme};
use wr_common::wruntime::{
    GetOperatorStatusRequest, ListEnginesRequest, NodeOperationAction, RolloutPolicy,
    SubmitOperationRequest,
};

use crate::{client, display};

#[derive(Args)]
pub struct EnginesArgs {
    #[command(subcommand)]
    pub command: EnginesCommand,
}

#[derive(Subcommand)]
pub enum EnginesCommand {
    /// List all registered engines
    List,
    /// Show modules registered on a specific engine
    Get {
        /// Engine ID
        id: String,
    },
    /// Show operator status by stable deployment identity.
    Status {
        #[arg(long)]
        node_id: Option<String>,
        #[arg(long)]
        slot: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Gracefully stop and start one stable engine slot.
    Restart(MutationArgs),
}

#[derive(Args)]
pub struct MutationArgs {
    #[arg(long)]
    node_id: String,
    #[arg(long)]
    slot: String,
    #[arg(long)]
    request_token: Option<String>,
    /// Durable operation deadline in seconds.
    #[arg(long)]
    deadline: Option<u64>,
    /// Permit an operation that temporarily removes all serving capacity.
    #[arg(long)]
    allow_downtime: bool,
    /// Caller-only wait timeout in seconds.
    #[arg(long, default_value_t = 300)]
    wait_timeout: u64,
    #[arg(long)]
    no_wait: bool,
    #[arg(long)]
    json: bool,
}

pub async fn run(args: EnginesArgs, manager: &str) -> Result<()> {
    match args.command {
        EnginesCommand::List => list(manager).await,
        EnginesCommand::Get { id } => get(manager, &id).await,
        EnginesCommand::Status {
            node_id,
            slot,
            json,
        } => status(manager, node_id, slot, json).await,
        EnginesCommand::Restart(args) => mutate(manager, args).await,
    }
}

async fn list(manager: &str) -> Result<()> {
    let mut client = client::connect(manager).await?;
    let resp = client
        .list_engines(ListEnginesRequest {})
        .await?
        .into_inner();

    if resp.engines.is_empty() {
        println!("No engines registered.");
        return Ok(());
    }

    let mut builder = Builder::new();
    builder.push_record(["Engine", "Address", "Module"]);
    let mut separator_rows = Vec::new();
    let mut row_idx = 1; // row 0 is header
    for (i, engine) in resp.engines.iter().enumerate() {
        if i > 0 {
            separator_rows.push(row_idx);
        }
        if engine.modules.is_empty() {
            builder.push_record([engine.engine_id.as_str(), engine.address.as_str(), ""]);
            row_idx += 1;
        } else {
            let mut modules: Vec<_> = engine.modules.iter().collect();
            modules.sort_by(|a, b| {
                (&a.namespace, &a.name, &a.version).cmp(&(&b.namespace, &b.name, &b.version))
            });
            for (j, module) in modules.iter().enumerate() {
                let module_str =
                    format!("{}.{} v{}", module.namespace, module.name, module.version);
                if j == 0 {
                    builder.push_record([
                        engine.engine_id.as_str(),
                        engine.address.as_str(),
                        &module_str,
                    ]);
                } else {
                    builder.push_record(["", "", &module_str]);
                }
                row_idx += 1;
            }
        }
    }

    let mut table = builder.build();
    let mut theme = Theme::from_style(Style::rounded());
    for row in separator_rows {
        theme.insert_horizontal_line(
            row,
            HorizontalLine::new(Some('─'), Some('┼'), Some('├'), Some('┤')),
        );
    }
    table.with(theme);
    println!("{table}");
    Ok(())
}

async fn get(manager: &str, id: &str) -> Result<()> {
    let mut client = client::connect(manager).await?;
    let resp = client
        .list_engines(ListEnginesRequest {})
        .await?
        .into_inner();

    let engine = resp.engines.iter().find(|e| e.engine_id == id);
    let Some(engine) = engine else {
        bail!("Engine '{}' not found", id);
    };

    println!("Engine: {}  Address: {}", engine.engine_id, engine.address);
    println!();

    if engine.modules.is_empty() {
        println!("No modules registered on this engine.");
        return Ok(());
    }

    let mut modules: Vec<_> = engine.modules.iter().collect();
    modules.sort_by(|a, b| {
        (&a.namespace, &a.name, &a.version).cmp(&(&b.namespace, &b.name, &b.version))
    });
    let mut builder = Builder::new();
    builder.push_record(["Namespace", "Module", "Version"]);
    for module in &modules {
        builder.push_record([
            module.namespace.as_str(),
            module.name.as_str(),
            module.version.as_str(),
        ]);
    }
    display::print_table(builder);
    Ok(())
}

#[derive(Serialize)]
struct StatusRow<'a> {
    node_id: &'a str,
    engine_slot: &'a str,
    engine_id: &'a str,
    availability_severity: i32,
    revision: u64,
    authoritative_for_committed_revision: bool,
    lifecycle_stage: &'static str,
    process_instance_id: &'a str,
    backend_state: i32,
    observation_age_seconds: Option<i64>,
}

async fn status(
    manager: &str,
    node_id: Option<String>,
    slot: Option<String>,
    json: bool,
) -> Result<()> {
    if slot.is_some() && node_id.is_none() {
        bail!("--slot requires --node-id");
    }
    let response =
        client::connect_operator(manager, wr_common::manager_client::RetryClass::ReadOnly)
            .await?
            .get_status(GetOperatorStatusRequest {
                node_id: node_id.unwrap_or_default(),
                engine_slot: slot.unwrap_or_default(),
            })
            .await?
            .into_inner();
    let cluster = response
        .cluster
        .context("GetStatus omitted cluster status")?;
    let observations = response.observations;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let rows = cluster
        .engines
        .iter()
        .filter_map(|engine| {
            engine.deployment.as_ref().map(|deployment| {
                let observation = observations.iter().find(|observation| {
                    observation.node_id == deployment.node_id
                        && observation.engine_slot == deployment.engine_slot
                });
                let lifecycle = observation.and_then(|value| value.lifecycle.as_ref());
                let lifecycle_stage = match lifecycle.and_then(|status| {
                    wr_common::wruntime::ProcessLifecycleState::try_from(status.state).ok()
                }) {
                    Some(wr_common::wruntime::ProcessLifecycleState::Starting) => "starting",
                    Some(wr_common::wruntime::ProcessLifecycleState::Ready) => "ready",
                    Some(wr_common::wruntime::ProcessLifecycleState::Stopping) => "stopping",
                    _ => "unknown",
                };
                StatusRow {
                    node_id: &deployment.node_id,
                    engine_slot: &deployment.engine_slot,
                    engine_id: &engine.engine_id,
                    availability_severity: engine.severity,
                    revision: deployment.revision,
                    authoritative_for_committed_revision: engine.authoritative_for_desired_revision,
                    lifecycle_stage,
                    process_instance_id: lifecycle
                        .map(|status| status.process_instance_id.as_str())
                        .unwrap_or(""),
                    backend_state: observation
                        .map(|value| value.backend_state)
                        .unwrap_or_default(),
                    observation_age_seconds: observation
                        .and_then(|value| value.observed_at.as_ref())
                        .map(|value| now.saturating_sub(value.seconds)),
                }
            })
        })
        .collect::<Vec<_>>();
    if json {
        println!("{}", serde_json::to_string_pretty(&rows)?);
    } else if rows.is_empty() {
        println!("No matching engines.");
    } else {
        let mut builder = Builder::new();
        builder.push_record([
            "Node",
            "Slot",
            "Engine",
            "Revision",
            "Lifecycle",
            "Age",
            "Severity",
            "Authority",
        ]);
        for row in rows {
            builder.push_record([
                row.node_id.to_string(),
                row.engine_slot.to_string(),
                row.engine_id.to_string(),
                row.revision.to_string(),
                row.lifecycle_stage.to_string(),
                row.observation_age_seconds
                    .map(|age| format!("{age}s"))
                    .unwrap_or_else(|| "unknown".into()),
                row.availability_severity.to_string(),
                row.authoritative_for_committed_revision.to_string(),
            ]);
        }
        display::print_table(builder);
    }
    Ok(())
}

fn restart_policy(deadline: Option<u64>, allow_downtime: bool) -> RolloutPolicy {
    RolloutPolicy {
        max_unavailable: 1,
        allow_downtime,
        deadline_seconds: deadline.unwrap_or(300),
    }
}

fn restart_request(
    node_id: String,
    request_token: String,
    engine_slot: String,
    policy: RolloutPolicy,
) -> SubmitOperationRequest {
    SubmitOperationRequest {
        node_id,
        request_token,
        action: NodeOperationAction::Restart as i32,
        target_revision: 0,
        bundle_digest: String::new(),
        policy: Some(policy),
        resolved_release_digest: String::new(),
        engine_slot,
    }
}

async fn mutate(manager: &str, args: MutationArgs) -> Result<()> {
    let token = args
        .request_token
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let policy = restart_policy(args.deadline, args.allow_downtime);
    let operation = client::connect_operator(
        manager,
        wr_common::manager_client::RetryClass::DurableCreate,
    )
    .await?
    .submit_operation(restart_request(
        args.node_id,
        token.clone(),
        args.slot,
        policy,
    ))
    .await?
    .into_inner()
    .operation
    .context("SubmitOperation omitted operation")?;
    if !args.json {
        println!("Request token: {token}");
    }
    if args.no_wait {
        return super::operations::render_operation(&operation, args.json);
    }
    super::operations::wait_for_terminal(
        manager,
        operation,
        Duration::from_secs(args.wait_timeout),
        args.json,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restart_policy_requires_explicit_downtime_and_preserves_deadlines() {
        let default = restart_policy(None, false);
        assert!(!default.allow_downtime);
        assert_eq!(default.deadline_seconds, 300);

        let explicit = restart_policy(Some(45), true);
        assert!(explicit.allow_downtime);
        assert_eq!(explicit.deadline_seconds, 45);
    }

    #[test]
    fn restart_request_is_singular_and_has_no_deployment_identity() {
        let request = restart_request(
            "node-a".into(),
            "token-a".into(),
            "blue".into(),
            restart_policy(Some(45), true),
        );
        assert_eq!(request.action, NodeOperationAction::Restart as i32);
        assert_eq!(request.engine_slot, "blue");
        assert_eq!(request.target_revision, 0);
        assert!(request.bundle_digest.is_empty());
        assert!(request.resolved_release_digest.is_empty());
        let policy = request.policy.unwrap();
        assert!(policy.allow_downtime);
        assert_eq!(policy.deadline_seconds, 45);
    }
}
