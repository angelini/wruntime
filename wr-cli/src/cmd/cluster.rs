use std::collections::BTreeSet;

use anyhow::{bail, Result};
use clap::{Args, Subcommand, ValueEnum};
use serde::Serialize;
use tabled::builder::Builder;
use wr_common::wruntime::{
    DeploymentCondition, DeploymentRecord, DeploymentState, EngineStatus, GetClusterStatusResponse,
    ManagerMembershipState, ManagerStatus, ModuleIdentity, ModuleStatus, NodeStatus, RouteStatus,
    ServiceStatus, StatusSeverity,
};

use crate::client;

#[derive(Args)]
pub struct ClusterArgs {
    #[command(subcommand)]
    pub command: ClusterCommand,
}

#[derive(Subcommand)]
pub enum ClusterCommand {
    /// Show one coherent manager-composed cluster status snapshot
    Status(StatusArgs),
}

#[derive(Args)]
pub struct StatusArgs {
    /// Include healthy records as well as problems
    #[arg(long)]
    detail: bool,
    /// Restrict node and engine records to one stable node ID
    #[arg(long)]
    node: Option<String>,
    /// Restrict service records (namespace.module or namespace.module@version)
    #[arg(long)]
    service: Option<String>,
    /// Output format
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    output: OutputFormat,
    /// Exit non-zero at this severity; unknown enables strict unknown handling
    #[arg(long, value_enum, default_value_t = FailOn::Never)]
    fail_on: FailOn,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum OutputFormat {
    Table,
    Json,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum FailOn {
    Never,
    Degraded,
    Unhealthy,
    Unknown,
}

pub async fn run(args: ClusterArgs, manager: &str) -> Result<()> {
    match args.command {
        ClusterCommand::Status(args) => status(args, manager).await,
    }
}

async fn status(args: StatusArgs, manager: &str) -> Result<()> {
    let response = client::get_cluster_status(manager).await?;
    let response = filter(response, args.node.as_deref(), args.service.as_deref())?;

    match args.output {
        OutputFormat::Table => print!("{}", render_table(&response, args.detail)),
        OutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&ClusterDto::from(&response))?
        ),
    }

    if should_fail(&response, args.fail_on) {
        bail!(
            "cluster status reached --fail-on {}",
            match args.fail_on {
                FailOn::Never => "never",
                FailOn::Degraded => "degraded",
                FailOn::Unhealthy => "unhealthy",
                FailOn::Unknown => "unknown",
            }
        );
    }
    Ok(())
}

fn parse_service_filter(value: &str) -> Result<(&str, &str, Option<&str>)> {
    let (identity, version) = value
        .split_once('@')
        .map_or((value, None), |(identity, version)| {
            (identity, Some(version))
        });
    let Some((namespace, name)) = identity.split_once('.') else {
        bail!("invalid --service '{value}': expected namespace.module[@version]");
    };
    if namespace.is_empty() || name.is_empty() || version.is_some_and(str::is_empty) {
        bail!("invalid --service '{value}': expected namespace.module[@version]");
    }
    Ok((namespace, name, version))
}

fn filter(
    mut response: GetClusterStatusResponse,
    node: Option<&str>,
    service: Option<&str>,
) -> Result<GetClusterStatusResponse> {
    if let Some(node_id) = node {
        response.nodes.retain(|item| item.node_id == node_id);
        response.engines.retain(|item| {
            item.deployment
                .as_ref()
                .is_some_and(|metadata| metadata.node_id == node_id)
        });
        let engine_ids: BTreeSet<_> = response
            .engines
            .iter()
            .map(|engine| engine.engine_id.as_str())
            .collect();
        let desired_services: BTreeSet<_> = response
            .nodes
            .iter()
            .flat_map(|item| item.desired_deployment.iter())
            .flat_map(|deployment| deployment.expected_engines.iter())
            .flat_map(|engine| engine.modules.iter())
            .map(|module| {
                (
                    module.namespace.as_str(),
                    module.name.as_str(),
                    module.version.as_str(),
                )
            })
            .collect();
        response.services.retain_mut(|item| {
            let Some(identity) = &item.service else {
                return false;
            };
            let keep = desired_services.contains(&(
                identity.namespace.as_str(),
                identity.name.as_str(),
                identity.version.as_str(),
            ));
            item.routes.retain(|route| {
                route
                    .rule
                    .as_ref()
                    .is_some_and(|rule| engine_ids.contains(rule.engine_id.as_str()))
            });
            keep
        });
    }
    if let Some(value) = service {
        let (namespace, name, version) = parse_service_filter(value)?;
        response.services.retain(|item| {
            item.service.as_ref().is_some_and(|identity| {
                identity.namespace == namespace
                    && identity.name == name
                    && version.is_none_or(|version| identity.version == version)
            })
        });
    }
    Ok(response)
}

fn severity(value: i32) -> StatusSeverity {
    StatusSeverity::try_from(value).unwrap_or(StatusSeverity::Unknown)
}

fn severity_name(value: i32) -> &'static str {
    match severity(value) {
        StatusSeverity::Unknown => "unknown",
        StatusSeverity::Healthy => "healthy",
        StatusSeverity::Degraded => "degraded",
        StatusSeverity::Unhealthy => "unhealthy",
    }
}

fn unknown_present(response: &GetClusterStatusResponse) -> bool {
    severity(response.severity) == StatusSeverity::Unknown
        || response
            .conditions
            .iter()
            .any(|item| severity(item.severity) == StatusSeverity::Unknown)
        || response
            .managers
            .iter()
            .flat_map(|item| item.conditions.iter())
            .chain(
                response
                    .nodes
                    .iter()
                    .flat_map(|item| item.conditions.iter()),
            )
            .chain(
                response
                    .engines
                    .iter()
                    .flat_map(|item| item.conditions.iter()),
            )
            .chain(
                response
                    .services
                    .iter()
                    .flat_map(|item| item.conditions.iter()),
            )
            .any(|item| severity(item.severity) == StatusSeverity::Unknown)
        || response
            .engines
            .iter()
            .flat_map(|engine| engine.modules.iter())
            .flat_map(|module| module.conditions.iter())
            .any(|item| severity(item.severity) == StatusSeverity::Unknown)
        || response
            .services
            .iter()
            .flat_map(|service| service.routes.iter())
            .flat_map(|route| route.conditions.iter())
            .any(|item| severity(item.severity) == StatusSeverity::Unknown)
}

fn should_fail(response: &GetClusterStatusResponse, fail_on: FailOn) -> bool {
    match fail_on {
        FailOn::Never => false,
        FailOn::Unhealthy => severity(response.severity) >= StatusSeverity::Unhealthy,
        FailOn::Degraded => severity(response.severity) >= StatusSeverity::Degraded,
        FailOn::Unknown => unknown_present(response),
    }
}

fn render_table(response: &GetClusterStatusResponse, detail: bool) -> String {
    let mut output = format!(
        "Cluster: {}  routing-version: {}  managers: {}  nodes: {}  engines: {}  services: {}\n",
        severity_name(response.severity),
        response.routing_table_version,
        response.managers.len(),
        response.nodes.len(),
        response.engines.len(),
        response.services.len(),
    );
    let mut builder = Builder::new();
    builder.push_record(["Kind", "Identity", "Status", "Reason", "Detail"]);
    let mut rows = 0usize;

    macro_rules! push_records {
        ($kind:expr, $items:expr, $identity:expr) => {
            for item in $items {
                let item_severity = severity(item.severity);
                if detail || item_severity != StatusSeverity::Healthy {
                    if item.conditions.is_empty() {
                        builder.push_record([
                            $kind.to_string(),
                            $identity(item),
                            severity_name(item.severity).to_string(),
                            String::new(),
                            String::new(),
                        ]);
                        rows += 1;
                    } else {
                        for condition in &item.conditions {
                            builder.push_record([
                                $kind.to_string(),
                                $identity(item),
                                severity_name(item.severity).to_string(),
                                condition.code.clone(),
                                condition.detail.clone(),
                            ]);
                            rows += 1;
                        }
                    }
                }
            }
        };
    }

    push_records!("manager", &response.managers, |item: &ManagerStatus| item
        .manager_id
        .clone());
    push_records!("node", &response.nodes, |item: &NodeStatus| item
        .node_id
        .clone());
    push_records!("engine", &response.engines, |item: &EngineStatus| item
        .engine_id
        .clone());
    push_records!("service", &response.services, |item: &ServiceStatus| {
        item.service
            .as_ref()
            .map(service_identity)
            .unwrap_or_default()
    });
    if detail {
        for condition in &response.conditions {
            builder.push_record([
                "signal".to_string(),
                condition.affected_identity.clone(),
                severity_name(condition.severity).to_string(),
                condition.code.clone(),
                condition.detail.clone(),
            ]);
            rows += 1;
        }
    }
    if rows == 0 {
        output
            .push_str("No problems reported. Use --detail to show healthy and unknown records.\n");
    } else {
        let mut table = builder.build();
        table.with(tabled::settings::Style::rounded());
        output.push_str(&table.to_string());
        output.push('\n');
    }
    output
}

fn service_identity(identity: &ModuleIdentity) -> String {
    format!(
        "{}.{}@{}",
        identity.namespace, identity.name, identity.version
    )
}

#[derive(Serialize)]
struct TimestampDto {
    seconds: i64,
    nanos: i32,
}

impl From<&prost_types::Timestamp> for TimestampDto {
    fn from(value: &prost_types::Timestamp) -> Self {
        Self {
            seconds: value.seconds,
            nanos: value.nanos,
        }
    }
}

#[derive(Serialize)]
struct ConditionDto<'a> {
    code: &'a str,
    severity: &'static str,
    detail: &'a str,
    affected_identity: &'a str,
    desired: &'a str,
    actual: &'a str,
}

impl<'a> From<&'a DeploymentCondition> for ConditionDto<'a> {
    fn from(value: &'a DeploymentCondition) -> Self {
        Self {
            code: &value.code,
            severity: severity_name(value.severity),
            detail: &value.detail,
            affected_identity: &value.affected_identity,
            desired: &value.desired,
            actual: &value.actual,
        }
    }
}

#[derive(Serialize)]
struct ModuleIdentityDto<'a> {
    namespace: &'a str,
    name: &'a str,
    version: &'a str,
}

impl<'a> From<&'a ModuleIdentity> for ModuleIdentityDto<'a> {
    fn from(value: &'a ModuleIdentity) -> Self {
        Self {
            namespace: &value.namespace,
            name: &value.name,
            version: &value.version,
        }
    }
}

#[derive(Serialize)]
struct DeploymentDto<'a> {
    node_id: &'a str,
    revision: u64,
    attempt_token: &'a str,
    bundle_digest: &'a str,
    state: &'static str,
    source_revision: u64,
    expected_engines: Vec<ExpectedEngineDto<'a>>,
    created_at: Option<TimestampDto>,
    activated_at: Option<TimestampDto>,
    completed_at: Option<TimestampDto>,
    failure_detail: &'a str,
}

#[derive(Serialize)]
struct ExpectedEngineDto<'a> {
    engine_slot: &'a str,
    modules: Vec<ModuleIdentityDto<'a>>,
}

impl<'a> From<&'a DeploymentRecord> for DeploymentDto<'a> {
    fn from(value: &'a DeploymentRecord) -> Self {
        Self {
            node_id: &value.node_id,
            revision: value.revision,
            attempt_token: &value.attempt_token,
            bundle_digest: &value.bundle_digest,
            state: match DeploymentState::try_from(value.state)
                .unwrap_or(DeploymentState::Unspecified)
            {
                DeploymentState::Unspecified => "unspecified",
                DeploymentState::Pending => "pending",
                DeploymentState::Active => "active",
                DeploymentState::Succeeded => "succeeded",
                DeploymentState::Failed => "failed",
            },
            source_revision: value.source_revision,
            expected_engines: value
                .expected_engines
                .iter()
                .map(|engine| ExpectedEngineDto {
                    engine_slot: &engine.engine_slot,
                    modules: engine.modules.iter().map(ModuleIdentityDto::from).collect(),
                })
                .collect(),
            created_at: value.created_at.as_ref().map(TimestampDto::from),
            activated_at: value.activated_at.as_ref().map(TimestampDto::from),
            completed_at: value.completed_at.as_ref().map(TimestampDto::from),
            failure_detail: &value.failure_detail,
        }
    }
}

#[derive(Serialize)]
struct ManagerDto<'a> {
    manager_id: &'a str,
    grpc_address: &'a str,
    gossip_address: &'a str,
    severity: &'static str,
    membership: &'static str,
    registered_at: Option<TimestampDto>,
    last_heartbeat: Option<TimestampDto>,
    heartbeat_age_seconds: u64,
    conditions: Vec<ConditionDto<'a>>,
}

impl<'a> From<&'a ManagerStatus> for ManagerDto<'a> {
    fn from(value: &'a ManagerStatus) -> Self {
        Self {
            manager_id: &value.manager_id,
            grpc_address: &value.grpc_address,
            gossip_address: &value.gossip_address,
            severity: severity_name(value.severity),
            membership: match ManagerMembershipState::try_from(value.membership)
                .unwrap_or(ManagerMembershipState::Unknown)
            {
                ManagerMembershipState::Unknown => "unknown",
                ManagerMembershipState::Live => "live",
                ManagerMembershipState::Dead => "dead",
            },
            registered_at: value.registered_at.as_ref().map(TimestampDto::from),
            last_heartbeat: value.last_heartbeat.as_ref().map(TimestampDto::from),
            heartbeat_age_seconds: value.heartbeat_age_seconds,
            conditions: value.conditions.iter().map(ConditionDto::from).collect(),
        }
    }
}

#[derive(Serialize)]
struct ModuleDto<'a> {
    module: Option<ModuleIdentityDto<'a>>,
    severity: &'static str,
    last_healthy: Option<TimestampDto>,
    heartbeat_age_seconds: u64,
    conditions: Vec<ConditionDto<'a>>,
}

impl<'a> From<&'a ModuleStatus> for ModuleDto<'a> {
    fn from(value: &'a ModuleStatus) -> Self {
        Self {
            module: value.module.as_ref().map(ModuleIdentityDto::from),
            severity: severity_name(value.severity),
            last_healthy: value.last_healthy.as_ref().map(TimestampDto::from),
            heartbeat_age_seconds: value.heartbeat_age_seconds,
            conditions: value.conditions.iter().map(ConditionDto::from).collect(),
        }
    }
}

#[derive(Serialize)]
struct EngineDto<'a> {
    engine_id: &'a str,
    address: &'a str,
    deployment: Option<DeploymentMetadataDto<'a>>,
    severity: &'static str,
    authoritative_for_desired_revision: bool,
    registered_at: Option<TimestampDto>,
    last_heartbeat: Option<TimestampDto>,
    heartbeat_age_seconds: u64,
    modules: Vec<ModuleDto<'a>>,
    conditions: Vec<ConditionDto<'a>>,
}

#[derive(Serialize)]
struct DeploymentMetadataDto<'a> {
    node_id: &'a str,
    revision: u64,
    bundle_digest: &'a str,
    engine_slot: &'a str,
}

impl<'a> From<&'a EngineStatus> for EngineDto<'a> {
    fn from(value: &'a EngineStatus) -> Self {
        Self {
            engine_id: &value.engine_id,
            address: &value.address,
            deployment: value
                .deployment
                .as_ref()
                .map(|metadata| DeploymentMetadataDto {
                    node_id: &metadata.node_id,
                    revision: metadata.revision,
                    bundle_digest: &metadata.bundle_digest,
                    engine_slot: &metadata.engine_slot,
                }),
            severity: severity_name(value.severity),
            authoritative_for_desired_revision: value.authoritative_for_desired_revision,
            registered_at: value.registered_at.as_ref().map(TimestampDto::from),
            last_heartbeat: value.last_heartbeat.as_ref().map(TimestampDto::from),
            heartbeat_age_seconds: value.heartbeat_age_seconds,
            modules: value.modules.iter().map(ModuleDto::from).collect(),
            conditions: value.conditions.iter().map(ConditionDto::from).collect(),
        }
    }
}

#[derive(Serialize)]
struct RouteDto<'a> {
    rule_id: &'a str,
    source_namespace: &'a str,
    source_module: &'a str,
    destination_namespace: &'a str,
    destination_module: &'a str,
    destination_version: &'a str,
    engine_id: &'a str,
    engine_address: &'a str,
    peer_address: &'a str,
    healthy: bool,
    desired: bool,
    severity: &'static str,
    updated_at: Option<TimestampDto>,
    conditions: Vec<ConditionDto<'a>>,
}

impl<'a> From<&'a RouteStatus> for RouteDto<'a> {
    fn from(value: &'a RouteStatus) -> Self {
        let rule = value.rule.as_ref();
        Self {
            rule_id: rule.map_or("", |rule| rule.rule_id.as_str()),
            source_namespace: rule.map_or("", |rule| rule.source_namespace.as_str()),
            source_module: rule.map_or("", |rule| rule.source_module.as_str()),
            destination_namespace: rule.map_or("", |rule| rule.destination_namespace.as_str()),
            destination_module: rule.map_or("", |rule| rule.destination_module.as_str()),
            destination_version: rule.map_or("", |rule| rule.destination_version.as_str()),
            engine_id: rule.map_or("", |rule| rule.engine_id.as_str()),
            engine_address: rule.map_or("", |rule| rule.engine_address.as_str()),
            peer_address: rule.map_or("", |rule| rule.peer_address.as_str()),
            healthy: rule.is_some_and(|rule| rule.healthy),
            desired: value.desired,
            severity: severity_name(value.severity),
            updated_at: value.updated_at.as_ref().map(TimestampDto::from),
            conditions: value.conditions.iter().map(ConditionDto::from).collect(),
        }
    }
}

#[derive(Serialize)]
struct ServiceDto<'a> {
    service: Option<ModuleIdentityDto<'a>>,
    severity: &'static str,
    desired_routes: u32,
    healthy_routes: u32,
    unhealthy_routes: u32,
    routes: Vec<RouteDto<'a>>,
    conditions: Vec<ConditionDto<'a>>,
}

impl<'a> From<&'a ServiceStatus> for ServiceDto<'a> {
    fn from(value: &'a ServiceStatus) -> Self {
        Self {
            service: value.service.as_ref().map(ModuleIdentityDto::from),
            severity: severity_name(value.severity),
            desired_routes: value.desired_routes,
            healthy_routes: value.healthy_routes,
            unhealthy_routes: value.unhealthy_routes,
            routes: value.routes.iter().map(RouteDto::from).collect(),
            conditions: value.conditions.iter().map(ConditionDto::from).collect(),
        }
    }
}

#[derive(Serialize)]
struct NodeDto<'a> {
    node_id: &'a str,
    severity: &'static str,
    desired_deployment: Option<DeploymentDto<'a>>,
    deployment_history: Vec<DeploymentDto<'a>>,
    engines: Vec<EngineDto<'a>>,
    conditions: Vec<ConditionDto<'a>>,
}

impl<'a> From<&'a NodeStatus> for NodeDto<'a> {
    fn from(value: &'a NodeStatus) -> Self {
        Self {
            node_id: &value.node_id,
            severity: severity_name(value.severity),
            desired_deployment: value.desired_deployment.as_ref().map(DeploymentDto::from),
            deployment_history: value
                .deployment_history
                .iter()
                .map(DeploymentDto::from)
                .collect(),
            engines: value.engines.iter().map(EngineDto::from).collect(),
            conditions: value.conditions.iter().map(ConditionDto::from).collect(),
        }
    }
}

#[derive(Serialize)]
struct ClusterDto<'a> {
    schema_version: u32,
    severity: &'static str,
    response_at: Option<TimestampDto>,
    database_observed_at: Option<TimestampDto>,
    gossip_observed_at: Option<TimestampDto>,
    routing_table_version: u64,
    managers: Vec<ManagerDto<'a>>,
    nodes: Vec<NodeDto<'a>>,
    engines: Vec<EngineDto<'a>>,
    services: Vec<ServiceDto<'a>>,
    conditions: Vec<ConditionDto<'a>>,
}

impl<'a> From<&'a GetClusterStatusResponse> for ClusterDto<'a> {
    fn from(value: &'a GetClusterStatusResponse) -> Self {
        Self {
            schema_version: 1,
            severity: severity_name(value.severity),
            response_at: value.response_at.as_ref().map(TimestampDto::from),
            database_observed_at: value.database_observed_at.as_ref().map(TimestampDto::from),
            gossip_observed_at: value.gossip_observed_at.as_ref().map(TimestampDto::from),
            routing_table_version: value.routing_table_version,
            managers: value.managers.iter().map(ManagerDto::from).collect(),
            nodes: value.nodes.iter().map(NodeDto::from).collect(),
            engines: value.engines.iter().map(EngineDto::from).collect(),
            services: value.services.iter().map(ServiceDto::from).collect(),
            conditions: value.conditions.iter().map(ConditionDto::from).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(severity: StatusSeverity) -> GetClusterStatusResponse {
        GetClusterStatusResponse {
            severity: severity as i32,
            routing_table_version: 7,
            ..Default::default()
        }
    }

    #[test]
    fn fail_on_thresholds_are_explicit() {
        let degraded = response(StatusSeverity::Degraded);
        assert!(should_fail(&degraded, FailOn::Degraded));
        assert!(!should_fail(&degraded, FailOn::Unhealthy));
        assert!(!should_fail(&degraded, FailOn::Never));
    }

    #[test]
    fn strict_unknown_checks_unknown_conditions() {
        let mut healthy = response(StatusSeverity::Healthy);
        healthy.conditions.push(DeploymentCondition {
            code: "SIGNAL_NOT_REPORTED".into(),
            severity: StatusSeverity::Unknown as i32,
            ..Default::default()
        });
        assert!(should_fail(&healthy, FailOn::Unknown));
    }

    #[test]
    fn json_schema_and_table_are_stable() {
        let value = response(StatusSeverity::Healthy);
        let json = serde_json::to_value(ClusterDto::from(&value)).unwrap();
        assert_eq!(json["schema_version"], 1);
        assert_eq!(json["severity"], "healthy");
        assert!(render_table(&value, false).contains("No problems reported"));
    }

    #[test]
    fn filters_validate_and_select() {
        assert!(parse_service_filter("invalid").is_err());
        assert_eq!(
            parse_service_filter("payments.orders@1.2.3").unwrap(),
            ("payments", "orders", Some("1.2.3"))
        );
    }
}
