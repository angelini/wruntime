use std::collections::BTreeSet;
use std::time::Duration;

use anyhow::{bail, Result};
use clap::{Args, Subcommand, ValueEnum};
use serde::Serialize;
use tabled::builder::Builder;
use wr_common::wruntime::{
    DeploymentCondition, DeploymentRecord, DeploymentState, EngineStatus, GetClusterStatusResponse,
    ManagerMembershipState, ManagerStatus, ModuleIdentity, ModuleStatus, NodeStatus,
    ProcessLifecycleState, ProxyBreakerDestinationKind, ProxyBreakerStatus, ProxyInventoryStatus,
    ProxyListenerKind, ProxyListenerStatus, ProxyRoutingStatus, RouteStatus, ServiceStatus,
    StatusSeverity,
};

use super::helpers::{self, WaitAttempt};
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
    /// Wait until a selected cluster target has one exact health severity.
    Wait(WaitArgs),
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

#[derive(Args)]
pub struct WaitArgs {
    /// Restrict the expectation to one stable node ID.
    #[arg(long)]
    node: Option<String>,
    /// Restrict the expectation to namespace.module[@version].
    #[arg(long)]
    service: Option<String>,
    /// Exact severity that must be observed.
    #[arg(long, value_enum)]
    severity: ExpectedSeverity,
    /// One absolute wait deadline in seconds.
    #[arg(long, default_value_t = 60)]
    timeout_secs: u64,
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

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ExpectedSeverity {
    Healthy,
    Degraded,
    Unhealthy,
    Unknown,
}

impl From<ExpectedSeverity> for StatusSeverity {
    fn from(value: ExpectedSeverity) -> Self {
        match value {
            ExpectedSeverity::Healthy => Self::Healthy,
            ExpectedSeverity::Degraded => Self::Degraded,
            ExpectedSeverity::Unhealthy => Self::Unhealthy,
            ExpectedSeverity::Unknown => Self::Unknown,
        }
    }
}

pub async fn run(args: ClusterArgs, manager: &str) -> Result<()> {
    match args.command {
        ClusterCommand::Status(args) => status(args, manager).await,
        ClusterCommand::Wait(args) => wait(args, manager).await,
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

fn validated_severity(value: i32, target: &str) -> Result<StatusSeverity> {
    StatusSeverity::try_from(value)
        .map_err(|_| anyhow::anyhow!("malformed {target} severity value {value}"))
}

fn expectation_matches(
    response: &GetClusterStatusResponse,
    node: Option<&str>,
    service: Option<&str>,
    expected: StatusSeverity,
) -> Result<bool> {
    validated_severity(response.severity, "cluster")?;
    let observed = if service.is_some() {
        if response.services.is_empty() {
            bail!("cluster expectation target matched no services");
        }
        response
            .services
            .iter()
            .map(|item| validated_severity(item.severity, "service"))
            .collect::<Result<Vec<_>>>()?
    } else if node.is_some() {
        if response.nodes.is_empty() {
            bail!("cluster expectation target matched no nodes");
        }
        response
            .nodes
            .iter()
            .map(|item| validated_severity(item.severity, "node"))
            .collect::<Result<Vec<_>>>()?
    } else {
        vec![validated_severity(response.severity, "cluster")?]
    };
    Ok(observed.iter().all(|value| *value == expected))
}

#[derive(Serialize)]
struct WaitOutput<'a> {
    outcome: &'static str,
    expected_severity: &'static str,
    snapshot: ClusterDto<'a>,
}

async fn wait(args: WaitArgs, manager: &str) -> Result<()> {
    if let Some(service) = args.service.as_deref() {
        parse_service_filter(service)?;
    }
    let expected: StatusSeverity = args.severity.into();
    let timeout = Duration::from_secs(args.timeout_secs);
    let node = args.node.clone();
    let service = args.service.clone();
    let subject = format!("cluster expectation for {}", severity_name(expected as i32));
    let filtered =
        helpers::wait_with_deadline(&subject, timeout, Duration::from_millis(500), || async {
            let response = match client::get_cluster_status(manager).await {
                Ok(response) => response,
                Err(error) => return WaitAttempt::QueryFailure(error),
            };
            let filtered = match filter(response, node.as_deref(), service.as_deref()) {
                Ok(filtered) => filtered,
                Err(error) => return WaitAttempt::Terminal(error),
            };
            let matched =
                match expectation_matches(&filtered, node.as_deref(), service.as_deref(), expected)
                {
                    Ok(matched) => matched,
                    Err(error) => return WaitAttempt::Terminal(error),
                };
            if matched {
                WaitAttempt::Matched(filtered)
            } else {
                match serde_json::to_string(&ClusterDto::from(&filtered)) {
                    Ok(evidence) => WaitAttempt::Pending(evidence),
                    Err(error) => WaitAttempt::Terminal(
                        anyhow::Error::new(error)
                            .context("failed to serialize cluster expectation evidence"),
                    ),
                }
            }
        })
        .await?;

    println!(
        "{}",
        serde_json::to_string_pretty(&WaitOutput {
            outcome: "observed",
            expected_severity: severity_name(expected as i32),
            snapshot: ClusterDto::from(&filtered),
        })?
    );
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
        response.proxies.retain(|item| item.node_id == node_id);
        let engine_ids: BTreeSet<_> = response
            .engines
            .iter()
            .map(|engine| engine.engine_id.as_str())
            .collect();
        let desired_services: BTreeSet<_> = response
            .nodes
            .iter()
            .flat_map(|item| item.desired_deployment.iter())
            .flat_map(|deployment| {
                deployment
                    .inventory
                    .as_ref()
                    .into_iter()
                    .flat_map(|inventory| inventory.engines.iter())
            })
            .flat_map(|engine| engine.modules.iter())
            .filter_map(|module| module.identity.as_ref())
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
        || response
            .proxies
            .iter()
            .chain(response.nodes.iter().flat_map(|node| node.proxies.iter()))
            .any(|proxy| {
                proxy
                    .conditions
                    .iter()
                    .chain(
                        proxy
                            .listeners
                            .iter()
                            .flat_map(|item| item.conditions.iter()),
                    )
                    .chain(proxy.routing.iter().flat_map(|item| item.conditions.iter()))
                    .chain(
                        proxy
                            .breakers
                            .iter()
                            .flat_map(|item| item.conditions.iter()),
                    )
                    .any(|item| severity(item.severity) == StatusSeverity::Unknown)
            })
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
        "Cluster: {}  routing-version: {}  managers: {}  nodes: {}  engines: {}  proxies: {}  services: {}\n",
        severity_name(response.severity),
        response.routing_table_version,
        response.managers.len(),
        response.nodes.len(),
        response.engines.len(),
        response.proxies.len(),
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
    push_records!("proxy", &response.proxies, |item: &ProxyInventoryStatus| {
        format!("{}/{}", item.proxy_id, item.process_instance_id)
    });
    if detail {
        for proxy in &response.proxies {
            let identity = format!("{}/{}", proxy.proxy_id, proxy.process_instance_id);
            let report = proxy.report.as_ref();
            let deployment = proxy.deployment.as_ref();
            builder.push_record([
                "proxy-detail".to_string(),
                identity.clone(),
                severity_name(proxy.severity).to_string(),
                format!(
                    "expected={} selected={} superseded={}",
                    proxy.expected, proxy.selected, proxy.superseded
                ),
                format!(
                    "deployment={}/{} operation={} lifecycle={} admission={} report-age={}s receiver={}",
                    deployment.map_or("", |item| item.node_id.as_str()),
                    deployment.map_or(0, |item| item.revision),
                    deployment.map_or("", |item| item.operation_id.as_str()),
                    report.map_or("missing", |item| lifecycle_name(item.lifecycle_state)),
                    report.is_some_and(|item| item.admission_open),
                    proxy.report_age_seconds,
                    proxy.receiving_manager_id,
                ),
            ]);
            rows += 1;
            for listener in &proxy.listeners {
                builder.push_record([
                    "proxy-listener".to_string(),
                    identity.clone(),
                    severity_name(listener.severity).to_string(),
                    listener_kind_name(listener.kind).to_string(),
                    format!(
                        "configured={} accepting={}",
                        listener.configured, listener.accepting
                    ),
                ]);
                rows += 1;
            }
            if let Some(routing) = &proxy.routing {
                builder.push_record([
                    "proxy-routing".to_string(),
                    identity.clone(),
                    severity_name(routing.severity).to_string(),
                    format!("version={}", routing.installed_table_version),
                    format!(
                        "synchronized={} age={}s manager={}",
                        routing.synchronized,
                        routing.synchronization_age_seconds,
                        routing.source_manager_id
                    ),
                ]);
                rows += 1;
            }
            for breaker in &proxy.breakers {
                builder.push_record([
                    "proxy-breaker".to_string(),
                    identity.clone(),
                    severity_name(breaker.severity).to_string(),
                    breaker_kind_name(breaker.destination_kind).to_string(),
                    format!(
                        "total={} closed={} open={} half-open={}",
                        breaker.total, breaker.closed, breaker.open, breaker.half_open
                    ),
                ]);
                rows += 1;
            }
        }

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

fn lifecycle_name(value: i32) -> &'static str {
    match ProcessLifecycleState::try_from(value).unwrap_or(ProcessLifecycleState::Unspecified) {
        ProcessLifecycleState::Unspecified => "unspecified",
        ProcessLifecycleState::Starting => "starting",
        ProcessLifecycleState::Ready => "ready",
        ProcessLifecycleState::Stopping => "stopping",
    }
}

fn listener_kind_name(value: i32) -> &'static str {
    match ProxyListenerKind::try_from(value).unwrap_or(ProxyListenerKind::Unspecified) {
        ProxyListenerKind::Unspecified => "unspecified",
        ProxyListenerKind::DataPlane => "data-plane",
        ProxyListenerKind::NodeControl => "node-control",
        ProxyListenerKind::Peer => "peer",
        ProxyListenerKind::External => "external",
    }
}

fn breaker_kind_name(value: i32) -> &'static str {
    match ProxyBreakerDestinationKind::try_from(value)
        .unwrap_or(ProxyBreakerDestinationKind::Unspecified)
    {
        ProxyBreakerDestinationKind::Unspecified => "unspecified",
        ProxyBreakerDestinationKind::LocalEngine => "local-engine",
        ProxyBreakerDestinationKind::PeerProxy => "peer-proxy",
    }
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
    resolved_release_digest: &'a str,
    revision_digest: &'a str,
    operation_id: &'a str,
    state: &'static str,
    source_revision: u64,
    expected_engines: Vec<ExpectedEngineDto<'a>>,
    created_at: Option<TimestampDto>,
    finalized_at: Option<TimestampDto>,
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
            resolved_release_digest: &value.resolved_release_digest,
            revision_digest: &value.revision_digest,
            operation_id: &value.operation_id,
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
                .inventory
                .as_ref()
                .into_iter()
                .flat_map(|inventory| inventory.engines.iter())
                .map(|engine| ExpectedEngineDto {
                    engine_slot: &engine.engine_slot,
                    modules: engine
                        .modules
                        .iter()
                        .filter_map(|module| module.identity.as_ref())
                        .map(ModuleIdentityDto::from)
                        .collect(),
                })
                .collect(),
            created_at: value.created_at.as_ref().map(TimestampDto::from),
            finalized_at: value.finalized_at.as_ref().map(TimestampDto::from),
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
struct ProxyDeploymentDto<'a> {
    node_id: &'a str,
    revision: u64,
    bundle_digest: &'a str,
    operation_id: &'a str,
    revision_digest: &'a str,
}

#[derive(Serialize)]
struct ProxyListenerDto<'a> {
    kind: &'static str,
    configured: bool,
    accepting: bool,
    severity: &'static str,
    conditions: Vec<ConditionDto<'a>>,
}

impl<'a> From<&'a ProxyListenerStatus> for ProxyListenerDto<'a> {
    fn from(value: &'a ProxyListenerStatus) -> Self {
        Self {
            kind: listener_kind_name(value.kind),
            configured: value.configured,
            accepting: value.accepting,
            severity: severity_name(value.severity),
            conditions: value.conditions.iter().map(ConditionDto::from).collect(),
        }
    }
}

#[derive(Serialize)]
struct ProxyRoutingDto<'a> {
    installed_table_version: u64,
    synchronization_age_seconds: u64,
    source_manager_id: &'a str,
    synchronized: bool,
    severity: &'static str,
    conditions: Vec<ConditionDto<'a>>,
}

impl<'a> From<&'a ProxyRoutingStatus> for ProxyRoutingDto<'a> {
    fn from(value: &'a ProxyRoutingStatus) -> Self {
        Self {
            installed_table_version: value.installed_table_version,
            synchronization_age_seconds: value.synchronization_age_seconds,
            source_manager_id: &value.source_manager_id,
            synchronized: value.synchronized,
            severity: severity_name(value.severity),
            conditions: value.conditions.iter().map(ConditionDto::from).collect(),
        }
    }
}

#[derive(Serialize)]
struct ProxyBreakerDto<'a> {
    destination_kind: &'static str,
    total: u32,
    closed: u32,
    open: u32,
    half_open: u32,
    severity: &'static str,
    conditions: Vec<ConditionDto<'a>>,
}

impl<'a> From<&'a ProxyBreakerStatus> for ProxyBreakerDto<'a> {
    fn from(value: &'a ProxyBreakerStatus) -> Self {
        Self {
            destination_kind: breaker_kind_name(value.destination_kind),
            total: value.total,
            closed: value.closed,
            open: value.open,
            half_open: value.half_open,
            severity: severity_name(value.severity),
            conditions: value.conditions.iter().map(ConditionDto::from).collect(),
        }
    }
}

#[derive(Serialize)]
struct ProxyDto<'a> {
    proxy_id: &'a str,
    node_id: &'a str,
    process_instance_id: &'a str,
    deployment: Option<ProxyDeploymentDto<'a>>,
    severity: &'static str,
    expected: bool,
    selected: bool,
    superseded: bool,
    lifecycle: &'static str,
    admission_open: bool,
    registered_at: Option<TimestampDto>,
    report_received_at: Option<TimestampDto>,
    report_age_seconds: u64,
    receiving_manager_id: &'a str,
    listeners: Vec<ProxyListenerDto<'a>>,
    routing: Option<ProxyRoutingDto<'a>>,
    breakers: Vec<ProxyBreakerDto<'a>>,
    conditions: Vec<ConditionDto<'a>>,
}

impl<'a> From<&'a ProxyInventoryStatus> for ProxyDto<'a> {
    fn from(value: &'a ProxyInventoryStatus) -> Self {
        Self {
            proxy_id: &value.proxy_id,
            node_id: &value.node_id,
            process_instance_id: &value.process_instance_id,
            deployment: value.deployment.as_ref().map(|item| ProxyDeploymentDto {
                node_id: &item.node_id,
                revision: item.revision,
                bundle_digest: &item.bundle_digest,
                operation_id: &item.operation_id,
                revision_digest: &item.revision_digest,
            }),
            severity: severity_name(value.severity),
            expected: value.expected,
            selected: value.selected,
            superseded: value.superseded,
            lifecycle: value
                .report
                .as_ref()
                .map_or("missing", |report| lifecycle_name(report.lifecycle_state)),
            admission_open: value
                .report
                .as_ref()
                .is_some_and(|report| report.admission_open),
            registered_at: value.registered_at.as_ref().map(TimestampDto::from),
            report_received_at: value.report_received_at.as_ref().map(TimestampDto::from),
            report_age_seconds: value.report_age_seconds,
            receiving_manager_id: &value.receiving_manager_id,
            listeners: value.listeners.iter().map(ProxyListenerDto::from).collect(),
            routing: value.routing.as_ref().map(ProxyRoutingDto::from),
            breakers: value.breakers.iter().map(ProxyBreakerDto::from).collect(),
            conditions: value.conditions.iter().map(ConditionDto::from).collect(),
        }
    }
}

#[derive(Serialize)]
struct NodeDto<'a> {
    node_id: &'a str,
    severity: &'static str,
    desired_deployment: Option<DeploymentDto<'a>>,
    target_deployment: Option<DeploymentDto<'a>>,
    deployment_history: Vec<DeploymentDto<'a>>,
    engines: Vec<EngineDto<'a>>,
    proxies: Vec<ProxyDto<'a>>,
    conditions: Vec<ConditionDto<'a>>,
}

impl<'a> From<&'a NodeStatus> for NodeDto<'a> {
    fn from(value: &'a NodeStatus) -> Self {
        Self {
            node_id: &value.node_id,
            severity: severity_name(value.severity),
            desired_deployment: value.desired_deployment.as_ref().map(DeploymentDto::from),
            target_deployment: value.target_deployment.as_ref().map(DeploymentDto::from),
            deployment_history: value
                .deployment_history
                .iter()
                .map(DeploymentDto::from)
                .collect(),
            engines: value.engines.iter().map(EngineDto::from).collect(),
            proxies: value.proxies.iter().map(ProxyDto::from).collect(),
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
    routing_table_version: u64,
    managers: Vec<ManagerDto<'a>>,
    nodes: Vec<NodeDto<'a>>,
    engines: Vec<EngineDto<'a>>,
    proxies: Vec<ProxyDto<'a>>,
    services: Vec<ServiceDto<'a>>,
    conditions: Vec<ConditionDto<'a>>,
}

impl<'a> From<&'a GetClusterStatusResponse> for ClusterDto<'a> {
    fn from(value: &'a GetClusterStatusResponse) -> Self {
        Self {
            schema_version: 2,
            severity: severity_name(value.severity),
            response_at: value.response_at.as_ref().map(TimestampDto::from),
            database_observed_at: value.database_observed_at.as_ref().map(TimestampDto::from),
            routing_table_version: value.routing_table_version,
            managers: value.managers.iter().map(ManagerDto::from).collect(),
            nodes: value.nodes.iter().map(NodeDto::from).collect(),
            engines: value.engines.iter().map(EngineDto::from).collect(),
            proxies: value.proxies.iter().map(ProxyDto::from).collect(),
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
    fn proxy_filter_json_and_strict_unknown_cover_canonical_and_nested_inventory() {
        let proxy = ProxyInventoryStatus {
            proxy_id: "proxy-a".into(),
            node_id: "node-a".into(),
            process_instance_id: "process-a".into(),
            severity: StatusSeverity::Healthy as i32,
            selected: true,
            routing: Some(ProxyRoutingStatus {
                installed_table_version: 7,
                synchronized: true,
                severity: StatusSeverity::Healthy as i32,
                ..Default::default()
            }),
            breakers: vec![ProxyBreakerStatus {
                severity: StatusSeverity::Healthy as i32,
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut value = response(StatusSeverity::Healthy);
        value.proxies = vec![
            proxy.clone(),
            ProxyInventoryStatus {
                node_id: "node-b".into(),
                ..proxy.clone()
            },
        ];
        value.nodes = vec![NodeStatus {
            node_id: "node-a".into(),
            proxies: vec![proxy],
            ..Default::default()
        }];
        let filtered = filter(value, Some("node-a"), None).unwrap();
        assert_eq!(filtered.proxies.len(), 1);
        assert_eq!(filtered.nodes[0].proxies.len(), 1);
        let json = serde_json::to_value(ClusterDto::from(&filtered)).unwrap();
        assert_eq!(json["schema_version"], 2);
        assert_eq!(json["proxies"][0]["selected"], true);
        assert!(
            !unknown_present(&filtered),
            "known zero breaker evidence is not unknown"
        );

        let mut unknown = filtered;
        unknown.proxies[0]
            .routing
            .as_mut()
            .unwrap()
            .conditions
            .push(DeploymentCondition {
                severity: StatusSeverity::Unknown as i32,
                code: "MISSING_ROUTING_DIMENSION".into(),
                ..Default::default()
            });
        assert!(unknown_present(&unknown));
    }

    #[test]
    fn json_schema_and_table_are_stable() {
        let value = response(StatusSeverity::Healthy);
        let json = serde_json::to_value(ClusterDto::from(&value)).unwrap();
        assert_eq!(json["schema_version"], 2);
        assert_eq!(json["severity"], "healthy");
        assert!(render_table(&value, false).contains("No problems reported"));
    }

    #[test]
    fn json_includes_finalized_target_deployment() {
        let target = DeploymentRecord {
            node_id: "node-a".into(),
            revision: 2,
            attempt_token: "retry-token".into(),
            bundle_digest: "sha256:bundle".into(),
            resolved_release_digest: "sha256:resolved".into(),
            revision_digest: "sha256:revision".into(),
            operation_id: "00000000-0000-8000-8000-000000000001".into(),
            finalized_at: Some(prost_types::Timestamp {
                seconds: 123,
                nanos: 456,
            }),
            ..Default::default()
        };
        let node = NodeStatus {
            node_id: "node-a".into(),
            target_deployment: Some(target.clone()),
            deployment_history: vec![target],
            ..Default::default()
        };

        let json = serde_json::to_value(NodeDto::from(&node)).unwrap();
        assert_eq!(json["target_deployment"]["revision"], 2);
        assert_eq!(json["target_deployment"]["bundle_digest"], "sha256:bundle");
        assert_eq!(
            json["target_deployment"]["resolved_release_digest"],
            "sha256:resolved"
        );
        assert_eq!(
            json["target_deployment"]["revision_digest"],
            "sha256:revision"
        );
        assert_eq!(
            json["target_deployment"]["operation_id"],
            "00000000-0000-8000-8000-000000000001"
        );
        assert_eq!(
            json["target_deployment"]["finalized_at"],
            serde_json::json!({"seconds": 123, "nanos": 456})
        );
        assert_eq!(
            json["deployment_history"][0]["resolved_release_digest"],
            "sha256:resolved"
        );
    }

    #[test]
    fn filters_validate_and_select() {
        assert!(parse_service_filter("invalid").is_err());
        assert_eq!(
            parse_service_filter("payments.orders@1.2.3").unwrap(),
            ("payments", "orders", Some("1.2.3"))
        );
    }

    #[test]
    fn expectation_rejects_malformed_wire_severity() {
        let malformed = GetClusterStatusResponse {
            severity: 999,
            ..Default::default()
        };
        let error = expectation_matches(&malformed, None, None, StatusSeverity::Unknown)
            .expect_err("malformed severity must not satisfy UNKNOWN");
        assert!(error.to_string().contains("malformed cluster severity"));
    }

    #[test]
    fn expectation_requires_a_present_exact_target() {
        let empty = response(StatusSeverity::Unhealthy);
        assert!(expectation_matches(
            &empty,
            Some("missing-node"),
            None,
            StatusSeverity::Unhealthy,
        )
        .is_err());

        let mut selected = response(StatusSeverity::Healthy);
        selected.nodes.push(NodeStatus {
            node_id: "node-a".to_string(),
            severity: StatusSeverity::Unhealthy as i32,
            ..Default::default()
        });
        assert!(
            expectation_matches(&selected, Some("node-a"), None, StatusSeverity::Unhealthy,)
                .unwrap()
        );
        assert!(
            !expectation_matches(&selected, Some("node-a"), None, StatusSeverity::Healthy,)
                .unwrap()
        );
    }
}
