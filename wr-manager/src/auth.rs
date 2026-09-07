use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use tonic::{Request, Status};
use wr_common::authorization_policy::{PolicyRole, ValidatedPolicy};
use wr_common::identity::{ClusterId, PrincipalKind};

/// The complete set mounted on the manager listener. Completeness checks are
/// deliberately scoped to these services, not every service in the descriptor.
pub const MANAGER_SERVICE_SET: [&str; 6] = [
    "wruntime.ClusterService",
    "wruntime.InfrastructureService",
    "wruntime.NodeService",
    "wruntime.JobService",
    "wruntime.PolicyService",
    "wruntime.LifecycleService",
];

pub const NON_MANAGER_SERVICE_SET: [&str; 2] = [
    "wruntime.ProxyNodeControlService",
    "wruntime.EngineJobAdminService",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkloadBinding {
    NotApplicable,
    ProxyBound,
    NodeAgentBound,
    EnrolledGlobal,
    ProxyEnrolledGlobal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlProfile {
    SmallRead,
    LargeRead,
    ConfigWrite,
    Heartbeat,
    Operation,
    Job,
    Snapshot,
    RolloutControl,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RpcAuthorizationRow {
    pub service: &'static str,
    pub method: &'static str,
    pub capability_and_policy: &'static str,
    pub extract_and_lookup: &'static str,
    pub response_filter: &'static str,
    pub workload: WorkloadBinding,
    pub control: ControlProfile,
}

macro_rules! row {
    ($service:literal, $method:literal, $cap:literal, $extract:literal, $filter:literal, $workload:ident, $control:ident) => {
        RpcAuthorizationRow {
            service: concat!("wruntime.", $service),
            method: $method,
            capability_and_policy: $cap,
            extract_and_lookup: $extract,
            response_filter: $filter,
            workload: WorkloadBinding::$workload,
            control: ControlProfile::$control,
        }
    };
}

/// Proto-independent authorization contract. Runtime adapters introduced in
/// the policy phase consume this table; descriptor coverage already treats an
/// absent row as default-deny.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkloadKind {
    Manager,
    Proxy,
    NodeAgent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EnrolledWorkload<'a> {
    pub kind: WorkloadKind,
    pub node_id: Option<&'a str>,
}

/// Workload enrollment is evaluated as a mandatory conjunct after resource
/// tuple authorization. Global rows waive only node filtering, never
/// enrollment or workload-kind checks.
pub fn workload_binding_matches(
    binding: WorkloadBinding,
    enrollment: Option<EnrolledWorkload<'_>>,
    requested_node: Option<&str>,
) -> bool {
    match binding {
        WorkloadBinding::NotApplicable => true,
        WorkloadBinding::ProxyBound => enrollment.is_some_and(|workload| {
            workload.kind == WorkloadKind::Proxy
                && workload.node_id.is_some()
                && workload.node_id == requested_node
        }),
        WorkloadBinding::NodeAgentBound => enrollment.is_some_and(|workload| {
            workload.kind == WorkloadKind::NodeAgent
                && workload.node_id.is_some()
                && workload.node_id == requested_node
        }),
        WorkloadBinding::EnrolledGlobal => enrollment.is_some(),
        WorkloadBinding::ProxyEnrolledGlobal => {
            enrollment.is_some_and(|workload| workload.kind == WorkloadKind::Proxy)
        }
    }
}

pub const MANAGER_RPC_ROWS: [RpcAuthorizationRow; 46] = [
    row!("ClusterService", "ListEngines", "cluster.engines.read; H:view/admin, S:status", "node/namespace selectors; engine registry", "rows/count/page by matching tuples", NotApplicable, LargeRead),
    row!("ClusterService", "GetRoutingTable", "cluster.routes.read; H:view/admin, S:status, W:proxy", "namespace selector; routing repository", "scoped human/SA; complete cluster routing for proxy", ProxyEnrolledGlobal, LargeRead),
    row!("ClusterService", "UpsertRoutingRule", "cluster.routes.write; H:admin, S:route", "namespace/rule; routing repository", "n/a", NotApplicable, ConfigWrite),
    row!("ClusterService", "DeleteRoutingRule", "cluster.routes.write; H:admin, S:route", "namespace/rule; routing repository", "n/a", NotApplicable, ConfigWrite),
    row!("ClusterService", "ListManagers", "cluster.managers.read; H:view/admin, S:status, W:proxy", "manager lease repository", "bounded complete status", ProxyEnrolledGlobal, SmallRead),
    row!("ClusterService", "GetClusterStatus", "cluster.status.read; H:view/admin, S:status", "selectors; composed status owner", "components/aggregates by tuples", NotApplicable, LargeRead),
    row!("ClusterService", "GetSchema", "cluster.schema.read; H:view/admin, S:status, W:proxy", "namespace/module; schema repository", "requested schema", ProxyEnrolledGlobal, LargeRead),
    row!("ClusterService", "SetSecret", "cluster.secrets.write; H:admin, S:secret", "namespace/key; secret repository", "n/a", NotApplicable, ConfigWrite),
    row!("ClusterService", "DeleteSecret", "cluster.secrets.write; H:admin, S:secret", "namespace/key; secret repository", "n/a", NotApplicable, ConfigWrite),
    row!("ClusterService", "ListSecrets", "cluster.secrets.read; H:admin, S:secret", "namespace/all; secret metadata repository", "metadata/count/page by tuples; never values", NotApplicable, LargeRead),
    row!("ClusterService", "UpsertSchedule", "cluster.schedules.write; H:admin, S:schedule", "namespace/schedule; schedule repository", "n/a", NotApplicable, ConfigWrite),
    row!("ClusterService", "DeleteSchedule", "cluster.schedules.write; H:admin, S:schedule", "namespace/schedule; schedule repository", "n/a", NotApplicable, ConfigWrite),
    row!("ClusterService", "ListSchedules", "cluster.schedules.read; H:view/admin, S:status/schedule", "namespace/all; schedule repository", "rows/count/page by tuples", NotApplicable, LargeRead),
    row!("InfrastructureService", "GetStatus", "infrastructure.status.read; H:view/infra/admin, S:status/infra", "node selectors; status owner", "nodes/operations/counts by tuples", NotApplicable, LargeRead),
    row!("InfrastructureService", "SubmitOperation", "infrastructure.operations.write; H:infra/admin, S:infra", "request token plus node/target; operation repository", "n/a", NotApplicable, Operation),
    row!("InfrastructureService", "GetOperation", "infrastructure.operations.read; H:view/infra/admin, S:status/infra", "operation ID; repository resolves node first", "one operation after node match", NotApplicable, SmallRead),
    row!("InfrastructureService", "ListOperations", "infrastructure.operations.read; H:view/infra/admin, S:status/infra", "node/status selectors; operation repository", "rows/count/page by resolved node", NotApplicable, LargeRead),
    row!("InfrastructureService", "ResumeOperation", "infrastructure.operations.write; H:infra/admin, S:infra", "operation ID; locked node/state lookup", "n/a", NotApplicable, Operation),
    row!("InfrastructureService", "CancelOperation", "infrastructure.operations.write; H:infra/admin, S:infra", "operation ID; locked node/state lookup", "n/a", NotApplicable, Operation),
    row!("InfrastructureService", "BeginDeployment", "infrastructure.deployments.write; H:infra/admin, S:deploy", "request token plus node/deployment/revision; deployment repository", "n/a", NotApplicable, Operation),
    row!("InfrastructureService", "VerifyDeployment", "infrastructure.deployments.write; H:infra/admin, S:deploy", "deployment operation ID; node/revision lookup", "n/a", NotApplicable, Operation),
    row!("InfrastructureService", "FinalizeDeployment", "infrastructure.deployments.write; H:infra/admin, S:deploy", "node/revision/state; locked deployment lookup", "n/a", NotApplicable, Operation),
    row!("InfrastructureService", "AbandonDeployment", "infrastructure.deployments.write; H:infra/admin, S:deploy", "request token plus node; locked deployment and evidence lookup", "n/a", NotApplicable, Operation),
    row!("InfrastructureService", "BeginRollback",  "infrastructure.deployments.write; H:infra/admin, S:deploy", "request token plus deployment/target revision; authority lookup", "n/a", NotApplicable, Operation),
    row!("InfrastructureService", "BeginManagerRollout", "infrastructure.manager-rollout.write; H:infra/admin, S:deploy; bootstrap/recovery exceptions", "caller UUID plus canonical target generation/digest/expected set and optional predecessor; rollout repository", "original rollout ID/state on exact token replay", NotApplicable, RolloutControl),
    row!("InfrastructureService", "LeaseManagerRollout", "infrastructure.manager-rollout.write; while closed only recorded deployment principal", "rollout ID, executor/epoch; rollout CAS", "current lease/state", NotApplicable, RolloutControl),
    row!("InfrastructureService", "AdvanceManagerRollout", "infrastructure.manager-rollout.write; while closed only recorded deployment principal", "rollout ID, lease epoch, expected phase/member outcomes; rollout CAS", "committed phase/members", NotApplicable, RolloutControl),
    row!("InfrastructureService", "GetManagerRollout", "rollout read roles while open; recorded principal while closed", "rollout ID; rollout repository", "bounded phase/member status", NotApplicable, RolloutControl),
    row!("InfrastructureService", "PutNodeAgentPolicy", "infrastructure.node-agent-policy.write; H:infra/admin, S:deploy", "bound node; policy repository", "n/a", NotApplicable, ConfigWrite),
    row!("NodeService", "RegisterEngine",  "node.engines.register; W:proxy", "node + operation/revision/slot/activation; desired deployment/owner lookup", "credentials/current policy/fence for owner", ProxyBound, Operation),
    row!("NodeService", "DeregisterEngine", "node.engines.deregister; W:proxy", "complete ownership fence; locked owner lookup", "n/a", ProxyBound, Operation),
    row!("NodeService", "Heartbeat", "node.engines.heartbeat; W:proxy", "complete ownership fence; owner lookup", "matching complete engine snapshot", ProxyBound, Heartbeat),
    row!("NodeService", "BeginEngineDrain", "node.engines.drain; W:proxy", "complete ownership fence; locked owner lookup", "n/a", ProxyBound, Operation),
    row!("NodeService", "Attest", "node.attestations.write; W:node-agent", "bound node; agent policy lookup", "attestation decision and conditions", NodeAgentBound, Heartbeat),
    row!("NodeService", "ClaimOperation",  "node.operations.claim; W:node-agent", "bound node; queue selects that node", "returned operation matches node", NodeAgentBound, Operation),
    row!("NodeService", "RenewOperationLease", "node.operations.renew; W:node-agent", "operation/lease epoch; owner/node lookup", "n/a", NodeAgentBound, Heartbeat),
    row!("NodeService", "ReportObservation", "node.observations.write; W:node-agent", "operation/node; node lookup", "n/a", NodeAgentBound, Operation),
    row!("NodeService", "ReportStepResult", "node.steps.write; W:node-agent", "operation/step/lease; owner/node lookup", "n/a", NodeAgentBound, Operation),
    row!("JobService", "ListJobQueues", "jobs.read; H:job-view/job-admin/admin, S:job-view/job-admin", "queue catalog", "rows/counts by whole-grant union", NotApplicable, LargeRead),
    row!("JobService", "ListJobs", "jobs.read; H:job-view/job-admin/admin, S:job-view/job-admin", "queue/page; delegate lookup", "authorized queue", NotApplicable, Job),
    row!("JobService", "GetJobQueueSummary", "jobs.read; H:job-view/job-admin/admin, S:job-view/job-admin", "queue; delegate lookup", "authorized queue summary", NotApplicable, Job),
    row!("JobService", "GetJob", "jobs.read; H:job-view/job-admin/admin, S:job-view/job-admin", "queue/job; delegate lookup", "authorized queue/job", NotApplicable, Job),
    row!("JobService", "RetryJob", "jobs.retry; H:job-admin/admin, S:job-admin", "queue/job; deterministic delegate then locked engine queue lookup", "mutation result", NotApplicable, Job),
    row!("PolicyService", "GetPolicyStatus", "policy.status.read; H:view/admin, S:status, W:manager/proxy/node-agent", "immutable policy/admission snapshot", "bounded summary only", EnrolledGlobal, SmallRead),
    row!("PolicyService", "GetWorkloadSnapshot", "policy.workload.read; W:proxy", "authenticated proxy; precomputed projection", "complete peer enrollments/revocations", ProxyEnrolledGlobal, Snapshot),
    row!("LifecycleService", "GetStatus", "lifecycle status roles and all enrolled workloads", "internal lifecycle/rollout observation", "bounded readiness/generation/digest/admission/rollout", EnrolledGlobal, SmallRead),
];

/// One assignment is a complete tuple grant. `None` is explicit unscoped
/// authority for that dimension; dimensions are never combined across entries.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ResourceAssignment {
    dimensions: BTreeMap<String, Option<BTreeSet<String>>>,
}

impl ResourceAssignment {
    pub fn scoped(
        mut self,
        dimension: impl Into<String>,
        values: impl IntoIterator<Item = String>,
    ) -> Self {
        self.dimensions
            .insert(dimension.into(), Some(values.into_iter().collect()));
        self
    }

    pub fn unscoped(mut self, dimension: impl Into<String>) -> Self {
        self.dimensions.insert(dimension.into(), None);
        self
    }

    pub fn covers(&self, resource: &BTreeMap<String, String>) -> bool {
        resource
            .iter()
            .all(|(dimension, value)| match self.dimensions.get(dimension) {
                Some(None) => true,
                Some(Some(values)) => values.contains(value),
                None => false,
            })
    }
}

pub fn assignment_union_covers(
    assignments: &[ResourceAssignment],
    resource: &BTreeMap<String, String>,
) -> bool {
    assignments
        .iter()
        .any(|assignment| assignment.covers(resource))
}

pub fn filter_authorized<T>(
    rows: impl IntoIterator<Item = T>,
    assignments: &[ResourceAssignment],
    resource: impl Fn(&T) -> BTreeMap<String, String>,
) -> Vec<T> {
    rows.into_iter()
        .filter(|row| assignment_union_covers(assignments, &resource(row)))
        .collect()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorizedPrincipal {
    pub name: String,
    pub kind: PrincipalKind,
    pub node_id: Option<String>,
    pub fingerprint: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestExtractorClass {
    None,
    Namespace,
    Node,
    AttestationNode,
    JobQueue,
    ManagerSet,
    EngineOwnership,
    OperationId,
    RoutingRule,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthoritativeLookupClass {
    None,
    EngineRegistry,
    RoutingRepository,
    ManagerLease,
    StatusRepository,
    SchemaRepository,
    SecretRepository,
    ScheduleRepository,
    OperationRepository,
    DeploymentRepository,
    RolloutRepository,
    DesiredDeployment,
    JobDelegate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponseFilterClass {
    None,
    NamespaceTuple,
    NodeTuple,
    JobQueueTuple,
    BoundedStatus,
    WorkloadSnapshot,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HandlerAdapterBinding {
    pub extractor: RequestExtractorClass,
    pub lookup: AuthoritativeLookupClass,
    pub filter: ResponseFilterClass,
    pub workload: WorkloadBinding,
}

/// Independent checked-in handler binding catalogue. Unknown methods have no
/// binding and therefore fail closed rather than inheriting a generic adapter.
pub fn handler_adapter_binding(service: &str, method: &str) -> Option<HandlerAdapterBinding> {
    use AuthoritativeLookupClass as L;
    use RequestExtractorClass as E;
    use ResponseFilterClass as F;
    let (extractor, lookup, filter) = match (service, method) {
        ("wruntime.ClusterService", "ListEngines") => (E::None, L::EngineRegistry, F::NodeTuple),
        ("wruntime.ClusterService", "GetRoutingTable") => {
            (E::None, L::RoutingRepository, F::NamespaceTuple)
        }
        ("wruntime.ClusterService", "UpsertRoutingRule") => {
            (E::RoutingRule, L::RoutingRepository, F::None)
        }
        ("wruntime.ClusterService", "DeleteRoutingRule") => {
            (E::OperationId, L::RoutingRepository, F::None)
        }
        ("wruntime.ClusterService", "ListManagers") => (E::None, L::ManagerLease, F::BoundedStatus),
        ("wruntime.ClusterService", "GetClusterStatus") => {
            (E::None, L::StatusRepository, F::NodeTuple)
        }
        ("wruntime.ClusterService", "GetSchema") => {
            (E::Namespace, L::SchemaRepository, F::NamespaceTuple)
        }
        ("wruntime.ClusterService", "SetSecret" | "DeleteSecret") => {
            (E::Namespace, L::SecretRepository, F::None)
        }
        ("wruntime.ClusterService", "ListSecrets") => {
            (E::Namespace, L::SecretRepository, F::NamespaceTuple)
        }
        ("wruntime.ClusterService", "UpsertSchedule" | "DeleteSchedule") => {
            (E::Namespace, L::ScheduleRepository, F::None)
        }
        ("wruntime.ClusterService", "ListSchedules") => {
            (E::Namespace, L::ScheduleRepository, F::NamespaceTuple)
        }
        ("wruntime.InfrastructureService", "GetStatus") => {
            (E::Node, L::StatusRepository, F::NodeTuple)
        }
        ("wruntime.InfrastructureService", "SubmitOperation") => {
            (E::Node, L::OperationRepository, F::None)
        }
        (
            "wruntime.InfrastructureService",
            "GetOperation" | "ResumeOperation" | "CancelOperation",
        ) => (E::OperationId, L::OperationRepository, F::None),
        ("wruntime.InfrastructureService", "ListOperations") => {
            (E::Node, L::OperationRepository, F::NodeTuple)
        }
        ("wruntime.InfrastructureService", "BeginDeployment") => {
            (E::Node, L::DeploymentRepository, F::None)
        }
        (
            "wruntime.InfrastructureService",
            "VerifyDeployment" | "FinalizeDeployment" | "AbandonDeployment" | "BeginRollback",
        ) => (E::Node, L::DeploymentRepository, F::None),
        ("wruntime.InfrastructureService", "PutNodeAgentPolicy") => {
            (E::Node, L::OperationRepository, F::None)
        }
        ("wruntime.InfrastructureService", "BeginManagerRollout") => {
            (E::ManagerSet, L::RolloutRepository, F::BoundedStatus)
        }
        (
            "wruntime.InfrastructureService",
            "LeaseManagerRollout" | "AdvanceManagerRollout" | "GetManagerRollout",
        ) => (E::OperationId, L::RolloutRepository, F::BoundedStatus),
        ("wruntime.NodeService", "RegisterEngine") => (
            E::EngineOwnership,
            L::DesiredDeployment,
            F::WorkloadSnapshot,
        ),
        ("wruntime.NodeService", "DeregisterEngine" | "Heartbeat" | "BeginEngineDrain") => {
            (E::EngineOwnership, L::EngineRegistry, F::None)
        }
        ("wruntime.NodeService", "Attest") => {
            (E::AttestationNode, L::OperationRepository, F::NodeTuple)
        }
        ("wruntime.NodeService", "ClaimOperation") => {
            (E::Node, L::OperationRepository, F::NodeTuple)
        }
        (
            "wruntime.NodeService",
            "RenewOperationLease" | "ReportObservation" | "ReportStepResult",
        ) => (E::OperationId, L::OperationRepository, F::None),
        ("wruntime.JobService", "ListJobQueues") => (E::None, L::JobDelegate, F::JobQueueTuple),
        ("wruntime.JobService", "ListJobs" | "GetJobQueueSummary" | "GetJob" | "RetryJob") => {
            (E::JobQueue, L::JobDelegate, F::JobQueueTuple)
        }
        ("wruntime.PolicyService", "GetPolicyStatus") => (E::None, L::None, F::BoundedStatus),
        ("wruntime.PolicyService", "GetWorkloadSnapshot") => {
            (E::None, L::None, F::WorkloadSnapshot)
        }
        ("wruntime.LifecycleService", "GetStatus") => (E::None, L::None, F::BoundedStatus),
        _ => return None,
    };
    let workload = MANAGER_RPC_ROWS
        .iter()
        .find(|row| row.service == service && row.method == method)?
        .workload;
    Some(HandlerAdapterBinding {
        extractor,
        lookup,
        filter,
        workload,
    })
}

#[derive(Clone, Debug, Default)]
pub struct AuthorizationResource<'a> {
    pub namespace_id: Option<&'a str>,
    pub node_id: Option<&'a str>,
    pub job_queue_id: Option<&'a str>,
    pub manager_ids: &'a [String],
}

#[derive(Clone)]
pub struct PrincipalPolicy {
    snapshot: Arc<ValidatedPolicy>,
    cluster: ClusterId,
}

fn roles_for_row(row: &RpcAuthorizationRow, kind: PrincipalKind) -> &'static [PolicyRole] {
    use PolicyRole as R;
    match (row.service, row.method, kind) {
        (
            "wruntime.ClusterService",
            "UpsertRoutingRule" | "DeleteRoutingRule",
            PrincipalKind::Human,
        ) => &[R::Admin],
        (
            "wruntime.ClusterService",
            "UpsertRoutingRule" | "DeleteRoutingRule",
            PrincipalKind::ServiceAccount,
        ) => &[R::Route],
        (
            "wruntime.ClusterService",
            "SetSecret" | "DeleteSecret" | "ListSecrets",
            PrincipalKind::Human,
        ) => &[R::Admin],
        (
            "wruntime.ClusterService",
            "SetSecret" | "DeleteSecret" | "ListSecrets",
            PrincipalKind::ServiceAccount,
        ) => &[R::Secret],
        ("wruntime.ClusterService", "UpsertSchedule" | "DeleteSchedule", PrincipalKind::Human) => {
            &[R::Admin]
        }
        (
            "wruntime.ClusterService",
            "UpsertSchedule" | "DeleteSchedule",
            PrincipalKind::ServiceAccount,
        ) => &[R::Schedule],
        ("wruntime.ClusterService", "ListSchedules", PrincipalKind::Human) => &[R::Admin, R::View],
        ("wruntime.ClusterService", "ListSchedules", PrincipalKind::ServiceAccount) => {
            &[R::Status, R::Schedule]
        }
        ("wruntime.ClusterService", _, PrincipalKind::Human) => &[R::Admin, R::View],
        ("wruntime.ClusterService", _, PrincipalKind::ServiceAccount) => &[R::Status],
        (
            "wruntime.InfrastructureService",
            "SubmitOperation" | "ResumeOperation" | "CancelOperation",
            PrincipalKind::Human,
        ) => &[R::Admin, R::Infra],
        (
            "wruntime.InfrastructureService",
            "SubmitOperation" | "ResumeOperation" | "CancelOperation",
            PrincipalKind::ServiceAccount,
        ) => &[R::Infra],
        (
            "wruntime.InfrastructureService",
            "BeginDeployment"
            | "VerifyDeployment"
            | "FinalizeDeployment"
            | "AbandonDeployment"
            | "PutNodeAgentPolicy"
            | "BeginRollback"
            | "BeginManagerRollout"
            | "LeaseManagerRollout"
            | "AdvanceManagerRollout",
            PrincipalKind::Human,
        ) => &[R::Admin, R::Infra],
        (
            "wruntime.InfrastructureService",
            "BeginDeployment"
            | "VerifyDeployment"
            | "FinalizeDeployment"
            | "AbandonDeployment"
            | "PutNodeAgentPolicy"
            | "BeginRollback"
            | "BeginManagerRollout"
            | "LeaseManagerRollout"
            | "AdvanceManagerRollout",
            PrincipalKind::ServiceAccount,
        ) => &[R::Deploy],
        ("wruntime.InfrastructureService", _, PrincipalKind::Human) => {
            &[R::Admin, R::Infra, R::View]
        }
        ("wruntime.InfrastructureService", "GetManagerRollout", PrincipalKind::ServiceAccount) => {
            &[R::Deploy, R::Infra, R::Status]
        }
        ("wruntime.InfrastructureService", _, PrincipalKind::ServiceAccount) => {
            &[R::Infra, R::Status]
        }
        ("wruntime.JobService", "RetryJob", PrincipalKind::Human) => &[R::Admin, R::JobAdmin],
        ("wruntime.JobService", "RetryJob", PrincipalKind::ServiceAccount) => &[R::JobAdmin],
        ("wruntime.JobService", _, PrincipalKind::Human) => &[R::Admin, R::JobAdmin, R::JobView],
        ("wruntime.JobService", _, PrincipalKind::ServiceAccount) => &[R::JobAdmin, R::JobView],
        ("wruntime.PolicyService", _, PrincipalKind::Human) => &[R::Admin, R::View],
        ("wruntime.PolicyService", _, PrincipalKind::ServiceAccount) => &[R::Status],
        ("wruntime.LifecycleService", _, PrincipalKind::Human) => &[R::Admin, R::View, R::Infra],
        ("wruntime.LifecycleService", _, PrincipalKind::ServiceAccount) => {
            &[R::Status, R::Infra, R::Deploy]
        }
        _ => &[],
    }
}

/// Method-bound authorization proof. Its fields are private so only
/// `ManagerAuthorizer` can create a production caller context.
#[derive(Clone, Debug)]
pub struct AuthorizedCall {
    service: &'static str,
    method: &'static str,
    principal: AuthorizedPrincipal,
    resource: OwnedAuthorizationResource,
    assignments: Vec<wr_common::authorization_policy::RoleAssignment>,
}

#[derive(Clone, Debug, Default)]
struct OwnedAuthorizationResource {
    namespace_id: Option<String>,
    node_id: Option<String>,
    job_queue_id: Option<String>,
    manager_ids: Vec<String>,
    namespace_filter: Option<BTreeSet<String>>,
    node_filter: Option<BTreeSet<String>>,
    job_queue_filter: Option<BTreeSet<String>>,
    manager_filter: Option<BTreeSet<String>>,
}

impl AuthorizedCall {
    pub(crate) fn principal_node_id(&self) -> Option<&str> {
        self.principal.node_id.as_deref()
    }

    pub(crate) fn allows_collection_item(
        &self,
        namespace_id: Option<&str>,
        node_id: Option<&str>,
        job_queue_id: Option<&str>,
        manager_id: Option<&str>,
    ) -> bool {
        self.assignments.is_empty()
            || self.assignments.iter().any(|assignment| {
                namespace_id.is_none_or(|value| {
                    assignment
                        .scope
                        .namespace_ids
                        .as_ref()
                        .is_none_or(|allowed| allowed.contains(value))
                }) && node_id.is_none_or(|value| {
                    assignment
                        .scope
                        .node_ids
                        .as_ref()
                        .is_none_or(|allowed| allowed.contains(value))
                }) && job_queue_id.is_none_or(|value| {
                    assignment
                        .scope
                        .job_queue_ids
                        .as_ref()
                        .is_none_or(|allowed| allowed.contains(value))
                }) && manager_id.is_none_or(|value| {
                    assignment
                        .scope
                        .manager_ids
                        .as_ref()
                        .is_none_or(|allowed| allowed.contains(value))
                })
            })
    }

    pub(crate) fn job_queue_filter(&self) -> Option<&BTreeSet<String>> {
        self.resource.job_queue_filter.as_ref()
    }

    pub(crate) fn verify(&self, service: &str, method: &str) -> Result<(), Status> {
        if self.service != service || self.method != method {
            return Err(Status::permission_denied(
                "authorized caller context does not match the invoked RPC",
            ));
        }
        let _consumed_authorization_results = (
            &self.principal,
            self.resource.namespace_id.as_deref(),
            self.resource.node_id.as_deref(),
            self.resource.job_queue_id.as_deref(),
            self.resource.manager_ids.as_slice(),
            self.resource.namespace_filter.as_ref(),
            self.resource.node_filter.as_ref(),
            self.resource.job_queue_filter.as_ref(),
            self.resource.manager_filter.as_ref(),
        );
        Ok(())
    }
}

/// Shared authorization owner for the sole manager listener.
#[derive(Clone)]
pub struct ManagerAuthorizer {
    policy: PrincipalPolicy,
    admission: wr_common::lifecycle_service::AdmissionGate,
}

impl ManagerAuthorizer {
    pub fn new(
        policy: PrincipalPolicy,
        admission: wr_common::lifecycle_service::AdmissionGate,
    ) -> Self {
        Self { policy, admission }
    }

    pub fn policy(&self) -> &PrincipalPolicy {
        &self.policy
    }

    fn allowed_while_closed(service: &str, method: &str) -> bool {
        service == "wruntime.LifecycleService"
            || (service == "wruntime.InfrastructureService"
                && matches!(
                    method,
                    "BeginManagerRollout"
                        | "LeaseManagerRollout"
                        | "AdvanceManagerRollout"
                        | "GetManagerRollout"
                ))
    }

    pub fn authorize<T>(
        &self,
        request: &mut Request<T>,
        service: &'static str,
        method: &'static str,
        resource: &AuthorizationResource<'_>,
    ) -> Result<AuthorizedCall, Status> {
        let matching_rows = MANAGER_RPC_ROWS
            .iter()
            .filter(|row| row.service == service && row.method == method)
            .count();
        if matching_rows != 1 || handler_adapter_binding(service, method).is_none() {
            return Err(Status::permission_denied(
                "RPC authorization metadata is missing or ambiguous",
            ));
        }
        let evidence = self.policy.peer_evidence(request)?;
        let mut effective_node = resource.node_id;
        if effective_node.is_none() {
            if let Some(principal) = evidence.principal.as_ref() {
                effective_node = self
                    .policy
                    .snapshot
                    .proxy_enrollments
                    .iter()
                    .chain(self.policy.snapshot.node_agent_enrollments.iter())
                    .find(|enrollment| enrollment.principal == principal.as_str())
                    .map(|enrollment| enrollment.node_id.as_str());
            }
        }
        let effective = AuthorizationResource {
            namespace_id: resource.namespace_id,
            node_id: effective_node,
            job_queue_id: resource.job_queue_id,
            manager_ids: resource.manager_ids,
        };
        let principal = self
            .policy
            .authorize_row_evidence(service, method, &evidence, &effective)?;
        if !self.admission.is_open() && !Self::allowed_while_closed(service, method) {
            return Err(Status::unavailable(
                "privileged manager admission is closed",
            ));
        }
        let roles = roles_for_row(
            MANAGER_RPC_ROWS
                .iter()
                .find(|row| row.service == service && row.method == method)
                .expect("unique row checked above"),
            principal.kind,
        );
        let assignments = self
            .policy
            .snapshot
            .assignments
            .iter()
            .filter(|assignment| {
                assignment.principal == principal.name && roles.contains(&assignment.role)
            })
            .collect::<Vec<_>>();
        let collect_scope = |select: fn(
            &wr_common::authorization_policy::AssignmentScope,
        ) -> Option<&BTreeSet<String>>| {
            if assignments
                .iter()
                .any(|assignment| select(&assignment.scope).is_none())
            {
                None
            } else {
                Some(
                    assignments
                        .iter()
                        .filter_map(|assignment| select(&assignment.scope))
                        .flatten()
                        .cloned()
                        .collect::<BTreeSet<_>>(),
                )
            }
        };
        let namespace_filter = collect_scope(|scope| scope.namespace_ids.as_ref());
        let node_filter = collect_scope(|scope| scope.node_ids.as_ref());
        let job_queue_filter = collect_scope(|scope| scope.job_queue_ids.as_ref());
        let manager_filter = collect_scope(|scope| scope.manager_ids.as_ref());
        request.extensions_mut().insert(principal.clone());
        let call = AuthorizedCall {
            service,
            method,
            principal,
            assignments: assignments.into_iter().cloned().collect(),
            resource: OwnedAuthorizationResource {
                namespace_id: effective.namespace_id.map(str::to_owned),
                node_id: effective.node_id.map(str::to_owned),
                job_queue_id: effective.job_queue_id.map(str::to_owned),
                manager_ids: effective
                    .manager_ids
                    .iter()
                    .map(|id| (*id).to_owned())
                    .collect(),
                namespace_filter,
                node_filter,
                job_queue_filter,
                manager_filter,
            },
        };
        request.extensions_mut().insert(call.clone());
        Ok(call)
    }
}

impl PrincipalPolicy {
    pub fn new(snapshot: Arc<ValidatedPolicy>) -> Self {
        let cluster = ClusterId::parse(&snapshot.cluster_id).expect("validated policy cluster");
        Self { snapshot, cluster }
    }

    pub fn snapshot(&self) -> &Arc<ValidatedPolicy> {
        &self.snapshot
    }

    fn peer_evidence<T>(
        &self,
        request: &Request<T>,
    ) -> Result<wr_common::tls::LeafEvidence, Status> {
        let certificate = request
            .peer_certs()
            .and_then(|certificates| certificates.first().cloned())
            .ok_or_else(|| Status::unauthenticated("client certificate identity is unavailable"))?;
        wr_common::tls::validate_client_leaf(certificate.as_ref(), &self.cluster)
            .map_err(|error| Status::unauthenticated(error.to_string()))
    }

    fn peer<T>(&self, request: &Request<T>) -> Result<AuthorizedPrincipal, Status> {
        if let Some(principal) = request.extensions().get::<AuthorizedPrincipal>() {
            return Ok(principal.clone());
        }
        let evidence = self.peer_evidence(request)?;
        if self
            .snapshot
            .revoked_leaf_fingerprints
            .contains(&evidence.fingerprint)
        {
            return Err(Status::permission_denied(
                "client certificate leaf is revoked",
            ));
        }
        let uri = evidence.principal.expect("validated client leaf has URI");
        let kind = self
            .snapshot
            .principals
            .get(uri.as_str())
            .copied()
            .ok_or_else(|| {
                Status::permission_denied("client principal is not declared by policy")
            })?;
        let node_id = match kind {
            PrincipalKind::Proxy => self
                .snapshot
                .proxy_enrollments
                .iter()
                .find(|e| e.principal == uri.as_str())
                .map(|e| e.node_id.clone()),
            PrincipalKind::NodeAgent => self
                .snapshot
                .node_agent_enrollments
                .iter()
                .find(|e| e.principal == uri.as_str())
                .map(|e| e.node_id.clone()),
            PrincipalKind::Manager => {
                if self
                    .snapshot
                    .manager_enrollments
                    .iter()
                    .any(|e| e.principal == uri.as_str())
                {
                    None
                } else {
                    return Err(Status::permission_denied(
                        "manager principal is not enrolled",
                    ));
                }
            }
            _ => None,
        };
        Ok(AuthorizedPrincipal {
            name: uri.to_string(),
            kind,
            node_id,
            fingerprint: evidence.fingerprint,
        })
    }

    fn has_role(&self, principal: &AuthorizedPrincipal, roles: &[PolicyRole]) -> bool {
        self.snapshot.assignments.iter().any(|assignment| {
            assignment.principal == principal.name && roles.contains(&assignment.role)
        })
    }

    fn has_node_grant(
        &self,
        principal: &AuthorizedPrincipal,
        human_roles: &[PolicyRole],
        service_roles: &[PolicyRole],
        node_id: Option<&str>,
    ) -> bool {
        let roles = match principal.kind {
            PrincipalKind::Human => human_roles,
            PrincipalKind::ServiceAccount => service_roles,
            _ => return false,
        };
        self.snapshot.assignments.iter().any(|assignment| {
            assignment.principal == principal.name
                && roles.contains(&assignment.role)
                && node_id.is_none_or(|node_id| {
                    assignment
                        .scope
                        .node_ids
                        .as_ref()
                        .is_none_or(|allowed| allowed.contains(node_id))
                })
        })
    }

    pub fn authorize_row_evidence(
        &self,
        service: &str,
        method: &str,
        evidence: &wr_common::tls::LeafEvidence,
        resource: &AuthorizationResource<'_>,
    ) -> Result<AuthorizedPrincipal, Status> {
        if evidence.profile != wr_common::tls::LeafProfile::Client {
            return Err(Status::unauthenticated(
                "authenticated leaf is not a client profile",
            ));
        }
        let uri = evidence.principal.as_ref().ok_or_else(|| {
            Status::unauthenticated("client certificate principal is unavailable")
        })?;
        if uri.cluster_id() != &self.cluster {
            return Err(Status::permission_denied(
                "client principal belongs to the wrong cluster",
            ));
        }
        if self
            .snapshot
            .revoked_leaf_fingerprints
            .contains(&evidence.fingerprint)
        {
            return Err(Status::permission_denied(
                "client certificate leaf is revoked",
            ));
        }
        let kind = self
            .snapshot
            .principals
            .get(uri.as_str())
            .copied()
            .filter(|kind| *kind == uri.kind())
            .ok_or_else(|| {
                Status::permission_denied("client principal is not declared by policy")
            })?;
        let node_id = match kind {
            PrincipalKind::Proxy => self
                .snapshot
                .proxy_enrollments
                .iter()
                .find(|enrollment| enrollment.principal == uri.as_str())
                .map(|enrollment| enrollment.node_id.clone()),
            PrincipalKind::NodeAgent => self
                .snapshot
                .node_agent_enrollments
                .iter()
                .find(|enrollment| enrollment.principal == uri.as_str())
                .map(|enrollment| enrollment.node_id.clone()),
            PrincipalKind::Manager => self
                .snapshot
                .manager_enrollments
                .iter()
                .any(|enrollment| enrollment.principal == uri.as_str())
                .then_some(String::new()),
            _ => None,
        };
        if matches!(
            kind,
            PrincipalKind::Manager | PrincipalKind::Proxy | PrincipalKind::NodeAgent
        ) && node_id.is_none()
        {
            return Err(Status::permission_denied(
                "workload principal is not enrolled",
            ));
        }
        let row = MANAGER_RPC_ROWS
            .iter()
            .find(|row| row.service == service && row.method == method)
            .ok_or_else(|| Status::permission_denied("RPC has no authorization row"))?;
        handler_adapter_binding(service, method)
            .ok_or_else(|| Status::permission_denied("RPC has no handler adapter binding"))?;
        let principal = AuthorizedPrincipal {
            name: uri.to_string(),
            kind,
            node_id: node_id.filter(|value| !value.is_empty()),
            fingerprint: evidence.fingerprint.clone(),
        };
        if matches!(
            kind,
            PrincipalKind::Manager | PrincipalKind::Proxy | PrincipalKind::NodeAgent
        ) {
            let enrollment = Some(EnrolledWorkload {
                kind: match kind {
                    PrincipalKind::Manager => WorkloadKind::Manager,
                    PrincipalKind::Proxy => WorkloadKind::Proxy,
                    PrincipalKind::NodeAgent => WorkloadKind::NodeAgent,
                    _ => unreachable!(),
                },
                node_id: principal.node_id.as_deref(),
            });
            if row.workload != WorkloadBinding::NotApplicable
                && workload_binding_matches(row.workload, enrollment, resource.node_id)
            {
                return Ok(principal);
            }
            return Err(Status::permission_denied(
                "workload binding does not authorize this RPC",
            ));
        }
        let roles = roles_for_row(row, kind);
        let covered = self.snapshot.assignments.iter().any(|assignment| {
            assignment.principal == principal.name
                && roles.contains(&assignment.role)
                && resource.namespace_id.is_none_or(|value| {
                    assignment
                        .scope
                        .namespace_ids
                        .as_ref()
                        .is_none_or(|allowed| allowed.contains(value))
                })
                && resource.node_id.is_none_or(|value| {
                    assignment
                        .scope
                        .node_ids
                        .as_ref()
                        .is_none_or(|allowed| allowed.contains(value))
                })
                && resource.job_queue_id.is_none_or(|value| {
                    assignment
                        .scope
                        .job_queue_ids
                        .as_ref()
                        .is_none_or(|allowed| allowed.contains(value))
                })
                && assignment.scope.manager_ids.as_ref().is_none_or(|allowed| {
                    resource
                        .manager_ids
                        .iter()
                        .all(|value| allowed.contains(value))
                })
        });
        if !covered {
            return Err(Status::permission_denied(
                "role scope does not cover the complete resource tuple",
            ));
        }
        Ok(principal)
    }

    pub fn allows_infrastructure_read(
        &self,
        principal: &AuthorizedPrincipal,
        node_id: &str,
    ) -> bool {
        self.has_node_grant(
            principal,
            &[PolicyRole::Admin, PolicyRole::View, PolicyRole::Infra],
            &[PolicyRole::Status, PolicyRole::Infra],
            Some(node_id),
        )
    }

    pub fn authorize_infrastructure_read<T>(
        &self,
        request: &mut Request<T>,
        node_id: Option<&str>,
    ) -> Result<AuthorizedPrincipal, Status> {
        let principal = self.peer(request)?;
        if !self.has_node_grant(
            &principal,
            &[PolicyRole::Admin, PolicyRole::View, PolicyRole::Infra],
            &[PolicyRole::Status, PolicyRole::Infra],
            node_id,
        ) {
            return Err(Status::permission_denied(
                "infrastructure read scope does not cover the requested node",
            ));
        }
        request.extensions_mut().insert(principal.clone());
        Ok(principal)
    }

    pub fn authorize_infrastructure_write<T>(
        &self,
        request: &mut Request<T>,
        node_id: &str,
    ) -> Result<AuthorizedPrincipal, Status> {
        let principal = self.peer(request)?;
        if !self.has_node_grant(
            &principal,
            &[PolicyRole::Admin, PolicyRole::Infra],
            &[PolicyRole::Infra],
            Some(node_id),
        ) {
            return Err(Status::permission_denied(
                "infrastructure write scope does not cover the requested node",
            ));
        }
        request.extensions_mut().insert(principal.clone());
        Ok(principal)
    }

    pub fn authorize_read<T>(
        &self,
        request: &mut Request<T>,
    ) -> Result<AuthorizedPrincipal, Status> {
        let principal = self.peer(request)?;
        if !self.has_role(
            &principal,
            &[
                PolicyRole::Admin,
                PolicyRole::View,
                PolicyRole::Infra,
                PolicyRole::Status,
                PolicyRole::Deploy,
            ],
        ) {
            return Err(Status::permission_denied("read role is required"));
        }
        request.extensions_mut().insert(principal.clone());
        Ok(principal)
    }

    pub fn authorize_operator<T>(
        &self,
        request: &mut Request<T>,
    ) -> Result<AuthorizedPrincipal, Status> {
        let principal = self.peer(request)?;
        if !self.has_role(
            &principal,
            &[PolicyRole::Admin, PolicyRole::Infra, PolicyRole::Deploy],
        ) {
            return Err(Status::permission_denied(
                "infrastructure write role is required",
            ));
        }
        request.extensions_mut().insert(principal.clone());
        Ok(principal)
    }

    pub fn authorize_enrolled<T>(
        &self,
        request: &mut Request<T>,
    ) -> Result<AuthorizedPrincipal, Status> {
        let principal = self.peer(request)?;
        let human_read = matches!(
            principal.kind,
            PrincipalKind::Human | PrincipalKind::ServiceAccount
        ) && self.has_role(
            &principal,
            &[
                PolicyRole::Admin,
                PolicyRole::View,
                PolicyRole::Infra,
                PolicyRole::Status,
                PolicyRole::Deploy,
            ],
        );
        if !human_read
            && !matches!(
                principal.kind,
                PrincipalKind::Manager | PrincipalKind::Proxy | PrincipalKind::NodeAgent
            )
        {
            return Err(Status::permission_denied(
                "policy status role or workload enrollment is required",
            ));
        }
        request.extensions_mut().insert(principal.clone());
        Ok(principal)
    }

    pub fn authorize_proxy<T>(
        &self,
        request: &mut Request<T>,
    ) -> Result<AuthorizedPrincipal, Status> {
        let principal = self.peer(request)?;
        if principal.kind != PrincipalKind::Proxy || principal.node_id.is_none() {
            return Err(Status::permission_denied(
                "enrolled proxy identity is required",
            ));
        }
        request.extensions_mut().insert(principal.clone());
        Ok(principal)
    }

    pub fn authorize_agent<T>(
        &self,
        request: &mut Request<T>,
        requested_node_id: &str,
    ) -> Result<AuthorizedPrincipal, Status> {
        let principal = self.peer(request)?;
        if principal.kind != PrincipalKind::NodeAgent
            || principal.node_id.as_deref() != Some(requested_node_id)
        {
            return Err(Status::permission_denied(
                "node-agent principal is not bound to the requested node",
            ));
        }
        request.extensions_mut().insert(principal.clone());
        Ok(principal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use prost_types::FileDescriptorSet;

    fn strict_policy() -> PrincipalPolicy {
        let policy = br#"schema_version=1
generation=1
cluster_id="cluster-a"
revoked_leaf_fingerprints=["sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
principals=[
 {uri="urn:wruntime:cluster-a:human:admin",kind="human"},
 {uri="urn:wruntime:cluster-a:service-account:jobs",kind="service-account"},
 {uri="urn:wruntime:cluster-a:proxy:proxy-a",kind="proxy"},
 {uri="urn:wruntime:cluster-a:manager:manager-a",kind="manager"}
]
assignments=[
 {principal="urn:wruntime:cluster-a:human:admin",role="admin",scope={}},
 {principal="urn:wruntime:cluster-a:service-account:jobs",role="job-admin",scope={job_queue_ids=["queue-a"]}}
]
manager_enrollments=[{principal="urn:wruntime:cluster-a:manager:manager-a",manager_id="manager-a",endpoint="https://manager-a:9000"}]
proxy_enrollments=[{principal="urn:wruntime:cluster-a:proxy:proxy-a",node_id="node-a"}]
node_agent_enrollments=[]
"#;
        PrincipalPolicy::new(Arc::new(ValidatedPolicy::load(policy).unwrap()))
    }

    fn evidence(uri: &str, fingerprint_digit: char) -> wr_common::tls::LeafEvidence {
        wr_common::tls::LeafEvidence {
            profile: wr_common::tls::LeafProfile::Client,
            principal: Some(wr_common::identity::PrincipalUri::parse(uri).unwrap()),
            endpoint_dns_names: Vec::new(),
            endpoint_ip_addresses: Vec::new(),
            fingerprint: format!("sha256:{}", fingerprint_digit.to_string().repeat(64)),
            serial: "01".into(),
            spki_fingerprint: format!("sha256:{}", "f".repeat(64)),
        }
    }

    #[test]
    fn manager_descriptor_and_row_sets_are_exact() {
        let descriptor =
            FileDescriptorSet::decode(wr_common::wruntime::FILE_DESCRIPTOR_SET).unwrap();
        let mut descriptor_methods = BTreeSet::new();
        let mut present_services = BTreeSet::new();
        for file in descriptor.file {
            let package = file.package.unwrap_or_default();
            for service in file.service {
                let full_name = format!("{package}.{}", service.name.as_deref().unwrap());
                present_services.insert(full_name.clone());
                if MANAGER_SERVICE_SET.contains(&full_name.as_str()) {
                    for method in service.method {
                        descriptor_methods.insert((full_name.clone(), method.name.unwrap()));
                    }
                }
            }
        }
        let row_methods = MANAGER_RPC_ROWS
            .iter()
            .map(|row| (row.service.to_string(), row.method.to_string()))
            .collect::<BTreeSet<_>>();
        assert_eq!(row_methods.len(), MANAGER_RPC_ROWS.len());
        assert_eq!(descriptor_methods, row_methods);
        assert!(MANAGER_SERVICE_SET
            .iter()
            .all(|service| present_services.contains(*service)));
        assert!(NON_MANAGER_SERVICE_SET
            .iter()
            .all(|service| present_services.contains(*service)));
    }

    #[test]
    fn rollout_read_accepts_declared_open_admission_roles_and_rejects_write_only_roles() {
        let policy = PrincipalPolicy::new(Arc::new(
            ValidatedPolicy::load(
                br#"schema_version=1
generation=1
cluster_id="cluster-a"
revoked_leaf_fingerprints=[]
principals=[
 {uri="urn:wruntime:cluster-a:human:viewer",kind="human"},
 {uri="urn:wruntime:cluster-a:service-account:status",kind="service-account"},
 {uri="urn:wruntime:cluster-a:service-account:jobs",kind="service-account"}
]
assignments=[
 {principal="urn:wruntime:cluster-a:human:viewer",role="view",scope={}},
 {principal="urn:wruntime:cluster-a:service-account:status",role="status",scope={}},
 {principal="urn:wruntime:cluster-a:service-account:jobs",role="job-admin",scope={}}
]
manager_enrollments=[]
proxy_enrollments=[]
node_agent_enrollments=[]
"#,
            )
            .unwrap(),
        ));
        for principal in [
            "urn:wruntime:cluster-a:human:viewer",
            "urn:wruntime:cluster-a:service-account:status",
        ] {
            policy
                .authorize_row_evidence(
                    "wruntime.InfrastructureService",
                    "GetManagerRollout",
                    &evidence(principal, 'b'),
                    &AuthorizationResource::default(),
                )
                .unwrap();
        }
        assert_eq!(
            policy
                .authorize_row_evidence(
                    "wruntime.InfrastructureService",
                    "GetManagerRollout",
                    &evidence("urn:wruntime:cluster-a:service-account:jobs", 'b'),
                    &AuthorizationResource::default(),
                )
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
    }

    #[test]
    fn every_descriptor_has_one_typed_handler_binding_and_retry_is_not_workload_bound() {
        let bindings = MANAGER_RPC_ROWS
            .iter()
            .map(|row| {
                (
                    (row.service, row.method),
                    handler_adapter_binding(row.service, row.method).unwrap(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(bindings.len(), MANAGER_RPC_ROWS.len());
        let retry = bindings.get(&("wruntime.JobService", "RetryJob")).unwrap();
        assert_eq!(retry.extractor, RequestExtractorClass::JobQueue);
        assert_eq!(retry.lookup, AuthoritativeLookupClass::JobDelegate);
        assert_eq!(retry.workload, WorkloadBinding::NotApplicable);
        assert!(handler_adapter_binding("wruntime.Unknown", "Missing").is_none());
    }

    #[test]
    fn strict_entry_denies_before_lookup_and_filters_before_serialization() {
        let policy = strict_policy();
        let authorized = evidence("urn:wruntime:cluster-a:service-account:jobs", 'b');
        let denied = evidence("urn:wruntime:cluster-a:service-account:jobs", 'a');
        let mut trace = Vec::new();
        let enter = |evidence: &wr_common::tls::LeafEvidence,
                     queue: &str,
                     trace: &mut Vec<&'static str>| {
            trace.push("extract");
            policy.authorize_row_evidence(
                "wruntime.JobService",
                "RetryJob",
                evidence,
                &AuthorizationResource {
                    job_queue_id: Some(queue),
                    ..Default::default()
                },
            )?;
            trace.push("lookup");
            Ok::<_, Status>(())
        };
        assert_eq!(
            enter(&denied, "queue-a", &mut trace).unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(trace, ["extract"]);
        trace.clear();
        assert_eq!(
            enter(&authorized, "queue-b", &mut trace)
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(trace, ["extract"]);
        trace.clear();
        enter(&authorized, "queue-a", &mut trace).unwrap();
        trace.extend(["filter", "serialize"]);
        assert_eq!(trace, ["extract", "lookup", "filter", "serialize"]);
    }

    #[test]
    fn strict_entry_distinguishes_unauthenticated_and_bound_workload_denials() {
        let policy = strict_policy();
        let mut server_leaf = evidence("urn:wruntime:cluster-a:human:admin", 'b');
        server_leaf.profile = wr_common::tls::LeafProfile::Server;
        assert_eq!(
            policy
                .authorize_row_evidence(
                    "wruntime.ClusterService",
                    "ListManagers",
                    &server_leaf,
                    &AuthorizationResource::default()
                )
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );

        let proxy = evidence("urn:wruntime:cluster-a:proxy:proxy-a", 'b');
        for method in ["ListManagers", "GetRoutingTable"] {
            assert!(policy
                .authorize_row_evidence(
                    "wruntime.ClusterService",
                    method,
                    &proxy,
                    &AuthorizationResource::default()
                )
                .is_ok());
        }
        assert_eq!(
            policy
                .authorize_row_evidence(
                    "wruntime.NodeService",
                    "RegisterEngine",
                    &proxy,
                    &AuthorizationResource {
                        node_id: Some("node-b"),
                        ..Default::default()
                    }
                )
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
    }

    #[test]
    fn mount_ready_facades_cover_representatives_from_all_six_services() {
        let policy = strict_policy();
        let admin = evidence("urn:wruntime:cluster-a:human:admin", 'b');
        let proxy = evidence("urn:wruntime:cluster-a:proxy:proxy-a", 'b');
        for (service, method, evidence, resource) in [
            (
                "wruntime.ClusterService",
                "ListManagers",
                &admin,
                AuthorizationResource::default(),
            ),
            (
                "wruntime.InfrastructureService",
                "GetStatus",
                &admin,
                AuthorizationResource {
                    node_id: Some("node-a"),
                    ..Default::default()
                },
            ),
            (
                "wruntime.NodeService",
                "RegisterEngine",
                &proxy,
                AuthorizationResource {
                    node_id: Some("node-a"),
                    ..Default::default()
                },
            ),
            (
                "wruntime.PolicyService",
                "GetPolicyStatus",
                &admin,
                AuthorizationResource::default(),
            ),
            (
                "wruntime.LifecycleService",
                "GetStatus",
                &admin,
                AuthorizationResource::default(),
            ),
        ] {
            let facade =
                crate::service::AuthAwareServiceFacade::new(service, policy.clone()).unwrap();
            facade.authorize(method, evidence, &resource).unwrap();
        }
        let jobs = crate::job_admin::JobServiceAuthorizationFacade::new(policy);
        jobs.authorize_queue(
            "RetryJob",
            "queue-a",
            &evidence("urn:wruntime:cluster-a:service-account:jobs", 'b'),
        )
        .unwrap();
    }

    #[test]
    fn assignments_union_whole_tuples_without_cross_product() {
        let grants = vec![
            ResourceAssignment::default()
                .scoped("node", ["node-a".into()])
                .scoped("namespace", ["store".into()]),
            ResourceAssignment::default()
                .scoped("node", ["node-b".into()])
                .scoped("namespace", ["billing".into()]),
        ];
        let tuple = |node: &str, namespace: &str| {
            BTreeMap::from([
                ("node".into(), node.into()),
                ("namespace".into(), namespace.into()),
            ])
        };
        assert!(assignment_union_covers(&grants, &tuple("node-a", "store")));
        assert!(assignment_union_covers(
            &grants,
            &tuple("node-b", "billing")
        ));
        assert!(!assignment_union_covers(
            &grants,
            &tuple("node-a", "billing")
        ));
    }

    #[test]
    fn workload_enrollment_is_a_mandatory_bound_or_global_conjunct() {
        let proxy = EnrolledWorkload {
            kind: WorkloadKind::Proxy,
            node_id: Some("node-a"),
        };
        assert!(workload_binding_matches(
            WorkloadBinding::ProxyBound,
            Some(proxy),
            Some("node-a")
        ));
        assert!(!workload_binding_matches(
            WorkloadBinding::ProxyBound,
            Some(proxy),
            Some("node-b")
        ));
        assert!(!workload_binding_matches(
            WorkloadBinding::ProxyEnrolledGlobal,
            None,
            None
        ));
        assert!(workload_binding_matches(
            WorkloadBinding::ProxyEnrolledGlobal,
            Some(proxy),
            None
        ));
    }

    #[test]
    fn explicit_unscoped_authority_is_assignment_local() {
        let grants = vec![
            ResourceAssignment::default()
                .scoped("node", ["node-a".into()])
                .unscoped("namespace"),
            ResourceAssignment::default()
                .scoped("node", ["node-b".into()])
                .scoped("namespace", ["billing".into()]),
        ];
        let tuple = BTreeMap::from([
            ("node".into(), "node-b".into()),
            ("namespace".into(), "store".into()),
        ]);
        assert!(!assignment_union_covers(&grants, &tuple));
    }
}
