//! Typed facade for workflows that use more than one manager domain service
//! over a single channel. It adds no wire service: every call uses its domain
//! RPC path.

use std::ops::{Deref, DerefMut};
use std::time::Duration;
use tonic::codegen::{Body, Bytes, StdError};

use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

use crate::lifecycle_observation::validate_lifecycle_status;
use crate::wruntime::*;
use crate::wruntime::{
    cluster_service_client::ClusterServiceClient,
    infrastructure_service_client::InfrastructureServiceClient,
    job_service_client::JobServiceClient, lifecycle_service_client::LifecycleServiceClient,
    node_service_client::NodeServiceClient, policy_service_client::PolicyServiceClient,
};

/// Declared retry boundary for a manager workflow. Callers own one pinned
/// epoch and may replace it only as described by this class.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryClass {
    ReadOnly,
    DurableCreate,
    FreshRegistration,
    NoReplayMutation,
}

impl RetryClass {
    fn permits_unknown_result_repin(self) -> bool {
        matches!(
            self,
            Self::ReadOnly | Self::DurableCreate | Self::FreshRegistration
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagerCandidate {
    pub endpoint: String,
    pub server_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EpochIdentity {
    pub manager_id: String,
    pub policy_generation: u64,
    pub policy_digest: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EpochObservation {
    pub manager_id: String,
    pub process_instance_id: String,
    pub process_ready: bool,
    pub policy_generation: u64,
    pub policy_digest: String,
    pub privileged_admission: i32,
    pub rollout_id: String,
    pub rollout_phase: i32,
    pub rollout_expected_set_hash: String,
    pub rollout_lease_epoch: u64,
}

impl EpochObservation {
    pub fn require_identity(&self, expected: &EpochIdentity) -> Result<(), tonic::Status> {
        if expected.manager_id.is_empty()
            || expected.policy_generation == 0
            || expected.policy_digest.is_empty()
        {
            return Err(tonic::Status::invalid_argument(
                "expected manager epoch identity is incomplete",
            ));
        }
        if self.manager_id == expected.manager_id
            && self.policy_generation == expected.policy_generation
            && self.policy_digest == expected.policy_digest
        {
            Ok(())
        } else {
            Err(tonic::Status::failed_precondition(
                "pinned manager epoch identity is stale or mismatched",
            ))
        }
    }

    pub fn from_lifecycle(status: &LifecycleStatus) -> Result<Self, tonic::Status> {
        let validated = validate_lifecycle_status(status)
            .map_err(|error| tonic::Status::failed_precondition(error.to_string()))?;
        if validated.service_kind != ServiceKind::Manager {
            return Err(tonic::Status::failed_precondition(
                "pinned lifecycle endpoint is not a manager",
            ));
        }
        if status.manager_id.is_empty()
            || status.policy_generation == 0
            || status.policy_digest.is_empty()
        {
            return Err(tonic::Status::failed_precondition(
                "manager lifecycle observation omitted epoch identity",
            ));
        }
        let admission = PrivilegedAdmissionState::try_from(status.privileged_admission)
            .map_err(|_| tonic::Status::failed_precondition("unknown manager admission state"))?;
        if admission == PrivilegedAdmissionState::Unspecified {
            return Err(tonic::Status::failed_precondition(
                "manager lifecycle observation omitted admission state",
            ));
        }
        let phase = ManagerRolloutPhase::try_from(status.rollout_phase)
            .map_err(|_| tonic::Status::failed_precondition("unknown manager rollout phase"))?;
        if status.rollout_operation_id.is_empty() {
            if phase != ManagerRolloutPhase::Unspecified
                || !status.rollout_expected_set_hash.is_empty()
                || status.last_observed_rollout_lease_epoch != 0
            {
                return Err(tonic::Status::failed_precondition(
                    "manager lifecycle observation has rollout fields without an operation",
                ));
            }
        } else if phase == ManagerRolloutPhase::Unspecified
            || status.rollout_expected_set_hash.is_empty()
        {
            return Err(tonic::Status::failed_precondition(
                "manager lifecycle observation omitted active rollout fields",
            ));
        }

        Ok(Self {
            manager_id: status.manager_id.clone(),
            process_instance_id: validated.process_instance_id.to_owned(),
            process_ready: status.process_ready,
            policy_generation: status.policy_generation,
            policy_digest: status.policy_digest.clone(),
            privileged_admission: status.privileged_admission,
            rollout_id: status.rollout_operation_id.clone(),
            rollout_phase: status.rollout_phase,
            rollout_expected_set_hash: status.rollout_expected_set_hash.clone(),
            rollout_lease_epoch: status.last_observed_rollout_lease_epoch,
        })
    }
}

/// One immutable transport selection for a typed workflow.
#[derive(Debug, Clone)]
pub struct ManagerEpoch {
    endpoint: String,
    server_name: String,
    trust_roots_path: String,
    client_identity_path: String,
    retry_class: RetryClass,
    observation: EpochObservation,
    provider: Box<ManagerEpochProvider>,
    client: ManagerClient<Channel>,
}

impl Deref for ManagerEpoch {
    type Target = ManagerClient<Channel>;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl DerefMut for ManagerEpoch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}

impl ManagerEpoch {
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
    pub fn server_name(&self) -> &str {
        &self.server_name
    }
    pub fn trust_roots_path(&self) -> &str {
        &self.trust_roots_path
    }
    pub fn client_identity_path(&self) -> &str {
        &self.client_identity_path
    }
    pub fn retry_class(&self) -> RetryClass {
        self.retry_class
    }
    pub fn observation(&self) -> &EpochObservation {
        &self.observation
    }
    pub fn require_observation(&self, expected: &EpochObservation) -> Result<(), tonic::Status> {
        require_matching_observation(&self.observation, expected)
    }

    /// Whether an unknown post-dispatch outcome permits exact replay on a
    /// newly pinned epoch. No-replay mutations are always denied.
    pub fn permits_unknown_result_repin(&self) -> bool {
        self.retry_class.permits_unknown_result_repin()
    }

    pub async fn repin(&self) -> Result<Self, tonic::Status> {
        self.provider.repin(self).await
    }

    pub async fn register_proxy_exact(
        &mut self,
        request: RegisterProxyRequest,
    ) -> Result<tonic::Response<RegisterProxyResponse>, tonic::Status> {
        if self.retry_class != RetryClass::FreshRegistration {
            return Err(tonic::Status::failed_precondition(
                "proxy registration requires FreshRegistration retry semantics",
            ));
        }
        self.client.register_proxy(request).await
    }

    pub async fn report_proxy_inventory_exact(
        &mut self,
        request: ReportProxyInventoryRequest,
    ) -> Result<tonic::Response<ReportProxyInventoryResponse>, tonic::Status> {
        if self.retry_class != RetryClass::FreshRegistration {
            return Err(tonic::Status::failed_precondition(
                "proxy full-state reporting requires FreshRegistration retry semantics",
            ));
        }
        self.client.report_proxy_inventory(request).await
    }

    pub async fn deregister_proxy_once(
        &mut self,
        request: DeregisterProxyRequest,
    ) -> Result<tonic::Response<DeregisterProxyResponse>, tonic::Status> {
        if self.retry_class != RetryClass::NoReplayMutation {
            return Err(tonic::Status::failed_precondition(
                "proxy deregistration requires NoReplayMutation semantics",
            ));
        }
        self.client.deregister_proxy(request).await
    }
}

fn require_matching_observation(
    observed: &EpochObservation,
    expected: &EpochObservation,
) -> Result<(), tonic::Status> {
    let complete = |observation: &EpochObservation| {
        !observation.manager_id.is_empty()
            && !observation.process_instance_id.is_empty()
            && observation.policy_generation > 0
            && !observation.policy_digest.is_empty()
            && observation.privileged_admission != PrivilegedAdmissionState::Unspecified as i32
    };
    if complete(observed) && complete(expected) && observed == expected {
        Ok(())
    } else {
        Err(tonic::Status::failed_precondition(
            "pinned manager epoch observation is stale or mismatched",
        ))
    }
}

/// Sole constructor for authenticated manager channels. Candidate order is
/// stable; pinning tries it exactly once from first to last.
#[derive(Clone, Debug)]
pub struct ManagerEpochProvider {
    candidates: Vec<ManagerCandidate>,
    tls: ClientTlsConfig,
    trust_roots_path: String,
    client_identity_path: String,
}

impl ManagerEpochProvider {
    pub fn new(
        candidates: Vec<ManagerCandidate>,
        tls: ClientTlsConfig,
        trust_roots_path: impl Into<String>,
        client_identity_path: impl Into<String>,
    ) -> Result<Self, tonic::Status> {
        if candidates.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "manager candidate set must not be empty",
            ));
        }
        if candidates
            .iter()
            .any(|candidate| candidate.endpoint.is_empty() || candidate.server_name.is_empty())
        {
            return Err(tonic::Status::invalid_argument(
                "manager candidates require endpoint and server name",
            ));
        }
        let trust_roots_path = trust_roots_path.into();
        let client_identity_path = client_identity_path.into();
        if trust_roots_path.is_empty() || client_identity_path.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "manager epochs require explicit trust-root and client-identity metadata",
            ));
        }
        Ok(Self {
            candidates,
            tls,
            trust_roots_path,
            client_identity_path,
        })
    }

    pub fn candidates(&self) -> &[ManagerCandidate] {
        &self.candidates
    }

    pub async fn pin(&self, retry_class: RetryClass) -> Result<ManagerEpoch, tonic::Status> {
        let mut last_error = None;
        for candidate in &self.candidates {
            let endpoint = match Endpoint::from_shared(candidate.endpoint.clone()) {
                Ok(endpoint) => endpoint,
                Err(error) => {
                    last_error = Some(error.to_string());
                    continue;
                }
            };
            let tls = self.tls.clone().domain_name(candidate.server_name.clone());
            let endpoint = match endpoint
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(10))
                .tls_config(tls)
            {
                Ok(endpoint) => endpoint,
                Err(error) => {
                    last_error = Some(error.to_string());
                    continue;
                }
            };
            match endpoint.connect().await {
                Ok(channel) => {
                    let mut client = ManagerClient::new(channel);
                    let status = match client
                        .get_lifecycle_status(GetLifecycleStatusRequest {})
                        .await
                    {
                        Ok(response) => match response.into_inner().status {
                            Some(status) => status,
                            None => {
                                last_error =
                                    Some("manager lifecycle response omitted status".to_owned());
                                continue;
                            }
                        },
                        Err(error) => {
                            last_error = Some(error.to_string());
                            continue;
                        }
                    };
                    let observation = match EpochObservation::from_lifecycle(&status) {
                        Ok(observation) => observation,
                        Err(error) => {
                            last_error = Some(error.to_string());
                            continue;
                        }
                    };
                    return Ok(ManagerEpoch {
                        endpoint: candidate.endpoint.clone(),
                        server_name: candidate.server_name.clone(),
                        trust_roots_path: self.trust_roots_path.clone(),
                        client_identity_path: self.client_identity_path.clone(),
                        retry_class,
                        observation,
                        provider: Box::new(self.clone()),
                        client,
                    });
                }
                Err(error) => last_error = Some(error.to_string()),
            }
        }
        Err(tonic::Status::unavailable(format!(
            "all manager candidates were unreachable: {}",
            last_error.unwrap_or_else(|| "no connection attempt completed".into())
        )))
    }

    fn candidates_after(&self, endpoint: &str) -> Vec<ManagerCandidate> {
        let mut candidates = self.candidates.clone();
        if let Some(index) = candidates
            .iter()
            .position(|candidate| candidate.endpoint == endpoint)
        {
            let rotation = (index + 1) % candidates.len();
            candidates.rotate_left(rotation);
        }
        candidates
    }

    pub async fn repin(&self, previous: &ManagerEpoch) -> Result<ManagerEpoch, tonic::Status> {
        if !previous.permits_unknown_result_repin() {
            return Err(tonic::Status::failed_precondition(
                "workflow retry class forbids repinning after dispatch",
            ));
        }
        Self::new(
            self.candidates_after(previous.endpoint()),
            self.tls.clone(),
            self.trust_roots_path.clone(),
            self.client_identity_path.clone(),
        )?
        .pin(previous.retry_class)
        .await
    }
}

#[derive(Debug, Clone)]
pub struct ManagerClient<T> {
    cluster: ClusterServiceClient<T>,
    infrastructure: InfrastructureServiceClient<T>,
    node: NodeServiceClient<T>,
    job: JobServiceClient<T>,
    policy: PolicyServiceClient<T>,
    lifecycle: LifecycleServiceClient<T>,
}

impl<T> ManagerClient<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + Clone,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    pub(crate) fn new(inner: T) -> Self {
        Self::from_test_channels(inner.clone(), inner)
    }

    /// Test-harness constructor for representing identity-specific transports
    /// to the same authenticated production listener. Runtime callers must use
    /// [`ManagerEpochProvider`].
    #[doc(hidden)]
    pub fn from_test_channels(manager: T, node: T) -> Self {
        Self {
            cluster: ClusterServiceClient::new(manager.clone()),
            infrastructure: InfrastructureServiceClient::new(manager.clone()),
            node: NodeServiceClient::new(node),
            job: JobServiceClient::new(manager.clone()),
            policy: PolicyServiceClient::new(manager.clone()),
            lifecycle: LifecycleServiceClient::new(manager),
        }
    }

    pub async fn register_proxy(
        &mut self,
        request: impl tonic::IntoRequest<RegisterProxyRequest>,
    ) -> Result<tonic::Response<RegisterProxyResponse>, tonic::Status> {
        self.node.register_proxy(request).await
    }
    pub async fn report_proxy_inventory(
        &mut self,
        request: impl tonic::IntoRequest<ReportProxyInventoryRequest>,
    ) -> Result<tonic::Response<ReportProxyInventoryResponse>, tonic::Status> {
        self.node.report_proxy_inventory(request).await
    }
    pub async fn deregister_proxy(
        &mut self,
        request: impl tonic::IntoRequest<DeregisterProxyRequest>,
    ) -> Result<tonic::Response<DeregisterProxyResponse>, tonic::Status> {
        self.node.deregister_proxy(request).await
    }
    pub async fn register_engine(
        &mut self,
        request: impl tonic::IntoRequest<RegisterEngineRequest>,
    ) -> Result<tonic::Response<RegisterEngineResponse>, tonic::Status> {
        self.node.register_engine(request).await
    }
    pub async fn deregister_engine(
        &mut self,
        request: impl tonic::IntoRequest<DeregisterEngineRequest>,
    ) -> Result<tonic::Response<DeregisterEngineResponse>, tonic::Status> {
        self.node.deregister_engine(request).await
    }
    pub async fn heartbeat(
        &mut self,
        request: impl tonic::IntoRequest<HeartbeatRequest>,
    ) -> Result<tonic::Response<HeartbeatResponse>, tonic::Status> {
        self.node.heartbeat(request).await
    }
    pub async fn begin_engine_drain(
        &mut self,
        request: impl tonic::IntoRequest<BeginEngineDrainRequest>,
    ) -> Result<tonic::Response<BeginEngineDrainResponse>, tonic::Status> {
        self.node.begin_engine_drain(request).await
    }
    pub async fn attest(
        &mut self,
        request: impl tonic::IntoRequest<AttestNodeAgentRequest>,
    ) -> Result<tonic::Response<AttestNodeAgentResponse>, tonic::Status> {
        self.node.attest(request).await
    }
    pub async fn get_workload_snapshot(
        &mut self,
        request: impl tonic::IntoRequest<crate::wruntime::GetWorkloadSnapshotRequest>,
    ) -> Result<tonic::Response<crate::wruntime::GetWorkloadSnapshotResponse>, tonic::Status> {
        self.policy.get_workload_snapshot(request).await
    }
    pub async fn list_engines(
        &mut self,
        request: impl tonic::IntoRequest<ListEnginesRequest>,
    ) -> Result<tonic::Response<ListEnginesResponse>, tonic::Status> {
        self.cluster.list_engines(request).await
    }
    pub async fn get_routing_table(
        &mut self,
        request: impl tonic::IntoRequest<GetRoutingTableRequest>,
    ) -> Result<tonic::Response<GetRoutingTableResponse>, tonic::Status> {
        self.cluster.get_routing_table(request).await
    }
    pub async fn upsert_routing_rule(
        &mut self,
        request: impl tonic::IntoRequest<RoutingRule>,
    ) -> Result<tonic::Response<UpsertRoutingRuleResponse>, tonic::Status> {
        self.cluster.upsert_routing_rule(request).await
    }
    pub async fn delete_routing_rule(
        &mut self,
        request: impl tonic::IntoRequest<DeleteRoutingRuleRequest>,
    ) -> Result<tonic::Response<DeleteRoutingRuleResponse>, tonic::Status> {
        self.cluster.delete_routing_rule(request).await
    }
    pub async fn list_managers(
        &mut self,
        request: impl tonic::IntoRequest<ListManagersRequest>,
    ) -> Result<tonic::Response<ListManagersResponse>, tonic::Status> {
        self.cluster.list_managers(request).await
    }
    pub async fn get_cluster_status(
        &mut self,
        request: impl tonic::IntoRequest<GetClusterStatusRequest>,
    ) -> Result<tonic::Response<GetClusterStatusResponse>, tonic::Status> {
        self.cluster.get_cluster_status(request).await
    }
    pub async fn get_schema(
        &mut self,
        request: impl tonic::IntoRequest<GetSchemaRequest>,
    ) -> Result<tonic::Response<GetSchemaResponse>, tonic::Status> {
        self.cluster.get_schema(request).await
    }
    pub async fn set_secret(
        &mut self,
        request: impl tonic::IntoRequest<SetSecretRequest>,
    ) -> Result<tonic::Response<SetSecretResponse>, tonic::Status> {
        self.cluster.set_secret(request).await
    }
    pub async fn delete_secret(
        &mut self,
        request: impl tonic::IntoRequest<DeleteSecretRequest>,
    ) -> Result<tonic::Response<DeleteSecretResponse>, tonic::Status> {
        self.cluster.delete_secret(request).await
    }
    pub async fn list_secrets(
        &mut self,
        request: impl tonic::IntoRequest<ListSecretsRequest>,
    ) -> Result<tonic::Response<ListSecretsResponse>, tonic::Status> {
        self.cluster.list_secrets(request).await
    }
    pub async fn upsert_schedule(
        &mut self,
        request: impl tonic::IntoRequest<UpsertScheduleRequest>,
    ) -> Result<tonic::Response<UpsertScheduleResponse>, tonic::Status> {
        self.cluster.upsert_schedule(request).await
    }
    pub async fn delete_schedule(
        &mut self,
        request: impl tonic::IntoRequest<DeleteScheduleRequest>,
    ) -> Result<tonic::Response<DeleteScheduleResponse>, tonic::Status> {
        self.cluster.delete_schedule(request).await
    }
    pub async fn list_schedules(
        &mut self,
        request: impl tonic::IntoRequest<ListSchedulesRequest>,
    ) -> Result<tonic::Response<ListSchedulesResponse>, tonic::Status> {
        self.cluster.list_schedules(request).await
    }
    pub async fn get_status(
        &mut self,
        request: impl tonic::IntoRequest<GetOperatorStatusRequest>,
    ) -> Result<tonic::Response<GetOperatorStatusResponse>, tonic::Status> {
        self.infrastructure.get_status(request).await
    }
    pub async fn submit_operation(
        &mut self,
        request: impl tonic::IntoRequest<SubmitOperationRequest>,
    ) -> Result<tonic::Response<SubmitOperationResponse>, tonic::Status> {
        self.infrastructure.submit_operation(request).await
    }
    pub async fn get_operation(
        &mut self,
        request: impl tonic::IntoRequest<GetOperationRequest>,
    ) -> Result<tonic::Response<GetOperationResponse>, tonic::Status> {
        self.infrastructure.get_operation(request).await
    }
    pub async fn list_operations(
        &mut self,
        request: impl tonic::IntoRequest<ListOperationsRequest>,
    ) -> Result<tonic::Response<ListOperationsResponse>, tonic::Status> {
        self.infrastructure.list_operations(request).await
    }
    pub async fn resume_operation(
        &mut self,
        request: impl tonic::IntoRequest<ResumeOperationRequest>,
    ) -> Result<tonic::Response<ResumeOperationResponse>, tonic::Status> {
        self.infrastructure.resume_operation(request).await
    }
    pub async fn cancel_operation(
        &mut self,
        request: impl tonic::IntoRequest<CancelOperationRequest>,
    ) -> Result<tonic::Response<CancelOperationResponse>, tonic::Status> {
        self.infrastructure.cancel_operation(request).await
    }
    pub async fn begin_manager_rollout(
        &mut self,
        request: impl tonic::IntoRequest<BeginManagerRolloutRequest>,
    ) -> Result<tonic::Response<BeginManagerRolloutResponse>, tonic::Status> {
        self.infrastructure.begin_manager_rollout(request).await
    }
    pub async fn lease_manager_rollout(
        &mut self,
        request: impl tonic::IntoRequest<LeaseManagerRolloutRequest>,
    ) -> Result<tonic::Response<LeaseManagerRolloutResponse>, tonic::Status> {
        self.infrastructure.lease_manager_rollout(request).await
    }
    pub async fn advance_manager_rollout(
        &mut self,
        request: impl tonic::IntoRequest<AdvanceManagerRolloutRequest>,
    ) -> Result<tonic::Response<AdvanceManagerRolloutResponse>, tonic::Status> {
        self.infrastructure.advance_manager_rollout(request).await
    }
    pub async fn get_manager_rollout(
        &mut self,
        request: impl tonic::IntoRequest<GetManagerRolloutRequest>,
    ) -> Result<tonic::Response<GetManagerRolloutResponse>, tonic::Status> {
        self.infrastructure.get_manager_rollout(request).await
    }
    pub async fn get_node_cleanup_status(
        &mut self,
        request: impl tonic::IntoRequest<GetNodeCleanupStatusRequest>,
    ) -> Result<tonic::Response<GetNodeCleanupStatusResponse>, tonic::Status> {
        self.infrastructure.get_node_cleanup_status(request).await
    }
    pub async fn retry_node_cleanup(
        &mut self,
        request: impl tonic::IntoRequest<RetryNodeCleanupRequest>,
    ) -> Result<tonic::Response<RetryNodeCleanupResponse>, tonic::Status> {
        self.infrastructure.retry_node_cleanup(request).await
    }
    pub async fn claim_operation(
        &mut self,
        request: impl tonic::IntoRequest<ClaimOperationRequest>,
    ) -> Result<tonic::Response<ClaimOperationResponse>, tonic::Status> {
        self.node.claim_operation(request).await
    }
    pub async fn claim_node_cleanup(
        &mut self,
        request: impl tonic::IntoRequest<ClaimNodeCleanupRequest>,
    ) -> Result<tonic::Response<ClaimNodeCleanupResponse>, tonic::Status> {
        self.node.claim_node_cleanup(request).await
    }
    pub async fn renew_node_cleanup_lease(
        &mut self,
        request: impl tonic::IntoRequest<RenewNodeCleanupLeaseRequest>,
    ) -> Result<tonic::Response<RenewNodeCleanupLeaseResponse>, tonic::Status> {
        self.node.renew_node_cleanup_lease(request).await
    }
    pub async fn report_node_cleanup_result(
        &mut self,
        request: impl tonic::IntoRequest<ReportNodeCleanupResultRequest>,
    ) -> Result<tonic::Response<ReportNodeCleanupResultResponse>, tonic::Status> {
        self.node.report_node_cleanup_result(request).await
    }
    pub async fn renew_operation_lease(
        &mut self,
        request: impl tonic::IntoRequest<RenewOperationLeaseRequest>,
    ) -> Result<tonic::Response<RenewOperationLeaseResponse>, tonic::Status> {
        self.node.renew_operation_lease(request).await
    }
    pub async fn report_observation(
        &mut self,
        request: impl tonic::IntoRequest<ReportNodeObservationRequest>,
    ) -> Result<tonic::Response<ReportNodeObservationResponse>, tonic::Status> {
        self.node.report_observation(request).await
    }
    pub async fn report_step_result(
        &mut self,
        request: impl tonic::IntoRequest<ReportStepResultRequest>,
    ) -> Result<tonic::Response<ReportStepResultResponse>, tonic::Status> {
        self.node.report_step_result(request).await
    }

    pub async fn begin_deployment(
        &mut self,
        request: impl tonic::IntoRequest<BeginDeploymentRequest>,
    ) -> Result<tonic::Response<BeginDeploymentResponse>, tonic::Status> {
        self.infrastructure.begin_deployment(request).await
    }
    pub async fn verify_deployment(
        &mut self,
        request: impl tonic::IntoRequest<VerifyDeploymentRequest>,
    ) -> Result<tonic::Response<VerifyDeploymentResponse>, tonic::Status> {
        self.infrastructure.verify_deployment(request).await
    }
    pub async fn finalize_deployment(
        &mut self,
        request: impl tonic::IntoRequest<FinalizeDeploymentRequest>,
    ) -> Result<tonic::Response<FinalizeDeploymentResponse>, tonic::Status> {
        self.infrastructure.finalize_deployment(request).await
    }
    pub async fn abandon_deployment(
        &mut self,
        request: impl tonic::IntoRequest<AbandonDeploymentRequest>,
    ) -> Result<tonic::Response<AbandonDeploymentResponse>, tonic::Status> {
        self.infrastructure.abandon_deployment(request).await
    }
    pub async fn begin_rollback(
        &mut self,
        request: impl tonic::IntoRequest<BeginRollbackRequest>,
    ) -> Result<tonic::Response<BeginRollbackResponse>, tonic::Status> {
        self.infrastructure.begin_rollback(request).await
    }
    pub async fn put_node_agent_policy(
        &mut self,
        request: impl tonic::IntoRequest<PutNodeAgentPolicyRequest>,
    ) -> Result<tonic::Response<PutNodeAgentPolicyResponse>, tonic::Status> {
        self.infrastructure.put_node_agent_policy(request).await
    }
    pub async fn get_lifecycle_status(
        &mut self,
        request: impl tonic::IntoRequest<GetLifecycleStatusRequest>,
    ) -> Result<tonic::Response<GetLifecycleStatusResponse>, tonic::Status> {
        self.lifecycle.get_status(request).await
    }
    pub async fn get_policy_status(
        &mut self,
        request: impl tonic::IntoRequest<GetPolicyStatusRequest>,
    ) -> Result<tonic::Response<GetPolicyStatusResponse>, tonic::Status> {
        self.policy.get_policy_status(request).await
    }
    pub async fn list_job_queues(
        &mut self,
        request: impl tonic::IntoRequest<ListJobQueuesRequest>,
    ) -> Result<tonic::Response<ListJobQueuesResponse>, tonic::Status> {
        self.job.list_job_queues(request).await
    }
    pub async fn list_jobs(
        &mut self,
        request: impl tonic::IntoRequest<ListJobsRequest>,
    ) -> Result<tonic::Response<ListJobsResponse>, tonic::Status> {
        self.job.list_jobs(request).await
    }
    pub async fn get_job_queue_summary(
        &mut self,
        request: impl tonic::IntoRequest<GetJobQueueSummaryRequest>,
    ) -> Result<tonic::Response<GetJobQueueSummaryResponse>, tonic::Status> {
        self.job.get_job_queue_summary(request).await
    }
    pub async fn get_job(
        &mut self,
        request: impl tonic::IntoRequest<GetJobRequest>,
    ) -> Result<tonic::Response<GetJobResponse>, tonic::Status> {
        self.job.get_job(request).await
    }
    pub async fn retry_job(
        &mut self,
        request: impl tonic::IntoRequest<RetryJobRequest>,
    ) -> Result<tonic::Response<RetryJobResponse>, tonic::Status> {
        self.job.retry_job(request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lifecycle() -> LifecycleStatus {
        LifecycleStatus {
            state: ProcessLifecycleState::Ready as i32,
            service_kind: ServiceKind::Manager as i32,
            process_instance_id: "manager-process-1".to_owned(),
            manager_id: "manager-a".to_owned(),
            process_ready: true,
            policy_generation: 7,
            policy_digest: "sha256:policy".to_owned(),
            privileged_admission: PrivilegedAdmissionState::Open as i32,
            ..Default::default()
        }
    }

    #[test]
    fn lifecycle_observation_parses_complete_manager_epoch() {
        let observation = EpochObservation::from_lifecycle(&lifecycle()).unwrap();
        assert_eq!(observation.manager_id, "manager-a");
        assert_eq!(observation.process_instance_id, "manager-process-1");
        assert!(observation.process_ready);
        assert_eq!(observation.policy_generation, 7);
        assert_eq!(
            observation.privileged_admission,
            PrivilegedAdmissionState::Open as i32
        );
    }

    #[test]
    fn lifecycle_observation_rejects_malformed_rollout_and_wrong_service() {
        let mut malformed = lifecycle();
        malformed.rollout_operation_id = "rollout-1".to_owned();
        assert!(EpochObservation::from_lifecycle(&malformed).is_err());

        let mut wrong_service = lifecycle();
        wrong_service.service_kind = ServiceKind::Proxy as i32;
        assert!(EpochObservation::from_lifecycle(&wrong_service).is_err());
    }

    #[test]
    fn stale_or_mismatched_epoch_observation_is_rejected() {
        let observed = EpochObservation::from_lifecycle(&lifecycle()).unwrap();
        assert!(require_matching_observation(&observed, &observed).is_ok());
        let identity = EpochIdentity {
            manager_id: observed.manager_id.clone(),
            policy_generation: observed.policy_generation,
            policy_digest: observed.policy_digest.clone(),
        };
        assert!(observed.require_identity(&identity).is_ok());

        let mut stale = observed.clone();
        stale.policy_generation -= 1;
        assert!(require_matching_observation(&observed, &stale).is_err());
        assert!(stale.require_identity(&identity).is_err());

        let mut wrong_manager = identity.clone();
        wrong_manager.manager_id = "manager-b".to_owned();
        assert!(observed.require_identity(&wrong_manager).is_err());
    }

    #[test]
    fn no_replay_mutation_rejects_unknown_result_repin() {
        assert!(!RetryClass::NoReplayMutation.permits_unknown_result_repin());
        assert!(RetryClass::ReadOnly.permits_unknown_result_repin());
    }

    #[test]
    fn provider_preserves_candidate_order_and_requires_identity_metadata() {
        let candidates = vec![
            ManagerCandidate {
                endpoint: "https://manager-a:9000".to_owned(),
                server_name: "manager-a".to_owned(),
            },
            ManagerCandidate {
                endpoint: "https://manager-b:9000".to_owned(),
                server_name: "manager-b".to_owned(),
            },
        ];
        let provider = ManagerEpochProvider::new(
            candidates.clone(),
            ClientTlsConfig::new(),
            "roots.pem",
            "proxy-client.pem",
        )
        .unwrap();
        assert_eq!(provider.candidates(), candidates);
        assert_eq!(
            provider
                .candidates_after("https://manager-a:9000")
                .iter()
                .map(|candidate| candidate.server_name.as_str())
                .collect::<Vec<_>>(),
            vec!["manager-b", "manager-a"]
        );
        assert!(ManagerEpochProvider::new(
            candidates,
            ClientTlsConfig::new(),
            "",
            "proxy-client.pem"
        )
        .is_err());
    }
}
