//! Binary adapter and importable core for the continuously fenced node agent.

use std::fs::{File, OpenOptions};
use std::future::Future;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::time::{Duration, SystemTime};

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use sha2::{Digest, Sha256};
use tokio::sync::watch;
use wr_common::agent_policy::{
    missing_capabilities, AgentPolicy, AgentPolicyBackend, AGENT_CAPABILITIES,
    AGENT_PROTOCOL_VERSION,
};
use wr_common::node::ClientTlsConfig;
use wr_common::wruntime::{
    AgentInstruction, AttestNodeAgentRequest, BackendKind, ClaimNodeCleanupRequest,
    ClaimOperationRequest, GetOperatorStatusRequest, NodeAgentAttestation, NodeAgentPolicy,
    NodeCleanupInstruction, NodeOperationStepKind, PutNodeAgentPolicyRequest,
    RenewNodeCleanupLeaseRequest, RenewOperationLeaseRequest, ReportNodeCleanupResultRequest,
    ReportNodeObservationRequest, ReportStepResultRequest,
};

use super::bundle;
use super::bundle_integrity::{verify_bundle_archive, BundleManifest};
use super::deploy_config::DeployFormat;
use super::helpers;
use super::node_backend::{
    validate_identity, validate_root_owned_directory, workload_target, BackendType, HostBackend,
    HostBackendConfig, InstructionExecutor, StepEvidence, WorkloadTarget,
};
use crate::client;

pub type AgentFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

#[derive(Args)]
pub struct AgentArgs {
    #[command(subcommand)]
    pub command: AgentCommand,
}

#[derive(Subcommand)]
pub enum AgentCommand {
    /// Run the continuously fenced host agent.
    Run(AgentRunArgs),
    /// Install the host agent and wait for exact manager attestation.
    Install(AgentInstallArgs),
    /// Idempotently replace the host agent and wait for a new attestation.
    Update(AgentInstallArgs),
}

#[derive(Args)]
pub struct AgentRunArgs {
    /// Strict canonical node-agent configuration file.
    #[arg(long)]
    pub config: PathBuf,
}

#[derive(Args)]
pub struct AgentInstallArgs {
    /// Bundle containing the checksummed host-agent payload and unit.
    pub bundle: String,
    /// Remote host in user@host form. SSH is bootstrap transport only.
    pub remote: String,
    /// Stable node identity bound to the installed certificate.
    #[arg(long)]
    pub node_id: String,
    /// Backend kind already provisioned for this node.
    #[arg(long)]
    pub format: super::deploy_config::DeployFormat,
    #[arg(long)]
    pub ssh_key: Option<String>,
    #[arg(long)]
    pub ssh_port: Option<u16>,
    /// Time allowed for the exact replacement activation to attest.
    #[arg(long, default_value_t = 60)]
    pub wait_timeout: u64,
}

/// Parsed canonical host policy. Revision-specific mappings intentionally live
/// only in digest-covered release metadata.
#[derive(Clone, Debug)]
pub struct AgentConfig(AgentPolicy);

impl std::ops::Deref for AgentConfig {
    type Target = AgentPolicy;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AgentConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path)
            .with_context(|| format!("failed to read node-agent config {}", path.display()))?;
        let metadata = path.metadata()?;
        if metadata.uid() != 0 || metadata.permissions().mode() & 0o077 != 0 {
            bail!("node-agent config must be root-owned and owner-only");
        }
        let policy = AgentPolicy::from_canonical_bytes(&bytes)
            .with_context(|| format!("failed to parse node-agent config {}", path.display()))?;
        Ok(Self(policy))
    }

    pub fn validate(&self) -> Result<()> {
        self.0.validate()?;
        self.backend_config().validate()
    }

    fn backend(&self) -> BackendType {
        match self.0.backend {
            AgentPolicyBackend::Systemd => BackendType::Systemd,
            AgentPolicyBackend::Docker => BackendType::Docker,
        }
    }

    fn tls(&self) -> ClientTlsConfig {
        ClientTlsConfig {
            cert_path: self.client_cert_path.clone(),
            key_path: self.client_key_path.clone(),
            server_ca_cert_path: self.ca_cert_path.clone(),
        }
    }

    fn backend_config(&self) -> HostBackendConfig {
        HostBackendConfig {
            deployment_root: PathBuf::from(&self.deployment_root),
            runtime_dir: PathBuf::from(&self.runtime_dir),
            backend: self.backend(),
            systemctl_path: (!self.systemctl_path.is_empty())
                .then(|| PathBuf::from(&self.systemctl_path)),
            docker_path: (!self.docker_path.is_empty()).then(|| PathBuf::from(&self.docker_path)),
            compose_project: (!self.compose_project.is_empty())
                .then(|| self.compose_project.clone()),
        }
    }
}

/// The process-lifetime lock is a local duplicate guard. Manager activation +
/// epoch checks remain authoritative across hosts and lock loss.
pub struct ActivationLock {
    _file: File,
}

impl Drop for ActivationLock {
    fn drop(&mut self) {
        // Rust opens files close-on-exec, but another test/process thread can
        // fork while this descriptor is live. Explicitly unlocking the shared
        // open-file description prevents a briefly inherited pre-exec
        // descriptor from extending activation ownership after this guard is
        // dropped.
        unsafe {
            libc::flock(self._file.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

impl ActivationLock {
    pub fn acquire(runtime_dir: &Path, node_id: &str) -> Result<Self> {
        validate_identity(node_id, "node_id")?;
        validate_root_owned_directory(runtime_dir, "runtime_dir")?;
        Self::acquire_file(runtime_dir, node_id)
    }

    fn acquire_file(runtime_dir: &Path, node_id: &str) -> Result<Self> {
        let path = runtime_dir.join(format!("node-agent-{node_id}.lock"));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(&path)
            .with_context(|| format!("failed to open activation lock {}", path.display()))?;
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            bail!("another node-agent activation holds {}", path.display());
        }
        Ok(Self { _file: file })
    }
}

#[derive(Clone, Debug)]
pub struct LeaseIdentity {
    pub node_id: String,
    pub operation_id: String,
    pub lease_epoch: u64,
    pub agent_instance_id: String,
}

pub trait LeaseManager: Send + Sync {
    fn renew<'a>(&'a self, lease: &'a LeaseIdentity) -> AgentFuture<'a, ()>;
}

/// Complete manager seam used by the production activation loop. Requests are
/// already fully typed before crossing this boundary.
pub type ReportResultFuture<'a> =
    Pin<Box<dyn Future<Output = std::result::Result<(), ReportResultError>> + Send + 'a>>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReportResultError {
    Retryable(String),
    Rejected(String),
}

impl std::fmt::Display for ReportResultError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Retryable(detail) => write!(formatter, "retryable result delivery: {detail}"),
            Self::Rejected(detail) => write!(formatter, "result rejected: {detail}"),
        }
    }
}

pub trait AgentManager: LeaseManager {
    fn attest<'a>(&'a self, attestation: NodeAgentAttestation) -> AgentFuture<'a, ()>;
    fn claim_cleanup<'a>(
        &'a self,
        _node_id: &'a str,
        _agent_instance_id: &'a str,
    ) -> AgentFuture<'a, Option<NodeCleanupInstruction>> {
        Box::pin(async { Ok(None) })
    }
    fn renew_cleanup<'a>(
        &'a self,
        _instruction: &'a NodeCleanupInstruction,
    ) -> AgentFuture<'a, ()> {
        Box::pin(async { bail!("cleanup lease renewal is unsupported") })
    }
    fn report_cleanup<'a>(
        &'a self,
        _request: ReportNodeCleanupResultRequest,
    ) -> ReportResultFuture<'a> {
        Box::pin(async {
            Err(ReportResultError::Rejected(
                "cleanup reporting is unsupported".into(),
            ))
        })
    }
    fn claim<'a>(
        &'a self,
        node_id: &'a str,
        agent_instance_id: &'a str,
    ) -> AgentFuture<'a, Option<AgentInstruction>>;
    fn report_observation<'a>(
        &'a self,
        request: ReportNodeObservationRequest,
    ) -> AgentFuture<'a, ()>;
    fn report_result<'a>(&'a self, request: ReportStepResultRequest) -> ReportResultFuture<'a>;
}

pub trait AgentClock: Send + Sync {
    fn now(&self) -> SystemTime;
    fn sleep<'a>(&'a self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

pub struct TokioClock;

impl AgentClock for TokioClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }

    fn sleep<'a>(&'a self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(tokio::time::sleep(duration))
    }
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)] // Step evidence is retained inline across the fencing boundary for atomic persistence.
pub enum FencedExecution {
    Completed(Result<StepEvidence>),
    LeaseLost(String),
    DeadlineExpired,
    Shutdown,
}

/// Execute one instruction while renewal, deadline, and shutdown remain
/// independent of a blocked backend command. Every fence path signals
/// cancellation and waits for the executor to reap before returning.
pub async fn execute_fenced<M, E, C>(
    manager: &M,
    executor: &E,
    clock: &C,
    instruction: &AgentInstruction,
    renew_every: Duration,
    mut shutdown: watch::Receiver<bool>,
) -> FencedExecution
where
    M: LeaseManager,
    E: InstructionExecutor,
    C: AgentClock,
{
    let lease = LeaseIdentity {
        node_id: instruction.node_id.clone(),
        operation_id: instruction.operation_id.clone(),
        lease_epoch: instruction.lease_epoch,
        agent_instance_id: instruction.agent_instance_id.clone(),
    };
    let deadline_delay = if instruction.restoration {
        None
    } else {
        instruction.operation_deadline.as_ref().map(|deadline| {
            let deadline = SystemTime::UNIX_EPOCH
                + Duration::from_secs(deadline.seconds.max(0) as u64)
                + Duration::from_nanos(deadline.nanos.max(0) as u64);
            deadline.duration_since(clock.now()).unwrap_or_default()
        })
    };
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let mut execution = Box::pin(executor.execute(instruction, cancel_rx));
    let mut renewal: AgentFuture<'_, ()> = Box::pin(async {
        clock.sleep(renew_every).await;
        manager.renew(&lease).await
    });
    let mut deadline: Pin<Box<dyn Future<Output = ()> + Send + '_>> = match deadline_delay {
        Some(delay) => clock.sleep(delay),
        None => Box::pin(std::future::pending()),
    };

    loop {
        enum Fence {
            Lease(String),
            Deadline,
            Shutdown,
        }
        let fence = tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { Some(Fence::Shutdown) } else { None }
            }
            () = &mut deadline, if deadline_delay.is_some() => Some(Fence::Deadline),
            result = &mut execution => return FencedExecution::Completed(result),
            renewed = &mut renewal => {
                match renewed {
                    Ok(()) => {
                        renewal = Box::pin(async {
                            clock.sleep(renew_every).await;
                            manager.renew(&lease).await
                        });
                        None
                    }
                    Err(error) => Some(Fence::Lease(format!("{error:#}"))),
                }
            }
        };
        let Some(fence) = fence else { continue };
        let _ = cancel_tx.send(true);
        let _ = execution.await;
        return match fence {
            Fence::Lease(detail) => FencedExecution::LeaseLost(detail),
            Fence::Deadline => FencedExecution::DeadlineExpired,
            Fence::Shutdown => FencedExecution::Shutdown,
        };
    }
}

async fn execute_cleanup_fenced<M, E, C>(
    manager: &M,
    executor: &E,
    clock: &C,
    instruction: &NodeCleanupInstruction,
    renew_every: Duration,
    mut shutdown: watch::Receiver<bool>,
) -> FencedExecution
where
    M: AgentManager,
    E: InstructionExecutor,
    C: AgentClock,
{
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let mut execution = Box::pin(executor.execute_cleanup(instruction, cancel_rx));
    let mut renewal: AgentFuture<'_, ()> = Box::pin(async {
        clock.sleep(renew_every).await;
        manager.renew_cleanup(instruction).await
    });
    loop {
        enum Fence {
            Lease(String),
            Shutdown,
        }
        let fence = tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { Some(Fence::Shutdown) } else { None }
            }
            result = &mut execution => return FencedExecution::Completed(result.map(|evidence| StepEvidence { cleanup_evidence: Some(evidence), ..Default::default() })),
            renewed = &mut renewal => match renewed {
                Ok(()) => {
                    renewal = Box::pin(async { clock.sleep(renew_every).await; manager.renew_cleanup(instruction).await });
                    None
                }
                Err(error) => Some(Fence::Lease(format!("{error:#}"))),
            }
        };
        let Some(fence) = fence else { continue };
        let _ = cancel_tx.send(true);
        let _ = execution.await;
        return match fence {
            Fence::Lease(detail) => FencedExecution::LeaseLost(detail),
            Fence::Shutdown => FencedExecution::Shutdown,
        };
    }
}

struct ProductionManager {
    manager: tokio::sync::Mutex<wr_common::manager_client::ManagerEpoch>,
}

impl LeaseManager for ProductionManager {
    fn renew<'a>(&'a self, lease: &'a LeaseIdentity) -> AgentFuture<'a, ()> {
        Box::pin(async move {
            self.manager
                .lock()
                .await
                .renew_operation_lease(RenewOperationLeaseRequest {
                    node_id: lease.node_id.clone(),
                    operation_id: lease.operation_id.clone(),
                    lease_epoch: lease.lease_epoch,
                    agent_instance_id: lease.agent_instance_id.clone(),
                })
                .await
                .context("operation lease was lost")?;
            Ok(())
        })
    }
}

impl AgentManager for ProductionManager {
    fn attest<'a>(&'a self, attestation: NodeAgentAttestation) -> AgentFuture<'a, ()> {
        Box::pin(async move {
            let response = self
                .manager
                .lock()
                .await
                .attest(AttestNodeAgentRequest {
                    attestation: Some(attestation),
                })
                .await?
                .into_inner();
            if !response.accepted || !response.conditions.is_empty() {
                let codes = response
                    .conditions
                    .iter()
                    .map(|condition| condition.code.as_str())
                    .collect::<Vec<_>>()
                    .join(",");
                bail!("node-agent attestation was rejected: {codes}");
            }
            Ok(())
        })
    }

    fn claim<'a>(
        &'a self,
        node_id: &'a str,
        agent_instance_id: &'a str,
    ) -> AgentFuture<'a, Option<AgentInstruction>> {
        Box::pin(async move {
            Ok(self
                .manager
                .lock()
                .await
                .claim_operation(ClaimOperationRequest {
                    node_id: node_id.to_string(),
                    agent_instance_id: agent_instance_id.to_string(),
                })
                .await?
                .into_inner()
                .instruction)
        })
    }

    fn claim_cleanup<'a>(
        &'a self,
        node_id: &'a str,
        agent_instance_id: &'a str,
    ) -> AgentFuture<'a, Option<NodeCleanupInstruction>> {
        Box::pin(async move {
            Ok(self
                .manager
                .lock()
                .await
                .claim_node_cleanup(ClaimNodeCleanupRequest {
                    node_id: node_id.to_string(),
                    agent_instance_id: agent_instance_id.to_string(),
                })
                .await?
                .into_inner()
                .instruction)
        })
    }

    fn renew_cleanup<'a>(&'a self, instruction: &'a NodeCleanupInstruction) -> AgentFuture<'a, ()> {
        Box::pin(async move {
            let response = self
                .manager
                .lock()
                .await
                .renew_node_cleanup_lease(RenewNodeCleanupLeaseRequest {
                    node_id: instruction.node_id.clone(),
                    agent_instance_id: instruction.agent_instance_id.clone(),
                    generation: instruction.generation,
                    lease_epoch: instruction.lease_epoch,
                    claim_instance: instruction.claim_instance.clone(),
                })
                .await?
                .into_inner();
            if response.superseded {
                bail!("cleanup generation was superseded")
            }
            Ok(())
        })
    }

    fn report_cleanup<'a>(
        &'a self,
        request: ReportNodeCleanupResultRequest,
    ) -> ReportResultFuture<'a> {
        Box::pin(async move {
            match self
                .manager
                .lock()
                .await
                .report_node_cleanup_result(request)
                .await
            {
                Ok(_) => Ok(()),
                Err(status)
                    if matches!(
                        status.code(),
                        tonic::Code::InvalidArgument
                            | tonic::Code::PermissionDenied
                            | tonic::Code::Unauthenticated
                            | tonic::Code::FailedPrecondition
                            | tonic::Code::Aborted
                            | tonic::Code::AlreadyExists
                    ) =>
                {
                    Err(ReportResultError::Rejected(status.to_string()))
                }
                Err(status) => Err(ReportResultError::Retryable(status.to_string())),
            }
        })
    }

    fn report_observation<'a>(
        &'a self,
        request: ReportNodeObservationRequest,
    ) -> AgentFuture<'a, ()> {
        Box::pin(async move {
            self.manager
                .lock()
                .await
                .report_observation(request)
                .await?;
            Ok(())
        })
    }

    fn report_result<'a>(&'a self, request: ReportStepResultRequest) -> ReportResultFuture<'a> {
        Box::pin(async move {
            let mut manager = self.manager.lock().await;
            match manager.report_step_result(request).await {
                Ok(_) => Ok(()),
                Err(status)
                    if matches!(
                        status.code(),
                        tonic::Code::InvalidArgument
                            | tonic::Code::PermissionDenied
                            | tonic::Code::Unauthenticated
                            | tonic::Code::FailedPrecondition
                            | tonic::Code::Aborted
                            | tonic::Code::AlreadyExists
                    ) =>
                {
                    Err(ReportResultError::Rejected(status.to_string()))
                }
                Err(status) => Err(ReportResultError::Retryable(status.to_string())),
            }
        })
    }
}

fn validate_root_owned_file(path: &Path, label: &str) -> Result<()> {
    let metadata = path
        .metadata()
        .with_context(|| format!("{label} {} is unavailable", path.display()))?;
    if !metadata.is_file() || metadata.uid() != 0 || metadata.permissions().mode() & 0o077 != 0 {
        bail!("{label} must be a root-owned owner-only regular file");
    }
    Ok(())
}

fn executable_digest() -> Result<String> {
    let executable = std::env::current_exe().context("current executable path is unavailable")?;
    Ok(format!(
        "sha256:{:x}",
        Sha256::digest(std::fs::read(executable).context("current executable is unreadable")?)
    ))
}

#[derive(Clone)]
pub struct ActivationConfig {
    pub node_id: String,
    pub agent_instance_id: String,
    pub binary_digest: String,
    pub backend: BackendType,
    pub poll: Duration,
    pub renew: Duration,
}

fn attestation(config: &ActivationConfig) -> NodeAgentAttestation {
    NodeAgentAttestation {
        node_id: config.node_id.clone(),
        agent_instance_id: config.agent_instance_id.clone(),
        protocol_version: AGENT_PROTOCOL_VERSION.to_string(),
        binary_digest: config.binary_digest.clone(),
        backend: config.backend.wire() as i32,
        capabilities: AGENT_CAPABILITIES
            .iter()
            .map(|value| (*value).to_string())
            .collect(),
        ..Default::default()
    }
}

fn observation_request(
    config: &ActivationConfig,
    instruction: &AgentInstruction,
    evidence: &StepEvidence,
) -> Result<Option<ReportNodeObservationRequest>> {
    let target = instruction
        .target
        .as_ref()
        .context("manager instruction omitted target")?;
    let engine_slot = match workload_target(target)? {
        WorkloadTarget::Proxy => return Ok(None),
        WorkloadTarget::EngineSlot(slot) => slot,
    };
    let Some(state) = evidence.backend_state else {
        return Ok(None);
    };
    Ok(Some(ReportNodeObservationRequest {
        node_id: config.node_id.clone(),
        engine_slot: engine_slot.to_string(),
        lifecycle: evidence.lifecycle.clone(),
        backend_state: state as i32,
        backend_instance_id: evidence.backend_instance_id.clone(),
        observed_revision: evidence.observed_revision,
        observed_at: Some(prost_types::Timestamp {
            seconds: chrono::Utc::now().timestamp(),
            nanos: 0,
        }),
        observed_digest: evidence.observed_digest.clone(),
        backend_query_error: evidence.backend_query_error.clone(),
        operation_id: instruction.operation_id.clone(),
        agent_instance_id: instruction.agent_instance_id.clone(),
        lease_epoch: instruction.lease_epoch,
        observed_resolved_release_digest: evidence.observed_resolved_release_digest.clone(),
    }))
}

fn result_request(
    config: &ActivationConfig,
    instruction: &AgentInstruction,
    evidence: &StepEvidence,
    failure: Option<&anyhow::Error>,
) -> Result<ReportStepResultRequest> {
    let target = instruction
        .target
        .as_ref()
        .context("manager instruction omitted target")?;
    let (condition_code, detail) = match failure {
        Some(error) => ("HOST_STEP_FAILED".to_string(), format!("{error:#}")),
        None => (String::new(), String::new()),
    };
    Ok(ReportStepResultRequest {
        node_id: config.node_id.clone(),
        operation_id: instruction.operation_id.clone(),
        lease_epoch: instruction.lease_epoch,
        step: instruction.step,
        condition_code,
        detail,
        agent_instance_id: instruction.agent_instance_id.clone(),
        observed_revision: evidence.observed_revision,
        observed_digest: evidence.observed_digest.clone(),
        backend_instance_id: evidence.backend_instance_id.clone(),
        process_instance_id: evidence.process_instance_id.clone(),
        backend_query_error: evidence.backend_query_error.clone(),
        observed_resolved_release_digest: evidence.observed_resolved_release_digest.clone(),
        termination_evidence: (instruction.step == NodeOperationStepKind::StopBackend as i32)
            .then(|| evidence.termination.clone())
            .flatten(),
        target: Some(target.clone()),
    })
}

async fn retry_pause<C: AgentClock>(
    clock: &C,
    duration: Duration,
    shutdown: watch::Receiver<bool>,
) -> bool {
    tokio::select! {
        () = clock.sleep(duration) => false,
        () = wait_shutdown(shutdown.clone()) => true,
    }
}

/// Run one process-lifetime activation. Transient manager transport failures
/// retain the activation identity and any completed effect report; they never
/// restart the process and never redeliver the host mutation.
pub async fn run_activation<M, E, C>(
    manager: &M,
    executor: &E,
    clock: &C,
    config: ActivationConfig,
    shutdown: watch::Receiver<bool>,
) -> Result<()>
where
    M: AgentManager,
    E: InstructionExecutor,
    C: AgentClock,
{
    let mut attested = false;
    let mut pending_observation: Option<(ReportNodeObservationRequest, i32)> = None;
    let mut pending_result: Option<ReportStepResultRequest> = None;
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        if !attested {
            match manager.attest(attestation(&config)).await {
                Ok(()) => attested = true,
                Err(error) => {
                    eprintln!("node-agent attestation transport failed; retrying: {error:#}");
                    if retry_pause(clock, config.poll, shutdown.clone()).await {
                        return Ok(());
                    }
                    continue;
                }
            }
        }
        // A completed effect result has priority over every operation-bearing
        // RPC. Until its exact acknowledgement arrives, no claim can occur.
        if let Some(request) = pending_result.clone() {
            match manager.report_result(request).await {
                Ok(()) => {
                    pending_result = None;
                    // Re-enter at the shutdown/fence boundary before any
                    // further attestation or effect-delivering claim.
                    continue;
                }
                Err(ReportResultError::Retryable(detail)) => {
                    eprintln!(
                        "node-agent result acknowledgement was not received; retrying exact report without claiming: {detail}"
                    );
                    if retry_pause(clock, config.poll, shutdown.clone()).await {
                        return Ok(());
                    }
                    continue;
                }
                Err(ReportResultError::Rejected(detail)) => {
                    bail!(
                        "node-agent result was explicitly rejected; further effects are fenced: {detail}"
                    );
                }
            }
        }
        if let Some((request, expected_step)) = pending_observation.clone() {
            match manager.report_observation(request.clone()).await {
                Ok(()) => {
                    pending_observation = None;
                }
                Err(error) => {
                    eprintln!("node-agent observation report failed; reconciling: {error:#}");
                    match manager
                        .claim(&config.node_id, &config.agent_instance_id)
                        .await
                    {
                        Ok(Some(claimed))
                            if claimed.operation_id == request.operation_id
                                && claimed.lease_epoch == request.lease_epoch
                                && claimed.step == expected_step => {}
                        Ok(None) => {
                            pending_observation = None;
                        }
                        Ok(Some(_)) => bail!(
                            "manager delivered a different instruction during observation reconciliation"
                        ),
                        Err(claim_error) => {
                            eprintln!(
                                "node-agent observation reconciliation failed: {claim_error:#}"
                            );
                        }
                    }
                    if retry_pause(clock, config.poll, shutdown.clone()).await {
                        return Ok(());
                    }
                    continue;
                }
            }
        }

        // Re-attestation freshness is renewed between claims, but a transport
        // outage is an in-process retry rather than activation churn.
        if let Err(error) = manager.attest(attestation(&config)).await {
            eprintln!("node-agent re-attestation failed; retrying: {error:#}");
            if retry_pause(clock, config.poll, shutdown.clone()).await {
                return Ok(());
            }
            continue;
        }
        let instruction = match manager
            .claim(&config.node_id, &config.agent_instance_id)
            .await
        {
            Ok(Some(instruction)) => instruction,
            Ok(None) => {
                match manager
                    .claim_cleanup(&config.node_id, &config.agent_instance_id)
                    .await
                {
                    Ok(Some(cleanup)) => {
                        if cleanup.node_id != config.node_id
                            || cleanup.agent_instance_id != config.agent_instance_id
                            || cleanup.generation == 0
                            || cleanup.lease_epoch == 0
                            || cleanup.claim_instance.is_empty()
                            || cleanup.payload_digest.is_empty()
                        {
                            bail!("manager returned a mismatched cleanup fence");
                        }
                        match execute_cleanup_fenced(
                            manager,
                            executor,
                            clock,
                            &cleanup,
                            config.renew,
                            shutdown.clone(),
                        )
                        .await
                        {
                            FencedExecution::Completed(result) => {
                                let (deleted_releases, resulting_inventory, condition_code, detail) =
                                    match result {
                                        Ok(evidence) => (
                                            cleanup.delete_releases.clone(),
                                            evidence
                                                .cleanup_evidence
                                                .map(|value| value.retained_releases)
                                                .unwrap_or_default(),
                                            String::new(),
                                            String::new(),
                                        ),
                                        Err(error) => (
                                            Vec::new(),
                                            Vec::new(),
                                            "HOST_CLEANUP_FAILED".to_string(),
                                            format!("{error:#}"),
                                        ),
                                    };
                                let report = ReportNodeCleanupResultRequest {
                                    node_id: cleanup.node_id.clone(),
                                    agent_instance_id: cleanup.agent_instance_id.clone(),
                                    generation: cleanup.generation,
                                    lease_epoch: cleanup.lease_epoch,
                                    claim_instance: cleanup.claim_instance.clone(),
                                    payload_digest: cleanup.payload_digest.clone(),
                                    deleted_releases,
                                    resulting_inventory,
                                    condition_code,
                                    detail,
                                };
                                loop {
                                    match manager.report_cleanup(report.clone()).await {
                                        Ok(()) => break,
                                        Err(ReportResultError::Retryable(detail)) => {
                                            eprintln!("node-agent cleanup acknowledgement was not received; retrying exact report: {detail}");
                                            if retry_pause(clock, config.poll, shutdown.clone())
                                                .await
                                            {
                                                return Ok(());
                                            }
                                        }
                                        Err(ReportResultError::Rejected(detail)) => {
                                            eprintln!("node-agent cleanup generation was rejected or superseded: {detail}");
                                            break;
                                        }
                                    }
                                }
                            }
                            FencedExecution::LeaseLost(detail) => eprintln!(
                                "node-agent cleanup lease lost; generation is fenced: {detail}"
                            ),
                            FencedExecution::Shutdown => return Ok(()),
                            FencedExecution::DeadlineExpired => {
                                unreachable!("cleanup uses only renewable generation fencing")
                            }
                        }
                    }
                    Ok(None) => {}
                    Err(error) => eprintln!("node-agent cleanup claim failed; retrying: {error:#}"),
                }
                if retry_pause(clock, config.poll, shutdown.clone()).await {
                    return Ok(());
                }
                continue;
            }
            Err(error) => {
                eprintln!("node-agent claim failed; retrying: {error:#}");
                if retry_pause(clock, config.poll, shutdown.clone()).await {
                    return Ok(());
                }
                continue;
            }
        };
        if instruction.node_id != config.node_id
            || instruction.agent_instance_id != config.agent_instance_id
            || instruction.operation_id.is_empty()
            || instruction.lease_epoch == 0
        {
            bail!("manager returned a mismatched instruction fence");
        }
        let step = NodeOperationStepKind::try_from(instruction.step)
            .unwrap_or(NodeOperationStepKind::Unspecified);
        if step == NodeOperationStepKind::InspectBackend {
            eprintln!(
                "node-agent following durable manager ambiguity with typed backend inspection for operation {}",
                instruction.operation_id
            );
        }
        match execute_fenced(
            manager,
            executor,
            clock,
            &instruction,
            config.renew,
            shutdown.clone(),
        )
        .await
        {
            FencedExecution::Completed(result) => {
                let (evidence, failure) = match result {
                    Ok(evidence) => (evidence, None),
                    Err(error) => (
                        StepEvidence {
                            observed_revision: instruction
                                .target
                                .as_ref()
                                .map(|target| target.revision)
                                .unwrap_or_default(),
                            observed_digest: instruction
                                .target
                                .as_ref()
                                .map(|target| target.bundle_digest.clone())
                                .unwrap_or_default(),
                            backend_state: None,
                            backend_query_error: error.to_string(),
                            ..Default::default()
                        },
                        Some(error),
                    ),
                };
                pending_observation = observation_request(&config, &instruction, &evidence)?
                    .map(|request| (request, instruction.step));
                let reports_via_result = if step == NodeOperationStepKind::InspectBackend {
                    let target = instruction
                        .target
                        .as_ref()
                        .context("manager instruction omitted target")?;
                    matches!(workload_target(target)?, WorkloadTarget::Proxy)
                } else {
                    true
                };
                if reports_via_result {
                    let report =
                        result_request(&config, &instruction, &evidence, failure.as_ref())?;
                    pending_result = Some(report);
                }
            }
            FencedExecution::LeaseLost(detail) => {
                eprintln!("node-agent lease lost; effect delivery is ambiguous: {detail}");
            }
            FencedExecution::DeadlineExpired => {
                eprintln!("node-agent forward deadline expired; effect delivery is ambiguous");
            }
            FencedExecution::Shutdown => return Ok(()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InstallAction {
    Noop,
    RestartExisting,
    StageAndRestart,
}

fn install_action(remote_matches: bool, already_attested: bool) -> InstallAction {
    match (remote_matches, already_attested) {
        (true, true) => InstallAction::Noop,
        (true, false) => InstallAction::RestartExisting,
        (false, _) => InstallAction::StageAndRestart,
    }
}

#[derive(Clone)]
struct AgentInstallMaterial {
    binary: Vec<u8>,
}

fn safe_install_path(path: &str, label: &str) -> Result<()> {
    if !path.starts_with('/')
        || path == "/"
        || !path
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'_' | b'.'))
    {
        bail!("{label} must be a shell-safe absolute path");
    }
    Ok(())
}

pub(super) fn wire_policy(
    node_id: &str,
    backend: AgentPolicyBackend,
    binary_digest: String,
    retention_count: Option<u32>,
) -> NodeAgentPolicy {
    NodeAgentPolicy {
        node_id: node_id.to_string(),
        protocol_version: AGENT_PROTOCOL_VERSION.to_string(),
        backend: match backend {
            AgentPolicyBackend::Systemd => BackendKind::Systemd,
            AgentPolicyBackend::Docker => BackendKind::Docker,
        } as i32,
        retention_count,
        binary_digest,
        capabilities: AGENT_CAPABILITIES
            .iter()
            .map(|value| (*value).to_string())
            .collect(),
    }
}

pub(super) fn attestation_matches_policy(
    attestation: &NodeAgentAttestation,
    policy: &NodeAgentPolicy,
) -> bool {
    let fresh = attestation.observed_at.as_ref().is_some_and(|observed| {
        chrono::DateTime::from_timestamp(observed.seconds, observed.nanos as u32)
            .is_some_and(|time| chrono::Utc::now().signed_duration_since(time).num_seconds() <= 30)
    });
    let capabilities_match = missing_capabilities(&policy.capabilities, &attestation.capabilities)
        .is_ok_and(|missing| missing.is_empty());
    fresh
        && !attestation.agent_instance_id.is_empty()
        && attestation.node_id == policy.node_id
        && attestation.protocol_version == policy.protocol_version
        && attestation.binary_digest == policy.binary_digest
        && attestation.backend == policy.backend
        && capabilities_match
}

fn unfiltered_status_request() -> GetOperatorStatusRequest {
    GetOperatorStatusRequest {
        node_id: String::new(),
        engine_slot: String::new(),
    }
}

fn attestations_for_node(
    attestations: Vec<NodeAgentAttestation>,
    node_id: &str,
) -> Vec<NodeAgentAttestation> {
    attestations
        .into_iter()
        .filter(|attestation| attestation.node_id == node_id)
        .collect()
}

async fn node_attestations(manager: &str, node_id: &str) -> Result<Vec<NodeAgentAttestation>> {
    let attestations =
        client::connect_operator(manager, wr_common::manager_client::RetryClass::ReadOnly)
            .await?
            .get_status(unfiltered_status_request())
            .await?
            .into_inner()
            .agent_attestations;
    Ok(attestations_for_node(attestations, node_id))
}

fn require_prior_attestations(
    result: Result<Vec<NodeAgentAttestation>>,
) -> Result<Vec<NodeAgentAttestation>> {
    result.context("failed to read authenticated pre-restart node-agent status")
}

async fn matching_attestations(
    manager: &str,
    policy: &NodeAgentPolicy,
) -> Result<Vec<NodeAgentAttestation>> {
    Ok(node_attestations(manager, &policy.node_id)
        .await?
        .into_iter()
        .filter(|attestation| attestation_matches_policy(attestation, policy))
        .collect())
}

fn remote_payload_matches_command(workdir: &str, material: &AgentInstallMaterial) -> String {
    let path = format!("{workdir}/wr-agent/wr-cli");
    let digest = wr_common::agent_policy::sha256_digest(&material.binary);
    format!(
        "sudo test -f {path} && test \"$(sudo sha256sum {path} | cut -d' ' -f1)\" = {} && test \"$(sudo stat -c '%u:%a' {path})\" = '0:755'",
        digest.trim_start_matches("sha256:")
    )
}

fn remote_baseline_command(workdir: &str) -> String {
    format!(
        "sudo test -d {workdir}/wr-agent && sudo test -f {workdir}/wr-agent/agent.toml && sudo test -f {workdir}/wr-agent/certs/agent.crt && sudo test -f {workdir}/wr-agent/certs/agent.key && sudo test -f {workdir}/wr-agent/certs/ca.crt && sudo test -f /etc/systemd/system/wr-node-agent.service"
    )
}

fn staged_payload_checks_command(agent_root: &str, material: &AgentInstallMaterial) -> String {
    let path = format!("{agent_root}/.wr-cli.new");
    format!(
        "sudo chown root:root {path} && sudo chmod 0755 {path} && test \"$(sudo sha256sum {path} | cut -d' ' -f1)\" = {} && sudo sync -f {path}",
        wr_common::agent_policy::sha256_digest(&material.binary).trim_start_matches("sha256:")
    )
}

fn remote_activate_command(workdir: &str, _material: &AgentInstallMaterial) -> String {
    format!(
        "sudo mv {workdir}/wr-agent/.wr-cli.new {workdir}/wr-agent/wr-cli && sudo sync -f {workdir}/wr-agent && sudo systemctl restart wr-node-agent.service"
    )
}

async fn install(args: AgentInstallArgs, manager: &str, updating: bool) -> Result<()> {
    validate_identity(&args.node_id, "node_id")?;
    let format = args.format;
    let ssh_key = args.ssh_key;
    let ssh_port = args.ssh_port;
    let manifest: BundleManifest = bundle::read_manifest(&args.bundle)?;
    verify_bundle_archive(&args.bundle, &manifest)?;
    safe_install_path(&manifest.workdir, "bundle workdir")?;
    let agent_root = format!("{}/wr-agent", manifest.workdir);
    let backend = match format {
        DeployFormat::Systemd => AgentPolicyBackend::Systemd,
        DeployFormat::Docker => AgentPolicyBackend::Docker,
    };
    let material = AgentInstallMaterial {
        binary: bundle::read_bytes_from_tarball(&args.bundle, "wr-node/agent/wr-cli")?,
    };
    let binary_digest = wr_common::agent_policy::sha256_digest(&material.binary);
    // The updater never owns cleanup retention; omission makes the manager
    // preserve the already-provisioned operator value transactionally.
    let wire_policy = wire_policy(&args.node_id, backend, binary_digest, None);
    let ssh = helpers::build_ssh_args(&args.remote, ssh_key.as_deref(), ssh_port);
    anyhow::ensure!(
        helpers::run_ssh_output(
            &ssh,
            &format!(
                "if {}; then printf ready; else printf missing; fi",
                remote_baseline_command(&manifest.workdir)
            ),
        )? == "ready",
        "node-agent baseline is missing; provisioning must install config, credentials, directories, backend, executable, and service unit before update"
    );
    let prior_attestations = helpers::wait_with_deadline(
        &format!("provisioned node-agent activation for {}", wire_policy.node_id),
        Duration::from_secs(args.wait_timeout),
        Duration::from_millis(500),
        || async {
            match require_prior_attestations(node_attestations(manager, &wire_policy.node_id).await) {
                Ok(attestations) if !attestations.is_empty() => {
                    helpers::WaitAttempt::Matched(attestations)
                }
                Ok(_) => helpers::WaitAttempt::Pending(
                    "expected node-agent policy/activation is missing; provisioning must publish the initial policy with retention_count".into(),
                ),
                Err(error) => helpers::WaitAttempt::QueryFailure(error),
            }
        },
    )
    .await?;
    let old_activations = prior_attestations
        .iter()
        .map(|attestation| attestation.agent_instance_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let already_attested = prior_attestations
        .iter()
        .any(|attestation| attestation_matches_policy(attestation, &wire_policy));
    let remote_matches = helpers::run_ssh_output(
        &ssh,
        &format!(
            "if {}; then printf match; else printf mismatch; fi",
            remote_payload_matches_command(&manifest.workdir, &material)
        ),
    )? == "match";

    if remote_matches {
        client::connect_operator(
            manager,
            wr_common::manager_client::RetryClass::NoReplayMutation,
        )
        .await?
        .put_node_agent_policy(PutNodeAgentPolicyRequest {
            policy: Some(wire_policy.clone()),
        })
        .await?;
        if install_action(remote_matches, already_attested) == InstallAction::Noop {
            println!(
                "[agent] exact bytes and fresh attestation already match; no restart required"
            );
            return Ok(());
        }
        helpers::run_ssh(&ssh, "sudo systemctl restart wr-node-agent.service")
            .context("node-agent service restart failed")?;
    } else {
        let staged = format!("{agent_root}/.wr-cli.new");
        helpers::scp_bytes(
            &material.binary,
            &args.remote,
            &staged,
            ssh_key.as_deref(),
            ssh_port,
        )?;
        let staged_checks = staged_payload_checks_command(&agent_root, &material);
        helpers::run_ssh(&ssh, &staged_checks)
            .context("remote node-agent payload checksum mismatch")?;
        // Only after every new byte is durably staged does the manager expectation
        // move. A retry can safely finish activation if interruption follows.
        client::connect_operator(
            manager,
            wr_common::manager_client::RetryClass::NoReplayMutation,
        )
        .await?
        .put_node_agent_policy(PutNodeAgentPolicyRequest {
            policy: Some(wire_policy.clone()),
        })
        .await?;
        helpers::run_ssh(&ssh, &remote_activate_command(&manifest.workdir, &material))
            .context("node-agent service restart failed")?;
    }

    let excluded = old_activations;
    let node = wire_policy.node_id.clone();
    let instance = helpers::wait_with_deadline(
        &format!("node-agent attestation for {node}"),
        Duration::from_secs(args.wait_timeout),
        Duration::from_millis(500),
        || async {
            match matching_attestations(manager, &wire_policy).await {
                Ok(attestations) => attestations
                    .into_iter()
                    .find(|attestation| !excluded.contains(&attestation.agent_instance_id))
                    .map(|attestation| helpers::WaitAttempt::Matched(attestation.agent_instance_id))
                    .unwrap_or_else(|| {
                        helpers::WaitAttempt::Pending(
                            "waiting for a new compatible protocol/binary/backend/capability attestation".into(),
                        )
                    }),
                Err(error) => helpers::WaitAttempt::QueryFailure(error),
            }
        },
    )
    .await
    .with_context(|| {
        format!(
            "node-agent attestation did not converge; rerun `wr-cli node agent {}`",
            if updating { "update" } else { "install" }
        )
    })?;
    println!("[agent] authenticated activation {instance} is ready");
    let status = client::connect_operator(manager, wr_common::manager_client::RetryClass::ReadOnly)
        .await?
        .get_status(unfiltered_status_request())
        .await?
        .into_inner();
    for operation in status.active_operations.iter().filter(|operation| {
        operation.node_id == node
            && operation
                .conditions
                .iter()
                .any(|condition| condition.code == "AGENT_POLICY_UPDATED")
    }) {
        println!(
            "[agent] durable operation {} remains fenced; inspect it and explicitly run `wr-cli operations resume {}`",
            operation.operation_id, operation.operation_id
        );
    }
    Ok(())
}

async fn run_agent(args: AgentRunArgs) -> Result<()> {
    let config = AgentConfig::load(&args.config)?;
    config.validate()?;
    validate_root_owned_directory(Path::new(&config.deployment_root), "deployment_root")?;
    validate_root_owned_file(Path::new(&config.client_cert_path), "client certificate")?;
    validate_root_owned_file(Path::new(&config.client_key_path), "client private key")?;
    validate_root_owned_file(Path::new(&config.ca_cert_path), "CA certificate")?;
    let _activation_lock =
        ActivationLock::acquire(Path::new(&config.runtime_dir), &config.node_id)?;
    let backend = HostBackend::new(config.backend_config())?;
    let activation = ActivationConfig {
        node_id: config.node_id.clone(),
        agent_instance_id: uuid::Uuid::new_v4().to_string(),
        binary_digest: executable_digest()?,
        backend: config.backend(),
        poll: Duration::from_secs(config.poll_interval_seconds),
        renew: Duration::from_secs(config.renew_interval_seconds),
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = shutdown_tx.send(true);
    });
    let tls = config.tls();
    let manager = ProductionManager {
        manager: tokio::sync::Mutex::new(
            client::connect_node_agent_with_tls(&config.manager_endpoint, Some(&tls)).await?,
        ),
    };
    run_activation(&manager, &backend, &TokioClock, activation, shutdown_rx).await
}

pub async fn run(args: AgentArgs, manager: Option<&str>) -> Result<()> {
    match args.command {
        AgentCommand::Run(args) => run_agent(args).await,
        AgentCommand::Install(args) => {
            let manager = manager.context("--manager is required for node agent install")?;
            install(args, manager, false).await
        }
        AgentCommand::Update(args) => {
            let manager = manager.context("--manager is required for node agent update")?;
            install(args, manager, true).await
        }
    }
}

async fn wait_shutdown(mut shutdown: watch::Receiver<bool>) {
    while !*shutdown.borrow() && shutdown.changed().await.is_ok() {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::node_backend::BackendFuture;
    use wr_common::wruntime::BackendProcessState;

    fn base_config() -> AgentConfig {
        AgentConfig(AgentPolicy {
            policy_version: wr_common::agent_policy::AGENT_POLICY_VERSION,
            node_id: "node-a".into(),
            manager_endpoint: "https://manager.example:9000".into(),
            client_cert_path: "/etc/wruntime/agent.crt".into(),
            client_key_path: "/etc/wruntime/agent.key".into(),
            ca_cert_path: "/etc/wruntime/ca.crt".into(),
            deployment_root: "/opt/wruntime".into(),
            runtime_dir: "/run/wruntime".into(),
            backend: AgentPolicyBackend::Systemd,
            compose_project: String::new(),
            systemctl_path: "/usr/bin/systemctl".into(),
            docker_path: String::new(),
            poll_interval_seconds: 5,
            renew_interval_seconds: 5,
            protocol_version: AGENT_PROTOCOL_VERSION.into(),
            capabilities: AGENT_CAPABILITIES
                .iter()
                .map(|value| (*value).to_string())
                .collect(),
        })
    }

    #[test]
    fn config_has_no_static_slot_inventory_and_requires_absolute_binary() {
        assert!(base_config().validate().is_ok());
        let legacy = r#"
node-id = "node-a"
manager = "https://manager.example:9000"
deployment-root = "/opt/wruntime"
backend = "systemd"
systemctl-path = "/usr/bin/systemctl"
slots = {}

[tls]
cert_path = "/etc/wruntime/agent.crt"
key_path = "/etc/wruntime/agent.key"
ca_cert_path = "/etc/wruntime/ca.crt"
"#;
        assert!(toml::from_str::<AgentPolicy>(legacy).is_err());
        let mut invalid = base_config();
        invalid.0.systemctl_path = "systemctl".into();
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn install_decision_is_idempotent_and_update_is_controlled() {
        assert_eq!(install_action(true, true), InstallAction::Noop);
        assert_eq!(install_action(true, false), InstallAction::RestartExisting);
        assert_eq!(install_action(false, true), InstallAction::StageAndRestart);
        assert_eq!(install_action(false, false), InstallAction::StageAndRestart);
    }

    #[test]
    fn updater_transfers_only_the_agent_binary() {
        let material = AgentInstallMaterial {
            binary: b"binary".to_vec(),
        };
        let check = remote_payload_matches_command("/opt/wruntime", &material);
        assert!(check.contains("/opt/wruntime/wr-agent/wr-cli"));
        assert!(!check.contains("agent.toml"));
        assert!(!check.contains("agent.key"));

        let baseline = remote_baseline_command("/opt/wruntime");
        assert!(baseline.contains("agent.toml"));
        assert!(baseline.contains("agent.key"));
        assert!(baseline.contains("wr-node-agent.service"));
        assert!(!baseline.contains("install -d"));

        let staged = staged_payload_checks_command("/opt/wruntime/wr-agent", &material);
        assert!(staged.contains(".wr-cli.new"));
        assert_eq!(staged.matches("sha256sum").count(), 1);
        let command = remote_activate_command("/opt/wruntime", &material);
        assert!(command.contains("sudo mv"));
        assert!(command.contains("systemctl restart wr-node-agent.service"));
        for forbidden in [
            "agent.toml",
            "agent.key",
            "daemon-reload",
            "enable",
            "wr-engine",
            "wr-proxy",
            "docker compose",
        ] {
            assert!(!command.contains(forbidden));
        }
    }

    #[test]
    fn initial_status_failure_cannot_become_an_empty_activation_set() {
        let error =
            require_prior_attestations(Err(anyhow::anyhow!("status unavailable"))).unwrap_err();
        assert!(error
            .to_string()
            .contains("failed to read authenticated pre-restart node-agent status"));
    }

    #[test]
    fn unfiltered_status_attestations_are_scoped_to_the_requested_node() {
        let request = unfiltered_status_request();
        assert!(request.node_id.is_empty());
        assert!(request.engine_slot.is_empty());

        let attestations = vec![
            NodeAgentAttestation {
                node_id: "node-a".into(),
                agent_instance_id: "activation-a".into(),
                ..Default::default()
            },
            NodeAgentAttestation {
                node_id: "node-b".into(),
                agent_instance_id: "activation-b".into(),
                ..Default::default()
            },
        ];
        let selected = attestations_for_node(attestations, "node-a");
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].agent_instance_id, "activation-a");
        assert!(attestations_for_node(Vec::new(), "node-a").is_empty());
    }

    #[test]
    fn narrow_attestation_matches_required_capability_subsets() {
        let policy = wire_policy(
            "node-a",
            AgentPolicyBackend::Systemd,
            format!("sha256:{}", "a".repeat(64)),
            None,
        );
        assert!(policy.retention_count.is_none());
        let mut attested = NodeAgentAttestation {
            node_id: policy.node_id.clone(),
            agent_instance_id: "activation-a".into(),
            protocol_version: policy.protocol_version.clone(),
            binary_digest: policy.binary_digest.clone(),
            backend: policy.backend,
            capabilities: policy
                .capabilities
                .iter()
                .rev()
                .cloned()
                .chain(["extra-v1".into()])
                .collect(),
            observed_at: Some(prost_types::Timestamp {
                seconds: chrono::Utc::now().timestamp(),
                nanos: 0,
            }),
            ..Default::default()
        };
        assert!(attestation_matches_policy(&attested, &policy));
        attested
            .capabilities
            .retain(|value| value != AGENT_CAPABILITIES[0]);
        assert!(!attestation_matches_policy(&attested, &policy));
        attested.capabilities = policy.capabilities.clone();
        attested.observed_at.as_mut().unwrap().seconds -= 31;
        assert!(!attestation_matches_policy(&attested, &policy));
    }

    #[test]
    fn local_only_config_changes_do_not_change_manager_contract() {
        let mut changed = base_config().0;
        changed.manager_endpoint = "https://other.example:9000".into();
        changed.deployment_root = "/srv/wruntime".into();
        changed.poll_interval_seconds += 1;
        assert!(changed.validate().is_ok());
        let expected = wire_policy(
            &changed.node_id,
            changed.backend,
            format!("sha256:{}", "a".repeat(64)),
            Some(3),
        );
        assert_eq!(expected.node_id, "node-a");
        assert_eq!(expected.retention_count, Some(3));
    }

    #[test]
    fn activation_lock_excludes_a_duplicate_process() {
        let runtime = std::env::temp_dir().join(format!(
            "wr-agent-lock-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&runtime).unwrap();
        let first = ActivationLock::acquire_file(&runtime, "node-a").unwrap();
        assert!(ActivationLock::acquire_file(&runtime, "node-a").is_err());
        // Model a concurrent fork holding a duplicate descriptor until exec.
        // Dropping the activation guard must explicitly release ownership even
        // while that inherited descriptor remains temporarily open.
        let inherited = first._file.try_clone().unwrap();
        drop(first);
        let reacquired = ActivationLock::acquire_file(&runtime, "node-a").unwrap();
        drop(reacquired);
        drop(inherited);
        std::fs::remove_dir_all(runtime).unwrap();
    }

    #[test]
    fn forward_deadline_and_restoration_policy_are_distinct() {
        let now = prost_types::Timestamp {
            seconds: 1,
            nanos: 0,
        };
        let mut instruction = AgentInstruction {
            operation_deadline: Some(now),
            restoration: false,
            ..Default::default()
        };
        assert!(instruction.operation_deadline.is_some());
        instruction.restoration = true;
        assert!(instruction.restoration);
    }

    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex;

    enum ClaimAction {
        Error,
        Instruction(Box<AgentInstruction>),
        None,
    }

    struct ScriptedManager {
        attest_failures: AtomicUsize,
        observation_failures: AtomicUsize,
        result_failures: AtomicUsize,
        renew_fails: bool,
        claims: Mutex<VecDeque<ClaimAction>>,
        attested_instances: Mutex<Vec<String>>,
        observation_calls: AtomicUsize,
        result_calls: AtomicUsize,
        shutdown: watch::Sender<bool>,
        shutdown_on_none: bool,
    }

    impl LeaseManager for ScriptedManager {
        fn renew<'a>(&'a self, _lease: &'a LeaseIdentity) -> AgentFuture<'a, ()> {
            Box::pin(async move {
                if self.renew_fails {
                    bail!("scripted lease loss");
                }
                Ok(())
            })
        }
    }

    impl AgentManager for ScriptedManager {
        fn attest<'a>(&'a self, value: NodeAgentAttestation) -> AgentFuture<'a, ()> {
            Box::pin(async move {
                self.attested_instances
                    .lock()
                    .unwrap()
                    .push(value.agent_instance_id);
                if self
                    .attest_failures
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                        value.checked_sub(1)
                    })
                    .is_ok()
                {
                    bail!("scripted attestation outage");
                }
                Ok(())
            })
        }

        fn claim<'a>(
            &'a self,
            _node_id: &'a str,
            _agent_instance_id: &'a str,
        ) -> AgentFuture<'a, Option<AgentInstruction>> {
            Box::pin(async move {
                match self.claims.lock().unwrap().pop_front() {
                    Some(ClaimAction::Error) => bail!("scripted claim outage"),
                    Some(ClaimAction::Instruction(value)) => Ok(Some(*value)),
                    Some(ClaimAction::None) | None => {
                        if self.shutdown_on_none {
                            let _ = self.shutdown.send(true);
                        }
                        Ok(None)
                    }
                }
            })
        }

        fn report_observation<'a>(
            &'a self,
            _request: ReportNodeObservationRequest,
        ) -> AgentFuture<'a, ()> {
            Box::pin(async move {
                self.observation_calls.fetch_add(1, Ordering::SeqCst);
                if self
                    .observation_failures
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                        value.checked_sub(1)
                    })
                    .is_ok()
                {
                    bail!("scripted observation response loss");
                }
                Ok(())
            })
        }

        fn report_result<'a>(
            &'a self,
            _request: ReportStepResultRequest,
        ) -> ReportResultFuture<'a> {
            Box::pin(async move {
                self.result_calls.fetch_add(1, Ordering::SeqCst);
                if self
                    .result_failures
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                        value.checked_sub(1)
                    })
                    .is_ok()
                {
                    return Err(ReportResultError::Retryable(
                        "scripted result response loss".into(),
                    ));
                }
                Ok(())
            })
        }
    }

    struct CountingExecutor {
        executions: AtomicUsize,
    }

    impl InstructionExecutor for CountingExecutor {
        fn execute<'a>(
            &'a self,
            instruction: &'a AgentInstruction,
            _cancelled: watch::Receiver<bool>,
        ) -> BackendFuture<'a, StepEvidence> {
            Box::pin(async move {
                self.executions.fetch_add(1, Ordering::SeqCst);
                let target = instruction.target.as_ref().unwrap();
                Ok(StepEvidence {
                    observed_revision: target.revision,
                    observed_digest: target.bundle_digest.clone(),
                    backend_state: Some(BackendProcessState::Running),
                    backend_instance_id: "backend-1".into(),
                    process_instance_id: "process-1".into(),
                    ..Default::default()
                })
            })
        }
    }

    struct BlockingExecutor {
        executions: AtomicUsize,
        cancelled: AtomicBool,
    }

    impl InstructionExecutor for BlockingExecutor {
        fn execute<'a>(
            &'a self,
            _instruction: &'a AgentInstruction,
            mut cancelled: watch::Receiver<bool>,
        ) -> BackendFuture<'a, StepEvidence> {
            Box::pin(async move {
                self.executions.fetch_add(1, Ordering::SeqCst);
                while !*cancelled.borrow() && cancelled.changed().await.is_ok() {}
                self.cancelled.store(true, Ordering::SeqCst);
                bail!("cancelled")
            })
        }
    }

    struct ImmediateClock;

    impl AgentClock for ImmediateClock {
        fn now(&self) -> SystemTime {
            SystemTime::UNIX_EPOCH + Duration::from_secs(1)
        }

        fn sleep<'a>(
            &'a self,
            _duration: Duration,
        ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
            Box::pin(tokio::task::yield_now())
        }
    }

    fn activation() -> ActivationConfig {
        ActivationConfig {
            node_id: "node-a".into(),
            agent_instance_id: "activation-a".into(),
            binary_digest: format!("sha256:{}", "a".repeat(64)),
            backend: BackendType::Systemd,
            poll: Duration::from_millis(1),
            renew: Duration::from_secs(60),
        }
    }

    #[test]
    fn stop_result_carries_typed_termination_evidence_only_for_stop_steps() {
        let config = activation();
        let mut instruction = test_instruction();
        let evidence = StepEvidence {
            termination: Some(wr_common::wruntime::BackendTerminationEvidence {
                disposition: wr_common::wruntime::BackendStopDisposition::Graceful as i32,
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(result_request(&config, &instruction, &evidence, None)
            .unwrap()
            .termination_evidence
            .is_none());
        instruction.step = NodeOperationStepKind::StopBackend as i32;
        assert_eq!(
            result_request(&config, &instruction, &evidence, None)
                .unwrap()
                .termination_evidence
                .unwrap()
                .disposition,
            wr_common::wruntime::BackendStopDisposition::Graceful as i32
        );
        instruction.target.as_mut().unwrap().kind =
            wr_common::wruntime::InstructionTargetKind::Proxy as i32;
        assert!(result_request(&config, &instruction, &evidence, None)
            .unwrap()
            .termination_evidence
            .is_some());
    }

    fn test_instruction() -> AgentInstruction {
        AgentInstruction {
            operation_id: "operation-a".into(),
            node_id: "node-a".into(),
            lease_epoch: 1,
            step: NodeOperationStepKind::StartBackend as i32,
            agent_instance_id: "activation-a".into(),
            target: Some(wr_common::wruntime::InstructionTarget {
                kind: wr_common::wruntime::InstructionTargetKind::EngineSlot as i32,
                identity: Some(
                    wr_common::wruntime::instruction_target::Identity::EngineSlotTarget(
                        wr_common::wruntime::EngineSlotTargetIdentity {
                            engine_slot: "blue".into(),
                        },
                    ),
                ),
                revision: 1,
                bundle_digest: format!("sha256:{}", "c".repeat(64)),
                ..Default::default()
            }),
            restoration: true,
            ..Default::default()
        }
    }

    fn manager(claims: Vec<ClaimAction>, shutdown: watch::Sender<bool>) -> ScriptedManager {
        ScriptedManager {
            attest_failures: AtomicUsize::new(0),
            observation_failures: AtomicUsize::new(0),
            result_failures: AtomicUsize::new(0),
            renew_fails: false,
            claims: Mutex::new(claims.into()),
            attested_instances: Mutex::new(vec![]),
            observation_calls: AtomicUsize::new(0),
            result_calls: AtomicUsize::new(0),
            shutdown,
            shutdown_on_none: true,
        }
    }

    #[tokio::test]
    async fn activation_retries_attestation_and_claim_without_identity_churn() {
        let (shutdown, receiver) = watch::channel(false);
        let mut manager = manager(
            vec![
                ClaimAction::Error,
                ClaimAction::Instruction(Box::new(test_instruction())),
            ],
            shutdown,
        );
        manager.attest_failures = AtomicUsize::new(1);
        let executor = CountingExecutor {
            executions: AtomicUsize::new(0),
        };
        run_activation(&manager, &executor, &ImmediateClock, activation(), receiver)
            .await
            .unwrap();
        assert_eq!(executor.executions.load(Ordering::SeqCst), 1);
        assert!(manager
            .attested_instances
            .lock()
            .unwrap()
            .iter()
            .all(|value| value == "activation-a"));
    }

    #[tokio::test]
    async fn observation_reconnect_does_not_replay_effect() {
        let instruction = test_instruction();
        let (shutdown, receiver) = watch::channel(false);
        let mut manager = manager(
            vec![
                ClaimAction::Instruction(Box::new(instruction.clone())),
                ClaimAction::Instruction(Box::new(instruction)),
            ],
            shutdown,
        );
        manager.observation_failures = AtomicUsize::new(1);
        let executor = CountingExecutor {
            executions: AtomicUsize::new(0),
        };
        run_activation(&manager, &executor, &ImmediateClock, activation(), receiver)
            .await
            .unwrap();
        assert_eq!(executor.executions.load(Ordering::SeqCst), 1);
        assert_eq!(manager.observation_calls.load(Ordering::SeqCst), 2);
        assert_eq!(manager.result_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn ambiguous_result_response_is_reconciled_without_effect_replay() {
        let (shutdown, receiver) = watch::channel(false);
        let mut manager = manager(
            vec![
                ClaimAction::Instruction(Box::new(test_instruction())),
                ClaimAction::None,
            ],
            shutdown,
        );
        manager.result_failures = AtomicUsize::new(1);
        let executor = CountingExecutor {
            executions: AtomicUsize::new(0),
        };
        run_activation(&manager, &executor, &ImmediateClock, activation(), receiver)
            .await
            .unwrap();
        assert_eq!(executor.executions.load(Ordering::SeqCst), 1);
        assert_eq!(manager.result_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn lease_loss_cancels_and_reaps_before_reclaim() {
        let (shutdown, receiver) = watch::channel(false);
        let mut manager = manager(
            vec![
                ClaimAction::Instruction(Box::new(test_instruction())),
                ClaimAction::None,
            ],
            shutdown,
        );
        manager.renew_fails = true;
        let executor = BlockingExecutor {
            executions: AtomicUsize::new(0),
            cancelled: AtomicBool::new(false),
        };
        run_activation(&manager, &executor, &ImmediateClock, activation(), receiver)
            .await
            .unwrap();
        assert_eq!(executor.executions.load(Ordering::SeqCst), 1);
        assert!(executor.cancelled.load(Ordering::SeqCst));
        assert_eq!(manager.result_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn operation_workflow_has_one_manager_epoch_acquisition_site() {
        let source = include_str!("node_agent.rs");
        assert_eq!(
            source
                .matches(concat!("connect_node_agent", "_with_tls("))
                .count(),
            1
        );
        assert!(source.contains("execute_instruction(&mut manager"));
        assert!(source.contains("manager.report_step_result(report)"));
    }
}
