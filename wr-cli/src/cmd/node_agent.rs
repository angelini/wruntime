//! Binary adapter and importable core for the continuously fenced node agent.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::time::{Duration, SystemTime};

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use prost::Message;
use sha2::{Digest, Sha256};
use tokio::sync::watch;
use wr_common::agent_policy::{
    AgentPolicy, AgentPolicyBackend, AGENT_CAPABILITIES, AGENT_PROTOCOL_VERSION,
};
use wr_common::node::TlsConfig;
use wr_common::wruntime::{
    AgentInstruction, AttestNodeAgentRequest, BackendKind, ClaimOperationRequest,
    GetOperatorStatusRequest, NodeAgentAttestation, NodeAgentPolicy, NodeOperationStepKind,
    PutNodeAgentPolicyRequest, RenewOperationLeaseRequest, ReportNodeObservationRequest,
    ReportStepResultRequest,
};

use super::bundle;
use super::bundle_integrity::{verify_bundle_archive, BundleManifest};
use super::deploy_config::{self, DeployConfig, DeployFormat};
use super::helpers;
use super::node_backend::{
    validate_identity, validate_root_owned_directory, BackendType, HostBackend, HostBackendConfig,
    InstructionExecutor, StepEvidence,
};
use super::service_gen;
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
    /// Deploy configuration file (default: auto-discover wr-deploy.toml).
    #[arg(long)]
    pub config: Option<String>,
    /// Workload backend controlled by this host agent.
    #[arg(long)]
    pub format: Option<super::deploy_config::DeployFormat>,
    #[arg(long)]
    pub ssh_key: Option<String>,
    #[arg(long)]
    pub ssh_port: Option<u16>,
    /// Local node-agent mTLS certificate.
    #[arg(long)]
    pub agent_cert: Option<String>,
    /// Local node-agent mTLS private key.
    #[arg(long)]
    pub agent_key: Option<String>,
    /// Local CA certificate used to authenticate the manager.
    #[arg(long)]
    pub agent_ca_cert: Option<String>,
    #[arg(long)]
    pub systemctl_path: Option<String>,
    #[arg(long)]
    pub docker_path: Option<String>,
    #[arg(long)]
    pub compose_project: Option<String>,
    #[arg(long)]
    pub poll_seconds: Option<u64>,
    #[arg(long)]
    pub renew_seconds: Option<u64>,
    #[arg(long)]
    pub retention_count: Option<u32>,
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
    pub fn load(path: &Path) -> Result<(Self, String)> {
        let bytes = std::fs::read(path)
            .with_context(|| format!("failed to read node-agent config {}", path.display()))?;
        let metadata = path.metadata()?;
        if metadata.uid() != 0 || metadata.permissions().mode() & 0o077 != 0 {
            bail!("node-agent config must be root-owned and owner-only");
        }
        let policy = AgentPolicy::from_canonical_bytes(&bytes)
            .with_context(|| format!("failed to parse node-agent config {}", path.display()))?;
        let digest = policy.canonical_digest()?;
        Ok((Self(policy), digest))
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

    fn tls(&self) -> TlsConfig {
        TlsConfig {
            cert_path: self.client_cert_path.clone(),
            key_path: self.client_key_path.clone(),
            ca_cert_path: self.ca_cert_path.clone(),
        }
    }

    fn backend_config(&self) -> HostBackendConfig {
        HostBackendConfig {
            deployment_root: PathBuf::from(&self.deployment_root),
            backend: self.backend(),
            systemctl_path: (!self.systemctl_path.is_empty())
                .then(|| PathBuf::from(&self.systemctl_path)),
            docker_path: (!self.docker_path.is_empty()).then(|| PathBuf::from(&self.docker_path)),
            compose_project: (!self.compose_project.is_empty())
                .then(|| self.compose_project.clone()),
            retention_count: self.retention_count as usize,
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

struct ProductionManager<'a> {
    config: &'a AgentConfig,
}

impl LeaseManager for ProductionManager<'_> {
    fn renew<'a>(&'a self, lease: &'a LeaseIdentity) -> AgentFuture<'a, ()> {
        Box::pin(async move {
            let tls = self.config.tls();
            client::connect_node_agent_with_tls(&self.config.manager_endpoint, Some(&tls))
                .await?
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

impl AgentManager for ProductionManager<'_> {
    fn attest<'a>(&'a self, attestation: NodeAgentAttestation) -> AgentFuture<'a, ()> {
        Box::pin(async move {
            let tls = self.config.tls();
            let response =
                client::connect_node_agent_with_tls(&self.config.manager_endpoint, Some(&tls))
                    .await?
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
            let tls = self.config.tls();
            Ok(
                client::connect_node_agent_with_tls(&self.config.manager_endpoint, Some(&tls))
                    .await?
                    .claim_operation(ClaimOperationRequest {
                        node_id: node_id.to_string(),
                        agent_instance_id: agent_instance_id.to_string(),
                    })
                    .await?
                    .into_inner()
                    .instruction,
            )
        })
    }

    fn report_observation<'a>(
        &'a self,
        request: ReportNodeObservationRequest,
    ) -> AgentFuture<'a, ()> {
        Box::pin(async move {
            let tls = self.config.tls();
            client::connect_node_agent_with_tls(&self.config.manager_endpoint, Some(&tls))
                .await?
                .report_observation(request)
                .await?;
            Ok(())
        })
    }

    fn report_result<'a>(&'a self, request: ReportStepResultRequest) -> ReportResultFuture<'a> {
        Box::pin(async move {
            let tls = self.config.tls();
            let mut manager =
                client::connect_node_agent_with_tls(&self.config.manager_endpoint, Some(&tls))
                    .await
                    .map_err(|error| ReportResultError::Retryable(format!("{error:#}")))?;
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
    pub config_digest: String,
    pub backend: BackendType,
    pub retention_count: u32,
    pub recovery_dir: Option<PathBuf>,
    pub poll: Duration,
    pub renew: Duration,
}

fn attestation(config: &ActivationConfig) -> NodeAgentAttestation {
    NodeAgentAttestation {
        node_id: config.node_id.clone(),
        agent_instance_id: config.agent_instance_id.clone(),
        protocol_version: AGENT_PROTOCOL_VERSION.to_string(),
        binary_digest: config.binary_digest.clone(),
        config_digest: config.config_digest.clone(),
        backend: config.backend.wire() as i32,
        capabilities: AGENT_CAPABILITIES
            .iter()
            .map(|value| (*value).to_string())
            .collect(),
        retention_count: config.retention_count,
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
    let Some(state) = evidence.backend_state else {
        return Ok(None);
    };
    Ok(Some(ReportNodeObservationRequest {
        node_id: config.node_id.clone(),
        engine_slot: target.engine_slot.clone(),
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
        engine_slot: target.engine_slot.clone(),
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
        cleanup_evidence: evidence.cleanup_evidence.clone(),
        observed_resolved_release_digest: evidence.observed_resolved_release_digest.clone(),
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

const RECOVERY_MAGIC: &[u8] = b"WR-AGENT-RECOVERY-V1\n";

fn recovery_record_bytes(
    instruction: &AgentInstruction,
    result: Option<&ReportStepResultRequest>,
) -> Vec<u8> {
    let instruction = instruction.encode_to_vec();
    let result = result.map(Message::encode_to_vec).unwrap_or_default();
    let mut bytes =
        Vec::with_capacity(RECOVERY_MAGIC.len() + instruction.len() + result.len() + 79);
    bytes.extend_from_slice(RECOVERY_MAGIC);
    bytes.extend_from_slice(&(instruction.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&instruction);
    bytes.extend_from_slice(&(result.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&result);
    let digest = wr_common::agent_policy::sha256_digest(&bytes);
    bytes.extend_from_slice(digest.as_bytes());
    bytes.push(b'\n');
    bytes
}

#[derive(Clone, Debug)]
struct RecoveryRecord {
    instruction: AgentInstruction,
    result: Option<ReportStepResultRequest>,
}

fn decode_recovery_record(bytes: &[u8]) -> Result<RecoveryRecord> {
    if bytes.len() < RECOVERY_MAGIC.len() + 4 + 4 + 72
        || !bytes.starts_with(RECOVERY_MAGIC)
        || *bytes.last().unwrap_or(&0) != b'\n'
    {
        bail!("local recovery metadata has an invalid envelope");
    }
    let digest_start = bytes.len() - 72;
    let expected = std::str::from_utf8(&bytes[digest_start..bytes.len() - 1])?;
    if wr_common::agent_policy::sha256_digest(&bytes[..digest_start]) != expected {
        bail!("local recovery metadata digest mismatch");
    }
    let mut cursor = RECOVERY_MAGIC.len();
    let instruction_len =
        u32::from_be_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
    cursor += 4;
    let instruction_end = cursor
        .checked_add(instruction_len)
        .context("local recovery instruction length overflow")?;
    if instruction_end + 4 > digest_start {
        bail!("local recovery instruction length is invalid");
    }
    let instruction = AgentInstruction::decode(&bytes[cursor..instruction_end])
        .context("local recovery instruction is invalid")?;
    cursor = instruction_end;
    let result_len = u32::from_be_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
    cursor += 4;
    let result_end = cursor
        .checked_add(result_len)
        .context("local recovery result length overflow")?;
    if result_end != digest_start {
        bail!("local recovery result length is invalid");
    }
    let result = if result_len > 0 {
        Some(
            ReportStepResultRequest::decode(&bytes[cursor..result_end])
                .context("local recovery result is invalid")?,
        )
    } else {
        None
    };
    if let Some(result) = result.as_ref() {
        if result.operation_id != instruction.operation_id
            || result.node_id != instruction.node_id
            || result.lease_epoch != instruction.lease_epoch
            || result.agent_instance_id != instruction.agent_instance_id
            || result.step != instruction.step
        {
            bail!("local recovery result does not match its instruction fence");
        }
    }
    Ok(RecoveryRecord {
        instruction,
        result,
    })
}

fn validate_recovery_records(directory: &Path) -> Result<()> {
    validate_root_owned_directory(directory, "recovery_dir")?;
    for entry in std::fs::read_dir(directory).context("recovery_dir is unreadable")? {
        let entry = entry.context("recovery record is unreadable")?;
        if !entry.file_type()?.is_file() {
            bail!("recovery_dir contains a non-file entry");
        }
        let metadata = entry.metadata()?;
        if metadata.uid() != 0 || metadata.permissions().mode() & 0o077 != 0 {
            bail!("local recovery metadata must be root-owned and owner-only");
        }
        let _ = decode_recovery_record(&std::fs::read(entry.path())?)?;
    }
    Ok(())
}

fn load_recovery_records(config: &ActivationConfig) -> Result<BTreeMap<String, RecoveryRecord>> {
    let Some(directory) = config.recovery_dir.as_ref() else {
        return Ok(BTreeMap::new());
    };
    let mut records = BTreeMap::new();
    for entry in std::fs::read_dir(directory).context("recovery_dir is unreadable")? {
        let entry = entry.context("recovery record is unreadable")?;
        if !entry.file_type()?.is_file() {
            bail!("recovery_dir contains a non-file entry");
        }
        let metadata = entry.metadata()?;
        if metadata.permissions().mode() & 0o077 != 0 {
            bail!("local recovery metadata must be owner-only");
        }
        let record = decode_recovery_record(&std::fs::read(entry.path())?)?;
        validate_identity(&record.instruction.operation_id, "recovery operation_id")?;
        let expected_name = format!("{}.state", record.instruction.operation_id);
        if entry.file_name().to_string_lossy() != expected_name {
            bail!("local recovery filename does not match its operation identity");
        }
        if record.instruction.node_id != config.node_id {
            bail!("local recovery metadata belongs to a different node");
        }
        if records
            .insert(record.instruction.operation_id.clone(), record)
            .is_some()
        {
            bail!("recovery_dir contains a duplicate operation record");
        }
    }
    Ok(records)
}

fn remove_recovery_record(config: &ActivationConfig, operation_id: &str) -> Result<()> {
    let Some(directory) = config.recovery_dir.as_ref() else {
        return Ok(());
    };
    validate_identity(operation_id, "operation_id")?;
    let target = directory.join(format!("{operation_id}.state"));
    match std::fs::remove_file(&target) {
        Ok(()) => File::open(directory)?.sync_all()?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("failed to remove acknowledged recovery record"),
    }
    Ok(())
}

fn persist_recovery_record(
    config: &ActivationConfig,
    instruction: &AgentInstruction,
    result: Option<&ReportStepResultRequest>,
) -> Result<()> {
    let Some(directory) = config.recovery_dir.as_ref() else {
        return Ok(());
    };
    validate_identity(&instruction.operation_id, "operation_id")?;
    let metadata = std::fs::metadata(directory).context("recovery_dir is unavailable")?;
    if !metadata.is_dir() || metadata.permissions().mode() & 0o077 != 0 {
        bail!("recovery_dir must be an owner-only directory");
    }
    let target = directory.join(format!("{}.state", instruction.operation_id));
    let temporary = directory.join(format!(
        ".{}.{}.tmp",
        instruction.operation_id, config.agent_instance_id
    ));
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(&temporary)?;
    file.write_all(&recovery_record_bytes(instruction, result))?;
    file.sync_all()?;
    std::fs::rename(&temporary, &target)?;
    File::open(directory)?.sync_all()?;
    Ok(())
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
    // Recovery is loaded before the first attestation/claim. Stored results are
    // never replayed under this new activation: the manager must correlate the
    // durable delivery and return an inspection (or a safe read-only step).
    let mut recovery_records = load_recovery_records(&config)?;
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
            let operation_id = request.operation_id.clone();
            match manager.report_result(request).await {
                Ok(()) => {
                    remove_recovery_record(&config, &operation_id)?;
                    recovery_records.remove(&operation_id);
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
                    remove_recovery_record(&config, &request.operation_id)?;
                    recovery_records.remove(&request.operation_id);
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
                            remove_recovery_record(&config, &request.operation_id)?;
                            recovery_records.remove(&request.operation_id);
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
        while recovery_records
            .first_key_value()
            .is_some_and(|(operation_id, _)| instruction.operation_id != *operation_id)
        {
            // The manager can expose only one active operation per node. A
            // different claimed operation therefore proves this oldest record
            // no longer corresponds to manager-active work.
            let stale = recovery_records.first_key_value().unwrap().0.clone();
            remove_recovery_record(&config, &stale)?;
            recovery_records.remove(&stale);
        }
        if let Some(record) = recovery_records.get(&instruction.operation_id) {
            let delivered_mutation = matches!(
                NodeOperationStepKind::try_from(record.instruction.step)
                    .unwrap_or(NodeOperationStepKind::Unspecified),
                NodeOperationStepKind::StopBackend
                    | NodeOperationStepKind::SelectRelease
                    | NodeOperationStepKind::StartBackend
                    | NodeOperationStepKind::RestoreSource
                    | NodeOperationStepKind::CleanupRelease
            );
            let exact_cleanup_retry = NodeOperationStepKind::try_from(record.instruction.step)
                .unwrap_or(NodeOperationStepKind::Unspecified)
                == NodeOperationStepKind::CleanupRelease
                && step == NodeOperationStepKind::CleanupRelease
                && record.instruction.target == instruction.target
                && record.instruction.cleanup_delete_releases
                    == instruction.cleanup_delete_releases;
            if delivered_mutation
                && step != NodeOperationStepKind::InspectBackend
                && !exact_cleanup_retry
            {
                bail!(
                    "manager attempted to replay a recovery-ambiguous mutation without inspection"
                );
            }
            // Merely decoding a stored result is intentional: it proves the
            // envelope/fences correlate, but the old activation's request is
            // never copied into pending_result.
            let _old_result_was_complete = record.result.is_some();
        }
        persist_recovery_record(&config, &instruction, None)?;
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
                if step != NodeOperationStepKind::InspectBackend {
                    let report =
                        result_request(&config, &instruction, &evidence, failure.as_ref())?;
                    persist_recovery_record(&config, &instruction, Some(&report))?;
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
    config: Vec<u8>,
    certificate: Vec<u8>,
    private_key: Vec<u8>,
    ca_certificate: Vec<u8>,
    protocol_marker: Vec<u8>,
    unit: Vec<u8>,
}

impl AgentInstallMaterial {
    fn named_payloads(&self) -> [(&'static str, &[u8], u32); 7] {
        [
            ("wr-cli", &self.binary, 0o755),
            ("agent.toml", &self.config, 0o600),
            ("agent.crt", &self.certificate, 0o600),
            ("agent.key", &self.private_key, 0o600),
            ("ca.crt", &self.ca_certificate, 0o600),
            ("protocol-version", &self.protocol_marker, 0o644),
            ("wr-node-agent.service", &self.unit, 0o644),
        ]
    }
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

fn wire_policy(
    policy: &AgentPolicy,
    binary_digest: String,
    config_digest: String,
) -> NodeAgentPolicy {
    NodeAgentPolicy {
        node_id: policy.node_id.clone(),
        protocol_version: policy.protocol_version.clone(),
        config_digest,
        backend: match policy.backend {
            AgentPolicyBackend::Systemd => BackendKind::Systemd,
            AgentPolicyBackend::Docker => BackendKind::Docker,
        } as i32,
        retention_count: policy.retention_count,
        policy_version: policy.policy_version,
        binary_digest,
        manager_endpoint: policy.manager_endpoint.clone(),
        client_cert_path: policy.client_cert_path.clone(),
        client_key_path: policy.client_key_path.clone(),
        ca_cert_path: policy.ca_cert_path.clone(),
        deployment_root: policy.deployment_root.clone(),
        runtime_dir: policy.runtime_dir.clone(),
        compose_project: policy.compose_project.clone(),
        systemctl_path: policy.systemctl_path.clone(),
        docker_path: policy.docker_path.clone(),
        poll_interval_seconds: policy.poll_interval_seconds,
        renew_interval_seconds: policy.renew_interval_seconds,
        capabilities: policy.capabilities.clone(),
    }
}

fn attestation_matches_policy(
    attestation: &NodeAgentAttestation,
    policy: &NodeAgentPolicy,
) -> bool {
    let fresh = attestation.observed_at.as_ref().is_some_and(|observed| {
        chrono::DateTime::from_timestamp(observed.seconds, observed.nanos as u32)
            .is_some_and(|time| chrono::Utc::now().signed_duration_since(time).num_seconds() <= 30)
    });
    fresh
        && attestation.node_id == policy.node_id
        && attestation.protocol_version == policy.protocol_version
        && attestation.binary_digest == policy.binary_digest
        && attestation.config_digest == policy.config_digest
        && attestation.backend == policy.backend
        && attestation.retention_count == policy.retention_count
        && attestation.capabilities == policy.capabilities
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
    let attestations = client::connect_operator(manager)
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
    material
        .named_payloads()
        .iter()
        .map(|(name, bytes, mode)| {
            let path = if *name == "wr-node-agent.service" {
                "/etc/systemd/system/wr-node-agent.service".to_string()
            } else if matches!(*name, "agent.crt" | "agent.key" | "ca.crt") {
                format!("{workdir}/wr-agent/certs/{name}")
            } else {
                format!("{workdir}/wr-agent/{name}")
            };
            let digest = wr_common::agent_policy::sha256_digest(bytes);
            format!(
                "sudo test -f {path} && test \"$(sudo sha256sum {path} | cut -d' ' -f1)\" = {} && test \"$(sudo stat -c '%u:%a' {path})\" = '0:{mode:o}'",
                digest.trim_start_matches("sha256:")
            )
        })
        .collect::<Vec<_>>()
        .join(" && ")
}

fn staged_payload_checks_command(agent_root: &str, material: &AgentInstallMaterial) -> String {
    material
        .named_payloads()
        .iter()
        .map(|(name, bytes, _)| {
            let path = if *name == "wr-node-agent.service" {
                format!("{agent_root}/.wr-node-agent.service.new")
            } else {
                format!("{agent_root}/.{name}.new")
            };
            format!(
                "test \"$(sudo sha256sum {path} | cut -d' ' -f1)\" = {} && sudo sync -f {path}",
                wr_common::agent_policy::sha256_digest(bytes).trim_start_matches("sha256:")
            )
        })
        .collect::<Vec<_>>()
        .join(" && ")
}

fn remote_activate_command(workdir: &str, material: &AgentInstallMaterial) -> String {
    let mut installs = Vec::new();
    for (name, _, mode) in material.named_payloads() {
        let (staged, target) = if name == "wr-node-agent.service" {
            (
                format!("{workdir}/wr-agent/.wr-node-agent.service.new"),
                "/etc/systemd/system/wr-node-agent.service".to_string(),
            )
        } else if matches!(name, "agent.crt" | "agent.key" | "ca.crt") {
            (
                format!("{workdir}/wr-agent/.{name}.new"),
                format!("{workdir}/wr-agent/certs/{name}"),
            )
        } else {
            (
                format!("{workdir}/wr-agent/.{name}.new"),
                format!("{workdir}/wr-agent/{name}"),
            )
        };
        installs.push(format!(
            "sudo install -o root -g root -m {mode:o} {staged} {target} && sudo sync -f {target}"
        ));
    }
    format!(
        "{} && sudo systemctl daemon-reload && sudo systemctl enable wr-node-agent.service && sudo systemctl restart wr-node-agent.service",
        installs.join(" && ")
    )
}

async fn install(args: AgentInstallArgs, manager: &str, updating: bool) -> Result<()> {
    validate_identity(&args.node_id, "node_id")?;
    let deploy = DeployConfig::load_or_discover(args.config.as_deref())?;
    let format = deploy_config::resolve_format(args.format, deploy.format);
    let ssh_key = deploy_config::resolve_string(args.ssh_key, deploy.ssh_key, "WR_SSH_KEY");
    let ssh_port = deploy_config::resolve_ssh_port(args.ssh_port, deploy.ssh_port)?
        .map(helpers::DeployPort::get);
    let agent_cert = deploy_config::resolve_required(
        args.agent_cert,
        deploy.agent_cert,
        "WR_AGENT_CERT",
        "agent_cert",
    )?;
    let agent_key = deploy_config::resolve_required(
        args.agent_key,
        deploy.agent_key,
        "WR_AGENT_KEY",
        "agent_key",
    )?;
    let agent_ca = deploy_config::resolve_required(
        args.agent_ca_cert,
        deploy.agent_ca_cert,
        "WR_AGENT_CA_CERT",
        "agent_ca_cert",
    )?;
    let manifest: BundleManifest = bundle::read_manifest(&args.bundle)?;
    verify_bundle_archive(&args.bundle, &manifest)?;
    safe_install_path(&manifest.workdir, "bundle workdir")?;
    let agent_root = format!("{}/wr-agent", manifest.workdir);
    let cert_root = format!("{agent_root}/certs");
    let backend = match format {
        DeployFormat::Systemd => AgentPolicyBackend::Systemd,
        DeployFormat::Docker => AgentPolicyBackend::Docker,
    };
    let policy = AgentPolicy {
        policy_version: wr_common::agent_policy::AGENT_POLICY_VERSION,
        node_id: args.node_id,
        manager_endpoint: manager.to_string(),
        client_cert_path: format!("{cert_root}/agent.crt"),
        client_key_path: format!("{cert_root}/agent.key"),
        ca_cert_path: format!("{cert_root}/ca.crt"),
        deployment_root: manifest.workdir.clone(),
        runtime_dir: "/run/wruntime".into(),
        backend,
        compose_project: if backend == AgentPolicyBackend::Docker {
            args.compose_project
                .or(deploy.agent_compose_project)
                .unwrap_or_else(|| "wruntime-node".into())
        } else {
            String::new()
        },
        systemctl_path: if backend == AgentPolicyBackend::Systemd {
            args.systemctl_path
                .or(deploy.agent_systemctl_path)
                .unwrap_or_else(|| "/usr/bin/systemctl".into())
        } else {
            String::new()
        },
        docker_path: if backend == AgentPolicyBackend::Docker {
            args.docker_path
                .or(deploy.agent_docker_path)
                .unwrap_or_else(|| "/usr/bin/docker".into())
        } else {
            String::new()
        },
        poll_interval_seconds: args.poll_seconds.or(deploy.agent_poll_seconds).unwrap_or(5),
        renew_interval_seconds: args
            .renew_seconds
            .or(deploy.agent_renew_seconds)
            .unwrap_or(5),
        retention_count: args
            .retention_count
            .or(deploy.agent_retention_count)
            .unwrap_or(3),
        protocol_version: AGENT_PROTOCOL_VERSION.into(),
        capabilities: AGENT_CAPABILITIES
            .iter()
            .map(|value| (*value).to_string())
            .collect(),
    }
    .normalized()?;
    let config = policy.canonical_bytes()?;
    let binary = bundle::read_bytes_from_tarball(&args.bundle, "wr-node/agent/wr-cli")?;
    let protocol_marker =
        bundle::read_bytes_from_tarball(&args.bundle, "wr-node/agent/protocol-version")?;
    if protocol_marker != format!("{AGENT_PROTOCOL_VERSION}\n").as_bytes() {
        bail!("bundle node-agent protocol marker does not match this installer");
    }
    let unit =
        bundle::read_bytes_from_tarball(&args.bundle, "wr-node/agent/wr-node-agent.service")?;
    if unit != service_gen::node_agent_systemd_unit(&manifest.workdir).as_bytes() {
        bail!("bundle node-agent unit is not the canonical hardened host unit");
    }
    let material = AgentInstallMaterial {
        binary,
        config,
        certificate: std::fs::read(&agent_cert)
            .with_context(|| format!("failed to read {agent_cert}"))?,
        private_key: std::fs::read(&agent_key)
            .with_context(|| format!("failed to read {agent_key}"))?,
        ca_certificate: std::fs::read(&agent_ca)
            .with_context(|| format!("failed to read {agent_ca}"))?,
        protocol_marker,
        unit,
    };
    let binary_digest = wr_common::agent_policy::sha256_digest(&material.binary);
    let config_digest = policy.canonical_digest()?;
    let wire_policy = wire_policy(&policy, binary_digest, config_digest);
    let ssh = helpers::build_ssh_args(&args.remote, ssh_key.as_deref(), ssh_port);
    let prior_attestations =
        require_prior_attestations(node_attestations(manager, &wire_policy.node_id).await)?;
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
        client::connect_operator(manager)
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
        helpers::run_ssh(
            &ssh,
            "sudo systemctl daemon-reload && sudo systemctl enable wr-node-agent.service && sudo systemctl restart wr-node-agent.service",
        )
        .context("node-agent service restart failed")?;
    } else {
        helpers::run_ssh(
        &ssh,
        &format!(
            "sudo install -d -o root -g root -m 0755 {} {}/wr-node {}/wr-node/slots && sudo install -d -o root -g root -m 0700 {agent_root} {cert_root} {agent_root}/state /run/wruntime",
            manifest.workdir, manifest.workdir, manifest.workdir
        ),
    )?;
        for (name, bytes, _) in material.named_payloads() {
            let staged = if name == "wr-node-agent.service" {
                format!("{agent_root}/.wr-node-agent.service.new")
            } else {
                format!("{agent_root}/.{name}.new")
            };
            helpers::scp_bytes(bytes, &args.remote, &staged, ssh_key.as_deref(), ssh_port)?;
        }
        let staged_checks = staged_payload_checks_command(&agent_root, &material);
        helpers::run_ssh(&ssh, &staged_checks)
            .context("remote node-agent payload checksum mismatch")?;
        // Only after every new byte is durably staged does the manager expectation
        // move. A retry can safely finish activation if interruption follows.
        client::connect_operator(manager)
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
                            "waiting for a new exact protocol/config/binary/backend/capability/retention attestation".into(),
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
    let status = client::connect_operator(manager)
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
    let (config, config_digest) = AgentConfig::load(&args.config)?;
    config.validate()?;
    validate_root_owned_directory(Path::new(&config.deployment_root), "deployment_root")?;
    validate_root_owned_file(Path::new(&config.client_cert_path), "client certificate")?;
    validate_root_owned_file(Path::new(&config.client_key_path), "client private key")?;
    validate_root_owned_file(Path::new(&config.ca_cert_path), "CA certificate")?;
    let recovery_dir = PathBuf::from(&config.deployment_root).join("wr-agent/state");
    validate_recovery_records(&recovery_dir)?;
    let _activation_lock =
        ActivationLock::acquire(Path::new(&config.runtime_dir), &config.node_id)?;
    let backend = HostBackend::new(config.backend_config())?;
    let activation = ActivationConfig {
        node_id: config.node_id.clone(),
        agent_instance_id: uuid::Uuid::new_v4().to_string(),
        binary_digest: executable_digest()?,
        config_digest,
        backend: config.backend(),
        retention_count: config.retention_count,
        recovery_dir: Some(recovery_dir),
        poll: Duration::from_secs(config.poll_interval_seconds),
        renew: Duration::from_secs(config.renew_interval_seconds),
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = shutdown_tx.send(true);
    });
    run_activation(
        &ProductionManager { config: &config },
        &backend,
        &TokioClock,
        activation,
        shutdown_rx,
    )
    .await
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
            retention_count: 3,
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
    fn install_material_is_root_owned_and_never_touches_workloads() {
        let material = AgentInstallMaterial {
            binary: b"binary".to_vec(),
            config: base_config().canonical_bytes().unwrap(),
            certificate: b"certificate".to_vec(),
            private_key: b"private-key".to_vec(),
            ca_certificate: b"ca".to_vec(),
            protocol_marker: format!("{AGENT_PROTOCOL_VERSION}\n").into_bytes(),
            unit: service_gen::node_agent_systemd_unit("/opt/wruntime").into_bytes(),
        };
        let check = remote_payload_matches_command("/opt/wruntime", &material);
        assert!(check.contains("sudo test -f /opt/wruntime/wr-agent/agent.toml"));
        assert!(check.contains("sudo sha256sum /opt/wruntime/wr-agent/certs/agent.key"));
        assert!(check.contains("sudo stat -c '%u:%a' /opt/wruntime/wr-agent/certs/agent.crt"));
        assert!(!check.contains("&& sha256sum"));

        let staged = staged_payload_checks_command("/opt/wruntime/wr-agent", &material);
        for (name, bytes, _) in material.named_payloads() {
            let path = if name == "wr-node-agent.service" {
                "/opt/wruntime/wr-agent/.wr-node-agent.service.new".to_string()
            } else {
                format!("/opt/wruntime/wr-agent/.{name}.new")
            };
            let digest = wr_common::agent_policy::sha256_digest(bytes);
            assert!(staged.contains(&format!(
                "test \"$(sudo sha256sum {path} | cut -d' ' -f1)\" = {} && sudo sync -f {path}",
                digest.trim_start_matches("sha256:")
            )));
        }
        assert_eq!(staged.matches("$(sudo sha256sum").count(), 7);
        assert_eq!(staged.matches("sudo sync -f").count(), 7);
        assert!(!staged.contains("$(sha256sum"));

        let command = remote_activate_command("/opt/wruntime", &material);
        assert!(command.contains("install -o root -g root -m 600"));
        assert!(command.contains("systemctl restart wr-node-agent.service"));
        assert!(!command.contains("wr-engine"));
        assert!(!command.contains("wr-proxy"));
        assert!(!command.contains("docker compose"));
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
    fn recovery_metadata_is_digest_checked_and_typed() {
        let instruction = test_instruction();
        let report = ReportStepResultRequest {
            node_id: "node-a".into(),
            operation_id: instruction.operation_id.clone(),
            agent_instance_id: "activation-a".into(),
            lease_epoch: 1,
            step: instruction.step,
            ..Default::default()
        };
        let bytes = recovery_record_bytes(&instruction, Some(&report));
        decode_recovery_record(&bytes).unwrap();
        let mut corrupted = bytes;
        corrupted[RECOVERY_MAGIC.len() + 5] ^= 1;
        assert!(decode_recovery_record(&corrupted).is_err());
    }

    #[test]
    fn semantic_attestation_includes_retention_and_capabilities() {
        let policy = wire_policy(
            &base_config().0,
            format!("sha256:{}", "a".repeat(64)),
            base_config().canonical_digest().unwrap(),
        );
        let mut attested = NodeAgentAttestation {
            node_id: policy.node_id.clone(),
            protocol_version: policy.protocol_version.clone(),
            binary_digest: policy.binary_digest.clone(),
            config_digest: policy.config_digest.clone(),
            backend: policy.backend,
            capabilities: policy.capabilities.clone(),
            retention_count: policy.retention_count,
            observed_at: Some(prost_types::Timestamp {
                seconds: chrono::Utc::now().timestamp(),
                nanos: 0,
            }),
            ..Default::default()
        };
        assert!(attestation_matches_policy(&attested, &policy));
        attested.retention_count += 1;
        assert!(!attestation_matches_policy(&attested, &policy));
        attested.retention_count = policy.retention_count;
        attested.observed_at = Some(prost_types::Timestamp {
            seconds: chrono::Utc::now().timestamp() - 31,
            nanos: 0,
        });
        assert!(!attestation_matches_policy(&attested, &policy));
    }

    #[test]
    fn every_policy_field_changes_or_invalidates_expected_attestation() {
        let baseline = base_config().0;
        let baseline_digest = baseline.canonical_digest().unwrap();
        let expected = wire_policy(
            &baseline,
            format!("sha256:{}", "a".repeat(64)),
            baseline_digest.clone(),
        );
        let attested = NodeAgentAttestation {
            node_id: expected.node_id.clone(),
            protocol_version: expected.protocol_version.clone(),
            binary_digest: expected.binary_digest.clone(),
            config_digest: expected.config_digest.clone(),
            backend: expected.backend,
            capabilities: expected.capabilities.clone(),
            retention_count: expected.retention_count,
            observed_at: Some(prost_types::Timestamp {
                seconds: chrono::Utc::now().timestamp(),
                nanos: 0,
            }),
            ..Default::default()
        };
        type PolicyMutation = Box<dyn Fn(&mut AgentPolicy)>;
        let mutations: Vec<PolicyMutation> = vec![
            Box::new(|value| value.node_id = "node-b".into()),
            Box::new(|value| value.manager_endpoint = "https://other.example:9000".into()),
            Box::new(|value| value.client_cert_path = "/other/agent.crt".into()),
            Box::new(|value| value.client_key_path = "/other/agent.key".into()),
            Box::new(|value| value.ca_cert_path = "/other/ca.crt".into()),
            Box::new(|value| value.deployment_root = "/srv/wruntime".into()),
            Box::new(|value| value.runtime_dir = "/run/wruntime-other".into()),
            Box::new(|value| value.systemctl_path = "/opt/bin/systemctl".into()),
            Box::new(|value| value.poll_interval_seconds += 1),
            Box::new(|value| value.renew_interval_seconds += 1),
            Box::new(|value| value.retention_count += 1),
        ];
        for mutate in mutations {
            let mut changed = baseline.clone();
            mutate(&mut changed);
            let digest = changed.canonical_digest().unwrap();
            assert_ne!(digest, baseline_digest);
            assert!(!attestation_matches_policy(
                &attested,
                &wire_policy(&changed, expected.binary_digest.clone(), digest)
            ));
        }
        let mut docker = baseline.clone();
        docker.backend = AgentPolicyBackend::Docker;
        docker.systemctl_path.clear();
        docker.docker_path = "/usr/bin/docker".into();
        docker.compose_project = "wruntime-node".into();
        let docker_digest = docker.canonical_digest().unwrap();
        assert_ne!(docker_digest, baseline_digest);
        assert!(!attestation_matches_policy(
            &attested,
            &wire_policy(&docker, expected.binary_digest.clone(), docker_digest)
        ));
        for invalidate in [
            |value: &mut AgentPolicy| value.policy_version += 1,
            |value: &mut AgentPolicy| value.protocol_version = "other".into(),
            |value: &mut AgentPolicy| value.capabilities = vec!["incomplete".into()],
        ] as [fn(&mut AgentPolicy); 3]
        {
            let mut changed = baseline.clone();
            invalidate(&mut changed);
            assert!(changed.canonical_digest().is_err());
        }
        let mut binary_changed = expected.clone();
        binary_changed.binary_digest = format!("sha256:{}", "b".repeat(64));
        assert!(!attestation_matches_policy(&attested, &binary_changed));
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
            config_digest: format!("sha256:{}", "b".repeat(64)),
            backend: BackendType::Systemd,
            retention_count: 3,
            recovery_dir: None,
            poll: Duration::from_millis(1),
            renew: Duration::from_secs(60),
        }
    }

    fn test_instruction() -> AgentInstruction {
        AgentInstruction {
            operation_id: "operation-a".into(),
            node_id: "node-a".into(),
            lease_epoch: 1,
            step: NodeOperationStepKind::StartBackend as i32,
            agent_instance_id: "activation-a".into(),
            target: Some(wr_common::wruntime::InstructionTarget {
                engine_slot: "blue".into(),
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
}
