//! Constrained host backend for the node lifecycle agent.
//!
//! This module is the only production owner of systemd and Docker workload
//! effects.  Manager data selects a typed slot/revision; it never contributes
//! executable text or argv fragments.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
#[cfg(target_os = "linux")]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::process::Command;
use tokio::sync::watch;
use wr_common::wruntime::{
    instruction_target, AgentInstruction, BackendProcessState, BackendStopDisposition,
    BackendTerminationEvidence, CleanupReleaseEvidence, InstructionTarget, InstructionTargetKind,
    LifecycleStatus, NodeCleanupInstruction, NodeOperationStepKind, ProcessLifecycleState,
    ReleaseInventoryEntry, ServiceKind,
};

use super::bundle_integrity::{verify_resolved_release, BundleManifest};
use crate::client;

pub type BackendFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

pub(crate) enum WorkloadTarget<'a> {
    Proxy,
    EngineSlot(&'a str),
}

pub(crate) fn workload_target(target: &InstructionTarget) -> Result<WorkloadTarget<'_>> {
    match (
        InstructionTargetKind::try_from(target.kind).unwrap_or(InstructionTargetKind::Unspecified),
        target.identity.as_ref(),
    ) {
        (InstructionTargetKind::Proxy, Some(instruction_target::Identity::Proxy(_))) => {
            Ok(WorkloadTarget::Proxy)
        }
        (
            InstructionTargetKind::EngineSlot,
            Some(instruction_target::Identity::EngineSlotTarget(identity)),
        ) if !identity.engine_slot.is_empty() && identity.engine_slot != "proxy" => {
            Ok(WorkloadTarget::EngineSlot(&identity.engine_slot))
        }
        _ => bail!("instruction target kind and identity mismatch"),
    }
}

struct ValidatedTarget<'a> {
    target: &'a InstructionTarget,
    engine_slot: String,
}

impl std::ops::Deref for ValidatedTarget<'_> {
    type Target = InstructionTarget;
    fn deref(&self) -> &Self::Target {
        self.target
    }
}

const COMMAND_BUDGET: Duration = Duration::from_secs(45);
const STOP_BUDGET: Duration = Duration::from_secs(90);
const STOP_GRACE_BUDGET: Duration = Duration::from_secs(45);
const STOP_ESCALATION_BUDGET: Duration = Duration::from_secs(15);
const STOP_INSPECTION_BUDGET: Duration = Duration::from_secs(15);
const STOP_MARGIN: Duration = Duration::from_secs(15);
const INSPECTION_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum BackendType {
    Systemd,
    Docker,
}

impl BackendType {
    pub fn wire(self) -> wr_common::wruntime::BackendKind {
        match self {
            Self::Systemd => wr_common::wruntime::BackendKind::Systemd,
            Self::Docker => wr_common::wruntime::BackendKind::Docker,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ReleaseSlot {
    pub engine_slot: String,
    pub systemd_unit: String,
    pub docker_service: String,
    pub lifecycle_address: String,
    pub config_path: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ReleaseMetadata {
    pub format_version: u32,
    pub proxy_lifecycle_address: String,
    pub proxy_systemd_unit: String,
    pub proxy_docker_service: String,
    pub slots: Vec<ReleaseSlot>,
}

impl ReleaseMetadata {
    pub fn validate(&self) -> Result<()> {
        if self.format_version != 1 {
            bail!("unsupported release metadata format");
        }
        validate_loopback_uri(&self.proxy_lifecycle_address, "proxy lifecycle address")?;
        validate_systemd_unit(&self.proxy_systemd_unit, "proxy unit")?;
        validate_identity(&self.proxy_docker_service, "proxy service")?;
        let mut slots = BTreeSet::new();
        for slot in &self.slots {
            validate_identity(&slot.engine_slot, "engine slot")?;
            if !slots.insert(&slot.engine_slot) {
                bail!("release metadata contains a duplicate engine slot");
            }
            validate_systemd_unit(&slot.systemd_unit, "engine unit")?;
            validate_identity(&slot.docker_service, "docker service")?;
            validate_loopback_uri(&slot.lifecycle_address, "engine lifecycle address")?;
            validate_relative_path(&slot.config_path, "engine config path")?;
            if slot.systemd_unit != format!("wr-engine-{}.service", slot.engine_slot)
                || slot.docker_service != format!("engine-{}", slot.engine_slot)
            {
                bail!("release metadata service mapping does not match its engine slot");
            }
        }
        Ok(())
    }

    pub fn slot(&self, name: &str) -> Result<&ReleaseSlot> {
        validate_identity(name, "engine slot")?;
        self.slots
            .iter()
            .find(|slot| slot.engine_slot == name)
            .with_context(|| format!("release metadata does not authorize slot {name}"))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Selection {
    pub revision: u64,
    pub digest: String,
    pub resolved_release_digest: String,
}

#[cfg(target_os = "linux")]
type PinnedSystemdProcess = OwnedFd;

#[cfg(not(target_os = "linux"))]
struct PinnedSystemdProcess;

#[derive(Clone, Debug)]
pub struct BackendObservation {
    pub state: BackendProcessState,
    pub instance_id: String,
    pub main_pid: u32,
    pub query_error: String,
    pub terminal_result: String,
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
}

impl BackendObservation {
    fn query_error(error: impl std::fmt::Display) -> Self {
        Self {
            state: BackendProcessState::QueryError,
            instance_id: String::new(),
            main_pid: 0,
            query_error: error.to_string(),
            terminal_result: String::new(),
            exit_code: None,
            signal: None,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct StepEvidence {
    pub observed_revision: u64,
    pub observed_digest: String,
    pub observed_resolved_release_digest: String,
    pub backend_state: Option<BackendProcessState>,
    pub backend_instance_id: String,
    pub backend_main_pid: u32,
    pub process_instance_id: String,
    pub backend_query_error: String,
    pub lifecycle: Option<LifecycleStatus>,
    pub cleanup_evidence: Option<CleanupReleaseEvidence>,
    pub termination: Option<BackendTerminationEvidence>,
}

pub trait InstructionExecutor: Send + Sync {
    fn execute<'a>(
        &'a self,
        instruction: &'a AgentInstruction,
        cancelled: watch::Receiver<bool>,
    ) -> BackendFuture<'a, StepEvidence>;

    fn execute_cleanup<'a>(
        &'a self,
        _instruction: &'a NodeCleanupInstruction,
        _cancelled: watch::Receiver<bool>,
    ) -> BackendFuture<'a, CleanupReleaseEvidence> {
        Box::pin(async { anyhow::bail!("cleanup execution is unsupported") })
    }
}

#[derive(Clone, Debug)]
pub struct HostBackendConfig {
    pub deployment_root: PathBuf,
    pub backend: BackendType,
    pub systemctl_path: Option<PathBuf>,
    pub docker_path: Option<PathBuf>,
    pub compose_project: Option<String>,
}

impl HostBackendConfig {
    pub fn validate(&self) -> Result<()> {
        if !self.deployment_root.is_absolute() || self.deployment_root == Path::new("/") {
            bail!("deployment_root must be a non-root absolute path");
        }
        match self.backend {
            BackendType::Systemd => validate_binary(
                self.systemctl_path.as_deref(),
                "systemctl_path",
                "systemctl",
            )?,
            BackendType::Docker => {
                validate_binary(self.docker_path.as_deref(), "docker_path", "docker")?;
                validate_identity(
                    self.compose_project.as_deref().unwrap_or_default(),
                    "compose project",
                )?;
            }
        }
        Ok(())
    }
}

pub trait PathAttestor: Send + Sync {
    fn attest_directory(&self, path: &Path, name: &str) -> Result<()>;
    fn attest_binary(&self, path: Option<&Path>, name: &str) -> Result<()>;
}

struct RootOwnedPathAttestor;

impl PathAttestor for RootOwnedPathAttestor {
    fn attest_directory(&self, path: &Path, name: &str) -> Result<()> {
        validate_root_owned_directory(path, name)
    }

    fn attest_binary(&self, path: Option<&Path>, name: &str) -> Result<()> {
        validate_binary_owner(path, name)
    }
}

#[derive(Clone)]
pub struct HostBackend {
    config: HostBackendConfig,
    path_attestor: Arc<dyn PathAttestor>,
    #[cfg(test)]
    cleanup_delay: Option<Duration>,
}

impl HostBackend {
    pub fn new(config: HostBackendConfig) -> Result<Self> {
        Self::new_with_attestor(config, Box::new(RootOwnedPathAttestor))
    }

    /// Construct with an explicit path attestor. Production always uses
    /// `new`; this seam lets deterministic tests execute the real backend
    /// state machine against an isolated filesystem without root ownership.
    pub fn new_with_attestor(
        config: HostBackendConfig,
        path_attestor: Box<dyn PathAttestor>,
    ) -> Result<Self> {
        config.validate()?;
        match config.backend {
            BackendType::Systemd => {
                path_attestor.attest_binary(config.systemctl_path.as_deref(), "systemctl_path")?
            }
            BackendType::Docker => {
                path_attestor.attest_binary(config.docker_path.as_deref(), "docker_path")?
            }
        }
        Ok(Self {
            config,
            path_attestor: Arc::from(path_attestor),
            #[cfg(test)]
            cleanup_delay: None,
        })
    }

    fn release_dir(&self, revision: u64) -> Result<PathBuf> {
        if revision == 0 {
            bail!("instruction omitted release revision");
        }
        Ok(self
            .config
            .deployment_root
            .join("wr-node/releases")
            .join(revision.to_string()))
    }

    fn canonical_release(&self, revision: u64) -> Result<PathBuf> {
        let root = self
            .config
            .deployment_root
            .canonicalize()
            .context("deployment_root is unavailable")?;
        let release = self
            .release_dir(revision)?
            .canonicalize()
            .with_context(|| format!("release {revision} is not staged"))?;
        if !release.starts_with(&root) {
            bail!("release path escapes deployment_root");
        }
        self.path_attestor.attest_directory(&release, "release")?;
        Ok(release)
    }

    pub fn verify_release(
        &self,
        revision: u64,
        digest: &str,
        resolved_release_digest: &str,
    ) -> Result<ReleaseMetadata> {
        validate_digest(digest)?;
        validate_digest(resolved_release_digest)?;
        let release = self.canonical_release(revision)?;
        let resolved = verify_resolved_release(&release, digest, resolved_release_digest)?;
        anyhow::ensure!(
            resolved.revision == revision,
            "resolved release revision mismatch"
        );
        let manifest_bytes = std::fs::read(release.join("manifest.json"))
            .context("release manifest is unavailable")?;
        let manifest: BundleManifest =
            serde_json::from_slice(&manifest_bytes).context("release manifest is invalid")?;
        let metadata_bytes = std::fs::read(release.join("release-metadata.json"))
            .context("release metadata is unavailable")?;
        let metadata_checksum = format!("{:x}", Sha256::digest(&metadata_bytes));
        if manifest.checksums.get("wr-node/release-metadata.json") != Some(&metadata_checksum) {
            bail!("release metadata checksum does not match the verified source manifest");
        }
        let metadata: ReleaseMetadata =
            serde_json::from_slice(&metadata_bytes).context("release metadata is invalid")?;
        metadata.validate()?;
        Ok(metadata)
    }

    fn selection_path(&self, slot: &str) -> Result<PathBuf> {
        validate_identity(slot, "engine slot")?;
        Ok(self
            .config
            .deployment_root
            .join("wr-node/slots")
            .join(format!("{slot}.selection")))
    }

    pub fn selected(&self, slot: &str) -> Result<Option<Selection>> {
        let state_path = self.selection_path(slot)?;
        let link_path = self.config.deployment_root.join("wr-node/slots").join(slot);
        let state = std::fs::read_to_string(&state_path);
        let link = std::fs::read_link(&link_path);
        let text = match (state, link) {
            (Err(state), Err(link))
                if state.kind() == std::io::ErrorKind::NotFound
                    && link.kind() == std::io::ErrorKind::NotFound =>
            {
                return Ok(None)
            }
            (Ok(state), Ok(link)) => (state, link),
            (Err(error), _) if error.kind() != std::io::ErrorKind::NotFound => {
                return Err(error).with_context(|| format!("read {}", state_path.display()))
            }
            (_, Err(error)) if error.kind() != std::io::ErrorKind::NotFound => {
                return Err(error).with_context(|| format!("read {}", link_path.display()))
            }
            _ => bail!("slot selection link and sidecar disagree"),
        };
        let selection: SelectionFile =
            toml::from_str(&text.0).context("selection state is malformed")?;
        validate_digest(&selection.digest)?;
        let expected = self.canonical_release(selection.revision)?;
        let link_target = if text.1.is_absolute() {
            text.1
        } else {
            link_path
                .parent()
                .context("slot link has no parent")?
                .join(text.1)
        };
        let actual = link_target
            .canonicalize()
            .context("slot selection link target is unavailable")?;
        if actual != expected {
            bail!("slot selection link and sidecar identify different releases");
        }
        self.verify_release(
            selection.revision,
            &selection.digest,
            &selection.resolved_release_digest,
        )?;
        Ok(Some(Selection {
            revision: selection.revision,
            digest: selection.digest,
            resolved_release_digest: selection.resolved_release_digest,
        }))
    }

    pub fn select_release(
        &self,
        slot: &str,
        revision: u64,
        digest: &str,
        resolved_release_digest: &str,
    ) -> Result<()> {
        let metadata = self.verify_release(revision, digest, resolved_release_digest)?;
        metadata.slot(slot)?;
        let selections = self.config.deployment_root.join("wr-node/slots");
        std::fs::create_dir_all(&selections)?;
        let destination = selections.join(slot);
        let temporary = selections.join(format!(".{slot}.tmp-{}", std::process::id()));
        remove_any(&temporary)?;
        std::os::unix::fs::symlink(self.release_dir(revision)?, &temporary)?;
        std::fs::rename(&temporary, &destination)?;

        let state_path = self.selection_path(slot)?;
        let state_tmp = selections.join(format!(".{slot}.selection.tmp-{}", std::process::id()));
        remove_any(&state_tmp)?;
        let mut options = std::fs::OpenOptions::new();
        options.create_new(true).write(true).mode(0o600);
        use std::io::Write;
        let mut file = options.open(&state_tmp)?;
        write!(
            file,
            "revision = {revision}\ndigest = {digest:?}\nresolved_release_digest = {resolved_release_digest:?}\n"
        )?;
        file.sync_all()?;
        std::fs::rename(state_tmp, state_path)?;
        Ok(())
    }

    fn clear_selection(&self, slot: &str) -> Result<()> {
        remove_any(&self.config.deployment_root.join("wr-node/slots").join(slot))?;
        remove_any(&self.selection_path(slot)?)
    }

    fn proxy_selection_path(&self) -> PathBuf {
        self.config.deployment_root.join("wr-node/proxy.selection")
    }

    fn selected_proxy(&self) -> Result<Option<Selection>> {
        let state_path = self.proxy_selection_path();
        let link_path = self.config.deployment_root.join("wr-node/proxy");
        let state = std::fs::read_to_string(&state_path);
        let link = std::fs::read_link(&link_path);
        let (state, link) = match (state, link) {
            (Err(state), Err(link))
                if state.kind() == std::io::ErrorKind::NotFound
                    && link.kind() == std::io::ErrorKind::NotFound =>
            {
                return Ok(None)
            }
            (Ok(state), Ok(link)) => (state, link),
            _ => bail!("proxy selection link and sidecar disagree"),
        };
        let selection: SelectionFile =
            toml::from_str(&state).context("proxy selection state is malformed")?;
        let expected = self.canonical_release(selection.revision)?;
        let target = if link.is_absolute() {
            link
        } else {
            link_path
                .parent()
                .context("proxy link has no parent")?
                .join(link)
        };
        anyhow::ensure!(
            target.canonicalize()? == expected,
            "proxy selection link and sidecar identify different releases"
        );
        self.verify_release(
            selection.revision,
            &selection.digest,
            &selection.resolved_release_digest,
        )?;
        Ok(Some(Selection {
            revision: selection.revision,
            digest: selection.digest,
            resolved_release_digest: selection.resolved_release_digest,
        }))
    }

    fn select_proxy(&self, revision: u64, digest: &str, resolved_digest: &str) -> Result<()> {
        self.verify_release(revision, digest, resolved_digest)?;
        let root = self.config.deployment_root.join("wr-node");
        std::fs::create_dir_all(&root)?;
        let link = root.join("proxy");
        let temporary = root.join(format!(".proxy.tmp-{}", std::process::id()));
        remove_any(&temporary)?;
        std::os::unix::fs::symlink(self.release_dir(revision)?, &temporary)?;
        std::fs::rename(&temporary, &link)?;
        let state_tmp = root.join(format!(".proxy.selection.tmp-{}", std::process::id()));
        remove_any(&state_tmp)?;
        let mut options = std::fs::OpenOptions::new();
        options.create_new(true).write(true).mode(0o600);
        use std::io::Write;
        let mut file = options.open(&state_tmp)?;
        write!(file, "revision = {revision}\ndigest = {digest:?}\nresolved_release_digest = {resolved_digest:?}\n")?;
        file.sync_all()?;
        std::fs::rename(state_tmp, self.proxy_selection_path())?;
        Ok(())
    }

    fn clear_proxy_selection(&self) -> Result<()> {
        remove_any(&self.config.deployment_root.join("wr-node/proxy"))?;
        remove_any(&self.proxy_selection_path())
    }

    fn proxy_slot(metadata: &ReleaseMetadata) -> ReleaseSlot {
        ReleaseSlot {
            engine_slot: "proxy".into(),
            systemd_unit: metadata.proxy_systemd_unit.clone(),
            docker_service: metadata.proxy_docker_service.clone(),
            lifecycle_address: metadata.proxy_lifecycle_address.clone(),
            config_path: String::new(),
        }
    }

    async fn lifecycle(&self, address: &str) -> Option<LifecycleStatus> {
        client::connect_lifecycle(address, None)
            .await
            .ok()?
            .get_status(wr_common::wruntime::GetLifecycleStatusRequest {})
            .await
            .ok()?
            .into_inner()
            .status
    }

    async fn inspect_slot(&self, slot: &str) -> StepEvidence {
        let selected = match self.selected(slot) {
            Ok(value) => value,
            Err(error) => {
                return StepEvidence {
                    backend_state: Some(BackendProcessState::QueryError),
                    backend_query_error: error.to_string(),
                    ..Default::default()
                }
            }
        };
        let Some(selected) = selected else {
            return StepEvidence {
                backend_state: Some(BackendProcessState::QueryError),
                backend_query_error: "slot selection is unavailable; backend state is unknown"
                    .to_string(),
                ..Default::default()
            };
        };
        let metadata = match self.verify_release(
            selected.revision,
            &selected.digest,
            &selected.resolved_release_digest,
        ) {
            Ok(value) => value,
            Err(error) => {
                return StepEvidence {
                    observed_revision: selected.revision,
                    observed_digest: selected.digest,
                    observed_resolved_release_digest: selected.resolved_release_digest,
                    backend_state: Some(BackendProcessState::QueryError),
                    backend_query_error: error.to_string(),
                    ..Default::default()
                }
            }
        };
        let slot_metadata = match metadata.slot(slot) {
            Ok(value) => value,
            Err(error) => {
                return StepEvidence {
                    observed_revision: selected.revision,
                    observed_digest: selected.digest,
                    observed_resolved_release_digest: selected.resolved_release_digest,
                    backend_state: Some(BackendProcessState::QueryError),
                    backend_query_error: error.to_string(),
                    ..Default::default()
                }
            }
        };
        let backend = self
            .inspect_backend(slot_metadata, &self.release_dir(selected.revision).ok())
            .await;
        let lifecycle = if backend.state == BackendProcessState::Running {
            self.lifecycle(&slot_metadata.lifecycle_address).await
        } else {
            None
        };
        StepEvidence {
            observed_revision: selected.revision,
            observed_digest: selected.digest,
            observed_resolved_release_digest: selected.resolved_release_digest,
            backend_state: Some(backend.state),
            backend_instance_id: backend.instance_id,
            backend_main_pid: backend.main_pid,
            backend_query_error: backend.query_error,
            process_instance_id: lifecycle
                .as_ref()
                .map(|value| value.process_instance_id.clone())
                .unwrap_or_default(),
            lifecycle,
            cleanup_evidence: None,
            termination: None,
        }
    }

    async fn inspect_proxy(&self) -> StepEvidence {
        let selected = match self.selected_proxy() {
            Ok(Some(value)) => value,
            Ok(None) => {
                return StepEvidence {
                    backend_state: Some(BackendProcessState::Exited),
                    ..Default::default()
                }
            }
            Err(error) => {
                return StepEvidence {
                    backend_state: Some(BackendProcessState::QueryError),
                    backend_query_error: error.to_string(),
                    ..Default::default()
                }
            }
        };
        let metadata = match self.verify_release(
            selected.revision,
            &selected.digest,
            &selected.resolved_release_digest,
        ) {
            Ok(value) => value,
            Err(error) => {
                return StepEvidence {
                    observed_revision: selected.revision,
                    observed_digest: selected.digest,
                    observed_resolved_release_digest: selected.resolved_release_digest,
                    backend_state: Some(BackendProcessState::QueryError),
                    backend_query_error: error.to_string(),
                    ..Default::default()
                }
            }
        };
        let slot = Self::proxy_slot(&metadata);
        let backend = self
            .inspect_backend(&slot, &self.release_dir(selected.revision).ok())
            .await;
        let lifecycle = if backend.state == BackendProcessState::Running {
            self.lifecycle(&slot.lifecycle_address).await
        } else {
            None
        };
        StepEvidence {
            observed_revision: selected.revision,
            observed_digest: selected.digest,
            observed_resolved_release_digest: selected.resolved_release_digest,
            backend_state: Some(backend.state),
            backend_instance_id: backend.instance_id,
            backend_main_pid: backend.main_pid,
            backend_query_error: backend.query_error,
            process_instance_id: lifecycle
                .as_ref()
                .map(|value| value.process_instance_id.clone())
                .unwrap_or_default(),
            lifecycle,
            cleanup_evidence: None,
            termination: None,
        }
    }

    async fn inspect_backend(
        &self,
        slot: &ReleaseSlot,
        release: &Option<PathBuf>,
    ) -> BackendObservation {
        match self.config.backend {
            BackendType::Systemd => self.inspect_systemd(&slot.systemd_unit).await,
            BackendType::Docker => {
                let Some(release) = release else {
                    return BackendObservation::query_error("release path unavailable");
                };
                self.inspect_docker(&slot.docker_service, release).await
            }
        }
    }

    async fn inspect_systemd(&self, unit: &str) -> BackendObservation {
        let Some(binary) = self.config.systemctl_path.as_deref() else {
            return BackendObservation::query_error("systemctl_path is unavailable");
        };
        let output = constrained_command(
            binary,
            [
                "show",
                unit,
                "--property=LoadState,ActiveState,SubState,InvocationID,MainPID,Result,ExecMainCode,ExecMainStatus",
                "--no-pager",
            ],
        )
        .output()
        .await;
        match output {
            Ok(output) if output.status.success() => parse_systemd_observation(&output.stdout),
            Ok(output) => BackendObservation::query_error(format!(
                "systemctl show failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )),
            Err(error) => BackendObservation::query_error(error),
        }
    }

    async fn inspect_docker(&self, service: &str, release: &Path) -> BackendObservation {
        let Some(binary) = self.config.docker_path.as_deref() else {
            return BackendObservation::query_error("docker_path is unavailable");
        };
        let compose = release.join("docker/docker-compose.yml");
        let project = self.config.compose_project.as_deref().unwrap_or_default();
        let output = constrained_command(
            binary,
            [
                "compose",
                "-p",
                project,
                "-f",
                compose.to_string_lossy().as_ref(),
                "ps",
                "-a",
                "--format",
                "json",
                service,
            ],
        )
        .output()
        .await;
        match output {
            Ok(output) if output.status.success() => {
                parse_docker_observation(&output.stdout, service)
            }
            Ok(output) => BackendObservation::query_error(format!(
                "docker compose ps failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )),
            Err(error) => BackendObservation::query_error(error),
        }
    }

    fn effect_command(
        &self,
        action: EffectAction,
        slot: &ReleaseSlot,
        release: &Path,
    ) -> Result<Command> {
        match self.config.backend {
            BackendType::Systemd => {
                let binary = self
                    .config
                    .systemctl_path
                    .as_deref()
                    .context("systemctl_path missing")?;
                Ok(constrained_command(
                    binary,
                    [action.systemd(), slot.systemd_unit.as_str()],
                ))
            }
            BackendType::Docker => {
                let binary = self
                    .config
                    .docker_path
                    .as_deref()
                    .context("docker_path missing")?;
                let project = self
                    .config
                    .compose_project
                    .as_deref()
                    .context("compose project missing")?;
                let compose = release.join("docker/docker-compose.yml");
                let mut command = constrained_command(
                    binary,
                    [
                        "compose",
                        "-p",
                        project,
                        "-f",
                        compose.to_string_lossy().as_ref(),
                    ],
                );
                match action {
                    EffectAction::Start => {
                        command.args(["up", "-d", "--build", "--no-deps", &slot.docker_service])
                    }
                    EffectAction::Stop => {
                        command.args(["stop", "--timeout", "45", &slot.docker_service])
                    }
                };
                Ok(command)
            }
        }
    }

    async fn install_component_unit(&self, unit: &str, release: &Path) -> Result<()> {
        if self.config.backend != BackendType::Systemd {
            return Ok(());
        }
        validate_systemd_unit(unit, "component unit")?;
        let source = release.join("systemd").join(unit);
        let destination = Path::new("/etc/systemd/system").join(unit);
        std::fs::copy(&source, &destination)
            .with_context(|| format!("failed to install verified unit {unit}"))?;
        std::fs::set_permissions(&destination, std::fs::Permissions::from_mode(0o644))?;
        let binary = self
            .config
            .systemctl_path
            .as_deref()
            .context("systemctl_path missing")?;
        let status = constrained_command(binary, ["daemon-reload"])
            .status()
            .await?;
        anyhow::ensure!(status.success(), "systemd daemon-reload failed");
        Ok(())
    }

    async fn run_effect(
        &self,
        action: EffectAction,
        slot: &ReleaseSlot,
        release: &Path,
        cancelled: watch::Receiver<bool>,
    ) -> Result<()> {
        let mut command = self.effect_command(action, slot, release)?;
        command.stdout(Stdio::null()).stderr(Stdio::null());
        let status = run_cancellable(command, cancelled, COMMAND_BUDGET).await?;
        if !status.success() {
            bail!("backend {} failed with status {status}", action.name());
        }
        Ok(())
    }

    fn docker_signal_command(
        &self,
        signal: &str,
        slot: &ReleaseSlot,
        release: &Path,
    ) -> Result<Command> {
        let binary = self
            .config
            .docker_path
            .as_deref()
            .context("docker_path missing")?;
        let project = self
            .config
            .compose_project
            .as_deref()
            .context("compose project missing")?;
        let compose = release.join("docker/docker-compose.yml");
        let mut command = constrained_command(
            binary,
            [
                "compose",
                "-p",
                project,
                "-f",
                compose.to_string_lossy().as_ref(),
                "kill",
                "--signal",
                signal,
                &slot.docker_service,
            ],
        );
        command.stdout(Stdio::null()).stderr(Stdio::null());
        Ok(command)
    }

    async fn wait_for_terminal_backend(
        &self,
        slot: &ReleaseSlot,
        release: &Path,
        deadline: tokio::time::Instant,
        mut cancelled: watch::Receiver<bool>,
    ) -> Result<Option<BackendObservation>> {
        loop {
            let observation = self
                .inspect_backend(slot, &Some(release.to_path_buf()))
                .await;
            if observation.query_error.is_empty()
                && observation.state == BackendProcessState::Exited
            {
                return Ok(Some(observation));
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(None);
            }
            tokio::select! {
                changed = cancelled.changed() => {
                    if changed.is_err() || *cancelled.borrow() {
                        bail!("backend terminal inspection was cancelled by the lease guard");
                    }
                }
                () = tokio::time::sleep(INSPECTION_INTERVAL) => {}
            }
        }
    }

    async fn pin_systemd_process(
        &self,
        slot: &ReleaseSlot,
        before: &StepEvidence,
    ) -> Result<PinnedSystemdProcess> {
        anyhow::ensure!(
            before.backend_main_pid != 0,
            "systemd running activation has no main process"
        );
        let pinned = open_pidfd(before.backend_main_pid)?;
        let confirmed = self.inspect_systemd(&slot.systemd_unit).await;
        anyhow::ensure!(
            confirmed.query_error.is_empty()
                && confirmed.state == BackendProcessState::Running
                && confirmed.instance_id == before.backend_instance_id
                && confirmed.main_pid == before.backend_main_pid
                && !pidfd_exited(&pinned)?,
            "systemd backend/process identity changed before stop delivery"
        );
        Ok(pinned)
    }

    async fn wait_for_pinned_process_exit(
        &self,
        pinned: &PinnedSystemdProcess,
        deadline: tokio::time::Instant,
        mut cancelled: watch::Receiver<bool>,
    ) -> Result<()> {
        loop {
            if pidfd_exited(pinned)? {
                return Ok(());
            }
            anyhow::ensure!(
                tokio::time::Instant::now() < deadline,
                "systemd pinned process did not exit before the stop deadline"
            );
            tokio::select! {
                changed = cancelled.changed() => {
                    if changed.is_err() || *cancelled.borrow() {
                        bail!("systemd pinned-process inspection was cancelled by the lease guard");
                    }
                }
                () = tokio::time::sleep(INSPECTION_INTERVAL) => {}
            }
        }
    }

    async fn run_stop_effect(
        &self,
        slot: &ReleaseSlot,
        release: &Path,
        before: &StepEvidence,
        cancelled: watch::Receiver<bool>,
    ) -> Result<(BackendObservation, bool, bool)> {
        let stop_deadline = tokio::time::Instant::now() + STOP_BUDGET;
        match self.config.backend {
            BackendType::Systemd => {
                let pinned = self.pin_systemd_process(slot, before).await?;
                let mut command = self.effect_command(EffectAction::Stop, slot, release)?;
                command.stdout(Stdio::null()).stderr(Stdio::null());
                let remaining = stop_deadline
                    .saturating_duration_since(tokio::time::Instant::now())
                    .min(STOP_GRACE_BUDGET + STOP_MARGIN);
                let status = run_cancellable(command, cancelled.clone(), remaining).await?;
                anyhow::ensure!(status.success(), "backend stop failed with status {status}");
                self.wait_for_pinned_process_exit(&pinned, stop_deadline, cancelled.clone())
                    .await?;
                let inspection_deadline = std::cmp::min(
                    stop_deadline,
                    tokio::time::Instant::now() + STOP_INSPECTION_BUDGET,
                );
                let observation = self
                    .wait_for_terminal_backend(slot, release, inspection_deadline, cancelled)
                    .await?
                    .context("systemd terminal facts remained unavailable")?;
                Ok((observation, false, true))
            }
            BackendType::Docker => {
                let term = self.docker_signal_command("TERM", slot, release)?;
                let command_budget = stop_deadline
                    .saturating_duration_since(tokio::time::Instant::now())
                    .min(STOP_ESCALATION_BUDGET);
                let status = run_cancellable(term, cancelled.clone(), command_budget).await?;
                anyhow::ensure!(
                    status.success(),
                    "Docker TERM delivery failed with status {status}"
                );
                let grace_deadline = std::cmp::min(
                    stop_deadline,
                    tokio::time::Instant::now() + STOP_GRACE_BUDGET,
                );
                if let Some(observation) = self
                    .wait_for_terminal_backend(slot, release, grace_deadline, cancelled.clone())
                    .await?
                {
                    return Ok((observation, false, false));
                }
                let kill = self.docker_signal_command("KILL", slot, release)?;
                let escalation_budget = stop_deadline
                    .saturating_duration_since(tokio::time::Instant::now())
                    .min(STOP_ESCALATION_BUDGET);
                anyhow::ensure!(
                    !escalation_budget.is_zero(),
                    "stop deadline expired before escalation"
                );
                let status = run_cancellable(kill, cancelled.clone(), escalation_budget).await?;
                anyhow::ensure!(
                    status.success(),
                    "Docker KILL escalation failed with status {status}"
                );
                let inspection_deadline = std::cmp::min(
                    stop_deadline,
                    tokio::time::Instant::now() + STOP_INSPECTION_BUDGET,
                );
                let observation = self
                    .wait_for_terminal_backend(slot, release, inspection_deadline, cancelled)
                    .await?
                    .context("Docker terminal facts remained unavailable after escalation")?;
                Ok((observation, true, false))
            }
        }
    }

    fn termination_evidence(
        &self,
        before: &StepEvidence,
        terminal: &BackendObservation,
        escalated: bool,
        exact_process_exit: bool,
    ) -> BackendTerminationEvidence {
        // systemd releases inactive units quickly, clearing InvocationID and
        // ExecMain* properties. The pidfd pins the exact pre-stop main process.
        let same_identity = !before.backend_instance_id.is_empty()
            && (terminal.instance_id == before.backend_instance_id
                || (self.config.backend == BackendType::Systemd
                    && exact_process_exit
                    && terminal.instance_id.is_empty()));
        let forced_terminal = match self.config.backend {
            BackendType::Systemd => matches!(
                terminal.terminal_result.as_str(),
                "timeout" | "watchdog" | "signal" | "core-dump"
            ),
            BackendType::Docker => false,
        };
        let disposition = if !same_identity || terminal.state != BackendProcessState::Exited {
            BackendStopDisposition::Unknown
        } else if escalated || forced_terminal {
            BackendStopDisposition::Forced
        } else if match self.config.backend {
            BackendType::Systemd => terminal.terminal_result == "success",
            BackendType::Docker => matches!(terminal.exit_code, Some(0 | 143)),
        } {
            BackendStopDisposition::Graceful
        } else {
            BackendStopDisposition::Unknown
        };
        if disposition == BackendStopDisposition::Unknown {
            eprintln!(
                "node-agent backend termination evidence is inconclusive: backend={:?} expected_backend_instance_id={} terminal_backend_instance_id={} expected_main_pid={} terminal_main_pid={} exact_process_exit={} terminal_state={:?} terminal_result={} exit_code={:?} signal={:?} escalated={}",
                self.config.backend,
                before.backend_instance_id,
                terminal.instance_id,
                before.backend_main_pid,
                terminal.main_pid,
                exact_process_exit,
                terminal.state,
                terminal.terminal_result,
                terminal.exit_code,
                terminal.signal,
                escalated,
            );
        }
        BackendTerminationEvidence {
            backend: self.config.backend.wire() as i32,
            backend_instance_id: before.backend_instance_id.clone(),
            process_instance_id: before.process_instance_id.clone(),
            graceful_termination_requested: true,
            kill_escalated: escalated,
            disposition: disposition as i32,
            terminal_result: terminal.terminal_result.clone(),
            exit_code: terminal.exit_code,
            signal: terminal.signal,
        }
    }

    async fn wait_for_proxy(
        &self,
        selected: &Selection,
        running: bool,
        mut cancelled: watch::Receiver<bool>,
    ) -> Result<StepEvidence> {
        let deadline = tokio::time::Instant::now() + COMMAND_BUDGET;
        loop {
            let evidence = self.inspect_proxy().await;
            let state_matches = evidence.backend_query_error.is_empty()
                && evidence.observed_revision == selected.revision
                && evidence.observed_digest == selected.digest
                && evidence.observed_resolved_release_digest == selected.resolved_release_digest
                && evidence.backend_state
                    == Some(if running {
                        BackendProcessState::Running
                    } else {
                        BackendProcessState::Exited
                    });
            let lifecycle_matches = !running
                || evidence.lifecycle.as_ref().is_some_and(|status| {
                    status.state == ProcessLifecycleState::Ready as i32
                        && status.service_kind == ServiceKind::Proxy as i32
                        && !status.process_instance_id.is_empty()
                });
            if state_matches && lifecycle_matches {
                return Ok(evidence);
            }
            if tokio::time::Instant::now() >= deadline {
                bail!("proxy backend did not produce exact requested state within 45 seconds");
            }
            tokio::select! {
                changed = cancelled.changed() => {
                    if changed.is_err() || *cancelled.borrow() {
                        bail!("proxy backend inspection was cancelled by the lease guard");
                    }
                }
                () = tokio::time::sleep(INSPECTION_INTERVAL) => {}
            }
        }
    }

    async fn wait_for(
        &self,
        slot: &str,
        selected: &Selection,
        running: bool,
        mut cancelled: watch::Receiver<bool>,
    ) -> Result<StepEvidence> {
        let deadline = tokio::time::Instant::now() + COMMAND_BUDGET;
        loop {
            let evidence = self.inspect_slot(slot).await;
            let state_matches = evidence.backend_query_error.is_empty()
                && evidence.observed_revision == selected.revision
                && evidence.observed_digest == selected.digest
                && evidence.observed_resolved_release_digest == selected.resolved_release_digest
                && evidence.backend_state
                    == Some(if running {
                        BackendProcessState::Running
                    } else {
                        BackendProcessState::Exited
                    });
            let lifecycle_matches = !running
                || evidence.lifecycle.as_ref().is_some_and(|status| {
                    status.state == ProcessLifecycleState::Ready as i32
                        && status.service_kind == ServiceKind::Engine as i32
                        && !status.process_instance_id.is_empty()
                });
            if state_matches && lifecycle_matches {
                return Ok(evidence);
            }
            if tokio::time::Instant::now() >= deadline {
                bail!("backend did not produce exact requested state within 45 seconds");
            }
            tokio::select! {
                changed = cancelled.changed() => {
                    if changed.is_err() || *cancelled.borrow() {
                        bail!("backend inspection was cancelled by the lease guard");
                    }
                }
                () = tokio::time::sleep(INSPECTION_INTERVAL) => {}
            }
        }
    }

    #[cfg(test)]
    fn with_cleanup_delay(mut self, delay: Duration) -> Self {
        self.cleanup_delay = Some(delay);
        self
    }

    fn cleanup(
        &self,
        delete_allowlist: &[ReleaseInventoryEntry],
    ) -> Result<CleanupReleaseEvidence> {
        #[cfg(test)]
        if let Some(delay) = self.cleanup_delay {
            std::thread::sleep(delay);
        }
        let releases = self.config.deployment_root.join("wr-node/releases");
        let mut selected = BTreeSet::new();
        let slots = self.config.deployment_root.join("wr-node/slots");
        let mut slot_names = BTreeSet::new();
        for entry in std::fs::read_dir(&slots).context("slot inventory is unavailable")? {
            let entry = entry.context("slot inventory entry is unreadable")?;
            let file_name = entry
                .file_name()
                .into_string()
                .map_err(|_| anyhow::anyhow!("selection filename is not UTF-8"))?;
            if file_name.starts_with('.') {
                bail!("temporary selection state remains in slot inventory");
            }
            if let Some(slot) = file_name.strip_suffix(".selection") {
                slot_names.insert(slot.to_string());
            } else if entry
                .file_type()
                .context("slot inventory entry type is unavailable")?
                .is_symlink()
            {
                slot_names.insert(file_name);
            } else {
                bail!("unexpected slot inventory entry {file_name}");
            }
        }
        for slot in slot_names {
            let value = self
                .selected(&slot)?
                .context("slot selection is only partially present")?;
            selected.insert(value.revision);
        }
        if let Some(proxy) = self.selected_proxy()? {
            selected.insert(proxy.revision);
        }
        let mut inventory = BTreeMap::new();
        for entry in std::fs::read_dir(&releases).context("release inventory is unavailable")? {
            let entry = entry.context("release inventory entry is unreadable")?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| anyhow::anyhow!("release filename is not UTF-8"))?;
            if name.starts_with('.') {
                continue;
            }
            let revision = name
                .parse::<u64>()
                .with_context(|| format!("unexpected release inventory entry {name}"))?;
            let digest = read_trimmed(&entry.path().join("bundle.sha256"))?;
            let resolved_digest = read_trimmed(&entry.path().join("resolved-release.sha256"))?;
            validate_digest(&digest)?;
            validate_digest(&resolved_digest)?;
            self.verify_release(revision, &digest, &resolved_digest)?;
            if inventory
                .insert(revision, (digest, resolved_digest, entry.path()))
                .is_some()
            {
                bail!("release inventory contains a duplicate revision");
            }
        }
        let canonical_releases = releases.canonicalize()?;
        let mut authorized = BTreeSet::new();
        for release in delete_allowlist {
            if !authorized.insert(release.revision) {
                bail!("manager cleanup allow-list contains a duplicate revision");
            }
            if selected.contains(&release.revision) {
                bail!("manager cleanup allow-list includes a selected release");
            }
            // Absence is the idempotent post-delete state after an interrupted
            // report. A present release must still prove the exact digest/path.
            if let Some((digest, resolved_digest, path)) = inventory.get(&release.revision) {
                if digest != &release.bundle_digest
                    || resolved_digest != &release.resolved_release_digest
                {
                    bail!("manager cleanup allow-list identity does not match local release");
                }
                let canonical = path.canonicalize()?;
                if !canonical.starts_with(&canonical_releases) {
                    bail!("release pruning path escapes release root");
                }
            }
        }
        for revision in &authorized {
            if let Some((_, _, path)) = inventory.get(revision) {
                std::fs::remove_dir_all(path)
                    .with_context(|| format!("failed to delete authorized release {revision}"))?;
            }
        }
        Ok(CleanupReleaseEvidence {
            retained_releases: inventory
                .into_iter()
                .filter(|(revision, _)| !authorized.contains(revision))
                .map(|(revision, (bundle_digest, resolved_release_digest, _))| {
                    ReleaseInventoryEntry {
                        revision,
                        bundle_digest,
                        resolved_release_digest,
                    }
                })
                .collect(),
        })
    }
}

impl InstructionExecutor for HostBackend {
    fn execute_cleanup<'a>(
        &'a self,
        instruction: &'a NodeCleanupInstruction,
        mut cancelled: watch::Receiver<bool>,
    ) -> BackendFuture<'a, CleanupReleaseEvidence> {
        Box::pin(async move {
            let backend = self.clone();
            let allowlist = instruction.delete_releases.clone();
            let mut cleanup = tokio::task::spawn_blocking(move || backend.cleanup(&allowlist));
            tokio::select! {
                result = &mut cleanup => result.context("cleanup worker panicked")?,
                changed = cancelled.changed() => {
                    let _ = changed;
                    let _ = cleanup.await.context("cleanup worker panicked")??;
                    bail!("cleanup completed after lease cancellation; manager inspection is required")
                }
            }
        })
    }

    fn execute<'a>(
        &'a self,
        instruction: &'a AgentInstruction,
        cancelled: watch::Receiver<bool>,
    ) -> BackendFuture<'a, StepEvidence> {
        Box::pin(async move {
            let step = NodeOperationStepKind::try_from(instruction.step)
                .unwrap_or(NodeOperationStepKind::Unspecified);
            let raw_target = instruction
                .target
                .as_ref()
                .context("manager instruction omitted its typed target")?;
            let validated = workload_target(raw_target)?;
            let target = ValidatedTarget {
                target: raw_target,
                engine_slot: match validated {
                    WorkloadTarget::Proxy => String::new(),
                    WorkloadTarget::EngineSlot(slot) => slot.to_string(),
                },
            };
            if matches!(
                step,
                NodeOperationStepKind::SwitchAuthority | NodeOperationStepKind::VerifyServing
            ) {
                bail!("manager returned an internal-only instruction");
            }
            let target_kind = InstructionTargetKind::try_from(target.kind)
                .unwrap_or(InstructionTargetKind::Unspecified);
            let desired = Selection {
                revision: target.revision,
                digest: target.bundle_digest.clone(),
                resolved_release_digest: target.resolved_release_digest.clone(),
            };
            if target_kind == InstructionTargetKind::Proxy {
                anyhow::ensure!(
                    target.engine_slot.is_empty(),
                    "proxy instruction must not name an engine slot"
                );
                return match step {
                    NodeOperationStepKind::InspectBackend => Ok(self.inspect_proxy().await),
                    NodeOperationStepKind::SelectRelease => {
                        self.select_proxy(
                            desired.revision,
                            &desired.digest,
                            &desired.resolved_release_digest,
                        )?;
                        let metadata = self.verify_release(
                            desired.revision,
                            &desired.digest,
                            &desired.resolved_release_digest,
                        )?;
                        let slot = Self::proxy_slot(&metadata);
                        self.install_component_unit(
                            &slot.systemd_unit,
                            &self.release_dir(desired.revision)?,
                        )
                        .await?;
                        Ok(self.inspect_proxy().await)
                    }
                    NodeOperationStepKind::StartBackend => {
                        let metadata = self.verify_release(
                            desired.revision,
                            &desired.digest,
                            &desired.resolved_release_digest,
                        )?;
                        let slot = Self::proxy_slot(&metadata);
                        self.run_effect(
                            EffectAction::Start,
                            &slot,
                            &self.release_dir(desired.revision)?,
                            cancelled.clone(),
                        )
                        .await?;
                        let after = self.wait_for_proxy(&desired, true, cancelled).await?;
                        if (!instruction.pinned_backend_instance_id.is_empty()
                            && after.backend_instance_id == instruction.pinned_backend_instance_id)
                            || (!instruction.pinned_process_instance_id.is_empty()
                                && after.process_instance_id
                                    == instruction.pinned_process_instance_id)
                        {
                            bail!("proxy start did not produce a replacement activation");
                        }
                        Ok(after)
                    }
                    NodeOperationStepKind::StopBackend => {
                        let before = self.inspect_proxy().await;
                        anyhow::ensure!(
                            before.backend_query_error.is_empty(),
                            "cannot stop an unknown proxy backend"
                        );
                        anyhow::ensure!(
                            before.backend_instance_id == instruction.pinned_backend_instance_id
                                && before.process_instance_id
                                    == instruction.pinned_process_instance_id,
                            "proxy backend/process identity changed before stop delivery"
                        );
                        let metadata = self.verify_release(
                            desired.revision,
                            &desired.digest,
                            &desired.resolved_release_digest,
                        )?;
                        let slot = Self::proxy_slot(&metadata);
                        let release = self.release_dir(desired.revision)?;
                        let (terminal, escalated, exact_process_exit) = self
                            .run_stop_effect(&slot, &release, &before, cancelled)
                            .await?;
                        let termination = self.termination_evidence(
                            &before,
                            &terminal,
                            escalated,
                            exact_process_exit,
                        );
                        Ok(StepEvidence {
                            observed_revision: desired.revision,
                            observed_digest: desired.digest,
                            observed_resolved_release_digest: desired.resolved_release_digest,
                            backend_state: Some(BackendProcessState::Exited),
                            backend_instance_id: instruction.pinned_backend_instance_id.clone(),
                            process_instance_id: instruction.pinned_process_instance_id.clone(),
                            termination: Some(termination),
                            ..Default::default()
                        })
                    }
                    NodeOperationStepKind::VerifyTarget | NodeOperationStepKind::VerifyProxy => {
                        let evidence = self.inspect_proxy().await;
                        anyhow::ensure!(
                            evidence.observed_revision == desired.revision
                                && evidence.observed_digest == desired.digest
                                && evidence.observed_resolved_release_digest
                                    == desired.resolved_release_digest
                                && evidence.backend_state == Some(BackendProcessState::Running)
                                && evidence.backend_query_error.is_empty()
                                && evidence.lifecycle.as_ref().is_some_and(|status| {
                                    status.state == ProcessLifecycleState::Ready as i32
                                        && status.service_kind == ServiceKind::Proxy as i32
                                        && !status.process_instance_id.is_empty()
                                }),
                            "proxy is not selected and lifecycle READY with exact identity"
                        );
                        Ok(evidence)
                    }
                    NodeOperationStepKind::RestoreSource => {
                        let current = self.inspect_proxy().await;
                        if !current.backend_query_error.is_empty() {
                            return Ok(current);
                        }
                        if desired.revision == 0 {
                            if current.backend_state == Some(BackendProcessState::Running) {
                                let selected = self
                                    .selected_proxy()?
                                    .context("running proxy has no selection")?;
                                let metadata = self.verify_release(
                                    selected.revision,
                                    &selected.digest,
                                    &selected.resolved_release_digest,
                                )?;
                                let slot = Self::proxy_slot(&metadata);
                                self.run_effect(
                                    EffectAction::Stop,
                                    &slot,
                                    &self.release_dir(selected.revision)?,
                                    cancelled.clone(),
                                )
                                .await?;
                                self.wait_for_proxy(&selected, false, cancelled.clone())
                                    .await?;
                            }
                            self.clear_proxy_selection()?;
                            Ok(StepEvidence {
                                backend_state: Some(BackendProcessState::Exited),
                                ..Default::default()
                            })
                        } else {
                            if current.backend_state == Some(BackendProcessState::Running)
                                && (current.observed_revision != desired.revision
                                    || current.observed_digest != desired.digest
                                    || current.observed_resolved_release_digest
                                        != desired.resolved_release_digest)
                            {
                                let selected = self
                                    .selected_proxy()?
                                    .context("running proxy has no selection")?;
                                let metadata = self.verify_release(
                                    selected.revision,
                                    &selected.digest,
                                    &selected.resolved_release_digest,
                                )?;
                                let slot = Self::proxy_slot(&metadata);
                                self.run_effect(
                                    EffectAction::Stop,
                                    &slot,
                                    &self.release_dir(selected.revision)?,
                                    cancelled.clone(),
                                )
                                .await?;
                                self.wait_for_proxy(&selected, false, cancelled.clone())
                                    .await?;
                            }
                            self.select_proxy(
                                desired.revision,
                                &desired.digest,
                                &desired.resolved_release_digest,
                            )?;
                            let metadata = self.verify_release(
                                desired.revision,
                                &desired.digest,
                                &desired.resolved_release_digest,
                            )?;
                            let slot = Self::proxy_slot(&metadata);
                            self.install_component_unit(
                                &slot.systemd_unit,
                                &self.release_dir(desired.revision)?,
                            )
                            .await?;
                            let selected = self.inspect_proxy().await;
                            if selected.backend_state != Some(BackendProcessState::Running) {
                                self.run_effect(
                                    EffectAction::Start,
                                    &slot,
                                    &self.release_dir(desired.revision)?,
                                    cancelled.clone(),
                                )
                                .await?;
                            }
                            self.wait_for_proxy(&desired, true, cancelled).await
                        }
                    }
                    _ => bail!("manager returned an unsupported proxy instruction"),
                };
            }
            anyhow::ensure!(
                target_kind == InstructionTargetKind::EngineSlot,
                "manager returned an invalid engine target kind"
            );
            validate_identity(&target.engine_slot, "engine slot")?;
            match step {
                NodeOperationStepKind::VerifyReleaseMetadata => {
                    self.verify_release(
                        desired.revision,
                        &desired.digest,
                        &desired.resolved_release_digest,
                    )?;
                    Ok(StepEvidence {
                        observed_revision: desired.revision,
                        observed_digest: desired.digest,
                        observed_resolved_release_digest: desired.resolved_release_digest,
                        ..Default::default()
                    })
                }
                NodeOperationStepKind::InspectBackend => {
                    Ok(self.inspect_slot(&target.engine_slot).await)
                }
                NodeOperationStepKind::StopBackend => {
                    let before = self.inspect_slot(&target.engine_slot).await;
                    if !before.backend_query_error.is_empty() {
                        bail!(
                            "cannot stop an unknown backend instance: {}",
                            before.backend_query_error
                        );
                    }
                    if before.backend_instance_id != instruction.pinned_backend_instance_id
                        || before.process_instance_id != instruction.pinned_process_instance_id
                    {
                        bail!("backend/process identity changed before stop delivery");
                    }
                    let metadata = self.verify_release(
                        desired.revision,
                        &desired.digest,
                        &desired.resolved_release_digest,
                    )?;
                    let slot = metadata.slot(&target.engine_slot)?;
                    let release = self.release_dir(desired.revision)?;
                    let (terminal, escalated, exact_process_exit) = self
                        .run_stop_effect(slot, &release, &before, cancelled)
                        .await?;
                    let termination = self.termination_evidence(
                        &before,
                        &terminal,
                        escalated,
                        exact_process_exit,
                    );
                    Ok(StepEvidence {
                        observed_revision: desired.revision,
                        observed_digest: desired.digest,
                        observed_resolved_release_digest: desired.resolved_release_digest,
                        backend_state: Some(BackendProcessState::Exited),
                        backend_instance_id: instruction.pinned_backend_instance_id.clone(),
                        process_instance_id: instruction.pinned_process_instance_id.clone(),
                        termination: Some(termination),
                        ..Default::default()
                    })
                }
                NodeOperationStepKind::SelectRelease => {
                    self.select_release(
                        &target.engine_slot,
                        desired.revision,
                        &desired.digest,
                        &desired.resolved_release_digest,
                    )?;
                    let metadata = self.verify_release(
                        desired.revision,
                        &desired.digest,
                        &desired.resolved_release_digest,
                    )?;
                    let slot = metadata.slot(&target.engine_slot)?;
                    self.install_component_unit(
                        &slot.systemd_unit,
                        &self.release_dir(desired.revision)?,
                    )
                    .await?;
                    Ok(self.inspect_slot(&target.engine_slot).await)
                }
                NodeOperationStepKind::StartBackend => {
                    let metadata = self.verify_release(
                        desired.revision,
                        &desired.digest,
                        &desired.resolved_release_digest,
                    )?;
                    let slot = metadata.slot(&target.engine_slot)?;
                    self.run_effect(
                        EffectAction::Start,
                        slot,
                        &self.release_dir(desired.revision)?,
                        cancelled.clone(),
                    )
                    .await?;
                    let after = self
                        .wait_for(&target.engine_slot, &desired, true, cancelled)
                        .await?;
                    if (!instruction.pinned_backend_instance_id.is_empty()
                        && after.backend_instance_id == instruction.pinned_backend_instance_id)
                        || (!instruction.pinned_process_instance_id.is_empty()
                            && after.process_instance_id == instruction.pinned_process_instance_id)
                    {
                        bail!("backend start did not produce a replacement activation");
                    }
                    Ok(after)
                }
                NodeOperationStepKind::VerifyTarget => {
                    let evidence = self.inspect_slot(&target.engine_slot).await;
                    if evidence.observed_revision != desired.revision
                        || evidence.observed_digest != desired.digest
                        || evidence.observed_resolved_release_digest
                            != desired.resolved_release_digest
                        || evidence.backend_state != Some(BackendProcessState::Running)
                        || !evidence.backend_query_error.is_empty()
                        || evidence.lifecycle.as_ref().is_none_or(|status| {
                            status.state != ProcessLifecycleState::Ready as i32
                                || status.service_kind != ServiceKind::Engine as i32
                                || status.process_instance_id.is_empty()
                        })
                    {
                        bail!(
                            "target does not have exact backend and lifecycle readiness evidence"
                        );
                    }
                    Ok(evidence)
                }
                NodeOperationStepKind::RestoreSource => {
                    let mut current = self.inspect_slot(&target.engine_slot).await;
                    if !current.backend_query_error.is_empty()
                        || !matches!(
                            current.backend_state,
                            Some(BackendProcessState::Running | BackendProcessState::Exited)
                        )
                        || current.backend_instance_id.is_empty()
                    {
                        if current.backend_query_error.is_empty() {
                            current.backend_state = Some(BackendProcessState::QueryError);
                            current.backend_query_error =
                                "restoration requires a conclusive backend instance".into();
                        }
                        return Ok(current);
                    }

                    let current_selection = Selection {
                        revision: current.observed_revision,
                        digest: current.observed_digest.clone(),
                        resolved_release_digest: current.observed_resolved_release_digest.clone(),
                    };
                    let mut stopped = current.clone();
                    if current.backend_state == Some(BackendProcessState::Running)
                        && (current.observed_revision != desired.revision
                            || current.observed_digest != desired.digest
                            || current.observed_resolved_release_digest
                                != desired.resolved_release_digest
                            || desired.revision == 0)
                    {
                        let metadata = self.verify_release(
                            current_selection.revision,
                            &current_selection.digest,
                            &current_selection.resolved_release_digest,
                        )?;
                        let slot = metadata.slot(&target.engine_slot)?;
                        self.run_effect(
                            EffectAction::Stop,
                            slot,
                            &self.release_dir(current_selection.revision)?,
                            cancelled.clone(),
                        )
                        .await?;
                        stopped = self
                            .wait_for(
                                &target.engine_slot,
                                &current_selection,
                                false,
                                cancelled.clone(),
                            )
                            .await?;
                    }
                    if desired.revision == 0 {
                        if stopped.backend_state != Some(BackendProcessState::Exited)
                            || stopped.backend_instance_id.is_empty()
                            || !stopped.backend_query_error.is_empty()
                        {
                            stopped.backend_state = Some(BackendProcessState::QueryError);
                            stopped.backend_query_error =
                                "source removal requires exact backend exit evidence".into();
                            return Ok(stopped);
                        }
                        self.clear_selection(&target.engine_slot)?;
                        stopped.observed_revision = 0;
                        stopped.observed_digest.clear();
                        stopped.observed_resolved_release_digest.clear();
                        stopped.lifecycle = None;
                        stopped.process_instance_id.clear();
                        return Ok(stopped);
                    }
                    if current.backend_state == Some(BackendProcessState::Running)
                        && current.observed_revision == desired.revision
                        && current.observed_digest == desired.digest
                        && current.observed_resolved_release_digest
                            == desired.resolved_release_digest
                    {
                        return self
                            .wait_for(&target.engine_slot, &desired, true, cancelled)
                            .await;
                    }
                    self.select_release(
                        &target.engine_slot,
                        desired.revision,
                        &desired.digest,
                        &desired.resolved_release_digest,
                    )?;
                    let selected = self.inspect_slot(&target.engine_slot).await;
                    if !selected.backend_query_error.is_empty()
                        || !matches!(
                            selected.backend_state,
                            Some(BackendProcessState::Running | BackendProcessState::Exited)
                        )
                    {
                        return Ok(selected);
                    }
                    if selected.backend_state == Some(BackendProcessState::Exited) {
                        let metadata = self.verify_release(
                            desired.revision,
                            &desired.digest,
                            &desired.resolved_release_digest,
                        )?;
                        let slot = metadata.slot(&target.engine_slot)?;
                        self.run_effect(
                            EffectAction::Start,
                            slot,
                            &self.release_dir(desired.revision)?,
                            cancelled.clone(),
                        )
                        .await?;
                    }
                    self.wait_for(&target.engine_slot, &desired, true, cancelled)
                        .await
                }
                NodeOperationStepKind::Unspecified
                | NodeOperationStepKind::VerifyProxy
                | NodeOperationStepKind::SwitchAuthority
                | NodeOperationStepKind::VerifyServing => {
                    bail!("manager returned an unsupported instruction")
                }
            }
        })
    }
}

#[derive(Clone, Copy)]
enum EffectAction {
    Start,
    Stop,
}

impl EffectAction {
    fn systemd(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Stop => "stop",
        }
    }

    fn name(self) -> &'static str {
        self.systemd()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SelectionFile {
    revision: u64,
    digest: String,
    resolved_release_digest: String,
}

pub fn validate_identity(value: &str, label: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        bail!("{label} must be a URL-safe stable identity");
    }
    Ok(())
}

fn validate_systemd_unit(value: &str, label: &str) -> Result<()> {
    validate_identity(value, label)?;
    if !value.ends_with(".service") {
        bail!("{label} must be a service unit");
    }
    Ok(())
}

fn validate_digest(value: &str) -> Result<()> {
    let hex = value
        .strip_prefix("sha256:")
        .context("digest must use sha256")?;
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("digest must contain exactly 64 hexadecimal characters");
    }
    Ok(())
}

fn validate_binary(value: Option<&Path>, label: &str, expected_name: &str) -> Result<()> {
    let path = value.with_context(|| format!("{label} is required"))?;
    if !path.is_absolute()
        || path.file_name().and_then(|value| value.to_str()) != Some(expected_name)
    {
        bail!("{label} must be an absolute path to {expected_name}");
    }
    Ok(())
}

fn validate_binary_owner(value: Option<&Path>, label: &str) -> Result<()> {
    let path = value.with_context(|| format!("{label} is required"))?;
    let metadata = path
        .symlink_metadata()
        .with_context(|| format!("{label} {} is unavailable", path.display()))?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.uid() != 0
        || metadata.permissions().mode() & 0o022 != 0
    {
        bail!("{label} must be a root-owned regular file not writable by group or other");
    }
    Ok(())
}

fn validate_relative_path(value: &str, label: &str) -> Result<()> {
    let path = Path::new(value);
    if path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                std::path::Component::ParentDir
                    | std::path::Component::RootDir
                    | std::path::Component::Prefix(_)
            )
        })
    {
        bail!("{label} must remain beneath the release");
    }
    Ok(())
}

fn validate_loopback_uri(value: &str, label: &str) -> Result<()> {
    let uri: http::Uri = value.parse().with_context(|| format!("invalid {label}"))?;
    let host = uri.host().unwrap_or_default();
    if !matches!(host, "127.0.0.1" | "localhost" | "[::1]" | "::1") {
        bail!("{label} must be loopback");
    }
    Ok(())
}

pub fn validate_root_owned_directory(path: &Path, label: &str) -> Result<()> {
    let metadata = path
        .metadata()
        .with_context(|| format!("{label} {} is unavailable", path.display()))?;
    if !metadata.is_dir() || metadata.uid() != 0 || metadata.permissions().mode() & 0o022 != 0 {
        bail!("{label} must be a root-owned directory not writable by group or other");
    }
    Ok(())
}

fn read_trimmed(path: &Path) -> Result<String> {
    Ok(std::fs::read_to_string(path)
        .with_context(|| format!("{} is unavailable", path.display()))?
        .trim()
        .to_string())
}

fn remove_any(path: &Path) -> Result<()> {
    match path.symlink_metadata() {
        Ok(metadata) if metadata.is_dir() => std::fs::remove_dir_all(path)?,
        Ok(_) => std::fs::remove_file(path)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn constrained_command<I, S>(binary: &Path, args: I) -> Command
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let mut command = Command::new(binary);
    command
        .env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .args(args);
    command
}

async fn run_cancellable(
    mut command: Command,
    mut cancelled: watch::Receiver<bool>,
    budget: Duration,
) -> Result<std::process::ExitStatus> {
    // The process group contains only the fixed backend CLI and descendants.
    // Workload signaling remains owned by systemd/Docker, never by this group.
    unsafe {
        command.as_std_mut().pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command
        .spawn()
        .context("failed to spawn constrained backend command")?;
    let pid = child.id().context("backend command has no process id")? as i32;
    let result = tokio::select! {
        status = child.wait() => return status.context("backend command wait failed"),
        changed = cancelled.changed() => {
            let _ = changed;
            Err(anyhow::anyhow!("backend command cancelled by lease guard"))
        }
        () = tokio::time::sleep(budget) => Err(anyhow::anyhow!("backend command exceeded 45-second budget")),
    };
    // Cancellation/timeout is delivery-ambiguous. Kill only the command group,
    // then reap it; the next lease must inspect backend identity before retry.
    unsafe {
        libc::kill(-pid, libc::SIGKILL);
    }
    let _ = child.wait().await;
    result
}

#[cfg(target_os = "linux")]
fn open_pidfd(pid: u32) -> Result<PinnedSystemdProcess> {
    anyhow::ensure!(pid != 0, "cannot pin process ID zero");
    let raw_fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    if raw_fd < 0 {
        return Err(std::io::Error::last_os_error()).context("failed to pin systemd main process");
    }
    let raw_fd = i32::try_from(raw_fd).context("pidfd does not fit a file descriptor")?;
    Ok(unsafe { OwnedFd::from_raw_fd(raw_fd) })
}

#[cfg(not(target_os = "linux"))]
fn open_pidfd(_pid: u32) -> Result<PinnedSystemdProcess> {
    bail!("systemd process pinning requires Linux pidfd support")
}

#[cfg(target_os = "linux")]
fn pidfd_exited(pidfd: &PinnedSystemdProcess) -> Result<bool> {
    let mut descriptor = libc::pollfd {
        fd: pidfd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    let ready = unsafe { libc::poll(&mut descriptor, 1, 0) };
    if ready < 0 {
        return Err(std::io::Error::last_os_error()).context("failed to inspect pinned process");
    }
    anyhow::ensure!(
        descriptor.revents & libc::POLLNVAL == 0,
        "pinned process descriptor became invalid"
    );
    Ok(ready > 0 && descriptor.revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) != 0)
}

#[cfg(not(target_os = "linux"))]
fn pidfd_exited(_pidfd: &PinnedSystemdProcess) -> Result<bool> {
    bail!("systemd process pinning requires Linux pidfd support")
}

fn parse_systemd_observation(bytes: &[u8]) -> BackendObservation {
    let text = String::from_utf8_lossy(bytes);
    let fields = text
        .lines()
        .filter_map(|line| line.split_once('='))
        .map(|(key, value)| (key.trim(), value.trim()))
        .collect::<BTreeMap<_, _>>();
    let load = fields.get("LoadState").copied().unwrap_or_default();
    let active = fields.get("ActiveState").copied().unwrap_or_default();
    let sub = fields.get("SubState").copied().unwrap_or_default();
    let invocation = fields.get("InvocationID").copied().unwrap_or_default();
    let main_pid = fields
        .get("MainPID")
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or_default();
    let terminal_result = fields.get("Result").copied().unwrap_or_default();
    let main_code = fields.get("ExecMainCode").copied().unwrap_or_default();
    let main_status = fields
        .get("ExecMainStatus")
        .and_then(|value| value.parse::<i32>().ok());
    if load != "loaded" {
        return BackendObservation::query_error(format!("systemd unit load state is {load}"));
    }
    let state = if active == "active" && sub == "running" {
        BackendProcessState::Running
    } else if matches!(active, "inactive" | "failed") && matches!(sub, "dead" | "failed" | "exited")
    {
        BackendProcessState::Exited
    } else {
        return BackendObservation::query_error(format!(
            "systemd state is not conclusive: {active}/{sub}"
        ));
    };
    if invocation.is_empty() && !(active == "inactive" && sub == "dead") {
        return BackendObservation::query_error("systemd InvocationID is missing");
    }
    if state == BackendProcessState::Running && main_pid == 0 {
        return BackendObservation::query_error("systemd MainPID is missing");
    }
    // A newly installed unit that has never started is inactive/dead with no
    // InvocationID. Treat it as not running, but keep the identity empty so
    // callers that require proof of a prior process exit still fail closed.
    let (exit_code, signal) = match main_code {
        "1" | "exited" => (main_status, None),
        "2" | "3" | "killed" | "dumped" => (None, main_status),
        _ => (None, None),
    };
    BackendObservation {
        state,
        instance_id: invocation.to_string(),
        main_pid,
        query_error: String::new(),
        terminal_result: terminal_result.to_string(),
        exit_code,
        signal,
    }
}

fn parse_docker_observation(bytes: &[u8], expected_service: &str) -> BackendObservation {
    let text = String::from_utf8_lossy(bytes);
    if text.trim().is_empty() {
        return BackendObservation {
            state: BackendProcessState::Exited,
            instance_id: String::new(),
            main_pid: 0,
            query_error: String::new(),
            terminal_result: String::new(),
            exit_code: None,
            signal: None,
        };
    }
    let values: Vec<serde_json::Value> = match serde_json::from_str::<serde_json::Value>(&text) {
        Ok(serde_json::Value::Array(values)) => values,
        Ok(value @ serde_json::Value::Object(_)) => vec![value],
        _ => match text
            .lines()
            .map(serde_json::from_str)
            .collect::<std::result::Result<Vec<_>, _>>()
        {
            Ok(values) => values,
            Err(error) => return BackendObservation::query_error(error),
        },
    };
    if values.len() != 1 {
        return BackendObservation::query_error("docker service has an ambiguous container set");
    }
    let value = &values[0];
    let service = json_string(value, &["Service", "service"]);
    let id = json_string(value, &["ID", "Id", "id"]);
    let state = json_string(value, &["State", "state"]).to_ascii_lowercase();
    let exit_code = json_i32(value, &["ExitCode", "exit_code"]);
    let signal = exit_code
        .filter(|code| (128..=255).contains(code))
        .map(|code| code - 128);
    if service != expected_service || id.is_empty() {
        return BackendObservation::query_error("docker container identity does not match service");
    }
    let state = if state == "running" {
        BackendProcessState::Running
    } else if matches!(state.as_str(), "exited" | "dead" | "created") {
        BackendProcessState::Exited
    } else {
        return BackendObservation::query_error(format!("docker state is not conclusive: {state}"));
    };
    BackendObservation {
        state,
        instance_id: id,
        main_pid: 0,
        query_error: String::new(),
        terminal_result: if state == BackendProcessState::Exited {
            "exited".to_string()
        } else {
            String::new()
        },
        exit_code,
        signal,
    }
}

fn json_i32(value: &serde_json::Value, keys: &[&str]) -> Option<i32> {
    keys.iter().find_map(|key| {
        value.get(*key).and_then(|value| {
            value
                .as_i64()
                .and_then(|number| i32::try_from(number).ok())
                .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
        })
    })
}

fn json_string(value: &serde_json::Value, keys: &[&str]) -> String {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(serde_json::Value::as_str))
        .unwrap_or_default()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_and_binary_paths_are_constrained() {
        assert!(validate_identity("blue", "slot").is_ok());
        assert!(validate_identity("blue;shutdown", "slot").is_err());
        assert!(validate_binary(Some(Path::new("systemctl")), "systemctl", "systemctl").is_err());
        assert!(
            validate_binary(Some(Path::new("/usr/bin/sudo")), "systemctl", "systemctl").is_err()
        );
        assert!(validate_binary(
            Some(Path::new("/usr/bin/systemctl")),
            "systemctl",
            "systemctl"
        )
        .is_ok());
    }

    #[test]
    fn parses_exact_systemd_identity_and_unknown_states() {
        let running = parse_systemd_observation(
            b"LoadState=loaded\nActiveState=active\nSubState=running\nInvocationID=abc\nMainPID=123\n",
        );
        assert_eq!(running.state, BackendProcessState::Running);
        assert_eq!(running.instance_id, "abc");
        assert_eq!(running.main_pid, 123);

        let stopped = parse_systemd_observation(
            b"LoadState=loaded\nActiveState=inactive\nSubState=dead\nInvocationID=\nMainPID=0\nResult=success\n",
        );
        assert_eq!(stopped.state, BackendProcessState::Exited);
        assert!(stopped.instance_id.is_empty());
        assert_eq!(stopped.main_pid, 0);
        assert!(stopped.query_error.is_empty());

        let never_started = parse_systemd_observation(
            b"LoadState=loaded\nActiveState=inactive\nSubState=dead\nInvocationID=\nMainPID=0\n",
        );
        assert_eq!(never_started.state, BackendProcessState::Exited);
        assert!(never_started.instance_id.is_empty());
        assert_eq!(never_started.main_pid, 0);
        assert!(never_started.query_error.is_empty());

        let missing_running_identity = parse_systemd_observation(
            b"LoadState=loaded\nActiveState=active\nSubState=running\nInvocationID=\nMainPID=123\n",
        );
        assert_eq!(
            missing_running_identity.state,
            BackendProcessState::QueryError
        );
        assert_eq!(
            missing_running_identity.query_error,
            "systemd InvocationID is missing"
        );

        let missing_running_process = parse_systemd_observation(
            b"LoadState=loaded\nActiveState=active\nSubState=running\nInvocationID=abc\nMainPID=0\n",
        );
        assert_eq!(
            missing_running_process.state,
            BackendProcessState::QueryError
        );
        assert_eq!(
            missing_running_process.query_error,
            "systemd MainPID is missing"
        );

        let unknown = parse_systemd_observation(
            b"LoadState=loaded\nActiveState=activating\nSubState=start\nInvocationID=abc\nMainPID=123\n",
        );
        assert_eq!(unknown.state, BackendProcessState::QueryError);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn pidfd_pins_the_exact_process_until_exit() {
        let mut child = std::process::Command::new("/bin/sh")
            .args(["-c", "exec sleep 30"])
            .spawn()
            .expect("spawn process");
        let pinned = open_pidfd(child.id()).expect("pin process");
        assert!(!pidfd_exited(&pinned).expect("inspect live process"));
        child.kill().expect("kill process");
        child.wait().expect("reap process");
        assert!(pidfd_exited(&pinned).expect("inspect exited process"));
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn systemd_stop_pins_process_when_inactive_unit_clears_invocation() {
        let root = temp_root("systemd-stop-pidfd");
        std::fs::create_dir_all(&root).expect("create test root");
        let mut child = std::process::Command::new("/bin/sh")
            .args(["-c", "exec sleep 30"])
            .spawn()
            .expect("spawn process");
        std::fs::write(root.join("pid"), child.id().to_string()).expect("write pid");
        let systemctl = root.join("systemctl");
        std::fs::write(
            &systemctl,
            format!(
                "#!/bin/sh\nstate={}\npid=$(cat {}/pid)\ncase \"$1\" in\nshow)\n if [ -f \"$state\" ]; then\n  printf 'LoadState=loaded\\nActiveState=inactive\\nSubState=dead\\nInvocationID=\\nMainPID=0\\nResult=success\\n'\n else\n  printf 'LoadState=loaded\\nActiveState=active\\nSubState=running\\nInvocationID=invocation-1\\nMainPID=%s\\nResult=success\\n' \"$pid\"\n fi\n ;;\nstop)\n kill -TERM \"$pid\"\n touch \"$state\"\n ;;\nesac\n",
                root.join("stopped").display(),
                root.display(),
            ),
        )
        .expect("write fake systemctl");
        std::fs::set_permissions(&systemctl, std::fs::Permissions::from_mode(0o700))
            .expect("make fake systemctl executable");
        let backend = test_backend(&root);
        let slot = ReleaseSlot {
            engine_slot: "blue".into(),
            systemd_unit: "wr-engine-blue.service".into(),
            docker_service: "engine-blue".into(),
            lifecycle_address: "http://127.0.0.1:9100".into(),
            config_path: "config/engine.toml".into(),
        };
        let before = StepEvidence {
            backend_instance_id: "invocation-1".into(),
            backend_main_pid: child.id(),
            process_instance_id: "process-1".into(),
            ..Default::default()
        };
        let (_cancel, receiver) = watch::channel(false);
        let outcome = backend
            .run_stop_effect(&slot, &root, &before, receiver)
            .await;
        if child.try_wait().expect("inspect process").is_none() {
            child.kill().expect("kill test process");
        }
        child.wait().expect("reap test process");
        let (terminal, escalated, exact_process_exit) = outcome.expect("stop effect");
        assert!(terminal.instance_id.is_empty());
        assert!(!escalated);
        assert!(exact_process_exit);
        assert_eq!(
            backend
                .termination_evidence(&before, &terminal, escalated, exact_process_exit,)
                .disposition,
            BackendStopDisposition::Graceful as i32
        );
        std::fs::remove_dir_all(root).expect("remove test root");
    }

    #[test]
    fn parses_exact_docker_identity() {
        let observation = parse_docker_observation(
            br#"[{"Service":"engine-blue","ID":"container-1","State":"running","ExitCode":0}]"#,
            "engine-blue",
        );
        assert_eq!(observation.state, BackendProcessState::Running);
        assert_eq!(observation.instance_id, "container-1");
        assert_eq!(observation.exit_code, Some(0));
        let killed = parse_docker_observation(
            br#"[{"Service":"engine-blue","ID":"container-1","State":"exited","ExitCode":137}]"#,
            "engine-blue",
        );
        assert_eq!(killed.signal, Some(9));
        assert_eq!(
            parse_docker_observation(b"not json", "engine-blue").state,
            BackendProcessState::QueryError
        );
    }

    #[test]
    fn release_metadata_allows_empty_inventory_and_rejects_mismatch() {
        let empty = ReleaseMetadata {
            format_version: 1,
            proxy_lifecycle_address: "http://127.0.0.1:9001".into(),
            proxy_systemd_unit: "wr-proxy.service".into(),
            proxy_docker_service: "proxy".into(),
            slots: vec![],
        };
        assert!(empty.validate().is_ok());
        let mut invalid = empty;
        invalid.slots.push(ReleaseSlot {
            engine_slot: "blue".into(),
            systemd_unit: "wr-engine-red.service".into(),
            docker_service: "engine-blue".into(),
            lifecycle_address: "http://127.0.0.1:9100".into(),
            config_path: "config/engine.toml".into(),
        });
        assert!(invalid.validate().is_err());
    }

    struct TestPathAttestor;

    impl PathAttestor for TestPathAttestor {
        fn attest_directory(&self, _path: &Path, _name: &str) -> Result<()> {
            Ok(())
        }

        fn attest_binary(&self, _path: Option<&Path>, _name: &str) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn stop_budget_preserves_grace_escalation_inspection_and_margin() {
        assert_eq!(COMMAND_BUDGET, Duration::from_secs(45));
        assert_eq!(
            STOP_GRACE_BUDGET + STOP_ESCALATION_BUDGET + STOP_INSPECTION_BUDGET + STOP_MARGIN,
            STOP_BUDGET
        );
    }

    #[test]
    fn termination_disposition_fails_closed_and_records_escalation() {
        let backend = HostBackend::new_with_attestor(
            HostBackendConfig {
                deployment_root: PathBuf::from("/tmp/wruntime-test"),
                backend: BackendType::Docker,
                systemctl_path: None,
                docker_path: Some(PathBuf::from("/usr/bin/docker")),
                compose_project: Some("wruntime-test".into()),
            },
            Box::new(TestPathAttestor),
        )
        .expect("backend");
        let before = StepEvidence {
            backend_instance_id: "container-1".into(),
            process_instance_id: "process-1".into(),
            ..Default::default()
        };
        let terminal = BackendObservation {
            state: BackendProcessState::Exited,
            instance_id: "container-1".into(),
            main_pid: 0,
            query_error: String::new(),
            terminal_result: "exited".into(),
            exit_code: Some(0),
            signal: None,
        };
        assert_eq!(
            backend
                .termination_evidence(&before, &terminal, false, false)
                .disposition,
            BackendStopDisposition::Graceful as i32
        );
        assert_eq!(
            backend
                .termination_evidence(&before, &terminal, true, false)
                .disposition,
            BackendStopDisposition::Forced as i32
        );
        for exit_code in [1, 101, 134, 137, 139] {
            let failed = BackendObservation {
                exit_code: Some(exit_code),
                signal: (exit_code >= 128).then_some(exit_code - 128),
                ..terminal.clone()
            };
            assert_eq!(
                backend
                    .termination_evidence(&before, &failed, false, false)
                    .disposition,
                BackendStopDisposition::Unknown as i32,
                "exit code {exit_code} must fail closed"
            );
        }
        let terminated = BackendObservation {
            exit_code: Some(143),
            signal: Some(15),
            ..terminal.clone()
        };
        assert_eq!(
            backend
                .termination_evidence(&before, &terminated, false, false)
                .disposition,
            BackendStopDisposition::Graceful as i32
        );
        let replaced = BackendObservation {
            instance_id: "container-2".into(),
            ..terminal
        };
        assert_eq!(
            backend
                .termination_evidence(&before, &replaced, false, false)
                .disposition,
            BackendStopDisposition::Unknown as i32
        );
    }

    #[test]
    fn systemd_termination_requires_same_identity_and_success() {
        let backend = HostBackend::new_with_attestor(
            HostBackendConfig {
                deployment_root: PathBuf::from("/tmp/wruntime-test"),
                backend: BackendType::Systemd,
                systemctl_path: Some(PathBuf::from("/usr/bin/systemctl")),
                docker_path: None,
                compose_project: None,
            },
            Box::new(TestPathAttestor),
        )
        .expect("backend");
        let before = StepEvidence {
            backend_instance_id: "invocation-1".into(),
            backend_main_pid: 123,
            process_instance_id: "process-1".into(),
            ..Default::default()
        };
        let terminal = BackendObservation {
            state: BackendProcessState::Exited,
            instance_id: "invocation-1".into(),
            main_pid: 0,
            query_error: String::new(),
            terminal_result: "success".into(),
            exit_code: Some(0),
            signal: None,
        };
        assert_eq!(
            backend
                .termination_evidence(&before, &terminal, false, false)
                .disposition,
            BackendStopDisposition::Graceful as i32
        );
        let cleared_invocation = BackendObservation {
            instance_id: String::new(),
            ..terminal.clone()
        };
        assert_eq!(
            backend
                .termination_evidence(&before, &cleared_invocation, false, true)
                .disposition,
            BackendStopDisposition::Graceful as i32
        );
        assert_eq!(
            backend
                .termination_evidence(&before, &cleared_invocation, false, false)
                .disposition,
            BackendStopDisposition::Unknown as i32
        );
        let changed_invocation = BackendObservation {
            instance_id: "invocation-2".into(),
            ..terminal
        };
        assert_eq!(
            backend
                .termination_evidence(&before, &changed_invocation, false, true)
                .disposition,
            BackendStopDisposition::Unknown as i32
        );
    }

    fn test_backend(root: &Path) -> HostBackend {
        HostBackend::new_with_attestor(
            HostBackendConfig {
                deployment_root: root.to_path_buf(),
                backend: BackendType::Systemd,
                systemctl_path: Some(root.join("systemctl")),
                docker_path: None,
                compose_project: None,
            },
            Box::new(TestPathAttestor),
        )
        .unwrap()
    }

    fn stage_release(root: &Path, revision: u64, with_payload: bool) -> String {
        use super::super::bundle_integrity::{deterministic_bundle_digest, ManifestEngine};

        let release = root.join("wr-node/releases").join(revision.to_string());
        std::fs::create_dir_all(release.join("config")).unwrap();
        let metadata = ReleaseMetadata {
            format_version: 1,
            proxy_lifecycle_address: "http://127.0.0.1:1".into(),
            proxy_systemd_unit: "wr-proxy.service".into(),
            proxy_docker_service: "proxy".into(),
            slots: vec![ReleaseSlot {
                engine_slot: "blue".into(),
                systemd_unit: "wr-engine-blue.service".into(),
                docker_service: "engine-blue".into(),
                lifecycle_address: "http://127.0.0.1:1".into(),
                config_path: "config/engine.toml".into(),
            }],
        };
        let metadata_bytes = serde_json::to_vec(&metadata).unwrap();
        std::fs::write(release.join("release-metadata.json"), &metadata_bytes).unwrap();
        let mut checksums = BTreeMap::from([(
            "wr-node/release-metadata.json".to_string(),
            format!("{:x}", Sha256::digest(&metadata_bytes)),
        )]);
        if with_payload {
            std::fs::write(release.join("config/engine.toml"), b"trusted").unwrap();
            checksums.insert(
                "wr-node/config/engine.toml".to_string(),
                format!("{:x}", Sha256::digest(b"trusted")),
            );
        }
        let engines: Vec<ManifestEngine> = vec![];
        let digest = deterministic_bundle_digest(
            "x86_64-unknown-linux-gnu",
            "/opt/wruntime",
            "wr",
            &engines,
            &checksums,
            &None,
        )
        .unwrap();
        let manifest = BundleManifest {
            target: "x86_64-unknown-linux-gnu".into(),
            bundle_digest: digest.clone(),
            engines,
            workdir: "/opt/wruntime".into(),
            image_prefix: "wr".into(),
            modules: vec![],
            configs: vec![],
            template_vars: vec![],
            checksums,
            precompile_hash: None,
        };
        std::fs::write(
            release.join("manifest.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        std::fs::write(release.join("bundle.sha256"), &digest).unwrap();
        let resolved = super::super::bundle_integrity::build_resolved_manifest(
            &release, "node-a", revision, "systemd", &digest,
        )
        .unwrap();
        super::super::bundle_integrity::write_resolved_identity(&release, &resolved).unwrap();
        digest
    }

    fn resolved_digest(root: &Path, revision: u64) -> String {
        std::fs::read_to_string(
            root.join("wr-node/releases")
                .join(revision.to_string())
                .join("resolved-release.sha256"),
        )
        .unwrap()
        .trim()
        .to_string()
    }

    fn instruction(
        step: NodeOperationStepKind,
        revision: u64,
        digest: String,
        resolved_release_digest: String,
    ) -> AgentInstruction {
        AgentInstruction {
            node_id: "node-a".into(),
            operation_id: "operation-a".into(),
            agent_instance_id: "activation-a".into(),
            lease_epoch: 1,
            step: step as i32,
            target: Some(wr_common::wruntime::InstructionTarget {
                kind: wr_common::wruntime::InstructionTargetKind::EngineSlot as i32,
                identity: Some(
                    wr_common::wruntime::instruction_target::Identity::EngineSlotTarget(
                        wr_common::wruntime::EngineSlotTargetIdentity {
                            engine_slot: "blue".into(),
                        },
                    ),
                ),
                revision,
                bundle_digest: digest,
                resolved_release_digest,
            }),
            ..Default::default()
        }
    }

    fn temp_root(label: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "wr-host-backend-{label}-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(root.join("wr-node/slots")).unwrap();
        root
    }

    #[tokio::test]
    async fn proxy_source_and_target_verification_require_exact_selected_release() {
        let root = temp_root("proxy");
        let digest = stage_release(&root, 1, false);
        let backend = test_backend(&root);
        for step in [
            NodeOperationStepKind::VerifyTarget,
            NodeOperationStepKind::VerifyProxy,
        ] {
            let (_cancel, receiver) = watch::channel(false);
            let mut verify = instruction(step, 1, digest.clone(), resolved_digest(&root, 1));
            verify.target.as_mut().unwrap().kind =
                wr_common::wruntime::InstructionTargetKind::Proxy as i32;
            verify.target.as_mut().unwrap().identity =
                Some(wr_common::wruntime::instruction_target::Identity::Proxy(
                    wr_common::wruntime::ProxyTargetIdentity {},
                ));
            let error = backend.execute(&verify, receiver).await.unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("proxy is not selected and lifecycle READY with exact identity"),
                "unexpected proxy verification error: {error:#}"
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn selection_rejects_link_sidecar_mismatch() {
        let root = temp_root("selection");
        let digest1 = stage_release(&root, 1, false);
        let digest2 = stage_release(&root, 2, false);
        let backend = test_backend(&root);
        backend
            .select_release("blue", 1, &digest1, &resolved_digest(&root, 1))
            .unwrap();
        std::fs::write(
            root.join("wr-node/slots/blue.selection"),
            format!(
                "revision = 2\ndigest = {digest2:?}\nresolved_release_digest = {:?}\n",
                resolved_digest(&root, 2)
            ),
        )
        .unwrap();
        assert!(backend.selected("blue").is_err());
        std::fs::write(
            root.join("wr-node/slots/blue.selection"),
            format!(
                "revision = 1\ndigest = {digest1:?}\nresolved_release_digest = {:?}\n",
                resolved_digest(&root, 1)
            ),
        )
        .unwrap();
        std::fs::remove_file(root.join("wr-node/slots/blue")).unwrap();
        std::os::unix::fs::symlink(
            root.join("wr-node/releases/2"),
            root.join("wr-node/slots/blue"),
        )
        .unwrap();
        assert!(backend.selected("blue").is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn inspect_never_substitutes_desired_state_for_missing_selection() {
        let root = temp_root("inspect");
        let digest = stage_release(&root, 1, false);
        let backend = test_backend(&root);
        let (_cancel, receiver) = watch::channel(false);
        let evidence = backend
            .execute(
                &instruction(
                    NodeOperationStepKind::InspectBackend,
                    1,
                    digest,
                    resolved_digest(&root, 1),
                ),
                receiver,
            )
            .await
            .unwrap();
        assert_eq!(evidence.observed_revision, 0);
        assert_eq!(
            evidence.backend_state,
            Some(BackendProcessState::QueryError)
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn release_verification_rejects_tampered_declared_payload() {
        let root = temp_root("tamper");
        let digest = stage_release(&root, 1, true);
        let backend = test_backend(&root);
        backend
            .verify_release(1, &digest, &resolved_digest(&root, 1))
            .unwrap();
        std::fs::write(
            root.join("wr-node/releases/1/config/engine.toml"),
            b"tampered",
        )
        .unwrap();
        assert!(backend
            .verify_release(1, &digest, &resolved_digest(&root, 1))
            .is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cleanup_fails_closed_on_malformed_inventory() {
        let root = temp_root("cleanup");
        stage_release(&root, 1, false);
        std::fs::create_dir_all(root.join("wr-node/releases/unexpected")).unwrap();
        let backend = test_backend(&root);
        assert!(backend.cleanup(&[]).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cleanup_executes_only_the_exact_manager_allowlist_and_protects_selection() {
        let root = temp_root("retention");
        let digest1 = stage_release(&root, 1, false);
        let digest2 = stage_release(&root, 2, false);
        stage_release(&root, 3, false);
        let backend = test_backend(&root);
        backend
            .select_release("blue", 1, &digest1, &resolved_digest(&root, 1))
            .unwrap();
        assert!(backend
            .cleanup(&[ReleaseInventoryEntry {
                revision: 1,
                bundle_digest: digest1.clone(),
                resolved_release_digest: resolved_digest(&root, 1),
            }])
            .is_err());
        assert!(backend
            .cleanup(&[ReleaseInventoryEntry {
                revision: 2,
                bundle_digest: format!("sha256:{}", "f".repeat(64)),
                resolved_release_digest: resolved_digest(&root, 2),
            }])
            .is_err());
        let evidence = backend
            .cleanup(&[ReleaseInventoryEntry {
                revision: 2,
                bundle_digest: digest2,
                resolved_release_digest: resolved_digest(&root, 2),
            }])
            .unwrap();
        let revisions = evidence
            .retained_releases
            .iter()
            .map(|release| release.revision)
            .collect::<BTreeSet<_>>();
        assert_eq!(revisions, BTreeSet::from([1, 3]));
        assert!(!root.join("wr-node/releases/2").exists());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn cleanup_beyond_renewal_interval_waits_for_conclusive_worker_exit() {
        let root = temp_root("cleanup-fence");
        let digest = stage_release(&root, 1, false);
        let backend = test_backend(&root).with_cleanup_delay(Duration::from_millis(100));
        let (cancel, receiver) = watch::channel(false);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            cancel.send(true).unwrap();
        });
        let started = std::time::Instant::now();
        let outcome = backend
            .execute_cleanup(
                &NodeCleanupInstruction {
                    delete_releases: vec![ReleaseInventoryEntry {
                        revision: 1,
                        bundle_digest: digest,
                        resolved_release_digest: resolved_digest(&root, 1),
                    }],
                    ..Default::default()
                },
                receiver,
            )
            .await;
        assert!(outcome.is_err());
        assert!(started.elapsed() >= Duration::from_millis(100));
        assert!(!root.join("wr-node/releases/1").exists());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn restoration_unknown_state_reports_query_error_without_mutation() {
        let root = temp_root("restore");
        let digest = stage_release(&root, 1, false);
        let backend = test_backend(&root);
        backend
            .select_release("blue", 1, &digest, &resolved_digest(&root, 1))
            .unwrap();
        let (_cancel, receiver) = watch::channel(false);
        let evidence = backend
            .execute(
                &instruction(
                    NodeOperationStepKind::RestoreSource,
                    0,
                    String::new(),
                    String::new(),
                ),
                receiver,
            )
            .await
            .unwrap();
        assert_eq!(
            evidence.backend_state,
            Some(BackendProcessState::QueryError)
        );
        assert!(!evidence.backend_query_error.is_empty());
        assert_eq!(backend.selected("blue").unwrap().unwrap().revision, 1);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn restoration_to_absent_requires_exact_exit_before_clearing_selection() {
        use std::os::unix::fs::PermissionsExt;

        let root = temp_root("restore-absent");
        let digest = stage_release(&root, 1, false);
        let systemctl = root.join("systemctl");
        std::fs::write(
            &systemctl,
            b"#!/bin/sh\nprintf 'LoadState=loaded\\nActiveState=inactive\\nSubState=dead\\nInvocationID=old-instance\\n'\n",
        )
        .unwrap();
        std::fs::set_permissions(&systemctl, std::fs::Permissions::from_mode(0o700)).unwrap();
        let backend = test_backend(&root);
        backend
            .select_release("blue", 1, &digest, &resolved_digest(&root, 1))
            .unwrap();
        let (_cancel, receiver) = watch::channel(false);
        let evidence = backend
            .execute(
                &instruction(
                    NodeOperationStepKind::RestoreSource,
                    0,
                    String::new(),
                    String::new(),
                ),
                receiver,
            )
            .await
            .unwrap();
        assert_eq!(evidence.backend_state, Some(BackendProcessState::Exited));
        assert_eq!(evidence.backend_instance_id, "old-instance");
        assert!(backend.selected("blue").unwrap().is_none());
        std::fs::remove_dir_all(root).unwrap();
    }
}
