use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::Args;
use serde::Deserialize;
use tokio::process::Command;
use wr_common::node::TlsConfig;
use wr_common::wruntime::{
    BackendProcessState, ClaimOperationRequest, NodeOperationStepKind, ProcessLifecycleState,
    RenewOperationLeaseRequest, ReportNodeObservationRequest, ReportStepResultRequest,
};

use crate::client;

#[derive(Args)]
pub struct AgentArgs {
    /// Strict node-agent configuration file.
    #[arg(long)]
    pub config: PathBuf,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum BackendKind {
    Systemd,
    Docker,
}

#[derive(Clone, Debug, Deserialize)]
struct SlotConfig {
    lifecycle_address: String,
}

fn default_poll_seconds() -> u64 {
    5
}

#[derive(Clone, Debug, Deserialize)]
struct AgentConfig {
    node_id: String,
    manager: String,
    deployment_root: PathBuf,
    backend: BackendKind,
    #[serde(default)]
    compose_file: Option<PathBuf>,
    #[serde(default)]
    compose_project: Option<String>,
    #[serde(default = "default_poll_seconds")]
    poll_seconds: u64,
    slots: BTreeMap<String, SlotConfig>,
    tls: TlsConfig,
}

impl AgentConfig {
    fn load(path: &Path) -> Result<Self> {
        let value = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read node-agent config {}", path.display()))?;
        let config: Self = toml::from_str(&value)
            .with_context(|| format!("failed to parse node-agent config {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        valid_identity(&self.node_id, "node_id")?;
        if self.poll_seconds == 0 {
            bail!("poll_seconds must be positive");
        }
        if !self.deployment_root.is_absolute() {
            bail!("deployment_root must be absolute");
        }
        if self.slots.is_empty() {
            bail!("at least one configured slot is required");
        }
        for (slot, config) in &self.slots {
            valid_identity(slot, "slot")?;
            let uri: http::Uri = config
                .lifecycle_address
                .parse()
                .with_context(|| format!("slot {slot} has an invalid lifecycle address"))?;
            let host = uri.host().unwrap_or_default();
            if host != "127.0.0.1" && host != "localhost" && host != "[::1]" && host != "::1" {
                bail!("slot {slot} lifecycle address must be loopback");
            }
        }
        if matches!(self.backend, BackendKind::Docker)
            && (self.compose_file.is_none() || self.compose_project.is_none())
        {
            bail!("docker backend requires compose_file and compose_project");
        }
        Ok(())
    }
}

fn valid_identity(value: &str, label: &str) -> Result<()> {
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

struct HostBackend {
    config: AgentConfig,
}

impl HostBackend {
    fn service_name(&self, slot: &str) -> String {
        match self.config.backend {
            BackendKind::Systemd => format!("wr-engine-{slot}.service"),
            BackendKind::Docker => format!("engine-{slot}"),
        }
    }

    fn command(&self, action: &str, slot: &str) -> Result<Command> {
        valid_identity(slot, "slot")?;
        let service = self.service_name(slot);
        match self.config.backend {
            BackendKind::Systemd => {
                let mut command = Command::new("sudo");
                command.args(["systemctl", action, service.as_str()]);
                Ok(command)
            }
            BackendKind::Docker => {
                let compose = self
                    .config
                    .compose_file
                    .as_ref()
                    .context("compose_file is required")?;
                let project = self
                    .config
                    .compose_project
                    .as_deref()
                    .context("compose_project is required")?;
                let mut command = Command::new("sudo");
                command.args(["docker", "compose", "-p", project, "-f"]);
                command.arg(compose);
                command.args([action, service.as_str()]);
                Ok(command)
            }
        }
    }

    async fn running(&self, slot: &str) -> Result<bool> {
        let status = match self.config.backend {
            BackendKind::Systemd => self.command("is-active", slot)?.status().await?,
            BackendKind::Docker => {
                let compose = self
                    .config
                    .compose_file
                    .as_ref()
                    .context("compose_file is required")?;
                let project = self
                    .config
                    .compose_project
                    .as_deref()
                    .context("compose_project is required")?;
                let service = self.service_name(slot);
                let output = Command::new("sudo")
                    .args(["docker", "compose", "-p", project, "-f"])
                    .arg(compose)
                    .args(["ps", "--status", "running", "--services", service.as_str()])
                    .output()
                    .await?;
                return Ok(output.status.success()
                    && String::from_utf8_lossy(&output.stdout)
                        .lines()
                        .any(|line| line == service));
            }
        };
        Ok(status.success())
    }

    async fn stop(&self, slot: &str) -> Result<()> {
        let status = self.command("stop", slot)?.status().await?;
        if !status.success() && self.running(slot).await? {
            bail!("backend failed to stop {}", self.service_name(slot));
        }
        Ok(())
    }

    async fn start(&self, slot: &str) -> Result<()> {
        let status = match self.config.backend {
            BackendKind::Systemd => self.command("start", slot)?.status().await?,
            BackendKind::Docker => {
                let project = self
                    .config
                    .compose_project
                    .as_deref()
                    .context("compose_project is required")?;
                let compose = self
                    .config
                    .deployment_root
                    .join("wr-node/slots")
                    .join(slot)
                    .join("docker/docker-compose.yml");
                let mut command = Command::new("sudo");
                command.args(["docker", "compose", "-p", project, "-f"]);
                command.arg(compose);
                command.args([
                    "up",
                    "-d",
                    "--build",
                    "--no-deps",
                    self.service_name(slot).as_str(),
                ]);
                command.status().await?
            }
        };
        if !status.success() {
            bail!("backend failed to start {}", self.service_name(slot));
        }
        Ok(())
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

    fn verify_release(&self, revision: u64, digest: &str) -> Result<()> {
        let release = self.release_dir(revision)?;
        let marker = release.join("bundle.sha256");
        let canonical_root = self
            .config
            .deployment_root
            .canonicalize()
            .context("deployment_root is unavailable")?;
        let canonical_release = release
            .canonicalize()
            .with_context(|| format!("release {revision} is not staged"))?;
        if !canonical_release.starts_with(&canonical_root) {
            bail!("release path escapes deployment_root");
        }
        let actual = std::fs::read_to_string(&marker).with_context(|| {
            format!("release digest marker {} is unavailable", marker.display())
        })?;
        if actual.trim() != digest {
            bail!("staged release digest does not match operation digest");
        }
        Ok(())
    }

    fn selected_revision(&self, slot: &str) -> u64 {
        std::fs::read_to_string(
            self.config
                .deployment_root
                .join("wr-node/slots")
                .join(format!("{slot}.selection")),
        )
        .ok()
        .and_then(|value| {
            value
                .lines()
                .find_map(|line| line.strip_prefix("revision="))
                .and_then(|value| value.parse().ok())
        })
        .unwrap_or_default()
    }

    fn select_release(&self, slot: &str, revision: u64, digest: &str) -> Result<()> {
        self.verify_release(revision, digest)?;
        let selections = self.config.deployment_root.join("wr-node/slots");
        std::fs::create_dir_all(&selections)?;
        let destination = selections.join(slot);
        let temporary = selections.join(format!(".{slot}.tmp-{}", std::process::id()));
        if temporary.exists() || temporary.symlink_metadata().is_ok() {
            std::fs::remove_file(&temporary)?;
        }
        std::os::unix::fs::symlink(self.release_dir(revision)?, &temporary)?;
        std::fs::rename(&temporary, &destination)?;
        std::fs::write(
            selections.join(format!("{slot}.selection")),
            format!("revision={revision}\ndigest={digest}\n"),
        )?;
        Ok(())
    }
}

async fn lifecycle_status(address: &str) -> Option<wr_common::wruntime::LifecycleStatus> {
    client::connect_lifecycle(address, None)
        .await
        .ok()?
        .get_status(wr_common::wruntime::GetLifecycleStatusRequest {})
        .await
        .ok()?
        .into_inner()
        .status
}

async fn renew(config: &AgentConfig, operation_id: &str, epoch: u64) -> Result<()> {
    client::connect_node_agent_with_tls(&config.manager, Some(&config.tls))
        .await?
        .renew_operation_lease(RenewOperationLeaseRequest {
            node_id: config.node_id.clone(),
            operation_id: operation_id.to_string(),
            lease_epoch: epoch,
        })
        .await
        .context("operation lease was lost")?;
    Ok(())
}

async fn wait_for_backend(
    backend: &HostBackend,
    slot: &str,
    running: bool,
    operation_id: &str,
    epoch: u64,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    loop {
        if backend.running(slot).await? == running {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("backend did not reach requested process state within 45 seconds");
        }
        renew(&backend.config, operation_id, epoch).await?;
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn execute_instruction(
    backend: &HostBackend,
    instruction: &wr_common::wruntime::AgentInstruction,
) -> Result<()> {
    let step = NodeOperationStepKind::try_from(instruction.step)
        .unwrap_or(NodeOperationStepKind::Unspecified);
    match step {
        NodeOperationStepKind::VerifyRelease => {
            backend.verify_release(instruction.revision, &instruction.bundle_digest)
        }
        NodeOperationStepKind::StopSlot => {
            if !backend.running(&instruction.engine_slot).await? {
                return Ok(());
            }
            backend.stop(&instruction.engine_slot).await?;
            wait_for_backend(
                backend,
                &instruction.engine_slot,
                false,
                &instruction.operation_id,
                instruction.lease_epoch,
            )
            .await
        }
        NodeOperationStepKind::SelectRelease => backend.select_release(
            &instruction.engine_slot,
            instruction.revision,
            &instruction.bundle_digest,
        ),
        NodeOperationStepKind::StartSlot => {
            backend.start(&instruction.engine_slot).await?;
            wait_for_backend(
                backend,
                &instruction.engine_slot,
                true,
                &instruction.operation_id,
                instruction.lease_epoch,
            )
            .await
        }
        NodeOperationStepKind::VerifyReady => {
            let address = &backend
                .config
                .slots
                .get(&instruction.engine_slot)
                .context("instruction references an unconfigured slot")?
                .lifecycle_address;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
            loop {
                if lifecycle_status(address)
                    .await
                    .is_some_and(|status| status.state == ProcessLifecycleState::Ready as i32)
                {
                    return Ok(());
                }
                if tokio::time::Instant::now() >= deadline {
                    bail!("slot did not reach lifecycle READY within 45 seconds");
                }
                renew(
                    &backend.config,
                    &instruction.operation_id,
                    instruction.lease_epoch,
                )
                .await?;
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
        NodeOperationStepKind::Unspecified => bail!("manager returned an invalid instruction"),
    }
}

async fn report_observations(backend: &HostBackend) {
    for (slot, slot_config) in &backend.config.slots {
        let lifecycle = lifecycle_status(&slot_config.lifecycle_address).await;
        let running = backend.running(slot).await.unwrap_or(false);
        let request = ReportNodeObservationRequest {
            node_id: backend.config.node_id.clone(),
            engine_slot: slot.clone(),
            backend_state: if running {
                BackendProcessState::Running as i32
            } else {
                BackendProcessState::Exited as i32
            },
            backend_instance_id: lifecycle
                .as_ref()
                .map(|status| status.process_instance_id.clone())
                .unwrap_or_default(),
            lifecycle,
            observed_revision: backend.selected_revision(slot),
            observed_at: Some(prost_types::Timestamp {
                seconds: chrono::Utc::now().timestamp(),
                nanos: 0,
            }),
        };
        if let Ok(mut agent) =
            client::connect_node_agent_with_tls(&backend.config.manager, Some(&backend.config.tls))
                .await
        {
            let _ = agent.report_observation(request).await;
        }
    }
}

pub async fn run(args: AgentArgs) -> Result<()> {
    let config = AgentConfig::load(&args.config)?;
    let backend = HostBackend { config };
    let poll = Duration::from_secs(backend.config.poll_seconds);
    loop {
        report_observations(&backend).await;
        let response =
            client::connect_node_agent_with_tls(&backend.config.manager, Some(&backend.config.tls))
                .await?
                .claim_operation(ClaimOperationRequest {
                    node_id: backend.config.node_id.clone(),
                })
                .await?
                .into_inner();
        let Some(instruction) = response.instruction else {
            tokio::time::sleep(poll).await;
            continue;
        };
        if !backend.config.slots.contains_key(&instruction.engine_slot) {
            bail!(
                "manager instruction references unconfigured slot {}",
                instruction.engine_slot
            );
        }
        let result = execute_instruction(&backend, &instruction).await;
        let (succeeded, code, detail) = match result {
            Ok(()) => (true, String::new(), String::new()),
            Err(error) => (false, "HOST_STEP_FAILED".to_string(), format!("{error:#}")),
        };
        let report = ReportStepResultRequest {
            node_id: backend.config.node_id.clone(),
            operation_id: instruction.operation_id,
            engine_slot: instruction.engine_slot,
            lease_epoch: instruction.lease_epoch,
            step: instruction.step,
            succeeded,
            condition_code: code,
            detail,
        };
        let reported =
            client::connect_node_agent_with_tls(&backend.config.manager, Some(&backend.config.tls))
                .await?
                .report_step_result(report)
                .await;
        if let Err(error) = reported {
            eprintln!("failed to checkpoint node operation step: {error}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_service_names_reject_shell_input() {
        assert!(valid_identity("blue", "slot").is_ok());
        assert!(valid_identity("blue;shutdown", "slot").is_err());
    }

    #[test]
    fn selection_path_is_slot_scoped() {
        let root = PathBuf::from("/opt/wruntime");
        assert_eq!(
            root.join("wr-node/slots").join("blue"),
            PathBuf::from("/opt/wruntime/wr-node/slots/blue")
        );
    }
}
