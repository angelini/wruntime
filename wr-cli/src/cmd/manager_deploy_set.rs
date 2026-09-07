//! Crash-consistent manager-set deployment executor.
//!
//! The manifest is deliberately replay-stable: every remotely installed byte is
//! named by its digest and the create request is derived only from validated,
//! sorted manifest content. Active selectors are not touched while staging.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::Args;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use wr_common::authorization_policy::{RolloutTarget, ValidatedPolicy};
use wr_common::manager_client::RetryClass;
use wr_common::wruntime::{
    AdvanceManagerRolloutRequest, BeginManagerRolloutRequest, LeaseManagerRolloutRequest,
    ManagerRollout, ManagerRolloutMemberOutcome, ManagerRolloutPhase, ManagerRolloutSource,
    ManagerRolloutTarget,
};

use super::helpers::{self, RemoteInstallClass};
use crate::{client, cmd::service_gen};

const SCHEMA_VERSION: u32 = 1;
const ARTIFACT_ROOT: &str = "/opt/wruntime/manager-artifacts";
const STATE_ROOT: &str = "/var/lib/wruntime";
const PKI_ROOT: &str = "/etc/wruntime/pki";
const LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(10);
const SOLE_MANAGER_CONTINUATION_SECS: u64 = 120;

#[derive(Args)]
pub struct DeploySetArgs {
    /// Replay-stable TOML manager-set deployment manifest.
    #[arg(long)]
    pub manifest: String,
}

#[derive(Args)]
pub struct RestoreConfigArgs {
    /// The original qualified deployment manifest.
    #[arg(long)]
    pub manifest: String,
    /// Manager whose bounded previous selection should be restored.
    #[arg(long)]
    pub manager_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ManagerSetManifest {
    pub schema_version: u32,
    pub client_operation_id: String,
    pub executor_id: String,
    pub cluster_id: String,
    /// Existing control endpoint. Omit only for a pristine empty-cluster bootstrap.
    pub manager_endpoint: Option<String>,
    pub target_policy: PathBuf,
    pub deployment_certificate: String,
    pub recovery_of: Option<String>,
    #[serde(default = "default_parallelism")]
    pub max_parallel: usize,
    #[serde(default)]
    pub ssh_key: Option<String>,
    #[serde(default)]
    pub ssh_port: Option<u16>,
    #[serde(default)]
    pub sources: Vec<SourceManager>,
    pub targets: Vec<TargetManager>,
}

fn default_parallelism() -> usize {
    1
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceManager {
    pub manager_id: String,
    pub endpoint: String,
    pub remote: String,
    pub host_digest: String,
    pub selector_digest: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TargetManager {
    pub manager_id: String,
    pub endpoint: String,
    pub remote: String,
    pub backend: Backend,
    /// Local executable for systemd, or immutable `name@sha256:...` for Compose.
    pub executable: String,
    pub executable_digest: String,
    pub backend_spec: PathBuf,
    pub backend_spec_digest: String,
    pub config: PathBuf,
    pub config_digest: String,
    /// Local immutable credential set directory. Its basename is the set version.
    pub credential_set: PathBuf,
    pub credential_digest: String,
    pub old_selector_digest: String,
    pub new_selector_digest: String,
    pub host_digest: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Backend {
    Systemd,
    Compose,
}

#[derive(Debug, Serialize)]
struct ActivationDescriptor<'a> {
    schema_version: u32,
    manager_id: &'a str,
    backend: Backend,
    executable: String,
    executable_digest: &'a str,
    backend_spec_path: String,
    backend_spec_digest: &'a str,
    config_path: String,
    config_digest: &'a str,
    credential_set_path: String,
    credential_digest: &'a str,
}

#[derive(Debug, Serialize)]
struct HostActionEvidence<'a> {
    schema_version: u32,
    rollout_id: &'a str,
    manager_id: &'a str,
    lease_epoch: u64,
    action_sequence: u64,
    manifest_digest: &'a str,
    executable_digest: &'a str,
    backend_spec_digest: &'a str,
    config_digest: &'a str,
    credential_digest: &'a str,
    old_selector_digest: &'a str,
    new_selector_digest: &'a str,
    effect: &'a str,
    outcome: &'a str,
    lease_expires_unix: u64,
    continuation_deadline_unix: Option<u64>,
}

struct ValidatedManifest {
    manifest: ManagerSetManifest,
    policy: ValidatedPolicy,
    manifest_digest: String,
}

fn digest_bytes(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn validate_digest(value: &str, label: &str) -> Result<()> {
    if value.len() != 71
        || !value.starts_with("sha256:")
        || !value[7..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("{label} must be sha256:<64 lowercase hex>");
    }
    Ok(())
}

fn digest_file(path: &Path) -> Result<String> {
    digest_bytes(&fs::read(path).with_context(|| format!("failed to read {}", path.display()))?)
        .pipe(Ok)
}

trait Pipe: Sized {
    fn pipe<T>(self, f: impl FnOnce(Self) -> T) -> T {
        f(self)
    }
}
impl<T> Pipe for T {}

fn target_host_digest(target: &TargetManager) -> Result<String> {
    #[derive(Serialize)]
    struct HostContract<'a> {
        manager_id: &'a str,
        endpoint: &'a str,
        remote: &'a str,
        backend: Backend,
        executable_digest: &'a str,
        backend_spec_digest: &'a str,
        config_digest: &'a str,
        credential_digest: &'a str,
        old_selector_digest: &'a str,
        new_selector_digest: &'a str,
    }
    Ok(digest_bytes(&serde_json::to_vec(&HostContract {
        manager_id: &target.manager_id,
        endpoint: &target.endpoint,
        remote: &target.remote,
        backend: target.backend,
        executable_digest: &target.executable_digest,
        backend_spec_digest: &target.backend_spec_digest,
        config_digest: &target.config_digest,
        credential_digest: &target.credential_digest,
        old_selector_digest: &target.old_selector_digest,
        new_selector_digest: &target.new_selector_digest,
    })?))
}

fn load_manifest(path: &Path) -> Result<ValidatedManifest> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut manifest: ManagerSetManifest =
        toml::from_slice(&bytes).context("invalid deploy-set manifest")?;
    if manifest.schema_version != SCHEMA_VERSION {
        bail!("deploy-set manifest schema_version must be {SCHEMA_VERSION}");
    }
    uuid::Uuid::parse_str(&manifest.client_operation_id)
        .context("client_operation_id must be a UUID")?;
    uuid::Uuid::parse_str(&manifest.executor_id).context("executor_id must be a UUID")?;
    if let Some(recovery) = &manifest.recovery_of {
        uuid::Uuid::parse_str(recovery).context("recovery_of must be a UUID")?;
    }
    wr_common::identity::ClusterId::parse(&manifest.cluster_id)?;
    if manifest.deployment_certificate.is_empty()
        || manifest.deployment_certificate.contains('/')
        || manifest.deployment_certificate.len() > 128
    {
        bail!("deployment_certificate must be a bounded certificate-set name");
    }
    if manifest.max_parallel == 0 || manifest.max_parallel > 16 {
        bail!("max_parallel must be between 1 and 16");
    }
    if manifest.manager_endpoint.is_none() && !manifest.sources.is_empty() {
        bail!("empty-cluster bootstrap requires an empty source manager set");
    }
    if manifest.targets.is_empty() {
        bail!("target manager set must not be empty");
    }
    manifest
        .sources
        .sort_by(|a, b| a.manager_id.cmp(&b.manager_id));
    manifest
        .targets
        .sort_by(|a, b| a.manager_id.cmp(&b.manager_id));
    let mut ids = BTreeSet::new();
    for source in &manifest.sources {
        wr_common::identity::ManagerId::parse(&source.manager_id)?;
        wr_common::identity::PeerHttpsUrl::parse(&source.endpoint)?;
        validate_digest(&source.host_digest, "source host digest")?;
        validate_digest(&source.selector_digest, "source selector digest")?;
        if !ids.insert(("source", source.manager_id.as_str())) {
            bail!("duplicate source manager ID {}", source.manager_id);
        }
    }
    ids.clear();
    for target in &manifest.targets {
        wr_common::identity::ManagerId::parse(&target.manager_id)?;
        wr_common::identity::PeerHttpsUrl::parse(&target.endpoint)?;
        if !ids.insert(("target", target.manager_id.as_str())) {
            bail!("duplicate target manager ID {}", target.manager_id);
        }
        for (digest, label) in [
            (&target.executable_digest, "executable digest"),
            (&target.backend_spec_digest, "backend spec digest"),
            (&target.config_digest, "config digest"),
            (&target.credential_digest, "credential digest"),
            (&target.old_selector_digest, "old selector digest"),
            (&target.new_selector_digest, "new selector digest"),
            (&target.host_digest, "host digest"),
        ] {
            validate_digest(digest, label)?;
        }
        if digest_file(&target.backend_spec)? != target.backend_spec_digest
            || digest_file(&target.config)? != target.config_digest
            || helpers::local_tree_digest(&target.credential_set)? != target.credential_digest
        {
            bail!(
                "manager {} has an artifact digest mismatch",
                target.manager_id
            );
        }
        let backend_spec = fs::read_to_string(&target.backend_spec)
            .context("manager backend spec must be UTF-8")?;
        match target.backend {
            Backend::Systemd => {
                if digest_file(Path::new(&target.executable))? != target.executable_digest {
                    bail!("manager {} binary digest mismatch", target.manager_id);
                }
                if backend_spec != service_gen::manager_activation_systemd_unit() {
                    bail!(
                        "manager {} systemd spec must be the stable activation-launcher unit",
                        target.manager_id
                    );
                }
            }
            Backend::Compose => {
                if !target.executable.contains("@sha256:")
                    || !target.executable.ends_with(&target.executable_digest[7..])
                {
                    bail!(
                        "manager {} Compose image must be an immutable matching repo digest",
                        target.manager_id
                    );
                }
                if !backend_spec.contains(&target.executable)
                    || backend_spec.contains(".key")
                    || backend_spec.contains("/release")
                    || backend_spec.contains("config.toml")
                {
                    bail!("manager {} Compose spec must select only the immutable image and protected stable mounts", target.manager_id);
                }
            }
        }
        if target_host_digest(target)? != target.host_digest {
            bail!(
                "manager {} host digest does not bind its complete artifact/selector contract",
                target.manager_id
            );
        }
    }
    let policy_bytes = fs::read(&manifest.target_policy).with_context(|| {
        format!(
            "failed to read target policy {}",
            manifest.target_policy.display()
        )
    })?;
    let policy = ValidatedPolicy::load(&policy_bytes)?;
    if policy.cluster_id != manifest.cluster_id {
        bail!("target policy cluster differs from deploy-set manifest");
    }
    let tls = client::tls_config().context("global manager client TLS is not initialized")?;
    let cluster = wr_common::identity::ClusterId::parse(&policy.cluster_id)?;
    let caller = wr_common::tls::load_client_leaf_evidence(&tls.cert_path, Some(&cluster))?;
    policy.prevalidate_rollout(
        &caller,
        &manifest
            .targets
            .iter()
            .map(|target| RolloutTarget {
                manager_id: target.manager_id.clone(),
                endpoint: target.endpoint.clone(),
            })
            .collect::<Vec<_>>(),
    )?;
    let manifest_digest = digest_bytes(&serde_json::to_vec(&manifest)?);
    Ok(ValidatedManifest {
        manifest,
        policy,
        manifest_digest,
    })
}

fn activation_descriptor(target: &TargetManager) -> Result<(Vec<u8>, String)> {
    let executable = match target.backend {
        Backend::Systemd => format!(
            "{ARTIFACT_ROOT}/binaries/{}/wr-manager",
            &target.executable_digest[7..]
        ),
        Backend::Compose => target.executable.clone(),
    };
    let credential_version = target
        .credential_set
        .file_name()
        .and_then(|name| name.to_str())
        .context("credential_set must have a UTF-8 version basename")?;
    let bytes = serde_json::to_vec_pretty(&ActivationDescriptor {
        schema_version: SCHEMA_VERSION,
        manager_id: &target.manager_id,
        backend: target.backend,
        executable,
        executable_digest: &target.executable_digest,
        backend_spec_path: format!(
            "{ARTIFACT_ROOT}/backend-specs/{}.json",
            &target.backend_spec_digest[7..]
        ),
        backend_spec_digest: &target.backend_spec_digest,
        config_path: format!(
            "{STATE_ROOT}/manager-config/{}/current.toml",
            target.manager_id
        ),
        config_digest: &target.config_digest,
        credential_set_path: format!(
            "{PKI_ROOT}/manager-{}/sets/{credential_version}",
            target.manager_id
        ),
        credential_digest: &target.credential_digest,
    })?;
    Ok((bytes.clone(), digest_bytes(&bytes)))
}

fn create_request(validated: &ValidatedManifest) -> Result<BeginManagerRolloutRequest> {
    let policy = &validated.policy;
    let tls = client::tls_config().context("global manager client TLS is not initialized")?;
    let cluster = wr_common::identity::ClusterId::parse(&policy.cluster_id)?;
    let caller = wr_common::tls::load_client_leaf_evidence(&tls.cert_path, Some(&cluster))?;
    let receipt = policy.prevalidate_rollout(
        &caller,
        &validated
            .manifest
            .targets
            .iter()
            .map(|target| RolloutTarget {
                manager_id: target.manager_id.clone(),
                endpoint: target.endpoint.clone(),
            })
            .collect::<Vec<_>>(),
    )?;
    Ok(BeginManagerRolloutRequest {
        client_operation_id: validated.manifest.client_operation_id.clone(),
        cluster_id: receipt.cluster_id,
        target_generation: receipt.generation,
        target_policy_digest: receipt.digest,
        expected_targets: validated
            .manifest
            .targets
            .iter()
            .map(|target| ManagerRolloutTarget {
                manager_id: target.manager_id.clone(),
                endpoint: target.endpoint.clone(),
                host_digest: target.host_digest.clone(),
                config_digest: target.config_digest.clone(),
                backend: match target.backend {
                    Backend::Systemd => "systemd",
                    Backend::Compose => "compose",
                }
                .into(),
                executable_digest: target.executable_digest.clone(),
                backend_spec_digest: target.backend_spec_digest.clone(),
                credential_digest: target.credential_digest.clone(),
                old_selector_digest: target.old_selector_digest.clone(),
                new_selector_digest: target.new_selector_digest.clone(),
            })
            .collect(),
        recovery_of: validated.manifest.recovery_of.clone().unwrap_or_default(),
        target_policy_validator_version: receipt.validator_version,
        target_deployment_principal_uri: receipt.caller_principal_uri,
        target_deployment_leaf_fingerprint: receipt.caller_leaf_fingerprint,
        source_managers: validated
            .manifest
            .sources
            .iter()
            .map(|source| ManagerRolloutSource {
                manager_id: source.manager_id.clone(),
                endpoint: source.endpoint.clone(),
                host_digest: source.host_digest.clone(),
                selector_digest: source.selector_digest.clone(),
            })
            .collect(),
        manifest_digest: validated.manifest_digest.clone(),
        executor_id: validated.manifest.executor_id.clone(),
        deployment_certificate: validated.manifest.deployment_certificate.clone(),
    })
}

fn ssh(target: &TargetManager, manifest: &ManagerSetManifest) -> Vec<String> {
    helpers::build_ssh_args(
        &target.remote,
        manifest.ssh_key.as_deref(),
        manifest.ssh_port,
    )
}

fn install_bootstrap_backend(target: &TargetManager, manifest: &ManagerSetManifest) -> Result<()> {
    if target.backend == Backend::Systemd {
        let launcher = service_gen::manager_launcher_script().as_bytes();
        let launcher_digest = digest_bytes(launcher);
        helpers::install_remote_bytes(
            launcher,
            &target.remote,
            "/usr/local/libexec/wruntime-manager-launch",
            manifest.ssh_key.as_deref(),
            manifest.ssh_port,
            0o555,
            RemoteInstallClass::Public,
            Some(&launcher_digest),
        )?;
        helpers::install_remote_file(
            &target.backend_spec,
            &target.remote,
            "/etc/systemd/system/wr-manager.service",
            manifest.ssh_key.as_deref(),
            manifest.ssh_port,
            0o444,
            RemoteInstallClass::Public,
            Some(&target.backend_spec_digest),
        )?;
        helpers::run_ssh(
            &ssh(target, manifest),
            "sudo systemctl daemon-reload && sudo systemctl enable wr-manager.service",
        )?;
    }
    Ok(())
}

fn install_credential_tree(target: &TargetManager, manifest: &ManagerSetManifest) -> Result<()> {
    let version = target
        .credential_set
        .file_name()
        .and_then(|name| name.to_str())
        .context("credential set has no version basename")?;
    let destination = format!("{PKI_ROOT}/manager-{}/sets/{version}", target.manager_id);
    helpers::install_remote_directory(
        &target.credential_set,
        &target.remote,
        &destination,
        manifest.ssh_key.as_deref(),
        manifest.ssh_port,
        &target.credential_digest,
    )
}

fn stage_target(
    target: &TargetManager,
    validated: &ValidatedManifest,
    rollout_id: &str,
    lease_epoch: u64,
    lease_expires_unix: u64,
) -> Result<()> {
    let manifest = &validated.manifest;
    let ssh = ssh(target, manifest);
    let config_dir = format!("{STATE_ROOT}/manager-config/{}", target.manager_id);
    let spec_path = format!(
        "{ARTIFACT_ROOT}/backend-specs/{}.json",
        &target.backend_spec_digest[7..]
    );
    helpers::install_remote_file(
        &target.backend_spec,
        &target.remote,
        &spec_path,
        manifest.ssh_key.as_deref(),
        manifest.ssh_port,
        0o444,
        RemoteInstallClass::Public,
        Some(&target.backend_spec_digest),
    )?;
    if target.backend == Backend::Systemd {
        let binary_path = format!(
            "{ARTIFACT_ROOT}/binaries/{}/wr-manager",
            &target.executable_digest[7..]
        );
        helpers::install_remote_file(
            Path::new(&target.executable),
            &target.remote,
            &binary_path,
            manifest.ssh_key.as_deref(),
            manifest.ssh_port,
            0o555,
            RemoteInstallClass::Public,
            Some(&target.executable_digest),
        )?;
    } else {
        helpers::run_ssh(
            &ssh,
            &format!(
                "sudo docker pull {} >/dev/null && sudo docker image inspect --format '{{{{join .RepoDigests \"\\n\"}}}}' {} | grep -Fx -- {} >/dev/null",
                helpers::shell_quote(&target.executable),
                helpers::shell_quote(&target.executable),
                helpers::shell_quote(&target.executable)
            ),
        )?;
    }
    install_credential_tree(target, manifest)?;
    helpers::run_ssh(
        &ssh,
        &format!(
            "sudo install -d -m 0700 {}",
            helpers::shell_quote(&config_dir)
        ),
    )?;
    helpers::install_remote_file(
        &target.config,
        &target.remote,
        &format!("{config_dir}/next.tmp"),
        manifest.ssh_key.as_deref(),
        manifest.ssh_port,
        0o600,
        RemoteInstallClass::Sensitive,
        Some(&target.config_digest),
    )?;
    let (descriptor, digest) = activation_descriptor(target)?;
    if digest != target.new_selector_digest {
        bail!(
            "manager {} new selector digest does not match its activation descriptor",
            target.manager_id
        );
    }
    let descriptor_path = format!(
        "{STATE_ROOT}/manager-activation/{}/{}.next",
        target.manager_id,
        &digest[7..]
    );
    helpers::install_remote_bytes(
        &descriptor,
        &target.remote,
        &descriptor_path,
        manifest.ssh_key.as_deref(),
        manifest.ssh_port,
        0o600,
        RemoteInstallClass::Sensitive,
        Some(&digest),
    )?;
    write_evidence(
        target,
        validated,
        rollout_id,
        lease_epoch,
        1,
        "stage",
        "completed",
        lease_expires_unix,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn write_evidence(
    target: &TargetManager,
    validated: &ValidatedManifest,
    rollout_id: &str,
    lease_epoch: u64,
    sequence: u64,
    effect: &str,
    outcome: &str,
    lease_expires_unix: u64,
    deadline: Option<u64>,
) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(&HostActionEvidence {
        schema_version: SCHEMA_VERSION,
        rollout_id,
        manager_id: &target.manager_id,
        lease_epoch,
        action_sequence: sequence,
        manifest_digest: &validated.manifest_digest,
        executable_digest: &target.executable_digest,
        backend_spec_digest: &target.backend_spec_digest,
        config_digest: &target.config_digest,
        credential_digest: &target.credential_digest,
        old_selector_digest: &target.old_selector_digest,
        new_selector_digest: &target.new_selector_digest,
        effect,
        outcome,
        lease_expires_unix,
        continuation_deadline_unix: deadline,
    })?;
    helpers::install_remote_fenced_json(
        &bytes,
        &target.remote,
        &format!(
            "{STATE_ROOT}/manager-rollouts/{rollout_id}/{}.json",
            target.manager_id
        ),
        validated.manifest.ssh_key.as_deref(),
        validated.manifest.ssh_port,
    )
}

fn lease_expiry_unix(rollout: &ManagerRollout) -> u64 {
    rollout
        .lease_expires_at
        .as_ref()
        .and_then(|timestamp| u64::try_from(timestamp.seconds).ok())
        .unwrap_or(0)
}

fn activate_target(
    target: &TargetManager,
    validated: &ValidatedManifest,
    rollout: &ManagerRollout,
    sole_manager: bool,
) -> Result<ManagerRolloutMemberOutcome> {
    let ssh = ssh(target, &validated.manifest);
    let deadline = sole_manager.then(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            + SOLE_MANAGER_CONTINUATION_SECS
    });
    write_evidence(
        target,
        validated,
        &rollout.rollout_id,
        rollout.lease_epoch,
        2,
        "activate",
        "started",
        lease_expiry_unix(rollout),
        deadline,
    )?;
    let descriptor_path = format!(
        "{STATE_ROOT}/manager-activation/{}/{}.next",
        target.manager_id,
        &target.new_selector_digest[7..]
    );
    let current = format!("{STATE_ROOT}/manager-activation/current-activation.json");
    let config_dir = format!("{STATE_ROOT}/manager-config/{}", target.manager_id);
    let action = service_gen::manager_activation_command(
        target.backend == Backend::Systemd,
        &descriptor_path,
        &current,
        &config_dir,
        &target.config_digest,
        &target.old_selector_digest,
        &target.new_selector_digest,
    );
    let action = if sole_manager {
        format!(
            "sudo systemd-run --quiet --wait --collect --unit={} --property=RuntimeMaxSec={}s /bin/sh -c {}",
            helpers::shell_quote(&format!(
                "wruntime-manager-rollout-{}-{}",
                rollout.rollout_id, rollout.lease_epoch
            )),
            SOLE_MANAGER_CONTINUATION_SECS,
            helpers::shell_quote(&action),
        )
    } else {
        action
    };
    helpers::run_ssh(&ssh, &action).context("fenced target activation failed")?;
    write_evidence(
        target,
        validated,
        &rollout.rollout_id,
        rollout.lease_epoch,
        2,
        "activate",
        "completed",
        lease_expiry_unix(rollout),
        deadline,
    )?;
    Ok(ManagerRolloutMemberOutcome {
        manager_id: target.manager_id.clone(),
        member_role: "target".into(),
        host_action_outcome: "READY_CLOSED".into(),
        error: String::new(),
    })
}

async fn advance(
    epoch: &mut wr_common::manager_client::ManagerEpoch,
    rollout: &ManagerRollout,
    next: ManagerRolloutPhase,
    outcomes: Vec<ManagerRolloutMemberOutcome>,
) -> Result<ManagerRollout> {
    epoch
        .advance_manager_rollout(AdvanceManagerRolloutRequest {
            rollout_id: rollout.rollout_id.clone(),
            executor_id: rollout.executor_id.clone(),
            lease_epoch: rollout.lease_epoch,
            expected_phase: rollout.phase,
            next_phase: next as i32,
            member_outcomes: outcomes,
        })
        .await?
        .into_inner()
        .rollout
        .context("manager omitted rollout")
}

/// Observe manager-owned barriers without changing canonical action evidence.
/// The lease is renewed on the specified ten-second cadence while waiting.
async fn advance_when_ready(
    epoch: &mut wr_common::manager_client::ManagerEpoch,
    mut rollout: ManagerRollout,
    executor_id: &str,
    next: ManagerRolloutPhase,
    outcomes: Vec<ManagerRolloutMemberOutcome>,
) -> Result<ManagerRollout> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    let mut renew_at = tokio::time::Instant::now() + LEASE_RENEW_INTERVAL;
    loop {
        match epoch
            .advance_manager_rollout(AdvanceManagerRolloutRequest {
                rollout_id: rollout.rollout_id.clone(),
                executor_id: rollout.executor_id.clone(),
                lease_epoch: rollout.lease_epoch,
                expected_phase: rollout.phase,
                next_phase: next as i32,
                member_outcomes: outcomes.clone(),
            })
            .await
        {
            Ok(response) => {
                return response
                    .into_inner()
                    .rollout
                    .context("manager omitted rollout")
            }
            Err(error) if error.code() == tonic::Code::FailedPrecondition => {}
            Err(error) => return Err(error.into()),
        }
        if tokio::time::Instant::now() >= deadline {
            bail!(
                "timed out waiting for the {} rollout barrier",
                next.as_str_name()
            );
        }
        if tokio::time::Instant::now() >= renew_at {
            rollout = lease(epoch, &rollout, executor_id).await?;
            renew_at = tokio::time::Instant::now() + LEASE_RENEW_INTERVAL;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn lease(
    epoch: &mut wr_common::manager_client::ManagerEpoch,
    rollout: &ManagerRollout,
    executor_id: &str,
) -> Result<ManagerRollout> {
    epoch
        .lease_manager_rollout(LeaseManagerRolloutRequest {
            rollout_id: rollout.rollout_id.clone(),
            executor_id: executor_id.to_string(),
            expected_lease_epoch: rollout.lease_epoch,
        })
        .await?
        .into_inner()
        .rollout
        .context("manager omitted leased rollout")
}

async fn begin(
    validated: &ValidatedManifest,
) -> Result<(wr_common::manager_client::ManagerEpoch, ManagerRollout)> {
    let endpoint = validated
        .manifest
        .manager_endpoint
        .as_deref()
        .unwrap_or(&validated.manifest.targets[0].endpoint);
    let mut epoch = client::connect_operator(endpoint, RetryClass::DurableCreate).await?;
    let rollout = epoch
        .begin_manager_rollout(create_request(validated)?)
        .await?
        .into_inner()
        .rollout
        .context("manager omitted rollout")?;
    Ok((epoch, rollout))
}

pub fn restore_config(args: RestoreConfigArgs) -> Result<()> {
    let validated = load_manifest(Path::new(&args.manifest))?;
    let target = validated
        .manifest
        .targets
        .iter()
        .find(|target| target.manager_id == args.manager_id)
        .context("manager_id is not a target in the deployment manifest")?;
    let ssh = ssh(target, &validated.manifest);
    let config_dir = format!("{STATE_ROOT}/manager-config/{}", target.manager_id);
    let current = format!("{STATE_ROOT}/manager-activation/current-activation.json");
    let stop = if target.backend == Backend::Systemd {
        "sudo systemctl stop wr-manager.service"
    } else {
        "sudo docker compose --project-name wruntime-manager down"
    };
    let start = if target.backend == Backend::Systemd {
        "sudo systemctl start wr-manager.service"
    } else {
        "previous_spec=$(sudo python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[\"backend_spec_path\"])' \"$current\"); sudo docker compose --project-name wruntime-manager -f \"$previous_spec\" up -d --force-recreate --no-build"
    };
    let command = format!(
        "set -eu; current={current}; config={config}; test -f \"$config/previous.toml\"; test -f \"$current.previous\"; {stop}; sudo mv \"$config/current.toml\" \"$config/restore.tmp\"; sudo mv \"$config/previous.toml\" \"$config/current.toml\"; sudo mv \"$config/restore.tmp\" \"$config/previous.toml\"; sudo mv \"$current\" \"$current.restore.tmp\"; sudo mv \"$current.previous\" \"$current\"; sudo mv \"$current.restore.tmp\" \"$current.previous\"; sudo sync -f \"$config\"; sudo sync -f $(dirname \"$current\"); {start}",
        current = helpers::shell_quote(&current),
        config = helpers::shell_quote(&config_dir),
        stop = stop,
        start = start,
    );
    helpers::run_ssh(&ssh, &command).context("manual previous-config restore failed")
}

pub async fn run(args: DeploySetArgs) -> Result<()> {
    let validated = load_manifest(Path::new(&args.manifest))?;

    // Empty-cluster bootstrap is the only path that starts targets before create.
    // They remain CLOSED_STARTUP and the direct endpoint is fixed by the manifest.
    if validated.manifest.manager_endpoint.is_none() {
        let bootstrap_id = validated.manifest.client_operation_id.clone();
        for target in &validated.manifest.targets {
            stage_target(
                target,
                &validated,
                &bootstrap_id,
                1,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
                    + SOLE_MANAGER_CONTINUATION_SECS,
            )?;
            install_bootstrap_backend(target, &validated.manifest)?;
            let placeholder = ManagerRollout {
                rollout_id: bootstrap_id.clone(),
                lease_epoch: 1,
                ..Default::default()
            };
            activate_target(
                target,
                &validated,
                &placeholder,
                validated.manifest.targets.len() == 1,
            )?;
        }
    }

    let (mut epoch, mut rollout) = begin(&validated).await?;
    rollout = lease(&mut epoch, &rollout, &validated.manifest.executor_id).await?;
    let _renew_cadence = LEASE_RENEW_INTERVAL;

    if rollout.phase == ManagerRolloutPhase::Prepared as i32 {
        rollout = advance(&mut epoch, &rollout, ManagerRolloutPhase::Staging, vec![]).await?;
    }
    if validated.manifest.manager_endpoint.is_some()
        && rollout.phase == ManagerRolloutPhase::Staging as i32
    {
        for target in &validated.manifest.targets {
            if let Err(error) = stage_target(
                target,
                &validated,
                &rollout.rollout_id,
                rollout.lease_epoch,
                lease_expiry_unix(&rollout),
            ) {
                let failure = ManagerRolloutMemberOutcome {
                    manager_id: target.manager_id.clone(),
                    member_role: "target".into(),
                    host_action_outcome: "STAGING_FAILED".into(),
                    error: format!("{error:#}"),
                };
                let _ = advance(
                    &mut epoch,
                    &rollout,
                    ManagerRolloutPhase::FailedPreClose,
                    vec![failure],
                )
                .await;
                return Err(error.context(
                    "manager-set staging failed; FAILED_PRE_CLOSE preserves every active selector",
                ));
            }
            rollout = lease(&mut epoch, &rollout, &validated.manifest.executor_id).await?;
        }
    }
    if rollout.phase == ManagerRolloutPhase::Staging as i32 {
        rollout = advance(
            &mut epoch,
            &rollout,
            ManagerRolloutPhase::ClosingOld,
            vec![],
        )
        .await?;
    }
    if rollout.phase == ManagerRolloutPhase::ClosingOld as i32 {
        // The manager-side barrier observes every source CLOSED_ROLLOUT before accepting this.
        rollout = advance_when_ready(
            &mut epoch,
            rollout,
            &validated.manifest.executor_id,
            ManagerRolloutPhase::OldClosed,
            vec![],
        )
        .await?;
    }
    if rollout.phase == ManagerRolloutPhase::OldClosed as i32 {
        rollout = advance(
            &mut epoch,
            &rollout,
            ManagerRolloutPhase::StartingTarget,
            vec![],
        )
        .await?;
    }
    if rollout.phase == ManagerRolloutPhase::StartingTarget as i32 {
        let mut outcomes = Vec::new();
        for target in &validated.manifest.targets {
            if validated.manifest.manager_endpoint.is_none() {
                outcomes.push(ManagerRolloutMemberOutcome {
                    manager_id: target.manager_id.clone(),
                    member_role: "target".into(),
                    host_action_outcome: "READY_CLOSED".into(),
                    error: String::new(),
                });
                continue;
            }
            match activate_target(
                target,
                &validated,
                &rollout,
                validated.manifest.sources.len() == 1 && validated.manifest.targets.len() == 1,
            ) {
                Ok(outcome) => outcomes.push(outcome),
                Err(error) => {
                    let _ = write_evidence(
                        target,
                        &validated,
                        &rollout.rollout_id,
                        rollout.lease_epoch,
                        2,
                        "activate",
                        "failed",
                        lease_expiry_unix(&rollout),
                        None,
                    );
                    let failure = ManagerRolloutMemberOutcome {
                        manager_id: target.manager_id.clone(),
                        member_role: "target".into(),
                        host_action_outcome: "ACTIVATION_FAILED".into(),
                        error: format!("{error:#}"),
                    };
                    let _ = advance(
                        &mut epoch,
                        &rollout,
                        ManagerRolloutPhase::FailedClosed,
                        vec![failure],
                    )
                    .await;
                    return Err(error.context(
                        "manager activation failed closed; explicit host repair is required",
                    ));
                }
            }
        }
        rollout = advance_when_ready(
            &mut epoch,
            rollout,
            &validated.manifest.executor_id,
            ManagerRolloutPhase::TargetReadyClosed,
            outcomes,
        )
        .await?;
    }
    if rollout.phase == ManagerRolloutPhase::TargetReadyClosed as i32 {
        rollout = advance(
            &mut epoch,
            &rollout,
            ManagerRolloutPhase::ActivatingTarget,
            vec![],
        )
        .await?;
    }
    if rollout.phase == ManagerRolloutPhase::ActivatingTarget as i32 {
        rollout = advance_when_ready(
            &mut epoch,
            rollout,
            &validated.manifest.executor_id,
            ManagerRolloutPhase::Completed,
            vec![],
        )
        .await?;
    }
    println!(
        "{}\t{}",
        rollout.rollout_id,
        ManagerRolloutPhase::try_from(rollout.phase)
            .unwrap_or_default()
            .as_str_name()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target() -> TargetManager {
        TargetManager {
            manager_id: "manager-a".into(),
            endpoint: "https://manager-a.example:9000".into(),
            remote: "root@manager-a.example".into(),
            backend: Backend::Systemd,
            executable: "/tmp/wr-manager".into(),
            executable_digest: format!("sha256:{}", "1".repeat(64)),
            backend_spec: "/tmp/spec".into(),
            backend_spec_digest: format!("sha256:{}", "2".repeat(64)),
            config: "/tmp/config".into(),
            config_digest: format!("sha256:{}", "3".repeat(64)),
            credential_set: "/tmp/set-v1".into(),
            credential_digest: format!("sha256:{}", "4".repeat(64)),
            old_selector_digest: format!("sha256:{}", "5".repeat(64)),
            new_selector_digest: format!("sha256:{}", "6".repeat(64)),
            host_digest: String::new(),
        }
    }

    #[test]
    fn host_digest_binds_every_artifact_and_selector() {
        let mut first = target();
        let digest = target_host_digest(&first).unwrap();
        first.credential_digest = format!("sha256:{}", "7".repeat(64));
        assert_ne!(digest, target_host_digest(&first).unwrap());
    }

    #[test]
    fn activation_is_one_descriptor_and_uses_fixed_protected_roots() {
        let mut target = target();
        let (bytes, digest) = activation_descriptor(&target).unwrap();
        target.new_selector_digest = digest;
        let text = String::from_utf8(bytes).unwrap();
        assert!(text.contains("/opt/wruntime/manager-artifacts/binaries/"));
        assert!(text.contains("/var/lib/wruntime/manager-config/manager-a/current.toml"));
        assert!(text.contains("/etc/wruntime/pki/manager-manager-a/sets/set-v1"));
        assert!(!text.contains("/release"));
    }

    #[test]
    fn compose_requires_immutable_repo_digest() {
        let mut target = target();
        target.backend = Backend::Compose;
        target.executable = "registry.example/wr-manager:latest".into();
        assert!(!target.executable.contains("@sha256:"));
    }
}
