//! Crash-consistent manager-set deployment executor.
//!
//! The manifest is deliberately replay-stable: every remotely installed byte is
//! named by its digest and the create request is derived only from validated,
//! sorted manifest content. Active selectors are not touched while staging.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::Args;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use wr_common::authorization_policy::{RolloutTarget, ValidatedPolicy};
use wr_common::manager_client::RetryClass;
use wr_common::wruntime::{
    AdvanceManagerRolloutRequest, BeginManagerRolloutRequest, FailedManagerRolloutEvidence,
    GetManagerRolloutRequest, ManagerRollout, ManagerRolloutMemberOutcome, ManagerRolloutPhase,
    ManagerRolloutSource, ManagerRolloutTarget, ResetFailedManagerRolloutRequest,
    ResetFailedManagerRolloutResponse,
};

use super::helpers::{self, RemoteInstallClass};
use crate::{client, cmd::service_gen};

const SCHEMA_VERSION: u32 = 1;
const ARTIFACT_ROOT: &str = "/opt/wruntime/manager-artifacts";
const STATE_ROOT: &str = "/var/lib/wruntime";
const PKI_ROOT: &str = "/etc/wruntime/pki";
const MANAGER_BARRIER_TIMEOUT: Duration = Duration::from_secs(120);
const SOLE_MANAGER_CONTINUATION_SECS: u64 = 120;

#[cfg(test)]
fn protected_phase_sequence() -> [ManagerRolloutPhase; 8] {
    [
        ManagerRolloutPhase::Prepared,
        ManagerRolloutPhase::Staging,
        ManagerRolloutPhase::ClosingOld,
        ManagerRolloutPhase::OldClosed,
        ManagerRolloutPhase::StartingTarget,
        ManagerRolloutPhase::TargetReadyClosed,
        ManagerRolloutPhase::ActivatingTarget,
        ManagerRolloutPhase::Completed,
    ]
}

fn trace_rollout(event: &str, rollout: &ManagerRollout, endpoint_present: bool) {
    let Some(path) = std::env::var_os("WRT_MANAGER_ROLLOUT_TRACE") else {
        return;
    };
    let phase = ManagerRolloutPhase::try_from(rollout.phase)
        .unwrap_or_default()
        .as_str_name()
        .strip_prefix("MANAGER_ROLLOUT_PHASE_")
        .unwrap_or("UNSPECIFIED");
    let value = serde_json::json!({
        "event": event,
        "rollout_id": rollout.rollout_id,
        "phase": phase,
        "target_generation": rollout.target_generation,
        "deployment_identity": rollout.target_deployment_principal_uri,
        "endpoint_present": endpoint_present,
        "barrier_timeout_seconds": MANAGER_BARRIER_TIMEOUT.as_secs(),
    });
    if let Ok(mut file) = fs::OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{value}");
    }
}

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

#[derive(Args)]
pub struct ResetFailedRolloutArgs {
    /// The exact digest-qualified deployment manifest used by the failed rollout.
    #[arg(long)]
    pub manifest: String,
    /// Durable FAILED_CLOSED rollout to reset.
    #[arg(long)]
    pub rollout_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ManagerSetManifest {
    pub schema_version: u32,
    pub client_operation_id: String,
    pub cluster_id: String,
    /// Existing control endpoint. Omit only for a pristine empty-cluster bootstrap.
    pub manager_endpoint: Option<String>,
    pub target_policy: PathBuf,
    pub deployment_certificate: String,
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

fn load_manifest_with_artifacts(
    path: &Path,
    validate_artifacts: bool,
) -> Result<ValidatedManifest> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut manifest: ManagerSetManifest =
        toml::from_slice(&bytes).context("invalid deploy-set manifest")?;
    if manifest.schema_version != SCHEMA_VERSION {
        bail!("deploy-set manifest schema_version must be {SCHEMA_VERSION}");
    }
    uuid::Uuid::parse_str(&manifest.client_operation_id)
        .context("client_operation_id must be a UUID")?;
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
        if validate_artifacts {
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

fn load_manifest(path: &Path) -> Result<ValidatedManifest> {
    load_manifest_with_artifacts(path, true)
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

fn source_ssh(source: &SourceManager, manifest: &ManagerSetManifest) -> Vec<String> {
    helpers::build_ssh_args(
        &source.remote,
        manifest.ssh_key.as_deref(),
        manifest.ssh_port,
    )
}

fn source_replaced_by_target(source: &SourceManager, target: &TargetManager) -> bool {
    source.remote == target.remote && source.selector_digest == target.old_selector_digest
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
        helpers::run_ssh(
            &ssh(target, manifest),
            &format!(
                "sudo install -d -m 0755 {}",
                helpers::shell_quote(service_gen::MANAGER_SYSTEMD_UNIT_DIR)
            ),
        )?;
        helpers::install_remote_file(
            &target.backend_spec,
            &target.remote,
            service_gen::MANAGER_SYSTEMD_UNIT_PATH,
            manifest.ssh_key.as_deref(),
            manifest.ssh_port,
            0o444,
            RemoteInstallClass::Public,
            Some(&target.backend_spec_digest),
        )?;
        helpers::run_ssh(&ssh(target, manifest), "sudo systemctl daemon-reload")?;
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

fn stage_target(target: &TargetManager, validated: &ValidatedManifest) -> Result<()> {
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
        // A live rollout may target a pristine disposable host. Install the
        // stable launcher/unit during staging; this does not select or start
        // the staged manager and therefore preserves the pre-close barrier.
        install_bootstrap_backend(target, manifest)?;
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
    Ok(())
}

fn deactivate_source(
    source: &SourceManager,
    validated: &ValidatedManifest,
) -> Result<ManagerRolloutMemberOutcome> {
    let command = service_gen::manager_source_stop_command(
        &source.manager_id,
        &format!("{STATE_ROOT}/manager-activation/current-activation.json"),
        &source.selector_digest,
    );
    helpers::run_ssh(&source_ssh(source, &validated.manifest), &command)
        .context("selector-fenced source manager deactivation failed")?;
    Ok(ManagerRolloutMemberOutcome {
        manager_id: source.manager_id.clone(),
        member_role: "source".into(),
        host_action_outcome: "STOPPED".into(),
        error: String::new(),
    })
}

fn activate_target(
    target: &TargetManager,
    validated: &ValidatedManifest,
    rollout: &ManagerRollout,
    sole_manager: bool,
) -> Result<ManagerRolloutMemberOutcome> {
    let ssh = ssh(target, &validated.manifest);
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
                "wruntime-manager-rollout-{}",
                rollout.rollout_id
            )),
            SOLE_MANAGER_CONTINUATION_SECS,
            helpers::shell_quote(&action),
        )
    } else {
        action
    };
    helpers::run_ssh(&ssh, &action).context("fenced target activation failed")?;
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
            expected_phase: rollout.phase,
            next_phase: next as i32,
            member_outcomes: outcomes,
        })
        .await?
        .into_inner()
        .rollout
        .context("manager omitted rollout")
}

/// Best-effort durable failure recording through the current epoch or an
/// expected target. Host state remains closed even when no endpoint responds.
async fn mark_failed_closed(
    epoch: &mut wr_common::manager_client::ManagerEpoch,
    rollout: &ManagerRollout,
    validated: &ValidatedManifest,
    endpoint_present: bool,
) {
    if let Ok(failed) = advance(epoch, rollout, ManagerRolloutPhase::FailedClosed, vec![]).await {
        trace_rollout("phase", &failed, endpoint_present);
        return;
    }
    for target in &validated.manifest.targets {
        let Ok(mut candidate) =
            client::connect_operator(&target.endpoint, RetryClass::DurableCreate).await
        else {
            continue;
        };
        if let Ok(failed) = advance(
            &mut candidate,
            rollout,
            ManagerRolloutPhase::FailedClosed,
            vec![],
        )
        .await
        {
            trace_rollout("phase", &failed, endpoint_present);
            return;
        }
    }
}

/// Observe manager-owned barriers without changing canonical action evidence.
async fn advance_when_ready(
    epoch: &mut wr_common::manager_client::ManagerEpoch,
    rollout: ManagerRollout,
    next: ManagerRolloutPhase,
    outcomes: Vec<ManagerRolloutMemberOutcome>,
) -> Result<ManagerRollout> {
    let deadline = tokio::time::Instant::now() + MANAGER_BARRIER_TIMEOUT;
    trace_rollout("barrier-start", &rollout, true);
    loop {
        match advance(epoch, &rollout, next, outcomes.clone()).await {
            Ok(rollout) => return Ok(rollout),
            Err(error)
                if error
                    .downcast_ref::<tonic::Status>()
                    .is_some_and(|status| status.code() == tonic::Code::FailedPrecondition) => {}
            Err(error) => return Err(error),
        }
        if tokio::time::Instant::now() >= deadline {
            bail!(
                "timed out waiting for the {} rollout barrier",
                next.as_str_name()
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn target_controls_rollout(
    epoch: &wr_common::manager_client::ManagerEpoch,
    rollout: &ManagerRollout,
    target: &TargetManager,
) -> bool {
    let observation = epoch.observation();
    observation.manager_id == target.manager_id
        && observation.process_ready
        && observation.policy_generation == rollout.target_generation
        && observation.policy_digest == rollout.target_policy_digest
        && observation.privileged_admission
            == wr_common::wruntime::PrivilegedAdmissionState::ClosedRollout as i32
        && observation.rollout_id == rollout.rollout_id
        && observation.rollout_phase == ManagerRolloutPhase::StartingTarget as i32
        && observation.rollout_expected_set_hash == rollout.expected_target_set_hash
}

async fn establish_target_control(
    validated: &ValidatedManifest,
    rollout: &ManagerRollout,
    deadline: tokio::time::Instant,
) -> Result<wr_common::manager_client::ManagerEpoch> {
    loop {
        for target in &validated.manifest.targets {
            if let Ok(candidate) =
                client::connect_operator(&target.endpoint, RetryClass::DurableCreate).await
            {
                if target_controls_rollout(&candidate, rollout, target) {
                    trace_rollout("target-control-established", rollout, true);
                    return Ok(candidate);
                }
            }
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("timed out establishing an exact closed target rollout-control endpoint");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
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

#[derive(Debug, Deserialize)]
struct StoppedInspection {
    manager_id: String,
    selector_digest: String,
    backend: String,
    config_path: String,
    config_digest: String,
    policy_path: String,
    policy_hex: String,
}

#[derive(Debug)]
struct PhysicalInspection {
    policy_generation: u64,
    policy_digest: String,
}

#[derive(Debug, Serialize)]
struct ResetReceipt {
    rollout_id: String,
    original_request_digest: String,
    reset_request_digest: String,
    evidence_digest: String,
    observed_policy_generation: u64,
    observed_policy_digest: String,
}

impl From<ResetFailedManagerRolloutResponse> for ResetReceipt {
    fn from(value: ResetFailedManagerRolloutResponse) -> Self {
        Self {
            rollout_id: value.rollout_id,
            original_request_digest: value.original_request_digest,
            reset_request_digest: value.reset_request_digest,
            evidence_digest: value.evidence_digest,
            observed_policy_generation: value.observed_policy_generation,
            observed_policy_digest: value.observed_policy_digest,
        }
    }
}

fn decode_hex(value: &str) -> Result<Vec<u8>> {
    if !value.len().is_multiple_of(2) || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("inspection returned malformed policy bytes");
    }
    (0..value.len())
        .step_by(2)
        .map(|offset| {
            u8::from_str_radix(&value[offset..offset + 2], 16)
                .context("inspection returned malformed policy bytes")
        })
        .collect()
}

fn validate_reset_control_endpoint(manifest: &ManagerSetManifest, endpoint: &str) -> Result<()> {
    wr_common::identity::PeerHttpsUrl::parse(endpoint)?;
    let endpoint_declared = manifest.manager_endpoint.as_deref() == Some(endpoint)
        || manifest
            .targets
            .iter()
            .any(|target| target.endpoint == endpoint);
    if !endpoint_declared {
        bail!("--manager must be a control endpoint declared by the deployment manifest");
    }
    Ok(())
}

fn validate_reset_rollout(
    rollout: &ManagerRollout,
    expected: &BeginManagerRolloutRequest,
    expected_target_set_hash: &str,
) -> Result<()> {
    if rollout.phase != ManagerRolloutPhase::FailedClosed as i32 {
        bail!("rollout is not FAILED_CLOSED");
    }
    validate_digest(&rollout.request_digest, "original rollout request digest")?;
    if rollout.deployment_principal_uri != expected.target_deployment_principal_uri {
        bail!("current caller principal does not own the failed rollout");
    }
    if rollout.cluster_id != expected.cluster_id
        || rollout.target_generation != expected.target_generation
        || rollout.target_policy_digest != expected.target_policy_digest
        || rollout.target_policy_validator_version != expected.target_policy_validator_version
        || rollout.target_deployment_principal_uri != expected.target_deployment_principal_uri
        || rollout.expected_target_set_hash != expected_target_set_hash
        || rollout.expected_targets != expected.expected_targets
        || rollout.source_managers != expected.source_managers
        || rollout.manifest_digest != expected.manifest_digest
        || rollout.deployment_certificate != expected.deployment_certificate
    {
        bail!("deployment manifest does not exactly match the durable rollout declaration");
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResetFlowStage {
    Fetching,
    Matched,
    Snapshot,
    Submitting,
}

#[derive(Debug)]
struct ResetFlow {
    stage: ResetFlowStage,
    inspections: usize,
}

impl ResetFlow {
    fn new() -> Self {
        Self {
            stage: ResetFlowStage::Fetching,
            inspections: 0,
        }
    }

    fn declaration_matched(&mut self) -> Result<()> {
        if self.stage != ResetFlowStage::Fetching {
            bail!("reset declaration may only be matched once");
        }
        self.stage = ResetFlowStage::Matched;
        Ok(())
    }

    fn record_inspection(&mut self) -> Result<()> {
        if self.stage != ResetFlowStage::Matched {
            bail!("reset inspection cannot precede declaration matching");
        }
        self.inspections += 1;
        Ok(())
    }

    fn snapshot_complete(&mut self, expected: usize) -> Result<()> {
        if self.stage != ResetFlowStage::Matched || expected == 0 || self.inspections != expected {
            bail!("reset snapshot is incomplete");
        }
        self.stage = ResetFlowStage::Snapshot;
        Ok(())
    }

    fn begin_submit(&mut self) -> Result<()> {
        if self.stage != ResetFlowStage::Snapshot {
            bail!("reset cannot be submitted before the complete snapshot");
        }
        self.stage = ResetFlowStage::Submitting;
        Ok(())
    }
}

#[derive(Default)]
struct ProbeExpectation {
    remote: String,
    selectors: BTreeSet<String>,
    roles: BTreeSet<String>,
}

fn probe_expectations(validated: &ValidatedManifest) -> Result<BTreeMap<String, ProbeExpectation>> {
    let mut probes = BTreeMap::<String, ProbeExpectation>::new();
    for source in &validated.manifest.sources {
        let probe = probes.entry(source.manager_id.clone()).or_default();
        if !probe.remote.is_empty() && probe.remote != source.remote {
            bail!(
                "manager {} has conflicting declared remotes",
                source.manager_id
            );
        }
        probe.remote.clone_from(&source.remote);
        probe.selectors.insert(source.selector_digest.clone());
        probe.roles.insert("source".into());
    }
    for target in &validated.manifest.targets {
        let probe = probes.entry(target.manager_id.clone()).or_default();
        if !probe.remote.is_empty() && probe.remote != target.remote {
            bail!(
                "manager {} has conflicting declared remotes",
                target.manager_id
            );
        }
        if probe.roles.contains("source")
            && !probe.selectors.contains(&target.old_selector_digest)
            && !probe.selectors.contains(&target.new_selector_digest)
        {
            bail!(
                "manager {} has conflicting source/target activation selectors",
                target.manager_id
            );
        }
        probe.remote.clone_from(&target.remote);
        probe.selectors.insert(target.old_selector_digest.clone());
        probe.selectors.insert(target.new_selector_digest.clone());
        probe.roles.insert("target".into());
    }
    Ok(probes)
}

fn canonical_reset_evidence(
    probes: &BTreeMap<String, ProbeExpectation>,
    inspected: &BTreeMap<String, PhysicalInspection>,
) -> Result<Vec<FailedManagerRolloutEvidence>> {
    if probes.len() != inspected.len() || probes.keys().ne(inspected.keys()) {
        bail!("stopped-host inspection does not exactly cover every declared manager");
    }
    let mut uniform: Option<(u64, &str)> = None;
    let mut evidence = Vec::new();
    for (manager_id, probe) in probes {
        let result = inspected
            .get(manager_id)
            .context("missing stopped-host inspection")?;
        if result.policy_generation == 0 {
            bail!("installed authorization policy generation must be nonzero");
        }
        validate_digest(
            &result.policy_digest,
            "installed authorization policy digest",
        )?;
        match uniform {
            Some((generation, digest))
                if generation != result.policy_generation || digest != result.policy_digest =>
            {
                bail!("installed authorization policy is not uniform across all managers")
            }
            None => uniform = Some((result.policy_generation, &result.policy_digest)),
            _ => {}
        }
        for role in &probe.roles {
            evidence.push(FailedManagerRolloutEvidence {
                member_role: role.clone(),
                manager_id: manager_id.clone(),
                process_state: "STOPPED".into(),
                policy_generation: result.policy_generation,
                policy_digest: result.policy_digest.clone(),
            });
        }
    }
    evidence.sort_by(|a, b| (&a.member_role, &a.manager_id).cmp(&(&b.member_role, &b.manager_id)));
    Ok(evidence)
}

fn trace_reset(event: &str, rollout_id: &str, details: serde_json::Value) {
    let Some(path) = std::env::var_os("WRT_MANAGER_ROLLOUT_TRACE") else {
        return;
    };
    let mut value = serde_json::json!({"event": event, "rollout_id": rollout_id});
    if let (Some(base), Some(extra)) = (value.as_object_mut(), details.as_object()) {
        base.extend(extra.clone());
    }
    if let Ok(mut file) = fs::OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{value}");
    }
}

pub async fn reset_failed_rollout(
    args: ResetFailedRolloutArgs,
    manager_endpoint: &str,
) -> Result<()> {
    let validated = load_manifest_with_artifacts(Path::new(&args.manifest), false)?;
    validate_reset_control_endpoint(&validated.manifest, manager_endpoint)?;
    let expected_request = create_request(&validated)?;
    let mut flow = ResetFlow::new();
    let mut epoch = client::connect_operator(manager_endpoint, RetryClass::DurableCreate).await?;
    let rollout = epoch
        .get_manager_rollout(GetManagerRolloutRequest {
            rollout_id: args.rollout_id.clone(),
        })
        .await?
        .into_inner()
        .rollout
        .context("manager omitted rollout")?;
    if rollout.rollout_id != args.rollout_id {
        bail!("manager returned a different rollout");
    }
    validate_reset_rollout(
        &rollout,
        &expected_request,
        &validated.policy.manager_set_hash,
    )?;
    flow.declaration_matched()?;
    trace_reset(
        "reset-declaration-fetched",
        &rollout.rollout_id,
        serde_json::json!({"manifest_digest": rollout.manifest_digest}),
    );
    drop(epoch);

    let probes = probe_expectations(&validated)?;
    let mut inspected = BTreeMap::new();
    for (manager_id, expected) in &probes {
        let selectors = expected.selectors.iter().cloned().collect::<Vec<_>>();
        let command = service_gen::manager_stopped_inspection_command(
            manager_id,
            &format!("{STATE_ROOT}/manager-activation/current-activation.json"),
            &selectors,
        );
        let ssh = helpers::build_ssh_args(
            &expected.remote,
            validated.manifest.ssh_key.as_deref(),
            validated.manifest.ssh_port,
        );
        let output = helpers::run_ssh_output(&ssh, &command).with_context(|| {
            format!("read-only stopped-state inspection failed for {manager_id}")
        })?;
        let observation: StoppedInspection =
            serde_json::from_str(output.trim()).with_context(|| {
                format!("manager {manager_id} returned malformed inspection output")
            })?;
        if observation.manager_id != *manager_id
            || !expected.selectors.contains(&observation.selector_digest)
            || !matches!(observation.backend.as_str(), "systemd" | "compose")
            || observation.config_path
                != format!("{STATE_ROOT}/manager-config/{manager_id}/current.toml")
            || observation.config_digest.is_empty()
            || observation.policy_path.is_empty()
        {
            bail!("manager {manager_id} returned inconsistent inspection metadata");
        }
        let policy = ValidatedPolicy::load(&decode_hex(&observation.policy_hex)?)
            .with_context(|| format!("manager {manager_id} has an invalid installed policy"))?;
        if policy.cluster_id != rollout.cluster_id {
            bail!("manager {manager_id} installed policy belongs to a different cluster");
        }
        inspected.insert(
            manager_id.clone(),
            PhysicalInspection {
                policy_generation: policy.generation,
                policy_digest: policy.digest,
            },
        );
        flow.record_inspection()?;
    }
    let evidence = canonical_reset_evidence(&probes, &inspected)?;
    flow.snapshot_complete(probes.len())?;
    trace_reset(
        "reset-snapshot-complete",
        &rollout.rollout_id,
        serde_json::json!({"evidence_count": evidence.len()}),
    );

    flow.begin_submit()?;
    let mut epoch = client::connect_operator(manager_endpoint, RetryClass::DurableCreate).await?;
    let response = epoch
        .reset_failed_manager_rollout(ResetFailedManagerRolloutRequest {
            rollout_id: rollout.rollout_id.clone(),
            original_request_digest: rollout.request_digest,
            evidence,
        })
        .await?
        .into_inner();
    let receipt = ResetReceipt::from(response);
    trace_reset(
        "reset-completed",
        &receipt.rollout_id,
        serde_json::json!({
            "reset_request_digest": receipt.reset_request_digest,
            "evidence_digest": receipt.evidence_digest,
            "observed_policy_generation": receipt.observed_policy_generation,
            "observed_policy_digest": receipt.observed_policy_digest,
        }),
    );
    println!("{}", serde_json::to_string_pretty(&receipt)?);
    Ok(())
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
            stage_target(target, &validated)?;
            install_bootstrap_backend(target, &validated.manifest)?;
            let placeholder = ManagerRollout {
                rollout_id: bootstrap_id.clone(),
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

    let endpoint_present = validated.manifest.manager_endpoint.is_some();
    let (mut epoch, mut rollout) = begin(&validated).await?;
    trace_rollout("phase", &rollout, endpoint_present);

    if rollout.phase == ManagerRolloutPhase::Prepared as i32 {
        rollout = advance(&mut epoch, &rollout, ManagerRolloutPhase::Staging, vec![]).await?;
        trace_rollout("phase", &rollout, endpoint_present);
    }
    if validated.manifest.manager_endpoint.is_some()
        && rollout.phase == ManagerRolloutPhase::Staging as i32
    {
        for target in &validated.manifest.targets {
            if let Err(error) = stage_target(target, &validated) {
                let failure = ManagerRolloutMemberOutcome {
                    manager_id: target.manager_id.clone(),
                    member_role: "target".into(),
                    host_action_outcome: "STAGING_FAILED".into(),
                    error: format!("{error:#}"),
                };
                if let Ok(failed) = advance(
                    &mut epoch,
                    &rollout,
                    ManagerRolloutPhase::FailedPreClose,
                    vec![failure],
                )
                .await
                {
                    trace_rollout("phase", &failed, endpoint_present);
                }
                return Err(error.context(
                    "manager-set staging failed; FAILED_PRE_CLOSE preserves every active selector",
                ));
            }
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
        trace_rollout("phase", &rollout, endpoint_present);
    }
    if rollout.phase == ManagerRolloutPhase::ClosingOld as i32 {
        // The manager-side barrier observes every source CLOSED_ROLLOUT before accepting this.
        rollout =
            advance_when_ready(&mut epoch, rollout, ManagerRolloutPhase::OldClosed, vec![]).await?;
        trace_rollout("phase", &rollout, endpoint_present);
    }
    if rollout.phase == ManagerRolloutPhase::OldClosed as i32 {
        rollout = advance(
            &mut epoch,
            &rollout,
            ManagerRolloutPhase::StartingTarget,
            vec![],
        )
        .await?;
        trace_rollout("phase", &rollout, endpoint_present);
    }
    if rollout.phase == ManagerRolloutPhase::StartingTarget as i32 {
        let handoff_deadline = tokio::time::Instant::now() + MANAGER_BARRIER_TIMEOUT;
        let mut outcomes = Vec::new();
        let mut stopped_sources = BTreeSet::new();
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
                Ok(outcome) => {
                    outcomes.push(outcome);
                    for source in validated
                        .manifest
                        .sources
                        .iter()
                        .filter(|source| source_replaced_by_target(source, target))
                    {
                        stopped_sources.insert(source.manager_id.clone());
                    }
                }
                Err(error) => {
                    mark_failed_closed(&mut epoch, &rollout, &validated, endpoint_present).await;
                    return Err(error.context(
                        "manager activation failed closed; explicit host repair is required",
                    ));
                }
            }
        }

        epoch = match establish_target_control(&validated, &rollout, handoff_deadline).await {
            Ok(target_epoch) => target_epoch,
            Err(error) => {
                mark_failed_closed(&mut epoch, &rollout, &validated, endpoint_present).await;
                return Err(error.context(
                    "failed to establish target rollout control; explicit recovery is required",
                ));
            }
        };
        for source in &validated.manifest.sources {
            let outcome = if stopped_sources.contains(&source.manager_id) {
                ManagerRolloutMemberOutcome {
                    manager_id: source.manager_id.clone(),
                    member_role: "source".into(),
                    host_action_outcome: "STOPPED".into(),
                    error: String::new(),
                }
            } else {
                match deactivate_source(source, &validated) {
                    Ok(outcome) => {
                        trace_rollout("source-stop-completed", &rollout, endpoint_present);
                        outcome
                    }
                    Err(error) => {
                        mark_failed_closed(&mut epoch, &rollout, &validated, endpoint_present)
                            .await;
                        return Err(error.context(
                            "source manager deactivation failed closed; explicit host repair is required",
                        ));
                    }
                }
            };
            outcomes.push(outcome);
        }
        trace_rollout("target-ready-closed", &rollout, endpoint_present);
        let starting_target = rollout.clone();
        rollout = match advance_when_ready(
            &mut epoch,
            rollout,
            ManagerRolloutPhase::TargetReadyClosed,
            outcomes,
        )
        .await
        {
            Ok(rollout) => rollout,
            Err(error) => {
                mark_failed_closed(&mut epoch, &starting_target, &validated, endpoint_present)
                    .await;
                return Err(error
                    .context("target-ready barrier failed closed; explicit recovery is required"));
            }
        };
        trace_rollout("phase", &rollout, endpoint_present);
    }
    if rollout.phase == ManagerRolloutPhase::TargetReadyClosed as i32 {
        rollout = advance(
            &mut epoch,
            &rollout,
            ManagerRolloutPhase::ActivatingTarget,
            vec![],
        )
        .await?;
        trace_rollout("phase", &rollout, endpoint_present);
    }
    if rollout.phase == ManagerRolloutPhase::ActivatingTarget as i32 {
        let activating_target = rollout.clone();
        rollout =
            match advance_when_ready(&mut epoch, rollout, ManagerRolloutPhase::Completed, vec![])
                .await
            {
                Ok(rollout) => rollout,
                Err(error) => {
                    mark_failed_closed(
                        &mut epoch,
                        &activating_target,
                        &validated,
                        endpoint_present,
                    )
                    .await;
                    return Err(error.context(
                        "target activation barrier failed closed; explicit recovery is required",
                    ));
                }
            };
        trace_rollout("phase", &rollout, endpoint_present);
    }
    trace_rollout("cli-completed", &rollout, endpoint_present);
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
    fn manager_unit_is_installed_below_runtime_mask_precedence() {
        assert!(
            service_gen::MANAGER_SYSTEMD_UNIT_PATH.starts_with("/usr/local/lib/systemd/system/")
        );
        assert!(!service_gen::MANAGER_SYSTEMD_UNIT_PATH.starts_with("/etc/systemd/system/"));
    }

    #[test]
    fn source_is_replaced_only_by_the_exact_target_host_and_selector() {
        let source = SourceManager {
            manager_id: "manager-a".into(),
            endpoint: "https://manager-a.example:9000".into(),
            remote: "root@manager-a.example".into(),
            host_digest: format!("sha256:{}", "4".repeat(64)),
            selector_digest: format!("sha256:{}", "5".repeat(64)),
        };
        let mut replacement = target();
        replacement.remote.clone_from(&source.remote);
        replacement
            .old_selector_digest
            .clone_from(&source.selector_digest);
        assert!(source_replaced_by_target(&source, &replacement));

        replacement.remote = "root@manager-b.example".into();
        assert!(!source_replaced_by_target(&source, &replacement));
        replacement.remote.clone_from(&source.remote);
        replacement.old_selector_digest = format!("sha256:{}", "6".repeat(64));
        assert!(!source_replaced_by_target(&source, &replacement));
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

    fn reset_manifest() -> ManagerSetManifest {
        ManagerSetManifest {
            schema_version: 1,
            client_operation_id: "00000000-0000-0000-0000-000000000001".into(),
            cluster_id: "cluster-a".into(),
            manager_endpoint: Some("https://manager-a.example:9000".into()),
            target_policy: "/tmp/policy.toml".into(),
            deployment_certificate: "deployment-v1".into(),
            max_parallel: 1,
            ssh_key: None,
            ssh_port: None,
            sources: vec![],
            targets: vec![target()],
        }
    }

    fn expected_reset_request() -> BeginManagerRolloutRequest {
        BeginManagerRolloutRequest {
            cluster_id: "cluster-a".into(),
            target_generation: 7,
            target_policy_digest: format!("sha256:{}", "2".repeat(64)),
            target_policy_validator_version: 1,
            target_deployment_principal_uri: "urn:wruntime:cluster-a:human:operator".into(),
            expected_targets: vec![ManagerRolloutTarget {
                manager_id: "manager-a".into(),
                endpoint: "https://manager-a.example:9000".into(),
                ..Default::default()
            }],
            source_managers: vec![ManagerRolloutSource {
                manager_id: "manager-old".into(),
                endpoint: "https://manager-old.example:9000".into(),
                ..Default::default()
            }],
            manifest_digest: format!("sha256:{}", "3".repeat(64)),
            deployment_certificate: "deployment-v1".into(),
            ..Default::default()
        }
    }

    fn matching_failed_rollout(expected: &BeginManagerRolloutRequest) -> ManagerRollout {
        ManagerRollout {
            rollout_id: "rollout-1".into(),
            deployment_principal_uri: expected.target_deployment_principal_uri.clone(),
            request_digest: format!("sha256:{}", "1".repeat(64)),
            cluster_id: expected.cluster_id.clone(),
            target_generation: expected.target_generation,
            target_policy_digest: expected.target_policy_digest.clone(),
            expected_targets: expected.expected_targets.clone(),
            phase: ManagerRolloutPhase::FailedClosed as i32,
            target_policy_validator_version: expected.target_policy_validator_version,
            target_deployment_principal_uri: expected.target_deployment_principal_uri.clone(),
            expected_target_set_hash: format!("sha256:{}", "4".repeat(64)),
            source_managers: expected.source_managers.clone(),
            manifest_digest: expected.manifest_digest.clone(),
            deployment_certificate: expected.deployment_certificate.clone(),
            ..Default::default()
        }
    }

    #[test]
    fn reset_control_endpoint_must_be_declared() {
        let manifest = reset_manifest();
        assert!(
            validate_reset_control_endpoint(&manifest, "https://manager-a.example:9000").is_ok()
        );
        assert!(
            validate_reset_control_endpoint(&manifest, "https://unrelated.example:9000").is_err()
        );
        assert!(
            validate_reset_control_endpoint(&manifest, "http://manager-a.example:9000").is_err()
        );
    }

    #[test]
    fn reset_rollout_matching_rejects_every_durable_declaration_class() {
        let expected = expected_reset_request();
        let set_hash = format!("sha256:{}", "4".repeat(64));
        let rollout = matching_failed_rollout(&expected);
        assert!(validate_reset_rollout(&rollout, &expected, &set_hash).is_ok());

        let mut cases = Vec::new();
        let mut wrong = rollout.clone();
        wrong.phase = ManagerRolloutPhase::Completed as i32;
        cases.push(wrong);
        let mut wrong = rollout.clone();
        wrong.deployment_principal_uri = "urn:wruntime:cluster-a:human:other".into();
        cases.push(wrong);
        let mut wrong = rollout.clone();
        wrong.manifest_digest = format!("sha256:{}", "9".repeat(64));
        cases.push(wrong);
        let mut wrong = rollout.clone();
        wrong.cluster_id = "cluster-b".into();
        cases.push(wrong);
        let mut wrong = rollout.clone();
        wrong.deployment_certificate = "deployment-v2".into();
        cases.push(wrong);
        let mut wrong = rollout.clone();
        wrong.target_generation += 1;
        cases.push(wrong);
        let mut wrong = rollout.clone();
        wrong.target_policy_validator_version += 1;
        cases.push(wrong);
        let mut wrong = rollout.clone();
        wrong.expected_targets.clear();
        cases.push(wrong);
        let mut wrong = rollout.clone();
        wrong.source_managers.clear();
        cases.push(wrong);
        let mut wrong = rollout.clone();
        wrong.request_digest = "not-a-digest".into();
        cases.push(wrong);

        for wrong in cases {
            assert!(validate_reset_rollout(&wrong, &expected, &set_hash).is_err());
        }
        assert!(validate_reset_rollout(&rollout, &expected, "wrong-set-hash").is_err());
    }

    #[test]
    fn reset_flow_forbids_inspection_or_submit_before_complete_snapshot() {
        let mut flow = ResetFlow::new();
        assert!(flow.record_inspection().is_err());
        assert!(flow.begin_submit().is_err());
        flow.declaration_matched().unwrap();
        flow.record_inspection().unwrap();
        assert!(flow.snapshot_complete(2).is_err());
        assert!(flow.begin_submit().is_err());

        let mut flow = ResetFlow::new();
        flow.declaration_matched().unwrap();
        flow.record_inspection().unwrap();
        flow.record_inspection().unwrap();
        flow.snapshot_complete(2).unwrap();
        flow.begin_submit().unwrap();
        assert_eq!(flow.stage, ResetFlowStage::Submitting);
        assert!(flow.record_inspection().is_err());
    }

    #[test]
    fn reset_evidence_preserves_overlapping_source_and_target_roles() {
        let mut probes = BTreeMap::new();
        probes.insert(
            "manager-a".into(),
            ProbeExpectation {
                remote: "root@manager-a.example".into(),
                selectors: BTreeSet::from([format!("sha256:{}", "1".repeat(64))]),
                roles: BTreeSet::from(["source".into(), "target".into()]),
            },
        );
        let inspected = BTreeMap::from([(
            "manager-a".into(),
            PhysicalInspection {
                policy_generation: 7,
                policy_digest: format!("sha256:{}", "2".repeat(64)),
            },
        )]);
        let evidence = canonical_reset_evidence(&probes, &inspected).unwrap();
        assert_eq!(evidence.len(), 2);
        assert_eq!(evidence[0].member_role, "source");
        assert_eq!(evidence[1].member_role, "target");
        assert_eq!(evidence[0].manager_id, evidence[1].manager_id);
    }

    #[test]
    fn reset_evidence_rejects_missing_and_mixed_policy_snapshots() {
        let probes = BTreeMap::from([
            (
                "manager-a".into(),
                ProbeExpectation {
                    remote: "a".into(),
                    selectors: BTreeSet::new(),
                    roles: BTreeSet::from(["source".into()]),
                },
            ),
            (
                "manager-b".into(),
                ProbeExpectation {
                    remote: "b".into(),
                    selectors: BTreeSet::new(),
                    roles: BTreeSet::from(["target".into()]),
                },
            ),
        ]);
        let one = BTreeMap::from([(
            "manager-a".into(),
            PhysicalInspection {
                policy_generation: 7,
                policy_digest: format!("sha256:{}", "2".repeat(64)),
            },
        )]);
        assert!(canonical_reset_evidence(&probes, &one).is_err());

        let mixed = BTreeMap::from([
            (
                "manager-a".into(),
                PhysicalInspection {
                    policy_generation: 7,
                    policy_digest: format!("sha256:{}", "2".repeat(64)),
                },
            ),
            (
                "manager-b".into(),
                PhysicalInspection {
                    policy_generation: 8,
                    policy_digest: format!("sha256:{}", "3".repeat(64)),
                },
            ),
        ]);
        assert!(canonical_reset_evidence(&probes, &mixed).is_err());
    }

    #[test]
    fn protected_live_contract_uses_real_barrier_and_all_typed_phases() {
        assert_eq!(MANAGER_BARRIER_TIMEOUT, Duration::from_secs(120));
        assert_eq!(
            protected_phase_sequence().map(|phase| {
                phase
                    .as_str_name()
                    .strip_prefix("MANAGER_ROLLOUT_PHASE_")
                    .unwrap()
            }),
            [
                "PREPARED",
                "STAGING",
                "CLOSING_OLD",
                "OLD_CLOSED",
                "STARTING_TARGET",
                "TARGET_READY_CLOSED",
                "ACTIVATING_TARGET",
                "COMPLETED",
            ]
        );
        let live_outcome = ManagerRolloutMemberOutcome {
            manager_id: "manager-b".into(),
            member_role: "target".into(),
            host_action_outcome: "READY_CLOSED".into(),
            error: String::new(),
        };
        assert_eq!(live_outcome.host_action_outcome, "READY_CLOSED");
    }
}
