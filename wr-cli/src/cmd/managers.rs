use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use flate2::write::GzEncoder;
use flate2::Compression;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tabled::builder::Builder;
use wr_common::authorization_policy::ValidatedPolicy;

use super::build_helpers;
use super::bundle;
use super::config::ManagerConfig;
use super::deploy_config::{self, DeployConfig, DeployFormat};
use super::helpers;
use super::service_gen::{self, DockerfileSpec};
use crate::{client, display};

#[derive(Args)]
pub struct ManagersArgs {
    #[command(subcommand)]
    pub command: ManagersCommand,
}

#[derive(Subcommand)]
pub enum ManagersCommand {
    /// List all active managers in the cluster
    List,
    /// Build and package a host-agnostic manager deployment bundle
    Bundle(BundleArgs),
    /// Deploy a manager bundle to a remote host
    Deploy(DeployArgs),
    /// Deploy or take over a complete digest-qualified manager set
    DeploySet(super::manager_deploy_set::DeploySetArgs),
    /// Explicitly restore the bounded previous manager config/activation
    RestoreConfig(super::manager_deploy_set::RestoreConfigArgs),
    /// Inspect a manager bundle without deploying
    InspectBundle(StatusArgs),
    /// Validate a local authorization policy.
    Policy(PolicyArgs),
}

#[derive(Args)]
pub struct PolicyArgs {
    #[command(subcommand)]
    pub command: PolicyCommand,
}
#[derive(Subcommand)]
pub enum PolicyCommand {
    Validate(PolicyValidateArgs),
}
#[derive(Args)]
pub struct PolicyValidateArgs {
    #[arg(long)]
    pub policy: String,
}

#[derive(Args)]
pub struct BundleArgs {
    /// Manager config file (used as template; db_url will be replaced with {db_url})
    #[arg(long)]
    manager_config: String,
    /// Deploy config file (default: auto-discover wr-deploy.toml in CWD)
    #[arg(long)]
    config: Option<String>,
    /// Cargo target triple for cross-compilation
    #[arg(long, default_value = "x86_64-unknown-linux-gnu", env = "WR_TARGET")]
    target: String,
    /// Base directory for installed files on the remote host
    #[arg(long, default_value = "/opt/wruntime")]
    workdir: String,
    /// Docker image name prefix
    #[arg(long, default_value = "wr")]
    image_prefix: String,
    /// Output tarball path [default: wr-manager-bundle.tar.gz]
    #[arg(long)]
    output: Option<String>,
    /// Skip compilation (reuse existing binary)
    #[arg(long)]
    skip_build: bool,
    /// Disable OpenTelemetry export in generated service units
    #[arg(long)]
    no_otel: bool,
}

#[derive(Args)]
pub struct DeployArgs {
    /// Path to the manager bundle tarball
    bundle: String,
    /// Remote host in user@host format
    remote: String,
    /// Deploy config file (default: auto-discover wr-deploy.toml in CWD)
    #[arg(long)]
    config: Option<String>,
    /// Deployment format [default: systemd]
    #[arg(long)]
    format: Option<DeployFormat>,
    /// Postgres database URL
    #[arg(long)]
    db_url: Option<String>,
    /// SSH private key path
    #[arg(long)]
    ssh_key: Option<String>,
    /// SSH port
    #[arg(long)]
    ssh_port: Option<u16>,
    /// Secret encryption key (hex-encoded, 32 bytes / 64 hex chars)
    #[arg(long)]
    secret_key: Option<String>,
    /// Local directory containing CA + manager certificates (from `wr-cli cert`)
    #[arg(long, default_value = "./certs")]
    cert_dir: String,
    /// Manager's externally-reachable gRPC address (derived from remote host if omitted)
    #[arg(long)]
    advertise_address: Option<String>,
}

#[derive(Args)]
pub struct StatusArgs {
    /// Path to the manager bundle tarball
    bundle: String,
}

// --- Manifest ---

#[derive(serde::Serialize, serde::Deserialize)]
struct ManagerManifest {
    target: String,
    workdir: String,
    image_prefix: String,
    listen_address: String,
    template_vars: Vec<String>,
    checksums: BTreeMap<String, String>,
}

const MANAGER_LAUNCHER_ARCHIVE_PATH: &str = "wr-manager/bin/wr-manager-launch";
const MANAGER_LAUNCHER_INSTALL_PATH: &str = "/usr/local/libexec/wruntime-manager-launch";
const MANAGER_SECRET_ENV_PATH: &str = "/var/lib/wruntime/manager-secrets/runtime.env";

// --- Entry point ---

pub async fn run(args: ManagersArgs, manager: Option<&str>) -> Result<()> {
    match args.command {
        ManagersCommand::List => {
            let mgr = manager
                .ok_or_else(|| anyhow::anyhow!("--manager is required for managers list"))?;
            list(mgr).await
        }
        ManagersCommand::Bundle(bundle_args) => bundle(bundle_args),
        ManagersCommand::Deploy(deploy_args) => deploy(deploy_args).await,
        ManagersCommand::DeploySet(deploy_args) => {
            super::manager_deploy_set::run(deploy_args).await
        }
        ManagersCommand::RestoreConfig(restore_args) => {
            super::manager_deploy_set::restore_config(restore_args)
        }
        ManagersCommand::InspectBundle(status_args) => status(status_args),
        ManagersCommand::Policy(args) => policy_command(args),
    }
}

#[derive(Serialize)]
struct PolicyValidationOutput {
    validator_version: u32,
    schema_version: u32,
    generation: u64,
    digest: String,
    cluster_id: String,
    manager_ids: Vec<String>,
    target_set_hash: String,
    caller_principal_uri: String,
    caller_leaf_fingerprint: String,
    caller_can_begin: bool,
}

fn load_policy_and_caller(path: &str) -> Result<(ValidatedPolicy, wr_common::tls::LeafEvidence)> {
    let bytes =
        fs::read(path).with_context(|| format!("failed to read authorization policy {path}"))?;
    let policy = ValidatedPolicy::load(&bytes)?;
    let tls = client::tls_config().context("global manager client TLS is not initialized")?;
    let cluster = wr_common::identity::ClusterId::parse(&policy.cluster_id)?;
    let caller = wr_common::tls::load_client_leaf_evidence(&tls.cert_path, Some(&cluster))?;
    Ok((policy, caller))
}

fn policy_output(
    policy: &ValidatedPolicy,
    caller: &wr_common::tls::LeafEvidence,
) -> Result<PolicyValidationOutput> {
    let principal = caller
        .principal
        .as_ref()
        .context("client certificate has no principal")?;
    let targets = policy.manager_targets();
    let manager_ids = targets
        .iter()
        .map(|target| target.manager_id.clone())
        .collect::<Vec<_>>();
    Ok(PolicyValidationOutput {
        validator_version: wr_common::authorization_policy::AUTHORIZATION_POLICY_VALIDATOR_VERSION,
        schema_version: policy.schema_version,
        generation: policy.generation,
        digest: policy.digest.clone(),
        cluster_id: policy.cluster_id.clone(),
        manager_ids: manager_ids.clone(),
        target_set_hash: policy.manager_set_hash.clone(),
        caller_principal_uri: principal.to_string(),
        caller_leaf_fingerprint: caller.fingerprint.clone(),
        caller_can_begin: !policy
            .revoked_leaf_fingerprints
            .contains(&caller.fingerprint)
            && policy.authorizes_rollout(principal.as_str(), &manager_ids),
    })
}

fn policy_command(args: PolicyArgs) -> Result<()> {
    match args.command {
        PolicyCommand::Validate(args) => {
            let (policy, caller) = load_policy_and_caller(&args.policy)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&policy_output(&policy, &caller)?)?
            );
            Ok(())
        }
    }
}

// --- list ---

async fn list(manager: &str) -> Result<()> {
    let managers = client::list_managers(manager).await?;

    if managers.is_empty() {
        println!("No managers found.");
        return Ok(());
    }

    let mut builder = Builder::new();
    builder.push_record(["ID", "gRPC Address"]);
    for (id, addr) in &managers {
        builder.push_record([id.as_str(), addr.as_str()]);
    }
    display::print_table(builder);
    Ok(())
}

// --- bundle ---

fn manager_runtime_env() -> Vec<(&'static str, &'static str)> {
    vec![
        ("WRT_SECRET_ENCRYPTION_KEY", "{secret_key}"),
        ("WRT_LIFECYCLE_INSTANCE_ID", "{lifecycle_instance_id}"),
    ]
}

fn manager_systemd_environment(secret_key: &str, lifecycle_instance_id: &str) -> String {
    format!(
        "WRT_SECRET_ENCRYPTION_KEY={secret_key}\nWRT_LIFECYCLE_INSTANCE_ID={lifecycle_instance_id}\n"
    )
}

fn bundle(args: BundleArgs) -> Result<()> {
    if !Path::new(&args.manager_config).exists() {
        bail!("Manager config not found: {}", args.manager_config);
    }

    let deploy_cfg = DeployConfig::load_or_discover(args.config.as_deref())?;
    let target = deploy_config::resolve_with_default(
        &args.target,
        "x86_64-unknown-linux-gnu",
        deploy_cfg.target,
        "WR_TARGET",
    );
    let workdir = deploy_config::resolve_with_default(
        &args.workdir,
        "/opt/wruntime",
        deploy_cfg.workdir,
        "WR_WORKDIR",
    );
    let image_prefix = deploy_config::resolve_with_default(
        &args.image_prefix,
        "wr",
        deploy_cfg.image_prefix,
        "WR_IMAGE_PREFIX",
    );
    let no_otel = deploy_config::resolve_no_otel(args.no_otel, deploy_cfg.no_otel);

    let config = ManagerConfig::from_file(&args.manager_config)?;
    let source_config_dir = Path::new(&args.manager_config)
        .parent()
        .unwrap_or_else(|| Path::new("."));
    let source_policy_path = {
        let configured = Path::new(&config.authorization.policy_file);
        if configured.is_absolute() {
            configured.to_path_buf()
        } else {
            source_config_dir.join(configured)
        }
    };
    let policy_bytes = fs::read(&source_policy_path).with_context(|| {
        format!(
            "failed to read manager authorization policy {}",
            source_policy_path.display()
        )
    })?;
    ValidatedPolicy::load(&policy_bytes).context("manager authorization policy is invalid")?;
    let output = args
        .output
        .unwrap_or_else(|| "wr-manager-bundle.tar.gz".to_string());

    if !args.skip_build {
        build_helpers::build_manager_binary(&target)?;
    }

    println!("[bundle]  assembling tarball ...");

    let output_file = fs::File::create(&output)
        .with_context(|| format!("failed to create output file: {output}"))?;
    let enc = GzEncoder::new(output_file, Compression::default());
    let mut tar = tar::Builder::new(enc);
    let mut checksums: HashMap<String, String> = HashMap::new();

    // Add manager binary
    let bin_path = PathBuf::from(format!("target/{}/release/wr-manager", target));
    if !bin_path.exists() {
        bail!(
            "Binary not found: {}. Did cross-compilation succeed?",
            bin_path.display()
        );
    }
    bundle::tar_add_file(
        &mut tar,
        &mut checksums,
        "wr-manager/bin/wr-manager",
        &bin_path,
        0o755,
    )?;

    // Add template config
    let bundle_config = config.to_bundle_config(&workdir);
    bundle::tar_add_bytes_checked(
        &mut tar,
        &mut checksums,
        "wr-manager/config/manager.toml",
        bundle_config.to_toml()?.as_bytes(),
        0o644,
    )?;
    bundle::tar_add_bytes_checked(
        &mut tar,
        &mut checksums,
        "wr-manager/policy/authorization.toml",
        &policy_bytes,
        0o444,
    )?;

    // The stable launcher is the sole active selector. Rollout staging never
    // rewrites this script or the unit that invokes it.
    bundle::tar_add_bytes_checked(
        &mut tar,
        &mut checksums,
        MANAGER_LAUNCHER_ARCHIVE_PATH,
        service_gen::manager_launcher_script().as_bytes(),
        0o755,
    )?;
    bundle::tar_add_bytes_checked(
        &mut tar,
        &mut checksums,
        "wr-manager/systemd/wr-manager.service",
        service_gen::manager_activation_systemd_unit().as_bytes(),
        0o644,
    )?;

    // Docker artifacts
    helpers::extract_port(&config.listen_address)?;

    let dockerfile = DockerfileSpec {
        workdir: &workdir,
        binary: "bin/wr-manager",
        config: "config/manager.toml",
        extra_copies: vec![("policy/authorization.toml", "policy/authorization.toml")],
        env_vars: manager_runtime_env(),
        no_otel,
    };
    bundle::tar_add_bytes_checked(
        &mut tar,
        &mut checksums,
        "wr-manager/docker/Dockerfile.manager",
        dockerfile.render().as_bytes(),
        0o644,
    )?;

    let compose = manager_docker_compose(&workdir, &image_prefix);
    bundle::tar_add_bytes_checked(
        &mut tar,
        &mut checksums,
        "wr-manager/docker/docker-compose.yml",
        compose.as_bytes(),
        0o644,
    )?;

    bundle::tar_add_bytes_checked(
        &mut tar,
        &mut checksums,
        "wr-manager/docker/.dockerignore",
        b"*.tar.gz\n",
        0o644,
    )?;

    // Manifest
    let manifest = ManagerManifest {
        target: target.clone(),
        workdir: workdir.clone(),
        image_prefix: image_prefix.clone(),
        listen_address: config.listen_address.clone(),
        template_vars: vec!["db_url".to_string(), "advertise_address".to_string()],
        checksums: checksums.into_iter().collect(),
    };
    let manifest_json = serde_json::to_string_pretty(&manifest)?;
    bundle::tar_add_bytes(
        &mut tar,
        "wr-manager/manifest.json",
        manifest_json.as_bytes(),
        0o644,
    )?;

    tar.into_inner()?.finish()?;
    println!("[bundle]  wrote {output}");

    println!();
    println!("Bundle contents:");
    println!("  target:     {}", target);
    println!("  workdir:    {}", workdir);
    println!("  listen:  {}", config.listen_address);
    println!();
    println!("Deploy with:");
    println!("  wr-cli managers deploy {output} <user@host>");
    println!("  (configure via --config, wr-deploy.toml, or WR_* env vars)");
    Ok(())
}

// --- deploy ---

fn validate_manager_secret_key(secret_key: &str) -> Result<()> {
    if secret_key.len() != 64 || !secret_key.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("manager secret encryption key must contain exactly 64 hexadecimal characters");
    }
    Ok(())
}

fn resolve_manager_config_template(
    config_template: &str,
    db_url: &str,
    advertise_address: &str,
) -> Result<String> {
    let mut vars = HashMap::new();
    vars.insert("db_url", db_url);
    vars.insert("advertise_address", advertise_address);
    helpers::resolve_template(config_template, &vars)
        .context("failed to resolve template in manager.toml")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManagerDeployPhase {
    PrepareBundle,
    InstallResolvedRuntimeArtifacts,
    UploadResolvedConfig,
    ProvisionTls,
    CaptureFirstStartTimestamp,
    FirstStart,
}

const MANAGER_DEPLOY_PHASE_ORDER: [ManagerDeployPhase; 6] = [
    ManagerDeployPhase::PrepareBundle,
    ManagerDeployPhase::InstallResolvedRuntimeArtifacts,
    ManagerDeployPhase::UploadResolvedConfig,
    ManagerDeployPhase::ProvisionTls,
    ManagerDeployPhase::CaptureFirstStartTimestamp,
    ManagerDeployPhase::FirstStart,
];

fn manager_deploy_phase_order(_format: &DeployFormat) -> &'static [ManagerDeployPhase] {
    &MANAGER_DEPLOY_PHASE_ORDER
}

fn manager_secret_template_archive_paths() -> &'static [&'static str] {
    &[
        "wr-manager/systemd/wr-manager.service",
        "wr-manager/docker/Dockerfile.manager",
    ]
}

fn manager_systemd_start_command() -> &'static str {
    "sudo systemctl daemon-reload && sudo systemctl enable wr-manager.service && sudo systemctl restart wr-manager.service"
}

fn manager_docker_compose(workdir: &str, image_prefix: &str) -> String {
    service_gen::generate_compose(
        "",
        &[service_gen::ComposeService {
            name: "manager".into(),
            dockerfile: "docker/Dockerfile.manager".into(),
            context: "..".into(),
            image: Some(format!("{image_prefix}-manager")),
            network_mode: Some("host".into()),
            ports: vec![],
            volumes: vec![
                "/etc/wruntime/pki:/etc/wruntime/pki:ro".into(),
                "/var/lib/wruntime/manager-config:/var/lib/wruntime/manager-config:ro".into(),
            ],
            depends_on: vec![],
            healthcheck: service_gen::ComposeHealthcheck {
                test: vec![
                    "CMD".into(),
                    format!("{workdir}/bin/wr-manager"),
                    "--lifecycle-probe".into(),
                    format!("{workdir}/config/manager.toml"),
                ],
                interval: "2s",
                timeout: "2s",
                retries: 15,
                start_period: "30s",
            },
        }],
    )
}

const MANAGER_COMPOSE_PROJECT: &str = "wruntime-manager";

fn manager_docker_start_command(workdir: &str) -> String {
    format!(
        "cd {workdir}/wr-manager && sudo docker compose --project-name {MANAGER_COMPOSE_PROJECT} -f docker/docker-compose.yml up -d --build --force-recreate"
    )
}

fn manager_docker_logs_command(workdir: &str, tail: u32, follow: bool) -> String {
    let mut command = format!(
        "cd {workdir}/wr-manager && sudo docker compose --project-name {MANAGER_COMPOSE_PROJECT} -f docker/docker-compose.yml logs --tail {tail}"
    );
    if follow {
        command.push_str(" -f");
    }
    command
}

fn remote_socket_address(remote_ip: &str, port: u16) -> String {
    match remote_ip.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V6(_)) => format!("[{remote_ip}]:{port}"),
        _ => format!("{remote_ip}:{port}"),
    }
}

fn validate_manager_activation(
    observation: helpers::LifecycleObservation,
    expected_instance: &str,
) -> Result<helpers::LifecycleObservation> {
    let kind = observation.service_kind_enum()?;
    if kind != wr_common::wruntime::ServiceKind::Manager {
        bail!(
            "manager lifecycle endpoint reported service kind {}",
            kind.as_str_name()
        );
    }
    if observation.process_instance_id != expected_instance {
        bail!(
            "manager lifecycle endpoint reported instance {}, expected activation instance {expected_instance}",
            observation.process_instance_id
        );
    }
    Ok(observation)
}

async fn wait_for_manager_epoch_ready(
    endpoint: &str,
    tls: &wr_common::node::TlsConfig,
    expected_instance: &str,
    expected_identity: &wr_common::manager_client::EpochIdentity,
    timeout: Duration,
) -> Result<helpers::LifecycleObservation> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut epoch = client::connect_authenticated_with_tls(
        endpoint,
        tls,
        wr_common::manager_client::RetryClass::ReadOnly,
    )
    .await?;
    loop {
        match epoch
            .get_lifecycle_status(wr_common::wruntime::GetLifecycleStatusRequest {})
            .await
        {
            Ok(response) => {
                let status = response
                    .into_inner()
                    .status
                    .context("manager lifecycle response omitted status")?;
                let observation =
                    wr_common::manager_client::EpochObservation::from_lifecycle(&status)?;
                observation.require_identity(expected_identity)?;
                if observation.process_ready && observation.process_instance_id == expected_instance
                {
                    return Ok(helpers::LifecycleObservation {
                        state: status.state,
                        service_kind: status.service_kind,
                        process_instance_id: observation.process_instance_id,
                        reason: status.reason,
                        detail: status.detail,
                    });
                }
            }
            Err(error)
                if matches!(
                    error.code(),
                    tonic::Code::Cancelled
                        | tonic::Code::Unknown
                        | tonic::Code::DeadlineExceeded
                        | tonic::Code::Unavailable
                ) =>
            {
                epoch = epoch.repin().await?;
            }
            Err(error) => return Err(error.into()),
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("manager did not publish the expected authenticated READY epoch before timeout");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn combine_manager_readiness_and_tail(
    readiness: Result<helpers::LifecycleObservation>,
    tail_result: Result<()>,
) -> Result<helpers::LifecycleObservation> {
    match (readiness, tail_result) {
        (Ok(ready), Ok(())) => Ok(ready),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(tail_error)) => Err(anyhow::anyhow!(
            "live startup log tail did not shut down cleanly: {tail_error:#}"
        )),
        (Err(error), Err(tail_error)) => {
            bail!("{error:#}; live startup log tail also failed to shut down: {tail_error:#}")
        }
    }
}

async fn deploy(args: DeployArgs) -> Result<()> {
    if !Path::new(&args.bundle).exists() {
        bail!("Bundle not found: {}", args.bundle);
    }

    // Resolve args from CLI > config file > env vars > defaults
    let deploy_cfg = DeployConfig::load_or_discover(args.config.as_deref())?;
    let format = deploy_config::resolve_format(args.format, deploy_cfg.format);
    let db_url =
        deploy_config::resolve_required(args.db_url, deploy_cfg.db_url, "WR_DB_URL", "db_url")?;
    let secret_key = deploy_config::resolve_required(
        args.secret_key,
        deploy_cfg.secret_key,
        "WR_SECRET_KEY",
        "secret_key",
    )?;
    validate_manager_secret_key(&secret_key)?;
    let ssh_key = deploy_config::resolve_string(args.ssh_key, deploy_cfg.ssh_key, "WR_SSH_KEY");
    let ssh_port = deploy_config::resolve_ssh_port(args.ssh_port, deploy_cfg.ssh_port)?
        .map(helpers::DeployPort::get);
    let cert_dir = deploy_config::resolve_cert_dir(&args.cert_dir, deploy_cfg.cert_dir);

    let manifest: ManagerManifest = bundle::read_manifest(&args.bundle)?;
    verify_manager_bundle(&args.bundle, &manifest)?;

    let ssh_base = helpers::build_ssh_args(&args.remote, ssh_key.as_deref(), ssh_port);
    let remote_ip = helpers::resolve_remote_ip(&ssh_base, &args.remote)?;

    let listen_port = helpers::extract_port(&manifest.listen_address)?.get();
    let manager_addr = format!("https://{}", remote_socket_address(&remote_ip, listen_port));
    let advertise_address =
        deploy_config::resolve_string(args.advertise_address, None, "WR_ADVERTISE_ADDRESS")
            .unwrap_or_else(|| manager_addr.clone());

    let config_template = bundle::read_file_from_tarball(&args.bundle, "manager.toml")?;
    let bundled_config: ManagerConfig = toml::from_str(&config_template)
        .context("manager bundle contains an invalid manager config")?;
    let bundled_policy = ValidatedPolicy::load(
        bundle::read_file_from_tarball(&args.bundle, "wr-manager/policy/authorization.toml")?
            .as_bytes(),
    )
    .context("manager bundle contains an invalid authorization policy")?;
    let expected_epoch_identity = wr_common::manager_client::EpochIdentity {
        manager_id: bundled_config.manager_id.clone(),
        policy_generation: bundled_policy.generation,
        policy_digest: bundled_policy.digest.clone(),
    };
    let resolved = resolve_manager_config_template(&config_template, &db_url, &advertise_address)?;

    // Generate an activation identity before installing service artifacts. The
    // launched manager must report this exact token, so neither a stale process
    // nor a bind-race winner can satisfy deployment readiness.
    let expected_instance = format!("manager-deploy-{}", uuid::Uuid::new_v4());
    let deploy_tls = wr_common::node::TlsConfig {
        cert_path: format!("{cert_dir}/human-client/leaf.pem"),
        key_path: format!("{cert_dir}/human-client/key.pem"),
        ca_cert_path: format!("{cert_dir}/server-root/ca.crt"),
    };

    let mut first_start_timestamp = String::new();
    for phase in manager_deploy_phase_order(&format) {
        match phase {
            ManagerDeployPhase::PrepareBundle => match format {
                DeployFormat::Systemd => {
                    prepare_systemd(
                        &args.bundle,
                        &args.remote,
                        ssh_key.as_deref(),
                        ssh_port,
                        &manifest,
                        &ssh_base,
                    )?;
                }
                DeployFormat::Docker => {
                    prepare_docker(
                        &args.bundle,
                        &args.remote,
                        ssh_key.as_deref(),
                        ssh_port,
                        &manifest,
                        &ssh_base,
                    )?;
                }
            },
            ManagerDeployPhase::InstallResolvedRuntimeArtifacts => {
                install_resolved_manager_runtime_artifacts(&ManagerRuntimeArtifactInstall {
                    bundle: &args.bundle,
                    remote: &args.remote,
                    ssh_key: ssh_key.as_deref(),
                    ssh_port,
                    manifest: &manifest,
                    ssh_base: &ssh_base,
                    secret_key: &secret_key,
                    lifecycle_instance_id: &expected_instance,
                    format: &format,
                })?;
            }
            ManagerDeployPhase::UploadResolvedConfig => {
                // Overwrite template config with resolved version
                print!("[deploy]  writing resolved config ... ");
                let remote_path = format!("{}/wr-manager/config/manager.toml", manifest.workdir);
                helpers::scp_bytes(
                    resolved.as_bytes(),
                    &args.remote,
                    &remote_path,
                    ssh_key.as_deref(),
                    ssh_port,
                )
                .context("failed to upload resolved manager.toml")?;
                println!("OK");
            }
            ManagerDeployPhase::ProvisionTls => {
                print!("[deploy]  provisioning protected TLS profiles ... ");
                for (local, remote) in [
                    (
                        format!("{cert_dir}/server-root/ca.crt"),
                        "/etc/wruntime/pki/roots/server/ca.crt",
                    ),
                    (
                        format!("{cert_dir}/client-root/ca.crt"),
                        "/etc/wruntime/pki/roots/client/ca.crt",
                    ),
                ] {
                    helpers::install_remote_file(
                        Path::new(&local),
                        &args.remote,
                        remote,
                        ssh_key.as_deref(),
                        ssh_port,
                        0o444,
                        helpers::RemoteInstallClass::Public,
                        None,
                    )?;
                }
                for (local_name, remote_name) in [
                    ("manager-endpoint", "manager-endpoint"),
                    ("manager-client", "manager-client"),
                ] {
                    let local = PathBuf::from(format!("{cert_dir}/{local_name}"));
                    let digest = helpers::local_tree_digest(&local)?;
                    helpers::install_remote_directory(
                        &local,
                        &args.remote,
                        &format!("/etc/wruntime/pki/{remote_name}/sets/v1"),
                        ssh_key.as_deref(),
                        ssh_port,
                        &digest,
                    )?;
                }
                let policy = bundle::read_file_from_tarball(
                    &args.bundle,
                    "wr-manager/policy/authorization.toml",
                )?;
                helpers::install_remote_bytes(
                    policy.as_bytes(),
                    &args.remote,
                    &format!(
                        "/var/lib/wruntime/manager-config/{}/authorization.toml",
                        bundled_config.manager_id
                    ),
                    ssh_key.as_deref(),
                    ssh_port,
                    0o600,
                    helpers::RemoteInstallClass::Sensitive,
                    None,
                )?;
                install_initial_activation_descriptor(&InitialActivationInstall {
                    remote: &args.remote,
                    ssh_key: ssh_key.as_deref(),
                    ssh_port,
                    format: &format,
                    manifest: &manifest,
                    manager_id: &bundled_config.manager_id,
                    resolved_config: &resolved,
                    cert_dir: &cert_dir,
                })?;
                println!("OK");
            }
            ManagerDeployPhase::CaptureFirstStartTimestamp => {
                // Capture remote timestamp before first start to anchor the post-deploy log dump
                first_start_timestamp =
                    helpers::get_remote_timestamp(&ssh_base).unwrap_or_default();
            }
            ManagerDeployPhase::FirstStart => {
                match format {
                    DeployFormat::Systemd => {
                        print!("[deploy]  starting service ... ");
                        start_systemd(&ssh_base)?;
                    }
                    DeployFormat::Docker => {
                        print!("[deploy]  starting container ... ");
                        start_docker(&ssh_base, &manifest)?;
                    }
                }
                println!("OK");
            }
        }
    }

    // Readiness uses the deploy-scoped certificate directory, not global CLI TLS.
    println!("[deploy]  waiting for replacement manager to become ready...");

    let log_cmd = match format {
        DeployFormat::Systemd => {
            super::logs::build_journalctl_command(Some("wr-manager"), 20, "1m", true)
        }
        DeployFormat::Docker => manager_docker_logs_command(&manifest.workdir, 20, true),
    };
    let log_tail = match helpers::spawn_ssh_prefixed(&ssh_base, &log_cmd, "\t") {
        Ok(tail) => Some(tail),
        Err(error) => {
            eprintln!("[deploy]  could not start live log tail: {error:#}");
            None
        }
    };

    let readiness = wait_for_manager_epoch_ready(
        &manager_addr,
        &deploy_tls,
        &expected_instance,
        &expected_epoch_identity,
        Duration::from_secs(60),
    )
    .await;

    let tail_result = match log_tail {
        Some(tail) => tail.stop().await,
        None => Ok(()),
    };
    println!();

    // Dump all startup logs from the deploy window (catches fast starts the tail missed)
    if !first_start_timestamp.is_empty() {
        println!();
        println!("[deploy]  startup logs:");
        let dump_cmd = match format {
            DeployFormat::Systemd => super::logs::build_journalctl_command_absolute(
                Some("wr-manager"),
                200,
                &first_start_timestamp,
                false,
            ),
            DeployFormat::Docker => manager_docker_logs_command(&manifest.workdir, 200, false),
        };
        if let Err(error) = helpers::run_ssh_prefixed_diagnostic(&ssh_base, &dump_cmd, "\t") {
            eprintln!("[deploy]  startup log diagnostic unavailable: {error:#}");
        }
    }

    let ready = validate_manager_activation(
        combine_manager_readiness_and_tail(readiness, tail_result)?,
        &expected_instance,
    )?;
    println!(
        "[deploy]  verified replacement manager process {} READY at {} (advertised {})",
        ready.process_instance_id, manager_addr, advertise_address
    );

    Ok(())
}

fn prepare_systemd(
    bundle: &str,
    remote: &str,
    ssh_key: Option<&str>,
    ssh_port: Option<u16>,
    manifest: &ManagerManifest,
    ssh_base: &[String],
) -> Result<()> {
    let workdir = &manifest.workdir;

    print!("[deploy]  copying bundle to remote ... ");
    let remote_bundle = format!("{workdir}/.manager-bundle-{}.tar.gz", uuid::Uuid::new_v4());
    helpers::install_remote_file(
        Path::new(bundle),
        remote,
        &remote_bundle,
        ssh_key,
        ssh_port,
        0o600,
        helpers::RemoteInstallClass::Public,
        None,
    )?;
    println!("OK");

    print!("[deploy]  unpacking on remote ... ");
    let run_user = helpers::extract_remote_user(remote).unwrap_or("root");
    helpers::run_ssh(
        ssh_base,
        &format!("sudo mkdir -p {workdir} && sudo tar xzf {remote_bundle} -C {workdir} && sudo chown -R {run_user}:{run_user} {workdir}/wr-manager && sudo rm -f -- {remote_bundle}"),
    )?;
    println!("OK");

    print!("[deploy]  installing stable launcher ... ");
    let launcher = bundle::read_bytes_from_tarball(bundle, MANAGER_LAUNCHER_ARCHIVE_PATH)?;
    let expected_digest = manifest
        .checksums
        .get(MANAGER_LAUNCHER_ARCHIVE_PATH)
        .map(|digest| format!("sha256:{digest}"))
        .with_context(|| {
            format!("manager bundle manifest omitted {MANAGER_LAUNCHER_ARCHIVE_PATH}")
        })?;
    helpers::install_remote_bytes(
        &launcher,
        remote,
        MANAGER_LAUNCHER_INSTALL_PATH,
        ssh_key,
        ssh_port,
        0o555,
        helpers::RemoteInstallClass::Public,
        Some(&expected_digest),
    )?;
    println!("OK");

    Ok(())
}

fn prepare_docker(
    bundle: &str,
    remote: &str,
    ssh_key: Option<&str>,
    ssh_port: Option<u16>,
    manifest: &ManagerManifest,
    ssh_base: &[String],
) -> Result<()> {
    let workdir = &manifest.workdir;

    print!("[deploy]  copying bundle to remote ... ");
    let remote_bundle = format!("{workdir}/.manager-bundle-{}.tar.gz", uuid::Uuid::new_v4());
    helpers::install_remote_file(
        Path::new(bundle),
        remote,
        &remote_bundle,
        ssh_key,
        ssh_port,
        0o600,
        helpers::RemoteInstallClass::Public,
        None,
    )?;
    println!("OK");

    print!("[deploy]  unpacking on remote ... ");
    helpers::run_ssh(
        ssh_base,
        &format!("sudo mkdir -p {workdir} && sudo tar xzf {remote_bundle} -C {workdir} && sudo rm -f -- {remote_bundle}"),
    )?;
    println!("OK");

    Ok(())
}

struct InitialActivationInstall<'a> {
    remote: &'a str,
    ssh_key: Option<&'a str>,
    ssh_port: Option<u16>,
    format: &'a DeployFormat,
    manifest: &'a ManagerManifest,
    manager_id: &'a str,
    resolved_config: &'a str,
    cert_dir: &'a str,
}

fn install_initial_activation_descriptor(params: &InitialActivationInstall<'_>) -> Result<()> {
    let manifest = params.manifest;
    let checksum = |path: &str| -> Result<String> {
        manifest
            .checksums
            .get(path)
            .map(|digest| format!("sha256:{digest}"))
            .with_context(|| format!("manager bundle manifest omitted {path}"))
    };
    let executable_path = format!("{}/wr-manager/bin/wr-manager", manifest.workdir);
    let (backend, backend_spec_path, backend_spec_digest) = match params.format {
        DeployFormat::Systemd => (
            "systemd",
            format!("{}/wr-manager/systemd/wr-manager.service", manifest.workdir),
            checksum("wr-manager/systemd/wr-manager.service")?,
        ),
        DeployFormat::Docker => (
            "compose",
            format!("{}/wr-manager/docker/docker-compose.yml", manifest.workdir),
            checksum("wr-manager/docker/docker-compose.yml")?,
        ),
    };
    let credential_set = PathBuf::from(params.cert_dir).join("manager-endpoint");
    let credential_digest = helpers::local_tree_digest(&credential_set)?;
    let descriptor = serde_json::to_vec_pretty(&serde_json::json!({
        "schema_version": 1,
        "manager_id": params.manager_id,
        "backend": backend,
        "executable": executable_path,
        "executable_digest": checksum("wr-manager/bin/wr-manager")?,
        "backend_spec_path": backend_spec_path,
        "backend_spec_digest": backend_spec_digest,
        "config_path": format!("{}/wr-manager/config/manager.toml", manifest.workdir),
        "config_digest": format!("sha256:{:x}", Sha256::digest(params.resolved_config.as_bytes())),
        "credential_set_path": "/etc/wruntime/pki/manager-endpoint/sets/v1",
        "credential_digest": credential_digest,
    }))?;
    helpers::install_remote_bytes(
        &descriptor,
        params.remote,
        "/var/lib/wruntime/manager-activation/current-activation.json",
        params.ssh_key,
        params.ssh_port,
        0o600,
        helpers::RemoteInstallClass::Sensitive,
        None,
    )
}

struct ManagerRuntimeArtifactInstall<'a> {
    bundle: &'a str,
    remote: &'a str,
    ssh_key: Option<&'a str>,
    ssh_port: Option<u16>,
    manifest: &'a ManagerManifest,
    ssh_base: &'a [String],
    secret_key: &'a str,
    lifecycle_instance_id: &'a str,
    format: &'a DeployFormat,
}

fn install_resolved_manager_runtime_artifacts(
    params: &ManagerRuntimeArtifactInstall<'_>,
) -> Result<()> {
    print!("[deploy]  resolving secrets ... ");
    let workdir = &params.manifest.workdir;
    let run_user = helpers::extract_remote_user(params.remote)
        .unwrap_or("root")
        .to_string();
    let mut secret_vars = HashMap::new();
    secret_vars.insert("secret_key", params.secret_key);
    secret_vars.insert("lifecycle_instance_id", params.lifecycle_instance_id);
    secret_vars.insert("run_user", run_user.as_str());
    secret_vars.insert("run_group", run_user.as_str());

    for archive_path in manager_secret_template_archive_paths() {
        let template = bundle::read_file_from_tarball(params.bundle, archive_path)?;
        let resolved = helpers::resolve_template(&template, &secret_vars)
            .with_context(|| format!("failed to resolve secrets in {archive_path}"))?;
        let remote_path = format!("{workdir}/{archive_path}");
        helpers::scp_bytes(
            resolved.as_bytes(),
            params.remote,
            &remote_path,
            params.ssh_key,
            params.ssh_port,
        )?;
    }

    if matches!(params.format, DeployFormat::Systemd) {
        let runtime_environment =
            manager_systemd_environment(params.secret_key, params.lifecycle_instance_id);
        helpers::install_remote_bytes(
            runtime_environment.as_bytes(),
            params.remote,
            MANAGER_SECRET_ENV_PATH,
            params.ssh_key,
            params.ssh_port,
            0o600,
            helpers::RemoteInstallClass::Sensitive,
            None,
        )?;
        let service_path = format!("{workdir}/wr-manager/systemd/wr-manager.service");
        helpers::run_ssh(
            params.ssh_base,
            &format!("sudo cp {service_path} /etc/systemd/system/ && sudo systemctl daemon-reload"),
        )?;
    }
    println!("OK");
    Ok(())
}

fn start_systemd(ssh_base: &[String]) -> Result<()> {
    helpers::run_ssh(ssh_base, manager_systemd_start_command())
}

fn start_docker(ssh_base: &[String], manifest: &ManagerManifest) -> Result<()> {
    helpers::run_ssh(ssh_base, &manager_docker_start_command(&manifest.workdir))
}

fn verify_manager_bundle(bundle_path: &str, manifest: &ManagerManifest) -> Result<()> {
    let actual: BTreeMap<_, _> = bundle::read_payload_checksums(bundle_path)?
        .into_iter()
        .collect();
    if actual != manifest.checksums {
        let missing: Vec<_> = manifest
            .checksums
            .keys()
            .filter(|path| !actual.contains_key(*path))
            .cloned()
            .collect();
        let changed: Vec<_> = manifest
            .checksums
            .iter()
            .filter(|(path, checksum)| actual.get(*path) != Some(*checksum))
            .map(|(path, _)| path.clone())
            .collect();
        let unexpected: Vec<_> = actual
            .keys()
            .filter(|path| !manifest.checksums.contains_key(*path))
            .cloned()
            .collect();
        bail!(
            "bundle payload checksums do not match manifest (missing: {}; changed: {}; unexpected: {})",
            missing.join(", "),
            changed.join(", "),
            unexpected.join(", ")
        );
    }
    Ok(())
}

// --- status ---

fn status(args: StatusArgs) -> Result<()> {
    if !Path::new(&args.bundle).exists() {
        bail!("Bundle not found: {}", args.bundle);
    }

    let manifest: ManagerManifest = bundle::read_manifest(&args.bundle)?;
    verify_manager_bundle(&args.bundle, &manifest)?;

    println!("Bundle: {}", args.bundle);
    println!();
    println!("  target:     {}", manifest.target);
    println!("  workdir:    {}", manifest.workdir);
    println!("  listen:     {}", manifest.listen_address);
    println!();
    println!("Templates:");
    for var in &manifest.template_vars {
        let source = match var.as_str() {
            "db_url" => "--db-url flag / WR_DB_URL / wr-deploy.toml",
            "advertise_address" => "--advertise-address / WR_ADVERTISE_ADDRESS",
            _ => "unknown",
        };
        println!("  {{{var}}}  {source}");
    }
    println!();
    println!("Checksums:");
    let mut sorted: Vec<_> = manifest.checksums.iter().collect();
    sorted.sort_by_key(|(k, _)| (*k).clone());
    for (path, hash) in sorted {
        println!("  {hash:.12}  {path}");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    fn manager_test_bundle(payload: &[u8], declared_payload: &[u8]) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "wr-manager-bundle-test-{}-{}.tar.gz",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let output = fs::File::create(&path).unwrap();
        let mut tar = tar::Builder::new(GzEncoder::new(output, Compression::default()));
        bundle::tar_add_bytes(&mut tar, "wr-manager/bin/wr-manager", payload, 0o755).unwrap();
        let manifest = ManagerManifest {
            target: "x86_64-unknown-linux-gnu".into(),
            workdir: "/opt/wruntime".into(),
            image_prefix: "wr".into(),
            listen_address: "0.0.0.0:9000".into(),
            template_vars: vec!["db_url".into()],
            checksums: BTreeMap::from([(
                "wr-manager/bin/wr-manager".into(),
                format!("{:x}", Sha256::digest(declared_payload)),
            )]),
        };
        bundle::tar_add_bytes(
            &mut tar,
            "wr-manager/manifest.json",
            serde_json::to_vec_pretty(&manifest).unwrap().as_slice(),
            0o644,
        )
        .unwrap();
        tar.into_inner().unwrap().finish().unwrap();
        path
    }

    #[test]
    fn manager_activation_identity_is_installed_in_every_backend() {
        let environment = manager_runtime_env();
        assert!(environment.contains(&("WRT_LIFECYCLE_INSTANCE_ID", "{lifecycle_instance_id}")));
        let systemd_environment = manager_systemd_environment(&"a".repeat(64), "activation-id");
        assert!(systemd_environment.contains("WRT_SECRET_ENCRYPTION_KEY="));
        assert!(systemd_environment.contains("WRT_LIFECYCLE_INSTANCE_ID=activation-id\n"));
        assert!(manager_secret_template_archive_paths()
            .contains(&"wr-manager/systemd/wr-manager.service"));
        assert!(manager_secret_template_archive_paths()
            .contains(&"wr-manager/docker/Dockerfile.manager"));
    }

    #[test]
    fn manager_deploy_propagates_live_tail_failure() {
        let observation = helpers::LifecycleObservation {
            state: wr_common::wruntime::ProcessLifecycleState::Ready as i32,
            service_kind: wr_common::wruntime::ServiceKind::Manager as i32,
            process_instance_id: "manager-deploy-fixture".to_string(),
            reason: 0,
            detail: "ready".to_string(),
        };
        let error = combine_manager_readiness_and_tail(
            Ok(observation.clone()),
            Err(anyhow::anyhow!("tail exited 255")),
        )
        .unwrap_err();
        assert!(error.to_string().contains("tail exited 255"));
        assert!(error
            .to_string()
            .contains("live startup log tail did not shut down cleanly"));

        let combined = combine_manager_readiness_and_tail(
            Err(anyhow::anyhow!("readiness failed")),
            Err(anyhow::anyhow!("tail exited 255")),
        )
        .unwrap_err()
        .to_string();
        assert!(combined.contains("readiness failed"));
        assert!(combined.contains("tail exited 255"));

        validate_manager_activation(observation.clone(), "manager-deploy-fixture").unwrap();
        let wrong_kind = helpers::LifecycleObservation {
            service_kind: wr_common::wruntime::ServiceKind::Proxy as i32,
            ..observation
        };
        assert!(validate_manager_activation(wrong_kind, "manager-deploy-fixture").is_err());
    }

    #[test]
    fn manager_bundle_verification_rejects_tampered_payload() {
        let path = manager_test_bundle(b"tampered", b"original");
        let manifest: ManagerManifest = bundle::read_manifest(path.to_str().unwrap()).unwrap();
        let error = verify_manager_bundle(path.to_str().unwrap(), &manifest).unwrap_err();
        assert!(error
            .to_string()
            .contains("changed: wr-manager/bin/wr-manager"));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn manager_manifest_checksum_order_is_deterministic() {
        let manifest = ManagerManifest {
            target: "target".into(),
            workdir: "/opt/wruntime".into(),
            image_prefix: "wr".into(),
            listen_address: "0.0.0.0:9000".into(),
            template_vars: vec![],
            checksums: BTreeMap::from([("z".into(), "2".into()), ("a".into(), "1".into())]),
        };
        let json = serde_json::to_string(&manifest).unwrap();
        assert!(json.find("\"a\":\"1\"").unwrap() < json.find("\"z\":\"2\"").unwrap());
    }

    #[test]
    fn manager_deploy_resolution_sets_database_and_advertised_address() {
        assert!(validate_manager_secret_key(&"a".repeat(64)).is_ok());
        assert!(validate_manager_secret_key("not-a-secret").is_err());

        let template = r#"
listen_address = "127.0.0.1:9000"
local_proxy_address = "http://127.0.0.1:9001"

[database]
url = "{db_url}"

[cluster]
advertise_grpc_address = "{advertise_address}"
"#;
        let resolved = resolve_manager_config_template(
            template,
            "postgres://postgres@localhost/wruntime",
            "https://10.0.0.1:9000",
        )
        .unwrap();
        let value: toml::Value = toml::from_str(&resolved).unwrap();
        assert_eq!(
            value["database"]["url"].as_str(),
            Some("postgres://postgres@localhost/wruntime")
        );
        assert_eq!(
            value["cluster"]["advertise_grpc_address"].as_str(),
            Some("https://10.0.0.1:9000")
        );
    }

    fn index_of<T: PartialEq + std::fmt::Debug>(items: &[T], needle: T) -> usize {
        items
            .iter()
            .position(|item| item == &needle)
            .expect("expected item in deploy phase order")
    }

    #[test]
    fn manager_systemd_deploy_sequence_starts_after_runtime_artifacts() {
        let phases = manager_deploy_phase_order(&DeployFormat::Systemd);
        assert!(
            index_of(phases, ManagerDeployPhase::PrepareBundle)
                < index_of(phases, ManagerDeployPhase::InstallResolvedRuntimeArtifacts)
        );
        let unit = service_gen::manager_activation_systemd_unit();
        assert!(unit.contains(&format!("ExecStart={MANAGER_LAUNCHER_INSTALL_PATH}")));
        assert!(unit.contains(&format!("EnvironmentFile={MANAGER_SECRET_ENV_PATH}")));
        assert!(!unit.contains("WRT_SECRET_ENCRYPTION_KEY="));
        assert!(
            index_of(phases, ManagerDeployPhase::InstallResolvedRuntimeArtifacts)
                < index_of(phases, ManagerDeployPhase::UploadResolvedConfig)
        );
        assert!(
            index_of(phases, ManagerDeployPhase::UploadResolvedConfig)
                < index_of(phases, ManagerDeployPhase::ProvisionTls)
        );
        assert!(
            index_of(phases, ManagerDeployPhase::ProvisionTls)
                < index_of(phases, ManagerDeployPhase::CaptureFirstStartTimestamp)
        );
        assert!(
            index_of(phases, ManagerDeployPhase::CaptureFirstStartTimestamp)
                < index_of(phases, ManagerDeployPhase::FirstStart)
        );
        assert!(manager_systemd_start_command().contains("enable wr-manager.service"));
        assert!(manager_systemd_start_command().contains("restart wr-manager.service"));
        assert_eq!(phases, manager_deploy_phase_order(&DeployFormat::Systemd));
    }

    #[test]
    fn manager_docker_deploy_sequence_resolves_artifacts_before_compose_start() {
        let phases = manager_deploy_phase_order(&DeployFormat::Docker);
        assert!(
            index_of(phases, ManagerDeployPhase::PrepareBundle)
                < index_of(phases, ManagerDeployPhase::InstallResolvedRuntimeArtifacts)
        );
        assert!(
            index_of(phases, ManagerDeployPhase::InstallResolvedRuntimeArtifacts)
                < index_of(phases, ManagerDeployPhase::UploadResolvedConfig)
        );
        assert!(
            index_of(phases, ManagerDeployPhase::UploadResolvedConfig)
                < index_of(phases, ManagerDeployPhase::ProvisionTls)
        );
        assert!(
            index_of(phases, ManagerDeployPhase::ProvisionTls)
                < index_of(phases, ManagerDeployPhase::CaptureFirstStartTimestamp)
        );
        assert!(
            index_of(phases, ManagerDeployPhase::CaptureFirstStartTimestamp)
                < index_of(phases, ManagerDeployPhase::FirstStart)
        );
        assert!(manager_secret_template_archive_paths()
            .contains(&"wr-manager/systemd/wr-manager.service"));
        assert!(manager_secret_template_archive_paths()
            .contains(&"wr-manager/docker/Dockerfile.manager"));
        let command = manager_docker_start_command("/opt/wruntime");
        assert!(command.contains("docker compose"));
        assert!(command.contains("--project-name wruntime-manager"));
        assert!(command.contains("up -d --build --force-recreate"));
        assert!(!command.contains("restart"));

        let compose = manager_docker_compose("/opt/wruntime", "wr");
        assert!(compose.contains("network_mode: host"));
        assert!(compose.contains("\"/etc/wruntime/pki:/etc/wruntime/pki:ro\""));
        assert!(compose
            .contains("\"/var/lib/wruntime/manager-config:/var/lib/wruntime/manager-config:ro\""));
        assert!(!compose.contains("ports:"));

        for follow in [false, true] {
            let logs = manager_docker_logs_command("/opt/wruntime", 20, follow);
            assert!(logs.contains("sudo docker compose"));
            assert!(logs.contains("--project-name wruntime-manager"));
            assert!(logs.contains("--tail 20"));
            assert_eq!(logs.ends_with(" -f"), follow);
        }
    }
}
