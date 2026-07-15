use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use flate2::write::GzEncoder;
use flate2::Compression;
use tabled::builder::Builder;

use super::build_helpers;
use super::bundle;
use super::config::ManagerConfig;
use super::deploy_config::{self, DeployConfig, DeployFormat};
use super::helpers;
use super::service_gen::{self, DockerfileSpec, ServiceUnit};
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
    /// Inspect a manager bundle without deploying
    Status(StatusArgs),
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
    /// Gossip seed node addresses (repeatable, e.g. 10.0.1.11:9010)
    #[arg(long = "seed-node", value_name = "ADDR")]
    seed_nodes: Vec<String>,
    /// SSH private key path
    #[arg(long)]
    ssh_key: Option<String>,
    /// SSH port
    #[arg(long)]
    ssh_port: Option<u16>,
    /// Secret encryption key (hex-encoded, 32 bytes / 64 hex chars)
    #[arg(long)]
    secret_key: Option<String>,
    /// Local directory containing CA + manager certificates (from `wr cert`)
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
    cluster_id: String,
    template_vars: Vec<String>,
    checksums: HashMap<String, String>,
}

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
        ManagersCommand::Status(status_args) => status(status_args),
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
    let bundle_config = config.to_bundle_config();
    bundle::tar_add_bytes(
        &mut tar,
        "wr-manager/config/manager.toml",
        bundle_config.to_toml()?.as_bytes(),
        0o644,
    )?;

    // Systemd unit
    let unit = ServiceUnit {
        description: "wruntime manager",
        binary_path: &format!("{workdir}/wr-manager/bin/wr-manager"),
        config_path: &format!("{workdir}/wr-manager/config/manager.toml"),
        working_directory: &format!("{workdir}/wr-manager"),
        env_vars: vec![("WRT_SECRET_ENCRYPTION_KEY", "{secret_key}")],
        no_otel,
        after: vec![],
        requires: vec![],
    };
    bundle::tar_add_bytes(
        &mut tar,
        "wr-manager/systemd/wr-manager.service",
        unit.to_systemd().as_bytes(),
        0o644,
    )?;

    // Docker artifacts
    let listen_port = helpers::extract_port(&config.listen_address)?.get();
    let gossip_port = helpers::extract_port(&config.cluster.gossip_listen_address)?.get();

    let dockerfile = DockerfileSpec {
        workdir: &workdir,
        binary: "bin/wr-manager",
        config: "config/manager.toml",
        extra_copies: vec![],
        env_vars: vec![("WRT_SECRET_ENCRYPTION_KEY", "{secret_key}")],
        no_otel,
    };
    bundle::tar_add_bytes(
        &mut tar,
        "wr-manager/docker/Dockerfile.manager",
        dockerfile.render().as_bytes(),
        0o644,
    )?;

    let compose = service_gen::generate_compose(
        "",
        &[service_gen::ComposeService {
            name: "manager".into(),
            dockerfile: "docker/Dockerfile.manager".into(),
            context: "..".into(),
            image: Some(format!("{image_prefix}-manager")),
            ports: vec![
                format!("{listen_port}:{listen_port}"),
                format!("{gossip_port}:{gossip_port}/udp"),
            ],
            depends_on: vec![],
        }],
    );
    bundle::tar_add_bytes(
        &mut tar,
        "wr-manager/docker/docker-compose.yml",
        compose.as_bytes(),
        0o644,
    )?;

    bundle::tar_add_bytes(
        &mut tar,
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
        cluster_id: config.cluster.cluster_id.clone(),
        template_vars: vec!["db_url".to_string()],
        checksums,
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
    println!("  listen:     {}", config.listen_address);
    println!("  cluster_id: {}", config.cluster.cluster_id);
    println!();
    println!("Deploy with:");
    println!("  wr-cli managers deploy {output} <user@host>");
    println!("  (configure via --config, wr-deploy.toml, or WR_* env vars)");
    Ok(())
}

// --- deploy ---

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
    "sudo systemctl daemon-reload && sudo systemctl enable --now wr-manager.service"
}

fn manager_docker_start_command(workdir: &str) -> String {
    format!("cd {workdir}/wr-manager && sudo docker compose -f docker/docker-compose.yml up -d")
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
    let ssh_key = deploy_config::resolve_string(args.ssh_key, deploy_cfg.ssh_key, "WR_SSH_KEY");
    let ssh_port = deploy_config::resolve_ssh_port(args.ssh_port, deploy_cfg.ssh_port)?
        .map(helpers::DeployPort::get);
    let cert_dir = deploy_config::resolve_cert_dir(&args.cert_dir, deploy_cfg.cert_dir);

    let manifest: ManagerManifest = bundle::read_manifest(&args.bundle)?;

    let ssh_base = helpers::build_ssh_args(&args.remote, ssh_key.as_deref(), ssh_port);

    // Auto-derive advertise_address from remote host + listen port if not provided
    let listen_port = helpers::extract_port(&manifest.listen_address)?.get();
    let advertise_address =
        match deploy_config::resolve_string(args.advertise_address, None, "WR_ADVERTISE_ADDRESS") {
            Some(addr) => addr,
            None => {
                let ip = helpers::resolve_remote_ip(&ssh_base, &args.remote)?;
                format!("https://{ip}:{listen_port}")
            }
        };

    let config_template = bundle::read_file_from_tarball(&args.bundle, "manager.toml")?;
    let resolved = resolve_manager_config_template(&config_template, &db_url, &advertise_address)?;

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
                // Provision TLS certificates on the remote host
                print!("[deploy]  provisioning TLS certificates ... ");
                let remote_cert_dir = format!("{}/wr-manager/certs", manifest.workdir);

                let host = helpers::extract_remote_host(&args.remote);
                let ca_cert = format!("{cert_dir}/ca.crt");
                let host_cert = format!("{cert_dir}/{host}.crt");
                let host_key = format!("{cert_dir}/{host}.key");

                for (local, remote_name) in [
                    (&ca_cert, "ca.crt"),
                    (&host_cert, "manager.crt"),
                    (&host_key, "manager.key"),
                ] {
                    if !Path::new(local).exists() {
                        bail!("Certificate file not found: {local}. Run `wr cert generate {host}` first.");
                    }
                    let tmp_path = format!("/tmp/{remote_name}");
                    helpers::scp_file(local, &args.remote, &tmp_path, ssh_key.as_deref(), ssh_port)
                        .with_context(|| format!("failed to upload {local}"))?;
                    helpers::run_ssh(
                        &ssh_base,
                        &format!("sudo mkdir -p {remote_cert_dir} && sudo mv {tmp_path} {remote_cert_dir}/{remote_name}"),
                    )?;
                }
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

    // Configure mTLS so the readiness check can connect to the TLS-enabled manager.
    // Cert files are named by the SSH host alias; the cert must include the resolved
    // IP as a SAN (via `wr cert generate <host> --ip <addr>`).
    let remote_host = helpers::extract_remote_host(&args.remote);
    let remote_ip = helpers::resolve_remote_ip(&ssh_base, &args.remote)?;
    crate::client::set_tls_config(wr_common::node::TlsConfig {
        cert_path: format!("{cert_dir}/{remote_host}.crt"),
        key_path: format!("{cert_dir}/{remote_host}.key"),
        ca_cert_path: format!("{cert_dir}/ca.crt"),
    });

    // Connect to the resolved IP (the SSH alias may not be DNS-resolvable).
    // The cert must have this IP as a SAN for TLS verification to succeed.
    let manager_addr = format!("https://{remote_ip}:{listen_port}");
    println!("[deploy]  waiting for manager to become ready...");

    // Tail manager logs in the background while we wait
    let log_cmd = match format {
        DeployFormat::Systemd => {
            super::logs::build_journalctl_command(Some("wr-manager"), 20, "1m", true)
        }
        DeployFormat::Docker => {
            let compose = format!("{}/wr-manager/docker/docker-compose.yml", manifest.workdir);
            format!("docker compose -f {compose} logs --tail 20 -f")
        }
    };
    let _log_tail = helpers::spawn_ssh_prefixed(&ssh_base, &log_cmd, "\t");

    let ready = helpers::wait_for_manager_ready(&manager_addr, Duration::from_secs(60)).await;

    drop(_log_tail);
    println!();

    if ready {
        println!("[deploy]  manager is ready at {manager_addr}");
    } else {
        println!("[deploy]  WARNING: manager did not respond within 60 seconds");
        println!("          check remote logs for errors");
    }

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
            DeployFormat::Docker => {
                let compose = format!("{}/wr-manager/docker/docker-compose.yml", manifest.workdir);
                format!("docker compose -f {compose} logs --tail 200")
            }
        };
        helpers::run_ssh_prefixed_best_effort(&ssh_base, &dump_cmd, "\t");
    }

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
    helpers::scp_file(
        bundle,
        remote,
        "/tmp/wr-manager-bundle.tar.gz",
        ssh_key,
        ssh_port,
    )?;
    println!("OK");

    print!("[deploy]  unpacking on remote ... ");
    let run_user = helpers::extract_remote_user(remote).unwrap_or("root");
    helpers::run_ssh(
        ssh_base,
        &format!("sudo mkdir -p {workdir} && sudo tar xzf /tmp/wr-manager-bundle.tar.gz -C {workdir} && sudo chown -R {run_user}:{run_user} {workdir}/wr-manager && rm /tmp/wr-manager-bundle.tar.gz"),
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
    helpers::scp_file(
        bundle,
        remote,
        "/tmp/wr-manager-bundle.tar.gz",
        ssh_key,
        ssh_port,
    )?;
    println!("OK");

    print!("[deploy]  unpacking on remote ... ");
    helpers::run_ssh(
        ssh_base,
        &format!("sudo mkdir -p {workdir} && sudo tar xzf /tmp/wr-manager-bundle.tar.gz -C {workdir} && rm /tmp/wr-manager-bundle.tar.gz"),
    )?;
    println!("OK");

    Ok(())
}

struct ManagerRuntimeArtifactInstall<'a> {
    bundle: &'a str,
    remote: &'a str,
    ssh_key: Option<&'a str>,
    ssh_port: Option<u16>,
    manifest: &'a ManagerManifest,
    ssh_base: &'a [String],
    secret_key: &'a str,
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

// --- status ---

fn status(args: StatusArgs) -> Result<()> {
    if !Path::new(&args.bundle).exists() {
        bail!("Bundle not found: {}", args.bundle);
    }

    let manifest: ManagerManifest = bundle::read_manifest(&args.bundle)?;

    println!("Bundle: {}", args.bundle);
    println!();
    println!("  target:     {}", manifest.target);
    println!("  workdir:    {}", manifest.workdir);
    println!("  listen:     {}", manifest.listen_address);
    println!("  cluster_id: {}", manifest.cluster_id);
    println!();
    println!("Templates:");
    for var in &manifest.template_vars {
        let source = match var.as_str() {
            "db_url" => "--db-url flag / WR_DB_URL / wr-deploy.toml",
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

    #[test]
    fn manager_deploy_resolution_does_not_inject_seed_nodes() {
        let deploy_cfg: DeployConfig = toml::from_str(
            r#"
seed_nodes = ["10.0.0.2:9010"]
"#,
        )
        .unwrap();
        assert_eq!(
            deploy_cfg.seed_nodes.as_ref().unwrap(),
            &vec!["10.0.0.2:9010".to_string()]
        );

        let template = r#"
listen_address = "127.0.0.1:9000"
local_proxy_address = "http://127.0.0.1:9001"

[database]
url = "{db_url}"

[cluster]
cluster_id = "local"
gossip_listen_address = "127.0.0.1:9010"
advertise_grpc_address = "{advertise_address}"
"#;
        let resolved = resolve_manager_config_template(
            template,
            "postgres://postgres@localhost/wruntime",
            "https://10.0.0.1:9000",
        )
        .unwrap();
        let value: toml::Value = toml::from_str(&resolved).unwrap();
        assert!(value["cluster"].get("seed_nodes").is_none());
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
        assert!(manager_systemd_start_command().contains("enable --now wr-manager.service"));
        assert!(!manager_systemd_start_command().contains("restart"));
        let cfg: DeployConfig = toml::from_str(r#"seed_nodes = ["10.0.0.2:9010"]"#).unwrap();
        assert_eq!(cfg.seed_nodes.as_ref().unwrap().len(), 1);
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
        assert!(command.contains("up -d"));
        assert!(!command.contains("restart"));
    }
}
