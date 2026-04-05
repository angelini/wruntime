use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use sha2::{Digest, Sha256};
use tabled::builder::Builder;

use super::build_helpers;
use super::config::ManagerConfig;
use super::deploy_config::{self, DeployConfig, DeployFormat};
use super::helpers;
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
    tar_add_file(
        &mut tar,
        &mut checksums,
        "wr-manager/bin/wr-manager",
        &bin_path,
        0o755,
    )?;

    // Add template config
    let bundle_config = config.to_bundle_config();
    tar_add_bytes(
        &mut tar,
        "wr-manager/config/manager.toml",
        bundle_config.to_toml()?.as_bytes(),
        0o644,
    )?;

    // Systemd unit
    let service = generate_manager_service(&workdir, no_otel);
    tar_add_bytes(
        &mut tar,
        "wr-manager/systemd/wr-manager.service",
        service.as_bytes(),
        0o644,
    )?;

    // Docker artifacts
    let listen_port = helpers::extract_port(&config.listen_address);
    let gossip_port = helpers::extract_port(&config.cluster.gossip_listen_address);

    let dockerfile = generate_manager_dockerfile(&workdir, no_otel);
    tar_add_bytes(
        &mut tar,
        "wr-manager/docker/Dockerfile.manager",
        dockerfile.as_bytes(),
        0o644,
    )?;

    let compose = generate_manager_compose(&image_prefix, listen_port, gossip_port);
    tar_add_bytes(
        &mut tar,
        "wr-manager/docker/docker-compose.yml",
        compose.as_bytes(),
        0o644,
    )?;

    tar_add_bytes(
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
    tar_add_bytes(
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
    let ssh_port = deploy_config::resolve_ssh_port(args.ssh_port, deploy_cfg.ssh_port);

    let manifest = read_manifest_from_tarball(&args.bundle)?;

    let ssh_base = helpers::build_ssh_args(&args.remote, ssh_key.as_deref(), ssh_port);

    // Auto-derive advertise_address from remote host + listen port if not provided
    let listen_port = helpers::extract_port(&manifest.listen_address);
    let advertise_address =
        match deploy_config::resolve_string(args.advertise_address, None, "WR_ADVERTISE_ADDRESS") {
            Some(addr) => addr,
            None => {
                let ip = helpers::resolve_remote_ip(&ssh_base, &args.remote)?;
                format!("http://{ip}:{listen_port}")
            }
        };

    // Merge seed_nodes: CLI wins if non-empty, otherwise use config
    let seed_nodes = if args.seed_nodes.is_empty() {
        deploy_cfg.seed_nodes.unwrap_or_default()
    } else {
        args.seed_nodes
    };

    let config_template = read_config_from_tarball(&args.bundle)?;

    // Resolve template variables
    let mut vars = HashMap::new();
    vars.insert("db_url", db_url.as_str());
    vars.insert("advertise_address", advertise_address.as_str());
    let mut resolved = helpers::resolve_template(&config_template, &vars)
        .context("failed to resolve template in manager.toml")?;

    // Inject seed_nodes if provided (append to [cluster] section)
    if !seed_nodes.is_empty() {
        let mut config: ManagerConfig =
            toml::from_str(&resolved).context("failed to parse resolved manager config")?;
        config.cluster.seed_nodes = seed_nodes;
        resolved = config.to_toml()?;
    }

    match format {
        DeployFormat::Systemd => {
            deploy_systemd(
                &args.bundle,
                &args.remote,
                ssh_key.as_deref(),
                ssh_port,
                &manifest,
                &ssh_base,
            )?;
        }
        DeployFormat::Docker => {
            deploy_docker(
                &args.bundle,
                &args.remote,
                ssh_key.as_deref(),
                ssh_port,
                &manifest,
                &ssh_base,
            )?;
        }
    }

    // Resolve {secret_key}, {run_user}, {run_group} in systemd unit and reinstall
    print!("[deploy]  resolving secrets ... ");
    let workdir = &manifest.workdir;
    let run_user = helpers::extract_remote_user(&args.remote)
        .unwrap_or("root")
        .to_string();
    let service_template =
        read_file_from_tarball(&args.bundle, "wr-manager/systemd/wr-manager.service")?;
    let mut secret_vars = HashMap::new();
    secret_vars.insert("secret_key", secret_key.as_str());
    secret_vars.insert("run_user", run_user.as_str());
    secret_vars.insert("run_group", run_user.as_str());
    let resolved_service = helpers::resolve_template(&service_template, &secret_vars)
        .context("failed to resolve secret_key in systemd unit")?;
    let service_path = format!("{workdir}/wr-manager/systemd/wr-manager.service");
    helpers::scp_bytes(
        resolved_service.as_bytes(),
        &args.remote,
        &service_path,
        ssh_key.as_deref(),
        ssh_port,
    )?;
    if matches!(format, DeployFormat::Systemd) {
        helpers::run_ssh(
            &ssh_base,
            &format!("sudo cp {service_path} /etc/systemd/system/ && sudo systemctl daemon-reload"),
        )?;
    }
    println!("OK");

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

    // Restart service so it picks up the resolved config
    print!("[deploy]  restarting service ... ");
    match format {
        DeployFormat::Systemd => {
            helpers::run_ssh(&ssh_base, "sudo systemctl restart wr-manager.service")?;
        }
        DeployFormat::Docker => {
            helpers::run_ssh(
                &ssh_base,
                &format!(
                    "cd {}/wr-manager && sudo docker compose -f docker/docker-compose.yml restart",
                    manifest.workdir
                ),
            )?;
        }
    }
    println!("OK");

    // Use the advertise_address for polling — it's already resolved to a routable address
    let manager_addr = advertise_address.clone();
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
    let _log_tail = helpers::spawn_ssh_prefixed(&ssh_base, &log_cmd, "\t\t");

    let ready = helpers::wait_for_manager_ready(&manager_addr, Duration::from_secs(60)).await;

    drop(_log_tail);
    println!();

    if ready {
        println!("[deploy]  manager is ready at {manager_addr}");
    } else {
        println!("[deploy]  WARNING: manager did not respond within 60 seconds");
        println!("          check remote logs for errors");
    }

    Ok(())
}

fn deploy_systemd(
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

    print!("[deploy]  installing systemd unit ... ");
    helpers::run_ssh(
        ssh_base,
        &format!("sudo cp {workdir}/wr-manager/systemd/wr-manager.service /etc/systemd/system/"),
    )?;
    println!("OK");

    print!("[deploy]  starting service ... ");
    helpers::run_ssh(
        ssh_base,
        "sudo systemctl daemon-reload && sudo systemctl enable --now wr-manager.service",
    )?;
    println!("OK");

    Ok(())
}

fn deploy_docker(
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

    print!("[deploy]  starting container ... ");
    helpers::run_ssh(
        ssh_base,
        &format!(
            "cd {workdir}/wr-manager && sudo docker compose -f docker/docker-compose.yml up -d"
        ),
    )?;
    println!("OK");

    Ok(())
}

// --- status ---

fn status(args: StatusArgs) -> Result<()> {
    if !Path::new(&args.bundle).exists() {
        bail!("Bundle not found: {}", args.bundle);
    }

    let manifest = read_manifest_from_tarball(&args.bundle)?;

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

// --- Generators ---

fn generate_manager_service(workdir: &str, no_otel: bool) -> String {
    let otel_env = if no_otel {
        "Environment=OTEL_SDK_DISABLED=true\n"
    } else {
        ""
    };
    format!(
        r#"[Unit]
Description=wruntime manager
After=network.target

[Service]
Type=simple
User={{run_user}}
Group={{run_group}}
WorkingDirectory={workdir}/wr-manager
ExecStart={workdir}/wr-manager/bin/wr-manager {workdir}/wr-manager/config/manager.toml
Environment=WRT_SECRET_ENCRYPTION_KEY={{secret_key}}
{otel_env}Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
"#
    )
}

fn generate_manager_dockerfile(workdir: &str, no_otel: bool) -> String {
    let otel_env = if no_otel {
        "ENV OTEL_SDK_DISABLED=true\n"
    } else {
        ""
    };
    format!(
        r#"FROM gcr.io/distroless/cc-debian13
WORKDIR {workdir}
COPY bin/wr-manager bin/wr-manager
COPY config/manager.toml config/manager.toml
ENV WRT_SECRET_ENCRYPTION_KEY={{secret_key}}
{otel_env}ENTRYPOINT ["bin/wr-manager", "config/manager.toml"]
"#
    )
}

fn generate_manager_compose(image_prefix: &str, listen_port: u16, gossip_port: u16) -> String {
    format!(
        r#"services:
  manager:
    build:
      context: ..
      dockerfile: docker/Dockerfile.manager
    image: {image_prefix}-manager
    ports:
      - "{listen_port}:{listen_port}"
      - "{gossip_port}:{gossip_port}/udp"
    restart: on-failure
"#
    )
}

// --- Tar helpers ---

fn tar_add_file(
    tar: &mut tar::Builder<GzEncoder<fs::File>>,
    checksums: &mut HashMap<String, String>,
    archive_path: &str,
    src_path: &Path,
    mode: u32,
) -> Result<()> {
    let data =
        fs::read(src_path).with_context(|| format!("failed to read {}", src_path.display()))?;
    let mut header = tar::Header::new_gnu();
    header.set_size(data.len() as u64);
    header.set_mode(mode);
    header.set_cksum();
    tar.append_data(&mut header, archive_path, data.as_slice())?;
    let hash = format!("{:x}", Sha256::digest(&data));
    checksums.insert(archive_path.to_string(), hash);
    Ok(())
}

fn tar_add_bytes(
    tar: &mut tar::Builder<GzEncoder<fs::File>>,
    archive_path: &str,
    data: &[u8],
    mode: u32,
) -> Result<()> {
    let mut header = tar::Header::new_gnu();
    header.set_size(data.len() as u64);
    header.set_mode(mode);
    header.set_cksum();
    tar.append_data(&mut header, archive_path, data)?;
    Ok(())
}

fn read_manifest_from_tarball(path: &str) -> Result<ManagerManifest> {
    let file = fs::File::open(path).with_context(|| format!("failed to open {path}"))?;
    let decoder = GzDecoder::new(file);
    let mut archive = tar::Archive::new(decoder);

    for entry in archive.entries()? {
        let mut entry = entry?;
        let entry_path = entry.path()?.to_path_buf();
        if entry_path.ends_with("manifest.json") {
            let mut content = String::new();
            std::io::Read::read_to_string(&mut entry, &mut content)?;
            let manifest: ManagerManifest =
                serde_json::from_str(&content).context("failed to parse manifest.json")?;
            return Ok(manifest);
        }
    }

    bail!("manifest.json not found in bundle")
}

/// Read the manager config template from the bundle.
fn read_file_from_tarball(path: &str, target_file: &str) -> Result<String> {
    let file = fs::File::open(path).with_context(|| format!("failed to open {path}"))?;
    let decoder = GzDecoder::new(file);
    let mut archive = tar::Archive::new(decoder);

    for entry in archive.entries()? {
        let mut entry = entry?;
        let entry_path = entry.path()?.to_string_lossy().to_string();
        if entry_path.ends_with(target_file) {
            let mut content = String::new();
            std::io::Read::read_to_string(&mut entry, &mut content)?;
            return Ok(content);
        }
    }

    bail!("{target_file} not found in bundle")
}

fn read_config_from_tarball(path: &str) -> Result<String> {
    read_file_from_tarball(path, "manager.toml")
}
