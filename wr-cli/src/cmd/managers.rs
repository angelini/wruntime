use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand, ValueEnum};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use sha2::{Digest, Sha256};
use tabled::builder::Builder;

use super::build_helpers;
use super::config::ManagerConfig;
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
    /// Cargo target triple for cross-compilation
    #[arg(long)]
    target: String,
    /// Base directory for installed files on the remote host
    #[arg(long, default_value = "/opt/wruntime")]
    workdir: String,
    /// Docker image name prefix
    #[arg(long, default_value = "wr")]
    image_prefix: String,
    /// Output tarball path
    #[arg(long)]
    output: String,
    /// Skip compilation (reuse existing binary)
    #[arg(long)]
    skip_build: bool,
    /// Disable OpenTelemetry export in generated service units
    #[arg(long)]
    no_otel: bool,
}

#[derive(Clone, ValueEnum)]
pub enum DeployFormat {
    Systemd,
    Docker,
}

#[derive(Args)]
pub struct DeployArgs {
    /// Path to the manager bundle tarball
    bundle: String,
    /// Remote host in user@host format
    remote: String,
    /// Deployment format
    #[arg(long)]
    format: DeployFormat,
    /// Postgres database URL
    #[arg(long)]
    db_url: String,
    /// Gossip seed node addresses (repeatable, e.g. 10.0.1.11:9010)
    #[arg(long = "seed-node", value_name = "ADDR")]
    seed_nodes: Vec<String>,
    /// SSH private key path
    #[arg(long)]
    ssh_key: Option<String>,
    /// SSH port
    #[arg(long, default_value = "22")]
    ssh_port: u16,
    /// Secret encryption key (hex-encoded, 32 bytes / 64 hex chars)
    #[arg(long)]
    secret_key: String,
    /// Manager's externally-reachable gRPC address (e.g. http://10.0.2.2:9000)
    #[arg(long)]
    advertise_address: String,
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

    let config = ManagerConfig::from_file(&args.manager_config)?;

    if !args.skip_build {
        build_helpers::build_manager_binary(&args.target)?;
    }

    println!("[bundle]  assembling tarball ...");

    let output_file = fs::File::create(&args.output)
        .with_context(|| format!("failed to create output file: {}", args.output))?;
    let enc = GzEncoder::new(output_file, Compression::default());
    let mut tar = tar::Builder::new(enc);
    let mut checksums: HashMap<String, String> = HashMap::new();

    // Add manager binary
    let bin_path = PathBuf::from(format!("target/{}/release/wr-manager", args.target));
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
    let service = generate_manager_service(&args.workdir, args.no_otel);
    tar_add_bytes(
        &mut tar,
        "wr-manager/systemd/wr-manager.service",
        service.as_bytes(),
        0o644,
    )?;

    // Docker artifacts
    let listen_port = helpers::extract_port(&config.listen_address);
    let gossip_port = helpers::extract_port(&config.cluster.gossip_listen_address);

    let dockerfile = generate_manager_dockerfile(&args.workdir, args.no_otel);
    tar_add_bytes(
        &mut tar,
        "wr-manager/docker/Dockerfile.manager",
        dockerfile.as_bytes(),
        0o644,
    )?;

    let compose = generate_manager_compose(&args.image_prefix, listen_port, gossip_port);
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
        target: args.target.clone(),
        workdir: args.workdir.clone(),
        image_prefix: args.image_prefix.clone(),
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
    println!("[bundle]  wrote {}", args.output);

    println!();
    println!("Bundle contents:");
    println!("  target:     {}", args.target);
    println!("  workdir:    {}", args.workdir);
    println!("  listen:     {}", config.listen_address);
    println!("  cluster_id: {}", config.cluster.cluster_id);
    println!();
    println!("Deploy with:");
    println!(
        "  wr-cli managers deploy {} <user@host> --format systemd --db-url <URL>",
        args.output
    );
    Ok(())
}

// --- deploy ---

async fn deploy(args: DeployArgs) -> Result<()> {
    if !Path::new(&args.bundle).exists() {
        bail!("Bundle not found: {}", args.bundle);
    }

    let manifest = read_manifest_from_tarball(&args.bundle)?;
    let config_template = read_config_from_tarball(&args.bundle)?;

    // Resolve template variables
    let mut vars = HashMap::new();
    vars.insert("db_url", args.db_url.as_str());
    vars.insert("advertise_address", args.advertise_address.as_str());
    let mut resolved = helpers::resolve_template(&config_template, &vars)
        .context("failed to resolve template in manager.toml")?;

    // Inject seed_nodes if provided (append to [cluster] section)
    if !args.seed_nodes.is_empty() {
        // Parse the resolved config, set seed_nodes, re-serialize
        let mut config: ManagerConfig =
            toml::from_str(&resolved).context("failed to parse resolved manager config")?;
        config.cluster.seed_nodes = args.seed_nodes.clone();
        resolved = config.to_toml()?;
    }

    let ssh_base = helpers::build_ssh_args(&args.remote, args.ssh_key.as_deref(), args.ssh_port);

    match args.format {
        DeployFormat::Systemd => deploy_systemd(&args, &manifest, &ssh_base)?,
        DeployFormat::Docker => deploy_docker(&args, &manifest, &ssh_base)?,
    }

    // Resolve {secret_key} in systemd unit and reinstall
    print!("[deploy]  resolving secrets ... ");
    let workdir = &manifest.workdir;
    let service_template =
        read_file_from_tarball(&args.bundle, "wr-manager/systemd/wr-manager.service")?;
    let mut secret_vars = HashMap::new();
    secret_vars.insert("secret_key", args.secret_key.as_str());
    let resolved_service = helpers::resolve_template(&service_template, &secret_vars)
        .context("failed to resolve secret_key in systemd unit")?;
    let service_path = format!("{workdir}/wr-manager/systemd/wr-manager.service");
    helpers::scp_bytes(
        resolved_service.as_bytes(),
        &args.remote,
        &service_path,
        args.ssh_key.as_deref(),
        args.ssh_port,
    )?;
    if matches!(args.format, DeployFormat::Systemd) {
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
        args.ssh_key.as_deref(),
        args.ssh_port,
    )
    .context("failed to upload resolved manager.toml")?;
    println!("OK");

    // Restart service so it picks up the resolved config
    print!("[deploy]  restarting service ... ");
    match args.format {
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

    // Derive the manager gRPC address from the remote host + listen port
    let remote_host = helpers::extract_remote_host(&args.remote);
    let listen_port = helpers::extract_port(&manifest.listen_address);
    let manager_addr = format!("http://{remote_host}:{listen_port}");

    println!("[deploy]  waiting for manager to become ready...");
    let ready = helpers::wait_for_manager_ready(&manager_addr, Duration::from_secs(60)).await;

    if ready {
        println!("[deploy]  manager is ready at {manager_addr}");
    } else {
        println!("[deploy]  WARNING: manager did not respond within 60 seconds");
        println!("          check remote logs for errors");
    }

    Ok(())
}

fn deploy_systemd(
    args: &DeployArgs,
    manifest: &ManagerManifest,
    ssh_base: &[String],
) -> Result<()> {
    let workdir = &manifest.workdir;

    print!("[deploy]  copying bundle to remote ... ");
    scp_bundle(args)?;
    println!("OK");

    print!("[deploy]  unpacking on remote ... ");
    helpers::run_ssh(
        ssh_base,
        &format!("sudo mkdir -p {workdir} && sudo tar xzf /tmp/wr-manager-bundle.tar.gz -C {workdir} && rm /tmp/wr-manager-bundle.tar.gz"),
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

fn deploy_docker(args: &DeployArgs, manifest: &ManagerManifest, ssh_base: &[String]) -> Result<()> {
    let workdir = &manifest.workdir;

    print!("[deploy]  copying bundle to remote ... ");
    scp_bundle(args)?;
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

fn scp_bundle(args: &DeployArgs) -> Result<()> {
    helpers::scp_file(
        &args.bundle,
        &args.remote,
        "/tmp/wr-manager-bundle.tar.gz",
        args.ssh_key.as_deref(),
        args.ssh_port,
    )
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
            "db_url" => "--db-url flag",
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
