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

use super::build_helpers::{self, BuildModule};
use super::config::{
    EngineConfig, NodeConfig, ProxyCacheConfig, ProxyConfig, ProxyDatabaseConfig, ProxyNodeConfig,
};
use super::helpers;

#[derive(Args)]
pub struct NodeArgs {
    #[command(subcommand)]
    pub command: NodeCommand,
}

#[derive(Subcommand)]
pub enum NodeCommand {
    /// Generate a node config directory from an engine config template
    Init(InitArgs),
    /// Build and package a universal deployment bundle
    Bundle(BundleArgs),
    /// Deploy a bundle to a remote host
    Deploy(DeployArgs),
    /// Inspect a bundle without deploying
    Status(StatusArgs),
}

#[derive(Args)]
pub struct InitArgs {
    /// Output directory for generated configs
    output_dir: String,
    /// Host address for the node
    #[arg(long)]
    host: String,
    /// Manager database URL
    #[arg(long)]
    db_url: String,
    /// Template engine config file
    #[arg(long)]
    engine_config: String,
    /// Proxy listen port
    #[arg(long, default_value = "9001")]
    proxy_port: u16,
    /// Proxy control port
    #[arg(long, default_value = "9002")]
    control_port: u16,
    /// Engine listen port
    #[arg(long, default_value = "9100")]
    engine_port: u16,
    /// Guest database URL (optional, for module DB pools)
    #[arg(long)]
    guest_db_url: Option<String>,
}

#[derive(Args)]
pub struct BundleArgs {
    /// Proxy config file
    #[arg(long)]
    proxy_config: String,
    /// Engine config files (repeatable)
    #[arg(long = "engine-config", value_name = "PATH")]
    engine_configs: Vec<String>,
    /// Cargo target triple for cross-compilation
    #[arg(long)]
    target: String,
    /// Base directory for installed files
    #[arg(long, default_value = "/opt/wruntime")]
    workdir: String,
    /// Docker image name prefix
    #[arg(long, default_value = "wr")]
    image_prefix: String,
    /// Output tarball path
    #[arg(long)]
    output: String,
    /// Skip WASM and schema compilation
    #[arg(long)]
    skip_build: bool,
}

#[derive(Clone, ValueEnum)]
pub enum DeployFormat {
    Systemd,
    Docker,
}

#[derive(Args)]
pub struct DeployArgs {
    /// Path to the bundle tarball
    bundle: String,
    /// Remote host in user@host format
    remote: String,
    /// Deployment format
    #[arg(long)]
    format: DeployFormat,
    /// SSH private key path
    #[arg(long)]
    ssh_key: Option<String>,
    /// SSH port
    #[arg(long, default_value = "22")]
    ssh_port: u16,
}

#[derive(Args)]
pub struct StatusArgs {
    /// Path to the bundle tarball
    bundle: String,
}

// --- Manifest ---

#[derive(serde::Serialize, serde::Deserialize)]
struct Manifest {
    target: String,
    workdir: String,
    image_prefix: String,
    modules: Vec<ManifestModule>,
    configs: Vec<String>,
    checksums: HashMap<String, String>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ManifestModule {
    name: String,
    namespace: String,
    version: String,
}

// --- Entry point ---

pub async fn run(args: NodeArgs, manager: Option<&str>) -> Result<()> {
    match args.command {
        NodeCommand::Init(init_args) => init(init_args),
        NodeCommand::Bundle(bundle_args) => bundle(bundle_args),
        NodeCommand::Deploy(deploy_args) => {
            let mgr = manager.ok_or_else(|| {
                anyhow::anyhow!("--manager is required for node deploy (needed for verification)")
            })?;
            deploy(deploy_args, mgr).await
        }
        NodeCommand::Status(status_args) => status(status_args),
    }
}

// --- init ---

fn init(args: InitArgs) -> Result<()> {
    let config_path = &args.engine_config;
    if !Path::new(config_path).exists() {
        bail!("Engine config template not found: {config_path}");
    }

    let template = EngineConfig::from_file(config_path)?;

    let output_dir = Path::new(&args.output_dir);
    fs::create_dir_all(output_dir)
        .with_context(|| format!("failed to create output directory: {}", args.output_dir))?;

    // Generate proxy config via serde
    let proxy = ProxyConfig {
        listen_address: format!("0.0.0.0:{}", args.proxy_port),
        control_address: Some(format!("0.0.0.0:{}", args.control_port)),
        node: Some(ProxyNodeConfig {
            proxy_address: format!("http://{}:{}", args.host, args.proxy_port),
        }),
        database: Some(ProxyDatabaseConfig {
            url: args.db_url.clone(),
        }),
        cache: Some(ProxyCacheConfig {
            routing_table_ttl_secs: 5,
        }),
    };
    let proxy_path = output_dir.join("proxy.toml");
    fs::write(&proxy_path, proxy.to_toml()?)
        .with_context(|| format!("failed to write {}", proxy_path.display()))?;
    println!("[init]  wrote {}", proxy_path.display());

    // Generate engine config: override addresses and rewrite module paths for bundle
    let mut engine = template.to_bundle_config();
    engine.listen_address = format!("0.0.0.0:{}", args.engine_port);
    engine.node = Some(NodeConfig {
        proxy_address: format!("http://{}:{}", args.host, args.proxy_port),
        control_address: format!("http://{}:{}", args.host, args.control_port),
    });

    // Override database URL, preserve guest_url logic
    if let Some(ref mut db) = engine.database {
        if let Some(ref guest_url) = args.guest_db_url {
            db.guest_url = Some(guest_url.clone());
        }
        db.url = args.db_url.clone();
    }

    // Filter out migrations_path entries where the source directory doesn't exist
    for module in &mut engine.modules {
        if let Some(ref mig_path) = template
            .modules
            .iter()
            .find(|m| m.name == module.name)
            .and_then(|m| m.migrations_path.clone())
        {
            if !Path::new(mig_path).exists() {
                module.migrations_path = None;
            }
        }
    }

    let engine_path = output_dir.join("engine.toml");
    fs::write(&engine_path, engine.to_toml()?)
        .with_context(|| format!("failed to write {}", engine_path.display()))?;
    println!("[init]  wrote {}", engine_path.display());

    println!();
    println!("Node configs generated in {}", args.output_dir);
    println!("  proxy:   {}", proxy_path.display());
    println!("  engine:  {}", engine_path.display());
    println!();
    println!("Next: review configs, then run `wr-cli node bundle`");
    Ok(())
}

// --- bundle ---

fn bundle(args: BundleArgs) -> Result<()> {
    // Validate configs exist
    if !Path::new(&args.proxy_config).exists() {
        bail!("Proxy config not found: {}", args.proxy_config);
    }
    if args.engine_configs.is_empty() {
        bail!("At least one --engine-config is required");
    }

    // Parse all engine configs
    let mut all_engine_configs: Vec<(String, EngineConfig)> = Vec::new();
    for path in &args.engine_configs {
        let config = EngineConfig::from_file(path)?;
        all_engine_configs.push((path.clone(), config));
    }

    // Collect all modules for building
    let all_modules: Vec<&super::config::ModuleConfig> = all_engine_configs
        .iter()
        .flat_map(|(_, c)| c.modules.iter())
        .collect();

    if !args.skip_build {
        let build_modules: Vec<BuildModule> = all_modules
            .iter()
            .map(|m| BuildModule {
                name: m.name.clone(),
                wasm_path: m.wasm_path.clone(),
                schema_path: m.schema_path.clone(),
            })
            .collect();

        // Step 1: Compile schemas
        build_helpers::compile_schemas(&build_modules)?;

        // Step 2: Build WASM modules
        build_helpers::build_wasm_modules(&build_modules)?;

        // Step 3: Cross-compile host binaries
        build_helpers::build_host_binaries(&args.target)?;
    }

    // Step 4: Assemble the bundle
    println!("[bundle]  assembling tarball ...");

    let output_file = fs::File::create(&args.output)
        .with_context(|| format!("failed to create output file: {}", args.output))?;
    let enc = GzEncoder::new(output_file, Compression::default());
    let mut tar = tar::Builder::new(enc);

    let mut checksums: HashMap<String, String> = HashMap::new();
    let mut manifest_modules: Vec<ManifestModule> = Vec::new();
    let mut config_names: Vec<String> = Vec::new();

    // Add host binaries
    let target_dir = format!("target/{}/release", args.target);
    for bin_name in &["wr-proxy", "wr-engine"] {
        let src = PathBuf::from(&target_dir).join(bin_name);
        if !src.exists() {
            bail!(
                "Binary not found: {}. Did cross-compilation succeed?",
                src.display()
            );
        }
        let archive_path = format!("wr-node/bin/{bin_name}");
        tar_add_file(&mut tar, &mut checksums, &archive_path, &src, 0o755)?;
    }

    // Add proxy config
    tar_add_file(
        &mut tar,
        &mut checksums,
        "wr-node/config/proxy.toml",
        Path::new(&args.proxy_config),
        0o644,
    )?;
    config_names.push("proxy.toml".to_string());

    // Add engine configs + collect modules and artifacts
    let mut seen_modules: HashMap<String, bool> = HashMap::new();
    let mut engine_names: Vec<String> = Vec::new();
    let mut engine_listen_ports: Vec<u16> = Vec::new();

    for (i, (path, config)) in all_engine_configs.iter().enumerate() {
        let config_name = if all_engine_configs.len() == 1 {
            "engine.toml".to_string()
        } else {
            let stem = Path::new(path)
                .file_stem()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_else(|| format!("engine-{}", i + 1));
            format!("{stem}.toml")
        };

        // Rewrite engine config with bundle-relative paths via serde
        let bundle_config = config.to_bundle_config();
        tar_add_bytes(
            &mut tar,
            &format!("wr-node/config/{config_name}"),
            bundle_config.to_toml()?.as_bytes(),
            0o644,
        )?;
        config_names.push(config_name.clone());

        engine_listen_ports.push(helpers::extract_port(&config.listen_address));

        let engine_name = Path::new(path)
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| format!("engine-{}", i + 1));
        engine_names.push(engine_name);

        // Add module artifacts
        for module in &config.modules {
            if seen_modules.contains_key(&module.name) {
                continue;
            }
            seen_modules.insert(module.name.clone(), true);

            // WASM module
            let wasm_src = Path::new(&module.wasm_path);
            if !wasm_src.exists() {
                bail!(
                    "WASM file not found: {}. Run without --skip-build.",
                    wasm_src.display()
                );
            }
            tar_add_file(
                &mut tar,
                &mut checksums,
                &format!("wr-node/modules/{}.wasm", module.name),
                wasm_src,
                0o644,
            )?;

            // Schema
            if !module.schema_path.is_empty() {
                let schema_src = Path::new(&module.schema_path);
                if schema_src.exists() {
                    tar_add_file(
                        &mut tar,
                        &mut checksums,
                        &format!("wr-node/schemas/{}.binpb", module.name),
                        schema_src,
                        0o644,
                    )?;
                }
            }

            // Migrations
            if let Some(ref mig_path) = module.migrations_path {
                let mig_dir = Path::new(mig_path);
                if mig_dir.is_dir() {
                    add_migrations_dir(&mut tar, &mut checksums, mig_dir, &module.name)?;
                }
            }

            manifest_modules.push(ManifestModule {
                name: module.name.clone(),
                namespace: module.namespace.clone(),
                version: module.version.clone(),
            });
        }
    }

    // Parse proxy ports for docker-compose
    let proxy_config = ProxyConfig::from_file(&args.proxy_config)?;
    let proxy_port = helpers::extract_port(&proxy_config.listen_address);
    let control_port = proxy_config
        .control_address
        .as_deref()
        .map(helpers::extract_port)
        .unwrap_or(9002);

    // Generate systemd units
    let proxy_service = generate_proxy_service(&args.workdir, &config_names[0]);
    tar_add_bytes(
        &mut tar,
        "wr-node/systemd/wr-proxy.service",
        proxy_service.as_bytes(),
        0o644,
    )?;

    for (i, engine_name) in engine_names.iter().enumerate() {
        let cfg_name = &config_names[i + 1]; // +1 because config_names[0] is proxy.toml
        let service = generate_engine_service(&args.workdir, cfg_name, engine_name);
        tar_add_bytes(
            &mut tar,
            &format!("wr-node/systemd/wr-engine-{engine_name}.service"),
            service.as_bytes(),
            0o644,
        )?;
    }

    // Generate Docker artifacts
    let proxy_dockerfile = generate_proxy_dockerfile(&args.workdir);
    tar_add_bytes(
        &mut tar,
        "wr-node/docker/Dockerfile.proxy",
        proxy_dockerfile.as_bytes(),
        0o644,
    )?;

    for (i, engine_name) in engine_names.iter().enumerate() {
        let cfg_name = &config_names[i + 1];
        let dockerfile = generate_engine_dockerfile(&args.workdir, cfg_name);
        tar_add_bytes(
            &mut tar,
            &format!("wr-node/docker/Dockerfile.engine-{engine_name}"),
            dockerfile.as_bytes(),
            0o644,
        )?;
    }

    let compose = generate_docker_compose(
        &args.image_prefix,
        &engine_names,
        proxy_port,
        control_port,
        &engine_listen_ports,
    );
    tar_add_bytes(
        &mut tar,
        "wr-node/docker/docker-compose.yml",
        compose.as_bytes(),
        0o644,
    )?;

    let dockerignore = "*.tar.gz\n";
    tar_add_bytes(
        &mut tar,
        "wr-node/docker/.dockerignore",
        dockerignore.as_bytes(),
        0o644,
    )?;

    // Generate manifest
    let manifest = Manifest {
        target: args.target.clone(),
        workdir: args.workdir.clone(),
        image_prefix: args.image_prefix.clone(),
        modules: manifest_modules,
        configs: config_names,
        checksums,
    };
    let manifest_json = serde_json::to_string_pretty(&manifest)?;
    tar_add_bytes(
        &mut tar,
        "wr-node/manifest.json",
        manifest_json.as_bytes(),
        0o644,
    )?;

    tar.into_inner()?.finish()?;
    println!("[bundle]  wrote {}", args.output);

    // Print summary
    println!();
    println!("Bundle contents:");
    println!("  target:       {}", args.target);
    println!("  workdir:      {}", args.workdir);
    println!("  image_prefix: {}", args.image_prefix);
    for m in &manifest.modules {
        println!("  module:       {}.{} v{}", m.namespace, m.name, m.version);
    }
    println!();
    println!("Deploy with:");
    println!(
        "  wr-cli node deploy {} <user@host> --format systemd",
        args.output
    );
    println!(
        "  wr-cli node deploy {} <user@host> --format docker",
        args.output
    );
    Ok(())
}

// --- deploy ---

async fn deploy(args: DeployArgs, manager: &str) -> Result<()> {
    if !Path::new(&args.bundle).exists() {
        bail!("Bundle not found: {}", args.bundle);
    }

    let manifest = read_manifest_from_tarball(&args.bundle)?;
    let ssh_base = helpers::build_ssh_args(&args.remote, args.ssh_key.as_deref(), args.ssh_port);

    match args.format {
        DeployFormat::Systemd => deploy_systemd(&args, &manifest, &ssh_base).await?,
        DeployFormat::Docker => deploy_docker(&args, &manifest, &ssh_base).await?,
    }

    // Poll for engine registration
    println!("[deploy]  waiting for engine registration...");
    let initial_count = helpers::get_engine_count(manager).await;
    let registered =
        helpers::wait_for_engine_registration(manager, initial_count, Duration::from_secs(60))
            .await;

    if registered {
        println!("[deploy]  engine registered successfully");
    } else {
        println!("[deploy]  WARNING: engine did not register within 60 seconds");
        println!("          check remote logs for errors");
    }

    Ok(())
}

async fn deploy_systemd(args: &DeployArgs, manifest: &Manifest, ssh_base: &[String]) -> Result<()> {
    let workdir = &manifest.workdir;

    // Step 1: scp tarball to remote
    print!("[deploy]  copying bundle to remote ... ");
    scp_bundle(args)?;
    println!("OK");

    // Step 2: unpack
    print!("[deploy]  unpacking on remote ... ");
    helpers::run_ssh(
        ssh_base,
        &format!("mkdir -p {workdir} && tar xzf /tmp/wr-bundle.tar.gz -C {workdir} && rm /tmp/wr-bundle.tar.gz"),
    )?;
    println!("OK");

    // Step 3: install systemd units
    print!("[deploy]  installing systemd units ... ");
    helpers::run_ssh(
        ssh_base,
        &format!("cp {workdir}/wr-node/systemd/*.service /etc/systemd/system/"),
    )?;
    println!("OK");

    // Step 4: enable and start services
    print!("[deploy]  starting services ... ");
    let mut service_names = vec!["wr-proxy.service".to_string()];
    for config in &manifest.configs {
        if config != "proxy.toml" {
            let stem = config.strip_suffix(".toml").unwrap_or(config);
            service_names.push(format!("wr-engine-{stem}.service"));
        }
    }
    let services = service_names.join(" ");
    helpers::run_ssh(
        ssh_base,
        &format!("systemctl daemon-reload && systemctl enable --now {services}"),
    )?;
    println!("OK");

    Ok(())
}

async fn deploy_docker(args: &DeployArgs, manifest: &Manifest, ssh_base: &[String]) -> Result<()> {
    let workdir = &manifest.workdir;

    // Step 1: scp tarball to remote
    print!("[deploy]  copying bundle to remote ... ");
    scp_bundle(args)?;
    println!("OK");

    // Step 2: unpack
    print!("[deploy]  unpacking on remote ... ");
    helpers::run_ssh(
        ssh_base,
        &format!("mkdir -p {workdir} && tar xzf /tmp/wr-bundle.tar.gz -C {workdir} && rm /tmp/wr-bundle.tar.gz"),
    )?;
    println!("OK");

    // Step 3: docker compose up
    print!("[deploy]  starting containers ... ");
    helpers::run_ssh(
        ssh_base,
        &format!("cd {workdir}/wr-node && docker compose -f docker/docker-compose.yml up -d"),
    )?;
    println!("OK");

    Ok(())
}

/// SCP the bundle tarball to the remote host.
fn scp_bundle(args: &DeployArgs) -> Result<()> {
    let mut scp_args = vec!["scp".to_string()];
    if let Some(ref key) = args.ssh_key {
        scp_args.extend(["-i".to_string(), key.clone()]);
    }
    scp_args.extend([
        "-P".to_string(),
        args.ssh_port.to_string(),
        args.bundle.clone(),
        format!("{}:/tmp/wr-bundle.tar.gz", args.remote),
    ]);
    helpers::run_command(&scp_args)
}

// --- status ---

fn status(args: StatusArgs) -> Result<()> {
    if !Path::new(&args.bundle).exists() {
        bail!("Bundle not found: {}", args.bundle);
    }

    let manifest = read_manifest_from_tarball(&args.bundle)?;

    println!("Bundle: {}", args.bundle);
    println!();
    println!("  target:       {}", manifest.target);
    println!("  workdir:      {}", manifest.workdir);
    println!("  image_prefix: {}", manifest.image_prefix);
    println!();
    println!("Modules:");
    for m in &manifest.modules {
        println!("  {}.{} v{}", m.namespace, m.name, m.version);
    }
    println!();
    println!("Configs:");
    for c in &manifest.configs {
        println!("  {c}");
    }
    println!();
    println!("Checksums:");
    let mut sorted_checksums: Vec<_> = manifest.checksums.iter().collect();
    sorted_checksums.sort_by_key(|(k, _)| (*k).clone());
    for (path, hash) in sorted_checksums {
        println!("  {hash:.12}  {path}");
    }

    Ok(())
}

// --- Generators (non-TOML: systemd, Dockerfile, docker-compose) ---

fn generate_proxy_service(workdir: &str, config_name: &str) -> String {
    format!(
        r#"[Unit]
Description=wruntime proxy
After=network.target

[Service]
Type=simple
WorkingDirectory={workdir}/wr-node
ExecStart={workdir}/wr-node/bin/wr-proxy {workdir}/wr-node/config/{config_name}
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
"#
    )
}

fn generate_engine_service(workdir: &str, config_name: &str, engine_name: &str) -> String {
    format!(
        r#"[Unit]
Description=wruntime engine ({engine_name})
After=network.target wr-proxy.service
Requires=wr-proxy.service

[Service]
Type=simple
WorkingDirectory={workdir}/wr-node
ExecStart={workdir}/wr-node/bin/wr-engine {workdir}/wr-node/config/{config_name}
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
"#
    )
}

fn generate_proxy_dockerfile(workdir: &str) -> String {
    format!(
        r#"FROM gcr.io/distroless/cc-debian12
WORKDIR {workdir}
COPY bin/wr-proxy bin/wr-proxy
COPY config/proxy.toml config/proxy.toml
ENTRYPOINT ["bin/wr-proxy", "config/proxy.toml"]
"#
    )
}

fn generate_engine_dockerfile(workdir: &str, config_name: &str) -> String {
    format!(
        r#"FROM gcr.io/distroless/cc-debian12
WORKDIR {workdir}
COPY bin/wr-engine bin/wr-engine
COPY config/{config_name} config/engine.toml
COPY modules/ modules/
COPY schemas/ schemas/
COPY migrations/ migrations/
ENTRYPOINT ["bin/wr-engine", "config/engine.toml"]
"#
    )
}

fn generate_docker_compose(
    _image_prefix: &str,
    engine_names: &[String],
    proxy_port: u16,
    control_port: u16,
    engine_ports: &[u16],
) -> String {
    let mut out = String::from("services:\n");

    // Proxy service
    out.push_str(&format!(
        r#"  proxy:
    build:
      context: ..
      dockerfile: docker/Dockerfile.proxy
    ports:
      - "{proxy_port}:{proxy_port}"
      - "{control_port}:{control_port}"
    restart: on-failure
"#
    ));

    // Engine services
    for (i, name) in engine_names.iter().enumerate() {
        let port = engine_ports.get(i).copied().unwrap_or(9100);
        out.push_str(&format!(
            r#"
  engine-{name}:
    build:
      context: ..
      dockerfile: docker/Dockerfile.engine-{name}
    ports:
      - "{port}:{port}"
    depends_on:
      - proxy
    restart: on-failure
"#
        ));
    }

    out
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

fn add_migrations_dir(
    tar: &mut tar::Builder<GzEncoder<fs::File>>,
    checksums: &mut HashMap<String, String>,
    mig_dir: &Path,
    module_name: &str,
) -> Result<()> {
    let entries = fs::read_dir(mig_dir)
        .with_context(|| format!("failed to read migrations dir: {}", mig_dir.display()))?;
    let mut files: Vec<_> = entries.filter_map(|e| e.ok()).collect();
    files.sort_by_key(|e| e.file_name());

    for entry in files {
        let path = entry.path();
        if path.is_file() {
            let fname = entry.file_name().to_string_lossy().to_string();
            let archive_path = format!("wr-node/migrations/{module_name}/{fname}");
            tar_add_file(tar, checksums, &archive_path, &path, 0o644)?;
        }
    }
    Ok(())
}

fn read_manifest_from_tarball(path: &str) -> Result<Manifest> {
    let file = fs::File::open(path).with_context(|| format!("failed to open {path}"))?;
    let decoder = GzDecoder::new(file);
    let mut archive = tar::Archive::new(decoder);

    for entry in archive.entries()? {
        let mut entry = entry?;
        let entry_path = entry.path()?.to_path_buf();
        if entry_path.ends_with("manifest.json") {
            let mut content = String::new();
            std::io::Read::read_to_string(&mut entry, &mut content)?;
            let manifest: Manifest =
                serde_json::from_str(&content).context("failed to parse manifest.json")?;
            return Ok(manifest);
        }
    }

    bail!("manifest.json not found in bundle")
}
