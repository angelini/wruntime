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
use super::config::EngineConfig;
use super::helpers;

#[derive(Args)]
pub struct NodeArgs {
    #[command(subcommand)]
    pub command: NodeCommand,
}

#[derive(Subcommand)]
pub enum NodeCommand {
    /// Build and package a host-agnostic deployment bundle
    Bundle(BundleArgs),
    /// Deploy a bundle to a remote host
    Deploy(DeployArgs),
    /// Inspect a bundle without deploying
    Status(StatusArgs),
}

#[derive(Args)]
pub struct BundleArgs {
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
    /// Database URL for proxy and engine routing table sync
    #[arg(long)]
    db_url: String,
    /// Guest database URL for module DB pools (required if engine uses guest databases)
    #[arg(long)]
    guest_db_url: Option<String>,
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
    template_vars: Vec<String>,
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

// --- bundle helpers ---

/// Add engine configs and their module artifacts (WASM, schemas, migrations)
/// to the tarball. Returns engine names and listen ports.
fn add_engine_artifacts(
    tar: &mut tar::Builder<GzEncoder<fs::File>>,
    checksums: &mut HashMap<String, String>,
    config_names: &mut Vec<String>,
    manifest_modules: &mut Vec<ManifestModule>,
    all_engine_configs: &[(String, EngineConfig)],
) -> Result<(Vec<String>, Vec<u16>)> {
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

        // Write the template-ized config into the bundle
        let bundle_config = config.to_bundle_config();
        tar_add_bytes(
            tar,
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

        for module in &config.modules {
            if seen_modules.contains_key(&module.name) {
                continue;
            }
            seen_modules.insert(module.name.clone(), true);

            let wasm_src = Path::new(&module.wasm_path);
            if !wasm_src.exists() {
                bail!(
                    "WASM file not found: {}. Run without --skip-build.",
                    wasm_src.display()
                );
            }
            tar_add_file(
                tar,
                checksums,
                &format!("wr-node/modules/{}.wasm", module.name),
                wasm_src,
                0o644,
            )?;

            if !module.schema_path.is_empty() {
                let schema_src = Path::new(&module.schema_path);
                if schema_src.exists() {
                    tar_add_file(
                        tar,
                        checksums,
                        &format!("wr-node/schemas/{}.binpb", module.name),
                        schema_src,
                        0o644,
                    )?;
                }
            }

            if let Some(ref mig_path) = module.migrations_path {
                let mig_dir = Path::new(mig_path);
                if mig_dir.is_dir() {
                    add_migrations_dir(tar, checksums, mig_dir, &module.name)?;
                }
            }

            manifest_modules.push(ManifestModule {
                name: module.name.clone(),
                namespace: module.namespace.clone(),
                version: module.version.clone(),
            });
        }
    }

    Ok((engine_names, engine_listen_ports))
}

/// Generate a proxy config template from the engine configs and add it to the bundle.
/// Returns the proxy listen port and control port.
fn add_proxy_config(
    tar: &mut tar::Builder<GzEncoder<fs::File>>,
    config_names: &mut Vec<String>,
    all_engine_configs: &[(String, EngineConfig)],
) -> Result<(u16, u16)> {
    // Derive proxy/control ports from the first engine's node config
    let first = &all_engine_configs[0].1;
    let (proxy_port, control_port) = if let Some(ref node) = first.node {
        (
            helpers::extract_port(&node.proxy_address),
            helpers::extract_port(&node.control_address),
        )
    } else {
        (9001u16, 9002u16)
    };

    let proxy = super::config::ProxyConfig {
        listen_address: format!("0.0.0.0:{proxy_port}"),
        control_address: Some(format!("0.0.0.0:{control_port}")),
        node: Some(super::config::ProxyNodeConfig {
            proxy_address: format!("http://{{host}}:{proxy_port}"),
        }),
        database: Some(super::config::ProxyDatabaseConfig {
            url: "{db_url}".to_string(),
        }),
        cache: Some(super::config::ProxyCacheConfig {
            routing_table_ttl_secs: 5,
        }),
    };

    tar_add_bytes(
        tar,
        "wr-node/config/proxy.toml",
        proxy.to_toml()?.as_bytes(),
        0o644,
    )?;
    config_names.push("proxy.toml".to_string());

    Ok((proxy_port, control_port))
}

struct DeployArtifactParams<'a> {
    workdir: &'a str,
    image_prefix: &'a str,
    config_names: &'a [String],
    engine_names: &'a [String],
    proxy_port: u16,
    control_port: u16,
    engine_listen_ports: &'a [u16],
}

/// Generate and add systemd units, Dockerfiles, and docker-compose.yml to the tarball.
fn add_deployment_artifacts(
    tar: &mut tar::Builder<GzEncoder<fs::File>>,
    params: &DeployArtifactParams<'_>,
) -> Result<()> {
    let DeployArtifactParams {
        workdir,
        config_names,
        engine_names,
        proxy_port,
        control_port,
        engine_listen_ports,
        ..
    } = params;
    // Systemd units
    let proxy_service = generate_proxy_service(workdir, &config_names[0]);
    tar_add_bytes(
        tar,
        "wr-node/systemd/wr-proxy.service",
        proxy_service.as_bytes(),
        0o644,
    )?;

    for (i, engine_name) in engine_names.iter().enumerate() {
        let cfg_name = &config_names[i + 1];
        let service = generate_engine_service(workdir, cfg_name, engine_name);
        tar_add_bytes(
            tar,
            &format!("wr-node/systemd/wr-engine-{engine_name}.service"),
            service.as_bytes(),
            0o644,
        )?;
    }

    // Docker artifacts
    let proxy_dockerfile = generate_proxy_dockerfile(workdir);
    tar_add_bytes(
        tar,
        "wr-node/docker/Dockerfile.proxy",
        proxy_dockerfile.as_bytes(),
        0o644,
    )?;

    for (i, engine_name) in engine_names.iter().enumerate() {
        let cfg_name = &config_names[i + 1];
        let dockerfile = generate_engine_dockerfile(workdir, cfg_name);
        tar_add_bytes(
            tar,
            &format!("wr-node/docker/Dockerfile.engine-{engine_name}"),
            dockerfile.as_bytes(),
            0o644,
        )?;
    }

    let compose = generate_docker_compose(
        params.image_prefix,
        engine_names,
        *proxy_port,
        *control_port,
        engine_listen_ports,
    );
    tar_add_bytes(
        tar,
        "wr-node/docker/docker-compose.yml",
        compose.as_bytes(),
        0o644,
    )?;

    tar_add_bytes(tar, "wr-node/docker/.dockerignore", b"*.tar.gz\n", 0o644)?;

    Ok(())
}

// --- bundle ---

fn bundle(args: BundleArgs) -> Result<()> {
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

    // Add proxy config template (generated from engine node config)
    let (proxy_port, control_port) =
        add_proxy_config(&mut tar, &mut config_names, &all_engine_configs)?;

    // Add engine configs + collect modules and artifacts
    let (engine_names, engine_listen_ports) = add_engine_artifacts(
        &mut tar,
        &mut checksums,
        &mut config_names,
        &mut manifest_modules,
        &all_engine_configs,
    )?;

    // Determine which template variables this bundle requires
    let mut template_vars = vec!["host".to_string(), "db_url".to_string()];
    let has_guest_db = all_engine_configs
        .iter()
        .any(|(_, c)| c.database.as_ref().is_some_and(|db| db.guest_url.is_some()));
    if has_guest_db {
        template_vars.push("guest_db_url".to_string());
    }

    // Generate and add deployment artifacts (systemd + docker)
    add_deployment_artifacts(
        &mut tar,
        &DeployArtifactParams {
            workdir: &args.workdir,
            image_prefix: &args.image_prefix,
            config_names: &config_names,
            engine_names: &engine_names,
            proxy_port,
            control_port,
            engine_listen_ports: &engine_listen_ports,
        },
    )?;

    // Generate manifest
    let manifest = Manifest {
        target: args.target.clone(),
        workdir: args.workdir.clone(),
        image_prefix: args.image_prefix.clone(),
        modules: manifest_modules,
        configs: config_names,
        template_vars,
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
        "  wr-cli node deploy {} <user@host> --format systemd --db-url <URL>",
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
    let configs = read_configs_from_tarball(&args.bundle)?;

    // Build template variables
    let host = helpers::extract_remote_host(&args.remote);
    let mut vars = HashMap::new();
    vars.insert("host", host);
    vars.insert("db_url", args.db_url.as_str());
    let guest_db_url_val;
    if let Some(ref url) = args.guest_db_url {
        guest_db_url_val = url.clone();
        vars.insert("guest_db_url", &guest_db_url_val);
    }

    // Resolve all config templates
    let mut resolved_configs: Vec<(String, String)> = Vec::new();
    for (name, template) in &configs {
        let resolved = helpers::resolve_template(template, &vars)
            .with_context(|| format!("failed to resolve template in {name}"))?;
        resolved_configs.push((name.clone(), resolved));
    }

    let ssh_base = helpers::build_ssh_args(&args.remote, args.ssh_key.as_deref(), args.ssh_port);

    match args.format {
        DeployFormat::Systemd => deploy_systemd(&args, &manifest, &ssh_base).await?,
        DeployFormat::Docker => deploy_docker(&args, &manifest, &ssh_base).await?,
    }

    // Overwrite template configs with resolved versions
    print!("[deploy]  writing resolved configs ... ");
    for (name, content) in &resolved_configs {
        let remote_path = format!("{}/wr-node/config/{name}", manifest.workdir);
        helpers::scp_bytes(
            content.as_bytes(),
            &args.remote,
            &remote_path,
            args.ssh_key.as_deref(),
            args.ssh_port,
        )
        .with_context(|| format!("failed to upload resolved {name}"))?;
    }
    println!("OK");

    // Restart services so they pick up the resolved configs
    print!("[deploy]  restarting services ... ");
    match args.format {
        DeployFormat::Systemd => {
            let mut service_names = vec!["wr-proxy.service".to_string()];
            for config in &manifest.configs {
                if config != "proxy.toml" {
                    let stem = config.strip_suffix(".toml").unwrap_or(config);
                    service_names.push(format!("wr-engine-{stem}.service"));
                }
            }
            let services = service_names.join(" ");
            helpers::run_ssh(&ssh_base, &format!("systemctl restart {services}"))?;
        }
        DeployFormat::Docker => {
            helpers::run_ssh(
                &ssh_base,
                &format!(
                    "cd {}/wr-node && docker compose -f docker/docker-compose.yml restart",
                    manifest.workdir
                ),
            )?;
        }
    }
    println!("OK");

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
    helpers::scp_file(
        &args.bundle,
        &args.remote,
        "/tmp/wr-bundle.tar.gz",
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
    println!("  target:       {}", manifest.target);
    println!("  workdir:      {}", manifest.workdir);
    println!("  image_prefix: {}", manifest.image_prefix);
    println!();
    println!("Modules:");
    for m in &manifest.modules {
        println!("  {}.{} v{}", m.namespace, m.name, m.version);
    }
    println!();
    println!("Templates:");
    for var in &manifest.template_vars {
        let source = match var.as_str() {
            "host" => "derived from deploy target",
            "db_url" => "--db-url flag",
            "guest_db_url" => "--guest-db-url flag",
            _ => "unknown",
        };
        println!("  {{{var}}}  {source}");
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
        r#"FROM gcr.io/distroless/cc-debian13
WORKDIR {workdir}
COPY bin/wr-proxy bin/wr-proxy
COPY config/proxy.toml config/proxy.toml
ENTRYPOINT ["bin/wr-proxy", "config/proxy.toml"]
"#
    )
}

fn generate_engine_dockerfile(workdir: &str, config_name: &str) -> String {
    format!(
        r#"FROM gcr.io/distroless/cc-debian13
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

/// Read all config files from the bundle's config/ directory.
fn read_configs_from_tarball(path: &str) -> Result<Vec<(String, String)>> {
    let file = fs::File::open(path).with_context(|| format!("failed to open {path}"))?;
    let decoder = GzDecoder::new(file);
    let mut archive = tar::Archive::new(decoder);
    let mut configs = Vec::new();

    for entry in archive.entries()? {
        let mut entry = entry?;
        let entry_path = entry.path()?.to_string_lossy().to_string();
        if entry_path.contains("/config/") && entry_path.ends_with(".toml") {
            let name = entry_path
                .rsplit('/')
                .next()
                .unwrap_or(&entry_path)
                .to_string();
            let mut content = String::new();
            std::io::Read::read_to_string(&mut entry, &mut content)?;
            configs.push((name, content));
        }
    }

    Ok(configs)
}
