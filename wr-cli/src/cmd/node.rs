use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use flate2::write::GzEncoder;
use flate2::Compression;

use super::build_helpers::{self, BuildModule};
use super::bundle;
use super::config::EngineConfig;
use super::deploy_config::{self, DeployConfig, DeployFormat};
use super::helpers;
use super::schedules::SchedulesFile;
use super::service_gen::{self, DockerfileSpec, ServiceUnit};

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
    /// Deploy config file (default: auto-discover wr-deploy.toml in CWD)
    #[arg(long)]
    config: Option<String>,
    /// Cargo target triple for cross-compilation
    #[arg(long, default_value = "x86_64-unknown-linux-gnu", env = "WR_TARGET")]
    target: String,
    /// Base directory for installed files
    #[arg(long, default_value = "/opt/wruntime")]
    workdir: String,
    /// Docker image name prefix
    #[arg(long, default_value = "wr")]
    image_prefix: String,
    /// Output tarball path [default: wr-node-bundle.tar.gz]
    #[arg(long)]
    output: Option<String>,
    /// Skip WASM and schema compilation
    #[arg(long)]
    skip_build: bool,
    /// Disable OpenTelemetry export in generated service units
    #[arg(long)]
    no_otel: bool,
    /// mTLS peer listener port (default: 9443)
    #[arg(long)]
    peer_port: Option<u16>,
}

#[derive(Args)]
pub struct DeployArgs {
    /// Path to the bundle tarball
    bundle: String,
    /// Remote host in user@host format
    remote: String,
    /// Deploy config file (default: auto-discover wr-deploy.toml in CWD)
    #[arg(long)]
    config: Option<String>,
    /// Deployment format [default: systemd]
    #[arg(long)]
    format: Option<DeployFormat>,
    /// Database URL for proxy and engine routing table sync
    #[arg(long)]
    db_url: Option<String>,
    /// SSH private key path
    #[arg(long)]
    ssh_key: Option<String>,
    /// SSH port
    #[arg(long)]
    ssh_port: Option<u16>,
    /// Local directory containing CA + node certificates (from `wr cert`)
    #[arg(long, default_value = "./certs")]
    cert_dir: String,
    /// mTLS peer listener port (default: 9443)
    #[arg(long)]
    peer_port: Option<u16>,
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
    /// Wasmtime compatibility hash for pre-compiled `.cwasm` artifacts.
    /// Engine verifies this at startup before deserializing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    precompile_hash: Option<String>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ManifestModule {
    name: String,
    namespace: String,
    version: String,
    /// Whether this module has a protobuf schema and is registered with the manager.
    #[serde(default)]
    has_schema: bool,
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
        bundle::tar_add_bytes(
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
            bundle::tar_add_file(
                tar,
                checksums,
                &format!("wr-node/modules/{}.wasm", module.name),
                wasm_src,
                0o644,
            )?;

            // Add pre-compiled native artifact when available
            let cwasm_src = wasm_src.with_extension("cwasm");
            if cwasm_src.exists() {
                bundle::tar_add_file(
                    tar,
                    checksums,
                    &format!("wr-node/modules/{}.cwasm", module.name),
                    &cwasm_src,
                    0o644,
                )?;
            }

            if !module.schema_path.is_empty() {
                let schema_src = Path::new(&module.schema_path);
                if schema_src.exists() {
                    bundle::tar_add_file(
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
                has_schema: !module.schema_path.is_empty(),
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

    // Derive peer_port from first engine's node config if available
    let peer_port = first
        .node
        .as_ref()
        .and_then(|n| n.peer_port)
        .unwrap_or(9443);

    let proxy = super::config::ProxyConfig {
        listen_address: format!("127.0.0.1:{proxy_port}"),
        control_address: Some(format!("127.0.0.1:{control_port}")),
        node: Some(super::config::ProxyNodeConfig {
            proxy_address: format!("http://{{host}}:{proxy_port}"),
            peer_port: Some(peer_port),
            tls: Some(super::config::CliTlsConfig {
                cert_path: "certs/node.crt".to_string(),
                key_path: "certs/node.key".to_string(),
                ca_cert_path: "certs/ca.crt".to_string(),
            }),
        }),
        database: Some(super::config::ProxyDatabaseConfig {
            url: "{db_url}".to_string(),
        }),
        cache: Some(super::config::ProxyCacheConfig {
            routing_table_ttl_secs: 5,
        }),
    };

    bundle::tar_add_bytes(
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
    config_names: &'a [String],
    engine_names: &'a [String],
    proxy_port: u16,
    control_port: u16,
    peer_port: u16,
    engine_listen_ports: &'a [u16],
    no_otel: bool,
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
        peer_port,
        engine_listen_ports,
        ..
    } = params;
    let no_otel = params.no_otel;

    // Systemd units
    let proxy_unit = ServiceUnit {
        description: "wruntime proxy",
        binary_path: &format!("{workdir}/wr-node/bin/wr-proxy"),
        config_path: &format!("{workdir}/wr-node/config/{}", config_names[0]),
        working_directory: &format!("{workdir}/wr-node"),
        env_vars: vec![],
        no_otel,
        after: vec![],
        requires: vec![],
    };
    bundle::tar_add_bytes(
        tar,
        "wr-node/systemd/wr-proxy.service",
        proxy_unit.to_systemd().as_bytes(),
        0o644,
    )?;

    for (i, engine_name) in engine_names.iter().enumerate() {
        let cfg_name = &config_names[i + 1];
        let engine_unit = ServiceUnit {
            description: &format!("wruntime engine ({engine_name})"),
            binary_path: &format!("{workdir}/wr-node/bin/wr-engine"),
            config_path: &format!("{workdir}/wr-node/config/{cfg_name}"),
            working_directory: &format!("{workdir}/wr-node"),
            env_vars: vec![],
            no_otel,
            after: vec!["wr-proxy.service"],
            requires: vec!["wr-proxy.service"],
        };
        bundle::tar_add_bytes(
            tar,
            &format!("wr-node/systemd/wr-engine-{engine_name}.service"),
            engine_unit.to_systemd().as_bytes(),
            0o644,
        )?;
    }

    // Sysctl tuning for wasmtime memory pooling
    bundle::tar_add_bytes(
        tar,
        "wr-node/systemd/99-wruntime.conf",
        service_gen::sysctl_config().as_bytes(),
        0o644,
    )?;

    // Docker artifacts
    let proxy_dockerfile = DockerfileSpec {
        workdir,
        binary: "bin/wr-proxy",
        config: "config/proxy.toml",
        extra_copies: vec![],
        env_vars: vec![],
        no_otel,
    };
    bundle::tar_add_bytes(
        tar,
        "wr-node/docker/Dockerfile.proxy",
        proxy_dockerfile.render().as_bytes(),
        0o644,
    )?;

    for (i, engine_name) in engine_names.iter().enumerate() {
        let cfg_name = &config_names[i + 1];
        let engine_dockerfile = DockerfileSpec {
            workdir,
            binary: "bin/wr-engine",
            config: &format!("config/{cfg_name}"),
            extra_copies: vec![
                ("modules/", "modules/"),
                ("schemas/", "schemas/"),
                ("migrations/", "migrations/"),
            ],
            env_vars: vec![],
            no_otel,
        };
        bundle::tar_add_bytes(
            tar,
            &format!("wr-node/docker/Dockerfile.engine-{engine_name}"),
            engine_dockerfile.render().as_bytes(),
            0o644,
        )?;
    }

    let compose_header = "# Requires vm.max_map_count >= 262144 on the Docker host for wasmtime memory pooling.\n\
                          # Apply with: sysctl -w vm.max_map_count=262144\n\
                          # Persist with: echo 'vm.max_map_count = 262144' > /etc/sysctl.d/99-wruntime.conf";

    let mut compose_services = vec![service_gen::ComposeService {
        name: "proxy".into(),
        dockerfile: "docker/Dockerfile.proxy".into(),
        context: "..".into(),
        image: None,
        ports: vec![
            format!("{proxy_port}:{proxy_port}"),
            format!("{control_port}:{control_port}"),
            format!("{peer_port}:{peer_port}"),
        ],
        depends_on: vec![],
    }];

    for (i, name) in engine_names.iter().enumerate() {
        let port = engine_listen_ports.get(i).copied().unwrap_or(9100);
        compose_services.push(service_gen::ComposeService {
            name: format!("engine-{name}"),
            dockerfile: format!("docker/Dockerfile.engine-{name}"),
            context: "..".into(),
            image: None,
            ports: vec![format!("{port}:{port}")],
            depends_on: vec!["proxy".into()],
        });
    }

    let compose = service_gen::generate_compose(compose_header, &compose_services);
    bundle::tar_add_bytes(
        tar,
        "wr-node/docker/docker-compose.yml",
        compose.as_bytes(),
        0o644,
    )?;

    bundle::tar_add_bytes(tar, "wr-node/docker/.dockerignore", b"*.tar.gz\n", 0o644)?;

    Ok(())
}

// --- bundle ---

fn bundle(args: BundleArgs) -> Result<()> {
    if args.engine_configs.is_empty() {
        bail!("At least one --engine-config is required");
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
    let peer_port = deploy_config::resolve_peer_port(args.peer_port, deploy_cfg.peer_port);

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

    let mut precompile_hash: Option<String> = None;

    if !args.skip_build {
        let mut seen = std::collections::HashSet::new();
        let build_modules: Vec<BuildModule> = all_modules
            .iter()
            .filter(|m| seen.insert(m.name.clone()))
            .map(|m| BuildModule {
                name: m.name.clone(),
                wasm_path: m.wasm_path.clone(),
                schema_path: m.schema_path.clone(),
            })
            .collect();

        // Step 1: Compile schemas
        build_helpers::compile_schemas(&build_modules)?;

        // Step 2: Build WASM modules
        build_helpers::build_wasm_modules(&build_modules, true)?;

        // Step 3: Pre-compile WASM → native for target architecture
        precompile_hash = Some(build_helpers::precompile_components(
            &build_modules,
            &target,
        )?);

        // Step 4: Cross-compile host binaries
        build_helpers::build_host_binaries(&target)?;
    }

    // Step 5: Assemble the bundle
    let output = args
        .output
        .unwrap_or_else(|| "wr-node-bundle.tar.gz".to_string());
    println!("[bundle]  assembling tarball ...");

    let output_file = fs::File::create(&output)
        .with_context(|| format!("failed to create output file: {output}"))?;
    let enc = GzEncoder::new(output_file, Compression::default());
    let mut tar = tar::Builder::new(enc);

    let mut checksums: HashMap<String, String> = HashMap::new();
    let mut manifest_modules: Vec<ManifestModule> = Vec::new();
    let mut config_names: Vec<String> = Vec::new();

    // Add host binaries
    let target_dir = format!("target/{}/release", target);
    for bin_name in &["wr-proxy", "wr-engine"] {
        let src = PathBuf::from(&target_dir).join(bin_name);
        if !src.exists() {
            bail!(
                "Binary not found: {}. Did cross-compilation succeed?",
                src.display()
            );
        }
        let archive_path = format!("wr-node/bin/{bin_name}");
        bundle::tar_add_file(&mut tar, &mut checksums, &archive_path, &src, 0o755)?;
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
    let template_vars = vec![
        "host".to_string(),
        "db_url".to_string(),
        "peer_port".to_string(),
    ];

    // Generate and add deployment artifacts (systemd + docker)
    add_deployment_artifacts(
        &mut tar,
        &DeployArtifactParams {
            workdir: &workdir,
            config_names: &config_names,
            engine_names: &engine_names,
            proxy_port,
            control_port,
            peer_port,
            engine_listen_ports: &engine_listen_ports,
            no_otel,
        },
    )?;

    // Generate manifest
    let manifest = Manifest {
        target: target.clone(),
        workdir: workdir.clone(),
        image_prefix: image_prefix.clone(),
        modules: manifest_modules,
        configs: config_names,
        template_vars,
        checksums,
        precompile_hash,
    };
    let manifest_json = serde_json::to_string_pretty(&manifest)?;
    bundle::tar_add_bytes(
        &mut tar,
        "wr-node/manifest.json",
        manifest_json.as_bytes(),
        0o644,
    )?;

    tar.into_inner()?.finish()?;
    println!("[bundle]  wrote {output}");

    // Print summary
    println!();
    println!("Bundle contents:");
    println!("  target:       {target}");
    println!("  workdir:      {workdir}");
    println!("  image_prefix: {image_prefix}");
    for m in &manifest.modules {
        println!("  module:       {}.{} v{}", m.namespace, m.name, m.version);
    }
    println!();
    println!("Deploy with:");
    println!("  wr-cli node deploy {output} <user@host>");
    println!("  (configure via --config, wr-deploy.toml, or WR_* env vars)");
    Ok(())
}

// --- deploy ---

async fn deploy(args: DeployArgs, manager: &str) -> Result<()> {
    if !Path::new(&args.bundle).exists() {
        bail!("Bundle not found: {}", args.bundle);
    }

    // Resolve args from CLI > config file > env vars > defaults
    let deploy_cfg = DeployConfig::load_or_discover(args.config.as_deref())?;
    let format = deploy_config::resolve_format(args.format, deploy_cfg.format);
    let db_url =
        deploy_config::resolve_required(args.db_url, deploy_cfg.db_url, "WR_DB_URL", "db_url")?;
    let ssh_key = deploy_config::resolve_string(args.ssh_key, deploy_cfg.ssh_key, "WR_SSH_KEY");
    let ssh_port = deploy_config::resolve_ssh_port(args.ssh_port, deploy_cfg.ssh_port);
    let cert_dir = deploy_config::resolve_cert_dir(&args.cert_dir, deploy_cfg.cert_dir);
    let peer_port = deploy_config::resolve_peer_port(args.peer_port, deploy_cfg.peer_port);

    let manifest: Manifest = bundle::read_manifest(&args.bundle)?;
    let configs = bundle::read_configs_from_tarball(&args.bundle)?;

    let ssh_base = helpers::build_ssh_args(&args.remote, ssh_key.as_deref(), ssh_port);

    // Build template variables
    let host_ip = helpers::resolve_remote_ip(&ssh_base, &args.remote)?;
    let host_name = helpers::extract_remote_host(&args.remote);
    let peer_port_str = peer_port.to_string();
    let mut vars = HashMap::new();
    vars.insert("host", host_ip.as_str());
    vars.insert("db_url", db_url.as_str());
    vars.insert("peer_port", peer_port_str.as_str());

    // Resolve all config templates
    let mut resolved_configs: Vec<(String, String)> = Vec::new();
    for (name, template) in &configs {
        let resolved = helpers::resolve_template(template, &vars)
            .with_context(|| format!("failed to resolve template in {name}"))?;
        resolved_configs.push((name.clone(), resolved));
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
            )
            .await?;
        }
        DeployFormat::Docker => {
            deploy_docker(
                &args.bundle,
                &args.remote,
                ssh_key.as_deref(),
                ssh_port,
                &manifest,
                &ssh_base,
            )
            .await?;
        }
    }

    // Overwrite template configs with resolved versions
    print!("[deploy]  writing resolved configs ... ");
    for (name, content) in &resolved_configs {
        let remote_path = format!("{}/wr-node/config/{name}", manifest.workdir);
        helpers::scp_bytes(
            content.as_bytes(),
            &args.remote,
            &remote_path,
            ssh_key.as_deref(),
            ssh_port,
        )
        .with_context(|| format!("failed to upload resolved {name}"))?;
    }
    println!("OK");

    // Provision TLS certificates on the remote host
    {
        print!("[deploy]  provisioning TLS certificates ... ");
        let remote_cert_dir = format!("{}/wr-node/certs", manifest.workdir);
        let ca_cert = format!("{cert_dir}/ca.crt");
        let host_cert = format!("{cert_dir}/{host_name}.crt");
        let host_key = format!("{cert_dir}/{host_name}.key");

        for (local, remote_name) in [
            (&ca_cert, "ca.crt"),
            (&host_cert, "node.crt"),
            (&host_key, "node.key"),
        ] {
            if !Path::new(local).exists() {
                bail!("Certificate file not found: {local}. Run `wr cert generate {host_name}` first.");
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

    // Capture remote timestamp before restart to anchor the post-deploy log dump
    let restart_timestamp = helpers::get_remote_timestamp(&ssh_base).unwrap_or_default();

    // Restart services so they pick up the resolved configs
    print!("[deploy]  restarting services ... ");
    match format {
        DeployFormat::Systemd => {
            let mut service_names = vec!["wr-proxy.service".to_string()];
            for config in &manifest.configs {
                if config != "proxy.toml" {
                    let stem = config.strip_suffix(".toml").unwrap_or(config);
                    service_names.push(format!("wr-engine-{stem}.service"));
                }
            }
            let services = service_names.join(" ");
            helpers::run_ssh(&ssh_base, &format!("sudo systemctl restart {services}"))?;
        }
        DeployFormat::Docker => {
            helpers::run_ssh(
                &ssh_base,
                &format!(
                    "cd {}/wr-node && sudo docker compose -f docker/docker-compose.yml restart",
                    manifest.workdir
                ),
            )?;
        }
    }
    println!("OK");

    // Poll until engines serving all schema-bearing modules are registered.
    let expected_modules: Vec<(String, String)> = manifest
        .modules
        .iter()
        .filter(|m| m.has_schema)
        .map(|m| (m.namespace.clone(), m.name.clone()))
        .collect();
    println!("[deploy]  waiting for engine registration...");

    // Tail service logs in the background while we wait
    let log_cmd = match format {
        DeployFormat::Systemd => super::logs::build_journalctl_command(None, 20, "1m", true),
        DeployFormat::Docker => {
            super::logs::build_docker_logs_command(&manifest.workdir, None, 20, true)
        }
    };
    let _log_tail = helpers::spawn_ssh_prefixed(&ssh_base, &log_cmd, "\t");

    let registered = if expected_modules.is_empty() {
        true
    } else {
        helpers::wait_for_modules(manager, &expected_modules, Duration::from_secs(60)).await
    };

    // _log_tail dropped here, killing the background SSH process
    drop(_log_tail);
    println!();

    if registered {
        println!("[deploy]  engine registered successfully");

        // Apply schedules from config if present
        if let Some(ref schedules_path) = deploy_cfg.schedules_path {
            if std::path::Path::new(schedules_path).exists() {
                println!("[deploy]  applying schedules from {schedules_path}...");
                let content = std::fs::read_to_string(schedules_path)?;
                let schedules_file: SchedulesFile = toml::from_str(&content)?;
                super::schedules::apply_entries(manager, &schedules_file.schedule).await?;
                println!(
                    "[deploy]  {} schedule(s) applied.",
                    schedules_file.schedule.len()
                );
            } else {
                println!(
                    "[deploy]  WARNING: schedules_path '{}' not found, skipping",
                    schedules_path
                );
            }
        }
    } else {
        println!("[deploy]  WARNING: engine did not register within 60 seconds");
        println!("          check remote logs for errors");
    }

    // Dump all startup logs from the deploy window (catches fast starts the tail missed)
    if !restart_timestamp.is_empty() {
        println!();
        println!("[deploy]  startup logs:");
        let dump_cmd = match format {
            DeployFormat::Systemd => {
                super::logs::build_journalctl_command_absolute(None, 200, &restart_timestamp, false)
            }
            DeployFormat::Docker => {
                super::logs::build_docker_logs_command(&manifest.workdir, None, 200, false)
            }
        };
        helpers::run_ssh_prefixed_best_effort(&ssh_base, &dump_cmd, "\t");
    }

    Ok(())
}

async fn deploy_systemd(
    bundle: &str,
    remote: &str,
    ssh_key: Option<&str>,
    ssh_port: Option<u16>,
    manifest: &Manifest,
    ssh_base: &[String],
) -> Result<()> {
    let workdir = &manifest.workdir;

    print!("[deploy]  copying bundle to remote ... ");
    helpers::scp_file(bundle, remote, "/tmp/wr-bundle.tar.gz", ssh_key, ssh_port)?;
    println!("OK");

    print!("[deploy]  unpacking on remote ... ");
    let run_user = helpers::extract_remote_user(remote).unwrap_or("root");
    helpers::run_ssh(
        ssh_base,
        &format!("sudo mkdir -p {workdir} && sudo tar xzf /tmp/wr-bundle.tar.gz -C {workdir} && sudo chown -R {run_user}:{run_user} {workdir}/wr-node && rm /tmp/wr-bundle.tar.gz"),
    )?;
    println!("OK");

    print!("[deploy]  installing systemd units ... ");
    // Resolve {run_user}/{run_group} in service files before installing
    let service_files = bundle::list_files_from_tarball(bundle, "wr-node/systemd/", ".service")?;
    let mut user_vars = std::collections::HashMap::new();
    user_vars.insert("run_user", run_user);
    user_vars.insert("run_group", run_user);
    for (archive_path, template) in &service_files {
        let resolved = helpers::resolve_template(template, &user_vars)
            .with_context(|| format!("failed to resolve {archive_path}"))?;
        let remote_path = format!("{workdir}/{archive_path}");
        helpers::scp_bytes(resolved.as_bytes(), remote, &remote_path, ssh_key, ssh_port)?;
    }
    helpers::run_ssh(
        ssh_base,
        &format!("sudo cp {workdir}/wr-node/systemd/*.service /etc/systemd/system/"),
    )?;
    println!("OK");

    print!("[deploy]  applying sysctl tuning ... ");
    helpers::run_ssh(
        ssh_base,
        &format!("sudo cp {workdir}/wr-node/systemd/99-wruntime.conf /etc/sysctl.d/ && sudo sysctl --system > /dev/null"),
    )?;
    println!("OK");

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
        &format!("sudo systemctl daemon-reload && sudo systemctl enable --now {services}"),
    )?;
    println!("OK");

    Ok(())
}

async fn deploy_docker(
    bundle: &str,
    remote: &str,
    ssh_key: Option<&str>,
    ssh_port: Option<u16>,
    manifest: &Manifest,
    ssh_base: &[String],
) -> Result<()> {
    let workdir = &manifest.workdir;

    print!("[deploy]  copying bundle to remote ... ");
    helpers::scp_file(bundle, remote, "/tmp/wr-bundle.tar.gz", ssh_key, ssh_port)?;
    println!("OK");

    print!("[deploy]  unpacking on remote ... ");
    helpers::run_ssh(
        ssh_base,
        &format!("sudo mkdir -p {workdir} && sudo tar xzf /tmp/wr-bundle.tar.gz -C {workdir} && rm /tmp/wr-bundle.tar.gz"),
    )?;
    println!("OK");

    print!("[deploy]  starting containers ... ");
    helpers::run_ssh(
        ssh_base,
        &format!("cd {workdir}/wr-node && sudo docker compose -f docker/docker-compose.yml up -d"),
    )?;
    println!("OK");

    Ok(())
}

// --- status ---

fn status(args: StatusArgs) -> Result<()> {
    if !Path::new(&args.bundle).exists() {
        bail!("Bundle not found: {}", args.bundle);
    }

    let manifest: Manifest = bundle::read_manifest(&args.bundle)?;

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
            "db_url" => "--db-url / WR_DB_URL / wr-deploy.toml",
            "peer_port" => "--peer-port / WR_PEER_PORT / wr-deploy.toml (default: 9443)",
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
            bundle::tar_add_file(tar, checksums, &archive_path, &path, 0o644)?;
        }
    }
    Ok(())
}
