use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use flate2::write::GzEncoder;
use flate2::Compression;
use sha2::{Digest, Sha256};
use wr_common::wruntime::{
    BeginDeploymentRequest, BeginRollbackRequest, CompleteDeploymentRequest, ExpectedEngine,
    ModuleIdentity, VerifyDeploymentRequest,
};

use super::build_helpers::{self, BuildModule};
use super::bundle;
use super::config::{EngineConfig, ProxyConfig};
use super::deploy_config::{self, DeployConfig, DeployFormat};
use super::helpers;
use super::schedules::SchedulesFile;
use super::service_gen::{self, DockerfileSpec, ServiceUnit};
use crate::client;

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
    /// Activate a retained prior bundle revision as a new desired revision
    Rollback(RollbackArgs),
    /// Inspect a bundle without deploying
    InspectBundle(StatusArgs),
}

#[derive(Args)]
pub struct BundleArgs {
    /// Engine config files (repeatable)
    #[arg(long = "engine-config", value_name = "PATH")]
    engine_configs: Vec<String>,
    /// Deploy config file (default: auto-discover wr-deploy.toml in CWD)
    #[arg(long)]
    config: Option<String>,
    /// Source proxy config file to preserve proxy-specific runtime sections
    #[arg(long = "proxy-config", value_name = "PATH")]
    proxy_config: Option<String>,
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
    /// Stable operator-supplied node identity
    #[arg(long)]
    node_id: String,
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
    /// Local directory containing CA + node certificates (from `wr-cli cert`)
    #[arg(long, default_value = "./certs")]
    cert_dir: String,
    /// mTLS peer listener port (default: 9443)
    #[arg(long)]
    peer_port: Option<u16>,
}

#[derive(Args)]
pub struct RollbackArgs {
    /// Remote host in user@host format
    remote: String,
    /// Stable operator-supplied node identity
    #[arg(long)]
    node_id: String,
    /// Historical successful revision to activate (default: previous successful revision)
    #[arg(long)]
    to: Option<u64>,
    /// Deploy config file (default: auto-discover wr-deploy.toml in CWD)
    #[arg(long)]
    config: Option<String>,
    /// Base directory used by the retained deployment
    #[arg(long, default_value = "/opt/wruntime")]
    workdir: String,
    /// SSH private key path
    #[arg(long)]
    ssh_key: Option<String>,
    /// SSH port
    #[arg(long)]
    ssh_port: Option<u16>,
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
    bundle_digest: String,
    engines: Vec<ManifestEngine>,
    workdir: String,
    image_prefix: String,
    modules: Vec<ManifestModule>,
    configs: Vec<String>,
    template_vars: Vec<String>,
    checksums: BTreeMap<String, String>,
    /// Wasmtime compatibility hash for pre-compiled `.cwasm` artifacts.
    /// Engine verifies this at startup before deserializing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    precompile_hash: Option<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
struct ManifestEngine {
    engine_slot: String,
    modules: Vec<ManifestModule>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
struct ManifestModule {
    name: String,
    namespace: String,
    version: String,
    /// Whether this module has a protobuf schema and is registered with the manager.
    #[serde(default)]
    has_schema: bool,
}

fn deterministic_bundle_digest(
    target: &str,
    workdir: &str,
    image_prefix: &str,
    engines: &[ManifestEngine],
    checksums: &BTreeMap<String, String>,
    precompile_hash: &Option<String>,
) -> Result<String> {
    let checksums: Vec<_> = checksums.iter().collect();
    let mut engines = engines.to_vec();
    engines.sort_by(|left, right| left.engine_slot.cmp(&right.engine_slot));
    for engine in &mut engines {
        engine.modules.sort_by(|left, right| {
            (&left.namespace, &left.name, &left.version).cmp(&(
                &right.namespace,
                &right.name,
                &right.version,
            ))
        });
    }
    let canonical = serde_json::json!({
        "target": target,
        "workdir": workdir,
        "image_prefix": image_prefix,
        "engines": engines,
        "checksums": checksums,
        "precompile_hash": precompile_hash,
    });
    Ok(format!(
        "sha256:{:x}",
        Sha256::digest(serde_json::to_vec(&canonical)?)
    ))
}

fn verify_bundle(bundle_path: &str, manifest: &Manifest) -> Result<()> {
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
    let digest = deterministic_bundle_digest(
        &manifest.target,
        &manifest.workdir,
        &manifest.image_prefix,
        &manifest.engines,
        &manifest.checksums,
        &manifest.precompile_hash,
    )?;
    if digest != manifest.bundle_digest {
        bail!(
            "bundle digest mismatch: manifest declares {}, computed {digest}",
            manifest.bundle_digest
        );
    }
    Ok(())
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
        NodeCommand::Rollback(rollback_args) => {
            let mgr = manager
                .ok_or_else(|| anyhow::anyhow!("--manager is required for node rollback"))?;
            rollback(rollback_args, mgr).await
        }
        NodeCommand::InspectBundle(status_args) => status(status_args),
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
    let mut engine_slots = std::collections::HashSet::new();

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

        // Write the template-ized config into the bundle. Deployment values are
        // filled only after the manager allocates the activation revision.
        let mut bundle_config = config.to_bundle_config()?;
        let engine_slot = Path::new(path)
            .file_stem()
            .map(|stem| stem.to_string_lossy().to_string())
            .unwrap_or_else(|| format!("engine-{}", i + 1));
        if engine_slot.is_empty()
            || engine_slot.len() > 128
            || !engine_slot
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
            || !engine_slots.insert(engine_slot.clone())
        {
            bail!("engine config file stems must be unique URL-safe engine slots");
        }
        bundle_config.deployment = Some(super::config::DeploymentConfig {
            node_id: "{node_id}".to_string(),
            revision: "{revision}".to_string(),
            bundle_digest: "{bundle_digest}".to_string(),
            engine_slot: engine_slot.clone(),
            extra: super::config::empty_extra_fields(),
        });
        let config_template = bundle_config
            .to_toml()?
            .replace("revision = \"{revision}\"", "revision = {revision}");
        bundle::tar_add_bytes_checked(
            tar,
            checksums,
            &format!("wr-node/config/{config_name}"),
            config_template.as_bytes(),
            0o644,
        )?;
        config_names.push(config_name.clone());

        engine_listen_ports.push(helpers::extract_port(&config.listen_address)?.get());

        let engine_name = engine_slot;
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

            if let Some(schema_path) = module.schema_path.as_deref().filter(|s| !s.is_empty()) {
                let schema_src = Path::new(schema_path);
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
                has_schema: module.schema_path.as_deref().is_some_and(|s| !s.is_empty()),
            });
        }
    }

    Ok((engine_names, engine_listen_ports))
}

/// Generate a proxy config template from the engine configs and add it to the bundle.
/// Returns the proxy listen port, control port, and artifact peer port.
fn add_proxy_config(
    tar: &mut tar::Builder<GzEncoder<fs::File>>,
    checksums: &mut HashMap<String, String>,
    config_names: &mut Vec<String>,
    all_engine_configs: &[(String, EngineConfig)],
    source_proxy_config: Option<&ProxyConfig>,
    fallback_artifact_peer_port: u16,
) -> Result<(u16, u16, u16)> {
    if let Some(source) = source_proxy_config {
        let control_address = source
            .control_address
            .as_deref()
            .filter(|address| !address.is_empty())
            .ok_or_else(|| anyhow::anyhow!("--proxy-config requires control_address"))?;
        let node = source
            .node
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("--proxy-config requires node"))?;
        let proxy_port = helpers::extract_port(&source.listen_address)?.get();
        let control_port = helpers::extract_port(control_address)?.get();
        let artifact_peer_port = helpers::extract_port(&node.peer_address)?.get();
        let proxy = source.to_bundle_config()?;
        bundle::tar_add_bytes_checked(
            tar,
            checksums,
            "wr-node/config/proxy.toml",
            proxy.to_toml()?.as_bytes(),
            0o644,
        )?;
        config_names.push("proxy.toml".to_string());
        return Ok((proxy_port, control_port, artifact_peer_port));
    }

    // Derive proxy/control ports from the first engine's node config
    let first = &all_engine_configs[0].1;
    let (proxy_port, control_port) = if let Some(ref node) = first.node {
        (
            helpers::extract_port(&node.proxy_address)?.get(),
            helpers::extract_port(&node.control_address)?.get(),
        )
    } else {
        (9001u16, 9002u16)
    };

    // Derive the advertised peer listener port from the first engine config.
    let peer_port = first
        .node
        .as_ref()
        .map(|node| helpers::extract_port(&node.peer_address).map(helpers::DeployPort::get))
        .transpose()?
        .unwrap_or(fallback_artifact_peer_port);

    let proxy = ProxyConfig {
        listen_address: format!("127.0.0.1:{proxy_port}"),
        control_address: Some(format!("127.0.0.1:{control_port}")),
        node: Some(super::config::ProxyNodeConfig {
            proxy_address: format!("http://127.0.0.1:{proxy_port}"),
            control_address: format!("http://127.0.0.1:{control_port}"),
            peer_address: format!("https://127.0.0.1:{peer_port}"),
            tls: Some(super::config::CliTlsConfig {
                cert_path: "certs/node.crt".to_string(),
                key_path: "certs/node.key".to_string(),
                ca_cert_path: "certs/ca.crt".to_string(),
                extra: super::config::empty_extra_fields(),
            }),
        }),
        database: Some(super::config::ProxyDatabaseConfig {
            url: "{db_url}".to_string(),
            extra: super::config::empty_extra_fields(),
        }),
        cache: Some(super::config::ProxyCacheConfig {
            routing_table_ttl_secs: 5,
            extra: super::config::empty_extra_fields(),
        }),
        extra: super::config::empty_extra_fields(),
    };

    bundle::tar_add_bytes_checked(
        tar,
        checksums,
        "wr-node/config/proxy.toml",
        proxy.to_bundle_config()?.to_toml()?.as_bytes(),
        0o644,
    )?;
    config_names.push("proxy.toml".to_string());

    Ok((proxy_port, control_port, peer_port))
}

struct DeployArtifactParams<'a> {
    workdir: &'a str,
    config_names: &'a [String],
    engine_names: &'a [String],
    no_otel: bool,
}

/// Generate and add systemd units, Dockerfiles, and docker-compose.yml to the tarball.
fn add_deployment_artifacts(
    tar: &mut tar::Builder<GzEncoder<fs::File>>,
    checksums: &mut HashMap<String, String>,
    params: &DeployArtifactParams<'_>,
) -> Result<()> {
    let DeployArtifactParams {
        workdir,
        config_names,
        engine_names,
        ..
    } = params;
    let no_otel = params.no_otel;

    // Systemd units
    let proxy_unit = ServiceUnit {
        description: "wruntime proxy",
        binary_path: &format!("{workdir}/wr-node/current/bin/wr-proxy"),
        config_path: &format!("{workdir}/wr-node/current/config/{}", config_names[0]),
        working_directory: &format!("{workdir}/wr-node/current"),
        env_vars: vec![],
        no_otel,
        after: vec![],
        requires: vec![],
    };
    bundle::tar_add_bytes_checked(
        tar,
        checksums,
        "wr-node/systemd/wr-proxy.service",
        proxy_unit.to_systemd().as_bytes(),
        0o644,
    )?;

    for (i, engine_name) in engine_names.iter().enumerate() {
        let cfg_name = &config_names[i + 1];
        let engine_unit = ServiceUnit {
            description: &format!("wruntime engine ({engine_name})"),
            binary_path: &format!("{workdir}/wr-node/current/bin/wr-engine"),
            config_path: &format!("{workdir}/wr-node/current/config/{cfg_name}"),
            working_directory: &format!("{workdir}/wr-node/current"),
            env_vars: vec![],
            no_otel,
            after: vec!["wr-proxy.service"],
            requires: vec!["wr-proxy.service"],
        };
        bundle::tar_add_bytes_checked(
            tar,
            checksums,
            &format!("wr-node/systemd/wr-engine-{engine_name}.service"),
            engine_unit.to_systemd().as_bytes(),
            0o644,
        )?;
    }

    // Sysctl tuning for wasmtime memory pooling
    bundle::tar_add_bytes_checked(
        tar,
        checksums,
        "wr-node/systemd/99-wruntime.conf",
        service_gen::sysctl_config().as_bytes(),
        0o644,
    )?;

    // Docker artifacts
    let proxy_dockerfile = DockerfileSpec {
        workdir,
        binary: "bin/wr-proxy",
        config: "config/proxy.toml",
        extra_copies: vec![("certs/", "certs/")],
        env_vars: vec![],
        no_otel,
    };
    bundle::tar_add_bytes_checked(
        tar,
        checksums,
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
                ("certs/", "certs/"),
                ("modules/", "modules/"),
                ("schemas/", "schemas/"),
                ("migrations/", "migrations/"),
            ],
            env_vars: vec![],
            no_otel,
        };
        bundle::tar_add_bytes_checked(
            tar,
            checksums,
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
        network_mode: Some("host".into()),
        ports: vec![],
        volumes: vec![],
        depends_on: vec![],
    }];

    for name in *engine_names {
        compose_services.push(service_gen::ComposeService {
            name: format!("engine-{name}"),
            dockerfile: format!("docker/Dockerfile.engine-{name}"),
            context: "..".into(),
            image: None,
            network_mode: Some("host".into()),
            ports: vec![],
            volumes: vec![],
            depends_on: vec!["proxy".into()],
        });
    }

    let compose = service_gen::generate_compose(compose_header, &compose_services);
    bundle::tar_add_bytes_checked(
        tar,
        checksums,
        "wr-node/docker/docker-compose.yml",
        compose.as_bytes(),
        0o644,
    )?;

    bundle::tar_add_bytes_checked(
        tar,
        checksums,
        "wr-node/docker/.dockerignore",
        b"*.tar.gz\n",
        0o644,
    )?;

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
    let peer_port = deploy_config::resolve_peer_port(args.peer_port, deploy_cfg.peer_port)?.get();
    let proxy_config_path = deploy_config::resolve_string(
        args.proxy_config,
        deploy_cfg.proxy_config,
        "WR_PROXY_CONFIG",
    );
    let source_proxy_config = proxy_config_path
        .as_deref()
        .map(ProxyConfig::from_file)
        .transpose()?;

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
                schema_path: m.schema_path.clone().unwrap_or_default(),
                proto_path: None,
                cargo_dir: None,
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

    // Add proxy config template (generated from engine node config or source proxy config)
    let (proxy_port, control_port, artifact_peer_port) = add_proxy_config(
        &mut tar,
        &mut checksums,
        &mut config_names,
        &all_engine_configs,
        source_proxy_config.as_ref(),
        peer_port,
    )?;

    // Add engine configs + collect modules and artifacts
    let (engine_names, engine_listen_ports) = add_engine_artifacts(
        &mut tar,
        &mut checksums,
        &mut config_names,
        &mut manifest_modules,
        &all_engine_configs,
    )?;
    let mut host_ports = std::collections::HashSet::new();
    for port in std::iter::once(proxy_port)
        .chain(std::iter::once(control_port))
        .chain(std::iter::once(artifact_peer_port))
        .chain(engine_listen_ports.iter().copied())
    {
        if !host_ports.insert(port) {
            bail!("proxy, control, peer, and engine listener ports must be unique on a node");
        }
    }

    // Determine which template variables this bundle requires
    let template_vars = vec![
        "host".to_string(),
        "db_url".to_string(),
        "peer_port".to_string(),
        "node_id".to_string(),
        "revision".to_string(),
        "bundle_digest".to_string(),
    ];

    // Generate and add deployment artifacts (systemd + docker)
    add_deployment_artifacts(
        &mut tar,
        &mut checksums,
        &DeployArtifactParams {
            workdir: &workdir,
            config_names: &config_names,
            engine_names: &engine_names,
            no_otel,
        },
    )?;

    // Record the exact per-slot inventory; this includes modules without a
    // protobuf schema because readiness must reflect the desired engine, not
    // only manager-registered HTTP services.
    let engines: Vec<ManifestEngine> = engine_names
        .iter()
        .zip(&all_engine_configs)
        .map(|(slot, (_, config))| {
            let mut seen = std::collections::HashSet::new();
            ManifestEngine {
                engine_slot: slot.clone(),
                modules: config
                    .modules
                    .iter()
                    .filter(|module| {
                        seen.insert((&module.namespace, &module.name, &module.version))
                    })
                    .map(|module| ManifestModule {
                        name: module.name.clone(),
                        namespace: module.namespace.clone(),
                        version: module.version.clone(),
                        has_schema: module
                            .schema_path
                            .as_deref()
                            .is_some_and(|path| !path.is_empty()),
                    })
                    .collect(),
            }
        })
        .collect();
    let manifest_checksums: BTreeMap<_, _> = checksums.into_iter().collect();
    let bundle_digest = deterministic_bundle_digest(
        &target,
        &workdir,
        &image_prefix,
        &engines,
        &manifest_checksums,
        &precompile_hash,
    )?;

    // Generate manifest. The digest intentionally excludes target-specific
    // resolved values and certificates, which are not immutable bundle input.
    let manifest = Manifest {
        target: target.clone(),
        bundle_digest,
        engines,
        workdir: workdir.clone(),
        image_prefix: image_prefix.clone(),
        modules: manifest_modules,
        configs: config_names,
        template_vars,
        checksums: manifest_checksums,
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
    println!("  wr-cli node deploy --node-id <stable-node-id> {output} <user@host>");
    println!("  (configure via --config, wr-deploy.toml, or WR_* env vars)");
    Ok(())
}

// --- deploy ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeDeployPhase {
    PrepareBundle,
    UploadResolvedConfigs,
    ProvisionTls,
    CaptureFirstStartTimestamp,
    FirstStart,
}

const NODE_DEPLOY_PHASE_ORDER: [NodeDeployPhase; 5] = [
    NodeDeployPhase::PrepareBundle,
    NodeDeployPhase::UploadResolvedConfigs,
    NodeDeployPhase::ProvisionTls,
    NodeDeployPhase::CaptureFirstStartTimestamp,
    NodeDeployPhase::FirstStart,
];

fn node_deploy_phase_order(_format: &DeployFormat) -> &'static [NodeDeployPhase] {
    &NODE_DEPLOY_PHASE_ORDER
}

fn node_service_names(engine_slots: &[String]) -> Vec<String> {
    let mut service_names = vec!["wr-proxy.service".to_string()];
    service_names.extend(
        engine_slots
            .iter()
            .map(|slot| format!("wr-engine-{slot}.service")),
    );
    service_names
}

fn node_systemd_start_command(engine_slots: &[String]) -> String {
    let services = node_service_names(engine_slots).join(" ");
    format!(
        "sudo systemctl daemon-reload && sudo systemctl enable {services} && sudo systemctl restart {services} && \
         for unit in $(systemctl list-unit-files --no-legend 'wr-engine-*.service' | awk '{{print $1}}'); do \
         case ' {services} ' in *\" $unit \"*) ;; *) sudo systemctl disable --now \"$unit\"; sudo rm -f \"/etc/systemd/system/$unit\" ;; esac; done; \
         sudo systemctl daemon-reload"
    )
}

const NODE_COMPOSE_PROJECT: &str = "wruntime-node";

fn node_docker_start_command(workdir: &str) -> String {
    format!(
        "cd {workdir}/wr-node/current && sudo docker compose --project-name {NODE_COMPOSE_PROJECT} -f docker/docker-compose.yml up -d --build --force-recreate --remove-orphans"
    )
}

fn staging_release_dir(workdir: &str, revision: u64) -> String {
    format!("{workdir}/wr-node/releases/.{revision}.tmp")
}

fn release_dir(workdir: &str, revision: u64) -> String {
    format!("{workdir}/wr-node/releases/{revision}")
}

fn activate_release_command(workdir: &str, revision: u64) -> String {
    let staging = staging_release_dir(workdir, revision);
    let release = release_dir(workdir, revision);
    let root = format!("{workdir}/wr-node");
    format!(
        "test -d {staging} && test ! -e {release} && sudo mv {staging} {release} && \
         sudo rm -f {root}/.current-{revision}.tmp && sudo ln -s releases/{revision} {root}/.current-{revision}.tmp && \
         sudo mv -Tf {root}/.current-{revision}.tmp {root}/current"
    )
}

fn validate_remote_workdir(workdir: &str) -> Result<()> {
    if !workdir.starts_with('/')
        || workdir == "/"
        || !workdir
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'_' | b'.'))
    {
        bail!("workdir must be a safe absolute path");
    }
    Ok(())
}

fn deployment_failure_detail(error: &anyhow::Error) -> String {
    let detail = error.to_string();
    if detail.contains("postgres://") || detail.contains("postgresql://") {
        return "deployment stage failed; database URL omitted".to_string();
    }
    detail.chars().take(4096).collect()
}

fn deployment_attempt_token(prefix: &str) -> String {
    format!(
        "{prefix}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    )
}

fn validate_deploy_listener_ports(configs: &[(String, String)], peer_port: u16) -> Result<()> {
    let mut ports = std::collections::HashSet::from([peer_port]);
    for (name, content) in configs {
        let config: toml::Value = toml::from_str(content)
            .with_context(|| format!("failed to parse bundled config {name}"))?;
        for field in ["listen_address", "control_address"] {
            if let Some(address) = config.get(field).and_then(toml::Value::as_str) {
                let port = helpers::extract_port(address)?.get();
                if !ports.insert(port) {
                    bail!("deployed peer port conflicts with a listener in bundled config {name}");
                }
            }
        }
    }
    Ok(())
}

fn expected_engines(manifest: &Manifest) -> Vec<ExpectedEngine> {
    manifest
        .engines
        .iter()
        .map(|engine| ExpectedEngine {
            engine_slot: engine.engine_slot.clone(),
            modules: engine
                .modules
                .iter()
                .map(|module| ModuleIdentity {
                    namespace: module.namespace.clone(),
                    name: module.name.clone(),
                    version: module.version.clone(),
                })
                .collect(),
        })
        .collect()
}

async fn wait_for_deployment(
    manager: &str,
    node_id: &str,
    revision: u64,
    timeout: Duration,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let conditions = match client::connect(manager).await {
            Ok(mut client) => match client
                .verify_deployment(VerifyDeploymentRequest {
                    node_id: node_id.to_string(),
                    revision,
                })
                .await
            {
                Ok(response) => {
                    let verification = response.into_inner();
                    if verification.ready {
                        return Ok(());
                    }
                    verification
                        .conditions
                        .into_iter()
                        .map(|condition| format!("{}: {}", condition.code, condition.detail))
                        .collect()
                }
                Err(error) => vec![format!("verification RPC: {error}")],
            },
            Err(error) => vec![format!("manager connection: {error}")],
        };
        if tokio::time::Instant::now() >= deadline {
            bail!(
                "deployment {node_id}/{revision} did not become ready within {}s: {}",
                timeout.as_secs(),
                conditions.join("; ")
            );
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

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
    let ssh_port = deploy_config::resolve_ssh_port(args.ssh_port, deploy_cfg.ssh_port)?
        .map(helpers::DeployPort::get);
    let cert_dir = deploy_config::resolve_cert_dir(&args.cert_dir, deploy_cfg.cert_dir);
    let peer_port = deploy_config::resolve_peer_port(args.peer_port, deploy_cfg.peer_port)?.get();

    let manifest: Manifest = bundle::read_manifest(&args.bundle)?;
    verify_bundle(&args.bundle, &manifest)?;
    let configs = bundle::read_configs_from_tarball(&args.bundle)?;
    validate_remote_workdir(&manifest.workdir)?;
    validate_deploy_listener_ports(&configs, peer_port)?;
    let attempt_token = deployment_attempt_token("deploy");
    let deployment = client::connect(manager)
        .await?
        .begin_deployment(BeginDeploymentRequest {
            node_id: args.node_id.clone(),
            attempt_token,
            bundle_digest: manifest.bundle_digest.clone(),
            expected_engines: expected_engines(&manifest),
        })
        .await?
        .into_inner()
        .deployment
        .ok_or_else(|| anyhow::anyhow!("manager returned no deployment record"))?;
    println!(
        "[deploy]  manager assigned node '{}' revision {} ({})",
        deployment.node_id, deployment.revision, deployment.bundle_digest
    );

    let operation: Result<()> = async {
    let ssh_base = helpers::build_ssh_args(&args.remote, ssh_key.as_deref(), ssh_port);

    // Build template variables
    let host_ip = helpers::resolve_remote_ip(&ssh_base, &args.remote)?;
    let host_name = helpers::extract_remote_host(&args.remote);
    let peer_port_str = peer_port.to_string();
    let mut vars = HashMap::new();
    vars.insert("host", host_ip.as_str());
    vars.insert("db_url", db_url.as_str());
    vars.insert("peer_port", peer_port_str.as_str());
    let revision = deployment.revision.to_string();
    vars.insert("node_id", deployment.node_id.as_str());
    vars.insert("revision", revision.as_str());
    vars.insert("bundle_digest", deployment.bundle_digest.as_str());

    // Resolve all config templates
    let mut resolved_configs: Vec<(String, String)> = Vec::new();
    for (name, template) in &configs {
        let resolved = helpers::resolve_template(template, &vars)
            .with_context(|| format!("failed to resolve template in {name}"))?;
        resolved_configs.push((name.clone(), resolved));
    }

    let mut first_start_timestamp = String::new();
    for phase in node_deploy_phase_order(&format) {
        match phase {
            NodeDeployPhase::PrepareBundle => match format {
                DeployFormat::Systemd => {
                    prepare_systemd(
                        &args.bundle,
                        &args.remote,
                        ssh_key.as_deref(),
                        ssh_port,
                        &manifest,
                        deployment.revision,
                        &ssh_base,
                    )
                    .await?;
                }
                DeployFormat::Docker => {
                    prepare_docker(
                        &args.bundle,
                        &args.remote,
                        ssh_key.as_deref(),
                        ssh_port,
                        &manifest,
                        deployment.revision,
                        &ssh_base,
                    )
                    .await?;
                }
            },
            NodeDeployPhase::UploadResolvedConfigs => {
                // Overwrite template configs with resolved versions
                print!("[deploy]  writing resolved configs ... ");
                let staging = staging_release_dir(&manifest.workdir, deployment.revision);
                for (name, content) in &resolved_configs {
                    let remote_path = format!("{staging}/config/{name}");
                    helpers::scp_bytes(
                        content.as_bytes(),
                        &args.remote,
                        &remote_path,
                        ssh_key.as_deref(),
                        ssh_port,
                    )
                    .with_context(|| format!("failed to upload resolved {name}"))?;
                }
                let marker = serde_json::json!({
                    "node_id": deployment.node_id,
                    "revision": deployment.revision,
                    "bundle_digest": deployment.bundle_digest,
                    "format": match &format {
                        DeployFormat::Systemd => "systemd",
                        DeployFormat::Docker => "docker",
                    },
                    "engines": manifest.engines,
                });
                helpers::scp_bytes(
                    serde_json::to_vec_pretty(&marker)?.as_slice(),
                    &args.remote,
                    &format!("{staging}/deployment.json"),
                    ssh_key.as_deref(),
                    ssh_port,
                )?;
                println!("OK");
            }
            NodeDeployPhase::ProvisionTls => {
                // Provision TLS certificates on the remote host
                print!("[deploy]  provisioning TLS certificates ... ");
                let remote_cert_dir = format!(
                    "{}/certs",
                    staging_release_dir(&manifest.workdir, deployment.revision)
                );
                let ca_cert = format!("{cert_dir}/ca.crt");
                let host_cert = format!("{cert_dir}/{host_name}.crt");
                let host_key = format!("{cert_dir}/{host_name}.key");

                for (local, remote_name) in [
                    (&ca_cert, "ca.crt"),
                    (&host_cert, "node.crt"),
                    (&host_key, "node.key"),
                ] {
                    if !Path::new(local).exists() {
                        bail!("Certificate file not found: {local}. Run `wr-cli cert generate {host_name}` first.");
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
            NodeDeployPhase::CaptureFirstStartTimestamp => {
                // Capture remote timestamp before first start to anchor the post-deploy log dump
                first_start_timestamp =
                    helpers::get_remote_timestamp(&ssh_base).unwrap_or_default();
            }
            NodeDeployPhase::FirstStart => {
                helpers::run_ssh(
                    &ssh_base,
                    &activate_release_command(&manifest.workdir, deployment.revision),
                )
                .context("failed to atomically activate staged release")?;
                match format {
                    DeployFormat::Systemd => {
                        print!("[deploy]  starting services ... ");
                        start_systemd(&ssh_base, &manifest)?;
                    }
                    DeployFormat::Docker => {
                        print!("[deploy]  starting containers ... ");
                        start_docker(&ssh_base, &manifest)?;
                    }
                }
                println!("OK");
            }
        }
    }

    println!("[deploy]  waiting for exact revision readiness...");

    // Tail service logs in the background while we wait
    let log_cmd = match format {
        DeployFormat::Systemd => super::logs::build_journalctl_command(None, 20, "1m", true),
        DeployFormat::Docker => {
            super::logs::build_docker_logs_command(&manifest.workdir, None, 20, true)
        }
    };
    let _log_tail = helpers::spawn_ssh_prefixed(&ssh_base, &log_cmd, "\t");

    let registered = wait_for_deployment(
        manager,
        &deployment.node_id,
        deployment.revision,
        Duration::from_secs(60),
    )
    .await;

    // _log_tail dropped here, killing the background SSH process
    drop(_log_tail);
    println!();

    if registered.is_ok() {
        println!("[deploy]  exact revision is healthy and routable");

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
        println!("[deploy]  revision verification failed; check remote logs for conditions");
    }

    // Dump all startup logs from the deploy window (catches fast starts the tail missed)
    if !first_start_timestamp.is_empty() {
        println!();
        println!("[deploy]  startup logs:");
        let dump_cmd = match format {
            DeployFormat::Systemd => super::logs::build_journalctl_command_absolute(
                None,
                200,
                &first_start_timestamp,
                false,
            ),
            DeployFormat::Docker => {
                super::logs::build_docker_logs_command(&manifest.workdir, None, 200, false)
            }
        };
        helpers::run_ssh_prefixed_best_effort(&ssh_base, &dump_cmd, "\t");
    }

    registered?;
    Ok(())
    }
    .await;

    match operation {
        Ok(()) => {
            client::connect(manager)
                .await?
                .complete_deployment(CompleteDeploymentRequest {
                    node_id: deployment.node_id,
                    revision: deployment.revision,
                    succeeded: true,
                    failure_detail: String::new(),
                })
                .await?;
            Ok(())
        }
        Err(error) => {
            if let Ok(mut manager_client) = client::connect(manager).await {
                let _ = manager_client
                    .complete_deployment(CompleteDeploymentRequest {
                        node_id: deployment.node_id,
                        revision: deployment.revision,
                        succeeded: false,
                        failure_detail: deployment_failure_detail(&error),
                    })
                    .await;
            }
            Err(error)
        }
    }
}

async fn prepare_systemd(
    bundle: &str,
    remote: &str,
    ssh_key: Option<&str>,
    ssh_port: Option<u16>,
    manifest: &Manifest,
    revision: u64,
    ssh_base: &[String],
) -> Result<()> {
    prepare_release(
        bundle, remote, ssh_key, ssh_port, manifest, revision, ssh_base,
    )
    .await?;
    let workdir = &manifest.workdir;
    let staging = staging_release_dir(workdir, revision);
    let run_user = helpers::extract_remote_user(remote).unwrap_or("root");
    print!("[deploy]  installing systemd units ... ");
    let service_files = bundle::list_files_from_tarball(bundle, "wr-node/systemd/", ".service")?;
    let mut user_vars = std::collections::HashMap::new();
    user_vars.insert("run_user", run_user);
    user_vars.insert("run_group", run_user);
    for (archive_path, template) in &service_files {
        let resolved = helpers::resolve_template(template, &user_vars)
            .with_context(|| format!("failed to resolve {archive_path}"))?;
        let name = archive_path.rsplit('/').next().unwrap_or(archive_path);
        helpers::scp_bytes(
            resolved.as_bytes(),
            remote,
            &format!("{staging}/systemd/{name}"),
            ssh_key,
            ssh_port,
        )?;
    }
    helpers::run_ssh(
        ssh_base,
        &format!("sudo cp {staging}/systemd/*.service /etc/systemd/system/"),
    )?;
    println!("OK");
    helpers::run_ssh(ssh_base, &format!("sudo cp {staging}/systemd/99-wruntime.conf /etc/sysctl.d/ && sudo sysctl --system > /dev/null"))?;
    Ok(())
}

async fn prepare_docker(
    bundle: &str,
    remote: &str,
    ssh_key: Option<&str>,
    ssh_port: Option<u16>,
    manifest: &Manifest,
    revision: u64,
    ssh_base: &[String],
) -> Result<()> {
    prepare_release(
        bundle, remote, ssh_key, ssh_port, manifest, revision, ssh_base,
    )
    .await
}

async fn prepare_release(
    bundle: &str,
    remote: &str,
    ssh_key: Option<&str>,
    ssh_port: Option<u16>,
    manifest: &Manifest,
    revision: u64,
    ssh_base: &[String],
) -> Result<()> {
    let root = format!("{}/wr-node", manifest.workdir);
    let bundle_dir = format!("{root}/bundles/{}", manifest.bundle_digest);
    let staging = staging_release_dir(&manifest.workdir, revision);
    print!("[deploy]  staging immutable release ... ");
    helpers::scp_file(bundle, remote, "/tmp/wr-bundle.tar.gz", ssh_key, ssh_port)?;
    helpers::run_ssh(
        ssh_base,
        &format!(
            "sudo mkdir -p {root}/bundles {root}/releases && test ! -e {staging} && test ! -e {root}/releases/{revision} && (test -d {bundle_dir} || (sudo mkdir {bundle_dir}.tmp && sudo tar xzf /tmp/wr-bundle.tar.gz -C {bundle_dir}.tmp --strip-components=1 && sudo mv {bundle_dir}.tmp {bundle_dir})) && sudo cp -a {bundle_dir} {staging} && sudo chown -R $(id -u):$(id -g) {staging} && sudo rm -f /tmp/wr-bundle.tar.gz"
        ),
    )?;
    println!("OK");
    Ok(())
}

fn start_systemd(ssh_base: &[String], manifest: &Manifest) -> Result<()> {
    let slots = manifest
        .engines
        .iter()
        .map(|engine| engine.engine_slot.clone())
        .collect::<Vec<_>>();
    helpers::run_ssh(ssh_base, &node_systemd_start_command(&slots))
}

fn start_docker(ssh_base: &[String], manifest: &Manifest) -> Result<()> {
    helpers::run_ssh(ssh_base, &node_docker_start_command(&manifest.workdir))
}

// --- status ---

async fn rollback(args: RollbackArgs, manager: &str) -> Result<()> {
    let deploy_cfg = DeployConfig::load_or_discover(args.config.as_deref())?;
    let workdir = deploy_config::resolve_with_default(
        &args.workdir,
        "/opt/wruntime",
        deploy_cfg.workdir,
        "WR_WORKDIR",
    );
    validate_remote_workdir(&workdir)?;
    let ssh_key = deploy_config::resolve_string(args.ssh_key, deploy_cfg.ssh_key, "WR_SSH_KEY");
    let ssh_port = deploy_config::resolve_ssh_port(args.ssh_port, deploy_cfg.ssh_port)?
        .map(helpers::DeployPort::get);
    let ssh_base = helpers::build_ssh_args(&args.remote, ssh_key.as_deref(), ssh_port);
    let attempt_token = deployment_attempt_token("rollback");
    let deployment = client::connect(manager)
        .await?
        .begin_rollback(BeginRollbackRequest {
            node_id: args.node_id.clone(),
            to_revision: args.to.unwrap_or(0),
            attempt_token,
        })
        .await?
        .into_inner()
        .deployment
        .ok_or_else(|| anyhow::anyhow!("manager returned no rollback deployment record"))?;

    println!(
        "[rollback] manager assigned node '{}' revision {} from revision {} ({})",
        deployment.node_id,
        deployment.revision,
        deployment.source_revision,
        deployment.bundle_digest
    );
    let result: Result<()> = async {
        if deployment.source_revision == 0 {
            bail!("manager rollback response omitted source_revision");
        }
        let root = format!("{workdir}/wr-node");
        let source = release_dir(&workdir, deployment.source_revision);
        let staging = staging_release_dir(&workdir, deployment.revision);
        let retained_bundle = format!("{root}/bundles/{}", deployment.bundle_digest);
        let source_marker: serde_json::Value = serde_json::from_str(&helpers::run_ssh_output(
            &ssh_base,
            &format!("sudo cat {source}/deployment.json"),
        )?)
        .context("retained release has an invalid deployment marker")?;
        let format = match source_marker.get("format").and_then(serde_json::Value::as_str) {
            Some("systemd") => DeployFormat::Systemd,
            Some("docker") => DeployFormat::Docker,
            _ => bail!("retained release deployment marker has no valid format"),
        };
        helpers::run_ssh(
            &ssh_base,
            &format!(
                "test -d {source} && test -d {retained_bundle} && test ! -e {staging} && \
                 test ! -e {root}/releases/{} && sudo cp -a {source} {staging} && \
                 sudo chown -R $(id -u):$(id -g) {staging}",
                deployment.revision
            ),
        )
        .context("retained rollback release or immutable bundle is missing")?;

        let revision = deployment.revision;
        let digest = &deployment.bundle_digest;
        let node_id = &deployment.node_id;
        let expected_config_count = deployment.expected_engines.len();
        helpers::run_ssh(
            &ssh_base,
            &format!(
                "test $(grep -l '^\\[deployment\\]$' {staging}/config/*.toml | wc -l) -eq {expected_config_count} && \
                 for config in {staging}/config/*.toml; do \
                   sed -i '/^\\[deployment\\]$/,/^\\[/ s/^node_id = .*/node_id = \"{node_id}\"/' \"$config\"; \
                   sed -i '/^\\[deployment\\]$/,/^\\[/ s/^revision = .*/revision = {revision}/' \"$config\"; \
                   sed -i '/^\\[deployment\\]$/,/^\\[/ s|^bundle_digest = .*|bundle_digest = \"{digest}\"|' \"$config\"; \
                 done",
            ),
        )
        .context("failed to inject rollback activation metadata")?;
        let marker_engines = deployment
            .expected_engines
            .iter()
            .map(|engine| {
                serde_json::json!({
                    "engine_slot": engine.engine_slot,
                    "modules": engine.modules.iter().map(|module| serde_json::json!({
                        "namespace": module.namespace,
                        "name": module.name,
                        "version": module.version,
                    })).collect::<Vec<_>>(),
                })
            })
            .collect::<Vec<_>>();
        let marker = serde_json::json!({
            "node_id": deployment.node_id,
            "revision": deployment.revision,
            "bundle_digest": deployment.bundle_digest,
            "source_revision": deployment.source_revision,
            "format": match &format {
                DeployFormat::Systemd => "systemd",
                DeployFormat::Docker => "docker",
            },
            "expected_engines": marker_engines,
        });
        helpers::scp_bytes(
            serde_json::to_vec_pretty(&marker)?.as_slice(),
            &args.remote,
            &format!("{staging}/deployment.json"),
            ssh_key.as_deref(),
            ssh_port,
        )?;

        if matches!(format, DeployFormat::Systemd) {
            helpers::run_ssh(
                &ssh_base,
                &format!("sudo cp {staging}/systemd/*.service /etc/systemd/system/"),
            )?;
        }
        helpers::run_ssh(
            &ssh_base,
            &activate_release_command(&workdir, deployment.revision),
        )?;
        match format {
            DeployFormat::Systemd => {
                let slots = deployment
                    .expected_engines
                    .iter()
                    .map(|engine| engine.engine_slot.clone())
                    .collect::<Vec<_>>();
                helpers::run_ssh(&ssh_base, &node_systemd_start_command(&slots))?;
            }
            DeployFormat::Docker => {
                helpers::run_ssh(&ssh_base, &node_docker_start_command(&workdir))?;
            }
        }
        wait_for_deployment(
            manager,
            &deployment.node_id,
            deployment.revision,
            Duration::from_secs(60),
        )
        .await
    }
    .await;

    match result {
        Ok(()) => {
            client::connect(manager)
                .await?
                .complete_deployment(CompleteDeploymentRequest {
                    node_id: deployment.node_id,
                    revision: deployment.revision,
                    succeeded: true,
                    failure_detail: String::new(),
                })
                .await?;
            println!("[rollback] exact rollback revision is healthy and routable");
            Ok(())
        }
        Err(error) => {
            if let Ok(mut manager_client) = client::connect(manager).await {
                let _ = manager_client
                    .complete_deployment(CompleteDeploymentRequest {
                        node_id: deployment.node_id,
                        revision: deployment.revision,
                        succeeded: false,
                        failure_detail: deployment_failure_detail(&error),
                    })
                    .await;
            }
            Err(error)
        }
    }
}

fn status(args: StatusArgs) -> Result<()> {
    if !Path::new(&args.bundle).exists() {
        bail!("Bundle not found: {}", args.bundle);
    }

    let manifest: Manifest = bundle::read_manifest(&args.bundle)?;
    verify_bundle(&args.bundle, &manifest)?;

    println!("Bundle: {}", args.bundle);
    println!();
    println!("  target:       {}", manifest.target);
    println!("  digest:       {}", manifest.bundle_digest);
    println!("  workdir:      {}", manifest.workdir);
    println!("  image_prefix: {}", manifest.image_prefix);
    println!();
    println!("Engine slots:");
    for engine in &manifest.engines {
        println!("  {}", engine.engine_slot);
    }
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
            "node_id" => "--node-id",
            "revision" => "manager-assigned activation revision",
            "bundle_digest" => "verified immutable bundle digest",
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_bundle_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("{name}-{}-{nanos}.tar.gz", std::process::id()))
    }

    fn index_of<T: PartialEq + std::fmt::Debug>(items: &[T], needle: T) -> usize {
        items
            .iter()
            .position(|item| item == &needle)
            .expect("expected item in deploy phase order")
    }

    #[test]
    fn bundle_digest_is_stable_across_inventory_order() {
        let mut checksums = BTreeMap::new();
        checksums.insert("wr-node/bin/wr-engine".into(), "a".repeat(64));
        checksums.insert("wr-node/config/engine.toml".into(), "b".repeat(64));
        let left = vec![ManifestEngine {
            engine_slot: "primary".into(),
            modules: vec![
                ManifestModule {
                    namespace: "store".into(),
                    name: "orders".into(),
                    version: "1.0.0".into(),
                    has_schema: true,
                },
                ManifestModule {
                    namespace: "store".into(),
                    name: "inventory".into(),
                    version: "1.0.0".into(),
                    has_schema: true,
                },
            ],
        }];
        let mut right = left.clone();
        right[0].modules.reverse();
        assert_eq!(
            deterministic_bundle_digest(
                "x86_64-unknown-linux-gnu",
                "/opt/wruntime",
                "wr",
                &left,
                &checksums,
                &None,
            )
            .unwrap(),
            deterministic_bundle_digest(
                "x86_64-unknown-linux-gnu",
                "/opt/wruntime",
                "wr",
                &right,
                &checksums,
                &None,
            )
            .unwrap()
        );
    }

    #[test]
    fn activation_switches_current_atomically() {
        let command = activate_release_command("/opt/wruntime", 7);
        assert!(command.contains("releases/.7.tmp"));
        assert!(command.contains("releases/7"));
        assert!(command.contains("mv -Tf"));
        assert!(!command.contains("ln -sfn"));
    }

    #[test]
    fn node_systemd_deploy_sequence_uploads_configs_and_certs_before_start() {
        let slots = vec!["engine-a".to_string(), "engine-b".to_string()];
        let phases = node_deploy_phase_order(&DeployFormat::Systemd);
        assert!(
            index_of(phases, NodeDeployPhase::PrepareBundle)
                < index_of(phases, NodeDeployPhase::UploadResolvedConfigs)
        );
        assert!(
            index_of(phases, NodeDeployPhase::UploadResolvedConfigs)
                < index_of(phases, NodeDeployPhase::ProvisionTls)
        );
        assert!(
            index_of(phases, NodeDeployPhase::ProvisionTls)
                < index_of(phases, NodeDeployPhase::CaptureFirstStartTimestamp)
        );
        assert!(
            index_of(phases, NodeDeployPhase::CaptureFirstStartTimestamp)
                < index_of(phases, NodeDeployPhase::FirstStart)
        );
        assert_eq!(
            node_service_names(&slots),
            vec![
                "wr-proxy.service".to_string(),
                "wr-engine-engine-a.service".to_string(),
                "wr-engine-engine-b.service".to_string()
            ]
        );
        let command = node_systemd_start_command(&slots);
        assert!(command.contains("systemctl daemon-reload"));
        assert!(command.contains("systemctl enable"));
        assert!(command.contains("systemctl restart"));
        assert!(command.contains("wr-proxy.service"));
        assert!(command.contains("wr-engine-engine-a.service"));
        assert!(command.contains("wr-engine-engine-b.service"));
        assert!(command.contains("disable --now"));
        assert!(command.contains("rm -f"));
    }

    #[test]
    fn node_docker_deploy_sequence_uploads_source_proxy_config_before_compose_start() {
        let configs = ["proxy.toml".to_string(), "engine.toml".to_string()];
        let phases = node_deploy_phase_order(&DeployFormat::Docker);
        assert!(
            index_of(phases, NodeDeployPhase::UploadResolvedConfigs)
                < index_of(phases, NodeDeployPhase::ProvisionTls)
        );
        assert!(
            index_of(phases, NodeDeployPhase::ProvisionTls)
                < index_of(phases, NodeDeployPhase::FirstStart)
        );
        assert_eq!(configs[0], "proxy.toml");
        let command = node_docker_start_command("/opt/wruntime");
        assert!(command.contains("docker compose"));
        assert!(command.contains("--project-name wruntime-node"));
        assert!(!command.contains("--project-name wruntime-manager"));
        assert!(command.contains("up -d"));
        assert!(command.contains("--build"));
        assert!(command.contains("--force-recreate"));
        assert!(command.contains("--remove-orphans"));
    }

    #[test]
    fn node_proxy_source_config_is_templated_and_drives_artifact_ports() {
        let path = temp_bundle_path("node-proxy-source");
        let result = (|| -> Result<()> {
            let output_file = fs::File::create(&path)?;
            let enc = GzEncoder::new(output_file, Compression::default());
            let mut tar = tar::Builder::new(enc);
            let mut config_names = Vec::new();
            let mut checksums = HashMap::new();
            let engine: EngineConfig = toml::from_str(
                r#"
listen_address = "127.0.0.1:9100"

[node]
proxy_address = "http://127.0.0.1:9001"
control_address = "http://127.0.0.1:9002"
peer_address = "https://127.0.0.1:9443"

[[module]]
name = "inventory"
namespace = "ecommerce"
version = "1.0.0"
wasm_path = "inventory.wasm"
"#,
            )?;
            let source: ProxyConfig = toml::from_str(
                r#"
listen_address = "127.0.0.1:9101"
control_address = "127.0.0.1:9102"

[database]
url = "postgres://localhost/source"
max_connections = 12

[node]
proxy_address = "http://127.0.0.1:9555"
control_address = "http://127.0.0.1:9102"
peer_address = "https://10.0.0.5:9555"

[node.tls]
cert_path = "certs/source.crt"
key_path = "certs/source.key"
ca_cert_path = "certs/source-ca.crt"

[circuit_breaker]
failure_threshold = 7

[egress]
allowed_hosts = ["api.example.com"]
"#,
            )?;
            let engines = vec![("engine.toml".to_string(), engine)];
            let (proxy_port, control_port, artifact_peer_port) = add_proxy_config(
                &mut tar,
                &mut checksums,
                &mut config_names,
                &engines,
                Some(&source),
                9443,
            )?;
            assert_eq!(
                (proxy_port, control_port, artifact_peer_port),
                (9101, 9102, 9555)
            );
            config_names.push("engine.toml".to_string());
            add_deployment_artifacts(
                &mut tar,
                &mut checksums,
                &DeployArtifactParams {
                    workdir: "/opt/wruntime",
                    config_names: &config_names,
                    engine_names: &["engine".to_string()],
                    no_otel: false,
                },
            )?;
            tar.into_inner()?.finish()?;

            let proxy_toml = bundle::read_file_from_tarball(path.to_str().unwrap(), "proxy.toml")?;
            let proxy_value: toml::Value = toml::from_str(&proxy_toml)?;
            assert_eq!(proxy_value["database"]["url"].as_str(), Some("{db_url}"));
            assert_eq!(
                proxy_value["node"]["proxy_address"].as_str(),
                Some("http://127.0.0.1:9555")
            );
            assert_eq!(
                proxy_value["circuit_breaker"]["failure_threshold"].as_integer(),
                Some(7)
            );
            assert_eq!(
                proxy_value["egress"]["allowed_hosts"].as_array().unwrap()[0].as_str(),
                Some("api.example.com")
            );
            assert_eq!(
                proxy_value["node"]["tls"]["cert_path"].as_str(),
                Some("certs/node.crt")
            );
            assert_eq!(
                proxy_value["node"]["tls"]["key_path"].as_str(),
                Some("certs/node.key")
            );
            assert_eq!(
                proxy_value["node"]["tls"]["ca_cert_path"].as_str(),
                Some("certs/ca.crt")
            );

            let compose =
                bundle::read_file_from_tarball(path.to_str().unwrap(), "docker-compose.yml")?;
            assert_eq!(
                proxy_value["node"]["peer_address"].as_str(),
                Some("https://{host}:{peer_port}")
            );
            assert!(compose.contains("network_mode: host"));
            assert!(!compose.contains("ports:"));
            Ok(())
        })();
        let _ = fs::remove_file(&path);
        result.unwrap();
    }

    #[test]
    fn node_proxy_generated_config_fallback_still_writes_minimal_proxy() {
        let path = temp_bundle_path("node-proxy-generated");
        let result = (|| -> Result<()> {
            let output_file = fs::File::create(&path)?;
            let enc = GzEncoder::new(output_file, Compression::default());
            let mut tar = tar::Builder::new(enc);
            let mut config_names = Vec::new();
            let mut checksums = HashMap::new();
            let engine: EngineConfig = toml::from_str(
                r#"
listen_address = "127.0.0.1:9100"

[node]
proxy_address = "http://127.0.0.1:9001"
control_address = "http://127.0.0.1:9002"
peer_address = "https://127.0.0.1:9444"

[[module]]
name = "inventory"
namespace = "ecommerce"
version = "1.0.0"
wasm_path = "inventory.wasm"
"#,
            )?;
            let engines = vec![("engine.toml".to_string(), engine)];
            let (proxy_port, control_port, artifact_peer_port) = add_proxy_config(
                &mut tar,
                &mut checksums,
                &mut config_names,
                &engines,
                None,
                9443,
            )?;
            assert_eq!(
                (proxy_port, control_port, artifact_peer_port),
                (9001, 9002, 9444)
            );
            tar.into_inner()?.finish()?;

            let proxy_toml = bundle::read_file_from_tarball(path.to_str().unwrap(), "proxy.toml")?;
            let proxy_value: toml::Value = toml::from_str(&proxy_toml)?;
            assert_eq!(
                proxy_value["listen_address"].as_str(),
                Some("127.0.0.1:9001")
            );
            assert_eq!(
                proxy_value["control_address"].as_str(),
                Some("127.0.0.1:9002")
            );
            assert_eq!(proxy_value["database"]["url"].as_str(), Some("{db_url}"));
            assert!(proxy_value.get("external").is_none());
            assert!(proxy_value.get("egress").is_none());
            Ok(())
        })();
        let _ = fs::remove_file(&path);
        result.unwrap();
    }
}
