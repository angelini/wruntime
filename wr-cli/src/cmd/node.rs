use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use flate2::write::GzEncoder;
use flate2::Compression;
use wr_common::agent_policy::AGENT_PROTOCOL_VERSION;
use wr_common::wruntime::{
    AbandonDeploymentRequest, BeginDeploymentRequest, BeginRollbackRequest, DeploymentInventoryV1,
    ExpectedEngine, ExpectedModule, FinalizeDeploymentRequest, GetNodeCleanupStatusRequest,
    ModuleIdentity, NodeCleanupState, NodeCleanupSummary, NodeOperationAction,
    RetryNodeCleanupRequest, RolloutPolicy, SecretRequest, SubmitOperationRequest,
};

use super::build_helpers::{self, BuildModule};
use super::bundle;
use super::bundle_integrity::{
    build_resolved_manifest, deterministic_bundle_digest, verify_bundle_archive,
    write_resolved_identity, BundleManifest as Manifest, ManifestEngine, ManifestModule,
    ResolvedReleaseManifest,
};
use super::config::{EngineConfig, ProxyConfig};
use super::deploy_config::{self, DeployConfig, DeployFormat};
use super::helpers;
use super::node_backend::{ReleaseMetadata, ReleaseSlot};
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
    /// Submit a rolling upgrade for an already verified, pre-staged release.
    Upgrade(DeployArgs),
    /// Submit an inventory-changing rollout for an already verified, pre-staged release.
    Scale(DeployArgs),
    /// Safely abandon an unsubmitted inactive allocation and its exact bytes.
    Abandon(AbandonArgs),
    /// Run the node-local fenced lifecycle executor.
    Agent(super::node_agent::AgentArgs),
    /// Inspect or retry manager-owned release cleanup.
    Cleanup(CleanupArgs),
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
    /// Stable idempotency token spanning allocation, staging, and submission.
    #[arg(long)]
    request_token: Option<String>,
    #[arg(long, default_value_t = 1)]
    max_unavailable: u32,
    #[arg(long)]
    canary: Option<String>,
    #[arg(long)]
    pause_after_canary: bool,
    #[arg(long)]
    allow_downtime: bool,
    #[arg(long, default_value_t = 1800)]
    deadline: u64,
    #[arg(long, default_value_t = 1800)]
    wait_timeout: u64,
    #[arg(long)]
    no_wait: bool,
    #[arg(long)]
    json: bool,
    /// Deterministic test hook: exit after remote finalization and before submission.
    #[arg(long, hide = true)]
    exit_after_finalization: bool,
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
    #[arg(long)]
    request_token: Option<String>,
    #[arg(long, default_value_t = 1)]
    max_unavailable: u32,
    #[arg(long)]
    canary: Option<String>,
    #[arg(long)]
    pause_after_canary: bool,
    #[arg(long)]
    allow_downtime: bool,
    #[arg(long, default_value_t = 1800)]
    deadline: u64,
    #[arg(long, default_value_t = 1800)]
    wait_timeout: u64,
    #[arg(long)]
    no_wait: bool,
    #[arg(long)]
    json: bool,
    #[arg(long, hide = true)]
    exit_after_finalization: bool,
}

#[derive(Args)]
pub struct AbandonArgs {
    remote: String,
    #[arg(long)]
    node_id: String,
    #[arg(long)]
    request_token: String,
    #[arg(long)]
    config: Option<String>,
    #[arg(long, default_value = "/opt/wruntime")]
    workdir: String,
    #[arg(long)]
    ssh_key: Option<String>,
    #[arg(long)]
    ssh_port: Option<u16>,
}

#[derive(Args)]
pub struct CleanupArgs {
    #[command(subcommand)]
    command: CleanupCommand,
}

#[derive(Subcommand)]
enum CleanupCommand {
    Status {
        node_id: String,
        #[arg(long)]
        json: bool,
    },
    Retry {
        node_id: String,
        #[arg(long)]
        generation: u64,
        #[arg(long)]
        json: bool,
    },
}

#[derive(serde::Serialize)]
struct CleanupDto<'a> {
    node_id: &'a str,
    state: &'static str,
    generation: u64,
    candidate_count: u32,
    inventory_count: u32,
    diagnostic_code: &'a str,
    diagnostic_detail: &'a str,
}

fn cleanup_state_name(value: i32) -> &'static str {
    match NodeCleanupState::try_from(value).unwrap_or(NodeCleanupState::Unspecified) {
        NodeCleanupState::Clean => "clean",
        NodeCleanupState::NeedsReconcile => "needs-reconcile",
        NodeCleanupState::Pending => "pending",
        NodeCleanupState::Claimed => "claimed",
        NodeCleanupState::Paused => "paused",
        NodeCleanupState::Unspecified => "unspecified",
    }
}

fn cleanup_output(summary: &NodeCleanupSummary, json: bool) -> Result<String> {
    let dto = CleanupDto {
        node_id: &summary.node_id,
        state: cleanup_state_name(summary.state),
        generation: summary.generation,
        candidate_count: summary.candidate_count,
        inventory_count: summary.inventory_count,
        diagnostic_code: &summary.diagnostic_code,
        diagnostic_detail: &summary.diagnostic_detail,
    };
    if json {
        Ok(serde_json::to_string_pretty(&dto)?)
    } else {
        Ok(format!(
            "Node {} cleanup={} generation={} candidates={} inventory={} diagnostic={} {}",
            dto.node_id,
            dto.state,
            dto.generation,
            dto.candidate_count,
            dto.inventory_count,
            dto.diagnostic_code,
            dto.diagnostic_detail
        ))
    }
}

fn render_cleanup(summary: &NodeCleanupSummary, json: bool) -> Result<()> {
    println!("{}", cleanup_output(summary, json)?);
    Ok(())
}

#[derive(Args)]
pub struct StatusArgs {
    /// Path to the bundle tarball
    bundle: String,
}

// --- Manifest ---

fn verify_bundle(bundle_path: &str, manifest: &Manifest) -> Result<()> {
    verify_bundle_archive(bundle_path, manifest)
}

// --- Entry point ---

pub async fn run(args: NodeArgs, manager: Option<&str>) -> Result<()> {
    match args.command {
        NodeCommand::Bundle(bundle_args) => bundle(bundle_args),
        NodeCommand::Deploy(deploy_args) => {
            let mgr =
                manager.ok_or_else(|| anyhow::anyhow!("--manager is required for node deploy"))?;
            durable_deploy(deploy_args, mgr, NodeOperationAction::InitialApply).await
        }
        NodeCommand::Rollback(rollback_args) => {
            let mgr = manager
                .ok_or_else(|| anyhow::anyhow!("--manager is required for node rollback"))?;
            durable_rollback(rollback_args, mgr).await
        }
        NodeCommand::Upgrade(deploy_args) => {
            let manager =
                manager.ok_or_else(|| anyhow::anyhow!("--manager is required for node upgrade"))?;
            durable_deploy(deploy_args, manager, NodeOperationAction::RollingUpgrade).await
        }
        NodeCommand::Scale(deploy_args) => {
            let manager =
                manager.ok_or_else(|| anyhow::anyhow!("--manager is required for node scale"))?;
            durable_deploy(deploy_args, manager, NodeOperationAction::Scale).await
        }
        NodeCommand::Abandon(abandon_args) => {
            let manager =
                manager.ok_or_else(|| anyhow::anyhow!("--manager is required for node abandon"))?;
            abandon(abandon_args, manager).await
        }
        NodeCommand::Agent(agent_args) => super::node_agent::run(agent_args, manager).await,
        NodeCommand::Cleanup(cleanup_args) => {
            let manager =
                manager.ok_or_else(|| anyhow::anyhow!("--manager is required for node cleanup"))?;
            let mut client = client::connect_operator(
                manager,
                wr_common::manager_client::RetryClass::NoReplayMutation,
            )
            .await?;
            match cleanup_args.command {
                CleanupCommand::Status { node_id, json } => {
                    let summary = client
                        .get_node_cleanup_status(GetNodeCleanupStatusRequest { node_id })
                        .await?
                        .into_inner()
                        .cleanup
                        .context("manager omitted cleanup summary")?;
                    render_cleanup(&summary, json)
                }
                CleanupCommand::Retry {
                    node_id,
                    generation,
                    json,
                } => {
                    let summary = client
                        .retry_node_cleanup(RetryNodeCleanupRequest {
                            node_id,
                            observed_generation: generation,
                        })
                        .await?
                        .into_inner()
                        .cleanup
                        .context("manager omitted cleanup summary")?;
                    render_cleanup(&summary, json)
                }
            }
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
        if validate_engine_slot(&engine_slot).is_err() || !engine_slots.insert(engine_slot.clone())
        {
            bail!("engine config file stems must be unique URL-safe engine slots");
        }
        bundle_config.deployment = Some(super::config::DeploymentConfig {
            node_id: "{node_id}".to_string(),
            revision: "{revision}".to_string(),
            bundle_digest: "{bundle_digest}".to_string(),
            engine_slot: engine_slot.clone(),
            operation_id: "{operation_id}".to_string(),
            revision_digest: "{revision_digest}".to_string(),
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
        }),
        endpoint_tls: super::config::CliServerTlsConfig {
            cert_path: "/etc/wruntime/pki/proxy-endpoint/sets/v1/leaf.pem".to_string(),
            key_path: "/etc/wruntime/pki/proxy-endpoint/sets/v1/key.pem".to_string(),
            client_ca_cert_path: "/etc/wruntime/pki/roots/client/ca.crt".to_string(),
        },
        client_tls: super::config::CliClientTlsConfig {
            cert_path: "/etc/wruntime/pki/proxy-client/sets/v1/leaf.pem".to_string(),
            key_path: "/etc/wruntime/pki/proxy-client/sets/v1/key.pem".to_string(),
            server_ca_cert_path: "/etc/wruntime/pki/roots/server/ca.crt".to_string(),
        },
        database: Some(super::config::ProxyDatabaseConfig {
            url: "{db_url}".to_string(),
            manager_liveness_threshold_secs: Some(
                wr_common::DEFAULT_MANAGER_LIVENESS_THRESHOLD_SECS,
            ),
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
    engine_listen_ports: &'a [u16],
    proxy_control_port: u16,
    no_otel: bool,
}

fn engine_docker_extra_copies(
    has_schema_artifacts: bool,
    has_migration_artifacts: bool,
) -> Vec<(&'static str, &'static str)> {
    let mut copies = vec![("modules/", "modules/")];
    if has_schema_artifacts {
        copies.push(("schemas/", "schemas/"));
    }
    if has_migration_artifacts {
        copies.push(("migrations/", "migrations/"));
    }
    copies
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
        engine_listen_ports,
        proxy_control_port,
        ..
    } = params;
    let no_otel = params.no_otel;
    if engine_names.len() != engine_listen_ports.len()
        || config_names.len() != engine_names.len() + 1
    {
        bail!("release service metadata inputs are inconsistent");
    }
    let mut release_slots = engine_names
        .iter()
        .zip(engine_listen_ports.iter())
        .enumerate()
        .map(|(index, (slot, port))| ReleaseSlot {
            engine_slot: slot.clone(),
            systemd_unit: format!("wr-engine-{slot}.service"),
            docker_service: format!("engine-{slot}"),
            lifecycle_address: format!("http://127.0.0.1:{port}"),
            config_path: format!("config/{}", config_names[index + 1]),
        })
        .collect::<Vec<_>>();
    release_slots.sort_by(|left, right| left.engine_slot.cmp(&right.engine_slot));
    let release_metadata = ReleaseMetadata {
        format_version: 1,
        proxy_lifecycle_address: format!("http://127.0.0.1:{proxy_control_port}"),
        proxy_systemd_unit: "wr-proxy.service".to_string(),
        proxy_docker_service: "proxy".to_string(),
        slots: release_slots,
    };
    release_metadata.validate()?;
    bundle::tar_add_bytes_checked(
        tar,
        checksums,
        "wr-node/release-metadata.json",
        serde_json::to_vec_pretty(&release_metadata)?.as_slice(),
        0o644,
    )?;
    let has_schema_artifacts = checksums
        .keys()
        .any(|path| path.starts_with("wr-node/schemas/"));
    let has_migration_artifacts = checksums
        .keys()
        .any(|path| path.starts_with("wr-node/migrations/"));

    // Systemd units
    let proxy_unit = ServiceUnit {
        description: "wruntime proxy",
        binary_path: &format!("{workdir}/wr-node/proxy/bin/wr-proxy"),
        config_path: &format!("{workdir}/wr-node/proxy/config/{}", config_names[0]),
        working_directory: &format!("{workdir}/wr-node/proxy"),
        env_vars: vec![],
        no_otel,
        after: vec![],
        wants: vec![],
    };
    bundle::tar_add_bytes_checked(
        tar,
        checksums,
        "wr-node/systemd/wr-proxy.service",
        proxy_unit.to_systemd().as_bytes(),
        0o644,
    )?;
    bundle::tar_add_bytes_checked(
        tar,
        checksums,
        "wr-node/agent/wr-node-agent.service",
        service_gen::node_agent_systemd_unit(workdir).as_bytes(),
        0o644,
    )?;

    for (i, engine_name) in engine_names.iter().enumerate() {
        let cfg_name = &config_names[i + 1];
        let engine_unit = ServiceUnit {
            description: &format!("wruntime engine ({engine_name})"),
            binary_path: &format!("{workdir}/wr-node/slots/{engine_name}/bin/wr-engine"),
            config_path: &format!("{workdir}/wr-node/slots/{engine_name}/config/{cfg_name}"),
            working_directory: &format!("{workdir}/wr-node/slots/{engine_name}"),
            env_vars: vec![],
            no_otel,
            after: vec!["wr-proxy.service"],
            wants: vec!["wr-proxy.service"],
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
        extra_copies: vec![],
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
            extra_copies: engine_docker_extra_copies(has_schema_artifacts, has_migration_artifacts),
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
        volumes: vec!["/etc/wruntime/pki:/etc/wruntime/pki:ro".into()],
        depends_on: vec![],
        healthcheck: service_gen::ComposeHealthcheck {
            test: vec![
                "CMD".into(),
                format!("{workdir}/bin/wr-proxy"),
                "--lifecycle-probe".into(),
                format!("{workdir}/config/proxy.toml"),
            ],
            interval: "2s",
            timeout: "2s",
            retries: 15,
            start_period: "30s",
        },
    }];

    for (index, name) in engine_names.iter().enumerate() {
        let cfg_name = &config_names[index + 1];
        compose_services.push(service_gen::ComposeService {
            name: format!("engine-{name}"),
            dockerfile: format!("docker/Dockerfile.engine-{name}"),
            context: "..".into(),
            image: None,
            network_mode: Some("host".into()),
            ports: vec![],
            volumes: vec!["/etc/wruntime/pki:/etc/wruntime/pki:ro".into()],
            depends_on: vec![service_gen::ComposeDependency {
                service: "proxy".into(),
                condition: "service_healthy",
            }],
            healthcheck: service_gen::ComposeHealthcheck {
                test: vec![
                    "CMD".into(),
                    format!("{workdir}/bin/wr-engine"),
                    "--lifecycle-probe".into(),
                    format!("{workdir}/config/{cfg_name}"),
                ],
                interval: "2s",
                timeout: "2s",
                retries: 30,
                start_period: "60s",
            },
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

struct NodeBundleAssembly<'a> {
    output: &'a Path,
    target: &'a str,
    host_binary_dir: &'a Path,
    workdir: &'a str,
    image_prefix: &'a str,
    peer_port: u16,
    no_otel: bool,
    source_proxy_config: Option<&'a ProxyConfig>,
    engine_configs: &'a [(String, EngineConfig)],
    precompile_hash: Option<String>,
}

/// The single production bundle assembly path. Production resolves/builds its
/// inputs before this seam; determinism tests provide complete disposable
/// inputs and execute this exact archive/config/unit/metadata path twice.
fn assemble_node_bundle(input: NodeBundleAssembly<'_>) -> Result<Manifest> {
    let output_file = fs::File::create(input.output)
        .with_context(|| format!("failed to create output file: {}", input.output.display()))?;
    let enc = GzEncoder::new(output_file, Compression::default());
    let mut tar = tar::Builder::new(enc);
    let mut checksums: HashMap<String, String> = HashMap::new();
    let mut manifest_modules: Vec<ManifestModule> = Vec::new();
    let mut config_names: Vec<String> = Vec::new();

    for bin_name in &["wr-proxy", "wr-engine", "wr-cli"] {
        let src = input.host_binary_dir.join(bin_name);
        if !src.exists() {
            bail!(
                "Binary not found: {}. Did cross-compilation succeed?",
                src.display()
            );
        }
        let archive_path = if *bin_name == "wr-cli" {
            "wr-node/agent/wr-cli".to_string()
        } else {
            format!("wr-node/bin/{bin_name}")
        };
        bundle::tar_add_file(&mut tar, &mut checksums, &archive_path, &src, 0o755)?;
    }
    bundle::tar_add_bytes_checked(
        &mut tar,
        &mut checksums,
        "wr-node/agent/protocol-version",
        format!("{AGENT_PROTOCOL_VERSION}\n").as_bytes(),
        0o644,
    )?;

    let (proxy_port, control_port, artifact_peer_port) = add_proxy_config(
        &mut tar,
        &mut checksums,
        &mut config_names,
        input.engine_configs,
        input.source_proxy_config,
        input.peer_port,
    )?;
    let (engine_names, engine_listen_ports) = add_engine_artifacts(
        &mut tar,
        &mut checksums,
        &mut config_names,
        &mut manifest_modules,
        input.engine_configs,
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

    let template_vars = vec![
        "host".to_string(),
        "db_url".to_string(),
        "peer_port".to_string(),
        "node_id".to_string(),
        "revision".to_string(),
        "bundle_digest".to_string(),
        "operation_id".to_string(),
        "revision_digest".to_string(),
    ];
    add_deployment_artifacts(
        &mut tar,
        &mut checksums,
        &DeployArtifactParams {
            workdir: input.workdir,
            config_names: &config_names,
            engine_names: &engine_names,
            engine_listen_ports: &engine_listen_ports,
            proxy_control_port: control_port,
            no_otel: input.no_otel,
        },
    )?;

    let engines: Vec<ManifestEngine> = engine_names
        .iter()
        .zip(input.engine_configs)
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
                secrets: config
                    .modules
                    .iter()
                    .flat_map(|module| {
                        module
                            .extra
                            .get("env")
                            .and_then(toml::Value::as_table)
                            .into_iter()
                            .flat_map(move |env| {
                                env.iter().filter_map(move |(key, value)| {
                                    value
                                        .as_table()
                                        .and_then(|table| table.get("secret"))
                                        .and_then(toml::Value::as_bool)
                                        .filter(|value| *value)
                                        .map(|_| (module.namespace.clone(), key.clone()))
                                })
                            })
                    })
                    .collect(),
                db_namespaces: config
                    .modules
                    .iter()
                    .filter(|module| module.database)
                    .map(|module| module.namespace.clone())
                    .collect(),
                job_queue_id: config
                    .job_admin
                    .as_ref()
                    .map(|admin| admin.queue_id.clone())
                    .unwrap_or_default(),
                job_admin_address: config
                    .job_admin
                    .as_ref()
                    .map(|admin| admin.advertise_address.clone())
                    .unwrap_or_default(),
            }
        })
        .collect();
    let manifest_checksums: BTreeMap<_, _> = checksums.into_iter().collect();
    let bundle_digest = deterministic_bundle_digest(
        input.target,
        input.workdir,
        input.image_prefix,
        &engines,
        &manifest_checksums,
        &input.precompile_hash,
    )?;
    let manifest = Manifest {
        target: input.target.to_string(),
        bundle_digest,
        engines,
        workdir: input.workdir.to_string(),
        image_prefix: input.image_prefix.to_string(),
        modules: manifest_modules,
        configs: config_names,
        template_vars,
        checksums: manifest_checksums,
        precompile_hash: input.precompile_hash,
    };
    bundle::tar_add_bytes(
        &mut tar,
        "wr-node/manifest.json",
        serde_json::to_string_pretty(&manifest)?.as_bytes(),
        0o644,
    )?;
    tar.into_inner()?.finish()?;
    Ok(manifest)
}

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

    // Step 5: Assemble the bundle through the shared production seam.
    let output = args
        .output
        .unwrap_or_else(|| "wr-node-bundle.tar.gz".to_string());
    println!("[bundle]  assembling tarball ...");
    let target_dir = PathBuf::from(format!("target/{target}/release"));
    let manifest = assemble_node_bundle(NodeBundleAssembly {
        output: Path::new(&output),
        target: &target,
        host_binary_dir: &target_dir,
        workdir: &workdir,
        image_prefix: &image_prefix,
        peer_port,
        no_otel,
        source_proxy_config: source_proxy_config.as_ref(),
        engine_configs: &all_engine_configs,
        precompile_hash,
    })?;
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

fn staging_release_dir(workdir: &str, revision: u64) -> String {
    format!("{workdir}/wr-node/releases/.{revision}.tmp")
}

fn release_dir(workdir: &str, revision: u64) -> String {
    format!("{workdir}/wr-node/releases/{revision}")
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

fn validate_engine_slot(slot: &str) -> Result<()> {
    if slot.is_empty()
        || slot.len() > 128
        || !slot
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        bail!("engine slot must be a non-empty URL-safe deployment slot");
    }
    Ok(())
}

fn validate_deploy_listener_ports(configs: &[(String, String)], peer_port: u16) -> Result<()> {
    let mut ports = std::collections::HashSet::from([peer_port]);
    for (name, content) in configs {
        // Bundled engine configs carry an unquoted numeric placeholder until the
        // manager assigns a revision. Substitute a valid sentinel solely for
        // pre-deployment listener validation.
        let parseable = content.replace("revision = {revision}", "revision = 1");
        let config: toml::Value = toml::from_str(&parseable)
            .with_context(|| format!("failed to parse bundled config {name}"))?;
        for field in ["listen_address", "control_address"] {
            if let Some(address) = config.get(field).and_then(toml::Value::as_str) {
                let port = helpers::extract_port(address)?.get();
                if !ports.insert(port) {
                    bail!("deployed listener port conflicts in bundled config {name}");
                }
            }
        }
        if let Some(address) = config
            .get("job_admin")
            .and_then(|value| value.get("listen_address"))
            .and_then(toml::Value::as_str)
        {
            let port = helpers::extract_port(address)?.get();
            if !ports.insert(port) {
                bail!("deployed job-admin listener port conflicts in bundled config {name}");
            }
        }
    }
    Ok(())
}

fn expected_engines(manifest: &Manifest, host: &str) -> Result<Vec<ExpectedEngine>> {
    manifest
        .engines
        .iter()
        .map(|engine| {
            let job_admin_address = if engine.job_admin_address.is_empty() {
                String::new()
            } else {
                super::config::deployed_job_admin_address(&engine.job_admin_address, host)?
            };
            Ok(ExpectedEngine {
                engine_slot: engine.engine_slot.clone(),
                modules: engine
                    .modules
                    .iter()
                    .map(|module| ExpectedModule {
                        identity: Some(ModuleIdentity {
                            namespace: module.namespace.clone(),
                            name: module.name.clone(),
                            version: module.version.clone(),
                        }),
                        proto_schema_digest: if module.has_schema {
                            manifest
                                .checksums
                                .get(&format!("wr-node/schemas/{}.binpb", module.name))
                                .map(|digest| format!("sha256:{digest}"))
                                .unwrap_or_default()
                        } else {
                            String::new()
                        },
                    })
                    .collect(),
                secrets: engine
                    .secrets
                    .iter()
                    .map(|(namespace, key)| SecretRequest {
                        namespace: namespace.clone(),
                        key: key.clone(),
                    })
                    .collect(),
                db_namespaces: engine.db_namespaces.clone(),
                job_queue_id: engine.job_queue_id.clone(),
                job_admin_address,
            })
        })
        .collect()
}

struct ResolvedStage {
    root: PathBuf,
    archive: PathBuf,
    manifest: ResolvedReleaseManifest,
    digest: String,
}

impl Drop for ResolvedStage {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn resolved_stage_root() -> PathBuf {
    std::env::temp_dir().join(format!(
        "wr-resolved-release-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ))
}

fn node_tls_profiles(requires_job_admin_tls: bool) -> Vec<&'static str> {
    let mut profiles = vec!["proxy-endpoint", "proxy-client"];
    if requires_job_admin_tls {
        profiles.push("engine-admin-endpoint");
    }
    profiles
}

fn node_tls_runtime_access_command(profiles: &[&str], run_group: &str) -> String {
    let owner = helpers::shell_quote(&format!("root:{run_group}"));
    let credential_sets = profiles
        .iter()
        .map(|profile| format!("/etc/wruntime/pki/{profile}/sets/v1"))
        .collect::<Vec<_>>();
    let credential_sets = credential_sets
        .iter()
        .map(|path| helpers::shell_quote(path))
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        "set -eu; sudo chown {owner} /etc/wruntime/pki/roots/server /etc/wruntime/pki/roots/client /etc/wruntime/pki/roots/server/ca.crt /etc/wruntime/pki/roots/client/ca.crt; sudo chmod 0750 /etc/wruntime/pki/roots/server /etc/wruntime/pki/roots/client; sudo chmod 0440 /etc/wruntime/pki/roots/server/ca.crt /etc/wruntime/pki/roots/client/ca.crt; for set in {credential_sets}; do parent=$(dirname \"$set\"); sudo chown {owner} \"$parent\"; sudo chmod 0750 \"$parent\"; sudo chown -R {owner} \"$set\"; sudo find \"$set\" -type d -exec chmod 0550 {{}} +; sudo find \"$set\" -type f -exec chmod 0440 {{}} +; done"
    )
}

fn provision_node_tls(
    cert_dir: &str,
    remote: &str,
    ssh_key: Option<&str>,
    ssh_port: Option<u16>,
    requires_job_admin_tls: bool,
) -> Result<()> {
    for (local, remote_path) in [
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
            remote,
            remote_path,
            ssh_key,
            ssh_port,
            0o444,
            helpers::RemoteInstallClass::Public,
            None,
        )?;
    }
    let profiles = node_tls_profiles(requires_job_admin_tls);
    for profile in &profiles {
        let local = PathBuf::from(format!("{cert_dir}/{profile}"));
        let digest = helpers::local_tree_digest(&local)?;
        helpers::install_remote_directory(
            &local,
            remote,
            &format!("/etc/wruntime/pki/{profile}/sets/v1"),
            ssh_key,
            ssh_port,
            &digest,
        )?;
    }
    let run_group = helpers::extract_remote_user(remote).unwrap_or("root");
    helpers::run_ssh(
        &helpers::build_ssh_args(remote, ssh_key, ssh_port),
        &node_tls_runtime_access_command(&profiles, run_group),
    )
    .context("failed to grant the workload group read-only TLS access")
}

#[allow(clippy::too_many_arguments)] // Resolution binds the complete deploy contract in one atomic staging operation.
fn materialize_resolved_release(
    bundle_path: &str,
    manifest: &Manifest,
    configs: &[(String, String)],
    node_id: &str,
    revision: u64,
    operation_id: &str,
    revision_digest: &str,
    format: DeployFormat,
    db_url: &str,
    peer_port: u16,
    remote: &str,
    host_ip: &str,
) -> Result<ResolvedStage> {
    anyhow::ensure!(
        wr_common::deployment_contract::deployment_operation_id(revision_digest)? == operation_id,
        "deployment operation ID does not match its canonical revision digest"
    );
    let root = resolved_stage_root();
    std::fs::create_dir_all(&root)?;
    let archive_file = std::fs::File::open(bundle_path)?;
    let decoder = flate2::read::GzDecoder::new(archive_file);
    let mut archive = tar::Archive::new(decoder);
    archive
        .unpack(&root)
        .context("failed to extract verified source bundle")?;
    let release = root.join("wr-node");
    anyhow::ensure!(release.is_dir(), "bundle omitted wr-node release root");
    let mut vars = HashMap::new();
    let peer_port_string = peer_port.to_string();
    let revision_string = revision.to_string();
    vars.insert("host", host_ip);
    vars.insert("db_url", db_url);
    vars.insert("peer_port", peer_port_string.as_str());
    vars.insert("node_id", node_id);
    vars.insert("revision", revision_string.as_str());
    vars.insert("bundle_digest", manifest.bundle_digest.as_str());
    vars.insert("operation_id", operation_id);
    vars.insert("revision_digest", revision_digest);
    for (name, template) in configs {
        let resolved = helpers::resolve_template(template, &vars)
            .with_context(|| format!("failed to resolve template in {name}"))?;
        std::fs::write(release.join("config").join(name), resolved)?;
    }
    let run_user = helpers::extract_remote_user(remote).unwrap_or("root");
    let systemd = release.join("systemd");
    if systemd.is_dir() {
        for entry in std::fs::read_dir(&systemd)? {
            let path = entry?.path();
            if path.extension().and_then(|value| value.to_str()) == Some("service") {
                let template = std::fs::read_to_string(&path)?;
                let resolved = template
                    .replace("{run_user}", run_user)
                    .replace("{run_group}", run_user);
                std::fs::write(path, resolved)?;
            }
        }
    }
    let marker = serde_json::json!({
        "node_id": node_id,
        "revision": revision,
        "bundle_digest": manifest.bundle_digest,
        "operation_id": operation_id,
        "revision_digest": revision_digest,
        "format": match format { DeployFormat::Systemd => "systemd", DeployFormat::Docker => "docker" },
        "engines": manifest.engines,
    });
    std::fs::write(
        release.join("deployment.json"),
        serde_json::to_vec_pretty(&marker)?,
    )?;
    std::fs::write(
        release.join("bundle.sha256"),
        format!("{}\n", manifest.bundle_digest),
    )?;
    let backend = match format {
        DeployFormat::Systemd => "systemd",
        DeployFormat::Docker => "docker",
    };
    let resolved_manifest = build_resolved_manifest(
        &release,
        node_id,
        revision,
        backend,
        &manifest.bundle_digest,
    )?;
    let digest = write_resolved_identity(&release, &resolved_manifest)?;
    let output = root.join("resolved-release.tar.gz");
    let file = std::fs::File::create(&output)?;
    let encoder = GzEncoder::new(file, Compression::default());
    let mut builder = tar::Builder::new(encoder);
    builder.append_dir_all(".", &release)?;
    builder.into_inner()?.finish()?;
    Ok(ResolvedStage {
        root,
        archive: output,
        manifest: resolved_manifest,
        digest,
    })
}

fn remote_resolved_verification(
    release: &str,
    manifest: &ResolvedReleaseManifest,
    resolved_digest: &str,
) -> String {
    let mut checks = vec![
        format!(
            "test \"$(sudo cat {release}/bundle.sha256)\" = '{}'",
            manifest.bundle_digest
        ),
        format!("test \"$(sudo cat {release}/resolved-release.sha256)\" = '{resolved_digest}'"),
    ];
    checks.extend(manifest.files.iter().map(|(path, file)| {
        format!(
            "test \"$(sudo sha256sum {release}/{path} | cut -d' ' -f1)\" = '{}' && test \"$(sudo stat -c '%a' {release}/{path})\" = '{:o}'",
            file.sha256, file.mode
        )
    }));
    checks.join(" && ")
}

fn remote_release_hardening(release: &str, run_group: &str) -> String {
    format!("sudo chown -R root:{run_group} {release} && sudo chmod 755 {release}")
}

fn finalize_remote_release(
    stage: &ResolvedStage,
    remote: &str,
    ssh_key: Option<&str>,
    ssh_port: Option<u16>,
    ssh_base: &[String],
    workdir: &str,
    revision: u64,
) -> Result<()> {
    let root = format!("{workdir}/wr-node/releases");
    let temporary = format!("{root}/.{revision}.tmp");
    let release = format!("{root}/{revision}");
    let upload = format!("/tmp/wr-resolved-{revision}.tar.gz");
    helpers::scp_file(
        stage
            .archive
            .to_str()
            .context("temporary archive path is not UTF-8")?,
        remote,
        &upload,
        ssh_key,
        ssh_port,
    )?;
    let verify = remote_resolved_verification(&release, &stage.manifest, &stage.digest);
    let verify_tmp = remote_resolved_verification(&temporary, &stage.manifest, &stage.digest);
    let run_group = helpers::extract_remote_user(remote).unwrap_or("root");
    let harden = remote_release_hardening(&release, run_group);
    let harden_tmp = remote_release_hardening(&temporary, run_group);
    helpers::run_ssh(
        ssh_base,
        &format!(
            "sudo mkdir -p {root} && if test -d {release}; then {verify} && {harden}; else sudo rm -rf {temporary} && sudo mkdir {temporary} && sudo tar xzf {upload} -C {temporary} && {verify_tmp} && {harden_tmp} && sudo mv {temporary} {release}; fi && sudo rm -f {upload}"
        ),
    )
}

async fn require_compatible_attestation(
    manager: &str,
    node_id: &str,
    format: DeployFormat,
    manifest: &Manifest,
) -> Result<()> {
    let status = client::connect_operator(manager, wr_common::manager_client::RetryClass::ReadOnly)
        .await?
        .get_status(wr_common::wruntime::GetOperatorStatusRequest {
            node_id: node_id.to_string(),
            engine_slot: String::new(),
        })
        .await?
        .into_inner();
    let expected_backend = match format {
        DeployFormat::Systemd => wr_common::wruntime::BackendKind::Systemd,
        DeployFormat::Docker => wr_common::wruntime::BackendKind::Docker,
    } as i32;
    let expected_binary = format!(
        "sha256:{}",
        manifest
            .checksums
            .get("wr-node/agent/wr-cli")
            .context("bundle omits the digest-covered node-agent binary")?
    );
    let fresh = chrono::Utc::now().timestamp();
    anyhow::ensure!(
        status.agent_attestations.iter().any(|attestation| {
            attestation.protocol_version == AGENT_PROTOCOL_VERSION
                && attestation.backend == expected_backend
                && attestation.binary_digest == expected_binary
                && attestation
                    .observed_at
                    .as_ref()
                    .is_some_and(|time| fresh.saturating_sub(time.seconds) <= 30)
        }),
        "fresh compatible installed node-agent attestation is required before submission"
    );
    Ok(())
}

async fn durable_deploy(
    args: DeployArgs,
    manager: &str,
    action: NodeOperationAction,
) -> Result<()> {
    anyhow::ensure!(
        Path::new(&args.bundle).is_file(),
        "Bundle not found: {}",
        args.bundle
    );
    let deploy_cfg = DeployConfig::load_or_discover(args.config.as_deref())?;
    let format = deploy_config::resolve_format(args.format, deploy_cfg.format);
    let db_url = deploy_config::resolve_required(
        args.db_url,
        deploy_cfg.db_url.clone(),
        "WR_DB_URL",
        "db_url",
    )?;
    let ssh_key =
        deploy_config::resolve_string(args.ssh_key, deploy_cfg.ssh_key.clone(), "WR_SSH_KEY");
    let ssh_port = deploy_config::resolve_ssh_port(args.ssh_port, deploy_cfg.ssh_port)?
        .map(helpers::DeployPort::get);
    let cert_dir = deploy_config::resolve_cert_dir(&args.cert_dir, deploy_cfg.cert_dir.clone());
    let peer_port = deploy_config::resolve_peer_port(args.peer_port, deploy_cfg.peer_port)?.get();
    let manifest: Manifest = bundle::read_manifest(&args.bundle)?;
    verify_bundle(&args.bundle, &manifest)?;
    validate_remote_workdir(&manifest.workdir)?;
    let configs = bundle::read_configs_from_tarball(&args.bundle)?;
    validate_deploy_listener_ports(&configs, peer_port)?;
    let token = args
        .request_token
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    println!("Request token: {token}");
    println!("Bundle digest: {}", manifest.bundle_digest);
    println!(
        "Target slots: {}",
        manifest
            .engines
            .iter()
            .map(|engine| engine.engine_slot.as_str())
            .collect::<Vec<_>>()
            .join(",")
    );
    let ssh_base = helpers::build_ssh_args(&args.remote, ssh_key.as_deref(), ssh_port);
    let host_ip = helpers::resolve_remote_ip(&ssh_base, &args.remote)?;
    let deployment = client::connect_operator(
        manager,
        wr_common::manager_client::RetryClass::DurableCreate,
    )
    .await?
    .begin_deployment(BeginDeploymentRequest {
        node_id: args.node_id.clone(),
        attempt_token: token.clone(),
        bundle_digest: manifest.bundle_digest.clone(),
        inventory: Some(DeploymentInventoryV1 {
            schema_version: 1,
            engines: expected_engines(&manifest, &host_ip)?,
        }),
    })
    .await?
    .into_inner()
    .deployment
    .context("manager returned no deployment record")?;
    provision_node_tls(
        &cert_dir,
        &args.remote,
        ssh_key.as_deref(),
        ssh_port,
        configs
            .iter()
            .any(|(_, config)| config.contains("[job_admin]")),
    )?;
    let stage = materialize_resolved_release(
        &args.bundle,
        &manifest,
        &configs,
        &args.node_id,
        deployment.revision,
        &deployment.operation_id,
        &deployment.revision_digest,
        format,
        &db_url,
        peer_port,
        &args.remote,
        &host_ip,
    )?;
    println!("Resolved release digest: {}", stage.digest);
    finalize_remote_release(
        &stage,
        &args.remote,
        ssh_key.as_deref(),
        ssh_port,
        &ssh_base,
        &manifest.workdir,
        deployment.revision,
    )?;
    let finalized = client::connect_operator(
        manager,
        wr_common::manager_client::RetryClass::NoReplayMutation,
    )
    .await?
    .finalize_deployment(FinalizeDeploymentRequest {
        node_id: args.node_id.clone(),
        attempt_token: token.clone(),
        revision: deployment.revision,
        bundle_digest: manifest.bundle_digest.clone(),
        resolved_release_digest: stage.digest.clone(),
    })
    .await?
    .into_inner()
    .deployment
    .context("FinalizeDeployment omitted deployment")?;
    anyhow::ensure!(
        finalized.resolved_release_digest == stage.digest,
        "manager finalized a conflicting release identity"
    );
    if args.exit_after_finalization {
        bail!("deterministic exit after inactive release finalization");
    }
    require_compatible_attestation(manager, &args.node_id, format, &manifest).await?;
    let mut slots = manifest
        .engines
        .iter()
        .map(|engine| engine.engine_slot.clone())
        .collect::<Vec<_>>();
    slots.sort();
    let canary = args
        .canary
        .unwrap_or_else(|| slots.first().cloned().unwrap_or_default());
    let operation = client::connect_operator(
        manager,
        wr_common::manager_client::RetryClass::DurableCreate,
    )
    .await?
    .submit_operation(SubmitOperationRequest {
        node_id: args.node_id,
        request_token: token,
        action: action as i32,
        engine_slots: slots,
        target_revision: deployment.revision,
        bundle_digest: manifest.bundle_digest,
        policy: Some(RolloutPolicy {
            max_unavailable: args.max_unavailable,
            canary_slot: canary,
            pause_after_canary: args.pause_after_canary,
            allow_downtime: args.allow_downtime,
            deadline_seconds: args.deadline,
        }),
        resolved_release_digest: stage.digest.clone(),
    })
    .await?
    .into_inner()
    .operation
    .context("SubmitOperation omitted operation")?;
    println!("Operation ID: {}", operation.operation_id);
    if args.no_wait {
        super::operations::render_operation(&operation, args.json)
    } else {
        super::operations::wait_for_terminal(
            manager,
            operation,
            Duration::from_secs(args.wait_timeout),
            args.json,
        )
        .await
    }
}

async fn abandon(args: AbandonArgs, manager: &str) -> Result<()> {
    let deploy = DeployConfig::load_or_discover(args.config.as_deref())?;
    let workdir = deploy_config::resolve_with_default(
        &args.workdir,
        "/opt/wruntime",
        deploy.workdir,
        "WR_WORKDIR",
    );
    validate_remote_workdir(&workdir)?;
    let ssh_key = deploy_config::resolve_string(args.ssh_key, deploy.ssh_key, "WR_SSH_KEY");
    let ssh_port = deploy_config::resolve_ssh_port(args.ssh_port, deploy.ssh_port)?
        .map(helpers::DeployPort::get);
    let response = client::connect_operator(
        manager,
        wr_common::manager_client::RetryClass::NoReplayMutation,
    )
    .await?
    .abandon_deployment(AbandonDeploymentRequest {
        node_id: args.node_id,
        attempt_token: args.request_token,
    })
    .await?
    .into_inner();
    let deployment = response
        .deployment
        .context("AbandonDeployment omitted deployment")?;
    let release = release_dir(&workdir, deployment.revision);
    let temporary = staging_release_dir(&workdir, deployment.revision);
    let ssh = helpers::build_ssh_args(&args.remote, ssh_key.as_deref(), ssh_port);
    helpers::run_ssh(
        &ssh,
        &format!(
            "if test -d {release}; then test \"$(cat {release}/bundle.sha256)\" = '{}' && test \"$(cat {release}/resolved-release.sha256)\" = '{}'; fi && sudo rm -rf {temporary} {release}",
            deployment.bundle_digest, deployment.resolved_release_digest
        ),
    )?;
    println!(
        "Abandoned request token {} revision {}",
        deployment.attempt_token, deployment.revision
    );
    Ok(())
}

async fn durable_rollback(args: RollbackArgs, manager: &str) -> Result<()> {
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
    let token = args
        .request_token
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    println!("Request token: {token}");
    let deployment = client::connect_operator(
        manager,
        wr_common::manager_client::RetryClass::DurableCreate,
    )
    .await?
    .begin_rollback(BeginRollbackRequest {
        node_id: args.node_id.clone(),
        to_revision: args.to.unwrap_or(0),
        attempt_token: token.clone(),
    })
    .await?
    .into_inner()
    .deployment
    .context("manager returned no rollback deployment")?;
    anyhow::ensure!(
        wr_common::deployment_contract::deployment_operation_id(&deployment.revision_digest)?
            == deployment.operation_id,
        "rollback operation ID does not match its canonical revision digest"
    );
    let source = release_dir(&workdir, deployment.source_revision);
    let target = release_dir(&workdir, deployment.revision);
    let temporary = staging_release_dir(&workdir, deployment.revision);
    let ssh = helpers::build_ssh_args(&args.remote, ssh_key.as_deref(), ssh_port);
    let run_group = helpers::extract_remote_user(&args.remote).unwrap_or("root");
    let harden = remote_release_hardening(&target, run_group);
    let script = format!(
        r#"set -eu
sudo env SOURCE={source:?} TARGET={target:?} TMP={temporary:?} NODE={node:?} REV={revision} BUNDLE={bundle:?} OPERATION={operation:?} REVISION_DIGEST={revision_digest:?} python3 - <<'PY'
import hashlib,json,os,pathlib,re,shutil,stat
source=pathlib.Path(os.environ['SOURCE']); target=pathlib.Path(os.environ['TARGET']); tmp=pathlib.Path(os.environ['TMP'])
node=os.environ['NODE']; revision=int(os.environ['REV']); bundle=os.environ['BUNDLE']
operation=os.environ['OPERATION']; revision_digest=os.environ['REVISION_DIGEST']
if target.exists():
 value=json.loads((target/'deployment.json').read_text())
 if value.get('revision')!=revision or value.get('operation_id')!=operation or value.get('revision_digest')!=revision_digest: raise SystemExit('existing rollback release identity mismatch')
 print((target/'resolved-release.sha256').read_text().strip()); raise SystemExit(0)
shutil.rmtree(tmp, ignore_errors=True); shutil.copytree(source,tmp,symlinks=False)
for name in ('resolved-release.json','resolved-release.sha256'): (tmp/name).unlink(missing_ok=True)
for config in (tmp/'config').glob('*.toml'):
 text=config.read_text(); parts=text.split('[deployment]',1)
 if len(parts)!=2: raise SystemExit(f'missing deployment metadata in {{config}}')
 head,tail=parts
 replacements=[
  (r'(?m)^revision\s*=\s*\d+',f'revision = {revision}','revision'),
  (r'(?m)^operation_id\s*=\s*"[^"]*"',f'operation_id = "{operation}"','operation_id'),
  (r'(?m)^revision_digest\s*=\s*"[^"]*"',f'revision_digest = "{revision_digest}"','revision_digest'),
 ]
 for pattern,replacement,name in replacements:
  tail,count=re.subn(pattern,replacement,tail,count=1)
  if count!=1: raise SystemExit(f'missing deployment {{name}} in {{config}}')
 config.write_text(head+'[deployment]'+tail)
marker=tmp/'deployment.json'; value=json.loads(marker.read_text()); value['revision']=revision; value['operation_id']=operation; value['revision_digest']=revision_digest; value['source_revision']={source_revision}; marker.write_text(json.dumps(value,indent=2)+'\n')
files={{}}
for path in sorted(tmp.rglob('*')):
 if path.is_symlink() or (path.exists() and not (path.is_file() or path.is_dir())): raise SystemExit('invalid release entry')
 if path.is_file() and path.name not in ('resolved-release.json','resolved-release.sha256'):
  rel=path.relative_to(tmp).as_posix(); files[rel]={{'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'mode':stat.S_IMODE(path.stat().st_mode)}}
backend=json.loads(marker.read_text())['format']
manifest={{'version':1,'node_id':node,'revision':revision,'backend':backend,'bundle_digest':bundle,'files':files}}
canonical=json.dumps(manifest,separators=(',',':')).encode(); digest='sha256:'+hashlib.sha256(b'wruntime.resolved-release.v1\0'+canonical).hexdigest()
(tmp/'resolved-release.json').write_text(json.dumps(manifest,indent=2)+'\n'); (tmp/'resolved-release.sha256').write_text(digest+'\n')
os.rename(tmp,target); print(digest)
PY
{harden}"#,
        source = source,
        target = target,
        temporary = temporary,
        node = args.node_id,
        revision = deployment.revision,
        bundle = deployment.bundle_digest,
        operation = deployment.operation_id,
        revision_digest = deployment.revision_digest,
        source_revision = deployment.source_revision,
    );
    let resolved_digest = helpers::run_ssh_output(&ssh, &script)?;
    let finalized = client::connect_operator(
        manager,
        wr_common::manager_client::RetryClass::NoReplayMutation,
    )
    .await?
    .finalize_deployment(FinalizeDeploymentRequest {
        node_id: args.node_id.clone(),
        attempt_token: token.clone(),
        revision: deployment.revision,
        bundle_digest: deployment.bundle_digest.clone(),
        resolved_release_digest: resolved_digest.clone(),
    })
    .await?
    .into_inner()
    .deployment
    .context("FinalizeDeployment omitted rollback deployment")?;
    anyhow::ensure!(
        finalized.resolved_release_digest == resolved_digest,
        "rollback finalization identity mismatch"
    );
    if args.exit_after_finalization {
        bail!("deterministic exit after inactive release finalization");
    }
    let mut slots = deployment
        .inventory
        .as_ref()
        .into_iter()
        .flat_map(|inventory| inventory.engines.iter())
        .map(|engine| engine.engine_slot.clone())
        .collect::<Vec<_>>();
    slots.sort();
    let canary = args
        .canary
        .unwrap_or_else(|| slots.first().cloned().unwrap_or_default());
    let operation = client::connect_operator(
        manager,
        wr_common::manager_client::RetryClass::DurableCreate,
    )
    .await?
    .submit_operation(SubmitOperationRequest {
        node_id: args.node_id,
        request_token: token,
        action: NodeOperationAction::Rollback as i32,
        engine_slots: slots,
        target_revision: deployment.revision,
        bundle_digest: deployment.bundle_digest,
        policy: Some(RolloutPolicy {
            max_unavailable: args.max_unavailable,
            canary_slot: canary,
            pause_after_canary: args.pause_after_canary,
            allow_downtime: args.allow_downtime,
            deadline_seconds: args.deadline,
        }),
        resolved_release_digest: resolved_digest,
    })
    .await?
    .into_inner()
    .operation
    .context("SubmitOperation omitted rollback operation")?;
    println!("Operation ID: {}", operation.operation_id);
    if args.no_wait {
        super::operations::render_operation(&operation, args.json)
    } else {
        super::operations::wait_for_terminal(
            manager,
            operation,
            Duration::from_secs(args.wait_timeout),
            args.json,
        )
        .await
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
            "operation_id" => "manager-derived deployment operation UUID",
            "revision_digest" => "manager-derived canonical revision digest",
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
    use crate::cmd::bundle_integrity::{ResolvedFile, RESOLVED_MANIFEST_VERSION};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn remote_resolved_verification_reads_root_owned_payloads_with_sudo() {
        let manifest = ResolvedReleaseManifest {
            version: RESOLVED_MANIFEST_VERSION,
            node_id: "node-a".into(),
            revision: 1,
            backend: "systemd".into(),
            bundle_digest: "sha256:bundle".into(),
            files: BTreeMap::from([(
                "config/engine.toml".into(),
                ResolvedFile {
                    sha256: "config-digest".into(),
                    mode: 0o644,
                },
            )]),
        };

        let command = remote_resolved_verification(
            "/opt/wruntime/wr-node/releases/.1.tmp",
            &manifest,
            "sha256:resolved",
        );
        assert!(command.contains("$(sudo cat /opt/wruntime/wr-node/releases/.1.tmp/bundle.sha256)"));
        assert!(command
            .contains("$(sudo cat /opt/wruntime/wr-node/releases/.1.tmp/resolved-release.sha256)"));
        assert!(command.contains(
            "$(sudo sha256sum /opt/wruntime/wr-node/releases/.1.tmp/config/engine.toml | cut -d' ' -f1)"
        ));
        assert!(command.contains(
            "$(sudo stat -c '%a' /opt/wruntime/wr-node/releases/.1.tmp/config/engine.toml)"
        ));
        assert!(command.contains("= '644'"));
        assert!(!command.contains("$(sha256sum"));
        assert!(!command.contains("$(stat"));
        assert_eq!(
            remote_release_hardening("/opt/wruntime/wr-node/releases/.1.tmp", "wruntime"),
            "sudo chown -R root:wruntime /opt/wruntime/wr-node/releases/.1.tmp && sudo chmod 755 /opt/wruntime/wr-node/releases/.1.tmp"
        );
    }

    #[test]
    fn protected_script_uses_profile_specific_host_credentials() {
        let script = include_str!("../../../dev/validate-deployment-lifecycle.sh");
        assert!(script.contains("cert issue proxy-peer-endpoint"));
        assert!(script.contains("cert issue proxy"));
        assert!(script.contains("cert issue node-agent"));
        assert!(script.contains("cert issue engine-admin-endpoint"));
        assert!(!script.contains("job-admin-delegation"));
    }

    #[test]
    fn node_tls_access_keeps_credentials_root_owned_and_workload_group_readable() {
        let profiles = node_tls_profiles(true);
        let command = node_tls_runtime_access_command(&profiles, "wruntime");
        assert!(command.contains("chown 'root:wruntime'"));
        assert!(command.contains("chmod 0750"));
        assert!(command.contains("chmod 0550"));
        assert!(command.contains("chmod 0440"));
        for profile in profiles {
            assert!(command.contains(&format!("/etc/wruntime/pki/{profile}/sets/v1")));
        }

        let command = node_tls_runtime_access_command(&node_tls_profiles(false), "wruntime");
        assert!(!command.contains("engine-admin-endpoint"));
    }

    fn temp_bundle_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("{name}-{}-{nanos}.tar.gz", std::process::id()))
    }

    #[test]
    fn engine_dockerfile_copies_only_present_optional_artifacts() {
        assert_eq!(
            engine_docker_extra_copies(false, false),
            vec![("modules/", "modules/")]
        );
        assert_eq!(
            engine_docker_extra_copies(true, true),
            vec![
                ("modules/", "modules/"),
                ("schemas/", "schemas/"),
                ("migrations/", "migrations/")
            ]
        );
    }

    struct ProductionBundleOutputs {
        archive: Vec<u8>,
        proxy_config: Vec<u8>,
        engine_config: Vec<u8>,
        agent_unit: Vec<u8>,
        release_metadata: Vec<u8>,
    }

    fn assemble_production_fixture(root: &Path, output: &Path) -> Result<ProductionBundleOutputs> {
        let binaries = root.join("bin");
        fs::create_dir_all(&binaries)?;
        for name in ["wr-proxy", "wr-engine", "wr-cli"] {
            fs::write(binaries.join(name), format!("fixture-{name}"))?;
        }
        let wasm = root.join("inventory.wasm");
        let cwasm = root.join("inventory.cwasm");
        let schema = root.join("inventory.binpb");
        let migrations = root.join("migrations");
        fs::write(&wasm, b"fixture-wasm")?;
        fs::write(&cwasm, b"fixture-cwasm")?;
        fs::write(&schema, b"fixture-schema")?;
        fs::create_dir_all(&migrations)?;
        fs::write(migrations.join("V1__fixture.sql"), b"SELECT 1;\n")?;
        let engine: EngineConfig = toml::from_str(&format!(
            r#"
listen_address = "127.0.0.1:9100"

[node]
proxy_address = "http://127.0.0.1:9001"
control_address = "http://127.0.0.1:9002"
peer_address = "https://127.0.0.1:9443"

[database]
url = "postgres://localhost/wruntime"

[job_admin]
listen_address = "0.0.0.0:9150"
advertise_address = "https://127.0.0.1:9150/"
queue_id = "fixture-jobs"

[job_admin.tls]
cert_path = "certs/job-admin.crt"
key_path = "certs/job-admin.key"
client_ca_cert_path = "certs/client-ca.crt"

[[module]]
name = "inventory"
namespace = "store"
version = "1.0.0"
wasm_path = {wasm:?}
schema_path = {schema:?}
migrations_path = {migrations:?}
"#,
            wasm = wasm.to_string_lossy(),
            schema = schema.to_string_lossy(),
            migrations = migrations.to_string_lossy(),
        ))?;
        let engine_configs = vec![("blue.toml".to_string(), engine)];
        let manifest = assemble_node_bundle(NodeBundleAssembly {
            output,
            target: "x86_64-unknown-linux-gnu",
            host_binary_dir: &binaries,
            workdir: "/opt/wruntime",
            image_prefix: "wr",
            peer_port: 9443,
            no_otel: false,
            source_proxy_config: None,
            engine_configs: &engine_configs,
            precompile_hash: Some("fixture-precompile-hash".into()),
        })?;
        verify_bundle_archive(
            output.to_str().context("fixture path is not UTF-8")?,
            &manifest,
        )?;
        let archive_path = output.to_str().context("fixture path is not UTF-8")?;
        for required in [
            "wr-node/bin/wr-proxy",
            "wr-node/bin/wr-engine",
            "wr-node/agent/wr-cli",
            "wr-node/agent/protocol-version",
            "wr-node/agent/wr-node-agent.service",
            "wr-node/config/proxy.toml",
            "wr-node/config/engine.toml",
            "wr-node/modules/inventory.wasm",
            "wr-node/modules/inventory.cwasm",
            "wr-node/schemas/inventory.binpb",
            "wr-node/migrations/inventory/V1__fixture.sql",
            "wr-node/systemd/wr-proxy.service",
            "wr-node/systemd/wr-engine-blue.service",
            "wr-node/docker/docker-compose.yml",
            "wr-node/release-metadata.json",
            "wr-node/manifest.json",
        ] {
            bundle::read_bytes_from_tarball(archive_path, required)
                .with_context(|| format!("production fixture omitted {required}"))?;
        }
        Ok(ProductionBundleOutputs {
            archive: fs::read(output)?,
            proxy_config: bundle::read_bytes_from_tarball(
                archive_path,
                "wr-node/config/proxy.toml",
            )?,
            engine_config: bundle::read_bytes_from_tarball(
                archive_path,
                "wr-node/config/engine.toml",
            )?,
            agent_unit: bundle::read_bytes_from_tarball(
                archive_path,
                "wr-node/agent/wr-node-agent.service",
            )?,
            release_metadata: bundle::read_bytes_from_tarball(
                archive_path,
                "wr-node/release-metadata.json",
            )?,
        })
    }

    #[test]
    fn complete_production_bundle_path_is_byte_deterministic() {
        let root = std::env::temp_dir().join(format!(
            "wr-production-bundle-determinism-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        fs::create_dir(&root).unwrap();
        let left = root.join("left.tar.gz");
        let right = root.join("right.tar.gz");
        let left_outputs = assemble_production_fixture(&root, &left).unwrap();
        let right_outputs = assemble_production_fixture(&root, &right).unwrap();
        assert_eq!(left_outputs.proxy_config, right_outputs.proxy_config);
        assert_eq!(left_outputs.engine_config, right_outputs.engine_config);
        assert_eq!(left_outputs.agent_unit, right_outputs.agent_unit);
        assert_eq!(
            left_outputs.release_metadata,
            right_outputs.release_metadata
        );
        let release_metadata: ReleaseMetadata =
            serde_json::from_slice(&left_outputs.release_metadata).unwrap();
        assert_eq!(
            release_metadata.proxy_lifecycle_address,
            "http://127.0.0.1:9002"
        );
        assert_eq!(
            release_metadata.slots[0].lifecycle_address,
            "http://127.0.0.1:9100"
        );
        assert_eq!(
            left_outputs.archive, right_outputs.archive,
            "production bundle archive bytes changed"
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn resolved_release_materialization_binds_operation_and_revision_identity() {
        let root = std::env::temp_dir().join(format!(
            "wr-resolved-release-identity-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        fs::create_dir(&root).unwrap();
        let bundle_path = root.join("node.tar.gz");
        assemble_production_fixture(&root, &bundle_path).unwrap();
        let bundle_path = bundle_path.to_str().unwrap();
        let manifest = bundle::read_manifest(bundle_path).unwrap();
        let configs = bundle::read_configs_from_tarball(bundle_path).unwrap();
        let revision_digest = format!("sha256:{}", "a".repeat(64));
        let operation_id =
            wr_common::deployment_contract::deployment_operation_id(&revision_digest).unwrap();

        let stage = materialize_resolved_release(
            bundle_path,
            &manifest,
            &configs,
            "node-a",
            7,
            &operation_id,
            &revision_digest,
            DeployFormat::Systemd,
            "postgres://localhost/wruntime",
            9443,
            "deploy@example.test",
            "192.0.2.10",
        )
        .unwrap();
        let engine = fs::read_to_string(stage.root.join("wr-node/config/engine.toml")).unwrap();
        assert!(!engine.contains("{operation_id}"));
        assert!(!engine.contains("{revision_digest}"));
        let engine: toml::Value = toml::from_str(&engine).unwrap();
        assert_eq!(
            engine["deployment"]["operation_id"].as_str(),
            Some(operation_id.as_str())
        );
        assert_eq!(
            engine["deployment"]["revision_digest"].as_str(),
            Some(revision_digest.as_str())
        );
        let expected = expected_engines(&manifest, "192.0.2.10").unwrap();
        assert_eq!(expected[0].job_queue_id, "fixture-jobs");
        assert_eq!(
            expected[0].job_admin_address,
            engine["job_admin"]["advertise_address"].as_str().unwrap()
        );
        assert_eq!(expected[0].job_admin_address, "https://192.0.2.10:9150/");
        let marker: serde_json::Value =
            serde_json::from_slice(&fs::read(stage.root.join("wr-node/deployment.json")).unwrap())
                .unwrap();
        assert_eq!(marker["operation_id"], operation_id);
        assert_eq!(marker["revision_digest"], revision_digest);

        let mismatch = materialize_resolved_release(
            bundle_path,
            &manifest,
            &configs,
            "node-a",
            7,
            "00000000-0000-8000-8000-000000000001",
            &revision_digest,
            DeployFormat::Systemd,
            "postgres://localhost/wruntime",
            9443,
            "deploy@example.test",
            "192.0.2.10",
        )
        .err()
        .expect("mismatched operation identity must fail");
        assert!(mismatch.to_string().contains("operation ID does not match"));

        let other_revision_digest = format!("sha256:{}", "b".repeat(64));
        let other_operation_id =
            wr_common::deployment_contract::deployment_operation_id(&other_revision_digest)
                .unwrap();
        let other = materialize_resolved_release(
            bundle_path,
            &manifest,
            &configs,
            "node-a",
            7,
            &other_operation_id,
            &other_revision_digest,
            DeployFormat::Systemd,
            "postgres://localhost/wruntime",
            9443,
            "deploy@example.test",
            "192.0.2.10",
        )
        .unwrap();
        assert_ne!(stage.digest, other.digest);
        drop((stage, other));
        fs::remove_dir_all(root).unwrap();
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
            secrets: Vec::new(),
            db_namespaces: Vec::new(),
            job_queue_id: String::new(),
            job_admin_address: String::new(),
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
    fn listener_validation_accepts_unresolved_revision_template() {
        let configs = vec![(
            "engine.toml".to_string(),
            r#"
listen_address = "127.0.0.1:9100"

[deployment]
node_id = "{node_id}"
revision = {revision}
bundle_digest = "{bundle_digest}"
engine_slot = "engine"
operation_id = "{operation_id}"
revision_digest = "{revision_digest}"
"#
            .to_string(),
        )];

        validate_deploy_listener_ports(&configs, 9443).unwrap();
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

[endpoint_tls]
cert_path = "certs/source-endpoint.crt"
key_path = "certs/source-endpoint.key"
client_ca_cert_path = "certs/source-client-root.crt"

[client_tls]
cert_path = "certs/source-client.crt"
key_path = "certs/source-client.key"
server_ca_cert_path = "certs/source-server-root.crt"

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
                    engine_listen_ports: &[9100],
                    proxy_control_port: control_port,
                    no_otel: false,
                },
            )?;
            tar.into_inner()?.finish()?;

            let release_metadata: ReleaseMetadata = serde_json::from_slice(
                &bundle::read_bytes_from_tarball(path.to_str().unwrap(), "release-metadata.json")?,
            )?;
            assert_eq!(
                release_metadata.proxy_lifecycle_address,
                "http://127.0.0.1:9102"
            );
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
                proxy_value["endpoint_tls"]["cert_path"].as_str(),
                Some("/etc/wruntime/pki/proxy-endpoint/sets/v1/leaf.pem")
            );
            assert_eq!(
                proxy_value["client_tls"]["cert_path"].as_str(),
                Some("/etc/wruntime/pki/proxy-client/sets/v1/leaf.pem")
            );
            assert!(proxy_value["node"].get("tls").is_none());

            let engine_dockerfile =
                bundle::read_file_from_tarball(path.to_str().unwrap(), "Dockerfile.engine-engine")?;
            assert!(!engine_dockerfile.contains("COPY schemas/ schemas/"));
            assert!(!engine_dockerfile.contains("COPY migrations/ migrations/"));
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
    fn cleanup_status_output_is_stable_for_humans_and_json() {
        let summary = NodeCleanupSummary {
            node_id: "node-a".into(),
            state: NodeCleanupState::Paused as i32,
            generation: 7,
            candidate_count: 2,
            inventory_count: 4,
            diagnostic_code: "QUERY_FAILED".into(),
            diagnostic_detail: "inventory unavailable".into(),
            ..Default::default()
        };
        assert_eq!(
            cleanup_output(&summary, false).unwrap(),
            "Node node-a cleanup=paused generation=7 candidates=2 inventory=4 diagnostic=QUERY_FAILED inventory unavailable"
        );
        let json: serde_json::Value =
            serde_json::from_str(&cleanup_output(&summary, true).unwrap()).unwrap();
        assert_eq!(json["node_id"], "node-a");
        assert_eq!(json["state"], "paused");
        assert_eq!(json["generation"], 7);
        assert_eq!(json["candidate_count"], 2);
        assert_eq!(json["inventory_count"], 4);
        assert_eq!(json["diagnostic_code"], "QUERY_FAILED");
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
