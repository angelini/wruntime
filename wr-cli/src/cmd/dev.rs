use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use wr_common::wruntime::{DeregisterEngineRequest, ListEnginesRequest};

use super::build_helpers::{self, BuildModule};
use super::config::EngineConfig;
use super::helpers;
use crate::client;

const PID_FILE_NAME: &str = ".wr-dev.pid";
const TEST_GUEST_MANIFEST: &str = "wr-tests/guests/build.toml";
const ECOMMERCE_ENGINE_CONFIGS: &[&str] = &[
    "examples/ecommerce/engine-client.toml",
    "examples/ecommerce/engine-inventory-1.toml",
    "examples/ecommerce/engine-inventory-2.toml",
];
const STOCKMARKET_ENGINE_CONFIGS: &[&str] = &[
    "examples/stockmarket/engine-exchange.toml",
    "examples/stockmarket/engine-ledger.toml",
    "examples/stockmarket/engine-simulator.toml",
];
const CODEGEN_ENGINE_CONFIGS: &[&str] = &["examples/codegen/engine.toml"];

/// Resolve the path to a prebuilt binary. Checks ./target/debug/ first,
/// then falls back to $PATH lookup.
fn resolve_binary(name: &str) -> String {
    let debug_path = format!("./target/debug/{name}");
    if Path::new(&debug_path).exists() {
        return debug_path;
    }
    name.to_string()
}

#[derive(Args)]
pub struct DevArgs {
    /// Directory containing dev lifecycle state such as the PID file
    #[arg(long, value_name = "DIR", default_value = ".", global = true)]
    pub state_dir: PathBuf,

    #[command(subcommand)]
    pub command: DevCommand,
}

#[derive(Subcommand)]
pub enum DevCommand {
    /// Start manager + proxy for local development
    Up {
        /// Path to manager config file
        #[arg(long, default_value = "examples/config/manager.toml")]
        manager_config: String,
        /// Path to proxy config file
        #[arg(long, default_value = "examples/config/proxy.toml")]
        proxy_config: String,
    },
    /// Stop all dev processes
    Down,
    /// Build WASM guests and schemas from build metadata
    Build(BuildArgs),
    /// Build WASM + schemas and (re)deploy an engine
    Deploy(DeployArgs),
    /// Show running dev processes and modules
    Status,
}

#[derive(Args)]
pub struct DeployArgs {
    /// Path to engine.toml config file
    config: String,
    /// Skip WASM and schema compilation (deploy only)
    #[arg(long)]
    skip_build: bool,
    /// Skip protoc schema compilation
    #[arg(long)]
    skip_schemas: bool,
    /// Only build/deploy the named module (repeatable)
    #[arg(long = "module", value_name = "NAME")]
    modules: Vec<String>,
}

#[derive(Args, Clone, Debug)]
pub struct BuildArgs {
    /// Build group: tests, ecommerce, stockmarket, codegen, or all
    #[arg(value_name = "GROUP")]
    group: Option<String>,
    /// Explicit engine TOML to use as build metadata (repeatable)
    #[arg(long = "config", value_name = "ENGINE_TOML")]
    configs: Vec<String>,
    /// Explicit build manifest to use as metadata (repeatable)
    #[arg(long = "manifest", value_name = "BUILD_MANIFEST")]
    manifests: Vec<String>,
    /// Skip protoc schema compilation
    #[arg(long)]
    skip_schemas: bool,
    /// Only build modules with this name (repeatable)
    #[arg(long = "module", value_name = "NAME")]
    modules: Vec<String>,
}

pub async fn run(args: DevArgs, manager: Option<&str>) -> Result<()> {
    let pid_file = args.state_dir.join(PID_FILE_NAME);
    match args.command {
        DevCommand::Up {
            manager_config,
            proxy_config,
        } => up(&manager_config, &proxy_config, &pid_file).await,
        DevCommand::Down => down(&pid_file).await,
        DevCommand::Build(build_args) => build(build_args),
        DevCommand::Deploy(deploy_args) => {
            let addr = manager.ok_or_else(|| {
                anyhow::anyhow!("--manager (or WR_MANAGER env var) is required for dev deploy")
            })?;
            deploy(deploy_args, addr, &pid_file).await
        }
        DevCommand::Status => {
            let addr = manager.ok_or_else(|| {
                anyhow::anyhow!("--manager (or WR_MANAGER env var) is required for dev status")
            })?;
            status(addr, &pid_file).await
        }
    }
}

// --- PID file helpers ---

struct PidEntry {
    role: String,
    pid: u32,
    config: Option<String>,
}

fn read_pid_file(pid_file: &Path) -> Result<Vec<PidEntry>> {
    if !pid_file.exists() {
        return Ok(vec![]);
    }
    let content = std::fs::read_to_string(pid_file)?;
    let mut entries = Vec::new();
    for line in content.lines() {
        let parts: Vec<&str> = line.splitn(3, ' ').collect();
        if parts.len() >= 2 {
            let pid: u32 = parts[1].parse().unwrap_or(0);
            if pid > 0 {
                entries.push(PidEntry {
                    role: parts[0].to_string(),
                    pid,
                    config: parts.get(2).map(|s| s.to_string()),
                });
            }
        }
    }
    Ok(entries)
}

fn write_pid_file(pid_file: &Path, entries: &[PidEntry]) -> Result<()> {
    let content: String = entries
        .iter()
        .map(|e| {
            if let Some(cfg) = &e.config {
                format!("{} {} {}", e.role, e.pid, cfg)
            } else {
                format!("{} {}", e.role, e.pid)
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    if let Some(parent) = pid_file.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    std::fs::write(
        pid_file,
        if content.is_empty() {
            content
        } else {
            content + "\n"
        },
    )?;
    Ok(())
}

fn is_process_alive(pid: u32) -> bool {
    unsafe { libc::kill(pid as i32, 0) == 0 }
}

fn kill_process(pid: u32) -> bool {
    unsafe { libc::kill(pid as i32, libc::SIGTERM) == 0 }
}

// --- Subcommands ---

/// Start a service process if not already running, or keep the existing PID entry.
async fn start_or_reuse_service(
    role: &str,
    binary_name: &str,
    config_path: &str,
    existing: &[PidEntry],
    entries: &mut Vec<PidEntry>,
) -> Result<()> {
    let alive = existing
        .iter()
        .any(|e| e.role == role && is_process_alive(e.pid));

    if alive {
        if let Some(e) = existing.iter().find(|e| e.role == role) {
            entries.push(PidEntry {
                role: e.role.clone(),
                pid: e.pid,
                config: e.config.clone(),
            });
        }
        return Ok(());
    }

    if !Path::new(config_path).exists() {
        bail!("{role} config not found: {config_path}");
    }

    let bin = resolve_binary(binary_name);
    let child = Command::new(&bin)
        .arg(config_path)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .with_context(|| format!("failed to start {bin}"))?;
    let pid = child.id();
    println!("{role:<8} pid={}  config={}", pid, config_path);

    let listen_addr = helpers::parse_listen_address(config_path)?;
    wait_for_port_or_exit(pid, &listen_addr, Duration::from_secs(30)).await?;

    entries.push(PidEntry {
        role: role.into(),
        pid,
        config: Some(config_path.to_string()),
    });
    Ok(())
}

async fn up(manager_config: &str, proxy_config: &str, pid_file: &Path) -> Result<()> {
    let existing = read_pid_file(pid_file).unwrap_or_default();
    let manager_alive = existing
        .iter()
        .any(|e| e.role == "manager" && is_process_alive(e.pid));
    let proxy_alive = existing
        .iter()
        .any(|e| e.role == "proxy" && is_process_alive(e.pid));

    if manager_alive && proxy_alive {
        println!("Dev infrastructure already running.");
        for e in &existing {
            if e.role == "manager" || e.role == "proxy" {
                println!("  {}  pid={}", e.role, e.pid);
            }
        }
        return Ok(());
    }

    let mut new_entries: Vec<PidEntry> = Vec::new();

    start_or_reuse_service(
        "manager",
        "wr-manager",
        manager_config,
        &existing,
        &mut new_entries,
    )
    .await?;
    start_or_reuse_service(
        "proxy",
        "wr-proxy",
        proxy_config,
        &existing,
        &mut new_entries,
    )
    .await?;

    // Keep any existing engine entries
    for e in &existing {
        if e.role == "engine" && is_process_alive(e.pid) {
            new_entries.push(PidEntry {
                role: e.role.clone(),
                pid: e.pid,
                config: e.config.clone(),
            });
        }
    }

    write_pid_file(pid_file, &new_entries)?;
    println!("Infrastructure ready.");
    Ok(())
}

async fn down(pid_file: &Path) -> Result<()> {
    let entries = read_pid_file(pid_file)?;
    if entries.is_empty() {
        println!("No dev processes running.");
        return Ok(());
    }

    // Kill engines first and wait for them to exit (so they can deregister
    // via the proxy before it goes away), then stop proxy + manager.
    for e in entries.iter().filter(|e| e.role == "engine") {
        if is_process_alive(e.pid) {
            if kill_process(e.pid) {
                println!("Stopped engine  pid={}", e.pid);
            } else {
                println!("Failed to stop engine  pid={}", e.pid);
            }
        } else {
            println!("engine  pid={}  (already dead)", e.pid);
        }
    }

    // Wait for all engines to finish deregistering before tearing down infra
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        let any_alive = entries
            .iter()
            .any(|e| e.role == "engine" && is_process_alive(e.pid));
        if !any_alive {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    for role in &["proxy", "manager"] {
        for e in entries.iter().filter(|e| e.role == *role) {
            if is_process_alive(e.pid) {
                if kill_process(e.pid) {
                    println!("Stopped {}  pid={}", e.role, e.pid);
                } else {
                    println!("Failed to stop {}  pid={}", e.role, e.pid);
                }
            } else {
                println!("{}  pid={}  (already dead)", e.role, e.pid);
            }
        }
    }

    // Clean up PID file
    let _ = std::fs::remove_file(pid_file);
    println!("All dev processes stopped.");
    Ok(())
}

async fn deploy(args: DeployArgs, manager: &str, pid_file: &Path) -> Result<()> {
    let config_path = &args.config;
    let config = EngineConfig::from_file(config_path)?;

    let modules_to_build: Vec<_> = if args.modules.is_empty() {
        config.modules.iter().collect()
    } else {
        config
            .modules
            .iter()
            .filter(|m| args.modules.contains(&m.name))
            .collect()
    };

    if modules_to_build.is_empty() && !args.modules.is_empty() {
        bail!(
            "No modules matched: {:?}. Available: {:?}",
            args.modules,
            config.modules.iter().map(|m| &m.name).collect::<Vec<_>>()
        );
    }

    if !args.skip_build {
        let build_modules: Vec<BuildModule> = modules_to_build
            .iter()
            .map(|m| BuildModule {
                name: m.name.clone(),
                wasm_path: m.wasm_path.clone(),
                schema_path: m.schema_path.clone().unwrap_or_default(),
                proto_path: None,
                cargo_dir: None,
            })
            .collect();

        // Step 1: Build schemas
        if !args.skip_schemas {
            build_helpers::compile_schemas(&build_modules)?;
        }

        // Step 2: Build WASM modules
        build_helpers::build_wasm_modules(&build_modules, false)?;
    }

    // Step 3: Stop old engine by matching listen_address
    let listen_addr = helpers::normalize_address(&config.listen_address);
    if let Ok(mut client) = client::connect(manager).await {
        if let Ok(resp) = client.list_engines(ListEnginesRequest {}).await {
            let engines = resp.into_inner().engines;
            for engine in &engines {
                if helpers::normalize_address(&engine.address) == listen_addr {
                    print!(
                        "[engine]  stopping old engine {} ... ",
                        &engine.engine_id[..8]
                    );
                    let _ = client
                        .deregister_engine(DeregisterEngineRequest {
                            engine_id: engine.engine_id.clone(),
                        })
                        .await;
                    println!("OK");
                }
            }
        }
    }

    // Also kill any tracked engine process for this config
    let mut entries = read_pid_file(pid_file).unwrap_or_default();
    for e in &entries {
        if e.role == "engine" && e.config.as_deref() == Some(config_path) && is_process_alive(e.pid)
        {
            kill_process(e.pid);
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    // Step 4: Start new engine
    let bin = resolve_binary("wr-engine");
    println!("[engine]  starting {bin} {config_path}");
    let child = Command::new(&bin)
        .arg(config_path)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .with_context(|| format!("failed to start {bin}"))?;
    let engine_pid = child.id();

    // Wait for registration, the post-load heartbeat, and manager health recomputation.
    println!("[engine]  waiting for route readiness...");
    let ready =
        helpers::wait_for_engine_ready(manager, &config.listen_address, Duration::from_secs(60))
            .await;

    if !ready {
        bail!("Engine did not become route-ready within 60 seconds");
    }

    // The proxy routing cache refreshes asynchronously after the manager table changes.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Update PID file
    entries.retain(|e| !(e.role == "engine" && e.config.as_deref() == Some(config_path)));
    entries.push(PidEntry {
        role: "engine".into(),
        pid: engine_pid,
        config: Some(config_path.to_string()),
    });
    write_pid_file(pid_file, &entries)?;

    // Print summary
    println!("[engine]  registered");
    println!();
    for module in &config.modules {
        println!(
            "  {}.{} v{}  ->  http://{}",
            module.namespace, module.name, module.version, listen_addr
        );
    }
    println!();
    println!("Ready.");
    Ok(())
}

fn build(args: BuildArgs) -> Result<()> {
    let modules = resolve_build_modules(&args)?;
    if !args.skip_schemas {
        build_helpers::compile_schemas(&modules)?;
    }
    build_helpers::build_wasm_modules(&modules, false)
}

fn resolve_build_modules(args: &BuildArgs) -> Result<Vec<BuildModule>> {
    let mut modules = Vec::new();
    if let Some(group) = &args.group {
        append_group_modules(group, &mut modules)?;
    }
    for config in &args.configs {
        append_config_modules(config, &mut modules)?;
    }
    for manifest in &args.manifests {
        append_manifest_modules(manifest, &mut modules)?;
    }
    if modules.is_empty() {
        bail!("no build modules requested; pass a group (tests, ecommerce, stockmarket, codegen, all), --config, or --manifest");
    }
    let modules = dedupe_build_modules(modules)?;
    filter_build_modules(modules, &args.modules)
}

fn append_group_modules(group: &str, modules: &mut Vec<BuildModule>) -> Result<()> {
    match group {
        "tests" => append_manifest_modules(TEST_GUEST_MANIFEST, modules),
        "ecommerce" => append_config_group(ECOMMERCE_ENGINE_CONFIGS, modules),
        "stockmarket" => append_config_group(STOCKMARKET_ENGINE_CONFIGS, modules),
        "codegen" => append_config_group(CODEGEN_ENGINE_CONFIGS, modules),
        "all" => {
            append_group_modules("tests", modules)?;
            append_group_modules("ecommerce", modules)?;
            append_group_modules("stockmarket", modules)?;
            append_group_modules("codegen", modules)
        }
        other => bail!("unknown build group '{other}'; expected tests, ecommerce, stockmarket, codegen, all, or use --config/--manifest"),
    }
}

fn append_config_group(configs: &[&str], modules: &mut Vec<BuildModule>) -> Result<()> {
    for config in configs {
        append_config_modules(config, modules)?;
    }
    Ok(())
}

fn resolve_metadata_path(path: &str) -> String {
    let direct = Path::new(path);
    if direct.exists() || direct.is_absolute() {
        return path.to_string();
    }
    if let Some(workspace_root) = Path::new(env!("CARGO_MANIFEST_DIR")).parent() {
        let candidate = workspace_root.join(path);
        if candidate.exists() {
            return candidate.to_string_lossy().to_string();
        }
    }
    path.to_string()
}

fn append_config_modules(path: &str, modules: &mut Vec<BuildModule>) -> Result<()> {
    let resolved_path = resolve_metadata_path(path);
    let config = EngineConfig::from_file(&resolved_path)
        .with_context(|| format!("failed to resolve build modules from engine config: {path}"))?;
    modules.extend(config.modules.into_iter().map(|m| BuildModule {
        name: m.name,
        wasm_path: m.wasm_path,
        schema_path: m.schema_path.unwrap_or_default(),
        proto_path: None,
        cargo_dir: None,
    }));
    Ok(())
}

fn append_manifest_modules(path: &str, modules: &mut Vec<BuildModule>) -> Result<()> {
    let resolved_path = resolve_metadata_path(path);
    let loaded = build_helpers::load_manifest(&resolved_path)
        .with_context(|| format!("failed to resolve build modules from manifest: {path}"))?;
    modules.extend(loaded);
    Ok(())
}

fn dedupe_build_modules(modules: Vec<BuildModule>) -> Result<Vec<BuildModule>> {
    let mut by_wasm: BTreeMap<String, BuildModule> = BTreeMap::new();
    for module in modules {
        if module.name.is_empty() {
            bail!(
                "build metadata contains a module with an empty name for wasm_path '{}'",
                module.wasm_path
            );
        }
        if module.wasm_path.is_empty() {
            bail!(
                "build metadata for module '{}' has an empty wasm_path",
                module.name
            );
        }
        match by_wasm.get_mut(&module.wasm_path) {
            None => {
                by_wasm.insert(module.wasm_path.clone(), module);
            }
            Some(existing) => {
                if existing.schema_path.is_empty() && !module.schema_path.is_empty() {
                    existing.schema_path = module.schema_path.clone();
                    existing.proto_path = module.proto_path.clone();
                } else if !module.schema_path.is_empty()
                    && existing.schema_path != module.schema_path
                {
                    bail!(
                        "conflicting schema paths for wasm '{}': '{}' vs '{}'",
                        module.wasm_path,
                        existing.schema_path,
                        module.schema_path
                    );
                }
                if existing.cargo_dir.is_none() {
                    existing.cargo_dir = module.cargo_dir.clone();
                } else if module.cargo_dir.is_some() && existing.cargo_dir != module.cargo_dir {
                    bail!(
                        "conflicting cargo directories for wasm '{}': {:?} vs {:?}",
                        module.wasm_path,
                        existing.cargo_dir,
                        module.cargo_dir
                    );
                }
            }
        }
    }
    Ok(by_wasm.into_values().collect())
}

fn filter_build_modules(
    modules: Vec<BuildModule>,
    requested: &[String],
) -> Result<Vec<BuildModule>> {
    if requested.is_empty() {
        return Ok(modules);
    }
    let requested: BTreeSet<&str> = requested.iter().map(String::as_str).collect();
    let available: BTreeSet<String> = modules.iter().map(|m| m.name.clone()).collect();
    let filtered: Vec<BuildModule> = modules
        .into_iter()
        .filter(|m| requested.contains(m.name.as_str()))
        .collect();
    if filtered.is_empty() {
        bail!(
            "no modules matched {:?}; available modules: {:?}",
            requested,
            available
        );
    }
    Ok(filtered)
}

async fn status(manager: &str, pid_file: &Path) -> Result<()> {
    let entries = read_pid_file(pid_file)?;
    if entries.is_empty() {
        println!("No dev processes tracked. Run `wr dev up` to start.");
        return Ok(());
    }

    for e in &entries {
        let alive = is_process_alive(e.pid);
        let status_str = if alive { "UP" } else { "DOWN" };
        let config_str = e
            .config
            .as_deref()
            .map(|c| format!("  ({c})"))
            .unwrap_or_default();
        println!("{:<9} pid={:<8} {}{config_str}", e.role, e.pid, status_str);
    }

    // Try to show registered modules from manager
    if let Ok(mut client) = client::connect(manager).await {
        if let Ok(resp) = client.list_engines(ListEnginesRequest {}).await {
            let engines = resp.into_inner().engines;
            if !engines.is_empty() {
                println!();
                println!("Registered modules:");
                for engine in &engines {
                    for module in &engine.modules {
                        println!(
                            "  {}.{} v{}  (engine {})",
                            module.namespace,
                            module.name,
                            module.version,
                            &engine.engine_id[..8]
                        );
                    }
                }
            }
        }
    }

    Ok(())
}

// --- Helpers ---

/// Wait for a TCP port to accept connections, or bail if the process exits early.
async fn wait_for_port_or_exit(pid: u32, addr: &str, timeout: Duration) -> Result<()> {
    let connect_addr = addr.replace("0.0.0.0", "127.0.0.1");
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if !is_process_alive(pid) {
            bail!(
                "Process (pid {pid}) exited before listening on {connect_addr} — check logs above"
            );
        }
        if tokio::net::TcpStream::connect(&connect_addr).await.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    bail!("Timed out waiting for {connect_addr} to accept connections");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_temp_state_dir(label: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("wr-cli-{label}-{}-{nanos}", std::process::id()))
    }

    fn build_args(group: Option<&str>) -> BuildArgs {
        BuildArgs {
            group: group.map(str::to_string),
            configs: vec![],
            manifests: vec![],
            skip_schemas: false,
            modules: vec![],
        }
    }

    #[test]
    fn pid_file_helpers_use_supplied_state_path() {
        let dir_a = unique_temp_state_dir("pid-a");
        let dir_b = unique_temp_state_dir("pid-b");
        let pid_file_a = dir_a.join(PID_FILE_NAME);
        let pid_file_b = dir_b.join(PID_FILE_NAME);

        write_pid_file(
            &pid_file_a,
            &[PidEntry {
                role: "manager".into(),
                pid: 12345,
                config: Some("manager.toml".into()),
            }],
        )
        .unwrap();

        let entries = read_pid_file(&pid_file_a).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].role, "manager");
        assert_eq!(entries[0].pid, 12345);
        assert_eq!(entries[0].config.as_deref(), Some("manager.toml"));
        assert!(read_pid_file(&pid_file_b).unwrap().is_empty());

        let _ = std::fs::remove_dir_all(&dir_a);
        let _ = std::fs::remove_dir_all(&dir_b);
    }

    #[test]
    fn dev_args_accept_state_dir_before_subcommand() {
        use clap::Parser as _;

        #[derive(clap::Parser)]
        struct TestCli {
            #[command(flatten)]
            dev: DevArgs,
        }

        let parsed =
            TestCli::try_parse_from(["test", "--state-dir", "/tmp/wr-run", "down"]).unwrap();
        assert_eq!(parsed.dev.state_dir, PathBuf::from("/tmp/wr-run"));
        assert!(matches!(parsed.dev.command, DevCommand::Down));
    }

    #[test]
    fn build_group_rejects_unknown_group() {
        let args = BuildArgs {
            group: Some("bogus".into()),
            configs: vec![],
            manifests: vec![],
            skip_schemas: false,
            modules: vec![],
        };
        let err = resolve_build_modules(&args).expect_err("unknown group must be rejected");
        assert!(
            format!("{err:#}").contains("unknown build group 'bogus'"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn tests_group_resolves_explicit_manifest() {
        let modules = resolve_build_modules(&build_args(Some("tests"))).unwrap();
        let names: BTreeSet<String> = modules.iter().map(|m| m.name.clone()).collect();
        assert_eq!(modules.len(), 5);
        assert_eq!(
            names,
            BTreeSet::from([
                "db-guest".to_string(),
                "tracing-guest".to_string(),
                "blobstore-guest".to_string(),
                "http-guest".to_string(),
                "llm-guest".to_string(),
            ])
        );
        assert!(modules.iter().all(|m| m.proto_path.is_some()));
        assert!(modules.iter().all(|m| m.cargo_dir.is_some()));
    }

    #[test]
    fn stockmarket_group_does_not_select_duplicate_schema_ledger_crate() {
        let modules = resolve_build_modules(&build_args(Some("stockmarket"))).unwrap();
        let names: BTreeSet<String> = modules.iter().map(|m| m.name.clone()).collect();
        assert_eq!(
            names,
            BTreeSet::from([
                "exchange".to_string(),
                "ledger".to_string(),
                "simulator".to_string(),
            ])
        );
        assert!(modules
            .iter()
            .all(|m| !m.wasm_path.contains("examples/stockmarket/schemas/ledger")));
        assert!(modules.iter().all(|m| !m
            .cargo_dir
            .as_deref()
            .unwrap_or_default()
            .contains("examples/stockmarket/schemas/ledger")));
    }

    #[test]
    fn explicit_missing_config_reports_path() {
        let args = BuildArgs {
            group: None,
            configs: vec!["does/not/exist.toml".into()],
            manifests: vec![],
            skip_schemas: false,
            modules: vec![],
        };
        let err = resolve_build_modules(&args).expect_err("missing config must be rejected");
        assert!(
            format!("{err:#}").contains(
                "failed to resolve build modules from engine config: does/not/exist.toml"
            ),
            "unexpected error: {err:#}"
        );
    }
}
