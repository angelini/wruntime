use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use wr_common::wruntime::{DeregisterEngineRequest, ListEnginesRequest};

use super::build_helpers::{self, BuildModule};
use super::config::EngineConfig;
use super::helpers;
use crate::client;

const PID_FILE: &str = ".wr-dev.pid";

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

pub async fn run(args: DevArgs, manager: Option<&str>) -> Result<()> {
    match args.command {
        DevCommand::Up {
            manager_config,
            proxy_config,
        } => up(&manager_config, &proxy_config).await,
        DevCommand::Down => down().await,
        DevCommand::Deploy(deploy_args) => {
            let addr = manager.ok_or_else(|| {
                anyhow::anyhow!("--manager (or WR_MANAGER env var) is required for dev deploy")
            })?;
            deploy(deploy_args, addr).await
        }
        DevCommand::Status => {
            let addr = manager.ok_or_else(|| {
                anyhow::anyhow!("--manager (or WR_MANAGER env var) is required for dev status")
            })?;
            status(addr).await
        }
    }
}

// --- PID file helpers ---

struct PidEntry {
    role: String,
    pid: u32,
    config: Option<String>,
}

fn read_pid_file() -> Result<Vec<PidEntry>> {
    let path = Path::new(PID_FILE);
    if !path.exists() {
        return Ok(vec![]);
    }
    let content = std::fs::read_to_string(path)?;
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

fn write_pid_file(entries: &[PidEntry]) -> Result<()> {
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
    std::fs::write(
        PID_FILE,
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

async fn up(manager_config: &str, proxy_config: &str) -> Result<()> {
    let existing = read_pid_file().unwrap_or_default();
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

    write_pid_file(&new_entries)?;
    println!("Infrastructure ready.");
    Ok(())
}

async fn down() -> Result<()> {
    let entries = read_pid_file()?;
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
    let _ = std::fs::remove_file(PID_FILE);
    println!("All dev processes stopped.");
    Ok(())
}

async fn deploy(args: DeployArgs, manager: &str) -> Result<()> {
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
                schema_path: m.schema_path.clone(),
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
    let mut entries = read_pid_file().unwrap_or_default();
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

    // Wait for engine to register
    println!("[engine]  waiting for registration...");
    let registered = helpers::wait_for_engine_at_address(
        manager,
        &config.listen_address,
        Duration::from_secs(60),
    )
    .await;

    if !registered {
        bail!("Engine did not register within 60 seconds");
    }

    // Update PID file
    entries.retain(|e| !(e.role == "engine" && e.config.as_deref() == Some(config_path)));
    entries.push(PidEntry {
        role: "engine".into(),
        pid: engine_pid,
        config: Some(config_path.to_string()),
    });
    write_pid_file(&entries)?;

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

async fn status(manager: &str) -> Result<()> {
    let entries = read_pid_file()?;
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
