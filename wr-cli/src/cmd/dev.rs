use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use wr_common::wruntime::{DeregisterEngineRequest, ListEnginesRequest};

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

pub async fn run(args: DevArgs, manager: &str) -> Result<()> {
    match args.command {
        DevCommand::Up {
            manager_config,
            proxy_config,
        } => up(&manager_config, &proxy_config).await,
        DevCommand::Down => down().await,
        DevCommand::Deploy(deploy_args) => deploy(deploy_args, manager).await,
        DevCommand::Status => status(manager).await,
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
    // kill -0 checks if process exists without sending a signal
    unsafe { libc::kill(pid as i32, 0) == 0 }
}

fn kill_process(pid: u32) -> bool {
    unsafe { libc::kill(pid as i32, libc::SIGTERM) == 0 }
}

// --- Subcommands ---

async fn up(manager_config: &str, proxy_config: &str) -> Result<()> {
    // Check if already running
    let entries = read_pid_file().unwrap_or_default();
    let manager_alive = entries
        .iter()
        .any(|e| e.role == "manager" && is_process_alive(e.pid));
    let proxy_alive = entries
        .iter()
        .any(|e| e.role == "proxy" && is_process_alive(e.pid));

    if manager_alive && proxy_alive {
        println!("Dev infrastructure already running.");
        for e in &entries {
            if e.role == "manager" || e.role == "proxy" {
                println!("  {}  pid={}", e.role, e.pid);
            }
        }
        return Ok(());
    }

    // Validate config files exist
    if !Path::new(manager_config).exists() {
        bail!("Manager config not found: {manager_config}");
    }
    if !Path::new(proxy_config).exists() {
        bail!("Proxy config not found: {proxy_config}");
    }

    let mut new_entries: Vec<PidEntry> = Vec::new();

    // Start manager
    if !manager_alive {
        let bin = resolve_binary("wr-manager");
        let child = Command::new(&bin)
            .arg(manager_config)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .with_context(|| format!("failed to start {bin}"))?;
        let pid = child.id();
        println!("manager  pid={}  config={}", pid, manager_config);
        new_entries.push(PidEntry {
            role: "manager".into(),
            pid,
            config: Some(manager_config.to_string()),
        });

        // Wait for manager to be ready
        println!("Waiting for manager to be ready...");
        let manager_addr = parse_manager_listen_addr(manager_config)?;
        wait_for_port_or_exit(pid, &manager_addr, Duration::from_secs(30)).await?;
    } else {
        // Keep existing manager entry
        if let Some(e) = entries.iter().find(|e| e.role == "manager") {
            new_entries.push(PidEntry {
                role: e.role.clone(),
                pid: e.pid,
                config: e.config.clone(),
            });
        }
    }

    // Start proxy
    if !proxy_alive {
        let bin = resolve_binary("wr-proxy");
        let proxy_listen = parse_proxy_listen_addr(proxy_config)?;
        let child = Command::new(&bin)
            .arg(proxy_config)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .with_context(|| format!("failed to start {bin}"))?;
        let pid = child.id();
        println!("proxy    pid={}  config={}", pid, proxy_config);

        // Wait for proxy to be ready
        wait_for_port_or_exit(pid, &proxy_listen, Duration::from_secs(30)).await?;

        new_entries.push(PidEntry {
            role: "proxy".into(),
            pid,
            config: Some(proxy_config.to_string()),
        });
    } else if let Some(e) = entries.iter().find(|e| e.role == "proxy") {
        new_entries.push(PidEntry {
            role: e.role.clone(),
            pid: e.pid,
            config: e.config.clone(),
        });
    }

    // Keep any existing engine entries
    for e in &entries {
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

    // Kill in reverse order: engines first, then proxy, then manager
    for role in &["engine", "proxy", "manager"] {
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
    if !Path::new(config_path).exists() {
        bail!("Engine config not found: {config_path}");
    }

    // Parse engine.toml without full validation (artifacts may not exist yet)
    let content = std::fs::read_to_string(config_path)
        .with_context(|| format!("failed to read config: {config_path}"))?;
    let config: DevEngineConfig =
        toml::from_str(&content).context("failed to parse engine config")?;

    let modules_to_build: Vec<&DevModuleConfig> = if args.modules.is_empty() {
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
        // Step 1: Build schemas
        if !args.skip_schemas {
            for module in &modules_to_build {
                if module.schema_path.is_empty() {
                    continue;
                }
                let proto_path = derive_proto_path(&module.schema_path);
                if !Path::new(&proto_path).exists() {
                    bail!(
                        "Proto file not found for module '{}': {} (derived from schema_path '{}')",
                        module.name,
                        proto_path,
                        module.schema_path,
                    );
                }
                let proto_dir = Path::new(&proto_path)
                    .parent()
                    .map(|p| p.to_string_lossy().to_string())
                    .unwrap_or_default();
                print!("[schema]  {} ... ", module.schema_path);
                let status = Command::new("protoc")
                    .args([
                        &format!("--descriptor_set_out={}", module.schema_path),
                        "--include_imports",
                        &format!("--proto_path={}", proto_dir),
                        &proto_path,
                    ])
                    .status()
                    .context("failed to run protoc")?;
                if !status.success() {
                    bail!("protoc failed for module '{}'", module.name);
                }
                println!("OK");
            }
        }

        // Step 2: Build WASM modules
        for module in &modules_to_build {
            let cargo_dir = derive_cargo_dir(&module.wasm_path)?;
            print!("[build]   {} ... ", cargo_dir.display());
            let output = Command::new("cargo")
                .args([
                    "component",
                    "build",
                    "--release",
                    "--target",
                    "wasm32-wasip2",
                ])
                .current_dir(&cargo_dir)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output()
                .context("failed to run cargo component build")?;
            if !output.status.success() {
                println!("FAILED");
                let stderr = String::from_utf8_lossy(&output.stderr);
                eprintln!("{stderr}");
                bail!("cargo component build failed for module '{}'", module.name);
            }
            println!("OK");
        }
    }

    // Step 3: Stop old engine by matching listen_address
    let listen_addr = normalize_address(&config.listen_address);
    if let Ok(mut client) = client::connect(manager).await {
        if let Ok(resp) = client.list_engines(ListEnginesRequest {}).await {
            let engines = resp.into_inner().engines;
            for engine in &engines {
                if normalize_address(&engine.address) == listen_addr {
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
            // Give it a moment to shut down
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

    // Wait for engine to register (poll manager)
    println!("[engine]  waiting for registration...");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut registered = false;
    while tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if let Ok(mut client) = client::connect(manager).await {
            if let Ok(resp) = client.list_engines(ListEnginesRequest {}).await {
                let engines = resp.into_inner().engines;
                if engines
                    .iter()
                    .any(|e| normalize_address(&e.address) == listen_addr)
                {
                    registered = true;
                    break;
                }
            }
        }
    }

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

/// Minimal engine config for parsing without validation
#[derive(serde::Deserialize)]
struct DevEngineConfig {
    listen_address: String,
    #[serde(rename = "module", default)]
    modules: Vec<DevModuleConfig>,
}

#[derive(serde::Deserialize)]
struct DevModuleConfig {
    name: String,
    namespace: String,
    version: String,
    wasm_path: String,
    #[serde(default)]
    schema_path: String,
}

/// Derive .proto path from .binpb schema_path
fn derive_proto_path(schema_path: &str) -> String {
    if schema_path.ends_with(".binpb") {
        format!("{}proto", &schema_path[..schema_path.len() - 5])
    } else {
        format!("{schema_path}.proto")
    }
}

/// Derive Cargo project directory from wasm_path by finding the `target/` component
fn derive_cargo_dir(wasm_path: &str) -> Result<PathBuf> {
    let path = Path::new(wasm_path);
    let mut current = path;
    while let Some(parent) = current.parent() {
        if current.file_name().map(|n| n == "target").unwrap_or(false) {
            return Ok(parent.to_path_buf());
        }
        current = parent;
    }
    bail!(
        "Cannot derive Cargo project directory from wasm_path: {wasm_path}. \
         Expected a path containing 'target/' (e.g., my-module/target/wasm32-wasip2/release/mod.wasm)"
    );
}

/// Normalize address: replace 0.0.0.0 with 127.0.0.1 for comparison
fn normalize_address(addr: &str) -> String {
    let addr = addr
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    addr.replace("0.0.0.0", "127.0.0.1").to_string()
}

/// Parse listen_address from a manager config file
fn parse_manager_listen_addr(config_path: &str) -> Result<String> {
    let content = std::fs::read_to_string(config_path)?;
    let config: toml::Value = toml::from_str(&content)?;
    config
        .get("listen_address")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow::anyhow!("no listen_address in manager config"))
}

/// Parse listen_address from a proxy config file
fn parse_proxy_listen_addr(config_path: &str) -> Result<String> {
    let content = std::fs::read_to_string(config_path)?;
    let config: toml::Value = toml::from_str(&content)?;
    config
        .get("listen_address")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow::anyhow!("no listen_address in proxy config"))
}

/// Wait for a TCP port to accept connections, or bail if the process exits early.
/// Normalizes 0.0.0.0 to 127.0.0.1 since you can't connect to the bind-any address.
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
