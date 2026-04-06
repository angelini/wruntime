//! Shared CLI helpers used by both `dev` and `node` commands.

use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{bail, Result};
use wr_common::wruntime::ListEnginesRequest;

use crate::client;

static VERBOSE: AtomicBool = AtomicBool::new(false);

/// Enable verbose debug output for deploy helpers.
pub fn set_verbose(enabled: bool) {
    VERBOSE.store(enabled, Ordering::Relaxed);
}

fn verbose() -> bool {
    VERBOSE.load(Ordering::Relaxed)
}

/// Print a debug message when verbose mode is enabled.
macro_rules! debug {
    ($($arg:tt)*) => {
        if $crate::cmd::helpers::verbose() {
            eprintln!("[debug]  {}", format!($($arg)*));
        }
    };
}
#[allow(unused_imports)]
pub(crate) use debug;

/// Normalize a listen address for comparison: strip scheme, replace 0.0.0.0 with 127.0.0.1.
pub fn normalize_address(addr: &str) -> String {
    let addr = addr
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    addr.replace("0.0.0.0", "127.0.0.1")
}

/// Extract the port number from an address string like "0.0.0.0:9001".
pub fn extract_port(addr: &str) -> u16 {
    addr.rsplit(':')
        .next()
        .and_then(|p| p.parse().ok())
        .unwrap_or(0)
}

/// Parse the `listen_address` field from a TOML config file.
pub fn parse_listen_address(config_path: &str) -> Result<String> {
    let content = std::fs::read_to_string(config_path)?;
    let config: toml::Value = toml::from_str(&content)?;
    config
        .get("listen_address")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow::anyhow!("no listen_address in {config_path}"))
}

/// Run a command (given as a slice of args) and bail on failure.
pub fn run_command(args: &[String]) -> Result<()> {
    debug!("exec: {}", args.join(" "));
    let output = Command::new(&args[0])
        .args(&args[1..])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| format!("failed to run {}", args[0]))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        debug!(
            "exit code {:?}, stderr: {}",
            output.status.code(),
            stderr.trim()
        );
        if !stderr.is_empty() {
            eprintln!("{stderr}");
        }
        bail!(
            "{} failed with exit code {:?}",
            args[0],
            output.status.code()
        );
    }
    debug!("exit code 0");
    Ok(())
}

use anyhow::Context;

/// Build the base SSH argument list from remote, key, and port.
/// When `ssh_port` is `None`, no `-p` flag is emitted so the SSH config default applies.
pub fn build_ssh_args(remote: &str, ssh_key: Option<&str>, ssh_port: Option<u16>) -> Vec<String> {
    let mut args = vec!["ssh".to_string()];
    if let Some(key) = ssh_key {
        args.extend(["-i".to_string(), key.to_string()]);
    }
    if let Some(port) = ssh_port {
        args.extend(["-p".to_string(), port.to_string()]);
    }
    args.push(remote.to_string());
    args
}

/// Run a command over SSH.
pub fn run_ssh(ssh_base: &[String], command: &str) -> Result<()> {
    let mut args = ssh_base.to_vec();
    args.push(command.to_string());
    run_command(&args)
}

/// Run a command over SSH with output streamed directly to the terminal.
/// Blocks until the remote command exits or the process receives SIGINT.
pub fn run_ssh_streaming(ssh_base: &[String], command: &str) -> Result<()> {
    let mut args = ssh_base.to_vec();
    args.push(command.to_string());
    let status = Command::new(&args[0])
        .args(&args[1..])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .with_context(|| format!("failed to run {}", args[0]))?;
    if !status.success() {
        bail!("{} exited with code {:?}", args[0], status.code());
    }
    Ok(())
}

/// Get the current timestamp from the remote host in `YYYY-MM-DD HH:MM:SS` format.
/// Used to anchor log queries to the remote clock rather than the local one.
pub fn get_remote_timestamp(ssh_base: &[String]) -> Result<String> {
    let mut args = ssh_base.to_vec();
    args.push("date '+%Y-%m-%d %H:%M:%S'".to_string());
    let output = Command::new(&args[0])
        .args(&args[1..])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .context("failed to get remote timestamp")?;
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Like `run_ssh_streaming` but ignores non-zero exit codes and prefixes each
/// output line with `prefix` (e.g. journalctl returning 1 when no entries match).
pub fn run_ssh_prefixed_best_effort(ssh_base: &[String], command: &str, prefix: &str) {
    let mut args = ssh_base.to_vec();
    args.push(command.to_string());
    let output = Command::new(&args[0])
        .args(&args[1..])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output();
    if let Ok(out) = output {
        let text = String::from_utf8_lossy(&out.stdout);
        for line in text.lines() {
            println!("{prefix}{line}");
        }
        let err = String::from_utf8_lossy(&out.stderr);
        for line in err.lines() {
            eprintln!("{prefix}{line}");
        }
    }
}

/// Spawn an SSH command in the background, prefixing each stdout line with `prefix`.
/// Returns a handle that kills the child and cancels the reader task on drop.
pub fn spawn_ssh_prefixed(
    ssh_base: &[String],
    command: &str,
    prefix: &'static str,
) -> Result<PrefixedTail> {
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::process::Command as TokioCommand;

    let mut args = ssh_base.to_vec();
    args.push(command.to_string());
    debug!("spawn background: {}", args.join(" "));
    let mut child = TokioCommand::new(&args[0])
        .args(&args[1..])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("failed to spawn {}", args[0]))?;

    let stdout = child.stdout.take().expect("stdout piped");
    let stderr = child.stderr.take().expect("stderr piped");
    let stdout_task = tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            println!("{prefix}{line}");
        }
    });
    let stderr_task = tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            eprintln!("{prefix}{line}");
        }
    });

    Ok(PrefixedTail {
        child,
        _stdout_task: stdout_task,
        _stderr_task: stderr_task,
    })
}

/// Handle for a background prefixed SSH tail. Kills the child on drop.
pub struct PrefixedTail {
    child: tokio::process::Child,
    _stdout_task: tokio::task::JoinHandle<()>,
    _stderr_task: tokio::task::JoinHandle<()>,
}

impl Drop for PrefixedTail {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

/// Poll the manager until an engine at the given address registers or timeout.
/// Returns `true` if the engine was detected.
pub async fn wait_for_engine_at_address(
    manager: &str,
    listen_addr: &str,
    timeout: Duration,
) -> bool {
    use tokio_retry::strategy::FixedInterval;
    use tokio_retry::Retry;

    let normalized = normalize_address(listen_addr);
    debug!("polling manager {manager} for engine at {listen_addr} (normalized: {normalized}, timeout {}s)", timeout.as_secs());
    let attempt = std::sync::atomic::AtomicU32::new(0);
    let normalized = &normalized;
    let strategy = FixedInterval::from_millis(1000).take(timeout.as_secs() as usize);
    Retry::spawn(strategy, || {
        let n = attempt.fetch_add(1, Ordering::Relaxed) + 1;
        async move {
            match client::connect(manager).await {
                Ok(mut client) => match client.list_engines(ListEnginesRequest {}).await {
                    Ok(resp) => {
                        let engines = resp.into_inner().engines;
                        let addrs: Vec<_> = engines.iter().map(|e| e.address.as_str()).collect();
                        debug!("attempt {n}: ListEngines OK, engines: {addrs:?}");
                        if engines
                            .iter()
                            .any(|e| normalize_address(&e.address) == *normalized)
                        {
                            return Ok(());
                        }
                        Err(())
                    }
                    Err(e) => {
                        debug!("attempt {n}: ListEngines RPC failed: {e}");
                        Err(())
                    }
                },
                Err(e) => {
                    debug!("attempt {n}: connection to {manager} failed: {e}");
                    Err(())
                }
            }
        }
    })
    .await
    .is_ok()
}

/// Poll a manager gRPC address until it responds to ListManagers or timeout.
/// Returns `true` if the manager became reachable.
pub async fn wait_for_manager_ready(manager_addr: &str, timeout: Duration) -> bool {
    use tokio_retry::strategy::FixedInterval;
    use tokio_retry::Retry;

    debug!(
        "polling manager at {manager_addr} (timeout {}s)",
        timeout.as_secs()
    );
    let attempt = std::sync::atomic::AtomicU32::new(0);
    let strategy = FixedInterval::from_millis(2000).take(timeout.as_secs() as usize / 2);
    Retry::spawn(strategy, || {
        let n = attempt.fetch_add(1, Ordering::Relaxed) + 1;
        async move {
            debug!("attempt {n}: connecting to {manager_addr}");
            match client::connect(manager_addr).await {
                Ok(mut c) => match c
                    .list_managers(wr_common::wruntime::ListManagersRequest {})
                    .await
                {
                    Ok(resp) => {
                        let count = resp.into_inner().managers.len();
                        debug!("attempt {n}: ListManagers OK ({count} managers)");
                        Ok(())
                    }
                    Err(e) => {
                        debug!("attempt {n}: ListManagers RPC failed: {e}");
                        Err(())
                    }
                },
                Err(e) => {
                    debug!("attempt {n}: connection failed: {e}");
                    Err(())
                }
            }
        }
    })
    .await
    .is_ok()
}

/// Extract the host portion from a `user@host` remote string.
pub fn extract_remote_host(remote: &str) -> &str {
    remote.split('@').next_back().unwrap_or(remote)
}

/// Extract the user portion from a `user@host` remote string.
/// Returns `None` if no `@` is present.
pub fn extract_remote_user(remote: &str) -> Option<&str> {
    if remote.contains('@') {
        remote.split('@').next()
    } else {
        None
    }
}

/// Resolve the routable IP address of a remote host.
///
/// If the remote is already in `user@<ip>` or bare `<ip>` form, returns the IP directly.
/// Otherwise (e.g. an SSH config alias), SSHes to the host and runs `hostname -I` to
/// discover its primary IP address.
pub fn resolve_remote_ip(ssh_base: &[String], remote: &str) -> Result<String> {
    let host = extract_remote_host(remote);
    // If it already looks like an IP address, use it directly
    if host.parse::<std::net::IpAddr>().is_ok() {
        return Ok(host.to_string());
    }
    // SSH to the host and resolve its IP
    let mut args = ssh_base.to_vec();
    args.push("hostname -I".to_string());
    let output = Command::new(&args[0])
        .args(&args[1..])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| format!("failed to resolve IP for {remote}"))?;
    if !output.status.success() {
        bail!(
            "failed to resolve IP for {remote}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let ip = String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_string();
    if ip.is_empty() {
        bail!("could not determine IP address for {remote} — pass --advertise-address explicitly");
    }
    println!("[deploy]  resolved {remote} -> {ip}");
    Ok(ip)
}

/// Resolve `{key}` placeholders in a config template string.
/// Bails if any `{...}` placeholder remains unresolved.
pub fn resolve_template(
    template: &str,
    vars: &std::collections::HashMap<&str, &str>,
) -> Result<String> {
    let mut result = template.to_string();
    for (key, value) in vars {
        let placeholder = format!("{{{key}}}");
        result = result.replace(&placeholder, value);
    }

    // Scan for unresolved placeholders
    let bytes = result.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'{' {
            if let Some(end) = result[i + 1..].find('}') {
                let name = &result[i + 1..i + 1 + end];
                // Skip empty braces or TOML inline tables (contain spaces/quotes/commas)
                if !name.is_empty()
                    && !name.contains(' ')
                    && !name.contains('"')
                    && !name.contains(',')
                {
                    bail!("unresolved template variable: {{{name}}}");
                }
            }
        }
        i += 1;
    }
    Ok(result)
}

/// SCP a local file to a remote path.
/// When `ssh_port` is `None`, no `-P` flag is emitted so the SSH config default applies.
pub fn scp_file(
    local_path: &str,
    remote: &str,
    remote_path: &str,
    ssh_key: Option<&str>,
    ssh_port: Option<u16>,
) -> Result<()> {
    let mut args = vec!["scp".to_string()];
    if let Some(key) = ssh_key {
        args.extend(["-i".to_string(), key.to_string()]);
    }
    if let Some(port) = ssh_port {
        args.extend(["-P".to_string(), port.to_string()]);
    }
    args.extend([local_path.to_string(), format!("{remote}:{remote_path}")]);
    run_command(&args)
}

/// Write content to a local temp file, SCP it to the remote, then sudo mv into place.
pub fn scp_bytes(
    content: &[u8],
    remote: &str,
    remote_path: &str,
    ssh_key: Option<&str>,
    ssh_port: Option<u16>,
) -> Result<()> {
    let tmp = std::env::temp_dir().join(format!("wr-deploy-{}", std::process::id()));
    std::fs::write(&tmp, content).context("failed to write temp file")?;
    let remote_tmp = format!("/tmp/wr-deploy-{}", std::process::id());
    let result = scp_file(
        &tmp.to_string_lossy(),
        remote,
        &remote_tmp,
        ssh_key,
        ssh_port,
    );
    let _ = std::fs::remove_file(&tmp);
    result?;
    let ssh_base = build_ssh_args(remote, ssh_key, ssh_port);
    run_ssh(&ssh_base, &format!("sudo mv {remote_tmp} {remote_path}"))
}

/// Poll the manager until an engine serving all the given (namespace, name) modules
/// is registered, or timeout. Works for both fresh deploys and re-deploys.
pub async fn wait_for_modules(
    manager: &str,
    modules: &[(String, String)],
    timeout: Duration,
) -> bool {
    use tokio_retry::strategy::FixedInterval;
    use tokio_retry::Retry;

    let expected: Vec<_> = modules.iter().map(|(ns, n)| format!("{ns}.{n}")).collect();
    debug!(
        "polling manager {manager} for modules: {expected:?} (timeout {}s)",
        timeout.as_secs()
    );
    let attempt = std::sync::atomic::AtomicU32::new(0);
    let strategy = FixedInterval::from_millis(2000).take(timeout.as_secs() as usize / 2);
    Retry::spawn(strategy, || {
        let n = attempt.fetch_add(1, Ordering::Relaxed) + 1;
        async move {
            match client::connect(manager).await {
                Ok(mut client) => match client.list_engines(ListEnginesRequest {}).await {
                    Ok(resp) => {
                        let engines = resp.into_inner().engines;
                        let registered: Vec<_> = engines
                            .iter()
                            .flat_map(|e| e.modules.iter().map(|m| format!("{}.{}", m.namespace, m.name)))
                            .collect();
                        let all_found = modules.iter().all(|(ns, name)| {
                            engines.iter().any(|e| {
                                e.modules
                                    .iter()
                                    .any(|m| m.namespace == *ns && m.name == *name)
                            })
                        });
                        debug!("attempt {n}: registered modules: {registered:?}, all_found: {all_found}");
                        if all_found {
                            return Ok(());
                        }
                        Err(())
                    }
                    Err(e) => {
                        debug!("attempt {n}: ListEngines RPC failed: {e}");
                        Err(())
                    }
                },
                Err(e) => {
                    debug!("attempt {n}: connection to {manager} failed: {e}");
                    Err(())
                }
            }
        }
    })
    .await
    .is_ok()
}
