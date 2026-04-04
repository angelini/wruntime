//! Shared CLI helpers used by both `dev` and `node` commands.

use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{bail, Result};
use wr_common::wruntime::ListEnginesRequest;

use crate::client;

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
    let status = Command::new(&args[0])
        .args(&args[1..])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .status()
        .with_context(|| format!("failed to run {}", args[0]))?;
    if !status.success() {
        bail!("{} failed with exit code {:?}", args[0], status.code());
    }
    Ok(())
}

use anyhow::Context;

/// Build the base SSH argument list from remote, key, and port.
pub fn build_ssh_args(remote: &str, ssh_key: Option<&str>, ssh_port: u16) -> Vec<String> {
    let mut args = vec!["ssh".to_string()];
    if let Some(key) = ssh_key {
        args.extend(["-i".to_string(), key.to_string()]);
    }
    args.extend(["-p".to_string(), ssh_port.to_string()]);
    args.push(remote.to_string());
    args
}

/// Run a command over SSH.
pub fn run_ssh(ssh_base: &[String], command: &str) -> Result<()> {
    let mut args = ssh_base.to_vec();
    args.push(command.to_string());
    run_command(&args)
}

/// Poll the manager until a new engine registers (count increases) or timeout.
/// Returns `true` if a new engine was detected.
pub async fn wait_for_engine_registration(
    manager: &str,
    initial_count: usize,
    timeout: Duration,
) -> bool {
    use tokio_retry::strategy::FixedInterval;
    use tokio_retry::Retry;

    let strategy = FixedInterval::from_millis(2000).take(timeout.as_secs() as usize / 2);
    Retry::spawn(strategy, || async {
        if get_engine_count(manager).await > initial_count {
            Ok(())
        } else {
            Err(())
        }
    })
    .await
    .is_ok()
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
    let strategy = FixedInterval::from_millis(1000).take(timeout.as_secs() as usize);
    Retry::spawn(strategy, || async {
        if let Ok(mut client) = client::connect(manager).await {
            if let Ok(resp) = client.list_engines(ListEnginesRequest {}).await {
                if resp
                    .into_inner()
                    .engines
                    .iter()
                    .any(|e| normalize_address(&e.address) == normalized)
                {
                    return Ok(());
                }
            }
        }
        Err(())
    })
    .await
    .is_ok()
}

/// Get the current number of registered engines from the manager.
pub async fn get_engine_count(manager: &str) -> usize {
    if let Ok(mut client) = client::connect(manager).await {
        if let Ok(resp) = client.list_engines(ListEnginesRequest {}).await {
            return resp.into_inner().engines.len();
        }
    }
    0
}
