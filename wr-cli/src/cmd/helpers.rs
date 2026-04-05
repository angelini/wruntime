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
    let output = Command::new(&args[0])
        .args(&args[1..])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| format!("failed to run {}", args[0]))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        if !stderr.is_empty() {
            eprintln!("{stderr}");
        }
        bail!(
            "{} failed with exit code {:?}",
            args[0],
            output.status.code()
        );
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

///Poll the manager until an engine at the given address registers or timeout.
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

/// Poll a manager gRPC address until it responds to ListManagers or timeout.
/// Returns `true` if the manager became reachable.
pub async fn wait_for_manager_ready(manager_addr: &str, timeout: Duration) -> bool {
    use tokio_retry::strategy::FixedInterval;
    use tokio_retry::Retry;

    let strategy = FixedInterval::from_millis(2000).take(timeout.as_secs() as usize / 2);
    Retry::spawn(strategy, || async {
        match client::connect(manager_addr).await {
            Ok(mut c) => match c
                .list_managers(wr_common::wruntime::ListManagersRequest {})
                .await
            {
                Ok(_) => Ok(()),
                Err(_) => Err(()),
            },
            Err(_) => Err(()),
        }
    })
    .await
    .is_ok()
}

/// Extract the host portion from a `user@host` remote string.
pub fn extract_remote_host(remote: &str) -> &str {
    remote.split('@').next_back().unwrap_or(remote)
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
pub fn scp_file(
    local_path: &str,
    remote: &str,
    remote_path: &str,
    ssh_key: Option<&str>,
    ssh_port: u16,
) -> Result<()> {
    let mut args = vec!["scp".to_string()];
    if let Some(key) = ssh_key {
        args.extend(["-i".to_string(), key.to_string()]);
    }
    args.extend([
        "-P".to_string(),
        ssh_port.to_string(),
        local_path.to_string(),
        format!("{remote}:{remote_path}"),
    ]);
    run_command(&args)
}

/// Write content to a local temp file, SCP it to the remote, then sudo mv into place.
pub fn scp_bytes(
    content: &[u8],
    remote: &str,
    remote_path: &str,
    ssh_key: Option<&str>,
    ssh_port: u16,
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

    let strategy = FixedInterval::from_millis(2000).take(timeout.as_secs() as usize / 2);
    Retry::spawn(strategy, || async {
        if let Ok(mut client) = client::connect(manager).await {
            if let Ok(resp) = client.list_engines(ListEnginesRequest {}).await {
                let engines = resp.into_inner().engines;
                let all_found = modules.iter().all(|(ns, name)| {
                    engines.iter().any(|e| {
                        e.modules
                            .iter()
                            .any(|m| m.namespace == *ns && m.name == *name)
                    })
                });
                if all_found {
                    return Ok(());
                }
            }
        }
        Err(())
    })
    .await
    .is_ok()
}
