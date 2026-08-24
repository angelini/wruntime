//! Shared CLI and deployment helpers.

use std::future::Future;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use wr_common::node::TlsConfig;
use wr_common::wruntime::{
    GetRoutingTableRequest, ListEnginesRequest, ListManagersRequest, ManagerInfo,
};

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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeployPort(std::num::NonZeroU16);

impl DeployPort {
    pub fn new(port: u16) -> Result<Self> {
        std::num::NonZeroU16::new(port)
            .map(Self)
            .ok_or_else(|| anyhow::anyhow!("deployment port must be > 0"))
    }

    pub fn get(self) -> u16 {
        self.0.get()
    }
}

pub fn extract_port(addr: &str) -> Result<DeployPort> {
    let (_, port) = addr
        .trim()
        .rsplit_once(':')
        .ok_or_else(|| anyhow::anyhow!("address '{addr}' is missing a port"))?;
    let parsed = port
        .parse::<u16>()
        .with_context(|| format!("invalid port in address '{addr}'"))?;
    DeployPort::new(parsed).with_context(|| format!("invalid port in address '{addr}'"))
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

/// Run a command over SSH and return trimmed UTF-8 stdout.
pub fn run_ssh_output(ssh_base: &[String], command: &str) -> Result<String> {
    let mut args = ssh_base.to_vec();
    args.push(command.to_string());
    debug!("exec: {}", args.join(" "));
    let output = Command::new(&args[0])
        .args(&args[1..])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| format!("failed to run {}", args[0]))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("remote command failed: {}", stderr.trim());
    }
    String::from_utf8(output.stdout)
        .map(|value| value.trim().to_string())
        .context("remote command returned non-UTF-8 output")
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
        stdout_task,
        stderr_task,
    })
}

/// Handle for a background prefixed SSH tail. Kills the child on drop.
pub struct PrefixedTail {
    child: tokio::process::Child,
    stdout_task: tokio::task::JoinHandle<()>,
    stderr_task: tokio::task::JoinHandle<()>,
}

impl PrefixedTail {
    /// Stop the remote tail and wait until its output readers can no longer print.
    pub async fn stop(mut self) {
        let _ = self.child.kill().await;
        self.stdout_task.abort();
        self.stderr_task.abort();
        let _ = (&mut self.stdout_task).await;
        let _ = (&mut self.stderr_task).await;
    }
}

impl Drop for PrefixedTail {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        self.stdout_task.abort();
        self.stderr_task.abort();
    }
}

/// Poll the manager until an engine registers and every advertised module has
/// a healthy default route. Engines without modules are ready after registration.
pub async fn wait_for_engine_ready(manager: &str, listen_addr: &str, timeout: Duration) -> bool {
    use tokio_retry::strategy::FixedInterval;
    use tokio_retry::Retry;

    let normalized = normalize_address(listen_addr);
    debug!("polling manager {manager} for ready engine at {listen_addr} (normalized: {normalized}, timeout {}s)", timeout.as_secs());
    let attempt = std::sync::atomic::AtomicU32::new(0);
    let normalized = &normalized;
    let strategy = FixedInterval::from_millis(1000).take(timeout.as_secs() as usize);
    Retry::start(strategy, || {
        let n = attempt.fetch_add(1, Ordering::Relaxed) + 1;
        async move {
            let mut client = match client::connect(manager).await {
                Ok(client) => client,
                Err(e) => {
                    debug!("attempt {n}: connection to {manager} failed: {e}");
                    return Err(());
                }
            };
            let engines = match client.list_engines(ListEnginesRequest {}).await {
                Ok(resp) => resp.into_inner().engines,
                Err(e) => {
                    debug!("attempt {n}: ListEngines RPC failed: {e}");
                    return Err(());
                }
            };
            let Some(engine) = engines
                .iter()
                .find(|engine| normalize_address(&engine.address) == *normalized)
            else {
                debug!("attempt {n}: engine has not registered");
                return Err(());
            };
            if engine.modules.is_empty() {
                return Ok(());
            }

            let rules = match client
                .get_routing_table(GetRoutingTableRequest { known_version: 0 })
                .await
            {
                Ok(resp) => resp
                    .into_inner()
                    .table
                    .map(|table| table.rules)
                    .unwrap_or_default(),
                Err(e) => {
                    debug!("attempt {n}: GetRoutingTable RPC failed: {e}");
                    return Err(());
                }
            };
            let all_ready = engine.modules.iter().all(|module| {
                rules.iter().any(|rule| {
                    rule.engine_id == engine.engine_id
                        && rule.destination_namespace == module.namespace
                        && rule.destination_module == module.name
                        && rule.destination_version == module.version
                        && rule.healthy
                })
            });
            debug!(
                "attempt {n}: engine {} has {}/{} ready module route(s)",
                engine.engine_id,
                engine
                    .modules
                    .iter()
                    .filter(|module| rules.iter().any(|rule| {
                        rule.engine_id == engine.engine_id
                            && rule.destination_namespace == module.namespace
                            && rule.destination_module == module.name
                            && rule.destination_version == module.version
                            && rule.healthy
                    }))
                    .count(),
                engine.modules.len()
            );
            if all_ready {
                Ok(())
            } else {
                Err(())
            }
        }
    })
    .await
    .is_ok()
}

const MANAGER_READINESS_POLL_INTERVAL: Duration = Duration::from_secs(2);
const MAX_OBSERVED_MANAGERS: usize = 8;
const MAX_EVIDENCE_FIELD_CHARS: usize = 160;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagerReadiness {
    pub manager_id: String,
    pub advertised_address: String,
    pub poll_endpoint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ManagerResponseClassification {
    Ready { manager_id: String },
    Pending { evidence: String },
}

fn bounded_evidence_field(value: &str) -> String {
    let mut bounded = String::new();
    let mut truncated = false;
    for (index, character) in value.chars().enumerate() {
        if index >= MAX_EVIDENCE_FIELD_CHARS {
            truncated = true;
            break;
        }
        bounded.push(if character.is_control() {
            '�'
        } else {
            character
        });
    }
    if truncated {
        bounded.push('…');
    }
    bounded
}

fn format_manager_observation(managers: &[ManagerInfo]) -> String {
    let shown = managers
        .iter()
        .take(MAX_OBSERVED_MANAGERS)
        .map(|manager| {
            let id = if manager.manager_id.is_empty() {
                "<empty>".to_string()
            } else {
                bounded_evidence_field(&manager.manager_id)
            };
            format!(
                "id='{id}' address='{}'",
                bounded_evidence_field(&manager.grpc_address)
            )
        })
        .collect::<Vec<_>>();
    let omitted = managers.len().saturating_sub(shown.len());
    let mut evidence = if shown.is_empty() {
        "no managers returned".to_string()
    } else {
        format!("returned [{}]", shown.join(", "))
    };
    if omitted > 0 {
        evidence.push_str(&format!(" ({omitted} more omitted)"));
    }
    evidence
}

fn classify_manager_response(
    managers: &[ManagerInfo],
    expected_advertised_address: &str,
) -> ManagerResponseClassification {
    if let Some(manager) = managers
        .iter()
        .find(|manager| manager.grpc_address == expected_advertised_address)
    {
        if !manager.manager_id.is_empty() {
            return ManagerResponseClassification::Ready {
                manager_id: manager.manager_id.clone(),
            };
        }
        return ManagerResponseClassification::Pending {
            evidence: format!(
                "matching address has an empty manager ID; {}",
                format_manager_observation(managers)
            ),
        };
    }

    ManagerResponseClassification::Pending {
        evidence: format!(
            "expected advertised address was absent; {}",
            format_manager_observation(managers)
        ),
    }
}

async fn wait_for_manager_ready_with<P, Fut>(
    poll_endpoint: &str,
    expected_advertised_address: &str,
    timeout: Duration,
    poll_interval: Duration,
    mut poll: P,
) -> Result<ManagerReadiness>
where
    P: FnMut() -> Fut,
    Fut: Future<Output = Result<Vec<ManagerInfo>>>,
{
    let started = tokio::time::Instant::now();
    let deadline = started + timeout;
    let mut attempts = 0_u32;
    let mut last_evidence: String;

    loop {
        attempts += 1;
        debug!("attempt {attempts}: polling {poll_endpoint}");
        match tokio::time::timeout_at(deadline, poll()).await {
            Ok(Ok(managers)) => {
                debug!(
                    "attempt {attempts}: ListManagers OK ({} managers)",
                    managers.len()
                );
                match classify_manager_response(&managers, expected_advertised_address) {
                    ManagerResponseClassification::Ready { manager_id } => {
                        return Ok(ManagerReadiness {
                            manager_id,
                            advertised_address: expected_advertised_address.to_string(),
                            poll_endpoint: poll_endpoint.to_string(),
                        });
                    }
                    ManagerResponseClassification::Pending { evidence } => {
                        last_evidence = evidence;
                    }
                }
            }
            Ok(Err(error)) => {
                debug!("attempt {attempts}: {error:#}");
                last_evidence = format!("request failed: {error:#}");
            }
            Err(_) => {
                last_evidence = "request exceeded the readiness deadline".to_string();
                break;
            }
        }

        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }
        tokio::time::sleep_until((now + poll_interval).min(deadline)).await;
        if tokio::time::Instant::now() >= deadline {
            break;
        }
    }

    bail!(
        "manager readiness timed out after {:?} (timeout {:?}); poll endpoint: {}; expected advertised address: {}; attempts: {}; last evidence: {}",
        started.elapsed(),
        timeout,
        poll_endpoint,
        expected_advertised_address,
        attempts,
        last_evidence
    )
}

/// Poll a manager through deploy-scoped mTLS until its exact advertised identity appears.
pub async fn wait_for_manager_ready(
    poll_endpoint: &str,
    expected_advertised_address: &str,
    tls: &TlsConfig,
    timeout: Duration,
) -> Result<ManagerReadiness> {
    debug!(
        "polling manager at {poll_endpoint} for {expected_advertised_address} (timeout {}s)",
        timeout.as_secs()
    );
    let poll_endpoint_owned = poll_endpoint.to_string();
    let tls = tls.clone();
    wait_for_manager_ready_with(
        poll_endpoint,
        expected_advertised_address,
        timeout,
        MANAGER_READINESS_POLL_INTERVAL,
        move || {
            let poll_endpoint = poll_endpoint_owned.clone();
            let tls = tls.clone();
            async move {
                let mut manager = client::connect_with_tls(&poll_endpoint, &tls)
                    .await
                    .context("manager TLS connection")?;
                Ok(manager
                    .list_managers(ListManagersRequest {})
                    .await
                    .context("ListManagers RPC")?
                    .into_inner()
                    .managers)
            }
        },
    )
    .await
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_port_accepts_socket_urls_ipv6_and_templates() {
        assert_eq!(extract_port("127.0.0.1:9001").unwrap().get(), 9001);
        assert_eq!(extract_port("http://{host}:9002").unwrap().get(), 9002);
        assert_eq!(extract_port("[::1]:9443").unwrap().get(), 9443);
    }

    #[test]
    fn extract_port_rejects_missing_malformed_and_zero_ports() {
        for address in ["localhost", "localhost:not-a-port", "localhost:0"] {
            assert!(extract_port(address).is_err(), "{address} must be rejected");
        }
    }

    fn manager(id: &str, address: &str) -> ManagerInfo {
        ManagerInfo {
            manager_id: id.to_string(),
            grpc_address: address.to_string(),
            gossip_address: String::new(),
        }
    }

    #[test]
    fn manager_response_requires_exact_address_and_nonempty_id() {
        let expected = "https://manager-a:9000";
        assert_eq!(
            classify_manager_response(
                &[
                    manager("other", "https://manager-b:9000"),
                    manager("manager-a-id", expected),
                ],
                expected,
            ),
            ManagerResponseClassification::Ready {
                manager_id: "manager-a-id".to_string()
            }
        );

        for managers in [
            vec![],
            vec![manager("other", "https://manager-b:9000")],
            vec![manager("", expected)],
            vec![manager("manager-a-id", "https://MANAGER-A:9000")],
            vec![manager("manager-a-id", "https://manager-a:9000/")],
        ] {
            assert!(matches!(
                classify_manager_response(&managers, expected),
                ManagerResponseClassification::Pending { .. }
            ));
        }
    }

    #[tokio::test]
    async fn manager_readiness_retains_absent_response_evidence() {
        let error = wait_for_manager_ready_with(
            "https://poll:9000",
            "https://expected:9000",
            Duration::from_millis(15),
            Duration::from_millis(1),
            || async { Ok(vec![manager("other-id", "https://other:9000")]) },
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(error.contains("poll endpoint: https://poll:9000"));
        assert!(error.contains("expected advertised address: https://expected:9000"));
        assert!(error.contains("attempts:"));
        assert!(error.contains("other-id"));
        assert!(error.contains("https://other:9000"));
    }

    #[tokio::test]
    async fn manager_readiness_can_recover_after_request_errors() {
        let mut calls = 0;
        let ready = wait_for_manager_ready_with(
            "https://poll:9000",
            "https://expected:9000",
            Duration::from_millis(100),
            Duration::from_millis(1),
            move || {
                calls += 1;
                let call = calls;
                async move {
                    if call < 3 {
                        Err(anyhow::anyhow!("temporary RPC failure {call}"))
                    } else {
                        Ok(vec![manager("runtime-id", "https://expected:9000")])
                    }
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(ready.manager_id, "runtime-id");
        assert_eq!(ready.poll_endpoint, "https://poll:9000");
        assert_eq!(ready.advertised_address, "https://expected:9000");
    }

    #[tokio::test]
    async fn manager_readiness_caps_a_hung_attempt_at_the_deadline() {
        let started = std::time::Instant::now();
        let error = wait_for_manager_ready_with(
            "https://poll:9000",
            "https://expected:9000",
            Duration::from_millis(20),
            Duration::from_secs(2),
            || async { std::future::pending::<Result<Vec<ManagerInfo>>>().await },
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(error.contains("request exceeded the readiness deadline"));
        assert!(error.contains("attempts: 1"));
    }

    #[tokio::test]
    async fn prefixed_tail_stop_waits_for_reader_shutdown() {
        let tail =
            spawn_ssh_prefixed(&["sh".to_string(), "-c".to_string()], "sleep 30", "\t").unwrap();

        tokio::time::timeout(Duration::from_secs(1), tail.stop())
            .await
            .expect("tail shutdown must be bounded");
    }
}
