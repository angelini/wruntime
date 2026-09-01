//! Shared CLI and deployment helpers.

use std::collections::BTreeMap;
use std::future::Future;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use wr_common::lifecycle_observation::{classify_lifecycle_state, LifecycleStateClassification};
use wr_common::node::TlsConfig;
use wr_common::wruntime::{
    GetLifecycleStatusRequest, GetProxyRoutingStatusRequest, GetProxyRoutingStatusResponse,
    GetRoutingTableRequest, LifecycleStatus, ProcessLifecycleState, ServiceKind,
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

/// Collect a supplemental SSH diagnostic while preserving its real outcome.
pub fn run_ssh_prefixed_diagnostic(ssh_base: &[String], command: &str, prefix: &str) -> Result<()> {
    let mut args = ssh_base.to_vec();
    args.push(command.to_string());
    let output = Command::new(&args[0])
        .args(&args[1..])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| format!("failed to run diagnostic command {}", args[0]))?;
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        println!("{prefix}{line}");
    }
    for line in String::from_utf8_lossy(&output.stderr).lines() {
        eprintln!("{prefix}{line}");
    }
    if !output.status.success() {
        bail!("diagnostic command exited with {}", output.status);
    }
    Ok(())
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

    let stdout = child
        .stdout
        .take()
        .context("spawned SSH command is missing its stdout pipe")?;
    let stderr = child
        .stderr
        .take()
        .context("spawned SSH command is missing its stderr pipe")?;
    let stdout_task = tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        while let Some(line) = lines.next_line().await? {
            println!("{prefix}{line}");
        }
        std::io::Result::Ok(())
    });
    let stderr_task = tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        while let Some(line) = lines.next_line().await? {
            eprintln!("{prefix}{line}");
        }
        std::io::Result::Ok(())
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
    stdout_task: tokio::task::JoinHandle<std::io::Result<()>>,
    stderr_task: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl PrefixedTail {
    /// Stop the remote tail and join the child plus both output readers.
    /// A child that exited before the local stop request is a diagnostic failure.
    pub async fn stop(mut self) -> Result<()> {
        let mut errors = Vec::new();
        let preexisting_status = match self.child.try_wait() {
            Ok(status) => status,
            Err(error) => {
                errors.push(format!("failed to inspect prefixed SSH tail: {error}"));
                None
            }
        };
        let status = if let Some(status) = preexisting_status {
            Some(status)
        } else {
            if let Err(error) = self.child.start_kill() {
                errors.push(format!(
                    "failed to request prefixed SSH tail termination: {error}"
                ));
            }
            match self.child.wait().await {
                Ok(status) => Some(status),
                Err(error) => {
                    errors.push(format!("failed to reap prefixed SSH tail: {error}"));
                    None
                }
            }
        };
        match (&mut self.stdout_task).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => errors.push(format!("prefixed stdout reader failed: {error}")),
            Err(error) => errors.push(format!("prefixed stdout reader panicked: {error}")),
        }
        match (&mut self.stderr_task).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => errors.push(format!("prefixed stderr reader failed: {error}")),
            Err(error) => errors.push(format!("prefixed stderr reader panicked: {error}")),
        }
        if preexisting_status.is_some() {
            errors.push(format!(
                "prefixed SSH tail exited before requested stop with {}",
                status
                    .map(|status| status.to_string())
                    .unwrap_or_else(|| "unknown status".to_string())
            ));
        }
        if errors.is_empty() {
            Ok(())
        } else {
            bail!("{}", errors.join("; "))
        }
    }
}

impl Drop for PrefixedTail {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        self.stdout_task.abort();
        self.stderr_task.abort();
    }
}

const LIFECYCLE_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Machine-readable lifecycle evidence returned by CLI waits and supervisor IPC.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleObservation {
    pub state: i32,
    pub service_kind: i32,
    pub process_instance_id: String,
    pub reason: i32,
    pub detail: String,
}

impl LifecycleObservation {
    pub fn state_enum(&self) -> Result<ProcessLifecycleState> {
        let state = ProcessLifecycleState::try_from(self.state)
            .map_err(|_| anyhow::anyhow!("unknown lifecycle state {}", self.state))?;
        if state == ProcessLifecycleState::Unspecified {
            bail!("lifecycle endpoint returned an unspecified state");
        }
        Ok(state)
    }

    pub fn state_name(&self) -> Result<&'static str> {
        Ok(self.state_enum()?.as_str_name())
    }

    pub fn service_kind_enum(&self) -> Result<ServiceKind> {
        let kind = ServiceKind::try_from(self.service_kind)
            .map_err(|_| anyhow::anyhow!("unknown lifecycle service kind {}", self.service_kind))?;
        if kind == ServiceKind::Unspecified {
            bail!("lifecycle endpoint returned an unspecified service kind");
        }
        Ok(kind)
    }
}

fn classify_lifecycle_observation(
    observation: LifecycleObservation,
    expected: ProcessLifecycleState,
    evidence: &str,
) -> WaitAttempt<LifecycleObservation> {
    let state = match observation.state_enum() {
        Ok(state) => state,
        Err(error) => return WaitAttempt::Terminal(error),
    };
    match classify_lifecycle_state(state, expected) {
        Ok(LifecycleStateClassification::Matched) => WaitAttempt::Matched(observation),
        Ok(LifecycleStateClassification::Pending) => WaitAttempt::Pending(evidence.to_string()),
        Ok(LifecycleStateClassification::Terminal) => WaitAttempt::Terminal(anyhow::anyhow!(
            "lifecycle process cannot reach expected state {} after observing {}: {evidence}",
            expected.as_str_name(),
            state.as_str_name()
        )),
        Err(error) => WaitAttempt::Terminal(error.into()),
    }
}

fn lifecycle_observation(status: LifecycleStatus) -> Result<LifecycleObservation> {
    let observation = LifecycleObservation {
        state: status.state,
        service_kind: status.service_kind,
        process_instance_id: status.process_instance_id,
        reason: status.reason,
        detail: status.detail,
    };
    observation.state_enum()?;
    observation.service_kind_enum()?;
    if observation.process_instance_id.is_empty() {
        bail!("lifecycle endpoint returned an empty process instance ID");
    }
    Ok(observation)
}

pub async fn get_lifecycle_status(
    endpoint: &str,
    tls: Option<&TlsConfig>,
) -> Result<LifecycleObservation> {
    let mut lifecycle = client::connect_lifecycle(endpoint, tls).await?;
    let response = lifecycle
        .get_status(GetLifecycleStatusRequest {})
        .await
        .with_context(|| format!("lifecycle status RPC failed for {endpoint}"))?
        .into_inner();
    lifecycle_observation(
        response
            .status
            .ok_or_else(|| anyhow::anyhow!("lifecycle endpoint returned no status"))?,
    )
}

/// One typed result from a protocol poll under an absolute deadline.
pub enum WaitAttempt<T> {
    Matched(T),
    Pending(String),
    Terminal(anyhow::Error),
    QueryFailure(anyhow::Error),
}

/// Poll one semantic owner under one absolute deadline while preserving the
/// distinction between a valid-but-pending observation and a query failure.
pub async fn wait_with_deadline<T, F, Fut>(
    subject: &str,
    timeout: Duration,
    poll_interval: Duration,
    mut poll: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = WaitAttempt<T>>,
{
    enum LastEvidence {
        Pending(String),
        QueryFailure(String),
    }

    let started = tokio::time::Instant::now();
    let deadline = started + timeout;
    let mut attempts = 0_u32;
    let mut last_evidence;

    loop {
        attempts += 1;
        match tokio::time::timeout_at(deadline, poll()).await {
            Ok(WaitAttempt::Matched(value)) => return Ok(value),
            Ok(WaitAttempt::Pending(evidence)) => {
                last_evidence = Some(LastEvidence::Pending(evidence));
            }
            Ok(WaitAttempt::Terminal(error)) => {
                return Err(error).with_context(|| format!("{subject} reached a terminal outcome"));
            }
            Ok(WaitAttempt::QueryFailure(error)) => {
                last_evidence = Some(LastEvidence::QueryFailure(format!("{error:#}")));
            }
            Err(_) => {
                last_evidence = Some(LastEvidence::QueryFailure(
                    "protocol query exceeded the absolute deadline".to_string(),
                ));
            }
        }

        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }
        tokio::time::sleep_until((now + poll_interval).min(deadline)).await;
    }

    match last_evidence {
        Some(LastEvidence::Pending(evidence)) => bail!(
            "{subject} timed out after {:?}; attempts: {attempts}; last observation: {evidence}",
            started.elapsed()
        ),
        Some(LastEvidence::QueryFailure(error)) => bail!(
            "{subject} ended in transport/query failure at the absolute deadline after {:?}; attempts: {attempts}; last error: {error}",
            started.elapsed()
        ),
        None => bail!(
            "{subject} timed out after {:?} without an observation; attempts: {attempts}",
            started.elapsed()
        ),
    }
}

/// Wait for one exact lifecycle state under a single absolute deadline.
pub async fn wait_for_lifecycle_state(
    endpoint: &str,
    tls: Option<&TlsConfig>,
    expected: ProcessLifecycleState,
    expected_kind: Option<ServiceKind>,
    expected_instance: Option<&str>,
    timeout: Duration,
) -> Result<LifecycleObservation> {
    if expected == ProcessLifecycleState::Unspecified {
        bail!("cannot wait for an unspecified lifecycle state");
    }
    let pinned_instance = std::rc::Rc::new(std::cell::RefCell::new(
        expected_instance.map(str::to_string),
    ));
    let subject = format!(
        "lifecycle wait at {endpoint} for {}",
        expected.as_str_name()
    );

    wait_with_deadline(&subject, timeout, LIFECYCLE_POLL_INTERVAL, || {
        let pinned_instance = std::rc::Rc::clone(&pinned_instance);
        async move {
            let observation = match get_lifecycle_status(endpoint, tls).await {
                Ok(observation) => observation,
                Err(error) => return WaitAttempt::QueryFailure(error),
            };
            let state = match observation.state_enum() {
                Ok(state) => state,
                Err(error) => return WaitAttempt::Terminal(error),
            };
            let evidence = format!(
                "state={} instance={} detail={}",
                state.as_str_name(),
                observation.process_instance_id,
                observation.detail
            );
            if let Some(expected_kind) = expected_kind {
                match observation.service_kind_enum() {
                    Ok(observed_kind) if observed_kind == expected_kind => {}
                    Ok(observed_kind) => {
                        return WaitAttempt::Terminal(anyhow::anyhow!(
                            "lifecycle service kind mismatch at {endpoint}: expected {}, observed {}",
                            expected_kind.as_str_name(),
                            observed_kind.as_str_name()
                        ));
                    }
                    Err(error) => return WaitAttempt::Terminal(error),
                }
            }
            {
                let mut pinned = pinned_instance.borrow_mut();
                if let Some(instance) = pinned.as_deref() {
                    if instance != observation.process_instance_id {
                        return WaitAttempt::Terminal(anyhow::anyhow!(
                            "lifecycle process instance mismatch at {endpoint}: expected {instance}, observed {}",
                            observation.process_instance_id
                        ));
                    }
                } else {
                    *pinned = Some(observation.process_instance_id.clone());
                }
            }
            classify_lifecycle_observation(observation, expected, &evidence)
        }
    })
    .await
}

pub async fn wait_for_lifecycle_ready(
    endpoint: &str,
    tls: Option<&TlsConfig>,
    expected_kind: ServiceKind,
    expected_instance: &str,
    timeout: Duration,
) -> Result<LifecycleObservation> {
    wait_for_lifecycle_state(
        endpoint,
        tls,
        ProcessLifecycleState::Ready,
        Some(expected_kind),
        Some(expected_instance),
        timeout,
    )
    .await
}

/// One proxy endpoint and the activation observed from READY on that endpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProxyRoutingBarrierTarget {
    pub name: String,
    pub endpoint: String,
    pub process_instance_id: String,
}

/// Read the authoritative routing-table version directly from the manager.
pub async fn get_manager_routing_table_version(endpoint: &str) -> Result<u64> {
    let mut manager = client::connect(endpoint).await?;
    let table = manager
        .get_routing_table(GetRoutingTableRequest { known_version: 0 })
        .await
        .with_context(|| format!("manager routing-table query failed for {endpoint}"))?
        .into_inner()
        .table
        .ok_or_else(|| anyhow::anyhow!("manager returned no routing table for version capture"))?;
    Ok(table.version)
}

async fn get_proxy_routing_status(endpoint: &str) -> Result<GetProxyRoutingStatusResponse> {
    let mut proxy = client::connect_node(endpoint).await?;
    Ok(proxy
        .get_proxy_routing_status(GetProxyRoutingStatusRequest {})
        .await
        .with_context(|| format!("proxy routing-status query failed for {endpoint}"))?
        .into_inner())
}

fn validate_proxy_routing_status(
    target: &ProxyRoutingBarrierTarget,
    status: &GetProxyRoutingStatusResponse,
    previous_version: Option<u64>,
    target_version: u64,
) -> Result<bool> {
    if status.process_instance_id != target.process_instance_id {
        bail!(
            "proxy {} routing identity mismatch at {}: expected {}, observed {}",
            target.name,
            target.endpoint,
            target.process_instance_id,
            status.process_instance_id
        );
    }
    if previous_version.is_some_and(|previous| status.installed_routing_table_version < previous) {
        bail!(
            "proxy {} routing version regressed at {}: previous {}, observed {}",
            target.name,
            target.endpoint,
            previous_version.unwrap_or_default(),
            status.installed_routing_table_version
        );
    }
    Ok(status.installed_routing_table_version >= target_version)
}

/// Wait for every named proxy to install at least `target_version` under one
/// absolute deadline shared by all transport calls and poll rounds.
pub async fn wait_for_proxy_routing_barrier(
    targets: &[ProxyRoutingBarrierTarget],
    target_version: u64,
    timeout: Duration,
) -> Result<()> {
    if targets.is_empty() {
        bail!("routing barrier requires at least one proxy");
    }
    let started = tokio::time::Instant::now();
    let deadline = started + timeout;
    let mut versions = BTreeMap::<String, u64>::new();

    loop {
        let mut pending = Vec::new();
        for target in targets {
            let status =
                tokio::time::timeout_at(deadline, get_proxy_routing_status(&target.endpoint))
                    .await
                    .with_context(|| {
                        format!(
                            "proxy {} routing query exceeded the shared barrier deadline",
                            target.name
                        )
                    })??;
            let previous = versions.get(&target.name).copied();
            let reached = validate_proxy_routing_status(target, &status, previous, target_version)?;
            versions.insert(target.name.clone(), status.installed_routing_table_version);
            if !reached {
                pending.push(format!(
                    "{}={}",
                    target.name, status.installed_routing_table_version
                ));
            }
        }
        if pending.is_empty() {
            return Ok(());
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            bail!(
                "proxy routing barrier timed out after {:?}: target version {}, pending {}",
                started.elapsed(),
                target_version,
                pending.join(", ")
            );
        }
        tokio::time::sleep_until((now + LIFECYCLE_POLL_INTERVAL).min(deadline)).await;
    }
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
        if bytes[i] != b'{' {
            i += 1;
            continue;
        }
        if let Some(end) = result[i + 1..].find('}') {
            let name = &result[i + 1..i + 1 + end];
            // Skip empty braces or TOML inline tables (contain spaces/quotes/commas)
            if !name.is_empty() && !name.contains(' ') && !name.contains('"') && !name.contains(',')
            {
                bail!("unresolved template variable: {{{name}}}");
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

    #[test]
    fn lifecycle_observation_rejects_invalid_wire_evidence() {
        assert!(lifecycle_observation(LifecycleStatus::default()).is_err());
        let missing_instance = LifecycleStatus {
            state: ProcessLifecycleState::Ready as i32,
            service_kind: ServiceKind::Manager as i32,
            ..Default::default()
        };
        assert!(lifecycle_observation(missing_instance).is_err());
        let missing_kind = LifecycleStatus {
            state: ProcessLifecycleState::Ready as i32,
            process_instance_id: "instance".to_string(),
            ..Default::default()
        };
        assert!(lifecycle_observation(missing_kind).is_err());
    }

    #[test]
    fn lifecycle_observation_preserves_typed_identity_and_state() -> Result<()> {
        let observation = lifecycle_observation(LifecycleStatus {
            state: ProcessLifecycleState::Ready as i32,
            service_kind: ServiceKind::Manager as i32,
            process_instance_id: "manager-instance".to_string(),
            detail: "startup complete".to_string(),
            ..Default::default()
        })?;
        assert_eq!(observation.state_enum()?, ProcessLifecycleState::Ready);
        assert_eq!(observation.process_instance_id, "manager-instance");
        assert_eq!(observation.detail, "startup complete");
        Ok(())
    }

    #[test]
    fn lifecycle_expectation_rejects_states_that_cannot_be_reached_exactly() {
        for (observed, expected) in [
            (
                ProcessLifecycleState::Ready,
                ProcessLifecycleState::Starting,
            ),
            (
                ProcessLifecycleState::Stopping,
                ProcessLifecycleState::Ready,
            ),
        ] {
            let observation = LifecycleObservation {
                state: observed as i32,
                service_kind: ServiceKind::Engine as i32,
                process_instance_id: "engine-instance".to_string(),
                reason: 0,
                detail: "advanced".to_string(),
            };
            assert!(matches!(
                classify_lifecycle_observation(observation, expected, "advanced"),
                WaitAttempt::Terminal(_)
            ));
        }
    }

    #[tokio::test]
    async fn absolute_wait_preserves_pending_timeout_and_query_failure() -> Result<()> {
        let pending = wait_with_deadline::<(), _, _>(
            "pending fixture",
            Duration::from_millis(5),
            Duration::from_millis(1),
            || async { WaitAttempt::Pending("state=STARTING".to_string()) },
        )
        .await;
        let pending_error = match pending {
            Ok(()) => bail!("pending fixture unexpectedly matched"),
            Err(error) => error,
        };
        assert!(pending_error.to_string().contains("timed out"));
        assert!(pending_error.to_string().contains("state=STARTING"));

        let unavailable = wait_with_deadline::<(), _, _>(
            "query fixture",
            Duration::from_millis(5),
            Duration::from_millis(1),
            || async { WaitAttempt::QueryFailure(anyhow::anyhow!("offline")) },
        )
        .await;
        let query_error = match unavailable {
            Ok(()) => bail!("query fixture unexpectedly matched"),
            Err(error) => error,
        };
        assert!(query_error.to_string().contains("transport/query failure"));
        assert!(query_error.to_string().contains("offline"));
        Ok(())
    }

    #[tokio::test]
    async fn absolute_wait_matches_after_valid_pending_evidence() -> Result<()> {
        let attempts = std::cell::Cell::new(0_u8);
        let value = wait_with_deadline(
            "matching fixture",
            Duration::from_secs(1),
            Duration::from_millis(1),
            || {
                let attempt = attempts.get() + 1;
                attempts.set(attempt);
                async move {
                    if attempt == 1 {
                        WaitAttempt::Pending("not yet".to_string())
                    } else {
                        WaitAttempt::Matched("ready")
                    }
                }
            },
        )
        .await?;
        assert_eq!(value, "ready");
        assert_eq!(attempts.get(), 2);
        Ok(())
    }

    #[test]
    fn proxy_routing_barrier_requires_activation_and_monotonic_progress() -> Result<()> {
        let target = ProxyRoutingBarrierTarget {
            name: "primary".to_string(),
            endpoint: "http://127.0.0.1:9002".to_string(),
            process_instance_id: "proxy-activation".to_string(),
        };
        let pending = GetProxyRoutingStatusResponse {
            process_instance_id: "proxy-activation".to_string(),
            installed_routing_table_version: 6,
        };
        assert!(!validate_proxy_routing_status(&target, &pending, None, 7)?);
        let reached = GetProxyRoutingStatusResponse {
            installed_routing_table_version: 7,
            ..pending.clone()
        };
        assert!(validate_proxy_routing_status(
            &target,
            &reached,
            Some(6),
            7
        )?);

        let wrong_activation = GetProxyRoutingStatusResponse {
            process_instance_id: "replacement".to_string(),
            installed_routing_table_version: 8,
        };
        assert!(
            validate_proxy_routing_status(&target, &wrong_activation, Some(7), 7)
                .unwrap_err()
                .to_string()
                .contains("identity mismatch")
        );
        let regressed = GetProxyRoutingStatusResponse {
            process_instance_id: "proxy-activation".to_string(),
            installed_routing_table_version: 5,
        };
        assert!(
            validate_proxy_routing_status(&target, &regressed, Some(6), 7)
                .unwrap_err()
                .to_string()
                .contains("regressed")
        );
        Ok(())
    }

    #[tokio::test]
    async fn prefixed_tail_reports_a_preexisting_nonzero_exit() -> Result<()> {
        let tail = spawn_ssh_prefixed(&["sh".to_string(), "-c".to_string()], "exit 23", "\t")?;
        tokio::time::sleep(Duration::from_millis(25)).await;
        let result = tail.stop().await;
        let error = match result {
            Ok(()) => bail!("pre-exited nonzero tail unexpectedly reported clean stop"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("exited before requested stop"));
        assert!(error.to_string().contains("23"));
        Ok(())
    }

    #[tokio::test]
    async fn prefixed_tail_stop_waits_for_reader_shutdown() -> Result<()> {
        let tail =
            spawn_ssh_prefixed(&["sh".to_string(), "-c".to_string()], "exec sleep 30", "\t")?;

        tokio::time::timeout(Duration::from_secs(1), tail.stop())
            .await
            .context("tail shutdown must be bounded")?
            .context("tail shutdown must be clean")?;
        Ok(())
    }
}
