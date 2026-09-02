use std::collections::{BTreeSet, VecDeque};
use std::io;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::task::{JoinHandle, JoinSet};
use uuid::Uuid;
use wr_common::process_lifecycle::PROCESS_INSTANCE_ID_ENV;
use wr_common::wruntime::ServiceKind;

use super::helpers::{self, LifecycleObservation, ProxyRoutingBarrierTarget};
use crate::client;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);
const INTERNAL_STOP_BUDGET: Duration = Duration::from_secs(30);
const TERM_GRACE: Duration = Duration::from_secs(10);
const KILL_GRACE: Duration = Duration::from_secs(5);
const STOP_BUDGETS: StopBudgets = StopBudgets {
    internal: INTERNAL_STOP_BUDGET,
    term: TERM_GRACE,
    kill: KILL_GRACE,
};
const OUTPUT_TAIL_LINES: usize = 80;
const POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Clone, Copy)]
struct StopBudgets {
    internal: Duration,
    term: Duration,
    kill: Duration,
}

#[derive(Default)]
struct StopPolicyHooks {
    deadline_notice: Option<tokio::sync::oneshot::Sender<String>>,
    suppress_signals: bool,
}

#[derive(Debug)]
enum ForegroundWait<T> {
    Completed(T),
    Interrupted,
}

fn record_error(errors: &mut Vec<String>, error: impl Into<String>) {
    const MAX_ERRORS: usize = 24;
    if errors.len() < MAX_ERRORS {
        errors.push(error.into());
    } else if errors.len() == MAX_ERRORS {
        errors.push("additional cleanup errors truncated".to_string());
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ServiceRole {
    Manager,
    Proxy,
    Engine,
}

impl ServiceRole {
    fn binary(self) -> &'static str {
        match self {
            Self::Manager => "wr-manager",
            Self::Proxy => "wr-proxy",
            Self::Engine => "wr-engine",
        }
    }

    fn lifecycle_kind(self) -> ServiceKind {
        match self {
            Self::Manager => ServiceKind::Manager,
            Self::Proxy => ServiceKind::Proxy,
            Self::Engine => ServiceKind::Engine,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Manager => "manager",
            Self::Proxy => "proxy",
            Self::Engine => "engine",
        }
    }
}

#[derive(Clone, Debug)]
struct ServiceSpec {
    name: String,
    role: ServiceRole,
    config: PathBuf,
    command_override: Option<Vec<String>>,
    endpoint_override: Option<String>,
    process_instance_id_override: Option<String>,
    skip_endpoint_check: bool,
}

#[derive(Clone, Debug)]
pub(super) struct RunSpec {
    pub(super) manager_config: PathBuf,
    pub(super) proxies: Vec<(String, PathBuf)>,
    pub(super) engine_configs: Vec<PathBuf>,
    pub(super) scenario: Vec<String>,
}

impl RunSpec {
    fn validate(self) -> Result<Self> {
        if self.proxies.is_empty() {
            bail!("dev run requires one or more --proxy-config NAME=PATH arguments");
        }
        let mut names = BTreeSet::new();
        for (name, path) in &self.proxies {
            if !valid_proxy_name(name) {
                bail!("invalid proxy name '{name}'; use ASCII letters, digits, '.' '_' or '-'");
            }
            if !names.insert(name.clone()) {
                bail!("duplicate proxy name '{name}'");
            }
            ensure_config(path)?;
        }
        ensure_config(&self.manager_config)?;
        for config in &self.engine_configs {
            ensure_config(config)?;
        }
        if self
            .scenario
            .first()
            .is_some_and(|program| program.is_empty())
        {
            bail!("scenario executable must not be empty");
        }
        Ok(self)
    }
}

fn valid_proxy_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn ensure_config(path: &Path) -> Result<()> {
    if !path.is_file() {
        bail!("service config not found: {}", path.display());
    }
    Ok(())
}

fn endpoint_from_config(config: &Path, role: ServiceRole) -> Result<(String, bool)> {
    let text = std::fs::read_to_string(config)
        .with_context(|| format!("failed to read service config {}", config.display()))?;
    let value: toml::Value = toml::from_str(&text)
        .with_context(|| format!("failed to parse service config {}", config.display()))?;
    let field = match role {
        ServiceRole::Manager | ServiceRole::Engine => {
            value.get("listen_address").and_then(toml::Value::as_str)
        }
        ServiceRole::Proxy => value
            .get("control_address")
            .and_then(toml::Value::as_str)
            .or_else(|| {
                value
                    .get("node")
                    .and_then(|node| node.get("control_address"))
                    .and_then(toml::Value::as_str)
            }),
    }
    .ok_or_else(|| anyhow::anyhow!("{} config has no lifecycle control address", role.label()))?;
    let address = helpers::normalize_address(field);
    let scheme = if role == ServiceRole::Manager {
        "https"
    } else {
        "http"
    };
    Ok((
        format!("{scheme}://{address}"),
        role == ServiceRole::Manager,
    ))
}

async fn ensure_endpoint_unoccupied(endpoint: &str) -> Result<()> {
    let address = endpoint
        .split_once("://")
        .map(|(_, address)| address)
        .context("lifecycle endpoint has no URL scheme")?;
    match tokio::time::timeout(
        Duration::from_millis(500),
        tokio::net::TcpStream::connect(address),
    )
    .await
    {
        Ok(Ok(_)) => bail!(
            "lifecycle endpoint {endpoint} already accepts connections; refusing to spawn an unowned process"
        ),
        Ok(Err(error)) if error.kind() == io::ErrorKind::ConnectionRefused => Ok(()),
        Ok(Err(error)) => Err(error)
            .with_context(|| format!("could not prove lifecycle endpoint {endpoint} is unoccupied")),
        Err(_) => bail!("timed out proving lifecycle endpoint {endpoint} is unoccupied"),
    }
}

fn resolve_binary(name: &str) -> String {
    let local = format!("./target/debug/{name}");
    if Path::new(&local).is_file() {
        local
    } else {
        name.to_string()
    }
}

fn push_tail(tail: &Arc<Mutex<VecDeque<String>>>, line: String) {
    let mut tail = tail.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if tail.len() == OUTPUT_TAIL_LINES {
        tail.pop_front();
    }
    tail.push_back(line);
}

fn stream_output<R>(
    reader: R,
    prefix: String,
    stream: &'static str,
    tail: Arc<Mutex<VecDeque<String>>>,
) -> JoinHandle<()>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let evidence = format!("{stream}: {line}");
            push_tail(&tail, evidence);
            eprintln!("[{prefix}] {line}");
        }
    })
}

struct ManagedProcess {
    name: String,
    role: ServiceRole,
    config: PathBuf,
    endpoint: String,
    manager_tls: bool,
    process_instance_id: String,
    child: Option<Child>,
    output_tasks: Vec<JoinHandle<()>>,
    tail: Arc<Mutex<VecDeque<String>>>,
    exit_status: Option<ExitStatus>,
}

impl ManagedProcess {
    async fn spawn(spec: ServiceSpec) -> Result<Self> {
        let config = std::fs::canonicalize(&spec.config)
            .with_context(|| format!("service config not found: {}", spec.config.display()))?;
        let (endpoint, manager_tls) = match spec.endpoint_override {
            Some(endpoint) => (endpoint, false),
            None => endpoint_from_config(&config, spec.role)?,
        };
        if !spec.skip_endpoint_check {
            ensure_endpoint_unoccupied(&endpoint).await?;
        }
        let process_instance_id = spec
            .process_instance_id_override
            .unwrap_or_else(|| format!("dev-run-{}", Uuid::new_v4()));
        let binary = resolve_binary(spec.role.binary());
        let mut command = if let Some(command_override) = spec.command_override {
            let program = command_override
                .first()
                .context("fixture service command is empty")?;
            let mut command = Command::new(program);
            command.args(&command_override[1..]);
            command
        } else {
            let mut command = Command::new(&binary);
            command.arg(&config);
            command
        };
        command
            .env(PROCESS_INSTANCE_ID_ENV, &process_instance_id)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        unsafe {
            command.as_std_mut().pre_exec(|| {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) != 0 {
                    return Err(io::Error::last_os_error());
                }
                if libc::getppid() == 1 {
                    return Err(io::Error::other("dev run owner exited before service exec"));
                }
                Ok(())
            });
        }
        let mut child = command
            .spawn()
            .with_context(|| format!("failed to start {binary} {}", config.display()))?;
        let tail = Arc::new(Mutex::new(VecDeque::new()));
        let prefix = format!("{}:{}", spec.role.label(), spec.name);
        let mut output_tasks = Vec::new();
        if let Some(stdout) = child.stdout.take() {
            output_tasks.push(stream_output(
                stdout,
                prefix.clone(),
                "stdout",
                Arc::clone(&tail),
            ));
        }
        if let Some(stderr) = child.stderr.take() {
            output_tasks.push(stream_output(stderr, prefix, "stderr", Arc::clone(&tail)));
        }
        Ok(Self {
            name: spec.name,
            role: spec.role,
            config,
            endpoint,
            manager_tls,
            process_instance_id,
            child: Some(child),
            output_tasks,
            tail,
            exit_status: None,
        })
    }

    fn tls(&self) -> Option<&'static wr_common::node::TlsConfig> {
        self.manager_tls.then(client::tls_config).flatten()
    }

    fn evidence(&self) -> String {
        let lines = self
            .tail
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let output = if lines.is_empty() {
            "<no captured output>".to_string()
        } else {
            lines.iter().cloned().collect::<Vec<_>>().join(" | ")
        };
        format!("config={} output={output}", self.config.display())
    }

    async fn wait_ready(
        &mut self,
        deadline: tokio::time::Instant,
        signals: &mut tokio::sync::watch::Receiver<usize>,
    ) -> Result<ForegroundWait<LifecycleObservation>> {
        let mut last = "no lifecycle observation".to_string();
        loop {
            if *signals.borrow() > 0 {
                return Ok(ForegroundWait::Interrupted);
            }
            match self.try_wait() {
                Ok(Some(status)) => {
                    bail!(
                        "{}:{} exited before READY with {status}; last lifecycle evidence: {last}; tail: {}",
                        self.role.label(),
                        self.name,
                        self.evidence()
                    );
                }
                Ok(None) => {}
                Err(error) => return Err(error.context("readiness child inspection failed")),
            }
            let query = helpers::get_lifecycle_status(&self.endpoint, self.tls());
            tokio::select! {
                changed = signals.changed() => {
                    if changed.is_err() {
                        bail!("signal tracker closed during readiness");
                    }
                    return Ok(ForegroundWait::Interrupted);
                }
                result = tokio::time::timeout_at(deadline, query) => match result {
                    Ok(Ok(observation)) => {
                        last = format!(
                            "state={} instance={} detail={}",
                            observation.state_name()?,
                            observation.process_instance_id,
                            observation.detail
                        );
                        if observation.service_kind_enum()? != self.role.lifecycle_kind() {
                            bail!(
                                "{}:{} lifecycle service-kind mismatch: {last}",
                                self.role.label(),
                                self.name
                            );
                        }
                        if observation.process_instance_id != self.process_instance_id {
                            bail!(
                                "{}:{} lifecycle activation mismatch: expected {}, observed {}",
                                self.role.label(),
                                self.name,
                                self.process_instance_id,
                                observation.process_instance_id
                            );
                        }
                        match observation.state_enum()? {
                            wr_common::wruntime::ProcessLifecycleState::Ready => {
                                return Ok(ForegroundWait::Completed(observation));
                            }
                            wr_common::wruntime::ProcessLifecycleState::Stopping => {
                                bail!(
                                    "{}:{} stopped before READY: {last}",
                                    self.role.label(),
                                    self.name
                                );
                            }
                            _ => {}
                        }
                    }
                    Ok(Err(error)) => last = format!("transport/query failure: {error:#}"),
                    Err(_) => break,
                }
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                break;
            }
            tokio::select! {
                changed = signals.changed() => {
                    if changed.is_err() {
                        bail!("signal tracker closed during readiness backoff");
                    }
                    return Ok(ForegroundWait::Interrupted);
                }
                _ = tokio::time::sleep_until((now + Duration::from_millis(200)).min(deadline)) => {}
            }
        }
        bail!(
            "{}:{} did not reach READY before the startup deadline; last evidence: {last}; tail: {}",
            self.role.label(),
            self.name,
            self.evidence()
        )
    }

    fn try_wait(&mut self) -> Result<Option<ExitStatus>> {
        if let Some(status) = self.exit_status {
            return Ok(Some(status));
        }
        let Some(child) = self.child.as_mut() else {
            return Ok(self.exit_status);
        };
        if let Some(status) = child
            .try_wait()
            .context("failed to inspect owned service")?
        {
            self.exit_status = Some(status);
            self.child = None;
            return Ok(Some(status));
        }
        Ok(None)
    }

    async fn stop_and_reap(self, escalation: Arc<AtomicUsize>) -> Result<()> {
        self.stop_and_reap_with_budgets(escalation, STOP_BUDGETS, StopPolicyHooks::default())
            .await
    }

    async fn stop_and_reap_with_budgets(
        mut self,
        escalation: Arc<AtomicUsize>,
        budgets: StopBudgets,
        mut hooks: StopPolicyHooks,
    ) -> Result<()> {
        let Some(mut child) = self.child.take() else {
            let mut errors = Vec::new();
            self.finish_output(&mut errors).await;
            return match self.exit_status {
                Some(status) if status.success() && errors.is_empty() => Ok(()),
                Some(status) => bail!(
                    "{}:{} exited unexpectedly with {status}; {}; tail: {}",
                    self.role.label(),
                    self.name,
                    errors.join("; "),
                    self.evidence()
                ),
                None if errors.is_empty() => Ok(()),
                None => bail!("{}", errors.join("; ")),
            };
        };
        let started = tokio::time::Instant::now();
        let kill_at = started + budgets.internal + budgets.term;
        let external_deadline = kill_at + budgets.kill;
        let mut errors = Vec::new();
        let mut forced = false;
        let mut kill_decided = false;
        let mut deadline_evidence = None;

        if !hooks.suppress_signals {
            match child.id() {
                Some(pid) => {
                    if unsafe { libc::kill(pid as i32, libc::SIGTERM) } != 0 {
                        record_error(
                            &mut errors,
                            format!("SIGTERM failed: {}", io::Error::last_os_error()),
                        );
                    }
                }
                None => record_error(&mut errors, "child had no PID for SIGTERM"),
            }
        }

        let final_status = loop {
            if deadline_evidence.is_some() {
                match child.try_wait() {
                    Ok(Some(status)) => break status,
                    Ok(None) => {}
                    Err(error) => {
                        record_error(&mut errors, format!("reap inspection failed: {error}"))
                    }
                }
                // Once latched this is reap-only: no new grace decisions or
                // signals, and the sole Child owner stays alive.
                tokio::time::sleep(POLL_INTERVAL.min(Duration::from_secs(1))).await;
                continue;
            }

            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(external_deadline) => {
                    if !forced && !hooks.suppress_signals {
                        kill_decided = true;
                        match child.start_kill() {
                            Ok(()) => forced = true,
                            Err(error) => record_error(
                                &mut errors,
                                format!("terminal-boundary SIGKILL request failed: {error}"),
                            ),
                        }
                    }
                    let latched = format!(
                        "deadline exceeded, awaiting reap: {}:{} {}; errors={}",
                        self.role.label(),
                        self.name,
                        self.evidence(),
                        if errors.is_empty() {
                            "none".to_string()
                        } else {
                            errors.join(" | ")
                        }
                    );
                    eprintln!("[dev-run] {latched}");
                    if let Some(notice) = hooks.deadline_notice.take() {
                        let _ = notice.send(latched.clone());
                    }
                    deadline_evidence = Some(latched);
                }
                wait_result = child.wait() => {
                    match wait_result {
                        Ok(status) => break status,
                        Err(error) => {
                            record_error(&mut errors, format!("reap wait failed: {error}"));
                            tokio::time::sleep(POLL_INTERVAL).await;
                        }
                    }
                }
                _ = tokio::time::sleep_until(kill_at), if !kill_decided => {
                    kill_decided = true;
                    if !hooks.suppress_signals {
                        match child.start_kill() {
                            Ok(()) => forced = true,
                            Err(error) => record_error(
                                &mut errors,
                                format!("SIGKILL request failed: {error}"),
                            ),
                        }
                    }
                }
                _ = tokio::time::sleep(POLL_INTERVAL) => {
                    if !kill_decided && escalation.load(Ordering::SeqCst) >= 2 {
                        kill_decided = true;
                        if !hooks.suppress_signals {
                            match child.start_kill() {
                                Ok(()) => forced = true,
                                Err(error) => record_error(
                                    &mut errors,
                                    format!("SIGKILL request failed: {error}"),
                                ),
                            }
                        }
                    }
                }
            }
        };
        self.exit_status = Some(final_status);
        self.finish_output(&mut errors).await;

        if let Some(latched) = deadline_evidence {
            bail!(
                "{latched}; final status={final_status}; intervening errors={}",
                if errors.is_empty() {
                    "none".to_string()
                } else {
                    errors.join(" | ")
                }
            );
        }
        let error_summary = if errors.is_empty() {
            "none".to_string()
        } else {
            errors.join(" | ")
        };
        if forced {
            bail!(
                "{}:{} required SIGKILL fallback; status {final_status}; errors={error_summary}; tail: {}",
                self.role.label(),
                self.name,
                self.evidence()
            );
        }
        if !final_status.success() || !errors.is_empty() {
            bail!(
                "{}:{} cleanup failed with {final_status}; errors={error_summary}; tail: {}",
                self.role.label(),
                self.name,
                self.evidence()
            );
        }
        Ok(())
    }

    async fn finish_output(&mut self, errors: &mut Vec<String>) {
        for mut task in self.output_tasks.drain(..) {
            if tokio::time::timeout(Duration::from_secs(1), &mut task)
                .await
                .is_err()
            {
                task.abort();
                let _ = task.await;
                record_error(errors, "output reader did not finish after child reap");
            }
        }
    }

    #[cfg(test)]
    fn fixture(name: &str, role: ServiceRole, script: &str) -> Result<Self> {
        let child = Command::new("sh")
            .arg("-c")
            .arg(script)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        let process_instance_id = format!("fixture-{name}");
        Ok(Self {
            name: name.to_string(),
            role,
            config: PathBuf::from("fixture.toml"),
            endpoint: "http://127.0.0.1:1".to_string(),
            manager_tls: false,
            process_instance_id,
            child: Some(child),
            output_tasks: Vec::new(),
            tail: Arc::new(Mutex::new(VecDeque::new())),
            exit_status: None,
        })
    }
}

struct ScenarioProcess {
    child: Option<Child>,
    direct_status: Option<ExitStatus>,
    pgid: i32,
}

impl ScenarioProcess {
    async fn spawn(command: &[String]) -> Result<Self> {
        let program = command.first().context("scenario command is empty")?;
        let mut process = Command::new(program);
        process
            .args(&command[1..])
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());
        unsafe {
            process.as_std_mut().pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(io::Error::last_os_error());
                }
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) != 0 {
                    return Err(io::Error::last_os_error());
                }
                if libc::getppid() == 1 {
                    return Err(io::Error::other(
                        "dev run owner exited before scenario exec",
                    ));
                }
                Ok(())
            });
        }
        let mut child = process
            .spawn()
            .with_context(|| format!("failed to start scenario {program}"))?;
        let Some(pgid) = child.id().map(|pid| pid as i32) else {
            let mut evidence = vec!["scenario child had no PID after spawn".to_string()];
            if let Err(error) = child.start_kill() {
                record_error(
                    &mut evidence,
                    format!("PID-less child kill failed: {error}"),
                );
            }
            loop {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        record_error(&mut evidence, format!("final status={status}"));
                        break;
                    }
                    Ok(None) => tokio::time::sleep(POLL_INTERVAL).await,
                    Err(error) => {
                        record_error(
                            &mut evidence,
                            format!("PID-less child reap failed: {error}"),
                        );
                        tokio::time::sleep(POLL_INTERVAL).await;
                    }
                }
            }
            bail!("{}", evidence.join("; "));
        };
        Ok(Self {
            child: Some(child),
            direct_status: None,
            pgid,
        })
    }

    fn try_wait(&mut self) -> Result<Option<ExitStatus>> {
        if let Some(status) = self.direct_status {
            return Ok(Some(status));
        }
        let Some(child) = self.child.as_mut() else {
            return Ok(self.direct_status);
        };
        if let Some(status) = child.try_wait()? {
            self.direct_status = Some(status);
            self.child = None;
            return Ok(Some(status));
        }
        Ok(None)
    }

    fn signal_group(&self, signal: i32) -> Result<bool> {
        if unsafe { libc::kill(-self.pgid, signal) } == 0 {
            return Ok(true);
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            Ok(false)
        } else {
            Err(error).context("failed to signal scenario process group")
        }
    }

    fn group_exists(&self) -> Result<bool> {
        if unsafe { libc::kill(-self.pgid, 0) } == 0 {
            return Ok(true);
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            Ok(false)
        } else {
            Err(error).context("failed to probe scenario process group")
        }
    }

    async fn terminate_and_reap(
        &mut self,
        escalation: Arc<AtomicUsize>,
        signals: &mut tokio::sync::watch::Receiver<usize>,
    ) -> Result<()> {
        let mut errors = Vec::new();
        let started = tokio::time::Instant::now();
        let term_deadline = started + TERM_GRACE;
        let kill_deadline = term_deadline + KILL_GRACE;
        let mut kill_sent = escalation.load(Ordering::SeqCst) >= 2;
        if kill_sent {
            record_error(
                &mut errors,
                "second signal forced scenario SIGKILL escalation",
            );
        }
        let first_signal = if kill_sent {
            libc::SIGKILL
        } else {
            libc::SIGTERM
        };
        if let Err(error) = self.signal_group(first_signal) {
            record_error(
                &mut errors,
                format!("initial scenario-group signal failed: {error:#}"),
            );
        }
        let mut group_deadline_latched = false;

        loop {
            if let Some(child) = self.child.as_mut() {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        self.direct_status = Some(status);
                        self.child = None;
                    }
                    Ok(None) => {}
                    Err(error) => {
                        record_error(
                            &mut errors,
                            format!("scenario reap inspection failed: {error}"),
                        );
                    }
                }
            }
            let group_exists = match self.group_exists() {
                Ok(exists) => exists,
                Err(error) => {
                    record_error(
                        &mut errors,
                        format!("scenario-group probe failed: {error:#}"),
                    );
                    true
                }
            };
            if self.child.is_none() && !group_exists {
                break;
            }

            let now = tokio::time::Instant::now();
            if !kill_sent && (now >= term_deadline || escalation.load(Ordering::SeqCst) >= 2) {
                kill_sent = true;
                if escalation.load(Ordering::SeqCst) >= 2 {
                    record_error(
                        &mut errors,
                        "second signal forced scenario SIGKILL escalation",
                    );
                }
                if let Err(error) = self.signal_group(libc::SIGKILL) {
                    record_error(
                        &mut errors,
                        format!("scenario-group SIGKILL failed: {error:#}"),
                    );
                }
            }
            if !group_deadline_latched && now >= kill_deadline {
                group_deadline_latched = true;
                record_error(
                    &mut errors,
                    format!(
                        "scenario process group {} exceeded termination budget, awaiting exit",
                        self.pgid
                    ),
                );
            }

            tokio::select! {
                changed = signals.changed() => {
                    if changed.is_err() {
                        record_error(&mut errors, "signal tracker closed during scenario cleanup");
                    }
                }
                _ = tokio::time::sleep(POLL_INTERVAL) => {}
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            bail!("{}", errors.join("; "))
        }
    }
}

struct SignalTracker {
    count: Arc<AtomicUsize>,
    updates: tokio::sync::watch::Receiver<usize>,
    task: JoinHandle<()>,
}

fn signal_is_recorded(count: &AtomicUsize, updates: &tokio::sync::watch::Receiver<usize>) -> bool {
    count.load(Ordering::SeqCst) > 0 || *updates.borrow() > 0
}

impl SignalTracker {
    fn start() -> Result<Self> {
        let mut interrupt =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        let count = Arc::new(AtomicUsize::new(0));
        let task_count = Arc::clone(&count);
        let (sender, updates) = tokio::sync::watch::channel(0_usize);
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    signal = interrupt.recv() => if signal.is_none() { break; },
                    signal = terminate.recv() => if signal.is_none() { break; },
                }
                let next = task_count.fetch_add(1, Ordering::SeqCst) + 1;
                let _ = sender.send(next);
            }
        });
        Ok(Self {
            count,
            updates,
            task,
        })
    }
}

impl Drop for SignalTracker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct ProcessGroup {
    manager: Option<ManagedProcess>,
    proxies: Vec<ManagedProcess>,
    engines: Vec<ManagedProcess>,
}

impl ProcessGroup {
    fn new() -> Self {
        Self {
            manager: None,
            proxies: Vec::new(),
            engines: Vec::new(),
        }
    }

    async fn start_one(
        spec: ServiceSpec,
        signal_count: Arc<AtomicUsize>,
        mut signals: tokio::sync::watch::Receiver<usize>,
    ) -> (
        Option<ManagedProcess>,
        Result<ForegroundWait<LifecycleObservation>>,
    ) {
        if signal_is_recorded(&signal_count, &signals) {
            return (None, Ok(ForegroundWait::Interrupted));
        }
        let mut process = match ManagedProcess::spawn(spec).await {
            Ok(process) => process,
            Err(error) => return (None, Err(error)),
        };
        let result = process
            .wait_ready(tokio::time::Instant::now() + STARTUP_TIMEOUT, &mut signals)
            .await;
        (Some(process), result)
    }

    async fn start_wave(
        specs: Vec<ServiceSpec>,
        signal_count: &Arc<AtomicUsize>,
        signals: &tokio::sync::watch::Receiver<usize>,
    ) -> (
        Vec<ManagedProcess>,
        Result<ForegroundWait<Vec<LifecycleObservation>>>,
    ) {
        let mut joins = JoinSet::new();
        for spec in specs {
            joins.spawn(Self::start_one(
                spec,
                Arc::clone(signal_count),
                signals.clone(),
            ));
        }
        let mut processes = Vec::new();
        let mut observations = Vec::new();
        let mut errors = Vec::new();
        let mut interrupted = false;
        while let Some(joined) = joins.join_next().await {
            match joined {
                Ok((process, result)) => {
                    if let Some(process) = process {
                        processes.push(process);
                    }
                    match result {
                        Ok(ForegroundWait::Completed(observation)) => {
                            observations.push(observation)
                        }
                        Ok(ForegroundWait::Interrupted) => interrupted = true,
                        Err(error) => errors.push(format!("{error:#}")),
                    }
                }
                Err(error) => errors.push(format!("startup task failed: {error}")),
            }
        }
        let result = if !errors.is_empty() {
            Err(anyhow::anyhow!(errors.join("; ")))
        } else if interrupted {
            Ok(ForegroundWait::Interrupted)
        } else {
            Ok(ForegroundWait::Completed(observations))
        };
        (processes, result)
    }

    fn unexpected_exit(&mut self) -> Result<Option<String>> {
        if let Some(manager) = self.manager.as_mut() {
            if let Some(status) = manager.try_wait()? {
                return Ok(Some(format!(
                    "manager:{} exited unexpectedly with {status}; tail: {}",
                    manager.name,
                    manager.evidence()
                )));
            }
        }
        for process in self.proxies.iter_mut().chain(self.engines.iter_mut()) {
            if let Some(status) = process.try_wait()? {
                return Ok(Some(format!(
                    "{}:{} exited unexpectedly with {status}; tail: {}",
                    process.role.label(),
                    process.name,
                    process.evidence()
                )));
            }
        }
        Ok(None)
    }

    async fn stop_wave(processes: Vec<ManagedProcess>, escalation: Arc<AtomicUsize>) -> Result<()> {
        let mut stops = JoinSet::new();
        for process in processes {
            let escalation = Arc::clone(&escalation);
            stops.spawn(process.stop_and_reap(escalation));
        }
        let mut errors = Vec::new();
        while let Some(joined) = stops.join_next().await {
            match joined {
                Ok(Ok(())) => {}
                Ok(Err(error)) => errors.push(format!("{error:#}")),
                Err(error) => errors.push(format!("stop task failed: {error}")),
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            bail!("{}", errors.join("; "))
        }
    }

    async fn shutdown(mut self, escalation: Arc<AtomicUsize>) -> Result<()> {
        let mut errors = Vec::new();
        if let Err(error) =
            Self::stop_wave(std::mem::take(&mut self.engines), Arc::clone(&escalation)).await
        {
            errors.push(format!("engine cleanup: {error:#}"));
        }
        if let Err(error) =
            Self::stop_wave(std::mem::take(&mut self.proxies), Arc::clone(&escalation)).await
        {
            errors.push(format!("proxy cleanup: {error:#}"));
        }
        if let Some(manager) = self.manager.take() {
            if let Err(error) = Self::stop_wave(vec![manager], escalation).await {
                errors.push(format!("manager cleanup: {error:#}"));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            bail!("{}", errors.join("; "))
        }
    }
}

fn combine_outcomes(primary: Option<anyhow::Error>, cleanup_errors: Vec<String>) -> Result<()> {
    match (primary, cleanup_errors.is_empty()) {
        (None, true) => Ok(()),
        (Some(primary), true) => Err(primary),
        (None, false) => bail!("foreground cleanup failed: {}", cleanup_errors.join("; ")),
        (Some(primary), false) => Err(primary).context(format!(
            "foreground cleanup also failed: {}",
            cleanup_errors.join("; ")
        )),
    }
}

async fn finish_owned(
    group: ProcessGroup,
    mut scenario: Option<ScenarioProcess>,
    signals: &SignalTracker,
    primary: Option<anyhow::Error>,
) -> Result<()> {
    let mut cleanup_errors = Vec::new();
    if let Some(scenario) = scenario.as_mut() {
        let mut updates = signals.updates.clone();
        if let Err(error) = scenario
            .terminate_and_reap(Arc::clone(&signals.count), &mut updates)
            .await
        {
            cleanup_errors.push(format!("scenario-tree cleanup: {error:#}"));
        }
    }
    if let Err(error) = group.shutdown(Arc::clone(&signals.count)).await {
        cleanup_errors.push(format!("service cleanup: {error:#}"));
    }
    combine_outcomes(primary, cleanup_errors)
}

async fn wait_for_routing_or_signal(
    targets: &[ProxyRoutingBarrierTarget],
    target_version: u64,
    timeout: Duration,
    signals: &mut tokio::sync::watch::Receiver<usize>,
) -> Result<ForegroundWait<()>> {
    if *signals.borrow() > 0 {
        return Ok(ForegroundWait::Interrupted);
    }
    tokio::select! {
        changed = signals.changed() => {
            if changed.is_err() {
                bail!("signal tracker closed during routing barrier");
            }
            Ok(ForegroundWait::Interrupted)
        }
        result = helpers::wait_for_proxy_routing_barrier(targets, target_version, timeout) => {
            result.map(|()| ForegroundWait::Completed(()))
        }
    }
}

async fn monitor_active(
    mut group: ProcessGroup,
    scenario_command: Vec<String>,
    signals: &SignalTracker,
) -> Result<()> {
    if signal_is_recorded(&signals.count, &signals.updates) {
        return finish_owned(group, None, signals, None).await;
    }
    let mut scenario = if scenario_command.is_empty() {
        None
    } else {
        match ScenarioProcess::spawn(&scenario_command).await {
            Ok(scenario) => Some(scenario),
            Err(error) => return finish_owned(group, None, signals, Some(error)).await,
        }
    };
    let mut signal_updates = signals.updates.clone();
    let primary = loop {
        match group.unexpected_exit() {
            Ok(Some(error)) => break Some(anyhow::anyhow!(error)),
            Ok(None) => {}
            Err(error) => break Some(error.context("service inspection failed")),
        }
        if let Some(scenario) = scenario.as_mut() {
            match scenario.try_wait() {
                Ok(Some(status)) => {
                    break if status.success() {
                        None
                    } else {
                        Some(anyhow::anyhow!("scenario exited with {status}"))
                    };
                }
                Ok(None) => {}
                Err(error) => break Some(error.context("scenario inspection failed")),
            }
        }
        tokio::select! {
            changed = signal_updates.changed() => {
                if changed.is_err() {
                    break Some(anyhow::anyhow!("signal tracker closed unexpectedly"));
                }
                break None;
            }
            _ = tokio::time::sleep(POLL_INTERVAL) => {}
        }
    };
    finish_owned(group, scenario, signals, primary).await
}

pub(super) async fn run(spec: RunSpec) -> Result<()> {
    let spec = spec.validate()?;
    let signals = SignalTracker::start()?;
    let mut group = ProcessGroup::new();
    let manager_spec = ServiceSpec {
        name: "primary".to_string(),
        role: ServiceRole::Manager,
        config: spec.manager_config,
        command_override: None,
        endpoint_override: None,
        process_instance_id_override: None,
        skip_endpoint_check: false,
    };
    let (manager, manager_ready) = ProcessGroup::start_one(
        manager_spec,
        Arc::clone(&signals.count),
        signals.updates.clone(),
    )
    .await;
    group.manager = manager;
    let manager_ready = match manager_ready {
        Ok(ForegroundWait::Completed(observation)) => observation,
        Ok(ForegroundWait::Interrupted) => {
            return finish_owned(group, None, &signals, None).await;
        }
        Err(error) => return finish_owned(group, None, &signals, Some(error)).await,
    };
    if signal_is_recorded(&signals.count, &signals.updates) {
        return finish_owned(group, None, &signals, None).await;
    }
    let manager_endpoint = match group.manager.as_ref() {
        Some(manager) => manager.endpoint.clone(),
        None => {
            return finish_owned(
                group,
                None,
                &signals,
                Some(anyhow::anyhow!("manager handle missing after readiness")),
            )
            .await;
        }
    };

    let proxy_specs = spec
        .proxies
        .into_iter()
        .map(|(name, config)| ServiceSpec {
            name,
            role: ServiceRole::Proxy,
            config,
            command_override: None,
            endpoint_override: None,
            process_instance_id_override: None,
            skip_endpoint_check: false,
        })
        .collect();
    let (proxies, proxy_ready) =
        ProcessGroup::start_wave(proxy_specs, &signals.count, &signals.updates).await;
    group.proxies = proxies;
    let proxy_ready = match proxy_ready {
        Ok(ForegroundWait::Completed(observations)) => observations,
        Ok(ForegroundWait::Interrupted) => {
            return finish_owned(group, None, &signals, None).await;
        }
        Err(error) => return finish_owned(group, None, &signals, Some(error)).await,
    };

    if signal_is_recorded(&signals.count, &signals.updates) {
        return finish_owned(group, None, &signals, None).await;
    }
    let engine_specs = spec
        .engine_configs
        .into_iter()
        .enumerate()
        .map(|(index, config)| ServiceSpec {
            name: format!("{}", index + 1),
            role: ServiceRole::Engine,
            config,
            command_override: None,
            endpoint_override: None,
            process_instance_id_override: None,
            skip_endpoint_check: false,
        })
        .collect();
    let (engines, engine_ready) =
        ProcessGroup::start_wave(engine_specs, &signals.count, &signals.updates).await;
    group.engines = engines;
    match engine_ready {
        Ok(ForegroundWait::Completed(_)) => {}
        Ok(ForegroundWait::Interrupted) => {
            return finish_owned(group, None, &signals, None).await;
        }
        Err(error) => return finish_owned(group, None, &signals, Some(error)).await,
    }

    if signal_is_recorded(&signals.count, &signals.updates) {
        return finish_owned(group, None, &signals, None).await;
    }
    let mut manager_signal = signals.updates.clone();
    let target_version = tokio::select! {
        changed = manager_signal.changed() => {
            if changed.is_err() {
                return finish_owned(group, None, &signals, Some(anyhow::anyhow!("signal tracker closed during manager routing-version capture"))).await;
            }
            return finish_owned(group, None, &signals, None).await;
        }
        result = helpers::get_manager_routing_table_version(&manager_endpoint) => match result {
            Ok(version) => version,
            Err(error) => return finish_owned(group, None, &signals, Some(error)).await,
        }
    };
    if signal_is_recorded(&signals.count, &signals.updates) {
        return finish_owned(group, None, &signals, None).await;
    }
    let targets = group
        .proxies
        .iter()
        .zip(proxy_ready)
        .map(|(proxy, ready)| ProxyRoutingBarrierTarget {
            name: proxy.name.clone(),
            endpoint: proxy.endpoint.clone(),
            process_instance_id: ready.process_instance_id,
        })
        .collect::<Vec<_>>();
    let mut barrier_signal = signals.updates.clone();
    match wait_for_routing_or_signal(
        &targets,
        target_version,
        STARTUP_TIMEOUT,
        &mut barrier_signal,
    )
    .await
    {
        Ok(ForegroundWait::Completed(())) => {}
        Ok(ForegroundWait::Interrupted) => {
            return finish_owned(group, None, &signals, None).await;
        }
        Err(error) => return finish_owned(group, None, &signals, Some(error)).await,
    }

    if signal_is_recorded(&signals.count, &signals.updates) {
        return finish_owned(group, None, &signals, None).await;
    }
    eprintln!(
        "[dev-run] topology ready: manager activation {}, routing version {} acknowledged by {} proxy/proxies",
        manager_ready.process_instance_id,
        target_version,
        targets.len()
    );
    monitor_active(group, spec.scenario, &signals).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::{Request, Response, Status};
    use wr_common::wruntime::lifecycle_service_server::{LifecycleService, LifecycleServiceServer};
    use wr_common::wruntime::node_service_server::{NodeService, NodeServiceServer};
    use wr_common::wruntime::*;

    #[derive(Clone)]
    struct FixtureLifecycle {
        kind: ServiceKind,
        instance: String,
        ready_at: tokio::time::Instant,
    }

    #[tonic::async_trait]
    impl LifecycleService for FixtureLifecycle {
        async fn get_status(
            &self,
            _request: Request<GetLifecycleStatusRequest>,
        ) -> std::result::Result<Response<GetLifecycleStatusResponse>, Status> {
            let state = if tokio::time::Instant::now() >= self.ready_at {
                ProcessLifecycleState::Ready
            } else {
                ProcessLifecycleState::Starting
            };
            Ok(Response::new(GetLifecycleStatusResponse {
                status: Some(LifecycleStatus {
                    state: state as i32,
                    service_kind: self.kind as i32,
                    process_instance_id: self.instance.clone(),
                    detail: state.as_str_name().to_string(),
                    ..Default::default()
                }),
            }))
        }
    }

    async fn spawn_lifecycle_fixture(
        kind: ServiceKind,
        instance: &str,
        ready_after: Duration,
    ) -> Result<(String, JoinHandle<()>)> {
        let incoming = tonic::transport::server::TcpIncoming::bind(
            "127.0.0.1:0".parse().expect("fixture bind address"),
        )?;
        let address = incoming.local_addr()?;
        let service = FixtureLifecycle {
            kind,
            instance: instance.to_string(),
            ready_at: tokio::time::Instant::now() + ready_after,
        };
        let task = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(LifecycleServiceServer::new(service))
                .serve_with_incoming(incoming)
                .await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        Ok((format!("http://{address}"), task))
    }

    fn fixture_service_spec(
        root: &Path,
        name: &str,
        role: ServiceRole,
        endpoint: String,
        instance: &str,
        script: String,
    ) -> Result<ServiceSpec> {
        let config = root.join(format!("{name}.toml"));
        std::fs::write(&config, "fixture=true\n")?;
        Ok(ServiceSpec {
            name: name.to_string(),
            role,
            config,
            command_override: Some(vec!["sh".to_string(), "-c".to_string(), script]),
            endpoint_override: Some(endpoint),
            process_instance_id_override: Some(instance.to_string()),
            skip_endpoint_check: true,
        })
    }

    #[derive(Clone)]
    struct FixtureNodeStatus {
        instance: String,
        target: u64,
        ready_at: tokio::time::Instant,
    }

    #[tonic::async_trait]
    impl NodeService for FixtureNodeStatus {
        async fn register_engine(
            &self,
            _request: Request<RegisterEngineRequest>,
        ) -> std::result::Result<Response<RegisterEngineResponse>, Status> {
            Err(Status::unimplemented("fixture"))
        }

        async fn deregister_engine(
            &self,
            _request: Request<DeregisterEngineRequest>,
        ) -> std::result::Result<Response<DeregisterEngineResponse>, Status> {
            Err(Status::unimplemented("fixture"))
        }

        async fn heartbeat(
            &self,
            _request: Request<HeartbeatRequest>,
        ) -> std::result::Result<Response<HeartbeatResponse>, Status> {
            Err(Status::unimplemented("fixture"))
        }

        async fn begin_engine_drain(
            &self,
            _request: Request<BeginEngineDrainRequest>,
        ) -> std::result::Result<Response<BeginEngineDrainResponse>, Status> {
            Err(Status::unimplemented("fixture"))
        }

        async fn get_proxy_routing_status(
            &self,
            _request: Request<GetProxyRoutingStatusRequest>,
        ) -> std::result::Result<Response<GetProxyRoutingStatusResponse>, Status> {
            Ok(Response::new(GetProxyRoutingStatusResponse {
                process_instance_id: self.instance.clone(),
                installed_routing_table_version: if tokio::time::Instant::now() >= self.ready_at {
                    self.target
                } else {
                    self.target.saturating_sub(1)
                },
            }))
        }
    }

    async fn spawn_node_fixture(
        instance: &str,
        target: u64,
        ready_after: Duration,
    ) -> Result<(String, JoinHandle<()>)> {
        let incoming = tonic::transport::server::TcpIncoming::bind(
            "127.0.0.1:0".parse().expect("fixture bind address"),
        )?;
        let address = incoming.local_addr()?;
        let service = FixtureNodeStatus {
            instance: instance.to_string(),
            target,
            ready_at: tokio::time::Instant::now() + ready_after,
        };
        let task = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(NodeServiceServer::new(service))
                .serve_with_incoming(incoming)
                .await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        Ok((format!("http://{address}"), task))
    }

    #[test]
    fn run_spec_validates_proxy_names_and_cardinality() -> Result<()> {
        let root = std::env::temp_dir().join(format!("wr-run-spec-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root)?;
        let manager = root.join("manager.toml");
        let proxy = root.join("proxy.toml");
        std::fs::write(&manager, "listen_address='127.0.0.1:1'\n")?;
        std::fs::write(&proxy, "control_address='127.0.0.1:2'\n")?;
        let base = RunSpec {
            manager_config: manager,
            proxies: vec![("primary".to_string(), proxy.clone())],
            engine_configs: vec![],
            scenario: vec!["sh".to_string(), "-c".to_string(), "exit 0".to_string()],
        };
        base.clone().validate()?;
        let mut missing = base.clone();
        missing.proxies.clear();
        assert!(missing
            .validate()
            .unwrap_err()
            .to_string()
            .contains("one or more"));
        let mut duplicate = base.clone();
        duplicate
            .proxies
            .push(("primary".to_string(), proxy.clone()));
        assert!(duplicate
            .validate()
            .unwrap_err()
            .to_string()
            .contains("duplicate"));
        let mut invalid = base;
        invalid.proxies[0].0 = "bad=name".to_string();
        assert!(invalid
            .validate()
            .unwrap_err()
            .to_string()
            .contains("invalid proxy name"));
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn fixed_startup_order_and_within_wave_readiness_are_concurrent() -> Result<()> {
        let root = std::env::temp_dir().join(format!("wr-start-waves-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root)?;
        let log = root.join("starts.log");
        let (manager_endpoint, manager_server) = spawn_lifecycle_fixture(
            ServiceKind::Manager,
            "manager-fixture",
            Duration::from_millis(120),
        )
        .await?;
        let manager_spec = fixture_service_spec(
            &root,
            "manager",
            ServiceRole::Manager,
            manager_endpoint,
            "manager-fixture",
            format!(
                "echo manager >> {}; trap 'exit 0' TERM; while :; do sleep 1; done",
                log.display()
            ),
        )?;
        let (_signal_tx, signal_rx) = tokio::sync::watch::channel(0_usize);
        let (manager, ready) = ProcessGroup::start_one(
            manager_spec,
            Arc::new(AtomicUsize::new(0)),
            signal_rx.clone(),
        )
        .await;
        assert!(matches!(ready?, ForegroundWait::Completed(_)));
        assert_eq!(std::fs::read_to_string(&log)?.trim(), "manager");

        let (proxy_a_endpoint, proxy_a_server) = spawn_lifecycle_fixture(
            ServiceKind::Proxy,
            "proxy-a-fixture",
            Duration::from_millis(220),
        )
        .await?;
        let (proxy_b_endpoint, proxy_b_server) = spawn_lifecycle_fixture(
            ServiceKind::Proxy,
            "proxy-b-fixture",
            Duration::from_millis(220),
        )
        .await?;
        let proxy_specs = vec![
            fixture_service_spec(
                &root,
                "proxy-a",
                ServiceRole::Proxy,
                proxy_a_endpoint,
                "proxy-a-fixture",
                format!(
                    "echo proxy-a >> {}; trap 'exit 0' TERM; while :; do sleep 1; done",
                    log.display()
                ),
            )?,
            fixture_service_spec(
                &root,
                "proxy-b",
                ServiceRole::Proxy,
                proxy_b_endpoint,
                "proxy-b-fixture",
                format!(
                    "echo proxy-b >> {}; trap 'exit 0' TERM; while :; do sleep 1; done",
                    log.display()
                ),
            )?,
        ];
        let started = tokio::time::Instant::now();
        let (proxies, ready) =
            ProcessGroup::start_wave(proxy_specs, &Arc::new(AtomicUsize::new(0)), &signal_rx).await;
        assert!(
            matches!(ready?, ForegroundWait::Completed(ref observations) if observations.len() == 2)
        );
        assert!(
            started.elapsed() < Duration::from_millis(380),
            "proxy readiness waits ran serially: {:?}",
            started.elapsed()
        );
        let starts = std::fs::read_to_string(&log)?;
        assert!(starts.lines().next().is_some_and(|line| line == "manager"));
        assert!(starts.contains("proxy-a"));
        assert!(starts.contains("proxy-b"));

        let group = ProcessGroup {
            manager,
            proxies,
            engines: Vec::new(),
        };
        group.shutdown(Arc::new(AtomicUsize::new(0))).await?;
        manager_server.abort();
        proxy_a_server.abort();
        proxy_b_server.abort();
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn recorded_signal_prevents_later_wave_and_scenario_spawns() -> Result<()> {
        let root = std::env::temp_dir().join(format!("wr-recorded-signal-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root)?;
        let wave_marker = root.join("wave-spawned");
        let spec = fixture_service_spec(
            &root,
            "must-not-spawn",
            ServiceRole::Proxy,
            "http://127.0.0.1:1".to_string(),
            "must-not-spawn",
            format!("touch '{}'; exit 0", wave_marker.display()),
        )?;
        // Exercise the split-state window in SignalTracker::start: the
        // authoritative count is already incremented while watch publication
        // is still stale. No later-wave spawn is permitted in this state.
        let signal_count = Arc::new(AtomicUsize::new(1));
        let (_sender, recorded_signal) = tokio::sync::watch::channel(0_usize);
        let (process, result) = ProcessGroup::start_one(spec, signal_count, recorded_signal).await;
        assert!(
            process.is_none(),
            "recorded signal still spawned a later-wave child"
        );
        assert!(matches!(result?, ForegroundWait::Interrupted));
        assert!(
            !wave_marker.exists(),
            "later-wave spawn side effect occurred"
        );

        let scenario_marker = root.join("scenario-spawned");
        let signals = SignalTracker::start()?;
        signals.count.store(1, Ordering::SeqCst);
        monitor_active(
            ProcessGroup::new(),
            vec![
                "sh".to_string(),
                "-c".to_string(),
                format!("touch '{}'", scenario_marker.display()),
            ],
            &signals,
        )
        .await?;
        assert!(
            !scenario_marker.exists(),
            "recorded signal still spawned the scenario"
        );
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn readiness_validates_activation_and_exit_before_ready_retains_tail() -> Result<()> {
        let root = std::env::temp_dir().join(format!("wr-ready-fixture-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root)?;
        let (endpoint, server) =
            spawn_lifecycle_fixture(ServiceKind::Engine, "wrong-activation", Duration::ZERO)
                .await?;
        let spec = fixture_service_spec(
            &root,
            "activation",
            ServiceRole::Engine,
            endpoint,
            "expected-activation",
            "trap 'exit 0' TERM; while :; do sleep 1; done".to_string(),
        )?;
        let mut process = ManagedProcess::spawn(spec).await?;
        let (_tx, mut signals) = tokio::sync::watch::channel(0_usize);
        let error = process
            .wait_ready(
                tokio::time::Instant::now() + Duration::from_secs(1),
                &mut signals,
            )
            .await
            .expect_err("wrong activation satisfied READY");
        assert!(error.to_string().contains("activation mismatch"));
        process.stop_and_reap(Arc::new(AtomicUsize::new(0))).await?;
        server.abort();

        let config = root.join("exit.toml");
        std::fs::write(&config, "fixture=true\n")?;
        let mut child = Command::new("sh")
            .arg("-c")
            .arg("echo EXIT_BEFORE_READY; exit 23")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let tail = Arc::new(Mutex::new(VecDeque::new()));
        let mut output_tasks = Vec::new();
        if let Some(stdout) = child.stdout.take() {
            output_tasks.push(stream_output(
                stdout,
                "engine:exit-tail".to_string(),
                "stdout",
                Arc::clone(&tail),
            ));
        }
        if let Some(stderr) = child.stderr.take() {
            output_tasks.push(stream_output(
                stderr,
                "engine:exit-tail".to_string(),
                "stderr",
                Arc::clone(&tail),
            ));
        }
        let mut process = ManagedProcess {
            name: "exit-tail".to_string(),
            role: ServiceRole::Engine,
            config,
            endpoint: "http://127.0.0.1:1".to_string(),
            manager_tls: false,
            process_instance_id: "exit-tail".to_string(),
            child: Some(child),
            output_tasks,
            tail,
            exit_status: None,
        };
        tokio::time::sleep(Duration::from_millis(30)).await;
        let error = process
            .wait_ready(
                tokio::time::Instant::now() + Duration::from_secs(1),
                &mut signals,
            )
            .await
            .expect_err("exit-before-ready fixture reached READY");
        let evidence = format!("{error:#}");
        assert!(evidence.contains("exited before READY"));
        assert!(evidence.contains("EXIT_BEFORE_READY"));
        process
            .stop_and_reap(Arc::new(AtomicUsize::new(0)))
            .await
            .ok();
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn shared_routing_deadline_gates_scenario_until_final_proxy() -> Result<()> {
        let target_version = 9;
        let (fast_endpoint, fast_server) =
            spawn_node_fixture("fast", target_version, Duration::ZERO).await?;
        let (slow_endpoint, slow_server) =
            spawn_node_fixture("slow", target_version, Duration::from_millis(220)).await?;
        let targets = vec![
            ProxyRoutingBarrierTarget {
                name: "fast".to_string(),
                endpoint: fast_endpoint,
                process_instance_id: "fast".to_string(),
            },
            ProxyRoutingBarrierTarget {
                name: "slow".to_string(),
                endpoint: slow_endpoint,
                process_instance_id: "slow".to_string(),
            },
        ];
        let started = tokio::time::Instant::now();
        helpers::wait_for_proxy_routing_barrier(&targets, target_version, Duration::from_secs(1))
            .await?;
        assert!(started.elapsed() >= Duration::from_millis(180));
        let scenario = Command::new("sh").arg("-c").arg("exit 0").status().await?;
        assert!(
            scenario.success(),
            "scenario launched only after final routing acknowledgement"
        );

        let (never_endpoint, never_server) =
            spawn_node_fixture("never", target_version, Duration::from_secs(10)).await?;
        let timeout_target = [ProxyRoutingBarrierTarget {
            name: "never".to_string(),
            endpoint: never_endpoint,
            process_instance_id: "never".to_string(),
        }];
        let timeout_started = tokio::time::Instant::now();
        let error = helpers::wait_for_proxy_routing_barrier(
            &timeout_target,
            target_version,
            Duration::from_millis(120),
        )
        .await
        .expect_err("slow proxy escaped the shared deadline");
        assert!(timeout_started.elapsed() < Duration::from_millis(300));
        assert!(format!("{error:#}").contains("deadline"));
        fast_server.abort();
        slow_server.abort();
        never_server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn stop_wave_is_concurrent_and_reaps_all_children() -> Result<()> {
        let processes = vec![
            ManagedProcess::fixture(
                "a",
                ServiceRole::Engine,
                "trap 'exit 0' TERM; while :; do sleep 1; done",
            )?,
            ManagedProcess::fixture(
                "b",
                ServiceRole::Engine,
                "trap 'exit 0' TERM; while :; do sleep 1; done",
            )?,
        ];
        tokio::time::sleep(Duration::from_millis(50)).await;
        let started = tokio::time::Instant::now();
        ProcessGroup::stop_wave(processes, Arc::new(AtomicUsize::new(0))).await?;
        assert!(started.elapsed() < Duration::from_secs(2));
        Ok(())
    }

    #[tokio::test]
    async fn term_timeout_escalates_visibly_and_still_reaps() -> Result<()> {
        let process = ManagedProcess::fixture(
            "resistant",
            ServiceRole::Engine,
            "trap '' TERM; while :; do :; done",
        )?;
        tokio::time::sleep(Duration::from_millis(25)).await;
        let error = process
            .stop_and_reap_with_budgets(
                Arc::new(AtomicUsize::new(0)),
                StopBudgets {
                    internal: Duration::from_millis(40),
                    term: Duration::from_millis(40),
                    kill: Duration::from_millis(100),
                },
                StopPolicyHooks::default(),
            )
            .await
            .expect_err("TERM-resistant fixture unexpectedly stopped gracefully");
        assert!(error.to_string().contains("required SIGKILL fallback"));
        Ok(())
    }

    #[tokio::test]
    async fn scenario_termination_covers_descendants() -> Result<()> {
        let root = std::env::temp_dir().join(format!("wr-scenario-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root)?;
        let pid_file = root.join("pid");
        let command = vec![
            "sh".to_string(),
            "-c".to_string(),
            "sleep 30 & pid=$!; printf '%s\\n' \"$pid\" > \"$1.tmp\" && mv \"$1.tmp\" \"$1\"; wait"
                .to_string(),
            "scenario-fixture".to_string(),
            pid_file.to_string_lossy().into_owned(),
        ];
        let mut scenario = ScenarioProcess::spawn(&command).await?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        while !pid_file.exists() {
            anyhow::ensure!(
                tokio::time::Instant::now() < deadline,
                "fixture descendant did not start"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let pid: i32 = std::fs::read_to_string(&pid_file)?.trim().parse()?;
        let signals = SignalTracker::start()?;
        let mut updates = signals.updates.clone();
        scenario
            .terminate_and_reap(Arc::clone(&signals.count), &mut updates)
            .await?;
        assert_eq!(
            unsafe { libc::kill(pid, 0) },
            -1,
            "scenario descendant survived cleanup"
        );
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn zero_descendant_one_shot_preserves_success() -> Result<()> {
        let signals = SignalTracker::start()?;
        let result = monitor_active(
            ProcessGroup::new(),
            vec!["sh".to_string(), "-c".to_string(), "exit 0".to_string()],
            &signals,
        )
        .await;
        result.context("zero-descendant one-shot must succeed")
    }

    #[tokio::test]
    async fn external_boundary_is_latched_before_owner_completes_and_survives_reap() -> Result<()> {
        let root = std::env::temp_dir().join(format!("wr-boundary-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root)?;
        let release = root.join("release");
        let process = ManagedProcess::fixture(
            "boundary",
            ServiceRole::Engine,
            &format!(
                "trap '' TERM; while [ ! -f '{}' ]; do sleep 0.01; done; exit 0",
                release.display()
            ),
        )?;
        let pid = process
            .child
            .as_ref()
            .and_then(Child::id)
            .context("fixture PID missing")?;
        tokio::time::sleep(Duration::from_millis(25)).await;
        let (notice_tx, notice_rx) = tokio::sync::oneshot::channel();
        let owner = tokio::spawn(process.stop_and_reap_with_budgets(
            Arc::new(AtomicUsize::new(0)),
            StopBudgets {
                internal: Duration::from_millis(20),
                term: Duration::from_millis(20),
                kill: Duration::from_millis(20),
            },
            StopPolicyHooks {
                deadline_notice: Some(notice_tx),
                suppress_signals: true,
            },
        ));
        let notice = tokio::time::timeout(Duration::from_secs(1), notice_rx)
            .await
            .context("deadline evidence was not emitted at the boundary")??;
        assert!(notice.contains("deadline exceeded, awaiting reap"));
        assert!(
            !owner.is_finished(),
            "owner completed before controlled post-boundary exit"
        );
        assert_eq!(
            unsafe { libc::kill(pid as i32, 0) },
            0,
            "fixture was not held past boundary"
        );

        std::fs::write(&release, b"release")?;
        let error = owner
            .await?
            .expect_err("latched deadline failure was lost after clean reap");
        let evidence = format!("{error:#}");
        assert!(evidence.contains("deadline exceeded, awaiting reap"));
        assert!(evidence.contains("final status=exit status: 0"));
        assert_eq!(
            unsafe { libc::kill(pid as i32, 0) },
            -1,
            "owner returned before reap"
        );
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn unexpected_service_exit_is_primary_and_scenario_tree_is_cleaned() -> Result<()> {
        let root = std::env::temp_dir().join(format!("wr-service-exit-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root)?;
        let pid_file = root.join("descendant.pid");
        let group = ProcessGroup {
            manager: None,
            proxies: Vec::new(),
            engines: vec![ManagedProcess::fixture(
                "unexpected",
                ServiceRole::Engine,
                "sleep 0.2; exit 17",
            )?],
        };
        let signals = SignalTracker::start()?;
        let result = monitor_active(
            group,
            vec![
                "sh".to_string(),
                "-c".to_string(),
                "sleep 30 & pid=$!; printf '%s\\n' \"$pid\" > \"$1.tmp\" && mv \"$1.tmp\" \"$1\"; wait"
                    .to_string(),
                "scenario-fixture".to_string(),
                pid_file.to_string_lossy().into_owned(),
            ],
            &signals,
        )
        .await;
        let error = result.expect_err("unexpected service exit was not primary");
        assert!(format!("{error:#}").contains("engine:unexpected exited unexpectedly"));
        let pid: i32 = std::fs::read_to_string(&pid_file)?.trim().parse()?;
        assert_eq!(
            unsafe { libc::kill(pid, 0) },
            -1,
            "scenario descendant survived service failure"
        );
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn monitor_reports_cleanup_only_and_combined_failures() -> Result<()> {
        let cleanup_only_group = ProcessGroup {
            manager: None,
            proxies: Vec::new(),
            engines: vec![ManagedProcess::fixture(
                "cleanup-only",
                ServiceRole::Engine,
                "trap 'exit 9' TERM; while :; do sleep 1; done",
            )?],
        };
        tokio::time::sleep(Duration::from_millis(30)).await;
        let signals = SignalTracker::start()?;
        let cleanup_only = monitor_active(
            cleanup_only_group,
            vec!["sh".to_string(), "-c".to_string(), "exit 0".to_string()],
            &signals,
        )
        .await
        .expect_err("cleanup-only failure reported success");
        assert!(format!("{cleanup_only:#}").contains("foreground cleanup failed"));

        let both_group = ProcessGroup {
            manager: None,
            proxies: Vec::new(),
            engines: vec![ManagedProcess::fixture(
                "both",
                ServiceRole::Engine,
                "trap 'exit 9' TERM; while :; do sleep 1; done",
            )?],
        };
        tokio::time::sleep(Duration::from_millis(30)).await;
        let both = monitor_active(
            both_group,
            vec!["sh".to_string(), "-c".to_string(), "exit 7".to_string()],
            &signals,
        )
        .await
        .expect_err("combined failure reported success");
        let evidence = format!("{both:#}");
        assert!(evidence.contains("scenario exited with exit status: 7"));
        assert!(evidence.contains("foreground cleanup also failed"));
        assert!(evidence.contains("cleanup failed with exit status: 9"));
        Ok(())
    }

    #[tokio::test]
    #[ignore = "subprocess fixture entrypoint; invoked by service_parent_death_protection_terminates_child"]
    async fn parent_death_fixture_owner() -> Result<()> {
        let Some(root) = std::env::var_os("WRT_PARENT_DEATH_FIXTURE_ROOT") else {
            return Ok(());
        };
        let root = PathBuf::from(root);
        let child_pid = root.join("child.pid");
        let term_marker = root.join("child.term");
        let owner_ready = root.join("owner.ready");
        let config = root.join("fixture.toml");
        std::fs::write(&config, "fixture=true\n")?;
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        drop(listener);
        let spec = ServiceSpec {
            name: "pdeath".to_string(),
            role: ServiceRole::Engine,
            config,
            command_override: Some(vec![
                "sh".to_string(),
                "-c".to_string(),
                format!(
                    "echo $$ > '{}'; trap 'echo TERM > \"{}\"; exit 0' TERM; while :; do sleep 0.1; done",
                    child_pid.display(),
                    term_marker.display()
                ),
            ]),
            endpoint_override: Some(format!("http://{address}")),
            process_instance_id_override: Some("pdeath-fixture".to_string()),
            skip_endpoint_check: false,
        };
        let _owned = ManagedProcess::spawn(spec).await?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while !child_pid.exists() {
            anyhow::ensure!(
                tokio::time::Instant::now() < deadline,
                "fixture child PID not written"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        std::fs::write(owner_ready, b"ready")?;
        std::future::pending::<()>().await;
        Ok(())
    }

    #[tokio::test]
    async fn service_parent_death_protection_terminates_child() -> Result<()> {
        let root = std::env::temp_dir().join(format!("wr-pdeath-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root)?;
        let executable = std::env::current_exe()?;
        let mut owner = Command::new(executable)
            .arg("--exact")
            .arg("cmd::foreground_runner::tests::parent_death_fixture_owner")
            .arg("--ignored")
            .arg("--nocapture")
            .env("WRT_PARENT_DEATH_FIXTURE_ROOT", &root)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        let ready = root.join("owner.ready");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while !ready.exists() {
            if let Some(status) = owner.try_wait()? {
                bail!("fixture owner exited before readiness with {status}");
            }
            anyhow::ensure!(
                tokio::time::Instant::now() < deadline,
                "fixture owner did not become ready"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let child_pid: i32 = std::fs::read_to_string(root.join("child.pid"))?
            .trim()
            .parse()?;
        owner.start_kill()?;
        let owner_status = owner.wait().await?;
        assert!(!owner_status.success(), "fixture owner was not killed");

        let termination_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while unsafe { libc::kill(child_pid, 0) } == 0 {
            anyhow::ensure!(
                tokio::time::Instant::now() < termination_deadline,
                "service child survived foreground-owner death"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::ESRCH));
        assert!(
            root.join("child.term").exists(),
            "service child did not receive PR_SET_PDEATHSIG SIGTERM"
        );
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "sends SIGINT to the test process; run serially via just test-lifecycle-runners"]
    async fn real_signal_interrupts_readiness_and_reaps_started_child() -> Result<()> {
        let root = std::env::temp_dir().join(format!("wr-start-signal-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root)?;
        let (endpoint, server) = spawn_lifecycle_fixture(
            ServiceKind::Manager,
            "slow-manager",
            Duration::from_secs(10),
        )
        .await?;
        let spec = fixture_service_spec(
            &root,
            "slow-manager",
            ServiceRole::Manager,
            endpoint,
            "slow-manager",
            "trap 'exit 0' TERM; while :; do sleep 1; done".to_string(),
        )?;
        let signals = SignalTracker::start()?;
        tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            unsafe { libc::kill(libc::getpid(), libc::SIGINT) };
        });
        let started = tokio::time::Instant::now();
        let (process, result) =
            ProcessGroup::start_one(spec, Arc::clone(&signals.count), signals.updates.clone())
                .await;
        assert!(matches!(result?, ForegroundWait::Interrupted));
        assert!(started.elapsed() < Duration::from_secs(1));
        let group = ProcessGroup {
            manager: process,
            proxies: Vec::new(),
            engines: Vec::new(),
        };
        finish_owned(group, None, &signals, None).await?;
        server.abort();
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "sends SIGINT to the test process; run serially via just test-lifecycle-runners"]
    async fn real_signal_interrupts_shared_routing_barrier() -> Result<()> {
        let (endpoint, server) = spawn_node_fixture("slow", 5, Duration::from_secs(10)).await?;
        let target = [ProxyRoutingBarrierTarget {
            name: "slow".to_string(),
            endpoint,
            process_instance_id: "slow".to_string(),
        }];
        let signals = SignalTracker::start()?;
        tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            unsafe { libc::kill(libc::getpid(), libc::SIGINT) };
        });
        let started = tokio::time::Instant::now();
        let mut updates = signals.updates.clone();
        let result =
            wait_for_routing_or_signal(&target, 5, Duration::from_secs(10), &mut updates).await?;
        assert!(matches!(result, ForegroundWait::Interrupted));
        assert!(started.elapsed() < Duration::from_secs(1));
        server.abort();
        Ok(())
    }

    #[tokio::test]
    #[ignore = "sends SIGINT to the test process; run serially via just test-lifecycle-runners"]
    async fn real_signal_interrupts_no_scenario_mode_and_reaps_services() -> Result<()> {
        let group = ProcessGroup {
            manager: None,
            proxies: Vec::new(),
            engines: vec![ManagedProcess::fixture(
                "signal",
                ServiceRole::Engine,
                "trap 'exit 0' TERM; while :; do sleep 1; done",
            )?],
        };
        let signals = SignalTracker::start()?;
        tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            unsafe { libc::kill(libc::getpid(), libc::SIGINT) };
        });
        monitor_active(group, Vec::new(), &signals).await
    }

    #[tokio::test]
    #[ignore = "sends SIGINT to the test process; run serially via just test-lifecycle-runners"]
    async fn real_second_signal_escalates_scenario_cleanup() -> Result<()> {
        let signals = SignalTracker::start()?;
        tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            unsafe { libc::kill(libc::getpid(), libc::SIGINT) };
            tokio::time::sleep(Duration::from_millis(100)).await;
            unsafe { libc::kill(libc::getpid(), libc::SIGINT) };
        });
        let started = tokio::time::Instant::now();
        let error = monitor_active(
            ProcessGroup::new(),
            vec![
                "sh".to_string(),
                "-c".to_string(),
                "trap '' TERM; while :; do :; done".to_string(),
            ],
            &signals,
        )
        .await
        .expect_err("second-signal escalation was not visible");
        assert!(started.elapsed() < Duration::from_secs(3));
        assert!(format!("{error:#}").contains("second signal forced scenario SIGKILL"));
        Ok(())
    }

    #[test]
    fn failure_precedence_preserves_primary_and_cleanup_evidence() {
        let error = combine_outcomes(
            Some(anyhow::anyhow!("scenario failed")),
            vec!["engine cleanup failed".to_string()],
        )
        .unwrap_err();
        let evidence = format!("{error:#}");
        assert!(evidence.contains("scenario failed"));
        assert!(evidence.contains("engine cleanup failed"));
        assert!(combine_outcomes(None, vec!["cleanup only".to_string()]).is_err());
    }
}
