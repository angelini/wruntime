use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::future::Future;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UnixListener, UnixStream};
use tokio::process::{Child, Command};
use uuid::Uuid;
use wr_common::process_lifecycle::PROCESS_INSTANCE_ID_ENV;
use wr_common::wruntime::{ProcessLifecycleState, ServiceKind};

use super::helpers;
use crate::client;

pub const LEGACY_PID_FILE: &str = ".wr-dev.pid";
const SOCKET_FILE: &str = "supervisor.sock";
const LOCK_FILE: &str = "supervisor.lock";
const STATE_FILE: &str = "supervisor.json";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);
const INTERNAL_STOP_BUDGET: Duration = Duration::from_secs(30);
const TERM_GRACE: Duration = Duration::from_secs(10);
const KILL_GRACE: Duration = Duration::from_secs(5);

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum Request {
    Up {
        manager_config: String,
        proxy_config: String,
    },
    DeployEngine {
        config: String,
    },
    StartProxy {
        name: String,
        config: String,
    },
    Status,
    Ping,
    Down,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChildView {
    pub role: String,
    pub config: String,
    pub lifecycle_endpoint: String,
    pub process_instance_id: Option<String>,
    pub pid: Option<u32>,
    pub lifecycle_state: Option<String>,
    pub exit_status: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub ok: bool,
    pub outcome: String,
    pub error: Option<String>,
    pub children: Vec<ChildView>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SupervisorState {
    schema_version: u32,
    supervisor_pid: u32,
    socket: String,
    outcome: String,
    children: Vec<ChildView>,
}

#[derive(Clone, Copy)]
struct StopBudgets {
    internal: Duration,
    term: Duration,
    kill: Duration,
}

const STOP_BUDGETS: StopBudgets = StopBudgets {
    internal: INTERNAL_STOP_BUDGET,
    term: TERM_GRACE,
    kill: KILL_GRACE,
};

struct OwnedChild {
    role: String,
    config: String,
    endpoint: String,
    manager_tls: bool,
    process_instance_id: Option<String>,
    last_state: Option<String>,
    child: Option<Child>,
    pid: Option<u32>,
    exit_status: Option<String>,
    requested_stop: bool,
}

impl OwnedChild {
    fn view(&self) -> ChildView {
        ChildView {
            role: self.role.clone(),
            config: self.config.clone(),
            lifecycle_endpoint: self.endpoint.clone(),
            process_instance_id: self.process_instance_id.clone(),
            pid: self.pid,
            lifecycle_state: self.last_state.clone(),
            exit_status: self.exit_status.clone(),
        }
    }

    fn tls(&self) -> Option<&'static wr_common::node::TlsConfig> {
        self.manager_tls.then(client::tls_config).flatten()
    }
}

fn socket_path(state_dir: &Path) -> PathBuf {
    state_dir.join(SOCKET_FILE)
}

fn state_path(state_dir: &Path) -> PathBuf {
    state_dir.join(STATE_FILE)
}

fn lock_path(state_dir: &Path) -> PathBuf {
    state_dir.join(LOCK_FILE)
}

pub fn prepare_state_dir(state_dir: &Path) -> Result<PathBuf> {
    std::fs::create_dir_all(state_dir).with_context(|| {
        format!(
            "failed to create dev state directory {}",
            state_dir.display()
        )
    })?;
    let state_dir = std::fs::canonicalize(state_dir).with_context(|| {
        format!(
            "failed to resolve dev state directory {}",
            state_dir.display()
        )
    })?;
    let legacy = state_dir.join(LEGACY_PID_FILE);
    if legacy.exists() {
        bail!(
            "legacy dev PID state {} is not supported; stop those processes explicitly and remove the file",
            legacy.display()
        );
    }
    Ok(state_dir)
}

fn endpoint_from_config(config: &Path, role: &str) -> Result<(String, bool)> {
    let text = std::fs::read_to_string(config)
        .with_context(|| format!("failed to read service config {}", config.display()))?;
    let value: toml::Value = toml::from_str(&text)
        .with_context(|| format!("failed to parse service config {}", config.display()))?;
    let field = match role {
        "manager" | "engine" => value.get("listen_address").and_then(toml::Value::as_str),
        "proxy" => value
            .get("control_address")
            .and_then(toml::Value::as_str)
            .or_else(|| {
                value
                    .get("node")
                    .and_then(|node| node.get("control_address"))
                    .and_then(toml::Value::as_str)
            }),
        _ => None,
    }
    .ok_or_else(|| anyhow::anyhow!("{role} config has no lifecycle control address"))?;
    let address = helpers::normalize_address(field);
    let scheme = if role == "manager" { "https" } else { "http" };
    Ok((format!("{scheme}://{address}"), role == "manager"))
}

async fn ensure_endpoint_unoccupied(endpoint: &str) -> Result<()> {
    let address = endpoint
        .split_once("://")
        .map(|(_, address)| address)
        .context("lifecycle endpoint has no URL scheme")?;
    match tokio::time::timeout(Duration::from_millis(500), TcpStream::connect(address)).await {
        Ok(Ok(_)) => bail!(
            "lifecycle endpoint {endpoint} already accepts connections; refusing to spawn an unowned process on the same listener"
        ),
        Ok(Err(error)) if error.kind() == io::ErrorKind::ConnectionRefused => Ok(()),
        Ok(Err(error)) => Err(error)
            .with_context(|| format!("could not prove lifecycle endpoint {endpoint} is unoccupied")),
        Err(_) => bail!(
            "timed out proving lifecycle endpoint {endpoint} is unoccupied before spawn"
        ),
    }
}

fn service_kind_for_role(role: &str) -> ServiceKind {
    match role {
        "manager" => ServiceKind::Manager,
        "proxy" => ServiceKind::Proxy,
        "engine" => ServiceKind::Engine,
        _ => unreachable!("validated service role"),
    }
}

fn validate_child_observation(
    child: &OwnedChild,
    observation: &helpers::LifecycleObservation,
) -> Result<ProcessLifecycleState> {
    let expected_kind = service_kind_for_role(&child.role);
    let observed_kind = observation.service_kind_enum()?;
    if observed_kind != expected_kind {
        bail!(
            "{} lifecycle endpoint reported service kind {}, expected {}",
            child.role,
            observed_kind.as_str_name(),
            expected_kind.as_str_name()
        );
    }
    let expected_instance = child
        .process_instance_id
        .as_deref()
        .context("owned child has no launcher-pinned lifecycle instance")?;
    if observation.process_instance_id != expected_instance {
        bail!(
            "{} lifecycle endpoint reported instance {}, expected launcher-pinned instance {expected_instance}",
            child.role,
            observation.process_instance_id
        );
    }
    observation.state_enum()
}

fn binary_for_role(role: &str) -> &'static str {
    match role {
        "manager" => "wr-manager",
        "proxy" => "wr-proxy",
        "engine" => "wr-engine",
        _ => unreachable!("validated service role"),
    }
}

fn resolve_binary(name: &str) -> String {
    let local = format!("./target/debug/{name}");
    if Path::new(&local).exists() {
        local
    } else {
        name.to_string()
    }
}

fn atomic_write_state(
    state_dir: &Path,
    outcome: &str,
    children: &BTreeMap<String, OwnedChild>,
) -> Result<()> {
    let path = state_path(state_dir);
    let temporary = state_dir.join(format!(".{STATE_FILE}.tmp"));
    let state = SupervisorState {
        schema_version: 1,
        supervisor_pid: std::process::id(),
        socket: socket_path(state_dir).display().to_string(),
        outcome: outcome.to_string(),
        children: children.values().map(OwnedChild::view).collect(),
    };
    std::fs::write(&temporary, serde_json::to_vec_pretty(&state)?)?;
    std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600))?;
    std::fs::rename(&temporary, &path)?;
    Ok(())
}

async fn spawn_child(role: &str, config: &Path) -> Result<OwnedChild> {
    let (endpoint, manager_tls) = endpoint_from_config(config, role)?;
    let binary = resolve_binary(binary_for_role(role));
    let expected_instance = format!("dev-supervisor-{}", Uuid::new_v4());
    let mut command = Command::new(&binary);
    command
        .env(PROCESS_INSTANCE_ID_ENV, &expected_instance)
        .arg(config)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    unsafe {
        command.as_std_mut().pre_exec(|| {
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) != 0 {
                return Err(io::Error::last_os_error());
            }
            if libc::getppid() == 1 {
                return Err(io::Error::other("dev supervisor exited before child exec"));
            }
            Ok(())
        });
    }
    let child = command
        .spawn()
        .with_context(|| format!("failed to start {binary} {}", config.display()))?;
    let pid = child.id();
    Ok(OwnedChild {
        role: role.to_string(),
        config: config.display().to_string(),
        endpoint,
        manager_tls,
        process_instance_id: Some(expected_instance),
        last_state: Some("STARTING".to_string()),
        child: Some(child),
        pid,
        exit_status: None,
        requested_stop: false,
    })
}

fn canonical_config(config: &str) -> Result<PathBuf> {
    std::fs::canonicalize(config).with_context(|| format!("service config not found: {config}"))
}

async fn wait_child_ready(child: &mut OwnedChild) -> Result<()> {
    let deadline = tokio::time::Instant::now() + STARTUP_TIMEOUT;
    let mut last_evidence = "no lifecycle observation".to_string();
    loop {
        let process = child
            .child
            .as_mut()
            .context("startup child handle disappeared before readiness")?;
        if let Some(status) = process
            .try_wait()
            .context("failed to inspect child process")?
        {
            child.exit_status = Some(status.to_string());
            child.child = None;
            bail!(
                "{} pid {:?} exited before READY with {status}; last lifecycle evidence: {last_evidence}",
                child.role,
                child.pid
            );
        }
        match tokio::time::timeout_at(
            deadline,
            helpers::get_lifecycle_status(&child.endpoint, child.tls()),
        )
        .await
        {
            Ok(Ok(observation)) => {
                let state = validate_child_observation(child, &observation)?;
                child.last_state = Some(state.as_str_name().to_string());
                last_evidence = format!(
                    "state={} instance={} detail={}",
                    state.as_str_name(),
                    observation.process_instance_id,
                    observation.detail
                );
                if state == ProcessLifecycleState::Ready {
                    return Ok(());
                }
                if matches!(
                    state,
                    ProcessLifecycleState::Draining | ProcessLifecycleState::Stopping
                ) {
                    bail!(
                        "{} reached terminal-before-ready: {last_evidence}",
                        child.role
                    );
                }
            }
            Ok(Err(error)) => last_evidence = format!("transport/query failure: {error:#}"),
            Err(_) => break,
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }
        tokio::time::sleep_until((now + Duration::from_millis(200)).min(deadline)).await;
    }
    bail!(
        "{} pid {:?} did not reach READY within {:?}; last evidence: {last_evidence}",
        child.role,
        child.pid,
        STARTUP_TIMEOUT
    )
}

async fn start_service(
    state_dir: &Path,
    children: &mut BTreeMap<String, OwnedChild>,
    key: String,
    role: &str,
    config: &str,
) -> Result<()> {
    let config = canonical_config(config)?;
    let config_name = config.display().to_string();
    if let Some(existing) = children.get_mut(&key) {
        if existing.config != config_name {
            bail!(
                "{role} role conflict: {} is already owned with config {}",
                key,
                existing.config
            );
        }
        if existing.child.is_none() {
            bail!(
                "{role} {} previously exited with {}; diagnostic state was retained",
                existing.config,
                existing.exit_status.as_deref().unwrap_or("unknown status")
            );
        }
        let observation = helpers::get_lifecycle_status(&existing.endpoint, existing.tls()).await?;
        let state = validate_child_observation(existing, &observation)?;
        existing.last_state = Some(state.as_str_name().to_string());
        if state != ProcessLifecycleState::Ready {
            bail!(
                "existing {role} is not READY: {}",
                observation.state_name()?
            );
        }
        return Ok(());
    }
    let (endpoint, _) = endpoint_from_config(&config, role)?;
    if let Some(conflict) = children
        .values()
        .find(|child| child.child.is_some() && child.endpoint == endpoint)
    {
        bail!(
            "lifecycle endpoint conflict: {endpoint} is already owned by {} ({})",
            conflict.role,
            conflict.config
        );
    }
    ensure_endpoint_unoccupied(&endpoint).await?;
    let owned = spawn_child(role, &config).await?;
    children.insert(key.clone(), owned);
    atomic_write_state(state_dir, "starting", children)?;
    let readiness = wait_child_ready(children.get_mut(&key).expect("inserted child")).await;
    atomic_write_state(
        state_dir,
        if readiness.is_ok() {
            "running"
        } else {
            "failed"
        },
        children,
    )?;
    readiness
}

fn refresh_exits(children: &mut BTreeMap<String, OwnedChild>) -> Result<Vec<String>> {
    let mut failures = Vec::new();
    for child in children.values_mut() {
        let Some(process) = child.child.as_mut() else {
            continue;
        };
        if let Some(status) = process
            .try_wait()
            .context("failed to inspect child process")?
        {
            child.exit_status = Some(status.to_string());
            child.child = None;
        }
    }
    failures.extend(
        children
            .values()
            .filter(|child| {
                child.child.is_none() && child.exit_status.is_some() && !child.requested_stop
            })
            .map(|child| {
                format!(
                    "{} config={} pid={:?} lifecycle={} exited unexpectedly with {}",
                    child.role,
                    child.config,
                    child.pid,
                    child.last_state.as_deref().unwrap_or("unknown"),
                    child.exit_status.as_deref().unwrap_or("unknown status")
                )
            }),
    );
    Ok(failures)
}

async fn stop_wave(children: &mut BTreeMap<String, OwnedChild>, role: &str) -> Result<()> {
    stop_wave_with_budgets(children, role, STOP_BUDGETS).await
}

async fn stop_wave_with_budgets(
    children: &mut BTreeMap<String, OwnedChild>,
    role: &str,
    budgets: StopBudgets,
) -> Result<()> {
    stop_wave_with_budgets_and(children, role, budgets, |endpoint, tls| async move {
        helpers::request_lifecycle_stop(&endpoint, tls.as_ref(), "dev supervisor ordered shutdown")
            .await
    })
    .await
}

async fn stop_wave_with_budgets_and<F, Fut>(
    children: &mut BTreeMap<String, OwnedChild>,
    role: &str,
    budgets: StopBudgets,
    stop_request: F,
) -> Result<()>
where
    F: Fn(String, Option<wr_common::node::TlsConfig>) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<helpers::LifecycleObservation>> + Send + 'static,
{
    let keys = children
        .iter()
        .filter(|(_, child)| child.role == role && child.child.is_some())
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    if keys.is_empty() {
        return Ok(());
    }
    let started = tokio::time::Instant::now();
    let internal_deadline = started + budgets.internal;
    let term_deadline = internal_deadline + budgets.term;
    let wave_deadline = term_deadline + budgets.kill;
    let mut errors = Vec::new();

    let mut stop_requests = tokio::task::JoinSet::new();
    for key in &keys {
        let child = children.get_mut(key).expect("selected child");
        child.requested_stop = true;
        let endpoint = child.endpoint.clone();
        let tls = child.tls().cloned();
        let request_key = key.clone();
        let stop_request = stop_request.clone();
        stop_requests.spawn(async move {
            let result =
                tokio::time::timeout_at(internal_deadline, stop_request(endpoint, tls)).await;
            (request_key, result)
        });
    }
    while let Some(joined) = stop_requests.join_next().await {
        match joined {
            Ok((key, Ok(Ok(observation)))) => {
                if let Some(child) = children.get_mut(&key) {
                    match validate_child_observation(child, &observation) {
                        Ok(state) => child.last_state = Some(state.as_str_name().to_string()),
                        Err(error) => errors.push(format!(
                            "{key} lifecycle stop returned invalid ownership evidence: {error:#}"
                        )),
                    }
                }
            }
            Ok((key, Ok(Err(error)))) => {
                errors.push(format!("{key} lifecycle stop failed: {error:#}"));
            }
            Ok((key, Err(_))) => {
                errors.push(format!(
                    "{key} lifecycle stop exceeded the {:?} internal budget",
                    budgets.internal
                ));
            }
            Err(error) => errors.push(format!("lifecycle stop task failed: {error}")),
        }
    }

    let mut term_sent = false;
    let mut kill_sent = false;
    loop {
        let mut remaining = Vec::new();
        for key in &keys {
            let child = children.get_mut(key).expect("selected child");
            let Some(process) = child.child.as_mut() else {
                continue;
            };
            match process.try_wait().context("failed to reap dev child") {
                Ok(Some(status)) => {
                    child.exit_status = Some(status.to_string());
                    child.child = None;
                    if !status.success() {
                        errors.push(format!(
                            "{} config={} pid={:?} exited non-zero during requested stop: {status}",
                            child.role, child.config, child.pid
                        ));
                    }
                }
                Ok(None) => remaining.push(key.clone()),
                Err(error) => {
                    errors.push(format!("{key} reap check failed: {error:#}"));
                    remaining.push(key.clone());
                }
            }
        }
        if remaining.is_empty() {
            break;
        }
        let now = tokio::time::Instant::now();
        if now >= wave_deadline {
            errors.push(format!(
                "{role} shutdown wave exceeded {:?}; unreaped children: {}",
                budgets.internal + budgets.term + budgets.kill,
                remaining.join(", ")
            ));
            break;
        }
        if now >= term_deadline && !kill_sent {
            for key in &remaining {
                if let Some(process) = children.get_mut(key).and_then(|child| child.child.as_mut())
                {
                    if let Err(error) = process.start_kill() {
                        errors.push(format!("{key} failed to send SIGKILL: {error}"));
                    } else {
                        errors.push(format!("{key} required SIGKILL fallback"));
                    }
                }
            }
            kill_sent = true;
        } else if now >= internal_deadline && !term_sent {
            for key in &remaining {
                if let Some(pid) = children.get(key).and_then(|child| child.pid) {
                    let result = unsafe { libc::kill(pid as i32, libc::SIGTERM) };
                    if result != 0 && io::Error::last_os_error().kind() != io::ErrorKind::NotFound {
                        errors.push(format!(
                            "{key} failed to send SIGTERM: {}",
                            io::Error::last_os_error()
                        ));
                    } else {
                        errors.push(format!("{key} required SIGTERM fallback"));
                    }
                }
            }
            term_sent = true;
        }
        tokio::time::sleep_until((now + Duration::from_millis(100)).min(wave_deadline)).await;
    }

    if errors.is_empty() {
        Ok(())
    } else {
        bail!("{}", errors.join("; "))
    }
}

async fn shutdown_all(children: &mut BTreeMap<String, OwnedChild>) -> Result<()> {
    shutdown_all_with_budgets(children, STOP_BUDGETS).await
}

async fn shutdown_all_with_budgets(
    children: &mut BTreeMap<String, OwnedChild>,
    budgets: StopBudgets,
) -> Result<()> {
    let unexpected = refresh_exits(children)?;
    let mut errors = Vec::new();
    for role in ["engine", "proxy", "manager"] {
        if let Err(error) = stop_wave_with_budgets(children, role, budgets).await {
            errors.push(format!("{role} wave: {error:#}"));
        }
    }
    if !unexpected.is_empty() {
        errors.extend(unexpected);
    }
    if errors.is_empty() {
        Ok(())
    } else {
        bail!("dev shutdown failed: {}", errors.join("; "))
    }
}

async fn finish_startup_operation(
    result: Result<()>,
    children: &mut BTreeMap<String, OwnedChild>,
    budgets: StopBudgets,
) -> (Result<()>, bool) {
    match result {
        Ok(()) => (Ok(()), false),
        Err(primary) => {
            let cleanup = shutdown_all_with_budgets(children, budgets).await;
            let result = match cleanup {
                Ok(()) => Err(primary),
                Err(cleanup_error) => Err(anyhow::anyhow!(
                    "startup operation failed: {primary:#}; ordered cleanup also failed: {cleanup_error:#}"
                )),
            };
            (result, true)
        }
    }
}

fn response(
    result: Result<()>,
    outcome: &str,
    children: &BTreeMap<String, OwnedChild>,
) -> Response {
    match result {
        Ok(()) => Response {
            ok: true,
            outcome: outcome.to_string(),
            error: None,
            children: children.values().map(OwnedChild::view).collect(),
        },
        Err(error) => Response {
            ok: false,
            outcome: "failed".to_string(),
            error: Some(format!("{error:#}")),
            children: children.values().map(OwnedChild::view).collect(),
        },
    }
}

async fn handle_request(
    state_dir: &Path,
    request: Request,
    children: &mut BTreeMap<String, OwnedChild>,
) -> (Response, bool, bool) {
    let unexpected = match refresh_exits(children) {
        Ok(failures) => failures,
        Err(error) => {
            return (response(Err(error), "failed", children), false, false);
        }
    };
    if !unexpected.is_empty() && !matches!(request, Request::Status | Request::Ping | Request::Down)
    {
        return (
            response(
                Err(anyhow::anyhow!(unexpected.join("; "))),
                "unexpected_exit",
                children,
            ),
            false,
            false,
        );
    }

    let mut terminate = false;
    let mut remove_state = false;
    let mut terminal_on_error = false;
    let result = match request {
        Request::Up {
            manager_config,
            proxy_config,
        } => {
            terminal_on_error = true;
            let manager = start_service(
                state_dir,
                children,
                "manager".to_string(),
                "manager",
                &manager_config,
            )
            .await;
            if manager.is_err() {
                manager
            } else {
                start_service(
                    state_dir,
                    children,
                    "proxy:primary".to_string(),
                    "proxy",
                    &proxy_config,
                )
                .await
            }
        }
        Request::DeployEngine { config } => {
            let canonical = canonical_config(&config);
            match canonical {
                Ok(path) => {
                    terminal_on_error = true;
                    let key = format!("engine:{}", path.display());
                    if children
                        .get(&key)
                        .is_some_and(|child| child.child.is_some())
                    {
                        if let Err(error) = stop_one(children, &key).await {
                            Err(error)
                        } else {
                            children.remove(&key);
                            start_service(
                                state_dir,
                                children,
                                key,
                                "engine",
                                path.to_string_lossy().as_ref(),
                            )
                            .await
                        }
                    } else {
                        children.remove(&key);
                        start_service(
                            state_dir,
                            children,
                            key,
                            "engine",
                            path.to_string_lossy().as_ref(),
                        )
                        .await
                    }
                }
                Err(error) => Err(error),
            }
        }
        Request::StartProxy { name, config } => {
            if name.is_empty() || name == "primary" {
                Err(anyhow::anyhow!(
                    "additional proxy name must be non-empty and not 'primary'"
                ))
            } else {
                terminal_on_error = true;
                start_service(
                    state_dir,
                    children,
                    format!("proxy:{name}"),
                    "proxy",
                    &config,
                )
                .await
            }
        }
        Request::Status => {
            for child in children.values_mut() {
                if child.child.is_some() {
                    match helpers::get_lifecycle_status(&child.endpoint, child.tls()).await {
                        Ok(observation) => match validate_child_observation(child, &observation) {
                            Ok(state) => {
                                child.last_state = Some(state.as_str_name().to_string());
                            }
                            Err(error) => {
                                return (response(Err(error), "failed", children), false, false)
                            }
                        },
                        Err(error) => {
                            return (response(Err(error), "failed", children), false, false)
                        }
                    }
                }
            }
            Ok(())
        }
        Request::Ping => Ok(()),
        Request::Down => {
            terminate = true;
            let shutdown = shutdown_all(children).await;
            let shutdown = match (shutdown, unexpected.is_empty()) {
                (Ok(()), true) => Ok(()),
                (Ok(()), false) => Err(anyhow::anyhow!(
                    "dev children had already exited unexpectedly: {}",
                    unexpected.join("; ")
                )),
                (Err(error), _) => Err(error),
            };
            remove_state = shutdown.is_ok();
            shutdown
        }
    };
    let result = if terminal_on_error {
        let (result, startup_failed) =
            finish_startup_operation(result, children, STOP_BUDGETS).await;
        terminate |= startup_failed;
        result
    } else {
        result
    };
    let outcome = if matches!(result, Ok(())) {
        if terminate {
            "stopped"
        } else {
            "observed"
        }
    } else {
        "failed"
    };
    let _ = atomic_write_state(state_dir, outcome, children);
    (response(result, outcome, children), terminate, remove_state)
}

async fn stop_one(children: &mut BTreeMap<String, OwnedChild>, key: &str) -> Result<()> {
    let role = children
        .get(key)
        .map(|child| child.role.clone())
        .ok_or_else(|| anyhow::anyhow!("unknown child {key}"))?;
    let other_keys = children
        .iter()
        .filter(|(candidate, child)| *candidate != key && child.role == role)
        .map(|(candidate, _)| candidate.clone())
        .collect::<Vec<_>>();
    for candidate in other_keys {
        if let Some(child) = children.get_mut(&candidate) {
            child.role = format!("{}-held", child.role);
        }
    }
    let result = stop_wave(children, &role).await;
    for child in children.values_mut() {
        if child.role == format!("{role}-held") {
            child.role = role.clone();
        }
    }
    result
}

async fn read_request(stream: &mut UnixStream) -> Result<Request> {
    let mut bytes = Vec::new();
    stream
        .read_to_end(&mut bytes)
        .await
        .context("failed to read supervisor request")?;
    serde_json::from_slice(&bytes).context("invalid supervisor request")
}

pub async fn run_supervisor(state_dir: PathBuf) -> Result<()> {
    let state_dir = prepare_state_dir(&state_dir)?;
    let state = state_path(&state_dir);
    if state.exists() {
        bail!(
            "retained dev supervisor state {} must be inspected and cleaned with `dev down` before restart",
            state.display()
        );
    }
    let lock = lock_path(&state_dir);
    let mut lock_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&lock)
        .with_context(|| {
            format!(
                "dev supervisor lock {} already exists; inspect retained state before removing it",
                lock.display()
            )
        })?;
    std::fs::set_permissions(&lock, std::fs::Permissions::from_mode(0o600))?;
    use std::io::Write as _;
    writeln!(lock_file, "{}", std::process::id())?;
    let socket = socket_path(&state_dir);
    if socket.exists() {
        std::fs::remove_file(&socket).context("failed to remove stale supervisor socket")?;
    }
    let listener = match UnixListener::bind(&socket) {
        Ok(listener) => listener,
        Err(error) => {
            let _ = std::fs::remove_file(&lock);
            return Err(error)
                .with_context(|| format!("failed to bind supervisor socket {}", socket.display()));
        }
    };
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
    let mut children = BTreeMap::new();
    atomic_write_state(&state_dir, "running", &children)?;
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let mut terminate_signal =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut terminal_result = Ok(());
    let mut terminal_client: Option<(UnixStream, Response)> = None;
    let mut monitor = tokio::time::interval(Duration::from_millis(250));
    monitor.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let remove_state = loop {
        enum Event {
            Connection(UnixStream),
            Signal,
            Monitor,
            ListenerError(io::Error),
        }
        let event = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => Event::Connection(stream),
                Err(error) => Event::ListenerError(error),
            },
            _ = interrupt.recv() => Event::Signal,
            _ = terminate_signal.recv() => Event::Signal,
            _ = monitor.tick() => Event::Monitor,
        };
        let mut stream = match event {
            Event::Connection(stream) => stream,
            Event::Signal => {
                terminal_result = shutdown_all(&mut children).await;
                let clean = terminal_result.is_ok();
                let _ = atomic_write_state(
                    &state_dir,
                    if clean { "stopped" } else { "failed" },
                    &children,
                );
                break clean;
            }
            Event::Monitor => {
                let failures = match refresh_exits(&mut children) {
                    Ok(failures) => failures,
                    Err(error) => {
                        let cleanup = shutdown_all(&mut children).await;
                        terminal_result = Err(match cleanup {
                            Ok(()) => error,
                            Err(cleanup_error) => anyhow::anyhow!(
                                "supervisor monitor failed: {error:#}; ordered cleanup also failed: {cleanup_error:#}"
                            ),
                        });
                        let _ = atomic_write_state(&state_dir, "failed", &children);
                        break false;
                    }
                };
                if failures.is_empty() {
                    continue;
                }
                let cleanup = shutdown_all(&mut children).await;
                terminal_result = Err(match cleanup {
                    Ok(()) => anyhow::anyhow!("unexpected child exit: {}", failures.join("; ")),
                    Err(error) => anyhow::anyhow!(
                        "unexpected child exit: {}; ordered cleanup also failed: {error:#}",
                        failures.join("; ")
                    ),
                });
                let _ = atomic_write_state(&state_dir, "failed", &children);
                break false;
            }
            Event::ListenerError(error) => {
                let cleanup = shutdown_all(&mut children).await;
                terminal_result = Err(match cleanup {
                    Ok(()) => anyhow::Error::new(error).context("supervisor listener failed"),
                    Err(cleanup_error) => anyhow::anyhow!(
                        "supervisor listener failed: {error}; ordered cleanup also failed: {cleanup_error:#}"
                    ),
                });
                let _ = atomic_write_state(&state_dir, "failed", &children);
                break false;
            }
        };
        let request = match read_request(&mut stream).await {
            Ok(request) => request,
            Err(error) => {
                if let Ok(payload) = serde_json::to_vec(&response(Err(error), "failed", &children))
                {
                    let _ = stream.write_all(&payload).await;
                    let _ = stream.shutdown().await;
                }
                continue;
            }
        };
        let (reply, terminate, clean) = handle_request(&state_dir, request, &mut children).await;
        if terminate {
            if !reply.ok {
                terminal_result = Err(anyhow::anyhow!(
                    "{}",
                    reply
                        .error
                        .as_deref()
                        .unwrap_or("supervisor request failed")
                ));
            }
            terminal_client = Some((stream, reply));
            break clean;
        }
        if let Ok(payload) = serde_json::to_vec(&reply) {
            let _ = stream.write_all(&payload).await;
            let _ = stream.shutdown().await;
        }
    };

    drop(listener);
    let mut cleanup_errors = Vec::new();
    let socket_cleanup = if socket.exists() {
        std::fs::remove_file(&socket)
    } else {
        Ok(())
    };
    if let Err(error) = socket_cleanup {
        cleanup_errors.push(format!("failed to remove supervisor socket: {error}"));
    }
    if let Err(error) = std::fs::remove_file(&lock) {
        cleanup_errors.push(format!("failed to remove supervisor lock: {error}"));
    }
    let state_cleanup = if remove_state && state.exists() {
        std::fs::remove_file(&state)
    } else {
        Ok(())
    };
    if let Err(error) = state_cleanup {
        cleanup_errors.push(format!("failed to remove clean supervisor state: {error}"));
    }
    if let Err(error) = terminal_result {
        cleanup_errors.insert(0, format!("{error:#}"));
    }
    if let Some((mut stream, mut reply)) = terminal_client {
        if !cleanup_errors.is_empty() {
            let cleanup = cleanup_errors.join("; ");
            let combined = match reply.error.take() {
                Some(primary) if primary != cleanup => {
                    format!("{primary}; supervisor terminal cleanup failed: {cleanup}")
                }
                _ => cleanup,
            };
            reply.ok = false;
            reply.outcome = "failed".to_string();
            reply.error = Some(combined);
        }
        let payload = serde_json::to_vec(&reply)?;
        stream.write_all(&payload).await?;
        stream.shutdown().await?;
    }
    if cleanup_errors.is_empty() {
        Ok(())
    } else {
        bail!("{}", cleanup_errors.join("; "))
    }
}

fn read_supervisor_state(state_dir: &Path) -> Result<SupervisorState> {
    let path = state_path(state_dir);
    let bytes = std::fs::read(&path).with_context(|| {
        format!(
            "failed to read retained supervisor state {}",
            path.display()
        )
    })?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("invalid retained supervisor state {}", path.display()))
}

fn process_cmdline(pid: u32) -> Option<Vec<String>> {
    let bytes = std::fs::read(format!("/proc/{pid}/cmdline")).ok()?;
    Some(
        bytes
            .split(|byte| *byte == 0)
            .filter(|argument| !argument.is_empty())
            .map(|argument| String::from_utf8_lossy(argument).into_owned())
            .collect(),
    )
}

fn process_executable_name(pid: u32) -> Option<String> {
    std::fs::read_link(format!("/proc/{pid}/exe"))
        .ok()?
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
}

fn lock_owner_pid(state_dir: &Path) -> Option<u32> {
    std::fs::read_to_string(lock_path(state_dir))
        .ok()?
        .trim()
        .parse()
        .ok()
}

fn supervisor_owner_matches(state_dir: &Path) -> bool {
    let Some(pid) = lock_owner_pid(state_dir) else {
        return false;
    };
    if let Ok(state) = read_supervisor_state(state_dir) {
        if state.supervisor_pid != pid {
            return false;
        }
    }
    let Some(arguments) = process_cmdline(pid) else {
        return false;
    };
    let expected_executable = std::env::current_exe().ok().and_then(|path| {
        path.file_name()
            .map(|name| name.to_string_lossy().into_owned())
    });
    let observed_executable = process_executable_name(pid);
    let executable_matches = observed_executable == expected_executable
        || cfg!(test)
            && observed_executable
                .as_deref()
                .is_some_and(|name| name.starts_with("wr_cli-"));
    if !executable_matches {
        return false;
    }
    if cfg!(test) {
        return true;
    }
    let expected_state_dir = state_dir.display().to_string();
    arguments.iter().any(|argument| argument == "dev")
        && arguments.iter().any(|argument| argument == "supervisor")
        && arguments
            .windows(2)
            .any(|pair| pair[0] == "--state-dir" && pair[1] == expected_state_dir)
}

fn retained_child_process_matches(child: &ChildView) -> bool {
    let Some(pid) = child.pid else {
        return false;
    };
    let expected_executable = format!("wr-{}", child.role);
    process_executable_name(pid).as_deref() == Some(expected_executable.as_str())
        && process_cmdline(pid)
            .is_some_and(|arguments| arguments.iter().any(|argument| argument == &child.config))
}

fn cleanup_stale_supervisor(state_dir: &Path) -> Result<Response> {
    let state = state_path(state_dir);
    let retained = if state.exists() {
        Some(read_supervisor_state(state_dir)?)
    } else {
        None
    };
    let live = retained
        .as_ref()
        .into_iter()
        .flat_map(|state| state.children.iter())
        .filter(|child| child.exit_status.is_none() && retained_child_process_matches(child))
        .map(|child| {
            format!(
                "{} config={} pid={:?} lifecycle={}",
                child.role,
                child.config,
                child.pid,
                child.lifecycle_state.as_deref().unwrap_or("unknown")
            )
        })
        .collect::<Vec<_>>();
    if !live.is_empty() {
        bail!(
            "retained supervisor state still names identity-matching live processes: {}; state retained at {}",
            live.join("; "),
            state.display()
        );
    }
    let mut errors = Vec::new();
    for (path, label) in [
        (socket_path(state_dir), "stale supervisor socket"),
        (lock_path(state_dir), "stale supervisor lock"),
        (state.clone(), "retained supervisor state"),
    ] {
        if path.exists() {
            if let Err(error) = std::fs::remove_file(&path) {
                errors.push(format!(
                    "failed to remove {label} {}: {error}",
                    path.display()
                ));
            }
        }
    }
    if !errors.is_empty() {
        bail!("{}", errors.join("; "));
    }
    Ok(Response {
        ok: true,
        outcome: if retained.is_some() {
            "cleaned_failed_state".to_string()
        } else {
            "already_stopped".to_string()
        },
        error: None,
        children: retained.map(|state| state.children).unwrap_or_default(),
    })
}

pub async fn ensure_supervisor(state_dir: &Path) -> Result<PathBuf> {
    let state_dir = prepare_state_dir(state_dir)?;
    let socket = socket_path(&state_dir);
    if socket.exists() || lock_path(&state_dir).exists() {
        if supervisor_owner_matches(&state_dir) {
            if socket.exists() {
                send(&state_dir, Request::Ping)
                    .await
                    .context("identity-matching dev supervisor is not responsive")?;
                return Ok(state_dir);
            }
            bail!(
                "identity-matching dev supervisor owns {} but its socket is missing",
                lock_path(&state_dir).display()
            );
        }
        bail!(
            "stale or identity-mismatched dev supervisor metadata blocks restart; run `wr-cli dev --state-dir {} down` for explicit cleanup",
            state_dir.display()
        );
    }
    if state_path(&state_dir).exists() {
        bail!(
            "retained dev supervisor state {} blocks restart; run `wr-cli dev --state-dir {} down` to validate cleanup and remove it",
            state_path(&state_dir).display(),
            state_dir.display()
        );
    }
    let executable = std::env::current_exe().context("failed to resolve wr-cli executable")?;
    let mut command = Command::new(executable);
    if let Some(tls) = client::tls_config() {
        command
            .arg("--ca-cert")
            .arg(&tls.ca_cert_path)
            .arg("--client-cert")
            .arg(&tls.cert_path)
            .arg("--client-key")
            .arg(&tls.key_path);
    }
    let mut supervisor = command
        .arg("dev")
        .arg("--state-dir")
        .arg(&state_dir)
        .arg("supervisor")
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .context("failed to start dev supervisor")?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if socket.exists() && lock_path(&state_dir).exists() {
            return Ok(state_dir);
        }
        if let Some(status) = supervisor.try_wait()? {
            bail!("dev supervisor exited during startup with {status}");
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            bail!(
                "timed out waiting for dev supervisor socket {}",
                socket.display()
            );
        }
        tokio::time::sleep_until((now + Duration::from_millis(50)).min(deadline)).await;
    }
}

pub async fn send(state_dir: &Path, request: Request) -> Result<Response> {
    let state_dir = prepare_state_dir(state_dir)?;
    let socket = socket_path(&state_dir);
    let mut stream = UnixStream::connect(&socket)
        .await
        .with_context(|| format!("dev supervisor is not running at {}", socket.display()))?;
    stream.write_all(&serde_json::to_vec(&request)?).await?;
    stream.shutdown().await?;
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await?;
    let response: Response =
        serde_json::from_slice(&bytes).context("invalid supervisor response")?;
    if !response.ok {
        bail!(
            "{}",
            response
                .error
                .as_deref()
                .unwrap_or("dev supervisor request failed")
        );
    }
    Ok(response)
}

pub async fn send_or_start(state_dir: &Path, request: Request) -> Result<Response> {
    let state_dir = ensure_supervisor(state_dir).await?;
    send(&state_dir, request).await
}

pub async fn wait(state_dir: &Path) -> Result<Response> {
    let state_dir = prepare_state_dir(state_dir)?;
    if !socket_path(&state_dir).exists() || !lock_path(&state_dir).exists() {
        if state_path(&state_dir).exists() {
            let retained = read_supervisor_state(&state_dir)?;
            if retained.outcome == "stopped" {
                return Ok(Response {
                    ok: true,
                    outcome: "stopped".to_string(),
                    error: None,
                    children: retained.children,
                });
            }
            bail!(
                "dev supervisor is stopped with retained outcome '{}': {}",
                retained.outcome,
                serde_json::to_string(&retained)?
            );
        }
        bail!(
            "dev supervisor is not running at {}",
            socket_path(&state_dir).display()
        );
    }
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    loop {
        tokio::select! {
            _ = interrupt.recv() => return down(&state_dir).await,
            _ = terminate.recv() => return down(&state_dir).await,
            _ = tokio::time::sleep(Duration::from_millis(250)) => {
                if let Err(error) = send(&state_dir, Request::Ping).await {
                    if state_path(&state_dir).exists() {
                        let retained = read_supervisor_state(&state_dir)?;
                        if retained.outcome == "stopped" {
                            return Ok(Response {
                                ok: true,
                                outcome: "stopped".to_string(),
                                error: None,
                                children: retained.children,
                            });
                        }
                        bail!(
                            "dev supervisor terminated with retained outcome '{}': {}; state: {}",
                            retained.outcome,
                            error,
                            serde_json::to_string(&retained)?
                        );
                    }
                    return Ok(Response {
                        ok: true,
                        outcome: "stopped".to_string(),
                        error: None,
                        children: Vec::new(),
                    });
                }
            }
        }
    }
}

pub async fn down(state_dir: &Path) -> Result<Response> {
    let state_dir = prepare_state_dir(state_dir)?;
    let socket = socket_path(&state_dir);
    let lock = lock_path(&state_dir);
    if socket.exists() || lock.exists() {
        if supervisor_owner_matches(&state_dir) {
            if !socket.exists() {
                bail!("identity-matching dev supervisor has no control socket");
            }
            return send(&state_dir, Request::Down).await;
        }
        return cleanup_stale_supervisor(&state_dir);
    }
    cleanup_stale_supervisor(&state_dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_state_dir(label: &str) -> Result<PathBuf> {
        let root = std::env::temp_dir().join(format!(
            "wr-cli-supervisor-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        ));
        std::fs::create_dir_all(&root)?;
        Ok(root)
    }

    fn fixture_child(script: &str, role: &str) -> Result<OwnedChild> {
        let mut command = Command::new("sh");
        let child = command
            .arg("-c")
            .arg(script)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        let pid = child.id();
        Ok(OwnedChild {
            role: role.to_string(),
            config: format!("fixture-{role}.toml"),
            endpoint: "http://127.0.0.1:1".to_string(),
            manager_tls: false,
            process_instance_id: Some(format!("fixture-{role}")),
            last_state: Some("READY".to_string()),
            child: Some(child),
            pid,
            exit_status: None,
            requested_stop: false,
        })
    }

    #[test]
    fn state_paths_are_scoped_to_requested_directory() {
        let root = Path::new("/tmp/wr-dev-test");
        assert_eq!(socket_path(root), root.join(SOCKET_FILE));
        assert_eq!(state_path(root), root.join(STATE_FILE));
        assert_eq!(lock_path(root), root.join(LOCK_FILE));
    }

    #[test]
    fn request_protocol_is_typed() {
        let request = Request::DeployEngine {
            config: "engine.toml".to_string(),
        };
        let json = serde_json::to_value(request).unwrap();
        assert_eq!(json["command"], "deploy_engine");
        assert_eq!(json["config"], "engine.toml");
    }

    #[tokio::test]
    async fn launcher_pin_rejects_a_bind_race_responder() -> Result<()> {
        let mut child = fixture_child("sleep 30", "engine")?;
        let observation = helpers::LifecycleObservation {
            state: ProcessLifecycleState::Ready as i32,
            service_kind: ServiceKind::Engine as i32,
            process_instance_id: "unowned-bind-race-winner".to_string(),
            reason: 0,
            detail: "ready".to_string(),
        };
        let error = validate_child_observation(&child, &observation)
            .expect_err("unowned responder must not satisfy readiness");
        assert!(error.to_string().contains("launcher-pinned instance"));
        child
            .child
            .as_mut()
            .context("fixture child missing")?
            .kill()
            .await?;
        let status = child
            .child
            .as_mut()
            .context("fixture child missing")?
            .wait()
            .await?;
        child.exit_status = Some(status.to_string());
        child.child = None;
        Ok(())
    }

    #[tokio::test]
    async fn occupied_endpoint_fails_startup_and_latches_terminal_cleanup() -> Result<()> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let root = test_state_dir("occupied-endpoint")?;
        let config = root.join("engine.toml");
        std::fs::write(&config, format!("listen_address = \"{address}\"\n"))?;
        let mut children = BTreeMap::new();

        let (reply, terminate, remove_state) = handle_request(
            &root,
            Request::DeployEngine {
                config: config.display().to_string(),
            },
            &mut children,
        )
        .await;
        assert!(!reply.ok);
        assert!(reply
            .error
            .as_deref()
            .is_some_and(|error| error.contains("already accepts connections")));
        assert!(terminate, "startup failure must terminate the supervisor");
        assert!(!remove_state, "failure evidence must be retained");
        assert!(children.is_empty(), "no unowned child may be spawned");
        let retained = read_supervisor_state(&root)?;
        assert_eq!(retained.outcome, "failed");
        drop(listener);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn startup_failure_reaps_a_live_owned_child_and_preserves_both_errors() -> Result<()> {
        let mut children = BTreeMap::new();
        children.insert(
            "engine:fixture".to_string(),
            fixture_child("trap 'exit 0' TERM; while :; do :; done", "engine")?,
        );
        let (result, terminate) = finish_startup_operation(
            Err(anyhow::anyhow!("malformed lifecycle evidence")),
            &mut children,
            StopBudgets {
                internal: Duration::from_millis(20),
                term: Duration::from_millis(200),
                kill: Duration::from_millis(200),
            },
        )
        .await;
        let error = match result {
            Ok(()) => bail!("live startup failure unexpectedly reported success"),
            Err(error) => error,
        };
        assert!(terminate);
        assert!(error.to_string().contains("malformed lifecycle evidence"));
        assert!(error.to_string().contains("ordered cleanup also failed"));
        assert!(children
            .get("engine:fixture")
            .context("fixture child missing")?
            .child
            .is_none());
        Ok(())
    }

    #[tokio::test]
    async fn graceful_stop_response_and_zero_exit_report_success() -> Result<()> {
        let mut children = BTreeMap::new();
        children.insert(
            "engine:fixture".to_string(),
            fixture_child("sleep 0.02; exit 0", "engine")?,
        );
        stop_wave_with_budgets_and(
            &mut children,
            "engine",
            StopBudgets {
                internal: Duration::from_millis(100),
                term: Duration::from_millis(100),
                kill: Duration::from_millis(100),
            },
            |_endpoint, _tls| async {
                Ok(helpers::LifecycleObservation {
                    state: ProcessLifecycleState::Stopping as i32,
                    service_kind: ServiceKind::Engine as i32,
                    process_instance_id: "fixture-engine".to_string(),
                    reason: 0,
                    detail: "test stop".to_string(),
                })
            },
        )
        .await?;
        let child = children
            .get("engine:fixture")
            .context("fixture child missing")?;
        assert!(child.child.is_none());
        assert_eq!(child.exit_status.as_deref(), Some("exit status: 0"));
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_stop_wave_reaps_every_selected_child() -> Result<()> {
        let mut children = BTreeMap::new();
        for key in ["engine:a", "engine:b"] {
            children.insert(
                key.to_string(),
                fixture_child("trap 'exit 0' TERM; while :; do :; done", "engine")?,
            );
        }
        let started = tokio::time::Instant::now();
        let result = stop_wave_with_budgets(
            &mut children,
            "engine",
            StopBudgets {
                internal: Duration::from_millis(20),
                term: Duration::from_millis(200),
                kill: Duration::from_millis(200),
            },
        )
        .await;
        assert!(result.is_err(), "fallback wave must remain nonzero");
        assert!(
            started.elapsed() < Duration::from_millis(350),
            "children were not stopped as one concurrent wave"
        );
        assert!(children.values().all(|child| child.child.is_none()));
        Ok(())
    }

    #[tokio::test]
    async fn sigkill_fallback_is_reported_and_reaped() -> Result<()> {
        let mut children = BTreeMap::new();
        children.insert(
            "engine:fixture".to_string(),
            fixture_child("trap '' TERM; while :; do :; done", "engine")?,
        );
        let result = stop_wave_with_budgets(
            &mut children,
            "engine",
            StopBudgets {
                internal: Duration::from_millis(20),
                term: Duration::from_millis(20),
                kill: Duration::from_millis(200),
            },
        )
        .await;
        let error = match result {
            Ok(()) => bail!("SIGKILL fallback unexpectedly reported success"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("required SIGKILL fallback"));
        assert!(children
            .get("engine:fixture")
            .context("fixture child missing")?
            .child
            .is_none());
        Ok(())
    }

    #[tokio::test]
    async fn fallback_stop_is_nonzero_but_reaps_the_owned_child() -> Result<()> {
        let mut children = BTreeMap::new();
        children.insert(
            "engine:fixture".to_string(),
            fixture_child("trap 'exit 0' TERM; while :; do :; done", "engine")?,
        );
        let result = stop_wave_with_budgets(
            &mut children,
            "engine",
            StopBudgets {
                internal: Duration::from_millis(20),
                term: Duration::from_millis(200),
                kill: Duration::from_millis(200),
            },
        )
        .await;
        let error = match result {
            Ok(()) => bail!("fallback shutdown unexpectedly reported success"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("lifecycle stop failed"));
        assert!(error.to_string().contains("required SIGTERM fallback"));
        let child = children
            .get("engine:fixture")
            .context("fixture child missing")?;
        assert!(child.child.is_none(), "fixture child was not reaped");
        assert!(child.exit_status.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn nonzero_requested_exit_is_classified_and_reaped() -> Result<()> {
        let mut children = BTreeMap::new();
        children.insert(
            "engine:fixture".to_string(),
            fixture_child("exit 7", "engine")?,
        );
        let result = stop_wave_with_budgets(
            &mut children,
            "engine",
            StopBudgets {
                internal: Duration::from_millis(20),
                term: Duration::from_millis(20),
                kill: Duration::from_millis(20),
            },
        )
        .await;
        let error = match result {
            Ok(()) => bail!("nonzero child exit unexpectedly reported success"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("exited non-zero"));
        assert!(children
            .get("engine:fixture")
            .context("fixture child missing")?
            .child
            .is_none());
        Ok(())
    }

    #[tokio::test]
    async fn wait_subscription_allows_concurrent_ping_and_down() -> Result<()> {
        let root = test_state_dir("wait-down")?;
        let supervisor_root = root.clone();
        let supervisor = tokio::spawn(async move { run_supervisor(supervisor_root).await });
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        while !socket_path(&root).exists() {
            if tokio::time::Instant::now() >= deadline {
                bail!("fixture supervisor socket did not appear");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let wait_root = root.clone();
        let subscription = tokio::spawn(async move { wait(&wait_root).await });
        let ping = send(&root, Request::Ping).await?;
        assert!(ping.ok);
        let stopped = down(&root).await?;
        assert_eq!(stopped.outcome, "stopped");
        let wait_response = tokio::time::timeout(Duration::from_secs(1), subscription)
            .await
            .context("wait subscription did not finish")???;
        assert_eq!(wait_response.outcome, "stopped");
        tokio::time::timeout(Duration::from_secs(1), supervisor)
            .await
            .context("fixture supervisor did not exit")???;
        assert!(!socket_path(&root).exists());
        assert!(!lock_path(&root).exists());
        assert!(!state_path(&root).exists());
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn wait_accepts_a_clean_stopped_state_during_terminal_cleanup() -> Result<()> {
        let root = test_state_dir("wait-stopped")?;
        let retained = SupervisorState {
            schema_version: 1,
            supervisor_pid: u32::MAX,
            socket: socket_path(&root).display().to_string(),
            outcome: "stopped".to_string(),
            children: Vec::new(),
        };
        std::fs::write(state_path(&root), serde_json::to_vec(&retained)?)?;
        let response = wait(&root).await?;
        assert_eq!(response.outcome, "stopped");
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn stale_socket_and_lock_are_removed_by_explicit_down() -> Result<()> {
        let root = test_state_dir("stale-owner")?;
        std::fs::write(lock_path(&root), format!("{}\n", u32::MAX))?;
        let listener = UnixListener::bind(socket_path(&root))?;
        let retained = SupervisorState {
            schema_version: 1,
            supervisor_pid: u32::MAX,
            socket: socket_path(&root).display().to_string(),
            outcome: "failed".to_string(),
            children: Vec::new(),
        };
        std::fs::write(state_path(&root), serde_json::to_vec(&retained)?)?;
        drop(listener);

        let response = down(&root).await?;
        assert_eq!(response.outcome, "cleaned_failed_state");
        assert!(!socket_path(&root).exists());
        assert!(!lock_path(&root).exists());
        assert!(!state_path(&root).exists());
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn retained_failed_state_blocks_restart_until_explicit_down() -> Result<()> {
        let root = test_state_dir("retained")?;
        let retained = SupervisorState {
            schema_version: 1,
            supervisor_pid: u32::MAX,
            socket: socket_path(&root).display().to_string(),
            outcome: "failed".to_string(),
            children: vec![ChildView {
                role: "engine".to_string(),
                config: "fixture.toml".to_string(),
                lifecycle_endpoint: "http://127.0.0.1:1".to_string(),
                process_instance_id: Some("fixture".to_string()),
                pid: Some(u32::MAX),
                lifecycle_state: Some("STARTING".to_string()),
                exit_status: Some("exit status: 7".to_string()),
            }],
        };
        std::fs::write(state_path(&root), serde_json::to_vec(&retained)?)?;

        let start_error = ensure_supervisor(&root)
            .await
            .expect_err("retained state must block supervisor restart");
        assert!(start_error.to_string().contains("blocks restart"));
        let cleaned = down(&root).await?;
        assert_eq!(cleaned.outcome, "cleaned_failed_state");
        assert!(!state_path(&root).exists());
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn legacy_pid_state_is_rejected() {
        let root = std::env::temp_dir().join(format!(
            "wr-cli-legacy-state-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join(LEGACY_PID_FILE), "manager 123\n").unwrap();
        let error = prepare_state_dir(&root).unwrap_err().to_string();
        assert!(error.contains("legacy dev PID state"));
        std::fs::remove_dir_all(root).unwrap();
    }
}
