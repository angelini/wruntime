mod circuit_breaker;
pub mod config;
pub mod indexed_routing;
mod layers;
pub mod node_service;
pub mod routing;
mod schema;

use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use http::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tonic::transport::server::TcpIncoming;
use tonic::transport::Server;
use tonic::Status;
use tower::{Service, ServiceBuilder};

use layers::{
    EgressLayer, ForwardService, IngressLayer, ProxyBody, ResBody, RoutingLayer,
    SchemaValidationLayer, TracingLayer,
};
use tokio_rustls::TlsAcceptor;
use tracing::{debug, error, info};
use wr_common::discovery::ManagerDiscovery;
use wr_common::lifecycle_service::{
    notify_supervisor, AdmissionGate, LifecycleServiceAdapter, ShutdownOperation,
};
use wr_common::process_lifecycle::{
    ProcessLifecycleCoordinator, ProcessState, ServiceKind, TransitionReason,
};
use wr_common::signal::{apply_shutdown_request, shutdown_signal_request};
use wr_common::task_group::{TaskCancellation, TaskExit, TaskGroup, TaskOutcomeKind};
use wr_common::wruntime::lifecycle_service_client::LifecycleServiceClient;
use wr_common::wruntime::lifecycle_service_server::LifecycleServiceServer;
use wr_common::wruntime::node_service_server::NodeServiceServer;
use wr_common::wruntime::{GetLifecycleStatusRequest, ProcessLifecycleState};

const SHUTDOWN_BUDGET: Duration = Duration::from_secs(30);

struct DataListenerShutdown {
    signal: tokio::sync::watch::Sender<bool>,
    remaining: AtomicUsize,
    stopped: tokio::sync::Notify,
    operation: Mutex<Option<DataListenerOperation>>,
}

#[derive(Clone, Copy)]
struct DataListenerOperation {
    kind: ShutdownOperation,
    deadline: tokio::time::Instant,
    result: Option<Result<(), usize>>,
}

struct DataListenerGuard {
    shutdown: Arc<DataListenerShutdown>,
    reported: bool,
}

impl DataListenerGuard {
    fn new(shutdown: Arc<DataListenerShutdown>) -> Self {
        Self {
            shutdown,
            reported: false,
        }
    }

    fn report(&mut self) {
        if !self.reported {
            self.reported = true;
            self.shutdown.listener_stopped();
        }
    }
}

impl Drop for DataListenerGuard {
    fn drop(&mut self) {
        self.report();
    }
}

impl DataListenerShutdown {
    fn new(listener_count: usize) -> (Arc<Self>, tokio::sync::watch::Receiver<bool>) {
        let (signal, receiver) = tokio::sync::watch::channel(false);
        (
            Arc::new(Self {
                signal,
                remaining: AtomicUsize::new(listener_count),
                stopped: tokio::sync::Notify::new(),
                operation: Mutex::new(None),
            }),
            receiver,
        )
    }

    fn begin_operation(
        &self,
        kind: ShutdownOperation,
    ) -> (tokio::time::Instant, Option<Result<(), usize>>) {
        let mut operation = self
            .operation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(active) = *operation {
            if active.result.is_none()
                || active.kind == kind
                || active.kind == ShutdownOperation::Stop
                || active.result != Some(Ok(()))
            {
                return (active.deadline, active.result);
            }
        }

        let deadline = tokio::time::Instant::now() + SHUTDOWN_BUDGET;
        *operation = Some(DataListenerOperation {
            kind,
            deadline,
            result: None,
        });
        (deadline, None)
    }

    fn complete_operation(
        &self,
        deadline: tokio::time::Instant,
        result: Result<(), usize>,
    ) -> Result<(), usize> {
        let mut operation = self
            .operation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(active) = operation.as_mut() else {
            return result;
        };
        if active.deadline != deadline {
            return result;
        }
        if active.result.is_none() {
            active.result = Some(result);
        }
        active.result.unwrap_or(result)
    }

    async fn stop_and_wait(
        &self,
        kind: ShutdownOperation,
    ) -> (tokio::time::Instant, Result<(), usize>) {
        let (deadline, completed) = self.begin_operation(kind);
        if let Some(result) = completed {
            return (deadline, result);
        }

        let _ = self.signal.send(true);
        let result = loop {
            let remaining = self.remaining.load(Ordering::Acquire);
            if remaining == 0 {
                break Ok(());
            }
            let stopped = self.stopped.notified();
            if self.remaining.load(Ordering::Acquire) == 0 {
                break Ok(());
            }
            if tokio::time::timeout_at(deadline, stopped).await.is_err() {
                break Err(self.remaining.load(Ordering::Acquire));
            }
        };
        (deadline, self.complete_operation(deadline, result))
    }

    fn listener_stopped(&self) {
        if self.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.stopped.notify_waiters();
        }
    }
}

async fn close_data_plane(
    admission: &AdmissionGate,
    data_shutdown: &DataListenerShutdown,
    operation: ShutdownOperation,
) -> (tokio::time::Instant, Result<(), usize>) {
    admission.close();
    data_shutdown.stop_and_wait(operation).await
}

fn expected_data_listener_exit(
    task_name: &str,
    kind: &TaskOutcomeKind,
    state: ProcessState,
) -> bool {
    state >= ProcessState::Draining
        && *kind == TaskOutcomeKind::Cancelled
        && matches!(
            task_name,
            "proxy-internal-listener" | "proxy-peer-listener" | "proxy-external-listener"
        )
}

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("failed to install rustls crypto provider"))?;

    let arguments: Vec<String> = std::env::args().collect();
    if arguments
        .get(1)
        .is_some_and(|arg| arg == "--lifecycle-probe")
    {
        return lifecycle_probe(arguments.get(2).map(String::as_str).unwrap_or("proxy.toml")).await;
    }

    let mut telemetry = wr_common::telemetry::init("wr-proxy")?;
    let result = run_service(arguments.get(1).map(String::as_str).unwrap_or("proxy.toml")).await;
    let finalized = telemetry.finalize();
    if !finalized.is_success() && result.is_ok() {
        anyhow::bail!("telemetry finalization failed: {:?}", finalized.failures);
    }
    result
}

async fn lifecycle_probe(config_path: &str) -> Result<()> {
    let config = config::ProxyConfig::load(config_path)?;
    let control = config.control_address.as_str();
    let status = LifecycleServiceClient::connect(format!("http://{control}"))
        .await?
        .get_status(GetLifecycleStatusRequest {})
        .await?
        .into_inner()
        .status
        .context("lifecycle response omitted status")?;
    anyhow::ensure!(
        status.state == ProcessLifecycleState::Ready as i32,
        "proxy lifecycle state is not READY"
    );
    Ok(())
}

async fn run_service(config_path: &str) -> Result<()> {
    let process_id = format!(
        "proxy-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    let lifecycle = ProcessLifecycleCoordinator::new(
        ServiceKind::Proxy,
        wr_common::process_lifecycle::resolve_process_instance_id(process_id),
    );
    let admission = AdmissionGate::closed();
    let config = config::ProxyConfig::load(config_path)?;
    let self_address = config.node.peer_address()?;

    let control_address = config.control_address.as_str();
    let control_addr: std::net::SocketAddr =
        control_address.parse().context("invalid control_address")?;
    anyhow::ensure!(
        control_addr.ip().is_loopback(),
        "proxy lifecycle control must bind to loopback"
    );

    let routing_table = routing::new_routing_table(
        config.circuit_breaker.clone(),
        Arc::<str>::from(self_address.as_str()),
    );
    let db_pool = wr_common::pool::build_pool_with_search_path(
        &config.database.url,
        config.database.max_connections,
        "wr_system",
    )
    .context("failed to create discovery pool")?;
    let manager_tls = wr_common::tls::build_tonic_client_tls(&config.node.tls)
        .context("failed to build manager TLS config")?;
    let discovery = Arc::new(ManagerDiscovery::new(db_pool, Some(manager_tls)));
    discovery.refresh().await;
    {
        let mut client = discovery
            .get_client()
            .await
            .map_err(|error| anyhow::anyhow!("initial manager connect failed: {error}"))?;
        routing::sync_once(&mut client, &routing_table)
            .await
            .context("initial routing table sync failed")?;
    }

    let schema_cache = Arc::new(schema::SchemaCache::new(Arc::clone(&discovery)));
    let node_agent = Arc::new(node_service::NodeAgent::new(
        Arc::clone(&discovery),
        routing_table.clone(),
    ));
    let control_incoming =
        TcpIncoming::bind(control_addr).context("failed to bind proxy control listener")?;

    let mtls_client_config = wr_common::tls::build_client_config(&config.node.tls)?;
    let mtls_pool = wr_common::tls::HttpsClientPool::new(
        wr_common::http_pool::DEFAULT_POOL_SIZE,
        mtls_client_config,
    );
    let tls_acceptor = wr_common::tls::build_acceptor(&config.node.tls)?;

    let egress_domains = config
        .egress
        .as_ref()
        .map(|egress| egress.allowed_domains.clone())
        .unwrap_or_default();
    let internal_service = ServiceBuilder::new()
        .layer(TracingLayer)
        .layer(RoutingLayer::new(routing_table.clone()).with_egress(egress_domains))
        .layer(EgressLayer::new(config.egress.clone()))
        .service(ForwardService::new(
            routing_table.open_duration_secs(),
            mtls_pool.clone(),
        ));

    let internal_listener = TcpListener::bind(&config.listen_address)
        .await
        .context("failed to bind internal proxy listener")?;
    let peer_bind = format!("0.0.0.0:{}", config.node.peer_port()?);
    let peer_listener = TcpListener::bind(&peer_bind)
        .await
        .context("failed to bind peer proxy listener")?;

    let external = if let Some(external) = &config.external {
        let service = ServiceBuilder::new()
            .layer(IngressLayer::new(external.routes.clone())?)
            .layer(TracingLayer)
            .layer(RoutingLayer::new(routing_table.clone()))
            .layer(SchemaValidationLayer::new(
                schema_cache,
                external.max_request_body_bytes,
            ))
            .service(ForwardService::new(
                routing_table.open_duration_secs(),
                mtls_pool,
            ));
        let listener = TcpListener::bind(&external.listen_address)
            .await
            .context("failed to bind external proxy listener")?;
        Some((listener, service, external.listen_address.clone()))
    } else {
        None
    };

    let listener_count = 2 + usize::from(external.is_some());
    let (data_shutdown, data_shutdown_rx) = DataListenerShutdown::new(listener_count);
    let lifecycle_data_shutdown = Arc::clone(&data_shutdown);
    let lifecycle_service =
        LifecycleServiceAdapter::new(lifecycle.clone(), Some(admission.clone()))
            .with_shutdown_hook(move |operation| {
                let data_shutdown = Arc::clone(&lifecycle_data_shutdown);
                async move {
                    let (_, result) = data_shutdown.stop_and_wait(operation).await;
                    result.map_err(|remaining| {
                        Status::deadline_exceeded(format!(
                            "proxy data listeners did not stop before the shutdown deadline ({remaining} remaining)"
                        ))
                    })
                }
            });
    let control_router = Server::builder()
        .add_service(NodeServiceServer::from_arc(Arc::clone(&node_agent)))
        .add_service(LifecycleServiceServer::new(lifecycle_service));

    let mut tasks = TaskGroup::new();
    {
        let discovery = Arc::clone(&discovery);
        tasks.spawn("proxy-manager-discovery", move |cancellation| {
            discovery.run_refresh_loop(cancellation)
        });
    }
    {
        let discovery = Arc::clone(&discovery);
        let table = routing_table.clone();
        let ttl = config.cache.routing_table_ttl_secs;
        tasks.spawn("proxy-routing-sync", move |cancellation| {
            routing::sync_routing_table(discovery, table, ttl, cancellation)
        });
    }
    {
        let node_agent = Arc::clone(&node_agent);
        tasks.spawn("proxy-heartbeat-flush", move |cancellation| {
            node_agent.run_heartbeat_loop(Duration::from_secs(3), cancellation)
        });
    }
    tasks.spawn("proxy-control-listener", move |cancellation| async move {
        let mut shutdown = cancellation.clone();
        control_router
            .serve_with_incoming_shutdown(control_incoming, async move {
                shutdown.cancelled().await;
            })
            .await?;
        Ok(if cancellation.is_cancelled() {
            TaskExit::Cancelled
        } else {
            TaskExit::Completed
        })
    });
    {
        let admission = admission.clone();
        let service = internal_service.clone();
        let data_shutdown_rx = data_shutdown_rx.clone();
        let data_shutdown = Arc::clone(&data_shutdown);
        tasks.spawn("proxy-internal-listener", move |cancellation| {
            accept_loop(
                internal_listener,
                service,
                admission,
                data_shutdown_rx,
                data_shutdown,
                cancellation,
            )
        });
    }
    {
        let admission = admission.clone();
        let data_shutdown_rx = data_shutdown_rx.clone();
        let data_shutdown = Arc::clone(&data_shutdown);
        tasks.spawn("proxy-peer-listener", move |cancellation| {
            tls_accept_loop(
                peer_listener,
                tls_acceptor,
                internal_service,
                admission,
                data_shutdown_rx,
                data_shutdown,
                cancellation,
            )
        });
    }
    if let Some((listener, service, address)) = external {
        let admission = admission.clone();
        let data_shutdown_rx = data_shutdown_rx.clone();
        let data_shutdown = Arc::clone(&data_shutdown);
        tasks.spawn("proxy-external-listener", move |cancellation| async move {
            info!(%address, "proxy external listener bound");
            accept_loop(
                listener,
                service,
                admission,
                data_shutdown_rx,
                data_shutdown,
                cancellation,
            )
            .await
        });
    }

    let mut failure: Option<anyhow::Error> = None;
    admission.open();
    if let Err(error) = lifecycle
        .mark_ready("manager discovery, routing snapshot, control, and data listeners ready")
    {
        failure = Some(error.into());
        let _ = lifecycle.request_stop(TransitionReason::TaskFailure, "proxy readiness failed");
    } else if let Err(error) = notify_supervisor("READY=1") {
        failure = Some(
            anyhow::Error::new(error).context("failed to notify supervisor that proxy is ready"),
        );
        let _ = lifecycle.request_stop(
            TransitionReason::TaskFailure,
            "proxy supervisor readiness notification failed",
        );
    } else {
        info!(internal = %config.listen_address, peer = %peer_bind, control = %control_address, "proxy ready");
    }

    let mut updates = lifecycle.handle().subscribe();
    if failure.is_none() {
        loop {
            tokio::select! {
                request = shutdown_signal_request() => {
                    if let Err(error) = apply_shutdown_request(&lifecycle, request) {
                        failure = Some(error.into());
                        let _ = lifecycle.request_stop(
                            TransitionReason::TaskFailure,
                            "proxy shutdown transition failed",
                        );
                    }
                    break;
                }
                changed = updates.changed() => {
                    if changed.is_err() {
                        failure = Some(anyhow::anyhow!("lifecycle coordinator closed unexpectedly"));
                        let _ = lifecycle.request_stop(
                            TransitionReason::TaskFailure,
                            "proxy lifecycle coordinator closed",
                        );
                        break;
                    }
                    if updates.borrow().state >= ProcessState::Draining {
                        break;
                    }
                }
                outcome = tasks.next_completion() => {
                    if let Some(outcome) = outcome {
                        if expected_data_listener_exit(&outcome.name, &outcome.kind, lifecycle.current().state) {
                            break;
                        }
                        failure = Some(anyhow::anyhow!("required task {} exited: {:?}", outcome.name, outcome.kind));
                        let _ = lifecycle.request_stop(TransitionReason::TaskFailure, outcome.name);
                    } else {
                        failure = Some(anyhow::anyhow!("all required proxy tasks exited"));
                        let _ = lifecycle.request_stop(
                            TransitionReason::TaskFailure,
                            "all required proxy tasks exited",
                        );
                    }
                    break;
                }
            }
        }
    }

    let operation = if lifecycle.current().state == ProcessState::Stopping {
        ShutdownOperation::Stop
    } else {
        ShutdownOperation::Drain
    };
    let (mut deadline, listener_result) =
        close_data_plane(&admission, &data_shutdown, operation).await;
    if let Err(remaining) = listener_result {
        failure.get_or_insert_with(|| {
            anyhow::anyhow!(
                "proxy data-listener shutdown timed out with {remaining} listeners remaining"
            )
        });
    }
    if let Err(remaining) = admission.wait_for_idle(deadline).await {
        failure.get_or_insert_with(|| {
            anyhow::anyhow!("proxy drain timed out with {remaining} requests in flight")
        });
    }

    if failure.is_none() && lifecycle.current().state == ProcessState::Draining {
        loop {
            tokio::select! {
                request = shutdown_signal_request() => {
                    if let Err(error) = apply_shutdown_request(&lifecycle, request) {
                        failure = Some(error.into());
                        let _ = lifecycle.request_stop(
                            TransitionReason::TaskFailure,
                            "proxy stop transition failed",
                        );
                    }
                    break;
                }
                changed = updates.changed() => {
                    if changed.is_err() || updates.borrow().state == ProcessState::Stopping {
                        break;
                    }
                }
                outcome = tasks.next_completion() => {
                    if let Some(outcome) = outcome {
                        if expected_data_listener_exit(&outcome.name, &outcome.kind, lifecycle.current().state) {
                            continue;
                        }
                        failure = Some(anyhow::anyhow!("required task {} exited while draining: {:?}", outcome.name, outcome.kind));
                        let _ = lifecycle.request_stop(TransitionReason::TaskFailure, outcome.name);
                    } else {
                        failure = Some(anyhow::anyhow!("all required proxy tasks exited while draining"));
                        let _ = lifecycle.request_stop(
                            TransitionReason::TaskFailure,
                            "all required proxy tasks exited while draining",
                        );
                    }
                    break;
                }
            }
        }
        // Drain is a stable control state. A later Stop request receives a
        // separate final-stop budget rather than extending an active wait.
        let (stop_deadline, listener_result) =
            data_shutdown.stop_and_wait(ShutdownOperation::Stop).await;
        deadline = stop_deadline;
        if let Err(remaining) = listener_result {
            failure.get_or_insert_with(|| {
                anyhow::anyhow!(
                    "proxy data-listener stop timed out with {remaining} listeners remaining"
                )
            });
        }
    }

    if lifecycle.current().state != ProcessState::Stopping {
        if let Err(error) = lifecycle.request_stop(
            TransitionReason::ShutdownOrchestration,
            "proxy data plane drained",
        ) {
            failure.get_or_insert_with(|| error.into());
        }
    }
    if let Err(error) = notify_supervisor("STOPPING=1") {
        failure.get_or_insert_with(|| {
            anyhow::Error::new(error).context("failed to notify supervisor that proxy is stopping")
        });
    }
    let report = tasks.shutdown(deadline).await;
    if !report.is_clean() {
        failure.get_or_insert_with(|| {
            anyhow::anyhow!("proxy task shutdown was not clean: {report:?}")
        });
    }

    info!("proxy stopped");
    if let Some(error) = failure {
        Err(error)
    } else {
        Ok(())
    }
}

async fn accept_loop<S>(
    listener: TcpListener,
    service: S,
    admission: AdmissionGate,
    mut data_shutdown_rx: tokio::sync::watch::Receiver<bool>,
    data_shutdown: Arc<DataListenerShutdown>,
    mut cancellation: TaskCancellation,
) -> Result<TaskExit>
where
    S: Service<Request<ProxyBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Error: std::fmt::Display + Send + 'static,
    S::Future: Send + 'static,
{
    let mut listener_guard = DataListenerGuard::new(data_shutdown);
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => break,
            changed = data_shutdown_rx.changed() => {
                if changed.is_err() || *data_shutdown_rx.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                let (stream, peer_addr) = accepted?;
                let service = service.clone();
                let admission = admission.clone();
                let connection_cancellation = cancellation.clone();
                connections.spawn(async move {
                    let io = TokioIo::new(stream);
                    let hyper_service = hyper::service::service_fn(move |request: Request<hyper::body::Incoming>| {
                        let mut service = service.clone();
                        let admission = admission.clone();
                        async move {
                            let Some(guard) = admission.try_enter() else {
                                return Ok::<_, Infallible>(layers::error_response(
                                    StatusCode::SERVICE_UNAVAILABLE,
                                    "proxy is draining",
                                ));
                            };
                            let request = request.map(ProxyBody::streaming);
                            match service.call(request).await {
                                Ok(response) => Ok(response.map(|body| body.with_admission_guard(guard))),
                                Err(error) => {
                                    error!(%error, "proxy service error");
                                    Ok(layers::error_response(StatusCode::BAD_GATEWAY, "internal proxy error"))
                                }
                            }
                        }
                    });
                    let builder = auto::Builder::new(TokioExecutor::new());
                    let mut connection = Box::pin(builder.serve_connection(io, hyper_service));
                    let mut shutdown = connection_cancellation;
                    tokio::select! {
                        result = &mut connection => {
                            if let Err(error) = result {
                                debug!(peer = %peer_addr, %error, "proxy connection closed");
                            }
                        }
                        _ = shutdown.cancelled() => {
                            connection.as_mut().graceful_shutdown();
                            if let Err(error) = connection.await {
                                debug!(peer = %peer_addr, %error, "proxy connection closed during shutdown");
                            }
                        }
                    }
                });
            }
            joined = connections.join_next(), if !connections.is_empty() => {
                if let Some(result) = joined {
                    result.context("proxy connection task panicked")?;
                }
            }
        }
    }
    drop(listener);
    listener_guard.report();
    while let Some(result) = connections.join_next().await {
        result.context("proxy connection task panicked during drain")?;
    }
    Ok(TaskExit::Cancelled)
}

async fn tls_accept_loop<S>(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    service: S,
    admission: AdmissionGate,
    mut data_shutdown_rx: tokio::sync::watch::Receiver<bool>,
    data_shutdown: Arc<DataListenerShutdown>,
    mut cancellation: TaskCancellation,
) -> Result<TaskExit>
where
    S: Service<Request<ProxyBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Error: std::fmt::Display + Send + 'static,
    S::Future: Send + 'static,
{
    let mut listener_guard = DataListenerGuard::new(data_shutdown);
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => break,
            changed = data_shutdown_rx.changed() => {
                if changed.is_err() || *data_shutdown_rx.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                let (stream, peer_addr) = accepted?;
                let acceptor = acceptor.clone();
                let service = service.clone();
                let admission = admission.clone();
                let mut connection_cancellation = cancellation.clone();
                connections.spawn(async move {
                    let tls_stream = tokio::select! {
                        _ = connection_cancellation.cancelled() => return,
                        result = acceptor.accept(stream) => match result {
                            Ok(stream) => stream,
                            Err(error) => {
                                debug!(peer = %peer_addr, %error, "proxy TLS handshake closed");
                                return;
                            }
                        }
                    };
                    let io = TokioIo::new(tls_stream);
                    let hyper_service = hyper::service::service_fn(move |request: Request<hyper::body::Incoming>| {
                        let mut service = service.clone();
                        let admission = admission.clone();
                        async move {
                            let Some(guard) = admission.try_enter() else {
                                return Ok::<_, Infallible>(layers::error_response(
                                    StatusCode::SERVICE_UNAVAILABLE,
                                    "proxy is draining",
                                ));
                            };
                            let request = request.map(ProxyBody::streaming);
                            match service.call(request).await {
                                Ok(response) => Ok(response.map(|body| body.with_admission_guard(guard))),
                                Err(error) => {
                                    error!(%error, "proxy service error");
                                    Ok(layers::error_response(StatusCode::BAD_GATEWAY, "internal proxy error"))
                                }
                            }
                        }
                    });
                    let builder = auto::Builder::new(TokioExecutor::new());
                    let mut connection = Box::pin(builder.serve_connection(io, hyper_service));
                    tokio::select! {
                        result = &mut connection => {
                            if let Err(error) = result {
                                debug!(peer = %peer_addr, %error, "proxy TLS connection closed");
                            }
                        }
                        _ = connection_cancellation.cancelled() => {
                            connection.as_mut().graceful_shutdown();
                            if let Err(error) = connection.await {
                                debug!(peer = %peer_addr, %error, "proxy TLS connection closed during shutdown");
                            }
                        }
                    }
                });
            }
            joined = connections.join_next(), if !connections.is_empty() => {
                if let Some(result) = joined {
                    result.context("proxy TLS connection task panicked")?;
                }
            }
        }
    }
    drop(listener);
    listener_guard.report();
    while let Some(result) = connections.join_next().await {
        result.context("proxy TLS connection task panicked during drain")?;
    }
    Ok(TaskExit::Cancelled)
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;

    #[test]
    fn draining_only_tolerates_expected_data_listener_cancellation() {
        assert!(expected_data_listener_exit(
            "proxy-internal-listener",
            &TaskOutcomeKind::Cancelled,
            ProcessState::Draining,
        ));
        assert!(!expected_data_listener_exit(
            "proxy-control-listener",
            &TaskOutcomeKind::Cancelled,
            ProcessState::Draining,
        ));
        assert!(!expected_data_listener_exit(
            "proxy-manager-discovery",
            &TaskOutcomeKind::PrematureCompletion,
            ProcessState::Draining,
        ));
        assert!(!expected_data_listener_exit(
            "proxy-peer-listener",
            &TaskOutcomeKind::Cancelled,
            ProcessState::Ready,
        ));
    }

    #[tokio::test]
    async fn signal_path_closes_admission_before_listener_acknowledgement() {
        let admission = AdmissionGate::closed();
        admission.open();
        let (shutdown, _receiver) = DataListenerShutdown::new(1);
        let closing = close_data_plane(&admission, &shutdown, ShutdownOperation::Stop);
        tokio::pin!(closing);

        tokio::select! {
            result = &mut closing => panic!("listener barrier completed unexpectedly: {result:?}"),
            _ = tokio::task::yield_now() => {}
        }
        assert!(
            !admission.is_open(),
            "request admission remained open while listener acknowledgement was pending"
        );

        shutdown.listener_stopped();
        let (_, result) = closing.await;
        assert_eq!(result, Ok(()));
    }

    #[tokio::test]
    async fn data_listener_shutdown_obeys_and_reuses_failed_operation_deadline() {
        let (shutdown, _receiver) = DataListenerShutdown::new(1);
        let expected_deadline = tokio::time::Instant::now();
        *shutdown
            .operation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(DataListenerOperation {
            kind: ShutdownOperation::Drain,
            deadline: expected_deadline,
            result: None,
        });

        let first = shutdown.stop_and_wait(ShutdownOperation::Drain).await;
        let retry = shutdown.stop_and_wait(ShutdownOperation::Drain).await;
        let concurrent_stop = shutdown.stop_and_wait(ShutdownOperation::Stop).await;
        assert_eq!(first, (expected_deadline, Err(1)));
        assert_eq!(retry, first);
        assert_eq!(concurrent_stop, first);
    }

    #[tokio::test]
    async fn data_listener_shutdown_acknowledges_closed_listener() -> anyhow::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (shutdown, receiver) = DataListenerShutdown::new(1);
        let service = tower::service_fn(|_request: Request<ProxyBody>| async {
            Ok::<_, Infallible>(layers::error_response(StatusCode::OK, "ok"))
        });
        let admission = AdmissionGate::closed();
        admission.open();
        let mut tasks = TaskGroup::new();
        let listener_shutdown = Arc::clone(&shutdown);
        tasks.spawn("test-proxy-listener", move |cancellation| {
            accept_loop(
                listener,
                service,
                admission,
                receiver,
                listener_shutdown,
                cancellation,
            )
        });

        let (first, duplicate) = tokio::join!(
            shutdown.stop_and_wait(ShutdownOperation::Drain),
            shutdown.stop_and_wait(ShutdownOperation::Drain),
        );
        assert_eq!(first, duplicate, "duplicate drains must share one barrier");
        let (drain_deadline, drain_result) = first;
        drain_result.map_err(|remaining| {
            anyhow::anyhow!("{remaining} listeners did not acknowledge shutdown")
        })?;
        assert!(
            tokio::net::TcpStream::connect(address).await.is_err(),
            "drain acknowledgement returned before the data listener closed"
        );
        let (stop_deadline, stop_result) = shutdown.stop_and_wait(ShutdownOperation::Stop).await;
        stop_result.map_err(|remaining| {
            anyhow::anyhow!("{remaining} listeners remained after repeated shutdown")
        })?;
        assert!(
            stop_deadline > drain_deadline,
            "a later Stop must receive a distinct operation deadline"
        );

        let report = tasks
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(1))
            .await;
        assert!(report.is_clean(), "{report:?}");
        Ok(())
    }
}
