mod circuit_breaker;
pub mod config;
pub mod indexed_routing;
mod layers;
pub mod node_service;
pub mod routing;
mod schema;
mod transcoding;

use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use http::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tonic::transport::server::TcpIncoming;
use tonic::transport::Server;
use tower::{Service, ServiceBuilder};

use layers::{
    EgressLayer, ForwardService, IngressLayer, ProxyBody, ResBody, RoutingLayer,
    SchemaValidationLayer, TracingLayer,
};
use tokio_rustls::TlsAcceptor;
use tracing::{debug, error, info};
use wr_common::discovery::ManagerDiscovery;
use wr_common::lifecycle_service::{notify_supervisor, AdmissionGate, LifecycleServiceAdapter};
use wr_common::process_lifecycle::{LifecycleDriver, ProcessState, ServiceKind, TransitionReason};
use wr_common::signal::{shutdown_signal_request, ShutdownCause, ShutdownRequest};
use wr_common::task_group::{TaskCancellation, TaskExit, TaskGroup};
use wr_common::wruntime::lifecycle_service_client::LifecycleServiceClient;
use wr_common::wruntime::lifecycle_service_server::LifecycleServiceServer;
use wr_common::wruntime::node_service_server::NodeServiceServer;
use wr_common::wruntime::{GetLifecycleStatusRequest, ProcessLifecycleState};

const SHUTDOWN_BUDGET: Duration = Duration::from_secs(30);

struct ProxyTaskScopes {
    background: TaskGroup,
    data_plane: TaskGroup,
    admission: AdmissionGate,
}

impl ProxyTaskScopes {
    fn new(admission: AdmissionGate) -> Self {
        Self {
            background: TaskGroup::new(),
            data_plane: TaskGroup::new(),
            admission,
        }
    }

    fn spawn_background<F, Fut>(&mut self, name: impl Into<String>, task: F)
    where
        F: FnOnce(TaskCancellation) -> Fut + Send + 'static,
        Fut: Future<Output = Result<TaskExit>> + Send + 'static,
    {
        self.background.spawn(name, task);
    }

    fn spawn_data_plane<F, Fut>(&mut self, name: impl Into<String>, task: F)
    where
        F: FnOnce(TaskCancellation) -> Fut + Send + 'static,
        Fut: Future<Output = Result<TaskExit>> + Send + 'static,
    {
        self.data_plane.spawn(name, task);
    }

    async fn shutdown_data_plane(&mut self, deadline: tokio::time::Instant) -> Result<()> {
        self.admission.close();
        let report = self.data_plane.shutdown(deadline).await;
        let idle = self.admission.wait_for_idle(deadline).await;
        match (report.is_clean(), idle) {
            (true, Ok(())) => Ok(()),
            (false, Ok(())) => anyhow::bail!("proxy data-plane task shutdown was not clean: {report:?}"),
            (true, Err(remaining)) => {
                anyhow::bail!("proxy drain timed out with {remaining} requests in flight")
            }
            (false, Err(remaining)) => anyhow::bail!(
                "proxy data-plane task shutdown was not clean: {report:?}; drain timed out with {remaining} requests in flight"
            ),
        }
    }

    async fn shutdown_background(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> wr_common::task_group::TaskShutdownReport {
        self.background.shutdown(deadline).await
    }
}

async fn wait_for_proxy_shutdown_trigger<F>(
    lifecycle: &wr_common::process_lifecycle::LifecycleHandle,
    scopes: &mut ProxyTaskScopes,
    signal: F,
) -> std::result::Result<ShutdownCause, wr_common::process_lifecycle::TransitionError>
where
    F: Future<Output = ShutdownRequest>,
{
    let cause = tokio::select! {
        request = signal => {
            request.submit(lifecycle).await?;
            return Ok(ShutdownCause::Signal);
        }
        outcome = scopes.background.next_completion() => ShutdownCause::RequiredTask(outcome),
        outcome = scopes.data_plane.next_completion() => ShutdownCause::RequiredTask(outcome),
    };
    let detail = match &cause {
        ShutdownCause::RequiredTask(Some(outcome)) => outcome.name.clone(),
        ShutdownCause::RequiredTask(None) => "all required proxy tasks exited".to_string(),
        ShutdownCause::Signal => unreachable!(),
    };
    lifecycle
        .request_stop(TransitionReason::TaskFailure, detail)
        .await?;
    Ok(cause)
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
    let (lifecycle_driver, lifecycle) = LifecycleDriver::new(
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
        lifecycle.snapshot(),
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

    let lifecycle_service = LifecycleServiceAdapter::new(lifecycle.snapshot());
    let control_router = Server::builder()
        .add_service(NodeServiceServer::from_arc(Arc::clone(&node_agent)))
        .add_service(LifecycleServiceServer::new(lifecycle_service));

    let mut scopes = ProxyTaskScopes::new(admission.clone());
    scopes.spawn_background("lifecycle-driver", move |cancellation| {
        lifecycle_driver.run(cancellation)
    });
    {
        let discovery = Arc::clone(&discovery);
        scopes.spawn_background("proxy-manager-discovery", move |cancellation| {
            discovery.run_refresh_loop(cancellation)
        });
    }
    {
        let discovery = Arc::clone(&discovery);
        let table = routing_table.clone();
        let ttl = config.cache.routing_table_ttl_secs;
        scopes.spawn_background("proxy-routing-sync", move |cancellation| {
            routing::sync_routing_table(discovery, table, ttl, cancellation)
        });
    }
    {
        let node_agent = Arc::clone(&node_agent);
        scopes.spawn_background("proxy-heartbeat-flush", move |cancellation| {
            node_agent.run_heartbeat_loop(Duration::from_secs(3), cancellation)
        });
    }
    scopes.spawn_background("proxy-control-listener", move |cancellation| async move {
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
        scopes.spawn_data_plane("proxy-internal-listener", move |cancellation| {
            accept_loop(internal_listener, service, admission, cancellation)
        });
    }
    {
        let admission = admission.clone();
        scopes.spawn_data_plane("proxy-peer-listener", move |cancellation| {
            tls_accept_loop(
                peer_listener,
                tls_acceptor,
                internal_service,
                admission,
                cancellation,
            )
        });
    }
    if let Some((listener, service, address)) = external {
        let admission = admission.clone();
        scopes.spawn_data_plane("proxy-external-listener", move |cancellation| async move {
            info!(%address, "proxy external listener bound");
            accept_loop(listener, service, admission, cancellation).await
        });
    }

    let mut failure: Option<anyhow::Error> = None;
    admission.open();
    if let Err(error) = lifecycle
        .mark_ready("manager discovery, routing snapshot, control, and data listeners ready")
        .await
    {
        failure = Some(error.into());
        let _ = lifecycle
            .request_stop(TransitionReason::TaskFailure, "proxy readiness failed")
            .await;
    } else if let Err(error) = notify_supervisor("READY=1") {
        failure = Some(
            anyhow::Error::new(error).context("failed to notify supervisor that proxy is ready"),
        );
        let _ = lifecycle
            .request_stop(
                TransitionReason::TaskFailure,
                "proxy supervisor readiness notification failed",
            )
            .await;
    } else {
        info!(internal = %config.listen_address, peer = %peer_bind, control = %control_address, "proxy ready");
    }

    if failure.is_none() {
        match wait_for_proxy_shutdown_trigger(&lifecycle, &mut scopes, shutdown_signal_request())
            .await
        {
            Ok(ShutdownCause::Signal) => {}
            Ok(ShutdownCause::RequiredTask(Some(outcome))) => {
                failure = Some(anyhow::anyhow!(
                    "required task {} exited: {:?}",
                    outcome.name,
                    outcome.kind
                ));
            }
            Ok(ShutdownCause::RequiredTask(None)) => {
                failure = Some(anyhow::anyhow!("all required proxy tasks exited"));
            }
            Err(error) => failure = Some(error.into()),
        }
    }

    if lifecycle.current().state != ProcessState::Stopping {
        if let Err(error) = lifecycle
            .request_stop(
                TransitionReason::ShutdownOrchestration,
                "proxy shutdown started",
            )
            .await
        {
            failure.get_or_insert_with(|| error.into());
        }
    }
    let deadline = tokio::time::Instant::now() + SHUTDOWN_BUDGET;
    if let Err(error) = scopes.shutdown_data_plane(deadline).await {
        failure.get_or_insert(error);
    }

    if let Err(error) = notify_supervisor("STOPPING=1") {
        failure.get_or_insert_with(|| {
            anyhow::Error::new(error).context("failed to notify supervisor that proxy is stopping")
        });
    }
    let report = scopes.shutdown_background(deadline).await;
    if !report.is_clean() {
        failure.get_or_insert_with(|| {
            anyhow::anyhow!("proxy background task shutdown was not clean: {report:?}")
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
    mut cancellation: TaskCancellation,
) -> Result<TaskExit>
where
    S: Service<Request<ProxyBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Error: std::fmt::Display + Send + 'static,
    S::Future: Send + 'static,
{
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => break,
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
    mut cancellation: TaskCancellation,
) -> Result<TaskExit>
where
    S: Service<Request<ProxyBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Error: std::fmt::Display + Send + 'static,
    S::Future: Send + 'static,
{
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => break,
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
    while let Some(result) = connections.join_next().await {
        result.context("proxy TLS connection task panicked during drain")?;
    }
    Ok(TaskExit::Cancelled)
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;

    #[tokio::test]
    async fn required_task_failure_starts_nonzero_proxy_shutdown() -> anyhow::Result<()> {
        let (driver, lifecycle) = LifecycleDriver::new(ServiceKind::Proxy, "proxy-test");
        let admission = AdmissionGate::closed();
        let mut scopes = ProxyTaskScopes::new(admission);
        scopes.spawn_background("lifecycle-driver", move |cancellation| {
            driver.run(cancellation)
        });
        scopes.spawn_data_plane("proxy-required", |_| async {
            anyhow::bail!("proxy required task fixture failed")
        });

        let cause = wait_for_proxy_shutdown_trigger(
            &lifecycle,
            &mut scopes,
            std::future::pending::<ShutdownRequest>(),
        )
        .await?;
        assert!(matches!(
            cause,
            ShutdownCause::RequiredTask(Some(ref outcome)) if outcome.name == "proxy-required"
        ));
        assert_eq!(lifecycle.current().state, ProcessState::Stopping);
        assert_eq!(lifecycle.current().reason, TransitionReason::TaskFailure);

        let error = scopes
            .shutdown_data_plane(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("not clean"),
            "task failure must remain nonzero evidence: {error:#}"
        );
        let report = scopes
            .shutdown_background(tokio::time::Instant::now() + Duration::from_secs(1))
            .await;
        assert!(report.is_clean(), "{report:?}");
        Ok(())
    }

    #[tokio::test]
    async fn component_scope_closes_admission_before_listener_cancellation() -> anyhow::Result<()> {
        let admission = AdmissionGate::closed();
        admission.open();
        let observer = admission.clone();
        let mut scopes = ProxyTaskScopes::new(admission);
        let (reported, observed) = tokio::sync::oneshot::channel();
        scopes.spawn_data_plane("listener", move |mut cancellation| async move {
            cancellation.cancelled().await;
            let _ = reported.send(!observer.is_open());
            Ok(TaskExit::Cancelled)
        });

        scopes
            .shutdown_data_plane(tokio::time::Instant::now() + Duration::from_secs(1))
            .await?;
        assert!(observed.await?);
        Ok(())
    }

    #[tokio::test]
    async fn component_scope_attributes_deadline_and_joins_aborted_listener() {
        let admission = AdmissionGate::closed();
        admission.open();
        let mut scopes = ProxyTaskScopes::new(admission);
        scopes.spawn_data_plane("stuck-listener", |_| async {
            std::future::pending::<()>().await;
            Ok(TaskExit::Completed)
        });

        let error = scopes
            .shutdown_data_plane(tokio::time::Instant::now())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("stuck-listener"));
        assert!(scopes.data_plane.is_empty());
    }

    #[tokio::test]
    async fn component_scope_shutdown_acknowledges_closed_listener() -> anyhow::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let service = tower::service_fn(|_request: Request<ProxyBody>| async {
            Ok::<_, Infallible>(layers::error_response(StatusCode::OK, "ok"))
        });
        let admission = AdmissionGate::closed();
        admission.open();
        let listener_admission = admission.clone();
        let mut scopes = ProxyTaskScopes::new(admission);
        scopes.spawn_data_plane("test-proxy-listener", move |cancellation| {
            accept_loop(listener, service, listener_admission, cancellation)
        });

        scopes
            .shutdown_data_plane(tokio::time::Instant::now() + Duration::from_secs(1))
            .await?;
        assert!(
            tokio::net::TcpStream::connect(address).await.is_err(),
            "component shutdown returned before the data listener closed"
        );
        Ok(())
    }
}
