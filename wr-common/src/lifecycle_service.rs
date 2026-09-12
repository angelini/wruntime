use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use anyhow::Context;
use tokio::sync::Notify;
use tokio::time::{timeout_at, Instant};
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use crate::lifecycle_observation::validate_lifecycle_status;
use crate::process_lifecycle::LifecycleSnapshotHandle;
use crate::wruntime::{
    lifecycle_service_client::LifecycleServiceClient, lifecycle_service_server::LifecycleService,
    GetLifecycleStatusRequest, GetLifecycleStatusResponse, LifecycleStatus, ProcessLifecycleState,
    ServiceKind,
};

struct AdmissionState {
    open: AtomicBool,
    in_flight: AtomicUsize,
    idle: Notify,
}

/// Shared semantic admission gate and in-flight request counter.
#[derive(Clone)]
pub struct AdmissionGate {
    state: Arc<AdmissionState>,
}

impl Default for AdmissionGate {
    fn default() -> Self {
        Self::closed()
    }
}

impl AdmissionGate {
    pub fn closed() -> Self {
        Self {
            state: Arc::new(AdmissionState {
                open: AtomicBool::new(false),
                in_flight: AtomicUsize::new(0),
                idle: Notify::new(),
            }),
        }
    }

    pub fn open(&self) {
        self.state.open.store(true, Ordering::Release);
    }

    pub fn close(&self) {
        self.state.open.store(false, Ordering::Release);
        if self.in_flight() == 0 {
            self.state.idle.notify_waiters();
        }
    }

    pub fn is_open(&self) -> bool {
        self.state.open.load(Ordering::Acquire)
    }

    pub fn in_flight(&self) -> usize {
        self.state.in_flight.load(Ordering::Acquire)
    }

    /// Enter admitted work. The second open check closes the race with shutdown.
    pub fn try_enter(&self) -> Option<AdmissionGuard> {
        if !self.is_open() {
            return None;
        }
        self.state.in_flight.fetch_add(1, Ordering::AcqRel);
        if !self.is_open() {
            self.leave();
            return None;
        }
        Some(AdmissionGuard { gate: self.clone() })
    }

    pub async fn wait_for_idle(&self, deadline: Instant) -> Result<(), usize> {
        loop {
            let remaining = self.in_flight();
            if remaining == 0 {
                return Ok(());
            }
            let notified = self.state.idle.notified();
            if self.in_flight() == 0 {
                return Ok(());
            }
            if timeout_at(deadline, notified).await.is_err() {
                return Err(self.in_flight());
            }
        }
    }

    fn leave(&self) {
        if self.state.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.state.idle.notify_waiters();
        }
    }
}

pub struct AdmissionGuard {
    gate: AdmissionGate,
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        self.gate.leave();
    }
}

/// Thin, read-only tonic adapter over the shared lifecycle snapshot handle.
#[derive(Clone)]
pub struct LifecycleServiceAdapter {
    lifecycle: LifecycleSnapshotHandle,
    manager: Option<ManagerLifecycleMetadata>,
}

#[derive(Clone, Debug, Default)]
struct ManagerRolloutObservation {
    admission: i32,
    rollout_operation_id: String,
    rollout_phase: i32,
    rollout_expected_set_hash: String,
}

/// Coherent rollout metadata shared by the manager's durable observer and its
/// read-only lifecycle endpoint.
#[derive(Clone, Debug, Default)]
pub struct ManagerLifecycleState {
    observation: Arc<RwLock<ManagerRolloutObservation>>,
}

impl ManagerLifecycleState {
    pub fn admission(&self) -> crate::wruntime::PrivilegedAdmissionState {
        let value = self
            .observation
            .read()
            .expect("manager lifecycle state poisoned")
            .admission;
        crate::wruntime::PrivilegedAdmissionState::try_from(value)
            .unwrap_or(crate::wruntime::PrivilegedAdmissionState::ClosedMismatch)
    }

    pub fn update(
        &self,
        admission: crate::wruntime::PrivilegedAdmissionState,
        rollout_operation_id: impl Into<String>,
        rollout_phase: i32,
        rollout_expected_set_hash: impl Into<String>,
    ) {
        *self
            .observation
            .write()
            .expect("manager lifecycle state poisoned") = ManagerRolloutObservation {
            admission: admission as i32,
            rollout_operation_id: rollout_operation_id.into(),
            rollout_phase,
            rollout_expected_set_hash: rollout_expected_set_hash.into(),
        };
    }
}

#[derive(Clone)]
struct ManagerLifecycleMetadata {
    manager_id: String,
    policy_generation: u64,
    policy_digest: String,
    rollout: ManagerLifecycleState,
}

impl LifecycleServiceAdapter {
    pub fn new(lifecycle: LifecycleSnapshotHandle) -> Self {
        Self {
            lifecycle,
            manager: None,
        }
    }

    pub fn new_manager(
        lifecycle: LifecycleSnapshotHandle,
        manager_id: String,
        policy_generation: u64,
        policy_digest: String,
        rollout: ManagerLifecycleState,
    ) -> Self {
        Self {
            lifecycle,
            manager: Some(ManagerLifecycleMetadata {
                manager_id,
                policy_generation,
                policy_digest,
                rollout,
            }),
        }
    }
}

#[tonic::async_trait]
impl LifecycleService for LifecycleServiceAdapter {
    async fn get_status(
        &self,
        _request: Request<GetLifecycleStatusRequest>,
    ) -> Result<Response<GetLifecycleStatusResponse>, Status> {
        let mut status: LifecycleStatus = (&self.lifecycle.current()).into();
        if let Some(manager) = &self.manager {
            status.manager_id = manager.manager_id.clone();
            status.process_ready = status.state == ProcessLifecycleState::Ready as i32;
            status.policy_generation = manager.policy_generation;
            status.policy_digest = manager.policy_digest.clone();
            let rollout = manager
                .rollout
                .observation
                .read()
                .expect("manager lifecycle state poisoned");
            status.privileged_admission = rollout.admission;
            status.rollout_operation_id = rollout.rollout_operation_id.clone();
            status.rollout_phase = rollout.rollout_phase;
            status.rollout_expected_set_hash = rollout.rollout_expected_set_hash.clone();
        }
        Ok(Response::new(GetLifecycleStatusResponse {
            status: Some(status),
        }))
    }
}

/// Query READY from an already-connected lifecycle client. Endpoint selection,
/// TLS policy, and channel construction deliberately remain service-local.
pub async fn query_ready_status(
    client: &mut LifecycleServiceClient<Channel>,
    expected_kind: ServiceKind,
) -> anyhow::Result<LifecycleStatus> {
    let status = client
        .get_status(GetLifecycleStatusRequest {})
        .await?
        .into_inner()
        .status;
    validate_ready_status(status, expected_kind)
}

fn validate_ready_status(
    status: Option<LifecycleStatus>,
    expected_kind: ServiceKind,
) -> anyhow::Result<LifecycleStatus> {
    let status = status.context("lifecycle response omitted status")?;
    let validated = validate_lifecycle_status(&status)?;
    anyhow::ensure!(
        validated.state == ProcessLifecycleState::Ready,
        "lifecycle state is not READY"
    );
    anyhow::ensure!(
        validated.service_kind == expected_kind,
        "lifecycle service kind mismatch: expected {}, observed {}",
        expected_kind.as_str_name(),
        validated.service_kind.as_str_name()
    );
    Ok(status)
}

/// Notify a systemd-style supervisor when `NOTIFY_SOCKET` is present.
pub fn notify_supervisor(message: &str) -> std::io::Result<()> {
    use std::os::unix::net::UnixDatagram;

    let Some(socket_name) = std::env::var_os("NOTIFY_SOCKET") else {
        return Ok(());
    };
    let socket = UnixDatagram::unbound()?;
    let bytes = socket_name.as_encoded_bytes();

    #[cfg(target_os = "linux")]
    if bytes.first() == Some(&b'@') {
        use std::os::linux::net::SocketAddrExt;
        let address = std::os::unix::net::SocketAddr::from_abstract_name(&bytes[1..])?;
        socket.send_to_addr(message.as_bytes(), &address)?;
        return Ok(());
    }

    socket.connect(std::path::Path::new(&socket_name))?;
    socket.send(message.as_bytes())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::process_lifecycle::{
        LifecycleOwner, ProcessState, ServiceKind as ProcessServiceKind,
    };

    use super::*;

    fn wire_status(state: i32, kind: i32, instance: &str) -> LifecycleStatus {
        LifecycleStatus {
            state,
            service_kind: kind,
            process_instance_id: instance.to_string(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn status_rpc_is_read_only() -> anyhow::Result<()> {
        let mut lifecycle = LifecycleOwner::new(ProcessServiceKind::Proxy, "proxy-test");
        lifecycle.mark_ready("test ready")?;
        let adapter = LifecycleServiceAdapter::new(lifecycle.snapshot());

        let first = adapter
            .get_status(Request::new(GetLifecycleStatusRequest {}))
            .await?
            .into_inner()
            .status
            .expect("status response");
        let second = adapter
            .get_status(Request::new(GetLifecycleStatusRequest {}))
            .await?
            .into_inner()
            .status
            .expect("status response");
        assert_eq!(first, second);
        assert_eq!(lifecycle.current().state, ProcessState::Ready);
        Ok(())
    }

    #[tokio::test]
    async fn ready_probe_queries_an_already_connected_client() -> anyhow::Result<()> {
        use tonic::transport::server::TcpIncoming;
        use tonic::transport::Server;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let incoming = TcpIncoming::from(listener);
        let mut lifecycle = LifecycleOwner::new(ProcessServiceKind::Proxy, "proxy-probe");
        lifecycle.mark_ready("probe ready")?;
        let service = crate::wruntime::lifecycle_service_server::LifecycleServiceServer::new(
            LifecycleServiceAdapter::new(lifecycle.snapshot()),
        );
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        let mut client = LifecycleServiceClient::connect(format!("http://{address}")).await?;
        let status = query_ready_status(&mut client, ServiceKind::Proxy).await?;
        assert_eq!(status.process_instance_id, "proxy-probe");
        assert_eq!(status.detail, "probe ready");

        let _ = shutdown_tx.send(());
        server.await??;
        Ok(())
    }

    #[test]
    fn ready_probe_validation_accepts_only_valid_ready_expected_kind() {
        let ready = wire_status(
            ProcessLifecycleState::Ready as i32,
            ServiceKind::Proxy as i32,
            "proxy-1",
        );
        assert_eq!(
            validate_ready_status(Some(ready.clone()), ServiceKind::Proxy).unwrap(),
            ready
        );

        let rejected = [
            None,
            Some(wire_status(99, ServiceKind::Proxy as i32, "proxy-1")),
            Some(wire_status(
                ProcessLifecycleState::Ready as i32,
                ServiceKind::Proxy as i32,
                "",
            )),
            Some(wire_status(
                ProcessLifecycleState::Starting as i32,
                ServiceKind::Proxy as i32,
                "proxy-1",
            )),
            Some(wire_status(
                ProcessLifecycleState::Stopping as i32,
                ServiceKind::Proxy as i32,
                "proxy-1",
            )),
            Some(wire_status(
                ProcessLifecycleState::Ready as i32,
                ServiceKind::Engine as i32,
                "proxy-1",
            )),
        ];
        for status in rejected {
            assert!(validate_ready_status(status, ServiceKind::Proxy).is_err());
        }
    }

    #[tokio::test]
    async fn manager_status_projects_the_latest_rollout_observation() -> anyhow::Result<()> {
        let mut lifecycle = LifecycleOwner::new(ProcessServiceKind::Manager, "manager-process");
        lifecycle.mark_ready("ready closed")?;
        let rollout = ManagerLifecycleState::default();
        rollout.update(
            crate::wruntime::PrivilegedAdmissionState::ClosedRollout,
            "11111111-1111-4111-8111-111111111111",
            crate::wruntime::ManagerRolloutPhase::ClosingOld as i32,
            format!("sha256:{}", "a".repeat(64)),
        );
        let adapter = LifecycleServiceAdapter::new_manager(
            lifecycle.snapshot(),
            "manager-a".into(),
            42,
            format!("sha256:{}", "b".repeat(64)),
            rollout,
        );
        let status = adapter
            .get_status(Request::new(GetLifecycleStatusRequest {}))
            .await?
            .into_inner()
            .status
            .expect("status");
        assert_eq!(status.manager_id, "manager-a");
        assert!(status.process_ready);
        assert_eq!(status.policy_generation, 42);
        assert_eq!(
            status.privileged_admission,
            crate::wruntime::PrivilegedAdmissionState::ClosedRollout as i32
        );
        assert_eq!(
            status.rollout_phase,
            crate::wruntime::ManagerRolloutPhase::ClosingOld as i32
        );
        Ok(())
    }

    #[tokio::test]
    async fn admission_closes_without_losing_in_flight_work() -> anyhow::Result<()> {
        let gate = AdmissionGate::closed();
        assert!(gate.try_enter().is_none());
        gate.open();
        let guard = gate
            .try_enter()
            .ok_or_else(|| anyhow::anyhow!("ready gate did not admit"))?;
        gate.close();
        assert!(gate.try_enter().is_none());
        assert_eq!(gate.in_flight(), 1);
        drop(guard);
        gate.wait_for_idle(Instant::now() + Duration::from_secs(1))
            .await
            .map_err(|remaining| anyhow::anyhow!("{remaining} requests remained"))?;
        Ok(())
    }
}
