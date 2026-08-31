use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::Notify;
use tokio::time::{timeout_at, Instant};
use tonic::{Request, Response, Status};

use crate::process_lifecycle::{ProcessLifecycleCoordinator, TransitionError, TransitionReason};
use crate::signal::{apply_shutdown_request, ShutdownRequest};
use crate::wruntime::{
    lifecycle_service_server::LifecycleService, DrainRequest, DrainResponse,
    GetLifecycleStatusRequest, GetLifecycleStatusResponse, StopRequest, StopResponse,
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

    /// Enter admitted work. The second open check closes the race with drain.
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

/// Lifecycle operation passed to an admission shutdown hook.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownOperation {
    Drain,
    Stop,
}

/// Thin tonic adapter over the shared lifecycle coordinator.
type ShutdownHook = Arc<
    dyn Fn(ShutdownOperation) -> Pin<Box<dyn Future<Output = Result<(), Status>> + Send>>
        + Send
        + Sync,
>;

#[derive(Clone)]
pub struct LifecycleServiceAdapter {
    coordinator: ProcessLifecycleCoordinator,
    admission: Option<AdmissionGate>,
    shutdown_hook: Option<ShutdownHook>,
}

impl LifecycleServiceAdapter {
    pub fn new(coordinator: ProcessLifecycleCoordinator, admission: Option<AdmissionGate>) -> Self {
        Self {
            coordinator,
            admission,
            shutdown_hook: None,
        }
    }

    pub fn with_shutdown_hook<F, Fut>(mut self, hook: F) -> Self
    where
        F: Fn(ShutdownOperation) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Status>> + Send + 'static,
    {
        self.shutdown_hook = Some(Arc::new(move |operation| Box::pin(hook(operation))));
        self
    }

    async fn close_admission(&self, operation: ShutdownOperation) -> Result<(), Status> {
        if let Some(admission) = &self.admission {
            admission.close();
        }
        if let Some(hook) = &self.shutdown_hook {
            hook(operation).await?;
        }
        Ok(())
    }
}

fn transition_status(error: TransitionError) -> Status {
    match error {
        TransitionError::DetailTooLong { .. } | TransitionError::InvalidReason { .. } => {
            Status::invalid_argument(error.to_string())
        }
        TransitionError::Backwards { .. } => Status::failed_precondition(error.to_string()),
    }
}

#[tonic::async_trait]
impl LifecycleService for LifecycleServiceAdapter {
    async fn get_status(
        &self,
        _request: Request<GetLifecycleStatusRequest>,
    ) -> Result<Response<GetLifecycleStatusResponse>, Status> {
        Ok(Response::new(GetLifecycleStatusResponse {
            status: Some((&self.coordinator.current()).into()),
        }))
    }

    async fn drain(
        &self,
        request: Request<DrainRequest>,
    ) -> Result<Response<DrainResponse>, Status> {
        let snapshot = apply_shutdown_request(
            &self.coordinator,
            ShutdownRequest::drain(
                TransitionReason::ControlDrainRequested,
                request.into_inner().detail,
            ),
        )
        .map_err(transition_status)?;
        self.close_admission(ShutdownOperation::Drain).await?;
        Ok(Response::new(DrainResponse {
            status: Some((&snapshot).into()),
        }))
    }

    async fn stop(&self, request: Request<StopRequest>) -> Result<Response<StopResponse>, Status> {
        let snapshot = apply_shutdown_request(
            &self.coordinator,
            ShutdownRequest::stop(
                TransitionReason::ControlStopRequested,
                request.into_inner().detail,
            ),
        )
        .map_err(transition_status)?;
        self.close_admission(ShutdownOperation::Stop).await?;
        Ok(Response::new(StopResponse {
            status: Some((&snapshot).into()),
        }))
    }
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

    use super::*;

    #[tokio::test]
    async fn lifecycle_shutdown_closes_admission_and_runs_hook_before_ack() -> anyhow::Result<()> {
        use crate::process_lifecycle::ServiceKind;

        let lifecycle = ProcessLifecycleCoordinator::new(ServiceKind::Proxy, "proxy-test");
        lifecycle.mark_ready("test ready")?;
        let gate = AdmissionGate::closed();
        gate.open();
        let hook_ran_after_close = Arc::new(AtomicBool::new(false));
        let hook_observer = Arc::clone(&hook_ran_after_close);
        let hook_gate = gate.clone();
        let adapter = LifecycleServiceAdapter::new(lifecycle, Some(gate.clone()))
            .with_shutdown_hook(move |_| {
                let hook_observer = Arc::clone(&hook_observer);
                let hook_gate = hook_gate.clone();
                async move {
                    hook_observer.store(!hook_gate.is_open(), Ordering::Release);
                    Ok(())
                }
            });

        let response = adapter
            .drain(Request::new(DrainRequest {
                detail: "test drain".into(),
            }))
            .await?
            .into_inner();

        assert!(!gate.is_open());
        assert!(hook_ran_after_close.load(Ordering::Acquire));
        assert_eq!(
            response.status.map(|status| status.state),
            Some(crate::wruntime::ProcessLifecycleState::Draining as i32)
        );
        Ok(())
    }

    #[tokio::test]
    async fn lifecycle_shutdown_hook_failure_is_returned_to_caller() -> anyhow::Result<()> {
        use crate::process_lifecycle::ServiceKind;

        let lifecycle = ProcessLifecycleCoordinator::new(ServiceKind::Proxy, "proxy-test");
        lifecycle.mark_ready("test ready")?;
        let adapter =
            LifecycleServiceAdapter::new(lifecycle.clone(), None).with_shutdown_hook(|_| async {
                Err(Status::deadline_exceeded("listener shutdown timed out"))
            });

        let error = adapter
            .drain(Request::new(DrainRequest {
                detail: "test drain".into(),
            }))
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("failing shutdown hook was acknowledged"))?;

        assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
        assert_eq!(
            lifecycle.current().state,
            crate::process_lifecycle::ProcessState::Draining
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
