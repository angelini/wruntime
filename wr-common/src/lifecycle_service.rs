use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::Notify;
use tokio::time::{timeout_at, Instant};
use tonic::{Request, Response, Status};

use crate::process_lifecycle::LifecycleSnapshotHandle;
use crate::wruntime::{
    lifecycle_service_server::LifecycleService, GetLifecycleStatusRequest,
    GetLifecycleStatusResponse,
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
}

impl LifecycleServiceAdapter {
    pub fn new(lifecycle: LifecycleSnapshotHandle) -> Self {
        Self { lifecycle }
    }
}

#[tonic::async_trait]
impl LifecycleService for LifecycleServiceAdapter {
    async fn get_status(
        &self,
        _request: Request<GetLifecycleStatusRequest>,
    ) -> Result<Response<GetLifecycleStatusResponse>, Status> {
        Ok(Response::new(GetLifecycleStatusResponse {
            status: Some((&self.lifecycle.current()).into()),
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

    use crate::process_lifecycle::{LifecycleDriver, ProcessState, ServiceKind, TransitionReason};
    use crate::task_group::TaskGroup;

    use super::*;

    #[tokio::test]
    async fn status_rpc_is_read_only() -> anyhow::Result<()> {
        let (driver, lifecycle) = LifecycleDriver::new(ServiceKind::Proxy, "proxy-test");
        let mut tasks = TaskGroup::new();
        tasks.spawn("lifecycle-driver", move |cancellation| {
            driver.run(cancellation)
        });
        lifecycle.mark_ready("test ready").await?;
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

        lifecycle
            .request_stop(TransitionReason::TaskFailure, "test complete")
            .await?;
        let report = tasks
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await;
        assert!(report.is_clean(), "{report:?}");
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
