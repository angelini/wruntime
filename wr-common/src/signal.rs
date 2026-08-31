use tokio::signal::unix::{signal, SignalKind};

use crate::process_lifecycle::{
    LifecycleSnapshot, ProcessLifecycleCoordinator, TransitionError, TransitionReason,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ShutdownRequest {
    Drain {
        reason: TransitionReason,
        detail: String,
    },
    Stop {
        reason: TransitionReason,
        detail: String,
    },
}

impl ShutdownRequest {
    pub fn drain(reason: TransitionReason, detail: impl Into<String>) -> Self {
        Self::Drain {
            reason,
            detail: detail.into(),
        }
    }

    pub fn stop(reason: TransitionReason, detail: impl Into<String>) -> Self {
        Self::Stop {
            reason,
            detail: detail.into(),
        }
    }
}

/// Feed either an injected control request or an OS-signal request through the
/// same monotonic coordinator path.
pub fn apply_shutdown_request(
    coordinator: &ProcessLifecycleCoordinator,
    request: ShutdownRequest,
) -> Result<LifecycleSnapshot, TransitionError> {
    match request {
        ShutdownRequest::Drain { reason, detail } => coordinator.request_drain(reason, detail),
        ShutdownRequest::Stop { reason, detail } => {
            match coordinator.request_drain(reason, detail.clone()) {
                Ok(_) => coordinator.request_stop(reason, detail),
                Err(TransitionError::Backwards {
                    current: crate::process_lifecycle::ProcessState::Stopping,
                    requested: crate::process_lifecycle::ProcessState::Draining,
                }) => coordinator.request_stop(reason, detail),
                Err(error) => Err(error),
            }
        }
    }
}

pub async fn shutdown_signal_request() -> ShutdownRequest {
    let mut sigint = signal(SignalKind::interrupt()).expect("failed to register SIGINT handler");
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
    tokio::select! {
        _ = sigint.recv() => ShutdownRequest::stop(
            TransitionReason::SignalInterrupt,
            "SIGINT received",
        ),
        _ = sigterm.recv() => ShutdownRequest::stop(
            TransitionReason::SignalTerminate,
            "SIGTERM received",
        ),
    }
}

pub async fn shutdown_signal_into(
    coordinator: &ProcessLifecycleCoordinator,
) -> Result<LifecycleSnapshot, TransitionError> {
    apply_shutdown_request(coordinator, shutdown_signal_request().await)
}

/// Compatibility wait for service wiring that remains owned by lifecycle Plan 2.
/// New code should use [`shutdown_signal_into`].
pub async fn shutdown_signal() {
    let _ = shutdown_signal_request().await;
}

#[cfg(test)]
mod tests {
    use crate::process_lifecycle::{ProcessState, ServiceKind};

    use super::*;

    #[test]
    fn injected_stop_uses_the_coordinator_path() {
        let coordinator = ProcessLifecycleCoordinator::new(ServiceKind::Proxy, "proxy-test");
        let stopped = apply_shutdown_request(
            &coordinator,
            ShutdownRequest::stop(TransitionReason::ControlStopRequested, "test request"),
        )
        .unwrap();

        assert_eq!(stopped.state, ProcessState::Stopping);
        assert_eq!(stopped.reason, TransitionReason::ControlStopRequested);
        assert_eq!(stopped.detail, "test request");
    }

    #[test]
    fn injected_drain_does_not_imply_exit() {
        let coordinator = ProcessLifecycleCoordinator::new(ServiceKind::Manager, "manager-test");
        let draining = apply_shutdown_request(
            &coordinator,
            ShutdownRequest::drain(TransitionReason::ControlDrainRequested, "test drain"),
        )
        .unwrap();

        assert_eq!(draining.state, ProcessState::Draining);
    }

    #[test]
    fn duplicate_injected_stop_is_idempotent() {
        let coordinator = ProcessLifecycleCoordinator::new(ServiceKind::Engine, "engine-test");
        let request =
            || ShutdownRequest::stop(TransitionReason::ControlStopRequested, "test request");

        let first = apply_shutdown_request(&coordinator, request()).unwrap();
        let duplicate = apply_shutdown_request(&coordinator, request()).unwrap();

        assert_eq!(duplicate, first);
    }
}
