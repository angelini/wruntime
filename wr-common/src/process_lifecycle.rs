use std::fmt;
use std::time::SystemTime;

use tokio::sync::watch;

use crate::wruntime;

/// Maximum UTF-8 byte length accepted for explanatory transition detail.
pub const MAX_TRANSITION_DETAIL_BYTES: usize = 1_024;

/// Optional launcher-provided identity used to bind lifecycle observations to
/// one exact process activation.
pub const PROCESS_INSTANCE_ID_ENV: &str = "WRT_LIFECYCLE_INSTANCE_ID";

pub fn resolve_process_instance_id(default: impl Into<String>) -> String {
    std::env::var(PROCESS_INSTANCE_ID_ENV)
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default.into())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessState {
    Starting,
    Ready,
    Stopping,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceKind {
    Manager,
    Proxy,
    Engine,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransitionReason {
    ProcessStarted,
    StartupComplete,
    SignalInterrupt,
    SignalTerminate,
    ShutdownOrchestration,
    TaskFailure,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LifecycleSnapshot {
    pub state: ProcessState,
    pub service_kind: ServiceKind,
    pub process_instance_id: String,
    pub transitioned_at: SystemTime,
    pub reason: TransitionReason,
    pub detail: String,
}

impl From<&LifecycleSnapshot> for wruntime::LifecycleStatus {
    fn from(snapshot: &LifecycleSnapshot) -> Self {
        Self {
            state: wruntime::ProcessLifecycleState::from(snapshot.state) as i32,
            service_kind: wruntime::ServiceKind::from(snapshot.service_kind) as i32,
            process_instance_id: snapshot.process_instance_id.clone(),
            transitioned_at: Some(snapshot.transitioned_at.into()),
            reason: wruntime::LifecycleTransitionReason::from(snapshot.reason) as i32,
            detail: snapshot.detail.clone(),
        }
    }
}

impl From<ProcessState> for wruntime::ProcessLifecycleState {
    fn from(state: ProcessState) -> Self {
        match state {
            ProcessState::Starting => Self::Starting,
            ProcessState::Ready => Self::Ready,
            ProcessState::Stopping => Self::Stopping,
        }
    }
}

impl From<ServiceKind> for wruntime::ServiceKind {
    fn from(kind: ServiceKind) -> Self {
        match kind {
            ServiceKind::Manager => Self::Manager,
            ServiceKind::Proxy => Self::Proxy,
            ServiceKind::Engine => Self::Engine,
        }
    }
}

impl From<TransitionReason> for wruntime::LifecycleTransitionReason {
    fn from(reason: TransitionReason) -> Self {
        match reason {
            TransitionReason::ProcessStarted => Self::ProcessStarted,
            TransitionReason::StartupComplete => Self::StartupComplete,
            TransitionReason::SignalInterrupt => Self::SignalInterrupt,
            TransitionReason::SignalTerminate => Self::SignalTerminate,
            TransitionReason::ShutdownOrchestration => Self::ShutdownOrchestration,
            TransitionReason::TaskFailure => Self::TaskFailure,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransitionError {
    Backwards {
        current: ProcessState,
        requested: ProcessState,
    },
    InvalidReason {
        state: ProcessState,
        reason: TransitionReason,
    },
    DetailTooLong {
        actual: usize,
        maximum: usize,
    },
}

impl fmt::Display for TransitionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Backwards { current, requested } => write!(
                f,
                "illegal lifecycle transition from {current:?} to {requested:?}"
            ),
            Self::InvalidReason { state, reason } => write!(
                f,
                "invalid lifecycle transition reason {reason:?} for {state:?}"
            ),
            Self::DetailTooLong { actual, maximum } => write!(
                f,
                "lifecycle transition detail is {actual} bytes; maximum is {maximum}"
            ),
        }
    }
}

impl std::error::Error for TransitionError {}

/// Sole writer for process lifecycle state. Mutable ownership serializes every
/// transition; the private watch slot is the authoritative current snapshot.
pub struct LifecycleOwner {
    updates: watch::Sender<LifecycleSnapshot>,
}

/// Cloneable read-only snapshot side of the lifecycle owner.
#[derive(Clone)]
pub struct LifecycleSnapshotHandle {
    updates: watch::Receiver<LifecycleSnapshot>,
}

impl LifecycleOwner {
    pub fn new(service_kind: ServiceKind, process_instance_id: impl Into<String>) -> Self {
        let snapshot = LifecycleSnapshot {
            state: ProcessState::Starting,
            service_kind,
            process_instance_id: process_instance_id.into(),
            transitioned_at: SystemTime::now(),
            reason: TransitionReason::ProcessStarted,
            detail: String::new(),
        };
        let (updates, _) = watch::channel(snapshot);
        Self { updates }
    }

    pub fn snapshot(&self) -> LifecycleSnapshotHandle {
        LifecycleSnapshotHandle {
            updates: self.updates.subscribe(),
        }
    }

    pub fn current(&self) -> LifecycleSnapshot {
        self.updates.borrow().clone()
    }

    pub fn mark_ready(
        &mut self,
        detail: impl Into<String>,
    ) -> Result<LifecycleSnapshot, TransitionError> {
        self.transition(
            ProcessState::Ready,
            TransitionReason::StartupComplete,
            detail.into(),
        )
    }

    pub fn request_stop(
        &mut self,
        reason: TransitionReason,
        detail: impl Into<String>,
    ) -> Result<LifecycleSnapshot, TransitionError> {
        self.transition(ProcessState::Stopping, reason, detail.into())
    }

    fn transition(
        &mut self,
        requested: ProcessState,
        reason: TransitionReason,
        detail: String,
    ) -> Result<LifecycleSnapshot, TransitionError> {
        let reason_is_valid = match requested {
            ProcessState::Starting => false,
            ProcessState::Ready => reason == TransitionReason::StartupComplete,
            ProcessState::Stopping => matches!(
                reason,
                TransitionReason::SignalInterrupt
                    | TransitionReason::SignalTerminate
                    | TransitionReason::ShutdownOrchestration
                    | TransitionReason::TaskFailure
            ),
        };
        if !reason_is_valid {
            return Err(TransitionError::InvalidReason {
                state: requested,
                reason,
            });
        }
        if detail.len() > MAX_TRANSITION_DETAIL_BYTES {
            return Err(TransitionError::DetailTooLong {
                actual: detail.len(),
                maximum: MAX_TRANSITION_DETAIL_BYTES,
            });
        }

        // Clone under a short borrow, then release it before validating and
        // publishing the complete replacement.
        let current = self.current();
        if requested == ProcessState::Ready && current.state == ProcessState::Stopping {
            return Err(TransitionError::Backwards {
                current: current.state,
                requested,
            });
        }
        if requested == current.state {
            return Ok(current);
        }

        let next = LifecycleSnapshot {
            state: requested,
            service_kind: current.service_kind,
            process_instance_id: current.process_instance_id,
            transitioned_at: SystemTime::now(),
            reason,
            detail,
        };
        self.updates.send_replace(next.clone());
        Ok(next)
    }
}

impl LifecycleSnapshotHandle {
    pub fn current(&self) -> LifecycleSnapshot {
        self.updates.borrow().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lifecycle() -> LifecycleOwner {
        LifecycleOwner::new(ServiceKind::Engine, "engine-test")
    }

    #[test]
    fn owner_moves_starting_ready_stopping_and_identity_is_stable() {
        let mut lifecycle = lifecycle();
        let starting = lifecycle.current();
        assert_eq!(starting.state, ProcessState::Starting);
        assert_eq!(starting.reason, TransitionReason::ProcessStarted);
        assert_eq!(starting.process_instance_id, "engine-test");

        let ready = lifecycle.mark_ready("modules loaded").unwrap();
        assert_eq!(ready.state, ProcessState::Ready);
        let stopped = lifecycle
            .request_stop(TransitionReason::SignalTerminate, "SIGTERM")
            .unwrap();
        assert_eq!(stopped.state, ProcessState::Stopping);
        assert_eq!(stopped.process_instance_id, "engine-test");
        let proto = wruntime::LifecycleStatus::from(&stopped);
        assert_eq!(
            proto.state,
            wruntime::ProcessLifecycleState::Stopping as i32
        );
        assert_eq!(proto.process_instance_id, "engine-test");
    }

    #[test]
    fn duplicate_transitions_preserve_the_first_complete_snapshot() {
        let mut lifecycle = lifecycle();
        let first_ready = lifecycle.mark_ready("first ready").unwrap();
        let duplicate_ready = lifecycle.mark_ready("second ready").unwrap();
        assert_eq!(duplicate_ready, first_ready);

        let first_stop = lifecycle
            .request_stop(TransitionReason::SignalInterrupt, "first stop")
            .unwrap();
        let duplicate_stop = lifecycle
            .request_stop(TransitionReason::TaskFailure, "second stop")
            .unwrap();
        assert_eq!(duplicate_stop, first_stop);
        assert_eq!(lifecycle.current(), first_stop);
    }

    #[test]
    fn stopping_before_ready_is_terminal_and_late_ready_is_rejected() {
        let mut lifecycle = lifecycle();
        lifecycle
            .request_stop(TransitionReason::TaskFailure, "startup failed")
            .unwrap();
        assert!(matches!(
            lifecycle.mark_ready("too late"),
            Err(TransitionError::Backwards { .. })
        ));
        assert_eq!(lifecycle.snapshot().current().state, ProcessState::Stopping);
    }

    #[test]
    fn invalid_reason_and_oversized_utf8_detail_reject_without_publication() {
        let mut lifecycle = lifecycle();
        let initial = lifecycle.current();
        assert!(matches!(
            lifecycle.request_stop(TransitionReason::StartupComplete, "invalid"),
            Err(TransitionError::InvalidReason { .. })
        ));
        assert_eq!(lifecycle.current(), initial);

        let detail = "é".repeat(MAX_TRANSITION_DETAIL_BYTES / 2 + 1);
        assert!(matches!(
            lifecycle.request_stop(TransitionReason::TaskFailure, detail),
            Err(TransitionError::DetailTooLong { .. })
        ));
        assert_eq!(lifecycle.current(), initial);
    }

    #[test]
    fn handles_before_and_after_transition_read_one_coherent_latest_snapshot() {
        let mut lifecycle = lifecycle();
        let before = lifecycle.snapshot();
        let ready = lifecycle.mark_ready("complete ready snapshot").unwrap();
        let after = lifecycle.snapshot();

        for observed in [before.current(), after.current(), lifecycle.current()] {
            assert_eq!(observed, ready);
            assert_eq!(observed.reason, TransitionReason::StartupComplete);
            assert_eq!(observed.detail, "complete ready snapshot");
        }
    }
}
