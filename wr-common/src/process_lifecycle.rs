use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use tokio::sync::watch;
use tokio::time::{sleep_until, Instant};

use crate::wruntime;

/// Maximum UTF-8 byte length accepted for explanatory transition detail.
pub const MAX_TRANSITION_DETAIL_BYTES: usize = 1_024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum ProcessState {
    Starting,
    Ready,
    Draining,
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
    ControlDrainRequested,
    ControlStopRequested,
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
            ProcessState::Draining => Self::Draining,
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
            TransitionReason::ControlDrainRequested => Self::ControlDrainRequested,
            TransitionReason::ControlStopRequested => Self::ControlStopRequested,
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
            Self::Backwards { current, requested } => {
                write!(
                    f,
                    "illegal lifecycle transition from {current:?} to {requested:?}"
                )
            }
            Self::InvalidReason { state, reason } => {
                write!(
                    f,
                    "invalid lifecycle transition reason {reason:?} for {state:?}"
                )
            }
            Self::DetailTooLong { actual, maximum } => write!(
                f,
                "lifecycle transition detail is {actual} bytes; maximum is {maximum}"
            ),
        }
    }
}

impl std::error::Error for TransitionError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LifecycleWaitError {
    Deadline { last_observation: LifecycleSnapshot },
    CoordinatorClosed { last_observation: LifecycleSnapshot },
}

impl fmt::Display for LifecycleWaitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Deadline { last_observation } => write!(
                f,
                "lifecycle wait reached its deadline; last state was {:?}",
                last_observation.state
            ),
            Self::CoordinatorClosed { last_observation } => write!(
                f,
                "lifecycle coordinator closed; last state was {:?}",
                last_observation.state
            ),
        }
    }
}

impl std::error::Error for LifecycleWaitError {}

struct CoordinatorInner {
    snapshot: Mutex<LifecycleSnapshot>,
    updates: watch::Sender<LifecycleSnapshot>,
}

/// Cloneable owner side of the process lifecycle state machine.
#[derive(Clone)]
pub struct ProcessLifecycleCoordinator {
    inner: Arc<CoordinatorInner>,
}

/// Cloneable read/subscription side of the process lifecycle state machine.
#[derive(Clone)]
pub struct ProcessLifecycleHandle {
    updates: watch::Receiver<LifecycleSnapshot>,
}

impl ProcessLifecycleCoordinator {
    pub fn new(service_kind: ServiceKind, process_instance_id: impl Into<String>) -> Self {
        let snapshot = LifecycleSnapshot {
            state: ProcessState::Starting,
            service_kind,
            process_instance_id: process_instance_id.into(),
            transitioned_at: SystemTime::now(),
            reason: TransitionReason::ProcessStarted,
            detail: String::new(),
        };
        let (updates, _) = watch::channel(snapshot.clone());
        Self {
            inner: Arc::new(CoordinatorInner {
                snapshot: Mutex::new(snapshot),
                updates,
            }),
        }
    }

    pub fn handle(&self) -> ProcessLifecycleHandle {
        ProcessLifecycleHandle {
            updates: self.inner.updates.subscribe(),
        }
    }

    pub fn current(&self) -> LifecycleSnapshot {
        self.inner
            .snapshot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn mark_ready(
        &self,
        detail: impl Into<String>,
    ) -> Result<LifecycleSnapshot, TransitionError> {
        self.transition(
            ProcessState::Ready,
            TransitionReason::StartupComplete,
            detail,
        )
    }

    pub fn request_drain(
        &self,
        reason: TransitionReason,
        detail: impl Into<String>,
    ) -> Result<LifecycleSnapshot, TransitionError> {
        self.transition(ProcessState::Draining, reason, detail)
    }

    pub fn request_stop(
        &self,
        reason: TransitionReason,
        detail: impl Into<String>,
    ) -> Result<LifecycleSnapshot, TransitionError> {
        self.transition(ProcessState::Stopping, reason, detail)
    }

    fn transition(
        &self,
        requested: ProcessState,
        reason: TransitionReason,
        detail: impl Into<String>,
    ) -> Result<LifecycleSnapshot, TransitionError> {
        let reason_is_valid = match requested {
            ProcessState::Starting => false,
            ProcessState::Ready => reason == TransitionReason::StartupComplete,
            ProcessState::Draining => matches!(
                reason,
                TransitionReason::ControlDrainRequested
                    | TransitionReason::ControlStopRequested
                    | TransitionReason::SignalInterrupt
                    | TransitionReason::SignalTerminate
                    | TransitionReason::ShutdownOrchestration
                    | TransitionReason::TaskFailure
            ),
            ProcessState::Stopping => matches!(
                reason,
                TransitionReason::ControlStopRequested
                    | TransitionReason::SignalInterrupt
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

        let detail = detail.into();
        if detail.len() > MAX_TRANSITION_DETAIL_BYTES {
            return Err(TransitionError::DetailTooLong {
                actual: detail.len(),
                maximum: MAX_TRANSITION_DETAIL_BYTES,
            });
        }

        let mut current = self
            .inner
            .snapshot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if requested < current.state {
            return Err(TransitionError::Backwards {
                current: current.state,
                requested,
            });
        }
        if requested == current.state {
            return Ok(current.clone());
        }

        current.state = requested;
        current.transitioned_at = SystemTime::now();
        current.reason = reason;
        current.detail = detail;
        let updated = current.clone();
        self.inner.updates.send_replace(updated.clone());
        Ok(updated)
    }
}

impl ProcessLifecycleHandle {
    pub fn current(&self) -> LifecycleSnapshot {
        self.updates.borrow().clone()
    }

    pub fn subscribe(&self) -> watch::Receiver<LifecycleSnapshot> {
        self.updates.clone()
    }

    /// Wait until startup reaches READY or has already entered a terminal path.
    pub async fn await_ready_or_terminal(
        &self,
        deadline: Instant,
    ) -> Result<LifecycleSnapshot, LifecycleWaitError> {
        self.await_state(deadline, |state| state >= ProcessState::Ready)
            .await
    }

    /// DRAINING and STOPPING both satisfy a drain wait because states are monotonic.
    pub async fn await_drain(
        &self,
        deadline: Instant,
    ) -> Result<LifecycleSnapshot, LifecycleWaitError> {
        self.await_state(deadline, |state| state >= ProcessState::Draining)
            .await
    }

    pub async fn await_stop(
        &self,
        deadline: Instant,
    ) -> Result<LifecycleSnapshot, LifecycleWaitError> {
        self.await_state(deadline, |state| state == ProcessState::Stopping)
            .await
    }

    async fn await_state(
        &self,
        deadline: Instant,
        reached: impl Fn(ProcessState) -> bool,
    ) -> Result<LifecycleSnapshot, LifecycleWaitError> {
        let mut updates = self.updates.clone();
        loop {
            let snapshot = updates.borrow_and_update().clone();
            if reached(snapshot.state) {
                return Ok(snapshot);
            }
            if Instant::now() >= deadline {
                return Err(LifecycleWaitError::Deadline {
                    last_observation: snapshot,
                });
            }

            tokio::select! {
                _ = sleep_until(deadline) => {
                    return Err(LifecycleWaitError::Deadline {
                        last_observation: updates.borrow().clone(),
                    });
                }
                changed = updates.changed() => {
                    if changed.is_err() {
                        return Err(LifecycleWaitError::CoordinatorClosed {
                            last_observation: updates.borrow().clone(),
                        });
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Duration;

    use super::*;

    fn coordinator() -> ProcessLifecycleCoordinator {
        ProcessLifecycleCoordinator::new(ServiceKind::Engine, "engine-test")
    }

    #[test]
    fn legal_transitions_are_monotonic_and_export_typed_status() {
        let lifecycle = coordinator();
        assert_eq!(lifecycle.current().state, ProcessState::Starting);
        lifecycle.mark_ready("modules loaded").unwrap();
        lifecycle
            .request_drain(TransitionReason::ControlDrainRequested, "operator request")
            .unwrap();
        let stopped = lifecycle
            .request_stop(TransitionReason::ShutdownOrchestration, "tasks joined")
            .unwrap();
        assert_eq!(stopped.state, ProcessState::Stopping);

        let proto = wruntime::LifecycleStatus::from(&stopped);
        assert_eq!(
            proto.state,
            wruntime::ProcessLifecycleState::Stopping as i32
        );
        assert_eq!(proto.process_instance_id, "engine-test");
    }

    #[test]
    fn duplicate_drain_and_stop_requests_are_idempotent() {
        let lifecycle = coordinator();
        let first = lifecycle
            .request_drain(TransitionReason::ControlDrainRequested, "first")
            .unwrap();
        let duplicate = lifecycle
            .request_drain(TransitionReason::SignalInterrupt, "duplicate")
            .unwrap();
        assert_eq!(duplicate, first);

        let first = lifecycle
            .request_stop(TransitionReason::ControlStopRequested, "first")
            .unwrap();
        let duplicate = lifecycle
            .request_stop(TransitionReason::SignalTerminate, "duplicate")
            .unwrap();
        assert_eq!(duplicate, first);
    }

    #[test]
    fn backwards_and_oversized_transitions_are_rejected() {
        let lifecycle = coordinator();
        lifecycle
            .request_drain(TransitionReason::ControlDrainRequested, "")
            .unwrap();
        assert!(matches!(
            lifecycle.mark_ready("too late"),
            Err(TransitionError::Backwards { .. })
        ));
        assert!(matches!(
            lifecycle.request_stop(
                TransitionReason::ControlStopRequested,
                "x".repeat(MAX_TRANSITION_DETAIL_BYTES + 1)
            ),
            Err(TransitionError::DetailTooLong { .. })
        ));

        let invalid_reason = coordinator()
            .request_drain(TransitionReason::StartupComplete, "not a drain reason")
            .unwrap_err();
        assert!(matches!(
            invalid_reason,
            TransitionError::InvalidReason {
                state: ProcessState::Draining,
                reason: TransitionReason::StartupComplete,
            }
        ));
    }

    #[tokio::test]
    async fn readiness_wait_returns_ready_terminal_and_deadline_observations() {
        let ready = coordinator();
        let ready_handle = ready.handle();
        ready.mark_ready("").unwrap();
        assert_eq!(
            ready_handle
                .await_ready_or_terminal(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap()
                .state,
            ProcessState::Ready
        );

        let stopping = coordinator();
        let stopping_handle = stopping.handle();
        stopping
            .request_stop(TransitionReason::ControlStopRequested, "")
            .unwrap();
        assert_eq!(
            stopping_handle
                .await_ready_or_terminal(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap()
                .state,
            ProcessState::Stopping
        );

        let waiting = coordinator();
        let error = waiting
            .handle()
            .await_ready_or_terminal(Instant::now())
            .await
            .unwrap_err();
        assert!(matches!(error, LifecycleWaitError::Deadline { .. }));
    }

    #[test]
    fn concurrent_requests_select_the_furthest_state() {
        let lifecycle = Arc::new(coordinator());
        let barrier = Arc::new(Barrier::new(3));
        let drain = {
            let lifecycle = Arc::clone(&lifecycle);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                let _ = lifecycle.request_drain(TransitionReason::SignalInterrupt, "signal");
            })
        };
        let stop = {
            let lifecycle = Arc::clone(&lifecycle);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                lifecycle
                    .request_stop(TransitionReason::ControlStopRequested, "control")
                    .unwrap();
            })
        };
        barrier.wait();
        drain.join().unwrap();
        stop.join().unwrap();
        assert_eq!(lifecycle.current().state, ProcessState::Stopping);
    }
}
