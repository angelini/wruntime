use std::fmt;
use std::time::SystemTime;

use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{sleep_until, Instant};

use crate::task_group::{TaskCancellation, TaskExit};
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
    DriverClosed,
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
            Self::DriverClosed => write!(f, "lifecycle driver is no longer running"),
        }
    }
}

impl std::error::Error for TransitionError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LifecycleWaitError {
    Deadline { last_observation: LifecycleSnapshot },
    DriverClosed { last_observation: LifecycleSnapshot },
}

impl fmt::Display for LifecycleWaitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Deadline { last_observation } => write!(
                f,
                "lifecycle wait reached its deadline; last state was {:?}",
                last_observation.state
            ),
            Self::DriverClosed { last_observation } => write!(
                f,
                "lifecycle driver closed; last state was {:?}",
                last_observation.state
            ),
        }
    }
}

impl std::error::Error for LifecycleWaitError {}

type IntentResult = Result<LifecycleSnapshot, TransitionError>;

struct LifecycleIntent {
    requested: ProcessState,
    reason: TransitionReason,
    detail: String,
    acknowledgement: oneshot::Sender<IntentResult>,
}

/// Sole writer for process lifecycle state. It is deliberately not cloneable;
/// all callers hold a read-only snapshot handle and submit typed intents.
pub struct LifecycleDriver {
    snapshot: LifecycleSnapshot,
    updates: watch::Sender<LifecycleSnapshot>,
    intents: mpsc::UnboundedReceiver<LifecycleIntent>,
}

/// Cloneable read-only snapshot side of the lifecycle driver.
#[derive(Clone)]
pub struct LifecycleSnapshotHandle {
    updates: watch::Receiver<LifecycleSnapshot>,
}

/// Cloneable intent-delivery side retained only by the process owner.
#[derive(Clone)]
pub struct LifecycleHandle {
    snapshots: LifecycleSnapshotHandle,
    intents: mpsc::UnboundedSender<LifecycleIntent>,
}

impl LifecycleDriver {
    pub fn new(
        service_kind: ServiceKind,
        process_instance_id: impl Into<String>,
    ) -> (Self, LifecycleHandle) {
        let snapshot = LifecycleSnapshot {
            state: ProcessState::Starting,
            service_kind,
            process_instance_id: process_instance_id.into(),
            transitioned_at: SystemTime::now(),
            reason: TransitionReason::ProcessStarted,
            detail: String::new(),
        };
        let (updates, receiver) = watch::channel(snapshot.clone());
        let (intent_sender, intents) = mpsc::unbounded_channel();
        (
            Self {
                snapshot,
                updates,
                intents,
            },
            LifecycleHandle {
                snapshots: LifecycleSnapshotHandle { updates: receiver },
                intents: intent_sender,
            },
        )
    }

    pub async fn run(mut self, mut cancellation: TaskCancellation) -> anyhow::Result<TaskExit> {
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => return Ok(TaskExit::Cancelled),
                intent = self.intents.recv() => {
                    let Some(intent) = intent else {
                        return Ok(TaskExit::Completed);
                    };
                    let result = self.transition(intent.requested, intent.reason, intent.detail);
                    let _ = intent.acknowledgement.send(result);
                }
            }
        }
    }

    fn transition(
        &mut self,
        requested: ProcessState,
        reason: TransitionReason,
        detail: String,
    ) -> IntentResult {
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
        if requested == ProcessState::Ready && self.snapshot.state == ProcessState::Stopping {
            return Err(TransitionError::Backwards {
                current: self.snapshot.state,
                requested,
            });
        }
        if requested == self.snapshot.state {
            return Ok(self.snapshot.clone());
        }

        self.snapshot.state = requested;
        self.snapshot.transitioned_at = SystemTime::now();
        self.snapshot.reason = reason;
        self.snapshot.detail = detail;
        self.updates.send_replace(self.snapshot.clone());
        Ok(self.snapshot.clone())
    }
}

impl LifecycleHandle {
    pub fn snapshot(&self) -> LifecycleSnapshotHandle {
        self.snapshots.clone()
    }

    pub fn current(&self) -> LifecycleSnapshot {
        self.snapshots.current()
    }

    pub async fn mark_ready(
        &self,
        detail: impl Into<String>,
    ) -> Result<LifecycleSnapshot, TransitionError> {
        self.submit(
            ProcessState::Ready,
            TransitionReason::StartupComplete,
            detail.into(),
        )
        .await
    }

    pub async fn request_stop(
        &self,
        reason: TransitionReason,
        detail: impl Into<String>,
    ) -> Result<LifecycleSnapshot, TransitionError> {
        self.submit(ProcessState::Stopping, reason, detail.into())
            .await
    }

    async fn submit(
        &self,
        requested: ProcessState,
        reason: TransitionReason,
        detail: String,
    ) -> IntentResult {
        let (acknowledgement, response) = oneshot::channel();
        self.intents
            .send(LifecycleIntent {
                requested,
                reason,
                detail,
                acknowledgement,
            })
            .map_err(|_| TransitionError::DriverClosed)?;
        response.await.map_err(|_| TransitionError::DriverClosed)?
    }
}

impl LifecycleSnapshotHandle {
    pub fn current(&self) -> LifecycleSnapshot {
        self.updates.borrow().clone()
    }

    pub fn subscribe(&self) -> watch::Receiver<LifecycleSnapshot> {
        self.updates.clone()
    }

    /// Wait until startup reaches READY or shutdown has started.
    pub async fn await_ready_or_stopping(
        &self,
        deadline: Instant,
    ) -> Result<LifecycleSnapshot, LifecycleWaitError> {
        self.await_state(deadline, |state| state != ProcessState::Starting)
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
                        return Err(LifecycleWaitError::DriverClosed {
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
    use std::time::Duration;

    use super::*;
    use crate::task_group::TaskGroup;

    fn lifecycle() -> (TaskGroup, LifecycleHandle) {
        let (driver, handle) = LifecycleDriver::new(ServiceKind::Engine, "engine-test");
        let mut tasks = TaskGroup::new();
        tasks.spawn("lifecycle-driver", move |cancellation| {
            driver.run(cancellation)
        });
        (tasks, handle)
    }

    #[tokio::test]
    async fn only_driver_moves_starting_ready_stopping_and_identity_is_stable() {
        let (mut tasks, lifecycle) = lifecycle();
        assert_eq!(lifecycle.current().state, ProcessState::Starting);
        let ready = lifecycle.mark_ready("modules loaded").await.unwrap();
        assert_eq!(ready.state, ProcessState::Ready);
        let stopped = lifecycle
            .request_stop(TransitionReason::SignalTerminate, "SIGTERM")
            .await
            .unwrap();
        assert_eq!(stopped.state, ProcessState::Stopping);
        assert_eq!(stopped.process_instance_id, "engine-test");
        let proto = wruntime::LifecycleStatus::from(&stopped);
        assert_eq!(
            proto.state,
            wruntime::ProcessLifecycleState::Stopping as i32
        );
        assert_eq!(proto.process_instance_id, "engine-test");
        let report = tasks
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await;
        assert!(report.is_clean(), "{report:?}");
    }

    #[tokio::test]
    async fn duplicate_and_racing_stop_intents_are_idempotent() {
        let (mut tasks, lifecycle) = lifecycle();
        lifecycle.mark_ready("").await.unwrap();
        let (first, second) = tokio::join!(
            lifecycle.request_stop(TransitionReason::SignalInterrupt, "first"),
            lifecycle.request_stop(TransitionReason::TaskFailure, "second")
        );
        let first = first.unwrap();
        let second = second.unwrap();
        assert_eq!(first, second);
        let report = tasks
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await;
        assert!(report.is_clean(), "{report:?}");
    }

    #[tokio::test]
    async fn stopping_before_ready_is_terminal_and_late_ready_is_rejected() {
        let (mut tasks, lifecycle) = lifecycle();
        lifecycle
            .request_stop(TransitionReason::TaskFailure, "startup failed")
            .await
            .unwrap();
        assert!(matches!(
            lifecycle.mark_ready("too late").await,
            Err(TransitionError::Backwards { .. })
        ));
        assert_eq!(
            lifecycle
                .snapshot()
                .await_ready_or_stopping(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap()
                .state,
            ProcessState::Stopping
        );
        let report = tasks
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await;
        assert!(report.is_clean(), "{report:?}");
    }

    #[tokio::test]
    async fn oversized_detail_is_rejected_without_mutation() {
        let (mut tasks, lifecycle) = lifecycle();
        let error = lifecycle
            .request_stop(
                TransitionReason::TaskFailure,
                "x".repeat(MAX_TRANSITION_DETAIL_BYTES + 1),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, TransitionError::DetailTooLong { .. }));
        assert_eq!(lifecycle.current().state, ProcessState::Starting);
        let report = tasks
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await;
        assert!(report.is_clean(), "{report:?}");
    }
}
