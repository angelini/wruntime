use std::future::Future;

use tokio::signal::unix::{signal, SignalKind};

use crate::process_lifecycle::{
    LifecycleOwner, LifecycleSnapshot, TransitionError, TransitionReason,
};
use crate::task_group::{TaskGroup, TaskOutcome};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShutdownRequest {
    pub reason: TransitionReason,
    pub detail: String,
}

impl ShutdownRequest {
    pub fn stop(reason: TransitionReason, detail: impl Into<String>) -> Self {
        Self {
            reason,
            detail: detail.into(),
        }
    }

    pub fn submit(
        self,
        lifecycle: &mut LifecycleOwner,
    ) -> Result<LifecycleSnapshot, TransitionError> {
        lifecycle.request_stop(self.reason, self.detail)
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ShutdownCause {
    Signal,
    RequiredTask(Option<TaskOutcome>),
}

/// Wait for the first process-owned shutdown cause and synchronously publish
/// STOPPING before returning to the executable's shutdown operations. Required
/// task evidence remains retained by `TaskGroup` for the final report.
pub async fn wait_for_shutdown_trigger<F>(
    lifecycle: &mut LifecycleOwner,
    tasks: &mut TaskGroup,
    signal: F,
) -> Result<ShutdownCause, TransitionError>
where
    F: Future<Output = ShutdownRequest>,
{
    tokio::select! {
        request = signal => {
            request.submit(lifecycle)?;
            Ok(ShutdownCause::Signal)
        }
        outcome = tasks.next_completion() => {
            let detail = outcome
                .as_ref()
                .map(|outcome| outcome.name.clone())
                .unwrap_or_else(|| "all required tasks exited".to_string());
            lifecycle.request_stop(TransitionReason::TaskFailure, detail)?;
            Ok(ShutdownCause::RequiredTask(outcome))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::process_lifecycle::{LifecycleOwner, ProcessState, ServiceKind, TransitionReason};

    use super::*;

    #[test]
    fn injected_stop_is_published_synchronously() {
        let mut lifecycle = LifecycleOwner::new(ServiceKind::Proxy, "proxy-test");
        let stopped = ShutdownRequest::stop(TransitionReason::TaskFailure, "test request")
            .submit(&mut lifecycle)
            .unwrap();
        assert_eq!(stopped.state, ProcessState::Stopping);
        assert_eq!(stopped.reason, TransitionReason::TaskFailure);
        assert_eq!(stopped.detail, "test request");
        assert_eq!(lifecycle.snapshot().current(), stopped);
    }

    #[tokio::test]
    async fn required_task_failure_is_published_and_retained_for_shutdown() {
        let mut lifecycle = LifecycleOwner::new(ServiceKind::Engine, "engine-test");
        let mut tasks = TaskGroup::new();
        tasks.spawn("engine-required", |_| async {
            anyhow::bail!("required task fixture failed")
        });

        let cause = wait_for_shutdown_trigger(
            &mut lifecycle,
            &mut tasks,
            std::future::pending::<ShutdownRequest>(),
        )
        .await
        .unwrap();
        assert!(matches!(
            cause,
            ShutdownCause::RequiredTask(Some(ref outcome)) if outcome.name == "engine-required"
        ));
        let stopped = lifecycle.current();
        assert_eq!(stopped.state, ProcessState::Stopping);
        assert_eq!(stopped.reason, TransitionReason::TaskFailure);
        assert_eq!(stopped.detail, "engine-required");

        let report = tasks
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(1))
            .await;
        assert!(!report.is_clean(), "observed failure must remain in report");
        assert!(report
            .failures()
            .any(|outcome| outcome.name == "engine-required"));
    }
}
