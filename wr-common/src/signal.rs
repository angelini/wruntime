use std::future::Future;

use tokio::signal::unix::{signal, SignalKind};

use crate::process_lifecycle::{
    LifecycleHandle, LifecycleSnapshot, TransitionError, TransitionReason,
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

    pub async fn submit(
        self,
        lifecycle: &LifecycleHandle,
    ) -> Result<LifecycleSnapshot, TransitionError> {
        lifecycle.request_stop(self.reason, self.detail).await
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

/// Wait for the first process-owned shutdown cause and submit exactly one stop
/// intent through the lifecycle driver. Required task evidence is retained for
/// the executable's service-specific nonzero result.
pub async fn wait_for_shutdown_trigger<F>(
    lifecycle: &LifecycleHandle,
    tasks: &mut TaskGroup,
    signal: F,
) -> Result<ShutdownCause, TransitionError>
where
    F: Future<Output = ShutdownRequest>,
{
    tokio::select! {
        request = signal => {
            request.submit(lifecycle).await?;
            Ok(ShutdownCause::Signal)
        }
        outcome = tasks.next_completion() => {
            let detail = outcome
                .as_ref()
                .map(|outcome| outcome.name.clone())
                .unwrap_or_else(|| "all required tasks exited".to_string());
            lifecycle
                .request_stop(TransitionReason::TaskFailure, detail)
                .await?;
            Ok(ShutdownCause::RequiredTask(outcome))
        }
    }
}

pub async fn shutdown_signal_into(
    lifecycle: &LifecycleHandle,
) -> Result<LifecycleSnapshot, TransitionError> {
    shutdown_signal_request().await.submit(lifecycle).await
}

/// Compatibility wait for service wiring that remains owned by lifecycle Plan 2.
pub async fn shutdown_signal() {
    let _ = shutdown_signal_request().await;
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::process_lifecycle::{LifecycleDriver, ProcessState, ServiceKind, TransitionReason};
    use crate::task_group::TaskGroup;

    use super::*;

    #[tokio::test]
    async fn injected_stop_is_delivered_to_the_driver() {
        let (driver, lifecycle) = LifecycleDriver::new(ServiceKind::Proxy, "proxy-test");
        let mut tasks = TaskGroup::new();
        tasks.spawn("lifecycle-driver", move |cancellation| {
            driver.run(cancellation)
        });

        let stopped = ShutdownRequest::stop(TransitionReason::TaskFailure, "test request")
            .submit(&lifecycle)
            .await
            .unwrap();
        assert_eq!(stopped.state, ProcessState::Stopping);
        assert_eq!(stopped.reason, TransitionReason::TaskFailure);
        assert_eq!(stopped.detail, "test request");

        let report = tasks
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(1))
            .await;
        assert!(report.is_clean(), "{report:?}");
    }
}
