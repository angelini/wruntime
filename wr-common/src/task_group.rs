use std::collections::HashMap;
use std::future::Future;

use anyhow::Result;
use tokio::sync::watch;
use tokio::task::{Id, JoinError, JoinSet};
use tokio::time::{timeout_at, Instant};

#[derive(Clone)]
pub struct TaskCancellation {
    cancelled: watch::Receiver<bool>,
}

impl TaskCancellation {
    pub fn is_cancelled(&self) -> bool {
        *self.cancelled.borrow()
    }

    pub async fn cancelled(&mut self) {
        if self.is_cancelled() {
            return;
        }
        while self.cancelled.changed().await.is_ok() {
            if self.is_cancelled() {
                return;
            }
        }
    }
}

/// A long-lived task must identify whether its exit followed cancellation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskExit {
    Cancelled,
    Completed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskOutcomeKind {
    Cancelled,
    PrematureCompletion,
    Failed(String),
    Panicked(String),
    Aborted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskOutcome {
    pub name: String,
    pub kind: TaskOutcomeKind,
}

#[derive(Debug, Default, Eq, PartialEq)]
pub struct TaskShutdownReport {
    pub outcomes: Vec<TaskOutcome>,
    pub deadline_exceeded: bool,
    pub unfinished_at_deadline: Vec<String>,
}

impl TaskShutdownReport {
    pub fn is_clean(&self) -> bool {
        !self.deadline_exceeded
            && self
                .outcomes
                .iter()
                .all(|outcome| outcome.kind == TaskOutcomeKind::Cancelled)
    }

    pub fn failures(&self) -> impl Iterator<Item = &TaskOutcome> {
        self.outcomes
            .iter()
            .filter(|outcome| outcome.kind != TaskOutcomeKind::Cancelled)
    }
}

struct TaskCompletion {
    exit: Result<TaskExit>,
}

/// Owns named long-lived Tokio tasks and guarantees shutdown joins every task.
pub struct TaskGroup {
    cancellation: watch::Sender<bool>,
    tasks: JoinSet<TaskCompletion>,
    names: HashMap<Id, String>,
    observed: Vec<TaskOutcome>,
}

impl Default for TaskGroup {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskGroup {
    pub fn new() -> Self {
        let (cancellation, _) = watch::channel(false);
        Self {
            cancellation,
            tasks: JoinSet::new(),
            names: HashMap::new(),
            observed: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    pub fn spawn<F, Fut>(&mut self, name: impl Into<String>, task: F)
    where
        F: FnOnce(TaskCancellation) -> Fut + Send + 'static,
        Fut: Future<Output = Result<TaskExit>> + Send + 'static,
    {
        let cancellation = TaskCancellation {
            cancelled: self.cancellation.subscribe(),
        };
        let abort_handle = self.tasks.spawn(async move {
            TaskCompletion {
                exit: task(cancellation).await,
            }
        });
        self.names.insert(abort_handle.id(), name.into());
    }

    pub fn cancellation(&self) -> TaskCancellation {
        TaskCancellation {
            cancelled: self.cancellation.subscribe(),
        }
    }

    /// Wait for one task to exit without cancelling the group. The outcome is
    /// retained for the eventual shutdown report so a service run loop can
    /// react immediately to premature completion, failure, or panic.
    pub async fn next_completion(&mut self) -> Option<TaskOutcome> {
        let joined = self.tasks.join_next_with_id().await?;
        let outcome = self.task_outcome(joined);
        self.observed.push(outcome.clone());
        Some(outcome)
    }

    /// Cancel producers, join until `deadline`, then abort and join every task
    /// still owned by the group.
    pub async fn shutdown(&mut self, deadline: Instant) -> TaskShutdownReport {
        self.cancellation.send_replace(true);
        let mut report = TaskShutdownReport {
            outcomes: std::mem::take(&mut self.observed),
            ..TaskShutdownReport::default()
        };

        while !self.tasks.is_empty() {
            match timeout_at(deadline, self.tasks.join_next_with_id()).await {
                Ok(Some(joined)) => report.outcomes.push(self.task_outcome(joined)),
                Ok(None) => break,
                Err(_) => {
                    report.deadline_exceeded = true;
                    report.unfinished_at_deadline = self.names.values().cloned().collect();
                    report.unfinished_at_deadline.sort();
                    break;
                }
            }
        }

        if !self.tasks.is_empty() {
            self.tasks.abort_all();
            while let Some(joined) = self.tasks.join_next_with_id().await {
                report.outcomes.push(self.task_outcome(joined));
            }
        }

        report
            .outcomes
            .sort_by(|left, right| left.name.cmp(&right.name));
        report
    }

    fn task_outcome(
        &mut self,
        joined: std::result::Result<(Id, TaskCompletion), JoinError>,
    ) -> TaskOutcome {
        match joined {
            Ok((id, completion)) => {
                let name = self.task_name(id);
                let kind = match completion.exit {
                    Ok(TaskExit::Cancelled) => TaskOutcomeKind::Cancelled,
                    Ok(TaskExit::Completed) => TaskOutcomeKind::PrematureCompletion,
                    Err(error) => TaskOutcomeKind::Failed(format!("{error:#}")),
                };
                TaskOutcome { name, kind }
            }
            Err(error) => {
                let name = self.task_name(error.id());
                let kind = if error.is_cancelled() {
                    TaskOutcomeKind::Aborted
                } else {
                    TaskOutcomeKind::Panicked(error.to_string())
                };
                TaskOutcome { name, kind }
            }
        }
    }

    fn task_name(&mut self, id: Id) -> String {
        self.names
            .remove(&id)
            .unwrap_or_else(|| format!("unknown-task-{id}"))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn cancellation_is_a_clean_join() {
        let mut group = TaskGroup::new();
        group.spawn("worker", |mut cancellation| async move {
            cancellation.cancelled().await;
            Ok(TaskExit::Cancelled)
        });

        let report = group
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await;
        assert!(report.is_clean());
        assert!(group.is_empty());
    }

    #[tokio::test]
    async fn premature_completion_and_panic_are_retained() {
        let mut group = TaskGroup::new();
        group.spawn("completed", |_| async { Ok(TaskExit::Completed) });
        group.spawn("panicked", |_| async {
            panic!("task panic");
            #[allow(unreachable_code)]
            Ok(TaskExit::Completed)
        });
        tokio::task::yield_now().await;

        let report = group
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await;
        assert!(report.outcomes.iter().any(|outcome| {
            outcome.name == "completed" && outcome.kind == TaskOutcomeKind::PrematureCompletion
        }));
        assert!(report.outcomes.iter().any(|outcome| {
            outcome.name == "panicked" && matches!(outcome.kind, TaskOutcomeKind::Panicked(_))
        }));
    }

    #[tokio::test]
    async fn live_task_failure_is_observable_and_retained_for_shutdown() {
        let mut group = TaskGroup::new();
        group.spawn("failed-live", |_| async {
            anyhow::bail!("listener stopped")
        });

        let Some(observed) = group.next_completion().await else {
            panic!("failed task was not observed");
        };
        assert!(matches!(
            &observed.kind,
            TaskOutcomeKind::Failed(message) if message.contains("listener stopped")
        ));

        let report = group
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await;
        assert_eq!(report.outcomes, [observed]);
        assert!(group.is_empty());
    }

    #[tokio::test]
    async fn task_error_is_retained() {
        let mut group = TaskGroup::new();
        group.spawn("failed", |_| async { anyhow::bail!("background failure") });
        tokio::task::yield_now().await;

        let report = group
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await;
        assert!(matches!(
            &report.outcomes[0].kind,
            TaskOutcomeKind::Failed(message) if message.contains("background failure")
        ));
    }

    #[tokio::test]
    async fn deadline_aborts_and_joins_remaining_tasks() {
        struct Dropped(Arc<AtomicBool>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let dropped = Arc::new(AtomicBool::new(false));
        let task_dropped = Arc::clone(&dropped);
        let mut group = TaskGroup::new();
        group.spawn("stuck", |_| async move {
            let _guard = Dropped(task_dropped);
            std::future::pending::<()>().await;
            Ok(TaskExit::Completed)
        });
        tokio::task::yield_now().await;

        let report = group.shutdown(Instant::now()).await;
        assert!(report.deadline_exceeded);
        assert_eq!(report.unfinished_at_deadline, ["stuck"]);
        assert_eq!(report.outcomes[0].kind, TaskOutcomeKind::Aborted);
        assert!(group.is_empty());
        assert!(dropped.load(Ordering::SeqCst));
    }
}
