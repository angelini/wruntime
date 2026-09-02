use std::fmt;

use anyhow::{Context, Result};
use tokio::process::Child;
use tokio::time::{sleep_until, Instant};
use tonic::transport::{Channel, Endpoint};
use tonic::Code;

use wr_common::lifecycle_observation::{
    classify_lifecycle_state, validate_lifecycle_status, LifecycleStateClassification,
    LifecycleStatusValidationError,
};
use wr_common::wruntime::{
    lifecycle_service_client::LifecycleServiceClient, GetLifecycleStatusRequest, LifecycleStatus,
    ProcessLifecycleState, ServiceKind,
};

use super::wait::DEFAULT_POLL_INTERVAL;

#[derive(Clone, Debug)]
pub enum LifecycleObservation {
    Status(LifecycleStatus),
    Rpc { code: Code, message: String },
}

#[derive(Clone, Debug)]
pub enum LifecycleWaitError {
    Deadline {
        expected: ProcessLifecycleState,
        last_observation: Option<LifecycleObservation>,
    },
    TerminalBeforeReady(LifecycleStatus),
    InvalidState(i32),
    InvalidStatus {
        error: LifecycleStatusValidationError,
        status: LifecycleStatus,
    },
    Rpc {
        code: Code,
        message: String,
    },
    ChildExited(String),
    ServiceKindMismatch {
        expected: ServiceKind,
        observed: i32,
    },
    ProcessInstanceMismatch {
        expected: String,
        observed: String,
    },
}

impl fmt::Display for LifecycleWaitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Deadline {
                expected,
                last_observation,
            } => write!(
                f,
                "lifecycle wait for {expected:?} reached its deadline; last observation: {last_observation:?}"
            ),
            Self::TerminalBeforeReady(status) => write!(
                f,
                "process entered lifecycle state {:?} before READY",
                ProcessLifecycleState::try_from(status.state).ok()
            ),
            Self::InvalidState(state) => write!(f, "invalid lifecycle state value {state}"),
            Self::InvalidStatus { error, status } => {
                write!(f, "invalid lifecycle status ({error}): {status:?}")
            }
            Self::Rpc { code, message } => {
                write!(f, "lifecycle RPC failed with {code}: {message}")
            }
            Self::ChildExited(status) => {
                write!(f, "supervised child exited while waiting for lifecycle state: {status}")
            }
            Self::ServiceKindMismatch { expected, observed } => write!(
                f,
                "lifecycle service kind mismatch: expected {expected:?}, observed {observed}"
            ),
            Self::ProcessInstanceMismatch { expected, observed } => write!(
                f,
                "lifecycle process instance mismatch: expected {expected}, observed {observed}"
            ),
        }
    }
}

impl std::error::Error for LifecycleWaitError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StateEvaluation {
    Reached,
    Pending,
}

pub(crate) fn evaluate_state(
    status: &LifecycleStatus,
    expected: ProcessLifecycleState,
) -> std::result::Result<StateEvaluation, LifecycleWaitError> {
    if expected == ProcessLifecycleState::Unspecified {
        return Err(LifecycleWaitError::InvalidState(expected as i32));
    }
    let observed = validate_lifecycle_status(status)
        .map_err(|error| LifecycleWaitError::InvalidStatus {
            error,
            status: status.clone(),
        })?
        .state;
    match classify_lifecycle_state(observed, expected) {
        Ok(LifecycleStateClassification::Matched) => Ok(StateEvaluation::Reached),
        Ok(LifecycleStateClassification::Pending) => Ok(StateEvaluation::Pending),
        Ok(LifecycleStateClassification::Terminal) => {
            Err(LifecycleWaitError::TerminalBeforeReady(status.clone()))
        }
        Err(_) => Err(LifecycleWaitError::InvalidState(status.state)),
    }
}

fn retryable(code: Code) -> bool {
    matches!(code, Code::Unavailable | Code::DeadlineExceeded)
}

pub async fn lifecycle_client(
    endpoint: impl AsRef<str>,
) -> Result<LifecycleServiceClient<Channel>> {
    let endpoint = Endpoint::from_shared(endpoint.as_ref().to_owned())
        .context("invalid lifecycle endpoint")?;
    Ok(LifecycleServiceClient::new(endpoint.connect_lazy()))
}

pub async fn wait_for_state(
    client: &mut LifecycleServiceClient<Channel>,
    expected: ProcessLifecycleState,
    deadline: Instant,
) -> std::result::Result<LifecycleStatus, LifecycleWaitError> {
    wait_for_state_inner(client, expected, deadline, None, None).await
}

pub async fn wait_for_ready_with_child(
    client: &mut LifecycleServiceClient<Channel>,
    child: &mut Child,
    expected_kind: ServiceKind,
    expected_instance: &str,
    deadline: Instant,
) -> std::result::Result<LifecycleStatus, LifecycleWaitError> {
    wait_for_state_inner(
        client,
        ProcessLifecycleState::Ready,
        deadline,
        Some(child),
        Some((expected_kind, expected_instance)),
    )
    .await
}

fn ensure_child_running(child: &mut Child) -> std::result::Result<(), LifecycleWaitError> {
    match child.try_wait() {
        Ok(None) => Ok(()),
        Ok(Some(status)) => Err(LifecycleWaitError::ChildExited(status.to_string())),
        Err(error) => Err(LifecycleWaitError::ChildExited(error.to_string())),
    }
}

async fn wait_for_state_inner(
    client: &mut LifecycleServiceClient<Channel>,
    expected: ProcessLifecycleState,
    deadline: Instant,
    mut child: Option<&mut Child>,
    expected_identity: Option<(ServiceKind, &str)>,
) -> std::result::Result<LifecycleStatus, LifecycleWaitError> {
    let mut last_observation = None;
    loop {
        if let Some(child) = child.as_deref_mut() {
            ensure_child_running(child)?;
        }

        let response = if let Some(child) = child.as_deref_mut() {
            tokio::select! {
                response = client.get_status(GetLifecycleStatusRequest {}) => response,
                exit = child.wait() => {
                    return Err(LifecycleWaitError::ChildExited(
                        exit.map(|status| status.to_string()).unwrap_or_else(|error| error.to_string()),
                    ));
                }
                _ = sleep_until(deadline) => {
                    return Err(LifecycleWaitError::Deadline { expected, last_observation });
                }
            }
        } else {
            tokio::select! {
                response = client.get_status(GetLifecycleStatusRequest {}) => response,
                _ = sleep_until(deadline) => {
                    return Err(LifecycleWaitError::Deadline { expected, last_observation });
                }
            }
        };

        if let Some(child) = child.as_deref_mut() {
            ensure_child_running(child)?;
        }

        match response {
            Ok(response) => {
                let Some(status) = response.into_inner().status else {
                    return Err(LifecycleWaitError::Rpc {
                        code: Code::Internal,
                        message: "lifecycle response omitted status".to_owned(),
                    });
                };
                let validated = validate_lifecycle_status(&status).map_err(|error| {
                    LifecycleWaitError::InvalidStatus {
                        error,
                        status: status.clone(),
                    }
                })?;
                if let Some((expected_kind, expected_instance)) = expected_identity {
                    if validated.service_kind != expected_kind {
                        return Err(LifecycleWaitError::ServiceKindMismatch {
                            expected: expected_kind,
                            observed: status.service_kind,
                        });
                    }
                    if status.process_instance_id != expected_instance {
                        return Err(LifecycleWaitError::ProcessInstanceMismatch {
                            expected: expected_instance.to_owned(),
                            observed: status.process_instance_id,
                        });
                    }
                }
                match evaluate_state(&status, expected)? {
                    StateEvaluation::Reached => return Ok(status),
                    StateEvaluation::Pending => {
                        last_observation = Some(LifecycleObservation::Status(status));
                    }
                }
            }
            Err(status) if retryable(status.code()) => {
                last_observation = Some(LifecycleObservation::Rpc {
                    code: status.code(),
                    message: status.message().to_owned(),
                });
            }
            Err(status) => {
                return Err(LifecycleWaitError::Rpc {
                    code: status.code(),
                    message: status.message().to_owned(),
                });
            }
        }

        let next_poll = (Instant::now() + DEFAULT_POLL_INTERVAL).min(deadline);
        if let Some(child) = child.as_deref_mut() {
            tokio::select! {
                _ = sleep_until(next_poll) => {}
                exit = child.wait() => {
                    return Err(LifecycleWaitError::ChildExited(
                        exit.map(|status| status.to_string()).unwrap_or_else(|error| error.to_string()),
                    ));
                }
            }
        } else {
            sleep_until(next_poll).await;
        }

        if Instant::now() >= deadline {
            return Err(LifecycleWaitError::Deadline {
                expected,
                last_observation,
            });
        }
    }
}
