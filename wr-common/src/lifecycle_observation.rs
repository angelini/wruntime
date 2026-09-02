use std::fmt;

use crate::wruntime::{LifecycleStatus, ProcessLifecycleState, ServiceKind};

/// Pure semantic result for one lifecycle status observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LifecycleStateClassification {
    Matched,
    Pending,
    Terminal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LifecycleClassificationError {
    UnspecifiedExpected,
    UnspecifiedObserved,
}

impl fmt::Display for LifecycleClassificationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnspecifiedExpected => {
                write!(f, "cannot wait for an unspecified lifecycle state")
            }
            Self::UnspecifiedObserved => {
                write!(f, "lifecycle endpoint returned an unspecified state")
            }
        }
    }
}

impl std::error::Error for LifecycleClassificationError {}

/// Validated typed fields from a protobuf lifecycle status. The protobuf value
/// remains owned by the caller, so reason, detail, and timestamp stay untouched.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValidatedLifecycleStatus<'a> {
    pub state: ProcessLifecycleState,
    pub service_kind: ServiceKind,
    pub process_instance_id: &'a str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LifecycleStatusValidationError {
    UnknownState(i32),
    UnspecifiedState,
    UnknownServiceKind(i32),
    UnspecifiedServiceKind,
    EmptyProcessInstance,
}

impl fmt::Display for LifecycleStatusValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownState(value) => write!(f, "unknown lifecycle state {value}"),
            Self::UnspecifiedState => write!(f, "lifecycle endpoint returned an unspecified state"),
            Self::UnknownServiceKind(value) => {
                write!(f, "unknown lifecycle service kind {value}")
            }
            Self::UnspecifiedServiceKind => {
                write!(f, "lifecycle endpoint returned an unspecified service kind")
            }
            Self::EmptyProcessInstance => {
                write!(
                    f,
                    "lifecycle endpoint returned an empty process instance ID"
                )
            }
        }
    }
}

impl std::error::Error for LifecycleStatusValidationError {}

/// Validate exactly the existing lifecycle wire contract: known, specified
/// state and service kind plus a nonempty process instance. Transition reason
/// and timestamp are intentionally not strengthened here.
pub fn validate_lifecycle_status(
    status: &LifecycleStatus,
) -> Result<ValidatedLifecycleStatus<'_>, LifecycleStatusValidationError> {
    let state = ProcessLifecycleState::try_from(status.state)
        .map_err(|_| LifecycleStatusValidationError::UnknownState(status.state))?;
    if state == ProcessLifecycleState::Unspecified {
        return Err(LifecycleStatusValidationError::UnspecifiedState);
    }

    let service_kind = ServiceKind::try_from(status.service_kind)
        .map_err(|_| LifecycleStatusValidationError::UnknownServiceKind(status.service_kind))?;
    if service_kind == ServiceKind::Unspecified {
        return Err(LifecycleStatusValidationError::UnspecifiedServiceKind);
    }
    if status.process_instance_id.is_empty() {
        return Err(LifecycleStatusValidationError::EmptyProcessInstance);
    }

    Ok(ValidatedLifecycleStatus {
        state,
        service_kind,
        process_instance_id: &status.process_instance_id,
    })
}

/// Classify an exact lifecycle observation without assigning an ordinal to
/// lifecycle states. READY is reached only by READY; STOPPING before READY is
/// terminal, while STARTING remains pending.
pub fn classify_lifecycle_state(
    observed: ProcessLifecycleState,
    expected: ProcessLifecycleState,
) -> Result<LifecycleStateClassification, LifecycleClassificationError> {
    use LifecycleStateClassification::{Matched, Pending, Terminal};
    use ProcessLifecycleState::{Ready, Starting, Stopping, Unspecified};

    if expected == Unspecified {
        return Err(LifecycleClassificationError::UnspecifiedExpected);
    }
    if observed == Unspecified {
        return Err(LifecycleClassificationError::UnspecifiedObserved);
    }
    if observed == expected {
        return Ok(Matched);
    }

    Ok(match (expected, observed) {
        (Ready, Starting) | (Stopping, Starting | Ready) => Pending,
        (Starting, Ready | Stopping) | (Ready, Stopping) => Terminal,
        _ => unreachable!("all specified lifecycle states are classified"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status(state: i32, kind: i32, instance: &str) -> LifecycleStatus {
        LifecycleStatus {
            state,
            service_kind: kind,
            process_instance_id: instance.to_string(),
            transitioned_at: None,
            reason: 0,
            detail: "raw detail".to_string(),
        }
    }

    #[test]
    fn exact_status_and_ready_only_semantics_cover_the_complete_state_matrix() {
        use LifecycleStateClassification::{Matched, Pending, Terminal};
        use ProcessLifecycleState::{Ready, Starting, Stopping};

        let cases = [
            (Starting, Starting, Matched),
            (Starting, Ready, Terminal),
            (Starting, Stopping, Terminal),
            (Ready, Starting, Pending),
            (Ready, Ready, Matched),
            (Ready, Stopping, Terminal),
            (Stopping, Starting, Pending),
            (Stopping, Ready, Pending),
            (Stopping, Stopping, Matched),
        ];
        for (expected, observed, classification) in cases {
            assert_eq!(
                classify_lifecycle_state(observed, expected),
                Ok(classification),
                "expected={expected:?} observed={observed:?}"
            );
        }
    }

    #[test]
    fn valid_status_returns_typed_fields_without_strengthening_reason_or_timestamp() {
        let raw = status(
            ProcessLifecycleState::Ready as i32,
            ServiceKind::Engine as i32,
            "engine-1",
        );
        let validated = validate_lifecycle_status(&raw).unwrap();
        assert_eq!(validated.state, ProcessLifecycleState::Ready);
        assert_eq!(validated.service_kind, ServiceKind::Engine);
        assert_eq!(validated.process_instance_id, "engine-1");
        assert_eq!(raw.reason, 0);
        assert!(raw.transitioned_at.is_none());
    }

    #[test]
    fn malformed_state_kind_and_instance_are_rejected_exactly() {
        let cases = [
            (
                status(99, ServiceKind::Engine as i32, "engine-1"),
                LifecycleStatusValidationError::UnknownState(99),
            ),
            (
                status(0, ServiceKind::Engine as i32, "engine-1"),
                LifecycleStatusValidationError::UnspecifiedState,
            ),
            (
                status(ProcessLifecycleState::Ready as i32, 99, "engine-1"),
                LifecycleStatusValidationError::UnknownServiceKind(99),
            ),
            (
                status(ProcessLifecycleState::Ready as i32, 0, "engine-1"),
                LifecycleStatusValidationError::UnspecifiedServiceKind,
            ),
            (
                status(
                    ProcessLifecycleState::Ready as i32,
                    ServiceKind::Engine as i32,
                    "",
                ),
                LifecycleStatusValidationError::EmptyProcessInstance,
            ),
        ];
        for (raw, expected) in cases {
            assert_eq!(validate_lifecycle_status(&raw), Err(expected));
        }
    }
}
