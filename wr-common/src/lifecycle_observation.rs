use std::fmt;

use crate::wruntime::ProcessLifecycleState;

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
}
