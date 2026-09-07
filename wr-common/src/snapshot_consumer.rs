//! Shared complete workload snapshot producer, decoder, and fail-closed consumer.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{ensure, Context, Result};
use prost::Message;
use prost_types::Timestamp;
use sha2::{Digest, Sha256};

use crate::authorization_policy::{
    ValidatedPolicy, MAX_MANAGERS, MAX_PROXIES, MAX_REVOKED_FINGERPRINTS,
    MAX_WORKLOAD_PROJECTION_BYTES,
};
use crate::identity::{ClusterId, ManagerId, NodeId, PrincipalKind, PrincipalUri};
use crate::wruntime::{WorkloadEnrollmentV1, WorkloadProjectionKind, WorkloadSnapshotV1};

const DOMAIN: &[u8] = b"wruntime-workload-snapshot-v1";
pub const SNAPSHOT_VALIDITY: Duration = Duration::from_secs(30);
pub const MAX_FUTURE_SKEW: Duration = Duration::from_secs(5);

fn push_u32(out: &mut Vec<u8>, value: usize) -> Result<()> {
    out.extend_from_slice(&u32::try_from(value)?.to_be_bytes());
    Ok(())
}
fn push_str(out: &mut Vec<u8>, value: &str) -> Result<()> {
    push_u32(out, value.len())?;
    out.extend_from_slice(value.as_bytes());
    Ok(())
}
fn valid_digest(value: &str) -> bool {
    value.len() == 71
        && value.starts_with("sha256:")
        && value[7..]
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}
fn system_duration(value: SystemTime) -> Result<Duration> {
    value
        .duration_since(UNIX_EPOCH)
        .context("wall clock precedes Unix epoch")
}
fn timestamp(value: SystemTime) -> Result<Timestamp> {
    let d = system_duration(value)?;
    Ok(Timestamp {
        seconds: i64::try_from(d.as_secs())?,
        nanos: d.subsec_nanos() as i32,
    })
}
fn timestamp_duration(value: &Timestamp) -> Result<Duration> {
    ensure!(
        (0..1_000_000_000).contains(&value.nanos) && value.seconds >= 0,
        "malformed snapshot timestamp"
    );
    Ok(Duration::new(value.seconds as u64, value.nanos as u32))
}

fn projection_digest(snapshot: &WorkloadSnapshotV1) -> Result<String> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(DOMAIN);
    bytes.extend_from_slice(&snapshot.schema_version.to_be_bytes());
    bytes.extend_from_slice(&snapshot.projection_kind.to_be_bytes());
    push_str(&mut bytes, &snapshot.cluster_id)?;
    bytes.extend_from_slice(&snapshot.policy_generation.to_be_bytes());
    push_str(&mut bytes, &snapshot.policy_digest)?;
    push_u32(&mut bytes, snapshot.enrollments.len())?;
    for item in &snapshot.enrollments {
        push_str(&mut bytes, &item.principal_uri)?;
        push_str(&mut bytes, &item.workload_id)?;
    }
    push_u32(&mut bytes, snapshot.revoked_leaf_fingerprints.len())?;
    for item in &snapshot.revoked_leaf_fingerprints {
        push_str(&mut bytes, item)?;
    }
    Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
}

pub fn build_snapshot(
    policy: &ValidatedPolicy,
    kind: WorkloadProjectionKind,
    now: SystemTime,
) -> Result<Vec<u8>> {
    ensure!(
        matches!(
            kind,
            WorkloadProjectionKind::ProxyPeerV1 | WorkloadProjectionKind::EngineJobAdminV1
        ),
        "unsupported workload projection kind"
    );
    let mut enrollments = match kind {
        WorkloadProjectionKind::ProxyPeerV1 => policy
            .proxy_enrollments
            .iter()
            .map(|e| WorkloadEnrollmentV1 {
                principal_uri: e.principal.clone(),
                workload_id: e.node_id.clone(),
            })
            .collect::<Vec<_>>(),
        WorkloadProjectionKind::EngineJobAdminV1 => policy
            .manager_enrollments
            .iter()
            .map(|e| WorkloadEnrollmentV1 {
                principal_uri: e.principal.clone(),
                workload_id: e.manager_id.clone(),
            })
            .collect(),
        _ => unreachable!(),
    };
    enrollments.sort_by(|a, b| {
        (&a.principal_uri, &a.workload_id).cmp(&(&b.principal_uri, &b.workload_id))
    });
    let revoked = policy
        .revoked_leaf_fingerprints
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    let mut snapshot = WorkloadSnapshotV1 {
        schema_version: 1,
        projection_kind: kind as i32,
        cluster_id: policy.cluster_id.clone(),
        policy_generation: policy.generation,
        policy_digest: policy.digest.clone(),
        projection_digest: String::new(),
        issued_at: Some(timestamp(now)?),
        valid_until: Some(timestamp(now + SNAPSHOT_VALIDITY)?),
        enrollment_count: enrollments.len() as u32,
        revoked_fingerprint_count: revoked.len() as u32,
        enrollments,
        revoked_leaf_fingerprints: revoked,
    };
    snapshot.projection_digest = projection_digest(&snapshot)?;
    let bytes = snapshot.encode_to_vec();
    ensure!(
        bytes.len() <= MAX_WORKLOAD_PROJECTION_BYTES,
        "workload snapshot exceeds 2 MiB"
    );
    Ok(bytes)
}

#[derive(Clone, Debug)]
pub struct IndexedSnapshot {
    pub wire: WorkloadSnapshotV1,
    pub enrollments: BTreeMap<String, String>,
    pub revocations: BTreeSet<String>,
}
impl IndexedSnapshot {
    pub fn identity(&self) -> SnapshotIdentity {
        SnapshotIdentity {
            generation: self.wire.policy_generation,
            policy_digest: self.wire.policy_digest.clone(),
            projection_digest: self.wire.projection_digest.clone(),
        }
    }
    pub fn admits(&self, principal: &str, fingerprint: &str) -> bool {
        self.enrollments.contains_key(principal) && !self.revocations.contains(fingerprint)
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotIdentity {
    pub generation: u64,
    pub policy_digest: String,
    pub projection_digest: String,
}

pub fn decode_snapshot(
    raw: &[u8],
    expected_cluster: &str,
    expected_kind: WorkloadProjectionKind,
    receipt_wall: SystemTime,
) -> Result<IndexedSnapshot> {
    ensure!(
        raw.len() <= MAX_WORKLOAD_PROJECTION_BYTES,
        "workload snapshot exceeds 2 MiB"
    );
    let snapshot = WorkloadSnapshotV1::decode(raw).context("malformed workload snapshot")?;
    ensure!(
        snapshot.encode_to_vec() == raw,
        "workload snapshot encoding is noncanonical"
    );
    ensure!(
        snapshot.schema_version == 1 && snapshot.policy_generation > 0,
        "invalid workload snapshot version/generation"
    );
    ensure!(
        snapshot.projection_kind == expected_kind as i32 && snapshot.cluster_id == expected_cluster,
        "wrong workload snapshot cluster/projection"
    );
    ClusterId::parse(&snapshot.cluster_id)?;
    ensure!(
        valid_digest(&snapshot.policy_digest) && valid_digest(&snapshot.projection_digest),
        "invalid workload snapshot digest"
    );
    ensure!(
        snapshot.enrollment_count as usize == snapshot.enrollments.len()
            && snapshot.revoked_fingerprint_count as usize
                == snapshot.revoked_leaf_fingerprints.len(),
        "workload snapshot cardinality mismatch"
    );
    let cap = match expected_kind {
        WorkloadProjectionKind::ProxyPeerV1 => MAX_PROXIES,
        WorkloadProjectionKind::EngineJobAdminV1 => MAX_MANAGERS,
        _ => 0,
    };
    ensure!(
        snapshot.enrollments.len() <= cap
            && snapshot.revoked_leaf_fingerprints.len() <= MAX_REVOKED_FINGERPRINTS,
        "workload snapshot cardinality cap exceeded"
    );
    let expected_kind_identity = match expected_kind {
        WorkloadProjectionKind::ProxyPeerV1 => PrincipalKind::Proxy,
        WorkloadProjectionKind::EngineJobAdminV1 => PrincipalKind::Manager,
        _ => unreachable!(),
    };
    let mut previous = None;
    let mut enrollments = BTreeMap::new();
    for item in &snapshot.enrollments {
        let principal = PrincipalUri::parse(&item.principal_uri)?;
        ensure!(
            principal.cluster_id().as_str() == expected_cluster
                && principal.kind() == expected_kind_identity,
            "workload enrollment principal mismatch"
        );
        match expected_kind {
            WorkloadProjectionKind::ProxyPeerV1 => {
                NodeId::parse(&item.workload_id)?;
            }
            WorkloadProjectionKind::EngineJobAdminV1 => {
                ManagerId::parse(&item.workload_id)?;
            }
            _ => {}
        }
        let key = (&item.principal_uri, &item.workload_id);
        ensure!(
            previous.is_none_or(|p| p < key),
            "workload enrollments must be strictly sorted and unique"
        );
        previous = Some(key);
        enrollments.insert(item.principal_uri.clone(), item.workload_id.clone());
    }
    let mut revocations = BTreeSet::new();
    let mut prior = "";
    for value in &snapshot.revoked_leaf_fingerprints {
        ensure!(
            valid_digest(value) && prior < value.as_str() && revocations.insert(value.clone()),
            "revocations must be valid, sorted, and unique"
        );
        prior = value;
    }
    ensure!(
        projection_digest(&snapshot)? == snapshot.projection_digest,
        "workload projection digest mismatch"
    );
    let issued = timestamp_duration(
        snapshot
            .issued_at
            .as_ref()
            .context("issued_at is required")?,
    )?;
    let until = timestamp_duration(
        snapshot
            .valid_until
            .as_ref()
            .context("valid_until is required")?,
    )?;
    ensure!(
        until.checked_sub(issued) == Some(SNAPSHOT_VALIDITY),
        "snapshot validity must be exactly 30 seconds"
    );
    let receipt = system_duration(receipt_wall)?;
    ensure!(
        issued <= receipt + MAX_FUTURE_SKEW && receipt < until,
        "snapshot freshness envelope is invalid"
    );
    Ok(IndexedSnapshot {
        wire: snapshot,
        enrollments,
        revocations,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryPredicate {
    AnyCompleteValid,
    MatchingRetainedOrHigher,
    StrictlyHigherGeneration,
}
#[derive(Clone, Debug, Default)]
pub enum SnapshotConsumerState {
    #[default]
    Empty,
    Fresh {
        snapshot: IndexedSnapshot,
        accepted_at: Instant,
        fresh_until: Instant,
    },
    Stale {
        retained: IndexedSnapshot,
    },
    Blocked {
        retained: Option<IndexedSnapshot>,
        reason: String,
        recovery: RecoveryPredicate,
    },
}
impl SnapshotConsumerState {
    pub fn is_fresh(&self, now: Instant) -> bool {
        matches!(self,Self::Fresh{fresh_until,..} if now<*fresh_until)
    }
    pub fn expire(&mut self, now: Instant) {
        if let Self::Fresh {
            snapshot,
            fresh_until,
            ..
        } = self
        {
            if now >= *fresh_until {
                *self = Self::Stale {
                    retained: snapshot.clone(),
                };
            }
        }
    }
    pub fn retained(&self) -> Option<&IndexedSnapshot> {
        match self {
            Self::Fresh { snapshot, .. } => Some(snapshot),
            Self::Stale { retained }
            | Self::Blocked {
                retained: Some(retained),
                ..
            } => Some(retained),
            _ => None,
        }
    }
}

pub fn consume(
    state: &mut SnapshotConsumerState,
    raw: Result<&[u8], String>,
    expected_cluster: &str,
    expected_kind: WorkloadProjectionKind,
    wall: SystemTime,
    now: Instant,
    fence_matches: bool,
) {
    state.expire(now);
    let retained = state.retained().cloned();
    let recovery = match state {
        SnapshotConsumerState::Blocked { recovery, .. } => Some(*recovery),
        _ => None,
    };
    let decoded = raw.and_then(|bytes| {
        if !fence_matches {
            return Err("ownership fence mismatch".into());
        }
        decode_snapshot(bytes, expected_cluster, expected_kind, wall).map_err(|e| e.to_string())
    });
    let candidate = match decoded {
        Ok(v) => v,
        Err(reason) => {
            let has_retained = retained.is_some();
            let recovery = recovery.unwrap_or(if has_retained {
                RecoveryPredicate::MatchingRetainedOrHigher
            } else {
                RecoveryPredicate::AnyCompleteValid
            });
            *state = SnapshotConsumerState::Blocked {
                retained,
                recovery,
                reason,
            };
            return;
        }
    };
    let cid = candidate.identity();
    if let Some(old) = retained.as_ref() {
        let oid = old.identity();
        if cid.generation < oid.generation {
            *state = SnapshotConsumerState::Blocked {
                retained,
                recovery: RecoveryPredicate::MatchingRetainedOrHigher,
                reason: "policy generation regressed".into(),
            };
            return;
        }
        if cid.generation == oid.generation && cid != oid {
            *state = SnapshotConsumerState::Blocked {
                retained,
                recovery: RecoveryPredicate::StrictlyHigherGeneration,
                reason: "same-generation snapshot digest conflict".into(),
            };
            return;
        }
        if recovery == Some(RecoveryPredicate::StrictlyHigherGeneration)
            && cid.generation <= oid.generation
        {
            return;
        }
        if recovery == Some(RecoveryPredicate::MatchingRetainedOrHigher)
            && cid.generation == oid.generation
            && cid != oid
        {
            return;
        }
    }
    let valid_until =
        timestamp_duration(candidate.wire.valid_until.as_ref().expect("decoded")).expect("decoded");
    let receipt = system_duration(wall).expect("validated wall");
    let budget = SNAPSHOT_VALIDITY.min(valid_until.saturating_sub(receipt));
    *state = SnapshotConsumerState::Fresh {
        snapshot: candidate,
        accepted_at: now,
        fresh_until: now + budget,
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    fn policy() -> ValidatedPolicy {
        ValidatedPolicy::load(include_bytes!(
            "../../examples/config/policy/authorization.toml"
        ))
        .unwrap()
    }
    #[test]
    fn typed_projection_round_trip_and_canonical_rejection() {
        let wall = UNIX_EPOCH + Duration::from_secs(1_000);
        let bytes = build_snapshot(&policy(), WorkloadProjectionKind::ProxyPeerV1, wall).unwrap();
        let decoded =
            decode_snapshot(&bytes, "default", WorkloadProjectionKind::ProxyPeerV1, wall).unwrap();
        assert_eq!(decoded.wire.enrollment_count, 1);
        assert!(decoded.admits(
            "urn:wruntime:default:proxy:proxy-a",
            &format!("sha256:{}", "f".repeat(64))
        ));
        let mut trailing = bytes;
        trailing.push(0);
        assert!(decode_snapshot(
            &trailing,
            "default",
            WorkloadProjectionKind::ProxyPeerV1,
            wall
        )
        .is_err());
    }
    #[test]
    fn exact_expiry_and_recovery_table() {
        let wall = UNIX_EPOCH + Duration::from_secs(2_000);
        let now = Instant::now();
        let bytes = build_snapshot(&policy(), WorkloadProjectionKind::ProxyPeerV1, wall).unwrap();
        let mut state = SnapshotConsumerState::Empty;
        consume(
            &mut state,
            Ok(&bytes),
            "default",
            WorkloadProjectionKind::ProxyPeerV1,
            wall,
            now,
            true,
        );
        assert!(state.is_fresh(now + Duration::from_secs(29)));
        state.expire(now + Duration::from_secs(30));
        assert!(matches!(state, SnapshotConsumerState::Stale { .. }));
        consume(
            &mut state,
            Ok(&bytes),
            "default",
            WorkloadProjectionKind::ProxyPeerV1,
            wall,
            now + Duration::from_secs(1),
            true,
        );
        assert!(state.is_fresh(now + Duration::from_secs(2)));
        consume(
            &mut state,
            Err("partial".into()),
            "default",
            WorkloadProjectionKind::ProxyPeerV1,
            wall,
            now + Duration::from_secs(2),
            true,
        );
        assert!(matches!(
            state,
            SnapshotConsumerState::Blocked {
                recovery: RecoveryPredicate::MatchingRetainedOrHigher,
                ..
            }
        ));
        consume(
            &mut state,
            Ok(&bytes),
            "default",
            WorkloadProjectionKind::ProxyPeerV1,
            wall,
            now + Duration::from_secs(3),
            true,
        );
        assert!(state.is_fresh(now + Duration::from_secs(4)));
    }
    #[test]
    fn same_generation_conflict_requires_higher_and_fence_mismatch_blocks() {
        let wall = UNIX_EPOCH + Duration::from_secs(3_000);
        let now = Instant::now();
        let bytes =
            build_snapshot(&policy(), WorkloadProjectionKind::EngineJobAdminV1, wall).unwrap();
        let mut state = SnapshotConsumerState::Empty;
        consume(
            &mut state,
            Ok(&bytes),
            "default",
            WorkloadProjectionKind::EngineJobAdminV1,
            wall,
            now,
            false,
        );
        assert!(matches!(
            state,
            SnapshotConsumerState::Blocked {
                recovery: RecoveryPredicate::AnyCompleteValid,
                ..
            }
        ));
        consume(
            &mut state,
            Ok(&bytes),
            "default",
            WorkloadProjectionKind::EngineJobAdminV1,
            wall,
            now,
            true,
        );
        let mut conflict = WorkloadSnapshotV1::decode(bytes.as_slice()).unwrap();
        conflict.policy_digest = format!("sha256:{}", "9".repeat(64));
        conflict.projection_digest = projection_digest(&conflict).unwrap();
        let conflict = conflict.encode_to_vec();
        consume(
            &mut state,
            Ok(&conflict),
            "default",
            WorkloadProjectionKind::EngineJobAdminV1,
            wall,
            now + Duration::from_secs(1),
            true,
        );
        assert!(matches!(
            state,
            SnapshotConsumerState::Blocked {
                recovery: RecoveryPredicate::StrictlyHigherGeneration,
                ..
            }
        ));
        consume(
            &mut state,
            Ok(&bytes),
            "default",
            WorkloadProjectionKind::EngineJobAdminV1,
            wall,
            now + Duration::from_secs(2),
            true,
        );
        assert!(matches!(
            state,
            SnapshotConsumerState::Blocked {
                recovery: RecoveryPredicate::StrictlyHigherGeneration,
                ..
            }
        ));
    }
}
