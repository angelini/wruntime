//! Public declarative manager authorization policy v1.
//!
//! This module is the sole parser, canonicalizer, digest owner, and rollout
//! prevalidator shared by manager startup and fleet tooling.

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

use anyhow::{bail, ensure, Context, Result};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::identity::{
    ClusterId, JobQueueId, ManagerId, Namespace, NodeId, PeerHttpsUrl, PrincipalKind, PrincipalUri,
};
#[cfg(feature = "tls")]
use crate::tls::LeafEvidence;

pub const AUTHORIZATION_POLICY_VALIDATOR_VERSION: u32 = 1;
pub const MAX_POLICY_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_PRINCIPALS: usize = 4_096;
pub const MAX_ASSIGNMENTS: usize = 16_384;
pub const MAX_ASSIGNMENTS_PER_PRINCIPAL: usize = 16;
pub const MAX_SCOPE_VALUES: usize = 65_536;
pub const MAX_SCOPE_VALUES_PER_ASSIGNMENT: usize = 1_024;
pub const MAX_MANAGERS: usize = 128;
pub const MAX_PROXIES: usize = 4_096;
pub const MAX_NODE_AGENTS: usize = 4_096;
pub const MAX_REVOKED_FINGERPRINTS: usize = 16_384;
pub const MAX_WORKLOAD_PROJECTION_BYTES: usize = 2 * 1024 * 1024;
const DIGEST_DOMAIN: &[u8] = b"wruntime-authorization-policy-v1";

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum PolicyRole {
    Admin,
    View,
    Infra,
    JobView,
    JobAdmin,
    Status,
    Route,
    Secret,
    Schedule,
    Deploy,
}

impl PolicyRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Admin => "admin",
            Self::View => "view",
            Self::Infra => "infra",
            Self::JobView => "job-view",
            Self::JobAdmin => "job-admin",
            Self::Status => "status",
            Self::Route => "route",
            Self::Secret => "secret",
            Self::Schedule => "schedule",
            Self::Deploy => "deploy",
        }
    }
}

impl FromStr for PolicyRole {
    type Err = anyhow::Error;
    fn from_str(value: &str) -> Result<Self> {
        match value {
            "admin" => Ok(Self::Admin),
            "view" => Ok(Self::View),
            "infra" => Ok(Self::Infra),
            "job-view" => Ok(Self::JobView),
            "job-admin" => Ok(Self::JobAdmin),
            "status" => Ok(Self::Status),
            "route" => Ok(Self::Route),
            "secret" => Ok(Self::Secret),
            "schedule" => Ok(Self::Schedule),
            "deploy" => Ok(Self::Deploy),
            _ => bail!("unsupported authorization role '{value}'"),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct AssignmentScope {
    pub namespace_ids: Option<BTreeSet<String>>,
    pub node_ids: Option<BTreeSet<String>>,
    pub job_queue_ids: Option<BTreeSet<String>>,
    pub manager_ids: Option<BTreeSet<String>>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RoleAssignment {
    pub principal: String,
    pub role: PolicyRole,
    pub scope: AssignmentScope,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ManagerEnrollment {
    pub principal: String,
    pub manager_id: String,
    pub endpoint: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct NodeEnrollment {
    pub principal: String,
    pub node_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapHeadroom {
    pub raw_bytes: usize,
    pub canonical_bytes: usize,
    pub principals: usize,
    pub assignments: usize,
    pub scope_values: usize,
    pub managers: usize,
    pub proxies: usize,
    pub node_agents: usize,
    pub revoked_fingerprints: usize,
}

#[derive(Clone, Debug)]
pub struct ValidatedPolicy {
    pub schema_version: u32,
    pub generation: u64,
    pub cluster_id: String,
    pub digest: String,
    pub principals: BTreeMap<String, PrincipalKind>,
    pub assignments: Vec<RoleAssignment>,
    pub manager_enrollments: Vec<ManagerEnrollment>,
    pub proxy_enrollments: Vec<NodeEnrollment>,
    pub node_agent_enrollments: Vec<NodeEnrollment>,
    pub revoked_leaf_fingerprints: BTreeSet<String>,
    pub manager_set_hash: String,
    pub cap_headroom: CapHeadroom,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RolloutTarget {
    pub manager_id: String,
    pub endpoint: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RolloutPrevalidation {
    pub validator_version: u32,
    pub schema_version: u32,
    pub generation: u64,
    pub digest: String,
    pub cluster_id: String,
    pub targets: Vec<RolloutTarget>,
    pub target_set_hash: String,
    pub caller_principal_uri: String,
    pub caller_leaf_fingerprint: String,
    pub caller_can_begin: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPolicy {
    schema_version: u32,
    generation: u64,
    cluster_id: String,
    principals: Vec<RawPrincipal>,
    assignments: Vec<RawAssignment>,
    manager_enrollments: Vec<RawManagerEnrollment>,
    proxy_enrollments: Vec<RawNodeEnrollment>,
    node_agent_enrollments: Vec<RawNodeEnrollment>,
    revoked_leaf_fingerprints: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPrincipal {
    uri: String,
    kind: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawAssignment {
    principal: String,
    role: String,
    scope: RawScope,
}
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RawScope {
    namespace_ids: Option<Vec<String>>,
    node_ids: Option<Vec<String>>,
    job_queue_ids: Option<Vec<String>>,
    manager_ids: Option<Vec<String>>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawManagerEnrollment {
    principal: String,
    manager_id: String,
    endpoint: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawNodeEnrollment {
    principal: String,
    node_id: String,
}

fn fingerprint(value: &str) -> bool {
    value.len() == 71
        && value.starts_with("sha256:")
        && value[7..]
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn checked_set<T>(
    values: Option<Vec<String>>,
    label: &str,
    parse: impl Fn(&str) -> Result<T>,
) -> Result<Option<BTreeSet<String>>> {
    let Some(values) = values else {
        return Ok(None);
    };
    ensure!(!values.is_empty(), "{label} must not be empty when present");
    let mut result = BTreeSet::new();
    for value in values {
        parse(&value).with_context(|| format!("invalid {label} value '{value}'"))?;
        ensure!(result.insert(value), "duplicate {label} value");
    }
    Ok(Some(result))
}

fn allowed_dimensions(kind: PrincipalKind, role: PolicyRole) -> Result<[bool; 4]> {
    use PolicyRole::*;
    match (kind, role) {
        (PrincipalKind::Human, Admin) => Ok([false, false, false, false]),
        (PrincipalKind::Human, View) => Ok([true, true, false, false]),
        (PrincipalKind::Human, Infra) => Ok([false, true, false, true]),
        (PrincipalKind::Human, JobView | JobAdmin) => Ok([false, false, true, false]),
        (PrincipalKind::ServiceAccount, Status) => Ok([true, true, false, false]),
        (PrincipalKind::ServiceAccount, Route | Secret | Schedule) => {
            Ok([true, false, false, false])
        }
        (PrincipalKind::ServiceAccount, Infra) => Ok([false, true, false, false]),
        (PrincipalKind::ServiceAccount, Deploy) => Ok([false, true, false, true]),
        (PrincipalKind::ServiceAccount, JobView | JobAdmin) => Ok([false, false, true, false]),
        _ => bail!(
            "role '{}' is invalid for principal kind {}",
            role.as_str(),
            kind.as_str()
        ),
    }
}

fn push_u32(out: &mut Vec<u8>, value: usize) -> Result<()> {
    let value = u32::try_from(value).context("canonical policy field exceeds u32")?;
    out.extend_from_slice(&value.to_be_bytes());
    Ok(())
}
fn push_str(out: &mut Vec<u8>, value: &str) -> Result<()> {
    push_u32(out, value.len())?;
    out.extend_from_slice(value.as_bytes());
    Ok(())
}
fn push_opt_set(out: &mut Vec<u8>, values: &Option<BTreeSet<String>>) -> Result<()> {
    match values {
        None => out.push(0),
        Some(values) => {
            out.push(1);
            push_u32(out, values.len())?;
            for value in values {
                push_str(out, value)?;
            }
        }
    }
    Ok(())
}
fn hash(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

/// Canonical hash of a sorted complete manager identity/endpoint set.
pub fn manager_set_hash(targets: &[RolloutTarget]) -> Result<String> {
    let mut targets = targets.to_vec();
    targets.sort_by(|left, right| left.manager_id.cmp(&right.manager_id));
    ensure!(
        targets
            .windows(2)
            .all(|pair| pair[0].manager_id != pair[1].manager_id),
        "manager target IDs must be unique"
    );
    let mut encoded = Vec::from(b"wruntime-manager-set-v1".as_slice());
    push_u32(&mut encoded, targets.len())?;
    for target in targets {
        push_str(&mut encoded, &target.manager_id)?;
        push_str(&mut encoded, &target.endpoint)?;
    }
    Ok(hash(&encoded))
}

impl ValidatedPolicy {
    pub fn load(bytes: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() <= MAX_POLICY_BYTES,
            "authorization policy exceeds 4 MiB raw limit"
        );
        let text = std::str::from_utf8(bytes).context("authorization policy must be UTF-8")?;
        let raw: RawPolicy = toml::from_str(text).context("invalid authorization policy TOML")?;
        ensure!(
            raw.schema_version == 1,
            "authorization policy schema_version must be 1"
        );
        ensure!(
            raw.generation != 0,
            "authorization policy generation must be nonzero"
        );
        let cluster = ClusterId::parse(&raw.cluster_id)?;
        ensure!(
            raw.principals.len() <= MAX_PRINCIPALS,
            "authorization policy exceeds principal cap"
        );
        ensure!(
            raw.assignments.len() <= MAX_ASSIGNMENTS,
            "authorization policy exceeds assignment cap"
        );
        ensure!(
            raw.manager_enrollments.len() <= MAX_MANAGERS,
            "authorization policy exceeds manager enrollment cap"
        );
        ensure!(
            raw.proxy_enrollments.len() <= MAX_PROXIES,
            "authorization policy exceeds proxy enrollment cap"
        );
        ensure!(
            raw.node_agent_enrollments.len() <= MAX_NODE_AGENTS,
            "authorization policy exceeds node-agent enrollment cap"
        );
        ensure!(
            raw.revoked_leaf_fingerprints.len() <= MAX_REVOKED_FINGERPRINTS,
            "authorization policy exceeds revocation cap"
        );

        let mut principals = BTreeMap::new();
        for raw_principal in raw.principals {
            let uri = PrincipalUri::parse(&raw_principal.uri)?;
            let kind = PrincipalKind::from_str(&raw_principal.kind)?;
            ensure!(
                uri.cluster_id() == &cluster,
                "principal belongs to the wrong cluster"
            );
            ensure!(uri.kind() == kind, "principal kind does not match URI kind");
            ensure!(
                principals.insert(uri.to_string(), kind).is_none(),
                "principal URIs must be unique"
            );
        }

        let mut assignment_counts = BTreeMap::<String, usize>::new();
        let mut total_scope_values = 0usize;
        let mut assignments = Vec::with_capacity(raw.assignments.len());
        for item in raw.assignments {
            let kind = *principals
                .get(&item.principal)
                .ok_or_else(|| anyhow::anyhow!("assignment principal is not declared"))?;
            ensure!(
                matches!(kind, PrincipalKind::Human | PrincipalKind::ServiceAccount),
                "workload principals cannot have role assignments"
            );
            let role = PolicyRole::from_str(&item.role)?;
            let allowed = allowed_dimensions(kind, role)?;
            let scope = AssignmentScope {
                namespace_ids: checked_set(item.scope.namespace_ids, "namespace_ids", |v| {
                    Namespace::parse(v)
                })?,
                node_ids: checked_set(item.scope.node_ids, "node_ids", |v| NodeId::parse(v))?,
                job_queue_ids: checked_set(item.scope.job_queue_ids, "job_queue_ids", |v| {
                    JobQueueId::parse(v)
                })?,
                manager_ids: checked_set(item.scope.manager_ids, "manager_ids", |v| {
                    ManagerId::parse(v)
                })?,
            };
            let present = [
                scope.namespace_ids.is_some(),
                scope.node_ids.is_some(),
                scope.job_queue_ids.is_some(),
                scope.manager_ids.is_some(),
            ];
            ensure!(
                present
                    .iter()
                    .zip(allowed)
                    .all(|(present, allowed)| !present || allowed),
                "scope dimension is not allowed for this principal kind/role"
            );
            let count = [
                &scope.namespace_ids,
                &scope.node_ids,
                &scope.job_queue_ids,
                &scope.manager_ids,
            ]
            .into_iter()
            .flatten()
            .map(BTreeSet::len)
            .sum::<usize>();
            ensure!(
                count <= MAX_SCOPE_VALUES_PER_ASSIGNMENT,
                "assignment exceeds scope-value cap"
            );
            total_scope_values = total_scope_values
                .checked_add(count)
                .context("scope count overflow")?;
            ensure!(
                total_scope_values <= MAX_SCOPE_VALUES,
                "authorization policy exceeds total scope-value cap"
            );
            let principal_count = assignment_counts.entry(item.principal.clone()).or_default();
            *principal_count += 1;
            ensure!(
                *principal_count <= MAX_ASSIGNMENTS_PER_PRINCIPAL,
                "principal exceeds assignment cap"
            );
            assignments.push(RoleAssignment {
                principal: item.principal,
                role,
                scope,
            });
        }
        assignments.sort();
        ensure!(
            assignments.windows(2).all(|w| w[0] != w[1]),
            "duplicate role assignments are invalid"
        );

        let mut enrolled_principals = BTreeSet::new();
        let mut manager_ids = BTreeSet::new();
        let mut endpoints = BTreeSet::new();
        let mut managers = Vec::new();
        for item in raw.manager_enrollments {
            let uri = PrincipalUri::parse(&item.principal)?;
            ensure!(
                principals.get(uri.as_str()) == Some(&PrincipalKind::Manager),
                "manager enrollment principal kind mismatch"
            );
            ManagerId::parse(&item.manager_id)?;
            ensure!(
                uri.name().as_str() == item.manager_id,
                "manager principal name must equal manager_id"
            );
            PeerHttpsUrl::parse(&item.endpoint)?;
            ensure!(
                enrolled_principals.insert(item.principal.clone()),
                "workload principal may be enrolled only once"
            );
            ensure!(
                manager_ids.insert(item.manager_id.clone()),
                "manager IDs must be unique"
            );
            ensure!(
                endpoints.insert(item.endpoint.clone()),
                "manager endpoints must be unique"
            );
            managers.push(ManagerEnrollment {
                principal: item.principal,
                manager_id: item.manager_id,
                endpoint: item.endpoint,
            });
        }
        managers.sort_by(|a, b| a.manager_id.cmp(&b.manager_id));
        let mut validate_nodes = |raw_items: Vec<RawNodeEnrollment>,
                                  expected: PrincipalKind|
         -> Result<Vec<NodeEnrollment>> {
            let mut items = Vec::new();
            for item in raw_items {
                let uri = PrincipalUri::parse(&item.principal)?;
                ensure!(
                    principals.get(uri.as_str()) == Some(&expected),
                    "workload enrollment principal kind mismatch"
                );
                NodeId::parse(&item.node_id)?;
                ensure!(
                    enrolled_principals.insert(item.principal.clone()),
                    "workload principal may be enrolled only once"
                );
                items.push(NodeEnrollment {
                    principal: item.principal,
                    node_id: item.node_id,
                });
            }
            items.sort();
            Ok(items)
        };
        let proxies = validate_nodes(raw.proxy_enrollments, PrincipalKind::Proxy)?;
        let node_agents = validate_nodes(raw.node_agent_enrollments, PrincipalKind::NodeAgent)?;
        for (uri, kind) in &principals {
            if matches!(
                kind,
                PrincipalKind::Manager | PrincipalKind::Proxy | PrincipalKind::NodeAgent
            ) {
                ensure!(
                    enrolled_principals.contains(uri),
                    "every workload principal must have exactly one enrollment"
                );
            }
        }
        let revoked_count = raw.revoked_leaf_fingerprints.len();
        let revoked = raw
            .revoked_leaf_fingerprints
            .into_iter()
            .map(|value| {
                ensure!(
                    fingerprint(&value),
                    "revocation must be sha256:<64 lowercase hex>"
                );
                Ok(value)
            })
            .collect::<Result<BTreeSet<_>>>()?;
        ensure!(
            revoked.len() == revoked_count,
            "duplicate revoked fingerprints are invalid"
        );

        let mut canonical = Vec::new();
        canonical.extend_from_slice(DIGEST_DOMAIN);
        canonical.extend_from_slice(&raw.schema_version.to_be_bytes());
        canonical.extend_from_slice(&raw.generation.to_be_bytes());
        push_str(&mut canonical, cluster.as_str())?;
        push_u32(&mut canonical, principals.len())?;
        for (uri, kind) in &principals {
            push_str(&mut canonical, uri)?;
            push_str(&mut canonical, kind.as_str())?;
        }
        push_u32(&mut canonical, assignments.len())?;
        for assignment in &assignments {
            push_str(&mut canonical, &assignment.principal)?;
            push_str(&mut canonical, assignment.role.as_str())?;
            push_opt_set(&mut canonical, &assignment.scope.namespace_ids)?;
            push_opt_set(&mut canonical, &assignment.scope.node_ids)?;
            push_opt_set(&mut canonical, &assignment.scope.job_queue_ids)?;
            push_opt_set(&mut canonical, &assignment.scope.manager_ids)?;
        }
        push_u32(&mut canonical, managers.len())?;
        for item in &managers {
            push_str(&mut canonical, &item.principal)?;
            push_str(&mut canonical, &item.manager_id)?;
            push_str(&mut canonical, &item.endpoint)?;
        }
        for items in [&proxies, &node_agents] {
            push_u32(&mut canonical, items.len())?;
            for item in items {
                push_str(&mut canonical, &item.principal)?;
                push_str(&mut canonical, &item.node_id)?;
            }
        }
        push_u32(&mut canonical, revoked.len())?;
        for value in &revoked {
            push_str(&mut canonical, value)?;
        }
        ensure!(
            canonical.len() <= MAX_POLICY_BYTES,
            "authorization policy exceeds 4 MiB canonical limit"
        );

        let rollout_targets = managers
            .iter()
            .map(|item| RolloutTarget {
                manager_id: item.manager_id.clone(),
                endpoint: item.endpoint.clone(),
            })
            .collect::<Vec<_>>();
        let digest = hash(&canonical);
        let principal_headroom = MAX_PRINCIPALS - principals.len();
        let assignment_headroom = MAX_ASSIGNMENTS - assignments.len();
        let proxy_headroom = MAX_PROXIES - proxies.len();
        let node_agent_headroom = MAX_NODE_AGENTS - node_agents.len();
        let validated = Self {
            schema_version: raw.schema_version,
            generation: raw.generation,
            cluster_id: cluster.to_string(),
            digest,
            principals,
            assignments,
            manager_enrollments: managers,
            proxy_enrollments: proxies,
            node_agent_enrollments: node_agents,
            revoked_leaf_fingerprints: revoked,
            manager_set_hash: manager_set_hash(&rollout_targets)?,
            cap_headroom: CapHeadroom {
                raw_bytes: MAX_POLICY_BYTES - bytes.len(),
                canonical_bytes: MAX_POLICY_BYTES - canonical.len(),
                principals: principal_headroom,
                assignments: assignment_headroom,
                scope_values: MAX_SCOPE_VALUES - total_scope_values,
                managers: MAX_MANAGERS - manager_ids.len(),
                proxies: proxy_headroom,
                node_agents: node_agent_headroom,
                revoked_fingerprints: MAX_REVOKED_FINGERPRINTS - revoked_count,
            },
        };
        // Enforce the actual deterministic protobuf projection cap during policy validation.
        crate::snapshot_consumer::build_snapshot(
            &validated,
            crate::wruntime::WorkloadProjectionKind::ProxyPeerV1,
            std::time::UNIX_EPOCH,
        )?;
        crate::snapshot_consumer::build_snapshot(
            &validated,
            crate::wruntime::WorkloadProjectionKind::EngineJobAdminV1,
            std::time::UNIX_EPOCH,
        )?;
        Ok(validated)
    }

    pub fn manager_targets(&self) -> Vec<RolloutTarget> {
        self.manager_enrollments
            .iter()
            .map(|item| RolloutTarget {
                manager_id: item.manager_id.clone(),
                endpoint: item.endpoint.clone(),
            })
            .collect()
    }

    pub fn authorizes_rollout(&self, principal: &str, manager_ids: &[String]) -> bool {
        let Some(kind) = self.principals.get(principal).copied() else {
            return false;
        };
        self.assignments.iter().any(|assignment| {
            assignment.principal == principal
                && matches!(
                    (kind, assignment.role),
                    (PrincipalKind::Human, PolicyRole::Admin | PolicyRole::Infra)
                        | (PrincipalKind::ServiceAccount, PolicyRole::Deploy)
                )
                && assignment
                    .scope
                    .manager_ids
                    .as_ref()
                    .is_none_or(|allowed| manager_ids.iter().all(|id| allowed.contains(id)))
        })
    }

    pub fn prevalidate_rollout_claims(
        &self,
        caller_principal_uri: &str,
        caller_leaf_fingerprint: &str,
        expected_targets: &[RolloutTarget],
    ) -> Result<RolloutPrevalidation> {
        let principal = PrincipalUri::parse(caller_principal_uri)?;
        ensure!(
            principal.cluster_id().as_str() == self.cluster_id,
            "rollout caller belongs to the wrong cluster"
        );
        ensure!(
            matches!(
                principal.kind(),
                PrincipalKind::Human | PrincipalKind::ServiceAccount
            ),
            "rollout caller must be human or service-account"
        );
        ensure!(
            fingerprint(caller_leaf_fingerprint),
            "rollout caller fingerprint must be sha256:<64 lowercase hex>"
        );
        ensure!(
            !self
                .revoked_leaf_fingerprints
                .contains(caller_leaf_fingerprint),
            "rollout caller leaf is revoked"
        );
        let mut targets = expected_targets.to_vec();
        targets.sort_by(|a, b| a.manager_id.cmp(&b.manager_id));
        ensure!(
            targets == self.manager_targets(),
            "rollout targets must exactly equal policy manager enrollments"
        );
        let ids = targets
            .iter()
            .map(|target| target.manager_id.clone())
            .collect::<Vec<_>>();
        let allowed = self.authorizes_rollout(principal.as_str(), &ids);
        ensure!(
            allowed,
            "rollout caller lacks one complete target-set grant"
        );
        Ok(RolloutPrevalidation {
            validator_version: AUTHORIZATION_POLICY_VALIDATOR_VERSION,
            schema_version: self.schema_version,
            generation: self.generation,
            digest: self.digest.clone(),
            cluster_id: self.cluster_id.clone(),
            targets,
            target_set_hash: self.manager_set_hash.clone(),
            caller_principal_uri: principal.to_string(),
            caller_leaf_fingerprint: caller_leaf_fingerprint.to_string(),
            caller_can_begin: allowed,
        })
    }

    #[cfg(feature = "tls")]
    pub fn prevalidate_rollout(
        &self,
        caller: &LeafEvidence,
        expected_targets: &[RolloutTarget],
    ) -> Result<RolloutPrevalidation> {
        let principal = caller
            .principal
            .as_ref()
            .context("rollout caller requires a client principal")?;
        self.prevalidate_rollout_claims(principal.as_str(), &caller.fingerprint, expected_targets)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(order: bool) -> String {
        let managers = if order {
            r#"[[manager_enrollments]]
principal = "urn:wruntime:cluster-a:manager:manager-b"
manager_id = "manager-b"
endpoint = "https://manager-b.example:9000"
[[manager_enrollments]]
principal = "urn:wruntime:cluster-a:manager:manager-a"
manager_id = "manager-a"
endpoint = "https://manager-a.example:9000""#
        } else {
            r#"[[manager_enrollments]]
principal = "urn:wruntime:cluster-a:manager:manager-a"
manager_id = "manager-a"
endpoint = "https://manager-a.example:9000"
[[manager_enrollments]]
principal = "urn:wruntime:cluster-a:manager:manager-b"
manager_id = "manager-b"
endpoint = "https://manager-b.example:9000""#
        };
        format!(
            r#"schema_version=1
generation=42
cluster_id="cluster-a"
revoked_leaf_fingerprints=[]
principals=[{{uri="urn:wruntime:cluster-a:human:deployer",kind="human"}},{{uri="urn:wruntime:cluster-a:manager:manager-a",kind="manager"}},{{uri="urn:wruntime:cluster-a:manager:manager-b",kind="manager"}}]
assignments=[{{principal="urn:wruntime:cluster-a:human:deployer",role="infra",scope={{manager_ids=["manager-a","manager-b"]}}}}]
proxy_enrollments=[]
node_agent_enrollments=[]
{managers}
"#
        )
    }

    #[test]
    fn digest_is_source_order_independent_and_grants_do_not_cross_product() {
        let a = ValidatedPolicy::load(policy(false).as_bytes()).unwrap();
        let b = ValidatedPolicy::load(policy(true).as_bytes()).unwrap();
        assert_eq!(a.digest, b.digest);
        assert!(a.authorizes_rollout(
            "urn:wruntime:cluster-a:human:deployer",
            &["manager-a".into(), "manager-b".into()]
        ));
        assert!(!a.authorizes_rollout("urn:wruntime:cluster-a:human:other", &["manager-a".into()]));
    }

    #[test]
    fn unknown_fields_and_duplicate_semantics_are_rejected() {
        let mut unknown = policy(false);
        unknown.push_str("surprise=true\n");
        assert!(ValidatedPolicy::load(unknown.as_bytes()).is_err());
        let duplicate = policy(false).replace(
            "revoked_leaf_fingerprints=[]",
            &format!(
                "revoked_leaf_fingerprints=[\"sha256:{}\",\"sha256:{}\"]",
                "a".repeat(64),
                "a".repeat(64)
            ),
        );
        assert!(ValidatedPolicy::load(duplicate.as_bytes()).is_err());
    }

    #[test]
    fn raw_byte_cap_accepts_the_maximum_and_rejects_max_plus_one() {
        let mut maximum = policy(false);
        maximum.push('#');
        maximum.extend(std::iter::repeat_n('x', MAX_POLICY_BYTES - maximum.len()));
        assert_eq!(maximum.len(), MAX_POLICY_BYTES);
        ValidatedPolicy::load(maximum.as_bytes()).unwrap();
        maximum.push('x');
        assert!(ValidatedPolicy::load(maximum.as_bytes())
            .unwrap_err()
            .to_string()
            .contains("4 MiB raw"));
    }

    #[test]
    fn per_assignment_scope_cap_accepts_maximum_and_rejects_max_plus_one() {
        fn scoped(count: usize) -> String {
            let values = (0..count)
                .map(|index| format!("\"node-{index:04}\""))
                .collect::<Vec<_>>()
                .join(",");
            policy(false).replace(
                "scope={manager_ids=[\"manager-a\",\"manager-b\"]}",
                &format!("scope={{node_ids=[{values}]}}"),
            )
        }
        ValidatedPolicy::load(scoped(MAX_SCOPE_VALUES_PER_ASSIGNMENT).as_bytes()).unwrap();
        assert!(
            ValidatedPolicy::load(scoped(MAX_SCOPE_VALUES_PER_ASSIGNMENT + 1).as_bytes())
                .unwrap_err()
                .to_string()
                .contains("scope-value cap")
        );
    }

    #[test]
    fn rollout_claim_validation_uses_the_same_target_set_hash_and_whole_grant() {
        let snapshot = ValidatedPolicy::load(policy(false).as_bytes()).unwrap();
        let fingerprint = format!("sha256:{}", "c".repeat(64));
        let mut targets = snapshot.manager_targets();
        targets.reverse();
        let receipt = snapshot
            .prevalidate_rollout_claims(
                "urn:wruntime:cluster-a:human:deployer",
                &fingerprint,
                &targets,
            )
            .unwrap();
        assert_eq!(receipt.target_set_hash, manager_set_hash(&targets).unwrap());
        assert_eq!(receipt.targets, snapshot.manager_targets());
        let incomplete = vec![snapshot.manager_targets()[0].clone()];
        assert!(snapshot
            .prevalidate_rollout_claims(
                "urn:wruntime:cluster-a:human:deployer",
                &fingerprint,
                &incomplete,
            )
            .is_err());
    }
}
