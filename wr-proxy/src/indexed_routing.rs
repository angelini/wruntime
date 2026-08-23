use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use http::HeaderValue;
use tracing::warn;
use wr_common::identity::{ModuleVersion, RouteKey};
use wr_common::wruntime::{RoutingRule, RoutingTable};

use crate::circuit_breaker::{CircuitBreakerRegistry, EngineBreaker};
use crate::layers::Destination;

type ModulesByName = HashMap<Arc<str>, RouteGroup>;

pub struct IndexedCandidate {
    pub destination: Destination,
    pub breaker: EngineBreaker,
    pub version_group_index: usize,
}

pub struct VersionGroup {
    pub parsed_version: ModuleVersion,
    pub version: Arc<str>,
    pub version_header: HeaderValue,
    pub candidate_indexes: Vec<usize>,
    pub counter: AtomicUsize,
}

pub struct RouteGroup {
    pub namespace: Arc<str>,
    pub module: Arc<str>,
    pub namespace_header: HeaderValue,
    pub module_header: HeaderValue,
    pub candidates: Vec<IndexedCandidate>,
    /// Unique exact versions in descending semantic-version order.
    pub version_groups: Vec<VersionGroup>,
    pub all_versions_counter: AtomicUsize,
}

pub struct IndexedRoutingTable {
    by_namespace: HashMap<Arc<str>, ModulesByName>,
    active_forward_addrs: HashSet<Arc<str>>,
    #[cfg(any(test, feature = "test-util"))]
    lookup_calls: AtomicUsize,
    pub version: u64,
}

struct PreparedCandidate {
    parsed_version: ModuleVersion,
    version: String,
    destination: Destination,
    breaker: EngineBreaker,
}

impl IndexedRoutingTable {
    pub fn empty() -> Self {
        Self {
            by_namespace: HashMap::new(),
            active_forward_addrs: HashSet::new(),
            #[cfg(any(test, feature = "test-util"))]
            lookup_calls: AtomicUsize::new(0),
            version: 0,
        }
    }

    /// Build an immutable request index from a manager routing table.
    pub fn from_proto(
        table: &RoutingTable,
        prev: Option<&IndexedRoutingTable>,
        registry: &CircuitBreakerRegistry,
        self_peer_address: &str,
    ) -> Self {
        let mut grouped: HashMap<RouteKey, Vec<PreparedCandidate>> = HashMap::new();
        let mut active_forward_addrs = HashSet::new();

        for rule in &table.rules {
            if !rule.healthy {
                continue;
            }
            if rule.peer_address.is_empty() {
                warn!(
                    rule_id = %rule.rule_id,
                    "skipping routing rule with empty peer_address"
                );
                continue;
            }
            let key = match RouteKey::parse(&rule.destination_namespace, &rule.destination_module) {
                Ok(key) => key,
                Err(error) => {
                    warn!(rule_id = %rule.rule_id, %error, "skipping routing rule with invalid identity");
                    continue;
                }
            };
            let parsed_version = match ModuleVersion::parse(&rule.destination_version) {
                Ok(version) => version,
                Err(error) => {
                    warn!(rule_id = %rule.rule_id, %error, "skipping routing rule with invalid semver");
                    continue;
                }
            };
            if let Err(error) = HeaderValue::from_str(&rule.destination_version) {
                warn!(rule_id = %rule.rule_id, %error, "skipping routing rule with invalid version header");
                continue;
            }

            let destination = make_destination(rule, self_peer_address);
            let address: Arc<str> = Arc::from(destination.address());
            let breaker = registry.resolve(&address);
            if !address.is_empty() {
                active_forward_addrs.insert(address);
            }
            grouped.entry(key).or_default().push(PreparedCandidate {
                parsed_version,
                version: rule.destination_version.clone(),
                destination,
                breaker,
            });
        }

        let mut by_namespace: HashMap<Arc<str>, ModulesByName> = HashMap::new();
        for (key, mut prepared) in grouped {
            prepared.sort_by(|a, b| b.parsed_version.cmp(&a.parsed_version));

            let namespace: Arc<str> = Arc::from(key.namespace.as_str());
            let module: Arc<str> = Arc::from(key.module.as_str());
            let namespace_header = match HeaderValue::from_str(&namespace) {
                Ok(value) => value,
                Err(error) => {
                    warn!(namespace = %namespace, %error, "skipping route group with invalid namespace header");
                    continue;
                }
            };
            let module_header = match HeaderValue::from_str(&module) {
                Ok(value) => value,
                Err(error) => {
                    warn!(module = %module, %error, "skipping route group with invalid module header");
                    continue;
                }
            };

            let mut version_indexes: HashMap<Arc<str>, usize> = HashMap::new();
            let mut version_groups: Vec<VersionGroup> = Vec::new();
            let mut candidates = Vec::with_capacity(prepared.len());
            for candidate in prepared {
                let version_group_index = if let Some(&index) =
                    version_indexes.get(candidate.version.as_str())
                {
                    index
                } else {
                    let version: Arc<str> = Arc::from(candidate.version.as_str());
                    let version_header = match HeaderValue::from_str(&version) {
                        Ok(value) => value,
                        Err(error) => {
                            warn!(version = %version, %error, "skipping routing candidate with invalid version header");
                            continue;
                        }
                    };
                    let index = version_groups.len();
                    version_indexes.insert(version.clone(), index);
                    version_groups.push(VersionGroup {
                        parsed_version: candidate.parsed_version.clone(),
                        version,
                        version_header,
                        candidate_indexes: Vec::new(),
                        counter: AtomicUsize::new(0),
                    });
                    index
                };

                let candidate_index = candidates.len();
                version_groups[version_group_index]
                    .candidate_indexes
                    .push(candidate_index);
                candidates.push(IndexedCandidate {
                    destination: candidate.destination,
                    breaker: candidate.breaker,
                    version_group_index,
                });
            }

            if candidates.is_empty() {
                continue;
            }

            let mut group = RouteGroup {
                namespace: namespace.clone(),
                module: module.clone(),
                namespace_header,
                module_header,
                candidates,
                version_groups,
                all_versions_counter: AtomicUsize::new(0),
            };
            if let Some(previous) = prev.and_then(|table| table.get(&namespace, &module)) {
                seed_group_counters(&mut group, previous);
            }

            let canonical_namespace = by_namespace
                .get_key_value(namespace.as_ref())
                .map_or(namespace, |(existing, _)| existing.clone());
            group.namespace = canonical_namespace.clone();
            by_namespace
                .entry(canonical_namespace)
                .or_default()
                .insert(module, group);
        }

        Self {
            by_namespace,
            active_forward_addrs,
            #[cfg(any(test, feature = "test-util"))]
            lookup_calls: AtomicUsize::new(0),
            version: table.version,
        }
    }

    pub(crate) fn seed_counters_from(&mut self, previous: &IndexedRoutingTable) {
        for (namespace, modules) in &mut self.by_namespace {
            for (module, group) in modules {
                if let Some(previous_group) = previous.get(namespace, module) {
                    seed_group_counters(group, previous_group);
                }
            }
        }
    }

    pub(crate) fn active_forward_addrs(&self) -> &HashSet<Arc<str>> {
        &self.active_forward_addrs
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn reset_lookup_calls(&self) {
        self.lookup_calls.store(0, Ordering::Relaxed);
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn lookup_calls(&self) -> usize {
        self.lookup_calls.load(Ordering::Relaxed)
    }

    /// Borrowed two-level lookup with no request-owned route key.
    pub fn get(&self, namespace: &str, module: &str) -> Option<&RouteGroup> {
        #[cfg(any(test, feature = "test-util"))]
        self.lookup_calls.fetch_add(1, Ordering::Relaxed);
        self.by_namespace.get(namespace)?.get(module)
    }
}

fn make_destination(rule: &RoutingRule, self_peer_address: &str) -> Destination {
    if rule.peer_address == self_peer_address {
        Destination::local(Arc::from(rule.engine_address.as_str()))
    } else {
        Destination::remote(Arc::from(rule.peer_address.as_str()))
    }
}

fn seed_group_counters(group: &mut RouteGroup, previous: &RouteGroup) {
    group.all_versions_counter =
        AtomicUsize::new(previous.all_versions_counter.load(Ordering::Relaxed));
    for version_group in &mut group.version_groups {
        if let Some(previous_version) = previous
            .version_groups
            .iter()
            .find(|candidate| candidate.version == version_group.version)
        {
            version_group.counter =
                AtomicUsize::new(previous_version.counter.load(Ordering::Relaxed));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CircuitBreakerConfig;

    const SELF: &str = "http://self-peer";

    fn registry() -> CircuitBreakerRegistry {
        CircuitBreakerRegistry::new(CircuitBreakerConfig::default())
    }

    fn rule(ns: &str, module: &str, version: &str, healthy: bool) -> RoutingRule {
        RoutingRule {
            rule_id: format!("{ns}/{module}/{version}"),
            source_module: String::new(),
            destination_module: module.to_string(),
            engine_id: "e1".to_string(),
            engine_address: format!("http://engine-{version}"),
            destination_version: version.to_string(),
            healthy,
            source_namespace: String::new(),
            destination_namespace: ns.to_string(),
            peer_address: SELF.to_string(),
        }
    }

    fn index(table: &RoutingTable, prev: Option<&IndexedRoutingTable>) -> IndexedRoutingTable {
        IndexedRoutingTable::from_proto(table, prev, &registry(), SELF)
    }

    #[test]
    fn nested_borrowed_lookup_and_canonical_identity() {
        let table = RoutingTable {
            rules: vec![
                rule("ns", "svc-a", "1.0.0", true),
                rule("ns", "svc-b", "1.0.0", true),
                rule("ns", "svc-a", "2.0.0", true),
            ],
            version: 1,
        };
        let indexed = index(&table, None);
        let group = indexed.get("ns", "svc-a").unwrap();
        let namespace_key = indexed
            .by_namespace
            .keys()
            .find(|key| &***key == "ns")
            .unwrap();
        let module_key = indexed
            .by_namespace
            .get("ns")
            .unwrap()
            .keys()
            .find(|key| &***key == "svc-a")
            .unwrap();
        assert!(Arc::ptr_eq(namespace_key, &group.namespace));
        assert!(Arc::ptr_eq(module_key, &group.module));
        assert_eq!(group.namespace_header, "ns");
        assert_eq!(group.module_header, "svc-a");
        assert_eq!(group.candidates.len(), 2);
        assert!(indexed.get("ns", "missing").is_none());
    }

    #[test]
    fn version_groups_are_unique_descending_and_candidates_reference_them() {
        let table = RoutingTable {
            rules: vec![
                rule("ns", "svc", "1.0.0", true),
                rule("ns", "svc", "3.0.0", true),
                rule("ns", "svc", "2.0.0", true),
                rule("ns", "svc", "3.0.0", true),
            ],
            version: 1,
        };
        let indexed = index(&table, None);
        let group = indexed.get("ns", "svc").unwrap();
        let versions: Vec<&str> = group
            .version_groups
            .iter()
            .map(|version| &*version.version)
            .collect();
        assert_eq!(versions, ["3.0.0", "2.0.0", "1.0.0"]);
        assert_eq!(group.version_groups[0].candidate_indexes.len(), 2);
        for (version_index, version) in group.version_groups.iter().enumerate() {
            for &candidate_index in &version.candidate_indexes {
                assert_eq!(
                    group.candidates[candidate_index].version_group_index,
                    version_index
                );
            }
        }
    }

    #[test]
    fn excludes_invalid_and_unhealthy_rules_before_breaker_resolution() {
        let registry = registry();
        let mut invalid_identity = rule("bad_name", "svc", "1.0.0", true);
        invalid_identity.rule_id = "invalid-identity".into();
        let mut invalid_version = rule("ns", "svc", "latest", true);
        invalid_version.rule_id = "invalid-version".into();
        let mut empty_peer = rule("ns", "svc", "1.0.0", true);
        empty_peer.peer_address.clear();
        let table = RoutingTable {
            rules: vec![
                rule("ns", "svc", "1.0.0", true),
                rule("ns", "svc", "2.0.0", false),
                invalid_identity,
                invalid_version,
                empty_peer,
            ],
            version: 1,
        };
        let indexed = IndexedRoutingTable::from_proto(&table, None, &registry, SELF);
        assert_eq!(indexed.get("ns", "svc").unwrap().candidates.len(), 1);
        assert_eq!(registry.resolver_calls(), 1);
    }

    #[test]
    fn prepares_local_remote_and_uri_bases() {
        let mut local = rule("ns", "local", "1.0.0", true);
        local.engine_address = "http://127.0.0.1:9100/".into();
        let mut remote = rule("ns", "remote", "1.0.0", true);
        remote.peer_address = "https://[::1]:9443".into();
        let mut fallback = rule("ns", "fallback", "1.0.0", true);
        fallback.engine_address = "http://engine/base".into();
        let table = RoutingTable {
            rules: vec![local, remote, fallback],
            version: 1,
        };
        let indexed = index(&table, None);
        assert!(indexed.get("ns", "local").unwrap().candidates[0]
            .destination
            .target()
            .base_uri()
            .is_some());
        assert!(indexed.get("ns", "remote").unwrap().candidates[0]
            .destination
            .target()
            .base_uri()
            .is_some());
        assert!(indexed.get("ns", "fallback").unwrap().candidates[0]
            .destination
            .target()
            .base_uri()
            .is_none());
    }

    #[test]
    fn counters_carry_over_by_route_and_exact_version() {
        let table = RoutingTable {
            rules: vec![
                rule("ns", "svc", "1.0.0", true),
                rule("ns", "svc", "2.0.0", true),
            ],
            version: 1,
        };
        let previous = index(&table, None);
        let previous_group = previous.get("ns", "svc").unwrap();
        previous_group
            .all_versions_counter
            .store(5, Ordering::Relaxed);
        previous_group.version_groups[0]
            .counter
            .store(7, Ordering::Relaxed);

        let next = index(
            &RoutingTable {
                version: 2,
                ..table
            },
            Some(&previous),
        );
        let next_group = next.get("ns", "svc").unwrap();
        assert_eq!(next_group.all_versions_counter.load(Ordering::Relaxed), 5);
        assert_eq!(
            next_group.version_groups[0].counter.load(Ordering::Relaxed),
            7
        );
    }
}
