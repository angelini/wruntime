use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tracing::warn;
use wr_common::identity::{ModuleVersion, RouteKey};
use wr_common::wruntime::{RoutingRule, RoutingTable};

/// A routing rule with its semver version pre-parsed at sync time.
pub struct ParsedRule {
    pub rule: RoutingRule,
    pub parsed_version: ModuleVersion,
}

/// Round-robin state for the set of candidates that share one exact version
/// string within a route group.
pub struct VersionGroup {
    /// Indexes into the owning `RouteGroup::candidates`.
    pub candidate_indexes: Vec<usize>,
    /// Per-version round-robin counter (used when `x-wr-version` pins a version).
    pub counter: AtomicUsize,
}

/// All healthy rules for one `(namespace, module)`, pre-sorted by parsed semver
/// version descending (best version first), with per-route round-robin counters.
pub struct RouteGroup {
    pub candidates: Vec<ParsedRule>,
    /// Round-robin counter used when no `x-wr-version` is pinned (spreads across
    /// all versions).
    pub all_versions_counter: AtomicUsize,
    /// Candidates grouped by exact `destination_version` string.
    pub by_version: HashMap<Arc<str>, VersionGroup>,
}

/// Pre-indexed routing table built once at sync time (every ~2s) rather than
/// scanned linearly on every request. Rules are grouped by `(namespace, module)`
/// into a `RouteGroup`; per-route `AtomicUsize` counters live here (not in the
/// middleware) and are carried over best-effort across syncs.
pub struct IndexedRoutingTable {
    by_module: HashMap<RouteKey, RouteGroup>,
    pub version: u64,
}

impl IndexedRoutingTable {
    pub fn empty() -> Self {
        Self {
            by_module: HashMap::new(),
            version: 0,
        }
    }

    /// Build the index from a protobuf `RoutingTable` received from the manager.
    /// Only healthy rules are indexed; unhealthy rules are dropped at sync time.
    pub fn from_proto(table: &RoutingTable, prev: Option<&IndexedRoutingTable>) -> Self {
        let mut grouped: HashMap<RouteKey, Vec<ParsedRule>> = HashMap::new();

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
            grouped.entry(key).or_default().push(ParsedRule {
                rule: rule.clone(),
                parsed_version,
            });
        }

        let mut by_module: HashMap<RouteKey, RouteGroup> = HashMap::with_capacity(grouped.len());

        for (key, mut candidates) in grouped {
            // Sort each group by version descending so the best version is at the front.
            candidates.sort_by(|a, b| b.parsed_version.cmp(&a.parsed_version));

            // Group candidate indexes by exact version string.
            let mut by_version: HashMap<Arc<str>, VersionGroup> = HashMap::new();
            for (idx, c) in candidates.iter().enumerate() {
                let ver: Arc<str> = Arc::from(c.rule.destination_version.as_str());
                by_version
                    .entry(ver)
                    .or_insert_with(|| VersionGroup {
                        candidate_indexes: Vec::new(),
                        counter: AtomicUsize::new(0),
                    })
                    .candidate_indexes
                    .push(idx);
            }

            // Best-effort counter carry-over from the previous table: same
            // (ns, module) seeds all_versions_counter; same (ns, module, version)
            // seeds the per-version counter. Missing keys start at 0.
            let prev_group = prev.and_then(|p| p.by_module.get(&key));
            for (ver, vg) in by_version.iter_mut() {
                if let Some(pvg) = prev_group.and_then(|g| g.by_version.get(ver)) {
                    vg.counter = AtomicUsize::new(pvg.counter.load(Ordering::Relaxed));
                }
            }
            let all_versions_counter = AtomicUsize::new(
                prev_group.map_or(0, |g| g.all_versions_counter.load(Ordering::Relaxed)),
            );

            by_module.insert(
                key,
                RouteGroup {
                    candidates,
                    all_versions_counter,
                    by_version,
                },
            );
        }

        Self {
            by_module,
            version: table.version,
        }
    }

    /// Collect the forwarding addresses the circuit breaker keys on, matching
    /// `make_destination`: the engine address for locally-dispatched rules
    /// (`peer_address == self_peer_address`), the peer address for remote rules.
    /// Empty addresses are skipped.
    pub fn active_forward_addrs(&self, self_peer_address: &str) -> std::collections::HashSet<&str> {
        self.by_module
            .values()
            .flat_map(|group| group.candidates.iter())
            .filter_map(|r| {
                let addr = if r.rule.peer_address == self_peer_address {
                    r.rule.engine_address.as_str()
                } else {
                    r.rule.peer_address.as_str()
                };
                (!addr.is_empty()).then_some(addr)
            })
            .collect()
    }

    /// Look up the route group for a `(namespace, module)` pair. Returns `None`
    /// when no healthy routes exist for this destination.
    pub fn get(&self, namespace: &str, module: &str) -> Option<&RouteGroup> {
        RouteKey::parse(namespace, module)
            .ok()
            .and_then(|key| self.by_module.get(&key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wr_common::wruntime::RoutingRule;

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
            peer_address: "http://self-peer".to_string(),
        }
    }

    #[test]
    fn indexes_by_namespace_module() {
        let table = RoutingTable {
            rules: vec![
                rule("ns", "svc-a", "1.0.0", true),
                rule("ns", "svc-b", "1.0.0", true),
                rule("ns", "svc-a", "2.0.0", true),
            ],
            version: 1,
        };
        let idx = IndexedRoutingTable::from_proto(&table, None);
        assert_eq!(idx.get("ns", "svc-a").unwrap().candidates.len(), 2);
        assert_eq!(idx.get("ns", "svc-b").unwrap().candidates.len(), 1);
        assert!(idx.get("ns", "svc-c").is_none());
    }

    #[test]
    fn sorted_descending_by_version() {
        let table = RoutingTable {
            rules: vec![
                rule("ns", "svc", "1.0.0", true),
                rule("ns", "svc", "3.0.0", true),
                rule("ns", "svc", "2.0.0", true),
            ],
            version: 1,
        };
        let idx = IndexedRoutingTable::from_proto(&table, None);
        let rules = &idx.get("ns", "svc").unwrap().candidates;
        let versions: Vec<&str> = rules
            .iter()
            .map(|r| r.rule.destination_version.as_str())
            .collect();
        assert_eq!(versions, vec!["3.0.0", "2.0.0", "1.0.0"]);
    }

    #[test]
    fn excludes_unhealthy_rules() {
        let table = RoutingTable {
            rules: vec![
                rule("ns", "svc", "1.0.0", true),
                rule("ns", "svc", "2.0.0", false),
            ],
            version: 1,
        };
        let idx = IndexedRoutingTable::from_proto(&table, None);
        let group = idx.get("ns", "svc").unwrap();
        assert_eq!(group.candidates.len(), 1);
        assert_eq!(group.candidates[0].rule.destination_version, "1.0.0");
    }

    #[test]
    fn empty_table() {
        let table = RoutingTable {
            rules: vec![],
            version: 0,
        };
        let idx = IndexedRoutingTable::from_proto(&table, None);
        assert!(idx.get("ns", "svc").is_none());
        assert_eq!(idx.version, 0);
    }

    #[test]
    fn active_forward_addrs_local_and_remote() {
        const SELF: &str = "http://self-peer";
        // Local rule: peer_address == self → contributes engine_address.
        let mut local = rule("ns", "svc-a", "1.0.0", true);
        local.peer_address = SELF.to_string();
        local.engine_address = "http://engine-local".to_string();
        // Remote rule: peer_address != self → contributes peer_address, not engine_address.
        let mut remote = rule("ns", "svc-b", "1.0.0", true);
        remote.peer_address = "http://remote-peer".to_string();
        remote.engine_address = "http://engine-remote".to_string();

        let table = RoutingTable {
            rules: vec![local, remote],
            version: 1,
        };
        let idx = IndexedRoutingTable::from_proto(&table, None);
        let addrs = idx.active_forward_addrs(SELF);

        assert!(addrs.contains("http://engine-local"));
        assert!(addrs.contains("http://remote-peer"));
        assert!(!addrs.contains("http://engine-remote"));
        assert!(!addrs.contains(SELF));
        assert_eq!(addrs.len(), 2);
    }

    #[test]
    fn counter_carry_over_seeds_from_previous_table() {
        let table = RoutingTable {
            rules: vec![
                rule("ns", "svc", "1.0.0", true),
                rule("ns", "svc", "2.0.0", true),
            ],
            version: 1,
        };
        let prev = IndexedRoutingTable::from_proto(&table, None);
        // Advance the all-versions counter on the previous table.
        let g = prev.get("ns", "svc").unwrap();
        g.all_versions_counter.fetch_add(5, Ordering::Relaxed);

        let next = IndexedRoutingTable::from_proto(&table, Some(&prev));
        assert_eq!(
            next.get("ns", "svc")
                .unwrap()
                .all_versions_counter
                .load(Ordering::Relaxed),
            5
        );
    }
}
