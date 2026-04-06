use std::collections::HashMap;
use std::sync::Arc;

use wr_common::wruntime::{RoutingRule, RoutingTable};

/// A routing rule with its semver version pre-parsed at sync time.
pub struct ParsedRule {
    pub rule: RoutingRule,
    pub parsed_version: Option<semver::Version>,
}

/// Pre-indexed routing table built once at sync time (every ~2s) rather than
/// scanned linearly on every request.
///
/// Rules are grouped by `(namespace, module)` and pre-sorted by parsed semver
/// version descending within each group.  This turns per-request routing from
/// O(n) filter + repeated semver parsing into an O(1) HashMap lookup.
pub struct IndexedRoutingTable {
    by_module: HashMap<(Arc<str>, Arc<str>), Vec<ParsedRule>>,
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
    pub fn from_proto(table: &RoutingTable) -> Self {
        let mut by_module: HashMap<(Arc<str>, Arc<str>), Vec<ParsedRule>> = HashMap::new();

        for rule in &table.rules {
            if !rule.healthy {
                continue;
            }
            let key = (
                Arc::from(rule.destination_namespace.as_str()),
                Arc::from(rule.destination_module.as_str()),
            );
            let parsed_version = semver::Version::parse(&rule.destination_version).ok();
            by_module.entry(key).or_default().push(ParsedRule {
                rule: rule.clone(),
                parsed_version,
            });
        }

        // Sort each group by version descending so best version is at the front.
        for rules in by_module.values_mut() {
            rules.sort_by(|a, b| match (&b.parsed_version, &a.parsed_version) {
                (Some(bv), Some(av)) => bv.cmp(av),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => b.rule.destination_version.cmp(&a.rule.destination_version),
            });
        }

        Self {
            by_module,
            version: table.version,
        }
    }

    /// Collect all unique engine addresses in the index (for circuit breaker eviction).
    pub fn active_engines(&self) -> std::collections::HashSet<&str> {
        self.by_module
            .values()
            .flat_map(|rules| rules.iter().map(|r| r.rule.engine_address.as_str()))
            .collect()
    }

    /// Look up rules for a (namespace, module) pair. Returns an empty slice if
    /// no healthy routes exist for this destination.
    pub fn get(&self, namespace: &str, module: &str) -> &[ParsedRule] {
        // Construct a lookup key from borrowed strings.  This requires iterating
        // the map because HashMap doesn't support heterogeneous lookup out of the
        // box for tuple keys. We use a small helper to avoid that cost.
        //
        // Instead, we accept the one-time Arc allocation at lookup time — this is
        // still far cheaper than the previous approach which cloned all rules and
        // re-parsed semver on every request.
        self.by_module
            .get(&(Arc::from(namespace), Arc::from(module)))
            .map(|v| v.as_slice())
            .unwrap_or(&[])
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
            proxy_address: "http://proxy:9001".to_string(),
            peer_address: String::new(),
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
        let idx = IndexedRoutingTable::from_proto(&table);
        assert_eq!(idx.get("ns", "svc-a").len(), 2);
        assert_eq!(idx.get("ns", "svc-b").len(), 1);
        assert_eq!(idx.get("ns", "svc-c").len(), 0);
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
        let idx = IndexedRoutingTable::from_proto(&table);
        let rules = idx.get("ns", "svc");
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
        let idx = IndexedRoutingTable::from_proto(&table);
        assert_eq!(idx.get("ns", "svc").len(), 1);
        assert_eq!(idx.get("ns", "svc")[0].rule.destination_version, "1.0.0");
    }

    #[test]
    fn empty_table() {
        let table = RoutingTable {
            rules: vec![],
            version: 0,
        };
        let idx = IndexedRoutingTable::from_proto(&table);
        assert_eq!(idx.get("ns", "svc").len(), 0);
        assert_eq!(idx.version, 0);
    }

    #[test]
    fn active_engines_deduplicates() {
        let mut r1 = rule("ns", "svc-a", "1.0.0", true);
        r1.engine_address = "http://engine-1".to_string();
        let mut r2 = rule("ns", "svc-b", "1.0.0", true);
        r2.engine_address = "http://engine-2".to_string();
        // Add a duplicate of engine-1 on a different module.
        let mut r3 = rule("ns", "svc-c", "1.0.0", true);
        r3.engine_address = "http://engine-1".to_string();

        let table = RoutingTable {
            rules: vec![r1, r2, r3],
            version: 1,
        };
        let idx = IndexedRoutingTable::from_proto(&table);
        let engines = idx.active_engines();
        // 3 rules but only 2 unique engine addresses.
        assert_eq!(engines.len(), 2);
        assert!(engines.contains("http://engine-1"));
        assert!(engines.contains("http://engine-2"));
    }
}
