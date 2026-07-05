use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::{Context, Poll};

use http::{Request, StatusCode};
use tower::{Layer, Service};
use tracing::{info_span, Instrument};

use super::egress::{domain_matches, ExternalEgress};
use super::{error_response, Destination, ProxyBody, ResBody, ResolvedDestination};
use crate::indexed_routing::{IndexedRoutingTable, RouteGroup};
use crate::routing::CachedRoutingTable;
use wr_common::http_headers::{WR_DESTINATION, WR_MODULE, WR_NAMESPACE, WR_VERSION};

/// A routing candidate with its resolved version attached so the caller can
/// inject the correct `x-wr-version` header after round-robin selection.
#[derive(Clone)]
struct VersionedCandidate {
    dest: Destination,
    version: Arc<str>,
}

/// Route-aware classification of an `x-wr-destination`. Produced by
/// `classify_destination` using the routing table + egress allowlist.
#[derive(Debug)]
enum ParsedDestination {
    Internal {
        namespace: Arc<str>,
        module: Arc<str>,
        #[allow(dead_code)]
        uri: http::Uri,
    },
    External {
        host: String,
        uri: http::Uri,
    },
}

/// The routing-layer decision, computed under a single read guard so no table
/// state is borrowed after the guard is dropped.
enum RouteOutcome {
    Internal {
        namespace: Arc<str>,
        module: Arc<str>,
        chosen: VersionedCandidate,
    },
    External {
        host: String,
        dest_uri: http::Uri,
    },
    Reject(StatusCode, String),
}

pub struct RoutingLayer {
    table: CachedRoutingTable,
    /// This proxy's own address — used to distinguish local vs. remote rules.
    self_peer_address: Arc<str>,
    /// Egress allowlist patterns. Only destinations matching one of these
    /// patterns are forwarded via egress; all other unroutable destinations
    /// get a 503. Empty means egress is disabled.
    egress_allowed_domains: Arc<Vec<String>>,
}

impl RoutingLayer {
    pub fn new(table: CachedRoutingTable, self_peer_address: impl Into<String>) -> Self {
        Self {
            table,
            self_peer_address: Arc::from(self_peer_address.into()),
            egress_allowed_domains: Arc::new(Vec::new()),
        }
    }

    pub fn with_egress(mut self, allowed_domains: Vec<String>) -> Self {
        // Pre-lowercase patterns once at construction time so domain_matches()
        // can skip per-request lowercasing of the pattern side.
        let lowered: Vec<String> = allowed_domains
            .into_iter()
            .map(|d| d.to_ascii_lowercase())
            .collect();
        self.egress_allowed_domains = Arc::new(lowered);
        self
    }
}

impl<S> Layer<S> for RoutingLayer {
    type Service = RoutingService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        RoutingService {
            inner,
            table: self.table.clone(),
            self_peer_address: self.self_peer_address.clone(),
            egress_allowed_domains: self.egress_allowed_domains.clone(),
        }
    }
}

#[derive(Clone)]
pub struct RoutingService<S> {
    inner: S,
    table: CachedRoutingTable,
    self_peer_address: Arc<str>,
    egress_allowed_domains: Arc<Vec<String>>,
}

// ── Routing helpers ─────────────────────────────────────────────────────────

fn make_destination(
    rule: &wr_common::wruntime::RoutingRule,
    self_peer_address: &str,
) -> Destination {
    if rule.peer_address == self_peer_address {
        Destination::LocalEngine(Arc::from(rule.engine_address.as_str()))
    } else {
        Destination::RemoteProxy(Arc::from(rule.peer_address.as_str()))
    }
}

/// Classify an `x-wr-destination` into an internal module route or an external
/// egress target. Route-aware: a two-label host is Internal only when a route
/// exists for `(namespace, module)`; otherwise, an egress-allowlisted host is
/// External. Returns `Err((status, message))` for malformed input (400) or for
/// a destination that is neither routable nor allowlisted (503).
fn classify_destination(
    table: &IndexedRoutingTable,
    egress_allowed_domains: &[String],
    dest_uri: Option<http::Uri>,
) -> Result<ParsedDestination, (StatusCode, String)> {
    let uri = match dest_uri {
        Some(u) => u,
        None => {
            return Err((
                StatusCode::BAD_REQUEST,
                "missing or malformed x-wr-destination header".to_string(),
            ))
        }
    };
    let host = match uri.host() {
        Some(h) if !h.is_empty() => h.to_string(),
        _ => {
            return Err((
                StatusCode::BAD_REQUEST,
                "x-wr-destination has no host".to_string(),
            ))
        }
    };

    // A single-label host (no dot) is a malformed internal destination: the
    // namespace is missing. Return 400 regardless of egress configuration.
    let labels: Vec<&str> = host.split('.').collect();
    if labels.len() < 2 {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "destination host '{host}' is missing a namespace (expected '{{namespace}}.{{module}}')"
            ),
        ));
    }

    // Internal: exactly two dot-separated labels AND a route exists for them.
    if labels.len() == 2 && !labels[0].is_empty() && !labels[1].is_empty() {
        let namespace: Arc<str> = Arc::from(labels[0]);
        let module: Arc<str> = Arc::from(labels[1]);
        if table.get(&namespace, &module).is_some() {
            return Ok(ParsedDestination::Internal {
                namespace,
                module,
                uri,
            });
        }
    }

    // External: egress configured AND the full host matches the allowlist.
    let host_lc = host.to_ascii_lowercase();
    if !egress_allowed_domains.is_empty()
        && egress_allowed_domains
            .iter()
            .any(|pattern| domain_matches(pattern, &host_lc))
    {
        return Ok(ParsedDestination::External { host: host_lc, uri });
    }

    Err((
        StatusCode::SERVICE_UNAVAILABLE,
        format!("no route for destination '{host}'"),
    ))
}

/// Round-robin select one candidate from a route group, honoring an optional
/// version requirement. Returns `None` when no candidate satisfies the request.
///
/// Unpinned (no `x-wr-version`): spread across all versions via
/// `all_versions_counter`. Pinned semver: pick the highest satisfying version
/// (candidates are pre-sorted descending), then round-robin within that
/// version's `VersionGroup::counter`. Exact non-semver string: direct
/// `by_version` lookup.
fn select_candidate(
    group: &RouteGroup,
    requested_version: &Option<String>,
    self_peer_address: &str,
) -> Option<VersionedCandidate> {
    let make = |r: &crate::indexed_routing::ParsedRule| VersionedCandidate {
        dest: make_destination(&r.rule, self_peer_address),
        version: Arc::from(r.rule.destination_version.as_str()),
    };

    match requested_version {
        None => {
            let len = group.candidates.len();
            if len == 0 {
                return None;
            }
            let idx = group.all_versions_counter.fetch_add(1, Ordering::Relaxed) % len;
            Some(make(&group.candidates[idx]))
        }
        Some(version_str) => {
            let req = semver::VersionReq::parse(version_str).ok();
            let best_ver: Arc<str> = match req {
                Some(req) => group
                    .candidates
                    .iter()
                    .find(|r| r.parsed_version.as_ref().is_some_and(|v| req.matches(v)))
                    .map(|r| Arc::from(r.rule.destination_version.as_str()))?,
                None => {
                    if group.by_version.contains_key(version_str.as_str()) {
                        Arc::from(version_str.as_str())
                    } else {
                        return None;
                    }
                }
            };
            let vg = group.by_version.get(&best_ver)?;
            let len = vg.candidate_indexes.len();
            if len == 0 {
                return None;
            }
            let slot = vg.counter.fetch_add(1, Ordering::Relaxed) % len;
            Some(make(&group.candidates[vg.candidate_indexes[slot]]))
        }
    }
}

/// Inject x-wr-namespace, x-wr-module, and x-wr-version headers.
fn inject_routing_headers(
    req: &mut Request<ProxyBody>,
    namespace: &str,
    module: &str,
    version: &str,
) {
    if let Ok(v) = http::HeaderValue::from_str(namespace) {
        req.headers_mut().insert(WR_NAMESPACE, v);
    }
    if !module.is_empty() {
        if let Ok(v) = http::HeaderValue::from_str(module) {
            req.headers_mut().insert(WR_MODULE, v);
        }
    }
    if let Ok(v) = http::HeaderValue::from_str(version) {
        req.headers_mut().insert(WR_VERSION, v);
    }
}

// ── Service implementation ──────────────────────────────────────────────────

impl<S> Service<Request<ProxyBody>> for RoutingService<S>
where
    S: Service<Request<ProxyBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Error: Send + 'static,
    S::Future: Send + 'static,
{
    type Response = http::Response<ResBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request<ProxyBody>) -> Self::Future {
        let table = self.table.clone();
        let self_peer_address = self.self_peer_address.clone();
        let egress_allowed_domains = self.egress_allowed_domains.clone();
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let dest_uri: Option<http::Uri> = req
                .headers()
                .get(WR_DESTINATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse().ok());

            let requested_version: Option<String> = req
                .headers()
                .get(WR_VERSION)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());

            // Classify + (for internal) select under a single read guard so no
            // table state is borrowed once we start forwarding.
            let outcome = {
                let t = table.read().await;
                match classify_destination(&t, &egress_allowed_domains, dest_uri) {
                    Err((status, msg)) => RouteOutcome::Reject(status, msg),
                    Ok(ParsedDestination::External { host, uri }) => RouteOutcome::External {
                        host,
                        dest_uri: uri,
                    },
                    Ok(ParsedDestination::Internal {
                        namespace, module, ..
                    }) => {
                        let chosen = t.get(&namespace, &module).and_then(|group| {
                            select_candidate(group, &requested_version, &self_peer_address)
                        });
                        match chosen {
                            Some(chosen) => RouteOutcome::Internal {
                                namespace,
                                module,
                                chosen,
                            },
                            None => {
                                let msg = match &requested_version {
                                    Some(v) => format!(
                                        "no route for module '{namespace}.{module}' matching version requirement '{v}'"
                                    ),
                                    None => format!("no route for module '{namespace}.{module}'"),
                                };
                                RouteOutcome::Reject(StatusCode::SERVICE_UNAVAILABLE, msg)
                            }
                        }
                    }
                }
            };

            let span = info_span!(
                "proxy.route",
                wr.module = tracing::field::Empty,
                wr.namespace = tracing::field::Empty,
                wr.version = tracing::field::Empty,
                wr.engine = tracing::field::Empty,
                otel.status_code = tracing::field::Empty,
            );

            match outcome {
                RouteOutcome::Internal {
                    namespace,
                    module,
                    chosen,
                } => {
                    span.record("wr.namespace", &*namespace);
                    span.record("wr.module", &*module);
                    span.record("wr.version", &*chosen.version);
                    span.record("wr.engine", chosen.dest.address());
                    inject_routing_headers(&mut req, &namespace, &module, &chosen.version);
                    req.extensions_mut()
                        .insert(ResolvedDestination(chosen.dest));
                    inner.call(req).instrument(span).await
                }
                RouteOutcome::External { host, dest_uri } => {
                    req.extensions_mut()
                        .insert(ExternalEgress { host, dest_uri });
                    inner.call(req).instrument(span).await
                }
                RouteOutcome::Reject(status, msg) => {
                    span.record("otel.status_code", "ERROR");
                    Ok(error_response(status, &msg))
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexed_routing::IndexedRoutingTable;
    use wr_common::wruntime::{RoutingRule, RoutingTable};

    fn rule(ns: &str, module: &str, version: &str) -> RoutingRule {
        RoutingRule {
            rule_id: format!("{ns}/{module}/{version}"),
            source_module: String::new(),
            destination_module: module.to_string(),
            engine_id: "e1".to_string(),
            engine_address: format!("http://engine-{version}"),
            destination_version: version.to_string(),
            healthy: true,
            source_namespace: String::new(),
            destination_namespace: ns.to_string(),
            peer_address: "http://self-peer".to_string(),
        }
    }

    fn table_with(rules: Vec<RoutingRule>) -> IndexedRoutingTable {
        IndexedRoutingTable::from_proto(&RoutingTable { rules, version: 1 }, None)
    }

    fn uri(s: &str) -> Option<http::Uri> {
        s.parse().ok()
    }

    #[test]
    fn classify_two_label_with_route_is_internal() {
        let t = table_with(vec![rule("store", "inventory", "1.0.0")]);
        match classify_destination(&t, &[], uri("http://store.inventory/Ping")).unwrap() {
            ParsedDestination::Internal {
                namespace, module, ..
            } => {
                assert_eq!(&*namespace, "store");
                assert_eq!(&*module, "inventory");
            }
            _ => panic!("expected Internal"),
        }
    }

    #[test]
    fn classify_two_label_no_route_allowlisted_is_external() {
        let t = table_with(vec![]);
        let domains = vec!["example.com".to_string()];
        assert!(matches!(
            classify_destination(&t, &domains, uri("http://example.com/x")).unwrap(),
            ParsedDestination::External { .. }
        ));
    }

    #[test]
    fn classify_two_label_no_route_no_egress_is_503() {
        let t = table_with(vec![]);
        let err = classify_destination(&t, &[], uri("http://example.com/x")).unwrap_err();
        assert_eq!(err.0, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn classify_multi_label_allowlisted_is_external() {
        let t = table_with(vec![]);
        let domains = vec!["*.openai.com".to_string()];
        assert!(matches!(
            classify_destination(&t, &domains, uri("http://api.openai.com/v1")).unwrap(),
            ParsedDestination::External { .. }
        ));
    }

    #[test]
    fn classify_multi_label_not_allowlisted_is_503() {
        let t = table_with(vec![]);
        let err = classify_destination(&t, &[], uri("http://api.openai.com/v1")).unwrap_err();
        assert_eq!(err.0, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn classify_missing_destination_is_400() {
        let t = table_with(vec![]);
        let err = classify_destination(&t, &[], None).unwrap_err();
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn select_candidate_unpinned_spreads_across_versions() {
        let t = table_with(vec![rule("ns", "svc", "1.0.0"), rule("ns", "svc", "2.0.0")]);
        let group = t.get("ns", "svc").unwrap();
        let mut seen = std::collections::HashSet::new();
        for _ in 0..6 {
            let c = select_candidate(group, &None, "http://self-peer").unwrap();
            seen.insert(c.version.to_string());
        }
        assert!(seen.contains("1.0.0"));
        assert!(seen.contains("2.0.0"));
    }
}
