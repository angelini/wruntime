use std::future::Future;
use std::pin::Pin;
#[cfg(any(test, feature = "test-util"))]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::{Context, Poll};

use http::{HeaderValue, Request, StatusCode};
use smallvec::SmallVec;
use tower::{Layer, Service};
use tracing::{info_span, Instrument};

use super::egress::{domain_matches, ExternalEgress};
use super::{error_response, ProxyBody, ResBody, ResolvedRoute};
use crate::indexed_routing::{IndexedRoutingTable, RouteGroup};
use crate::routing::CachedRoutingTable;
use wr_common::http_headers::{WR_DESTINATION, WR_MODULE, WR_NAMESPACE, WR_VERSION};
use wr_common::identity::{RouteKey, VersionSelector};

#[cfg(any(test, feature = "test-util"))]
static SELECTOR_PARSE_CALLS: AtomicUsize = AtomicUsize::new(0);

enum RequestedVersion {
    Unpinned,
    Requirement {
        selector: VersionSelector,
        raw: HeaderValue,
    },
}

struct SelectedCandidate {
    route: ResolvedRoute,
    version: Arc<str>,
    version_header: HeaderValue,
}

enum RouteOutcome {
    Internal {
        namespace: Arc<str>,
        module: Arc<str>,
        namespace_header: HeaderValue,
        module_header: HeaderValue,
        selected: SelectedCandidate,
    },
    External {
        host: String,
        dest_uri: http::Uri,
    },
    Reject(StatusCode, String),
}

pub struct RoutingLayer {
    table: CachedRoutingTable,
    egress_allowed_domains: Arc<Vec<String>>,
}

impl RoutingLayer {
    pub fn new(table: CachedRoutingTable) -> Self {
        Self {
            table,
            egress_allowed_domains: Arc::new(Vec::new()),
        }
    }

    pub fn with_egress(mut self, allowed_domains: Vec<String>) -> Self {
        self.egress_allowed_domains = Arc::new(
            allowed_domains
                .into_iter()
                .map(|domain| domain.to_ascii_lowercase())
                .collect(),
        );
        self
    }
}

impl<S> Layer<S> for RoutingLayer {
    type Service = RoutingService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RoutingService {
            inner,
            table: self.table.clone(),
            egress_allowed_domains: self.egress_allowed_domains.clone(),
        }
    }
}

#[derive(Clone)]
pub struct RoutingService<S> {
    inner: S,
    table: CachedRoutingTable,
    egress_allowed_domains: Arc<Vec<String>>,
}

fn parse_requested_version(value: Option<&HeaderValue>) -> Result<RequestedVersion, String> {
    let Some(raw) = value else {
        return Ok(RequestedVersion::Unpinned);
    };
    let text = raw
        .to_str()
        .map_err(|error| format!("invalid x-wr-version requirement: {error}"))?;
    #[cfg(any(test, feature = "test-util"))]
    SELECTOR_PARSE_CALLS.fetch_add(1, Ordering::Relaxed);
    let selector = VersionSelector::parse(text)
        .map_err(|error| format!("invalid x-wr-version requirement '{text}': {error}"))?;
    Ok(RequestedVersion::Requirement {
        selector,
        raw: raw.clone(),
    })
}

fn choose_candidate<I>(
    group: &RouteGroup,
    mut eligible_indexes: I,
    counter: &std::sync::atomic::AtomicUsize,
) -> Option<usize>
where
    I: ExactSizeIterator<Item = usize> + Clone,
{
    let eligible_count = eligible_indexes.len();
    if eligible_count == 0 {
        return None;
    }

    let mut permitted_indexes: SmallVec<[usize; 8]> = SmallVec::new();
    for index in eligible_indexes.clone() {
        if group.candidates[index].breaker.is_call_permitted() {
            permitted_indexes.push(index);
        }
    }

    let slot = counter.fetch_add(1, Ordering::Relaxed);
    if permitted_indexes.is_empty() {
        eligible_indexes.nth(slot % eligible_count)
    } else {
        Some(permitted_indexes[slot % permitted_indexes.len()])
    }
}

fn select_candidate(
    group: &RouteGroup,
    requested_version: &RequestedVersion,
) -> Option<SelectedCandidate> {
    let candidate_index = match requested_version {
        RequestedVersion::Unpinned => choose_candidate(
            group,
            0..group.candidates.len(),
            &group.all_versions_counter,
        )?,
        RequestedVersion::Requirement { selector, .. } => {
            let version_group = group
                .version_groups
                .iter()
                .find(|version| selector.matches(&version.parsed_version))?;
            choose_candidate(
                group,
                version_group.candidate_indexes.iter().copied(),
                &version_group.counter,
            )?
        }
    };

    let candidate = &group.candidates[candidate_index];
    let version_group = &group.version_groups[candidate.version_group_index];
    Some(SelectedCandidate {
        route: ResolvedRoute {
            destination: candidate.destination.clone(),
            breaker: candidate.breaker.clone(),
        },
        version: version_group.version.clone(),
        version_header: version_group.version_header.clone(),
    })
}

fn classify_external_host(host: &str, egress_allowed_domains: &[String]) -> Result<String, String> {
    if egress_allowed_domains.is_empty() {
        return Err(format!("no route for destination '{host}'"));
    }

    let host_lc = host.to_ascii_lowercase();
    if egress_allowed_domains
        .iter()
        .any(|pattern| domain_matches(pattern, &host_lc))
    {
        Ok(host_lc)
    } else {
        Err(format!("no route for destination '{host}'"))
    }
}

/// Classify, perform one borrowed route-group lookup, and select under one snapshot.
fn route_destination(
    table: &IndexedRoutingTable,
    egress_allowed_domains: &[String],
    dest_uri: Option<http::Uri>,
    requested_version: &RequestedVersion,
) -> RouteOutcome {
    let uri = match dest_uri {
        Some(uri) => uri,
        None => {
            return RouteOutcome::Reject(
                StatusCode::BAD_REQUEST,
                "missing or malformed x-wr-destination header".to_string(),
            )
        }
    };
    let host = match uri.host() {
        Some(host) if !host.is_empty() => host,
        _ => {
            return RouteOutcome::Reject(
                StatusCode::BAD_REQUEST,
                "x-wr-destination has no host".to_string(),
            )
        }
    };

    let Some((namespace, module)) = host.split_once('.') else {
        return RouteOutcome::Reject(
            StatusCode::BAD_REQUEST,
            format!(
                "destination host '{host}' is missing a namespace (expected '{{namespace}}.{{module}}')"
            ),
        );
    };

    if module.contains('.') {
        return match classify_external_host(host, egress_allowed_domains) {
            Ok(host) => RouteOutcome::External {
                host,
                dest_uri: uri,
            },
            Err(message) => RouteOutcome::Reject(StatusCode::SERVICE_UNAVAILABLE, message),
        };
    }

    if let Err(error) = RouteKey::validate(namespace, module) {
        return RouteOutcome::Reject(
            StatusCode::BAD_REQUEST,
            format!("invalid internal destination '{host}': {error}"),
        );
    }

    let Some(group) = table.get(namespace, module) else {
        return match classify_external_host(host, egress_allowed_domains) {
            Ok(host) => RouteOutcome::External {
                host,
                dest_uri: uri,
            },
            Err(message) => RouteOutcome::Reject(StatusCode::SERVICE_UNAVAILABLE, message),
        };
    };

    match select_candidate(group, requested_version) {
        Some(selected) => RouteOutcome::Internal {
            namespace: group.namespace.clone(),
            module: group.module.clone(),
            namespace_header: group.namespace_header.clone(),
            module_header: group.module_header.clone(),
            selected,
        },
        None => {
            let message = match requested_version {
                RequestedVersion::Requirement { raw, .. } => format!(
                    "no route for module '{}.{}' matching version requirement '{}'",
                    group.namespace,
                    group.module,
                    raw.to_str().expect("validated version header")
                ),
                RequestedVersion::Unpinned => {
                    format!("no route for module '{}.{}'", group.namespace, group.module)
                }
            };
            RouteOutcome::Reject(StatusCode::SERVICE_UNAVAILABLE, message)
        }
    }
}

fn inject_routing_headers(
    req: &mut Request<ProxyBody>,
    namespace: HeaderValue,
    module: HeaderValue,
    version: HeaderValue,
) {
    let headers = req.headers_mut();
    headers.insert(WR_NAMESPACE, namespace);
    headers.insert(WR_MODULE, module);
    headers.insert(WR_VERSION, version);
}

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
        let egress_allowed_domains = self.egress_allowed_domains.clone();
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let requested_version = match parse_requested_version(req.headers().get(WR_VERSION)) {
                Ok(version) => version,
                Err(message) => {
                    return Ok(error_response(StatusCode::BAD_REQUEST, &message));
                }
            };
            let dest_uri = req
                .headers()
                .get(WR_DESTINATION)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse().ok());

            let outcome = {
                let snapshot = table.read().await;
                route_destination(
                    &snapshot,
                    &egress_allowed_domains,
                    dest_uri,
                    &requested_version,
                )
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
                    namespace_header,
                    module_header,
                    selected,
                } => {
                    span.record("wr.namespace", &*namespace);
                    span.record("wr.module", &*module);
                    span.record("wr.version", &*selected.version);
                    span.record("wr.engine", selected.route.destination.address());
                    inject_routing_headers(
                        &mut req,
                        namespace_header,
                        module_header,
                        selected.version_header,
                    );
                    req.extensions_mut().insert(selected.route);
                    inner.call(req).instrument(span).await
                }
                RouteOutcome::External { host, dest_uri } => {
                    req.extensions_mut()
                        .insert(ExternalEgress { host, dest_uri });
                    inner.call(req).instrument(span).await
                }
                RouteOutcome::Reject(status, message) => {
                    span.record("otel.status_code", "ERROR");
                    Ok(error_response(status, &message))
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::circuit_breaker::CircuitBreakerRegistry;
    use crate::config::CircuitBreakerConfig;
    use crate::indexed_routing::IndexedRoutingTable;
    use std::sync::Mutex;
    use wr_common::wruntime::{RoutingRule, RoutingTable};

    const SELF: &str = "http://self-peer";
    static SELECTOR_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn rule(ns: &str, module: &str, version: &str, address: &str) -> RoutingRule {
        RoutingRule {
            rule_id: format!("{ns}/{module}/{version}/{address}"),
            source_module: String::new(),
            destination_module: module.to_string(),
            engine_id: "e1".to_string(),
            engine_address: address.to_string(),
            destination_version: version.to_string(),
            healthy: true,
            source_namespace: String::new(),
            destination_namespace: ns.to_string(),
            peer_address: SELF.to_string(),
        }
    }

    fn table_with(rules: Vec<RoutingRule>) -> IndexedRoutingTable {
        IndexedRoutingTable::from_proto(
            &RoutingTable { rules, version: 1 },
            None,
            &CircuitBreakerRegistry::new(CircuitBreakerConfig::default()),
            SELF,
        )
    }

    fn uri(value: &str) -> Option<http::Uri> {
        value.parse().ok()
    }

    fn unpinned() -> RequestedVersion {
        RequestedVersion::Unpinned
    }

    fn required(value: &str) -> RequestedVersion {
        let _guard = SELECTOR_TEST_LOCK.lock().unwrap();
        parse_requested_version(Some(&HeaderValue::from_str(value).unwrap())).unwrap()
    }

    #[test]
    fn classification_preserves_internal_and_egress_boundaries() {
        let table = table_with(vec![rule("store", "inventory", "1.0.0", "http://engine")]);
        table.reset_lookup_calls();
        assert!(matches!(
            route_destination(&table, &[], uri("http://store.inventory/Ping"), &unpinned()),
            RouteOutcome::Internal { .. }
        ));
        assert_eq!(table.lookup_calls(), 1);
        assert!(matches!(
            route_destination(
                &table,
                &["example.com".into()],
                uri("http://example.com/x"),
                &unpinned()
            ),
            RouteOutcome::External { .. }
        ));
        assert!(matches!(
            route_destination(
                &table,
                &["*.openai.com".into()],
                uri("http://api.openai.com/v1"),
                &unpinned()
            ),
            RouteOutcome::External { .. }
        ));
        assert!(matches!(
            route_destination(&table, &[], uri("http://single/Ping"), &unpinned()),
            RouteOutcome::Reject(StatusCode::BAD_REQUEST, _)
        ));
        assert!(matches!(
            route_destination(
                &table,
                &["bad_name.svc".into()],
                uri("http://bad_name.svc/Ping"),
                &unpinned()
            ),
            RouteOutcome::Reject(StatusCode::BAD_REQUEST, _)
        ));
    }

    #[test]
    fn present_non_utf8_or_invalid_version_is_bad_request() {
        let _guard = SELECTOR_TEST_LOCK.lock().unwrap();
        SELECTOR_PARSE_CALLS.store(0, Ordering::Relaxed);
        let non_utf8 = HeaderValue::from_bytes(b"\xff").unwrap();
        assert!(parse_requested_version(Some(&non_utf8)).is_err());
        assert_eq!(SELECTOR_PARSE_CALLS.load(Ordering::Relaxed), 0);
        assert!(parse_requested_version(Some(&HeaderValue::from_static("latest"))).is_err());
        assert_eq!(SELECTOR_PARSE_CALLS.load(Ordering::Relaxed), 1);
        assert!(parse_requested_version(Some(&HeaderValue::from_static("^1"))).is_ok());
        assert_eq!(SELECTOR_PARSE_CALLS.load(Ordering::Relaxed), 2);
        assert!(parse_requested_version(None).is_ok());
        assert_eq!(SELECTOR_PARSE_CALLS.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn exact_and_range_choose_highest_satisfying_unique_group() {
        let table = table_with(vec![
            rule("ns", "svc", "1.0.0", "http://one"),
            rule("ns", "svc", "2.0.0", "http://two-a"),
            rule("ns", "svc", "2.0.0", "http://two-b"),
            rule("ns", "svc", "3.0.0", "http://three"),
        ]);
        let group = table.get("ns", "svc").unwrap();
        assert_eq!(
            select_candidate(group, &required("^2"))
                .unwrap()
                .version
                .as_ref(),
            "2.0.0"
        );
        assert_eq!(
            select_candidate(group, &required("1.0.0"))
                .unwrap()
                .version
                .as_ref(),
            "1.0.0"
        );
    }

    #[test]
    fn unpinned_spreads_across_all_versions() {
        let table = table_with(vec![
            rule("ns", "svc", "1.0.0", "http://one"),
            rule("ns", "svc", "2.0.0", "http://two"),
        ]);
        let group = table.get("ns", "svc").unwrap();
        let mut seen = std::collections::HashSet::new();
        for _ in 0..6 {
            seen.insert(select_candidate(group, &unpinned()).unwrap().version);
        }
        assert_eq!(seen.len(), 2);
    }

    #[test]
    fn open_replicas_are_skipped_and_large_groups_are_not_truncated() {
        let rules: Vec<_> = (0..65)
            .map(|index| rule("ns", "svc", "1.0.0", &format!("http://engine-{index}")))
            .collect();
        let table = table_with(rules);
        let group = table.get("ns", "svc").unwrap();
        for _ in 0..5 {
            group.candidates[0].breaker.on_error();
        }

        let mut seen = std::collections::HashSet::new();
        for _ in 0..128 {
            seen.insert(
                select_candidate(group, &required("1.0.0"))
                    .unwrap()
                    .route
                    .destination
                    .address()
                    .to_string(),
            );
        }
        assert!(!seen.contains("http://engine-0"));
        assert_eq!(seen.len(), 64);
    }

    #[test]
    fn all_open_still_returns_forwarding_candidate() {
        let table = table_with(vec![
            rule("ns", "svc", "1.0.0", "http://one"),
            rule("ns", "svc", "1.0.0", "http://two"),
        ]);
        let group = table.get("ns", "svc").unwrap();
        for candidate in &group.candidates {
            candidate.breaker.on_error();
        }
        assert!(select_candidate(group, &required("1.0.0")).is_some());
    }

    #[cfg(feature = "count-allocations")]
    #[test]
    fn direct_selection_core_is_allocation_free_for_eight_candidates() {
        let _guard = SELECTOR_TEST_LOCK.lock().unwrap();
        let table = table_with(
            (0..8)
                .map(|index| rule("ns", "svc", "1.0.0", &format!("http://engine-{index}")))
                .collect(),
        );
        let destination: http::Uri = "http://ns.svc/rpc?x=1".parse().unwrap();
        let version = HeaderValue::from_static("^1");
        let requested = parse_requested_version(Some(&version)).unwrap();
        std::hint::black_box(route_destination(
            &table,
            &[],
            Some(destination.clone()),
            &requested,
        ));
        allocation_counter::measure(|| {});
        table.reset_lookup_calls();
        SELECTOR_PARSE_CALLS.store(0, Ordering::Relaxed);

        let info = allocation_counter::measure(|| {
            let outcome = route_destination(&table, &[], Some(destination.clone()), &requested);
            std::hint::black_box(outcome);
        });
        assert_eq!(info.count_total, 0, "allocation info: {info:?}");
        assert_eq!(table.lookup_calls(), 1);
        assert_eq!(SELECTOR_PARSE_CALLS.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn prepared_headers_overwrite_caller_values() {
        let table = table_with(vec![rule("ns", "svc", "1.0.0", "http://engine")]);
        let group = table.get("ns", "svc").unwrap();
        let selected = select_candidate(group, &unpinned()).unwrap();
        let mut request = Request::builder()
            .header(WR_NAMESPACE, "spoofed")
            .header(WR_MODULE, "spoofed")
            .header(WR_VERSION, "^1")
            .body(ProxyBody::full(Vec::new()))
            .unwrap();
        inject_routing_headers(
            &mut request,
            group.namespace_header.clone(),
            group.module_header.clone(),
            selected.version_header,
        );
        assert_eq!(request.headers()[WR_NAMESPACE], "ns");
        assert_eq!(request.headers()[WR_MODULE], "svc");
        assert_eq!(request.headers()[WR_VERSION], "1.0.0");
    }
}
