use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use http::{Request, StatusCode};
use tower::{Layer, Service};
use tracing::{info_span, Instrument};

use super::egress::{domain_matches, ExternalEgress};
use super::{error_response, Destination, ProxyBody, ResBody, ResolvedDestination};
use crate::indexed_routing::ParsedRule;
use crate::routing::CachedRoutingTable;

/// A routing candidate with its resolved version attached so the caller can
/// inject the correct `x-wr-version` header after round-robin selection.
#[derive(Clone)]
struct VersionedCandidate {
    dest: Destination,
    version: Arc<str>,
}

type RoundRobinCounters = Arc<Mutex<HashMap<(Arc<str>, Arc<str>, Arc<str>), usize>>>;

pub struct RoutingLayer {
    table: CachedRoutingTable,
    /// This proxy's own address — used to distinguish local vs. remote rules.
    self_proxy_address: Arc<str>,
    /// Monotonic counters per (namespace, module, version) for round-robin selection.
    counters: RoundRobinCounters,
    /// Egress allowlist patterns. Only destinations matching one of these
    /// patterns are forwarded via egress; all other unroutable destinations
    /// get a 503. Empty means egress is disabled.
    egress_allowed_domains: Arc<Vec<String>>,
}

impl RoutingLayer {
    pub fn new(table: CachedRoutingTable, self_proxy_address: impl Into<String>) -> Self {
        Self {
            table,
            self_proxy_address: Arc::from(self_proxy_address.into()),
            counters: Arc::new(Mutex::new(HashMap::new())),
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
            self_proxy_address: self.self_proxy_address.clone(),
            counters: self.counters.clone(),
            egress_allowed_domains: self.egress_allowed_domains.clone(),
        }
    }
}

#[derive(Clone)]
pub struct RoutingService<S> {
    inner: S,
    table: CachedRoutingTable,
    self_proxy_address: Arc<str>,
    counters: RoundRobinCounters,
    egress_allowed_domains: Arc<Vec<String>>,
}

// ── Routing helpers ─────────────────────────────────────────────────────────

/// Parse the x-wr-destination header into (namespace, module) and the raw URI.
#[allow(clippy::type_complexity)]
fn parse_destination(
    headers: &http::HeaderMap,
) -> Result<(Arc<str>, Arc<str>, Option<http::Uri>), String> {
    let dest_uri: Option<http::Uri> = headers
        .get("x-wr-destination")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok());

    let host = dest_uri
        .as_ref()
        .and_then(|u: &http::Uri| u.host())
        .unwrap_or("");

    match host.split_once('.') {
        Some((ns, svc)) => Ok((Arc::from(ns), Arc::from(svc), dest_uri)),
        None => Err(format!(
            "destination host '{host}' must use the format \
             '{{namespace}}.{{service}}' — namespace is required"
        )),
    }
}

fn make_destination(
    rule: &wr_common::wruntime::RoutingRule,
    self_proxy_address: &str,
) -> Destination {
    if rule.proxy_address == self_proxy_address {
        Destination::LocalEngine(Arc::from(rule.engine_address.as_str()))
    } else {
        Destination::RemoteProxy(Arc::from(rule.proxy_address.as_str()))
    }
}

/// Resolve candidates from the indexed routing table.
///
/// When a version requirement is provided (`x-wr-version` header), returns only
/// candidates matching the highest satisfying semver version.
///
/// When no version is specified, returns **all** healthy candidates regardless
/// of version so traffic is spread across every available engine.  The first
/// element of the returned tuple is the round-robin key version: the resolved
/// version when pinned, or an empty string when unversioned (so all candidates
/// share a single counter).
async fn resolve_candidates(
    table: &CachedRoutingTable,
    namespace: &str,
    module: &str,
    requested_version: &Option<String>,
    self_proxy_address: &str,
) -> (Arc<str>, Vec<VersionedCandidate>) {
    let t = table.read().await;
    let rules = t.get(namespace, module);

    if rules.is_empty() {
        return (Arc::from(""), vec![]);
    }

    if let Some(ref version_str) = requested_version {
        // Filter the (already small) per-module group using pre-parsed versions.
        let req = semver::VersionReq::parse(version_str).ok();
        let satisfying: Vec<&ParsedRule> = rules
            .iter()
            .filter(|r| match (&req, &r.parsed_version) {
                (Some(req), Some(v)) => req.matches(v),
                (None, _) => r.rule.destination_version == *version_str,
                _ => false,
            })
            .collect();

        if satisfying.is_empty() {
            return (Arc::from(version_str.as_str()), vec![]);
        }

        // satisfying is already sorted descending (inherits order from the index).
        let best_ver = &satisfying[0].rule.destination_version;
        let cands = satisfying
            .iter()
            .take_while(|r| r.rule.destination_version == *best_ver)
            .map(|r| VersionedCandidate {
                dest: make_destination(&r.rule, self_proxy_address),
                version: Arc::from(best_ver.as_str()),
            })
            .collect();
        (Arc::from(best_ver.as_str()), cands)
    } else {
        // No version requested — load-balance across all healthy candidates
        // regardless of version.
        let cands = rules
            .iter()
            .map(|r| VersionedCandidate {
                dest: make_destination(&r.rule, self_proxy_address),
                version: Arc::from(r.rule.destination_version.as_str()),
            })
            .collect();
        (Arc::from(""), cands)
    }
}

/// Pick a candidate via round-robin from the counter map.
fn select_round_robin(
    counters: &RoundRobinCounters,
    key: (Arc<str>, Arc<str>, Arc<str>),
    candidates: &[VersionedCandidate],
) -> VersionedCandidate {
    let mut map = counters.lock().unwrap();
    let counter = map.entry(key).or_insert(0);
    let idx = *counter % candidates.len();
    *counter = counter.wrapping_add(1);
    candidates[idx].clone()
}

/// Inject x-wr-namespace, x-wr-module, and x-wr-version headers.
fn inject_routing_headers(
    req: &mut Request<ProxyBody>,
    namespace: &str,
    module: &str,
    version: &str,
) {
    if let Ok(v) = http::HeaderValue::from_str(namespace) {
        req.headers_mut().insert("x-wr-namespace", v);
    }
    if !module.is_empty() {
        if let Ok(v) = http::HeaderValue::from_str(module) {
            req.headers_mut().insert("x-wr-module", v);
        }
    }
    if let Ok(v) = http::HeaderValue::from_str(version) {
        req.headers_mut().insert("x-wr-version", v);
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
        let counters = self.counters.clone();
        let self_proxy_address = self.self_proxy_address.clone();
        let egress_allowed_domains = self.egress_allowed_domains.clone();
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let (dest_namespace, module_name, dest_uri) = match parse_destination(req.headers()) {
                Ok(v) => v,
                Err(msg) => return Ok(error_response(StatusCode::BAD_REQUEST, &msg)),
            };

            let span = info_span!(
                "proxy.route",
                wr.module        = %module_name,
                wr.namespace     = %dest_namespace,
                wr.version       = tracing::field::Empty,
                wr.engine        = tracing::field::Empty,
                otel.status_code = tracing::field::Empty,
            );

            let requested_version: Option<String> = req
                .headers()
                .get("x-wr-version")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());

            let (resolved_version, candidates) = resolve_candidates(
                &table,
                &dest_namespace,
                &module_name,
                &requested_version,
                &self_proxy_address,
            )
            .await;

            if candidates.is_empty() {
                // Check egress allowlist before returning 503.
                let egress_host = dest_uri
                    .as_ref()
                    .and_then(|u| u.host())
                    .map(|h| h.to_ascii_lowercase());

                let egress_allowed = egress_host.as_ref().is_some_and(|host| {
                    egress_allowed_domains
                        .iter()
                        .any(|pattern| domain_matches(pattern, host))
                });

                if egress_allowed {
                    let host = egress_host.unwrap();
                    req.extensions_mut().insert(ExternalEgress {
                        host,
                        dest_uri: dest_uri.unwrap().clone(),
                    });
                    return inner.call(req).instrument(span).await;
                }

                let msg = match requested_version {
                    Some(v) => format!(
                        "no route for module '{module_name}.{dest_namespace}' matching version requirement '{v}'"
                    ),
                    None => format!("no route for module '{module_name}.{dest_namespace}'"),
                };
                span.record("otel.status_code", "ERROR");
                return Ok(error_response(StatusCode::SERVICE_UNAVAILABLE, &msg));
            }

            let chosen = select_round_robin(
                &counters,
                (
                    dest_namespace.clone(),
                    module_name.clone(),
                    resolved_version.clone(),
                ),
                &candidates,
            );
            let first_addr = chosen.dest.address();

            // Use the selected candidate's version for the header — this
            // matters when no version was requested and candidates span
            // multiple versions.
            inject_routing_headers(&mut req, &dest_namespace, &module_name, &chosen.version);
            span.record("wr.version", &*chosen.version);
            span.record("wr.engine", first_addr);

            req.extensions_mut()
                .insert(ResolvedDestination(chosen.dest));
            inner.call(req).instrument(span).await
        })
    }
}
