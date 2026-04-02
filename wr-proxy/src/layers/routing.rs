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
use crate::routing::CachedRoutingTable;

type RoundRobinCounters = Arc<Mutex<HashMap<(String, String, String), usize>>>;

pub struct RoutingLayer {
    table: CachedRoutingTable,
    /// This proxy's own address — used to distinguish local vs. remote rules.
    self_proxy_address: String,
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
            self_proxy_address: self_proxy_address.into(),
            counters: Arc::new(Mutex::new(HashMap::new())),
            egress_allowed_domains: Arc::new(Vec::new()),
        }
    }

    pub fn with_egress(mut self, allowed_domains: Vec<String>) -> Self {
        self.egress_allowed_domains = Arc::new(allowed_domains);
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
    self_proxy_address: String,
    counters: RoundRobinCounters,
    egress_allowed_domains: Arc<Vec<String>>,
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
        let counters = self.counters.clone();
        let self_proxy_address = self.self_proxy_address.clone();
        let egress_allowed_domains = self.egress_allowed_domains.clone();
        let mut inner = self.inner.clone();

        Box::pin(async move {
            // Extract destination module name and namespace from x-wr-destination.
            // Expected host format: "{namespace}.{service}"
            let dest_uri: Option<http::Uri> = req
                .headers()
                .get("x-wr-destination")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse().ok());

            let host = dest_uri
                .as_ref()
                .and_then(|u: &http::Uri| u.host())
                .unwrap_or("");

            let (dest_namespace, module_name) = match host.split_once('.') {
                Some((ns, svc)) => (ns.to_string(), svc.to_string()),
                None => {
                    let msg = format!(
                        "destination host '{host}' must use the format \
                         '{{namespace}}.{{service}}' — namespace is required"
                    );
                    return Ok(error_response(StatusCode::BAD_REQUEST, &msg));
                }
            };

            let span = info_span!(
                "proxy.route",
                wr.module        = %module_name,
                wr.namespace     = %dest_namespace,
                wr.version       = tracing::field::Empty,
                wr.engine        = tracing::field::Empty,
                otel.status_code = tracing::field::Empty,
            );

            // Optional explicit version requested by the caller
            let requested_version: Option<String> = req
                .headers()
                .get("x-wr-version")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());

            // Collect healthy candidates and resolve the version string.
            // Each candidate is a Destination (local engine or remote proxy).
            let (resolved_version, candidates) = {
                let t = table.read().await;
                let healthy: Vec<_> = t
                    .rules
                    .iter()
                    .filter(|r| {
                        r.destination_module == module_name
                            && r.destination_namespace == dest_namespace
                            && r.healthy
                    })
                    .collect();

                let make_destination = |r: &&wr_common::wruntime::RoutingRule| {
                    if r.proxy_address == self_proxy_address {
                        Destination::LocalEngine(r.engine_address.clone())
                    } else {
                        Destination::RemoteProxy(r.proxy_address.clone())
                    }
                };

                if let Some(ref version_str) = requested_version {
                    // Parse as a semver VersionReq (supports ^1, ~1.2, >=1.0 <2.0, exact 1.2.3, etc.)
                    let req = semver::VersionReq::parse(version_str).ok();

                    let satisfying: Vec<_> = healthy
                        .iter()
                        .filter(
                            |r| match (&req, semver::Version::parse(&r.destination_version)) {
                                (Some(req), Ok(v)) => req.matches(&v),
                                // Unparseable range header: fall back to exact string match
                                (None, _) => r.destination_version == *version_str,
                                // Rule version isn't valid semver: skip
                                _ => false,
                            },
                        )
                        .collect();

                    // Among satisfying rules, pick the highest semver
                    let best = satisfying
                        .iter()
                        .copied()
                        .max_by(|a, b| {
                            let va = semver::Version::parse(&a.destination_version);
                            let vb = semver::Version::parse(&b.destination_version);
                            match (va, vb) {
                                (Ok(a), Ok(b)) => a.cmp(&b),
                                (Ok(_), Err(_)) => std::cmp::Ordering::Greater,
                                (Err(_), Ok(_)) => std::cmp::Ordering::Less,
                                _ => a.destination_version.cmp(&b.destination_version),
                            }
                        })
                        .map(|r| r.destination_version.clone());

                    match best {
                        Some(ver) => {
                            let cands: Vec<Destination> = satisfying
                                .iter()
                                .copied()
                                .filter(|r| r.destination_version == ver)
                                .map(make_destination)
                                .collect();
                            (ver, cands)
                        }
                        None => (version_str.clone(), vec![]),
                    }
                } else {
                    // Find highest semver, then collect all rules at that version
                    let best = healthy
                        .iter()
                        .max_by(|a, b| {
                            let va = semver::Version::parse(&a.destination_version);
                            let vb = semver::Version::parse(&b.destination_version);
                            match (va, vb) {
                                (Ok(a), Ok(b)) => a.cmp(&b),
                                (Ok(_), Err(_)) => std::cmp::Ordering::Greater,
                                (Err(_), Ok(_)) => std::cmp::Ordering::Less,
                                _ => a.destination_version.cmp(&b.destination_version),
                            }
                        })
                        .map(|r| r.destination_version.clone());

                    match best {
                        Some(ver) => {
                            let cands: Vec<Destination> = healthy
                                .iter()
                                .filter(|r| r.destination_version == ver)
                                .map(make_destination)
                                .collect();
                            (ver, cands)
                        }
                        None => (String::new(), vec![]),
                    }
                }
            };

            if candidates.is_empty() {
                // Only forward to egress if the destination host explicitly
                // matches the egress allowlist. Unroutable internal
                // destinations (typos, unregistered modules) stay as 503.
                if !egress_allowed_domains.is_empty() {
                    if let Some(ref uri) = dest_uri {
                        if let Some(h) = uri.host() {
                            let host = h.to_ascii_lowercase();
                            let allowed = egress_allowed_domains
                                .iter()
                                .any(|pattern| domain_matches(pattern, &host));
                            if allowed {
                                req.extensions_mut().insert(ExternalEgress {
                                    host,
                                    dest_uri: uri.clone(),
                                });
                                return inner.call(req).instrument(span).await;
                            }
                        }
                    }
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

            // Round-robin: pick one candidate.
            let chosen = {
                let mut map = counters.lock().unwrap();
                let counter = map
                    .entry((
                        dest_namespace.clone(),
                        module_name.clone(),
                        resolved_version.clone(),
                    ))
                    .or_insert(0);
                let idx = *counter % candidates.len();
                *counter = counter.wrapping_add(1);
                candidates[idx].clone()
            };

            let first_addr = match &chosen {
                Destination::LocalEngine(a) => a.clone(),
                Destination::RemoteProxy(a) => a.clone(),
            };

            // Inject x-wr-namespace, x-wr-module, and x-wr-version so the
            // destination engine (or peer proxy's routing layer) knows which
            // WASM module and version to dispatch to.
            if let Ok(v) = http::HeaderValue::from_str(&dest_namespace) {
                req.headers_mut().insert("x-wr-namespace", v);
            }
            if !module_name.is_empty() {
                if let Ok(v) = http::HeaderValue::from_str(&module_name) {
                    req.headers_mut().insert("x-wr-module", v);
                }
            }
            if let Ok(v) = http::HeaderValue::from_str(&resolved_version) {
                req.headers_mut().insert("x-wr-version", v);
            }
            span.record("wr.version", &resolved_version);
            span.record("wr.engine", &first_addr);

            req.extensions_mut().insert(ResolvedDestination(chosen));
            inner.call(req).instrument(span).await
        })
    }
}
