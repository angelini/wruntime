use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::Bytes;
use http::{Request, StatusCode};
use tower::{Layer, Service};

use super::{error_response, ResBody, ResolvedDestination};
use crate::routing::CachedRoutingTable;

pub struct RoutingLayer {
    table: CachedRoutingTable,
    /// Monotonic counters per (module, version) for round-robin selection.
    counters: Arc<Mutex<HashMap<(String, String), usize>>>,
}

impl RoutingLayer {
    pub fn new(table: CachedRoutingTable) -> Self {
        Self {
            table,
            counters: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<S> Layer<S> for RoutingLayer {
    type Service = RoutingService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        RoutingService {
            inner,
            table: self.table.clone(),
            counters: self.counters.clone(),
        }
    }
}

#[derive(Clone)]
pub struct RoutingService<S> {
    inner: S,
    table: CachedRoutingTable,
    counters: Arc<Mutex<HashMap<(String, String), usize>>>,
}

impl<S> Service<Request<Bytes>> for RoutingService<S>
where
    S: Service<Request<Bytes>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Error: Send + 'static,
    S::Future: Send + 'static,
{
    type Response = http::Response<ResBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request<Bytes>) -> Self::Future {
        let table = self.table.clone();
        let counters = self.counters.clone();
        let mut inner = self.inner.clone();

        Box::pin(async move {
            // Extract destination module name from the x-wr-destination host
            let dest_uri: Option<http::Uri> = req
                .headers()
                .get("x-wr-destination")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse().ok());

            let module_name = dest_uri
                .as_ref()
                .and_then(|u: &http::Uri| u.host())
                .unwrap_or("")
                .to_string();

            // Optional explicit version requested by the caller
            let requested_version: Option<String> = req
                .headers()
                .get("x-wr-version")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());

            // Collect healthy candidate addresses and resolve the version string.
            // Multiple rules for the same (module, version) are all candidates.
            let (resolved_version, candidate_addrs) = {
                let t = table.read().await;
                let healthy: Vec<_> = t
                    .rules
                    .iter()
                    .filter(|r| r.destination_module == module_name && r.healthy)
                    .collect();

                if let Some(ref version) = requested_version {
                    // Exact version match across all healthy rules
                    let addrs: Vec<String> = healthy
                        .iter()
                        .filter(|r| r.destination_version == *version)
                        .map(|r| r.engine_address.clone())
                        .collect();
                    (version.clone(), addrs)
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
                            let addrs: Vec<String> = healthy
                                .iter()
                                .filter(|r| r.destination_version == ver)
                                .map(|r| r.engine_address.clone())
                                .collect();
                            (ver, addrs)
                        }
                        None => (String::new(), vec![]),
                    }
                }
            };

            if candidate_addrs.is_empty() {
                let msg = match requested_version {
                    Some(v) => format!("no route for module '{module_name}' version '{v}'"),
                    None => format!("no route for module '{module_name}'"),
                };
                return Ok(error_response(StatusCode::SERVICE_UNAVAILABLE, &msg));
            }

            // Round-robin across candidates using a per-(module, version) counter
            let addr = {
                let mut map = counters.lock().unwrap();
                let counter = map
                    .entry((module_name.clone(), resolved_version.clone()))
                    .or_insert(0);
                let idx = *counter % candidate_addrs.len();
                *counter = counter.wrapping_add(1);
                candidate_addrs[idx].clone()
            };

            // Inject x-wr-module and x-wr-version so the destination engine
            // knows which WASM module and version to dispatch to.
            if !module_name.is_empty() {
                if let Ok(v) = http::HeaderValue::from_str(&module_name) {
                    req.headers_mut().insert("x-wr-module", v);
                }
            }
            if let Ok(v) = http::HeaderValue::from_str(&resolved_version) {
                req.headers_mut().insert("x-wr-version", v);
            }
            req.extensions_mut().insert(ResolvedDestination(addr));
            inner.call(req).await
        })
    }
}
