use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use http::{Request, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::{connect::HttpConnector, Client};
use hyper_util::rt::TokioExecutor;
use tower::{Layer, Service};
use tracing::{info_span, Instrument};

use super::{full_body, ResBody};
use crate::config::EgressConfig;
use crate::routing::CachedRoutingTable;

type EgressClient = Client<HttpsConnector<HttpConnector>, Full<Bytes>>;

pub struct EgressLayer {
    config: Option<EgressConfig>,
    table: CachedRoutingTable,
    client: Arc<EgressClient>,
}

impl EgressLayer {
    pub fn new(config: Option<EgressConfig>, table: CachedRoutingTable) -> Self {
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_native_roots()
            .expect("failed to load native TLS roots")
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .build();
        let client = Client::builder(TokioExecutor::new()).build(https);
        Self {
            config,
            table,
            client: Arc::new(client),
        }
    }
}

impl<S> Layer<S> for EgressLayer {
    type Service = EgressService<S>;
    fn layer(&self, inner: S) -> EgressService<S> {
        EgressService {
            inner,
            config: self.config.clone(),
            table: self.table.clone(),
            client: self.client.clone(),
        }
    }
}

#[derive(Clone)]
pub struct EgressService<S> {
    inner: S,
    config: Option<EgressConfig>,
    table: CachedRoutingTable,
    client: Arc<EgressClient>,
}

impl<S> Service<Request<Bytes>> for EgressService<S>
where
    S: Service<Request<Bytes>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Error: Into<anyhow::Error> + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = http::Response<ResBody>;
    type Error = anyhow::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: Request<Bytes>) -> Self::Future {
        let config = self.config.clone();
        let table = self.table.clone();
        let client = self.client.clone();
        let mut inner = self.inner.clone();

        Box::pin(async move {
            // If egress is not configured, pass through unchanged.
            let egress_cfg = match config {
                Some(c) => c,
                None => return inner.call(req).await.map_err(Into::into),
            };

            // Parse x-wr-destination to find the intended host.
            let dest_uri: http::Uri = match req
                .headers()
                .get("x-wr-destination")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse().ok())
            {
                Some(u) => u,
                None => return inner.call(req).await.map_err(Into::into),
            };

            let host = match dest_uri.host() {
                Some(h) => h.to_ascii_lowercase(),
                None => return inner.call(req).await.map_err(Into::into),
            };

            // Check the routing table: if this host matches any registered module
            // it is an internal call — let it flow through to SchemaValidationLayer
            // and RoutingLayer as normal.
            let is_internal = {
                let t = table.read().await;
                match host.split_once('.') {
                    Some((ns, module)) => t
                        .rules
                        .iter()
                        .any(|r| r.destination_namespace == ns && r.destination_module == module),
                    None => false,
                }
            };

            if is_internal {
                return inner.call(req).await.map_err(Into::into);
            }

            // External request — enforce the allowlist.
            let allowed = egress_cfg
                .allowed_domains
                .iter()
                .any(|pattern| domain_matches(pattern, &host));

            if !allowed {
                let body = serde_json::json!({
                    "error": "egress_not_allowed",
                    "detail": format!("domain '{}' is not in the egress allowlist", host),
                })
                .to_string();
                return Ok(http::Response::builder()
                    .status(StatusCode::FORBIDDEN)
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .body(full_body(Bytes::from(body)))
                    .unwrap());
            }

            // Allowed: strip all x-wr-* headers before forwarding to the external host.
            let (mut parts, body_bytes) = req.into_parts();

            let source = parts
                .headers
                .get("x-wr-source")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("unknown")
                .to_string();

            for name in &[
                "x-wr-destination",
                "x-wr-source",
                "x-wr-source-ns",
                "x-wr-module",
                "x-wr-namespace",
                "x-wr-version",
                "x-wr-via-proxy",
            ] {
                parts.headers.remove(*name);
            }

            parts.uri = dest_uri.clone();
            wr_common::telemetry::inject_context(&mut parts.headers);

            let egress_req = Request::from_parts(parts, Full::new(body_bytes));

            let span = info_span!(
                "proxy.egress",
                wr.source                 = %source,
                egress.host               = %host,
                http.response.status_code = tracing::field::Empty,
                otel.status_code          = tracing::field::Empty,
            );

            let resp = client
                .request(egress_req)
                .instrument(span.clone())
                .await
                .map_err(|e| anyhow::anyhow!("egress forward failed: {e}"))?;

            let (resp_parts, resp_body) = resp.into_parts();
            span.record("http.response.status_code", resp_parts.status.as_u16());
            span.record("otel.status_code", "OK");

            let resp_bytes = resp_body
                .collect()
                .await
                .map_err(|e| anyhow::anyhow!("egress response body error: {e}"))?
                .to_bytes();

            Ok(http::Response::from_parts(
                resp_parts,
                full_body(resp_bytes),
            ))
        })
    }
}

/// Returns `true` if `host` matches `pattern` (case-insensitive).
///
/// `*` matches exactly one DNS label:
/// - `*.openai.com` matches `api.openai.com` ✓
/// - `*.openai.com` does NOT match `openai.com` or `a.b.openai.com`
fn domain_matches(pattern: &str, host: &str) -> bool {
    let pattern = pattern.to_ascii_lowercase();
    let host = host.to_ascii_lowercase();

    if !pattern.contains('*') {
        return pattern == host;
    }

    let suffix = match pattern.strip_prefix("*.") {
        Some(s) => s,
        None => return false,
    };

    match host.strip_suffix(suffix) {
        Some(prefix) => {
            // prefix must be exactly one label followed by '.', e.g. "api."
            prefix.len() >= 2 && prefix.ends_with('.') && !prefix[..prefix.len() - 1].contains('.')
        }
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::domain_matches;

    #[test]
    fn wildcard_single_label() {
        assert!(domain_matches("*.openai.com", "api.openai.com"));
        assert!(domain_matches("*.openai.com", "v1.openai.com"));
    }

    #[test]
    fn wildcard_does_not_match_apex() {
        assert!(!domain_matches("*.openai.com", "openai.com"));
    }

    #[test]
    fn wildcard_does_not_match_multi_label_subdomain() {
        assert!(!domain_matches("*.openai.com", "a.b.openai.com"));
    }

    #[test]
    fn exact_match() {
        assert!(domain_matches("api.github.com", "api.github.com"));
        assert!(!domain_matches("api.github.com", "v3.api.github.com"));
    }

    #[test]
    fn case_insensitive() {
        assert!(domain_matches("*.OpenAI.COM", "API.openai.com"));
    }

    #[test]
    fn no_partial_label_match() {
        assert!(!domain_matches("*.openai.com", "evilopenai.com"));
    }
}
