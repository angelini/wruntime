use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use http::{Request, StatusCode};
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::{connect::HttpConnector, Client};
use hyper_util::rt::TokioExecutor;
use tower::{Layer, Service};
use tracing::{info_span, Instrument};

use super::{full_body, ProxyBody, ResBody};
use crate::config::EgressConfig;

type EgressClient = Client<HttpsConnector<HttpConnector>, ProxyBody>;

/// Resolved by [`super::RoutingLayer`] when the destination is not an internal
/// module and egress is configured. Consumed by [`EgressService`] to forward
/// the request to the external host.
#[derive(Clone)]
pub struct ExternalEgress {
    pub host: String,
    pub dest_uri: http::Uri,
}

pub struct EgressLayer {
    config: Option<EgressConfig>,
    client: Arc<EgressClient>,
}

impl EgressLayer {
    pub fn new(config: Option<EgressConfig>) -> Self {
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
            client: self.client.clone(),
        }
    }
}

#[derive(Clone)]
pub struct EgressService<S> {
    inner: S,
    config: Option<EgressConfig>,
    client: Arc<EgressClient>,
}

impl<S> Service<Request<ProxyBody>> for EgressService<S>
where
    S: Service<Request<ProxyBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Error: Into<anyhow::Error> + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = http::Response<ResBody>;
    type Error = anyhow::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: Request<ProxyBody>) -> Self::Future {
        // If this request has an ExternalEgress extension, RoutingLayer already
        // determined it is not an internal module. Handle the external forward
        // without touching the inner stack (ForwardService).
        if let Some(egress) = req.extensions().get::<ExternalEgress>().cloned() {
            let config = self.config.clone();
            let client = self.client.clone();

            return Box::pin(async move {
                let egress_cfg = match config {
                    Some(c) => c,
                    None => {
                        return Ok(super::error_response(
                            StatusCode::SERVICE_UNAVAILABLE,
                            "egress not configured",
                        ));
                    }
                };

                let host = &egress.host;

                // Enforce the allowlist.
                let allowed = egress_cfg
                    .allowed_domains
                    .iter()
                    .any(|pattern| domain_matches(pattern, host));

                if !allowed {
                    let body = serde_json::json!({
                        "error": "egress_not_allowed",
                        "detail": format!("domain '{host}' is not in the egress allowlist"),
                    })
                    .to_string();
                    return Ok(http::Response::builder()
                        .status(StatusCode::FORBIDDEN)
                        .header(http::header::CONTENT_TYPE, "application/json")
                        .body(full_body(bytes::Bytes::from(body)))
                        .unwrap());
                }

                // Strip all x-wr-* headers before forwarding to the external host.
                let (mut parts, body) = req.into_parts();

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

                parts.uri = egress.dest_uri.clone();
                // Reset the HTTP version so the client can negotiate freely via
                // ALPN (HTTPS) or default to HTTP/1.1 (plain HTTP).
                parts.version = http::Version::HTTP_11;
                wr_common::telemetry::inject_context(&mut parts.headers);

                let egress_req = Request::from_parts(parts, body);

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

                Ok(http::Response::from_parts(
                    resp_parts,
                    ProxyBody::streaming(resp_body),
                ))
            });
        }

        // Internal request — pass through to ForwardService.
        let mut inner = self.inner.clone();
        Box::pin(async move { inner.call(req).await.map_err(Into::into) })
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
