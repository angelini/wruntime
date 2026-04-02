use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use http::Request;
use hyper_util::client::legacy::{connect::HttpConnector, Client};
use hyper_util::rt::TokioExecutor;
use tower::Service;
use tracing::{info_span, warn, Instrument};

use super::{Destination, ProxyBody, ResBody, ResolvedDestination};
use crate::circuit_breaker::CircuitBreakerRegistry;

#[derive(Clone)]
pub struct ForwardService {
    client: Client<HttpConnector, ProxyBody>,
    cb_registry: Arc<CircuitBreakerRegistry>,
}

impl ForwardService {
    pub fn new(cb_registry: Arc<CircuitBreakerRegistry>) -> Self {
        let client = Client::builder(TokioExecutor::new())
            .http2_only(true)
            .build_http();
        Self {
            client,
            cb_registry,
        }
    }
}

impl Service<Request<ProxyBody>> for ForwardService {
    type Response = http::Response<ResBody>;
    type Error = anyhow::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<ProxyBody>) -> Self::Future {
        let client = self.client.clone();
        let cb_registry = self.cb_registry.clone();

        Box::pin(async move {
            let destination = req
                .extensions()
                .get::<ResolvedDestination>()
                .map(|d| d.0.clone())
                .ok_or_else(|| anyhow::anyhow!("missing ResolvedDestination extension"))?;

            let path = req
                .uri()
                .path_and_query()
                .map(|pq: &http::uri::PathAndQuery| pq.as_str())
                .unwrap_or("/")
                .to_owned();

            let (mut parts, body) = req.into_parts();

            let forward_addr = match &destination {
                Destination::LocalEngine(addr) => {
                    parts.headers.remove("x-wr-destination");
                    parts.headers.remove("x-wr-source");
                    parts.headers.remove("x-wr-source-ns");
                    parts.headers.remove("x-wr-via-proxy");
                    addr.clone()
                }
                Destination::RemoteProxy(addr) => {
                    parts
                        .headers
                        .insert("x-wr-via-proxy", http::HeaderValue::from_static("1"));
                    addr.clone()
                }
            };

            let forward_uri: http::Uri =
                format!("{}{}", forward_addr.trim_end_matches('/'), path).parse()?;
            parts.uri = forward_uri;
            wr_common::telemetry::inject_context(&mut parts.headers);
            let forward_req = Request::from_parts(parts, body);

            let span = info_span!(
                "proxy.forward",
                wr.engine                 = %forward_addr,
                http.response.status_code = tracing::field::Empty,
                otel.status_code          = tracing::field::Empty,
            );

            let cb = cb_registry.get_or_create(&forward_addr);

            // Check circuit breaker before forwarding.
            if !cb.is_call_permitted() {
                warn!(parent: &span, engine = %forward_addr, "circuit open");
                span.record("otel.status_code", "circuit_open");
                let mut resp =
                    super::error_response(http::StatusCode::SERVICE_UNAVAILABLE, "circuit open");
                let secs = cb_registry.open_duration_secs();
                if let Ok(val) = http::HeaderValue::from_str(&secs.to_string()) {
                    resp.headers_mut().insert(http::header::RETRY_AFTER, val);
                }
                return Ok(resp);
            }

            let result = async {
                client
                    .request(forward_req)
                    .await
                    .map_err(|e| anyhow::anyhow!("forward failed: {e}"))
            }
            .instrument(span.clone())
            .await;

            match result {
                Ok(resp) => {
                    let (resp_parts, resp_body) = resp.into_parts();
                    let status = resp_parts.status.as_u16();
                    span.record("http.response.status_code", status);

                    // Record failure for circuit breaker on 5xx/429, but still
                    // pass the original response through to the caller.
                    if resp_parts.status.is_server_error()
                        || resp_parts.status == http::StatusCode::TOO_MANY_REQUESTS
                    {
                        cb.on_error();
                        span.record("otel.status_code", "ERROR");
                    } else {
                        cb.on_success();
                        span.record("otel.status_code", "OK");
                    }

                    Ok(http::Response::from_parts(
                        resp_parts,
                        ProxyBody::streaming(resp_body),
                    ))
                }
                Err(e) => {
                    cb.on_error();
                    span.record("otel.status_code", "ERROR");
                    Ok(super::error_response(
                        http::StatusCode::SERVICE_UNAVAILABLE,
                        &format!("forward failed: {e}"),
                    ))
                }
            }
        })
    }
}
