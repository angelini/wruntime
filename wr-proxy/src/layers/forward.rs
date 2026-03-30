use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use failsafe::futures::CircuitBreaker as _;
use http::Request;
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::{connect::HttpConnector, Client};
use hyper_util::rt::TokioExecutor;
use tower::Service;
use tracing::{info_span, warn, Instrument};

use super::{full_body, Destination, ResBody, ResolvedDestination};
use crate::circuit_breaker::CircuitBreakerRegistry;

/// Error type for the inner circuit-breaker closure.
///
/// Separating overload (429 / 5xx) from transport errors lets us carry the
/// engine's `Retry-After` value back out of the closure even though we must
/// return `Err` so the breaker records the failure.
enum CandidateError {
    /// The engine responded with 429 or 5xx.
    Overload { retry_after: Option<u64> },
    /// Network / transport failure.
    Transport(#[allow(dead_code)] anyhow::Error),
}

impl From<anyhow::Error> for CandidateError {
    fn from(e: anyhow::Error) -> Self {
        Self::Transport(e)
    }
}

/// Parses the `Retry-After` header as delta-seconds, ignoring HTTP-date values.
fn parse_retry_after(headers: &http::HeaderMap) -> Option<u64> {
    headers
        .get(http::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
}

#[derive(Clone)]
pub struct ForwardService {
    client: Client<HttpConnector, Full<Bytes>>,
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

impl Service<Request<Bytes>> for ForwardService {
    type Response = http::Response<ResBody>;
    type Error = anyhow::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Bytes>) -> Self::Future {
        let client = self.client.clone();
        let cb_registry = self.cb_registry.clone();

        Box::pin(async move {
            // Read the ordered candidate list injected by RoutingLayer (up to 3 entries).
            let candidates = req
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

            let (parts, body) = req.into_parts();
            let body_bytes = body; // already Bytes — clone cheaply for retries

            let mut all_open = true;
            // Smallest Retry-After seen across all candidates. We take the minimum
            // so the caller retries as soon as *any* engine might be available.
            let mut retry_after_secs: Option<u64> = None;

            for destination in &candidates {
                let mut headers = parts.headers.clone();

                let forward_addr = match destination {
                    Destination::LocalEngine(addr) => {
                        // Strip internal routing headers — engine doesn't need them.
                        // (x-wr-module, x-wr-namespace, x-wr-version are kept so the
                        //  engine can dispatch to the correct WASM instance)
                        headers.remove("x-wr-destination");
                        headers.remove("x-wr-source");
                        headers.remove("x-wr-source-ns");
                        headers.remove("x-wr-via-proxy");
                        addr.clone()
                    }
                    Destination::RemoteProxy(addr) => {
                        // Preserve x-wr-destination so the peer proxy can route.
                        // Mark as a proxy hop to suppress re-validation on the peer.
                        headers.insert("x-wr-via-proxy", http::HeaderValue::from_static("1"));
                        addr.clone()
                    }
                };

                let forward_uri: http::Uri =
                    format!("{}{}", forward_addr.trim_end_matches('/'), path).parse()?;

                let mut fwd_parts = parts.clone();
                fwd_parts.uri = forward_uri;
                fwd_parts.headers = headers;
                wr_common::telemetry::inject_context(&mut fwd_parts.headers);
                let forward_req = Request::from_parts(fwd_parts, Full::new(body_bytes.clone()));

                let span = info_span!(
                    "proxy.forward",
                    wr.engine                 = %forward_addr,
                    http.response.status_code = tracing::field::Empty,
                    otel.status_code          = tracing::field::Empty,
                );

                let cb = cb_registry.get_or_create(&forward_addr);

                let outcome = cb
                    .call(
                        async {
                            let resp = client
                                .request(forward_req)
                                .await
                                .map_err(|e| anyhow::anyhow!("forward failed: {e}"))?;

                            let (resp_parts, resp_body) = resp.into_parts();
                            let resp_bytes = resp_body
                                .collect()
                                .await
                                .map_err(|e| anyhow::anyhow!("response body error: {e}"))?
                                .to_bytes();

                            // Treat server errors and 429 as failures so the breaker
                            // tracks them — the caller will try the next candidate.
                            if resp_parts.status.is_server_error()
                                || resp_parts.status == http::StatusCode::TOO_MANY_REQUESTS
                            {
                                return Err(CandidateError::Overload {
                                    retry_after: parse_retry_after(&resp_parts.headers),
                                });
                            }

                            Ok::<_, CandidateError>((resp_parts, resp_bytes))
                        }
                        .instrument(span.clone()),
                    )
                    .await;

                match outcome {
                    Ok((resp_parts, resp_bytes)) => {
                        let status = resp_parts.status.as_u16();
                        span.record("http.response.status_code", status);
                        span.record("otel.status_code", "OK");
                        return Ok(http::Response::from_parts(
                            resp_parts,
                            full_body(resp_bytes),
                        ));
                    }
                    Err(failsafe::Error::Rejected) => {
                        // Circuit is open — skip without making a real request.
                        warn!(parent: &span, engine = %forward_addr, "circuit open, skipping candidate");
                        span.record("otel.status_code", "circuit_open");
                        // Use open_duration_secs as a conservative upper bound on when
                        // the circuit might probe again.
                        let secs = cb_registry.open_duration_secs();
                        retry_after_secs = Some(match retry_after_secs {
                            Some(existing) => existing.min(secs),
                            None => secs,
                        });
                    }
                    Err(failsafe::Error::Inner(CandidateError::Overload { retry_after })) => {
                        span.record("otel.status_code", "ERROR");
                        all_open = false;
                        if let Some(secs) = retry_after {
                            retry_after_secs = Some(match retry_after_secs {
                                Some(existing) => existing.min(secs),
                                None => secs,
                            });
                        }
                    }
                    Err(failsafe::Error::Inner(CandidateError::Transport(_))) => {
                        span.record("otel.status_code", "ERROR");
                        all_open = false;
                    }
                }
            }

            // Surface the right 503 message depending on whether all candidates
            // were skipped due to open circuits or actually attempted.
            let msg = if all_open && !candidates.is_empty() {
                "all engine circuits open"
            } else {
                "all engines at capacity"
            };

            let mut resp = super::error_response(http::StatusCode::SERVICE_UNAVAILABLE, msg);
            if let Some(secs) = retry_after_secs {
                if let Ok(val) = http::HeaderValue::from_str(&secs.to_string()) {
                    resp.headers_mut().insert(http::header::RETRY_AFTER, val);
                }
            }
            Ok(resp)
        })
    }
}
