use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http::Request;
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::{connect::HttpConnector, Client};
use hyper_util::rt::TokioExecutor;
use tower::Service;
use tracing::{info_span, Instrument};

use super::{full_body, Destination, ResBody, ResolvedDestination};

#[derive(Clone)]
pub struct ForwardService {
    client: Client<HttpConnector, Full<Bytes>>,
}

impl ForwardService {
    pub fn new() -> Self {
        let client = Client::builder(TokioExecutor::new()).build_http();
        Self { client }
    }
}

impl Default for ForwardService {
    fn default() -> Self {
        Self::new()
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

            for destination in &candidates {
                let mut headers = parts.headers.clone();

                let forward_addr = match destination {
                    Destination::LocalEngine(addr) => {
                        // Strip internal routing headers — engine doesn't need them.
                        // (x-wr-module is kept so the engine can dispatch correctly)
                        headers.remove("x-wr-destination");
                        headers.remove("x-wr-source");
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

                let result = async {
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

                    Ok::<_, anyhow::Error>((resp_parts, resp_bytes))
                }
                .instrument(span.clone())
                .await?;

                let (resp_parts, resp_bytes) = result;
                let status = resp_parts.status.as_u16();
                span.record("http.response.status_code", status);
                span.record(
                    "otel.status_code",
                    if status >= 500 { "ERROR" } else { "OK" },
                );

                if resp_parts.status != http::StatusCode::TOO_MANY_REQUESTS {
                    return Ok(http::Response::from_parts(
                        resp_parts,
                        full_body(resp_bytes),
                    ));
                }
                // 429 — engine at capacity, try next candidate
            }

            // All candidates returned 429; surface as 503 to the caller.
            Ok(super::error_response(
                http::StatusCode::SERVICE_UNAVAILABLE,
                "all engines at capacity",
            ))
        })
    }
}
