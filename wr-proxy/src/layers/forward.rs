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

use super::{full_body, ResBody, ResolvedDestination};

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

    fn call(&mut self, mut req: Request<Bytes>) -> Self::Future {
        let client = self.client.clone();

        Box::pin(async move {
            // Read the engine address injected by RoutingLayer
            let dest = req
                .extensions()
                .get::<ResolvedDestination>()
                .map(|d| d.0.clone())
                .ok_or_else(|| anyhow::anyhow!("missing ResolvedDestination extension"))?;

            // Strip internal routing headers before forwarding
            // (x-wr-module is kept so the destination engine can dispatch correctly)
            req.headers_mut().remove("x-wr-destination");
            req.headers_mut().remove("x-wr-source");

            // Build the forwarding URI: engine address + original path+query
            let path = req
                .uri()
                .path_and_query()
                .map(|pq: &http::uri::PathAndQuery| pq.as_str())
                .unwrap_or("/");
            let forward_uri: http::Uri =
                format!("{}{}", dest.trim_end_matches('/'), path).parse()?;

            // Re-use the original method and headers; replace URI and body type.
            // Inject the W3C traceparent header so the engine can link its span
            // to this proxy span.
            let (mut parts, body) = req.into_parts();
            parts.uri = forward_uri;
            wr_common::telemetry::inject_context(&mut parts.headers);
            let forward_req = Request::from_parts(parts, Full::new(body));

            let span = info_span!(
                "proxy.forward",
                wr.engine                 = %dest,
                http.response.status_code = tracing::field::Empty,
                otel.status_code          = tracing::field::Empty,
            );

            // Send and collect — both are part of upstream latency.
            let (resp_parts, resp_bytes) = async {
                let resp = client
                    .request(forward_req)
                    .await
                    .map_err(|e| anyhow::anyhow!("forward failed: {e}"))?;

                let (parts, body) = resp.into_parts();
                let bytes = body
                    .collect()
                    .await
                    .map_err(|e| anyhow::anyhow!("response body error: {e}"))?
                    .to_bytes();

                Ok::<_, anyhow::Error>((parts, bytes))
            }
            .instrument(span.clone())
            .await?;

            let status = resp_parts.status.as_u16();
            span.record("http.response.status_code", status);
            span.record(
                "otel.status_code",
                if status >= 500 { "ERROR" } else { "OK" },
            );

            Ok(http::Response::from_parts(
                resp_parts,
                full_body(resp_bytes),
            ))
        })
    }
}
