use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use http::Request;
use tower::{Layer, Service};
use tracing::{info_span, Instrument};

use super::{ProxyBody, ResBody};

pub struct TracingLayer;

impl<S> Layer<S> for TracingLayer {
    type Service = TracingService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        TracingService { inner }
    }
}

#[derive(Clone)]
pub struct TracingService<S> {
    inner: S,
}

impl<S> Service<Request<ProxyBody>> for TracingService<S>
where
    S: Service<Request<ProxyBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Error: std::fmt::Display + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = http::Response<ResBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ProxyBody>) -> Self::Future {
        let method = req.method().as_str().to_string();
        let path = req
            .uri()
            .path_and_query()
            .map(|pq| pq.as_str().to_string())
            .unwrap_or_else(|| "/".to_string());
        let source = header_str(req.headers(), WR_SOURCE).to_string();
        let dest = header_str(req.headers(), WR_DESTINATION).to_string();

        let span = info_span!(
            "proxy.request",
            otel.name          = format!("{method} {dest}"),
            http.request.method = %method,
            url.path           = %path,
            wr.source          = %source,
            wr.destination     = %dest,
            http.response.status_code = tracing::field::Empty,
            otel.status_code   = tracing::field::Empty,
        );

        // If the request carries a traceparent header (e.g. from an engine's
        // outbound_request span), adopt it as our parent so the proxy span
        // joins the existing trace rather than starting a new one.
        wr_common::telemetry::set_parent_from_headers(&span, req.headers());

        let mut inner = self.inner.clone();

        Box::pin(async move {
            let result = inner.call(req).instrument(span.clone()).await;

            match &result {
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    span.record("http.response.status_code", status);
                    if status >= 400 {
                        span.record("otel.status_code", "ERROR");
                    } else {
                        span.record("otel.status_code", "OK");
                    }
                }
                Err(e) => {
                    span.record("http.response.status_code", 502u16);
                    span.record("otel.status_code", "ERROR");
                    tracing::error!(parent: &span, error = %e, "proxy request failed");
                }
            }

            result
        })
    }
}

use wr_common::http_headers::{header_str, WR_DESTINATION, WR_SOURCE};
