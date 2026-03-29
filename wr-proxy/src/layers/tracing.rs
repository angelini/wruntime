use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http::Request;
use tower::{Layer, Service};
use tracing::{info_span, Instrument};

use super::ResBody;

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

impl<S> Service<Request<Bytes>> for TracingService<S>
where
    S: Service<Request<Bytes>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Error: std::fmt::Display + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = http::Response<ResBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Bytes>) -> Self::Future {
        let method = req.method().as_str().to_string();
        let path = req
            .uri()
            .path_and_query()
            .map(|pq| pq.as_str().to_string())
            .unwrap_or_else(|| "/".to_string());
        let source = header_str(req.headers(), "x-wr-source").to_string();
        let dest = header_str(req.headers(), "x-wr-destination").to_string();

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

fn header_str<'a>(headers: &'a http::HeaderMap, name: &str) -> &'a str {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown")
}
