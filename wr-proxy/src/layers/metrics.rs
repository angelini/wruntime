use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use bytes::Bytes;
use http::Request;
use tokio::sync::mpsc;
use tower::{Layer, Service};

use super::ResBody;
use wr_common::wruntime::RequestMetrics;

pub struct MetricsLayer {
    tx: mpsc::Sender<RequestMetrics>,
}

impl MetricsLayer {
    pub fn new(tx: mpsc::Sender<RequestMetrics>) -> Self {
        Self { tx }
    }
}

impl<S> Layer<S> for MetricsLayer {
    type Service = MetricsService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        MetricsService { inner, tx: self.tx.clone() }
    }
}

#[derive(Clone)]
pub struct MetricsService<S> {
    inner: S,
    tx:    mpsc::Sender<RequestMetrics>,
}

impl<S> Service<Request<Bytes>> for MetricsService<S>
where
    S: Service<Request<Bytes>, Response = http::Response<ResBody>>
        + Clone
        + Send
        + 'static,
    S::Error: std::fmt::Display + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = http::Response<ResBody>;
    type Error    = S::Error;
    type Future   = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Bytes>) -> Self::Future {
        let source = header_str(req.headers(), "x-wr-source").to_string();
        let dest   = header_str(req.headers(), "x-wr-destination").to_string();
        let start  = Instant::now();
        let tx     = self.tx.clone();

        // Clone inner so the future is 'static (doesn't borrow self).
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let result: Result<http::Response<ResBody>, S::Error> = inner.call(req).await;
            let elapsed = start.elapsed().as_millis() as u64;

            let (status, error) = match &result {
                Ok(resp) => (resp.status().as_u16() as u32, String::new()),
                Err(e)   => (502u32, e.to_string()),
            };

            let _ = tx.try_send(RequestMetrics {
                source,
                destination: dest,
                duration_ms: elapsed,
                status,
                error,
            });

            result
        })
    }
}

fn header_str<'a>(headers: &'a http::HeaderMap, name: &str) -> &'a str {
    headers
        .get(name)
        .and_then(|v: &http::HeaderValue| v.to_str().ok())
        .unwrap_or("unknown")
}
