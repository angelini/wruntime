use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use tower::{Layer, Service};

use super::{ResBody, full_body};
use crate::schema::SchemaCache;

pub struct SchemaValidationLayer {
    cache: Arc<SchemaCache>,
}

impl SchemaValidationLayer {
    pub fn new(cache: Arc<SchemaCache>) -> Self {
        Self { cache }
    }
}

impl<S> Layer<S> for SchemaValidationLayer {
    type Service = SchemaValidationService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        SchemaValidationService { inner, cache: self.cache.clone() }
    }
}

#[derive(Clone)]
pub struct SchemaValidationService<S> {
    inner: S,
    cache: Arc<SchemaCache>,
}

impl<S> Service<Request<Bytes>> for SchemaValidationService<S>
where
    S: Service<Request<Bytes>, Response = http::Response<ResBody>>
        + Clone
        + Send
        + 'static,
    S::Error: Send + 'static,
    S::Future: Send + 'static,
{
    type Response = http::Response<ResBody>;
    type Error    = S::Error;
    type Future   = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Bytes>) -> Self::Future {
        let cache     = self.cache.clone();
        let mut inner = self.inner.clone();

        Box::pin(async move {
            // Parse x-wr-destination to get module name and RPC path.
            let destination = req
                .headers()
                .get("x-wr-destination")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();

            if let Ok(dest_uri) = destination.parse::<http::Uri>() {
                let module = dest_uri.host().unwrap_or("").to_string();
                let path   = dest_uri.path().to_string();

                if let Some(detail) =
                    cache.validate(&module, &path, req.body().as_ref()).await
                {
                    let source = req
                        .headers()
                        .get("x-wr-source")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("")
                        .to_string();

                    return Ok(validation_error(&detail, &source, &module));
                }
            }

            inner.call(req).await
        })
    }
}

/// Build a `400 Bad Request` response with the structured JSON error body
/// described in the plan.
fn validation_error(detail: &str, source: &str, destination: &str) -> Response<ResBody> {
    let body = serde_json::json!({
        "error":       "schema_validation_failed",
        "detail":      detail,
        "source":      source,
        "destination": destination,
    })
    .to_string();

    Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(full_body(Bytes::from(body)))
        .unwrap()
}
