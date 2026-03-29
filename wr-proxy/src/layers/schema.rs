use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use tower::{Layer, Service};

use super::{full_body, ResBody};
use crate::schema::{SchemaCache, ValidationOutcome};

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
        SchemaValidationService {
            inner,
            cache: self.cache.clone(),
        }
    }
}

#[derive(Clone)]
pub struct SchemaValidationService<S> {
    inner: S,
    cache: Arc<SchemaCache>,
}

impl<S> Service<Request<Bytes>> for SchemaValidationService<S>
where
    S: Service<Request<Bytes>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Error: Send + 'static,
    S::Future: Send + 'static,
{
    type Response = http::Response<ResBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Bytes>) -> Self::Future {
        let cache = self.cache.clone();
        let mut inner = self.inner.clone();

        Box::pin(async move {
            // Requests arriving from a peer proxy were already validated at ingress.
            // Skip re-validation to avoid double-checking and allow the routing layer
            // to dispatch to the local engine.
            if req.headers().contains_key("x-wr-via-proxy") {
                return inner.call(req).await;
            }

            // Parse x-wr-destination to get module name and RPC path.
            let destination = req
                .headers()
                .get("x-wr-destination")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();

            if let Ok(dest_uri) = destination.parse::<http::Uri>() {
                let host = dest_uri.host().unwrap_or("");
                let path = dest_uri.path().to_string();

                // host format is "{service}.{namespace}"
                // If there is no dot the destination is malformed — pass
                // through so the routing layer can return a 400.
                let Some((module, namespace)) = host
                    .split_once('.')
                    .map(|(s, n)| (s.to_string(), n.to_string()))
                else {
                    return inner.call(req).await;
                };

                let source = req
                    .headers()
                    .get("x-wr-source")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();

                match cache
                    .validate(&namespace, &module, &path, req.body().as_ref())
                    .await
                {
                    ValidationOutcome::Pass => {}
                    ValidationOutcome::Fail(detail) => {
                        return Ok(validation_error(&detail, &source, &module));
                    }
                    ValidationOutcome::SchemaNotCached => {
                        return Ok(schema_not_cached_error(&module, &namespace));
                    }
                    ValidationOutcome::MethodNotFound(detail) => {
                        return Ok(method_not_found_error(&detail, &source, &module));
                    }
                }
            }

            inner.call(req).await
        })
    }
}

/// Build a `400 Bad Request` response for a body that fails schema validation.
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

/// Build a `404 Not Found` response when the path doesn't match any RPC in the schema.
fn method_not_found_error(detail: &str, source: &str, destination: &str) -> Response<ResBody> {
    let body = serde_json::json!({
        "error":       "method_not_found",
        "detail":      detail,
        "source":      source,
        "destination": destination,
    })
    .to_string();

    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(full_body(Bytes::from(body)))
        .unwrap()
}

/// Build a `503 Service Unavailable` response when no schema is cached yet.
fn schema_not_cached_error(module: &str, namespace: &str) -> Response<ResBody> {
    let body = serde_json::json!({
        "error":  "schema_not_cached",
        "detail": format!("schema for {module}.{namespace} has not been synced yet"),
        "module": module,
        "namespace": namespace,
    })
    .to_string();

    Response::builder()
        .status(StatusCode::SERVICE_UNAVAILABLE)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(full_body(Bytes::from(body)))
        .unwrap()
}
