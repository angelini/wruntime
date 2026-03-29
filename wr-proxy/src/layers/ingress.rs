use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::BodyExt as _;
use prost::Message as _;
use prost_reflect::DynamicMessage;
use tower::{Layer, Service};

use super::{error_response, full_body, ResBody};
use crate::config::ExternalRoute;
use crate::schema::{MessageLookup, SchemaCache};

pub struct IngressLayer {
    routes: Arc<Vec<ExternalRoute>>,
    schema_cache: Arc<SchemaCache>,
}

impl IngressLayer {
    pub fn new(routes: Vec<ExternalRoute>, schema_cache: Arc<SchemaCache>) -> Self {
        Self {
            routes: Arc::new(routes),
            schema_cache,
        }
    }
}

impl<S> Layer<S> for IngressLayer {
    type Service = IngressService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        IngressService {
            inner,
            routes: self.routes.clone(),
            schema_cache: self.schema_cache.clone(),
        }
    }
}

#[derive(Clone)]
pub struct IngressService<S> {
    inner: S,
    routes: Arc<Vec<ExternalRoute>>,
    schema_cache: Arc<SchemaCache>,
}

impl<S> Service<Request<Bytes>> for IngressService<S>
where
    S: Service<Request<Bytes>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Error: Send + 'static,
    S::Future: Send + 'static,
{
    type Response = Response<ResBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Bytes>) -> Self::Future {
        let routes = self.routes.clone();
        let schema_cache = self.schema_cache.clone();
        let mut inner = self.inner.clone();
        let method = req.method().as_str().to_uppercase();
        let path = req.uri().path().to_string();

        Box::pin(async move {
            // Decompose up front so we can strip internal headers before anything else.
            let (mut parts, body) = req.into_parts();

            // Strip all x-wr-* headers to prevent external callers from spoofing
            // internal routing identity or bypassing schema validation.
            for name in &[
                "x-wr-destination",
                "x-wr-source",
                "x-wr-module",
                "x-wr-namespace",
                "x-wr-version",
                "x-wr-via-proxy",
            ] {
                parts.headers.remove(*name);
            }

            // Match the request against the configured public routes.
            let route = match routes.iter().find(|r| {
                let method_ok =
                    r.methods.is_empty() || r.methods.iter().any(|m| m.to_uppercase() == method);
                method_ok && path_matches(&r.path, &path)
            }) {
                Some(r) => r,
                None => {
                    return Ok(error_response(
                        StatusCode::NOT_FOUND,
                        "no public route for this path",
                    ));
                }
            };

            let module = &route.module;
            let namespace = &route.namespace;

            match (&route.grpc_path, &route.request_type, &route.response_type) {
                // ── Transcoding path ─────────────────────────────────────────
                (Some(grpc_path), Some(req_type), Some(resp_type)) => {
                    // Resolve both message descriptors before doing any I/O.
                    let req_desc = match schema_cache
                        .message_descriptor(namespace, module, req_type)
                        .await
                    {
                        MessageLookup::Found(d) => d,
                        MessageLookup::SchemaNotCached => {
                            return Ok(error_response(
                                StatusCode::SERVICE_UNAVAILABLE,
                                &format!("schema for {module}.{namespace} has not been synced yet"),
                            ));
                        }
                        MessageLookup::TypeNotFound => {
                            return Ok(error_response(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                &format!(
                                    "request type '{req_type}' not found in schema \
                                         for {module}.{namespace}"
                                ),
                            ));
                        }
                    };
                    let resp_desc = match schema_cache
                        .message_descriptor(namespace, module, resp_type)
                        .await
                    {
                        MessageLookup::Found(d) => d,
                        MessageLookup::SchemaNotCached => {
                            return Ok(error_response(
                                StatusCode::SERVICE_UNAVAILABLE,
                                &format!("schema for {module}.{namespace} has not been synced yet"),
                            ));
                        }
                        MessageLookup::TypeNotFound => {
                            return Ok(error_response(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                &format!(
                                    "response type '{resp_type}' not found in schema \
                                         for {module}.{namespace}"
                                ),
                            ));
                        }
                    };

                    // JSON → protobuf
                    let mut de = serde_json::Deserializer::from_slice(&body);
                    let dynamic_req = match DynamicMessage::deserialize(req_desc, &mut de) {
                        Ok(m) => m,
                        Err(e) => {
                            return Ok(error_response(
                                StatusCode::BAD_REQUEST,
                                &format!("invalid JSON body: {e}"),
                            ));
                        }
                    };
                    let proto_bytes = Bytes::from(dynamic_req.encode_to_vec());

                    // Inject routing headers; gRPC path goes into x-wr-destination so
                    // the schema validation layer (on the internal stack) can resolve
                    // the RPC method if it's present.
                    let dest = format!("http://{module}.{namespace}{grpc_path}");
                    if let Ok(v) = http::HeaderValue::from_str(&dest) {
                        parts.headers.insert("x-wr-destination", v);
                    }
                    parts
                        .headers
                        .insert("x-wr-source", http::HeaderValue::from_static("external"));
                    parts.headers.remove(http::header::CONTENT_TYPE);
                    // Body size changed: remove Content-Length so hyper recomputes it.
                    parts.headers.remove(http::header::CONTENT_LENGTH);

                    let transcoded_req = Request::from_parts(parts, proto_bytes);
                    let response = inner.call(transcoded_req).await?;

                    // Protobuf → JSON
                    let (mut resp_parts, resp_body) = response.into_parts();
                    // ResBody error is Infallible, so collect() never fails.
                    let resp_bytes = resp_body.collect().await.unwrap().to_bytes();

                    match DynamicMessage::decode(resp_desc, resp_bytes.as_ref()) {
                        Ok(dynamic_resp) => match serde_json::to_string(&dynamic_resp) {
                            Ok(json) => {
                                resp_parts.headers.insert(
                                    http::header::CONTENT_TYPE,
                                    http::HeaderValue::from_static("application/json"),
                                );
                                // Body size changed: remove Content-Length so hyper recomputes it.
                                resp_parts.headers.remove(http::header::CONTENT_LENGTH);
                                Ok(Response::from_parts(
                                    resp_parts,
                                    full_body(Bytes::from(json)),
                                ))
                            }
                            Err(e) => Ok(error_response(
                                StatusCode::BAD_GATEWAY,
                                &format!("response serialization failed: {e}"),
                            )),
                        },
                        Err(e) => Ok(error_response(
                            StatusCode::BAD_GATEWAY,
                            &format!("response protobuf decode failed: {e}"),
                        )),
                    }
                }

                // ── Plain HTTP pass-through ───────────────────────────────────
                _ => {
                    let dest = format!("http://{module}.{namespace}/");
                    if let Ok(v) = http::HeaderValue::from_str(&dest) {
                        parts.headers.insert("x-wr-destination", v);
                    }
                    parts
                        .headers
                        .insert("x-wr-source", http::HeaderValue::from_static("external"));
                    inner.call(Request::from_parts(parts, body)).await
                }
            }
        })
    }
}

/// Returns `true` if `pattern` matches `path`.
///
/// Matching is segment-by-segment after splitting on `/`.  A pattern segment
/// wrapped in `{braces}` is treated as a wildcard and matches any single path
/// segment.  The number of segments must be equal (no suffix wildcards).
fn path_matches(pattern: &str, path: &str) -> bool {
    let pat_segs: Vec<&str> = pattern.split('/').collect();
    let path_segs: Vec<&str> = path.split('/').collect();

    if pat_segs.len() != path_segs.len() {
        return false;
    }

    pat_segs
        .iter()
        .zip(path_segs.iter())
        .all(|(p, s)| (p.starts_with('{') && p.ends_with('}')) || p == s)
}

#[cfg(test)]
mod tests {
    use super::path_matches;

    #[test]
    fn exact_match() {
        assert!(path_matches("/items", "/items"));
        assert!(!path_matches("/items", "/orders"));
    }

    #[test]
    fn wildcard_segment() {
        assert!(path_matches("/items/{id}", "/items/123"));
        assert!(!path_matches("/items/{id}", "/items/123/extra"));
    }

    #[test]
    fn root_path() {
        assert!(path_matches("/", "/"));
        assert!(!path_matches("/", "/items"));
    }

    #[test]
    fn segment_count_mismatch() {
        assert!(!path_matches("/items/{id}", "/items"));
    }
}
