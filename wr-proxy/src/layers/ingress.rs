use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use http::{Request, Response, StatusCode};
use tower::{Layer, Service};

use super::{error_response, ProxyBody, ResBody};
use crate::config::ExternalRoute;

pub struct IngressLayer {
    routes: Arc<Vec<ExternalRoute>>,
}

impl IngressLayer {
    pub fn new(routes: Vec<ExternalRoute>) -> Self {
        Self {
            routes: Arc::new(routes),
        }
    }
}

impl<S> Layer<S> for IngressLayer {
    type Service = IngressService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        IngressService {
            inner,
            routes: self.routes.clone(),
        }
    }
}

#[derive(Clone)]
pub struct IngressService<S> {
    inner: S,
    routes: Arc<Vec<ExternalRoute>>,
}

impl<S> Service<Request<ProxyBody>> for IngressService<S>
where
    S: Service<Request<ProxyBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Error: Send + 'static,
    S::Future: Send + 'static,
{
    type Response = Response<ResBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ProxyBody>) -> Self::Future {
        let routes = self.routes.clone();
        let mut inner = self.inner.clone();
        let method = req.method().as_str().to_uppercase();
        let path = req.uri().path().to_string();

        Box::pin(async move {
            let (mut parts, body) = req.into_parts();

            // Strip all x-wr-* headers to prevent external callers from spoofing
            // internal routing identity.
            for name in &[
                "x-wr-destination",
                "x-wr-source",
                "x-wr-source-ns",
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

            // Set routing headers and pass through to the inner stack.
            let dest = format!("http://{namespace}.{module}/");
            if let Ok(v) = http::HeaderValue::from_str(&dest) {
                parts.headers.insert("x-wr-destination", v);
            }
            parts
                .headers
                .insert("x-wr-source", http::HeaderValue::from_static("external"));
            inner.call(Request::from_parts(parts, body)).await
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
