use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use http::{Request, Response, StatusCode};
use tower::{Layer, Service};

use super::{error_response, ProxyBody, ResBody};
use crate::config::{build_external_route_index, ExternalRoute};
use wr_common::http_headers::{strip_external_spoofable_headers, WR_DESTINATION, WR_SOURCE};

pub struct IngressLayer {
    routes: Arc<Vec<ExternalRoute>>,
    router: Arc<matchit::Router<Vec<usize>>>,
}

impl IngressLayer {
    pub fn new(routes: Vec<ExternalRoute>) -> anyhow::Result<Self> {
        let router = build_external_route_index(&routes)?;
        Ok(Self {
            routes: Arc::new(routes),
            router: Arc::new(router),
        })
    }
}

impl<S> Layer<S> for IngressLayer {
    type Service = IngressService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        IngressService {
            inner,
            routes: self.routes.clone(),
            router: self.router.clone(),
        }
    }
}

#[derive(Clone)]
pub struct IngressService<S> {
    inner: S,
    routes: Arc<Vec<ExternalRoute>>,
    router: Arc<matchit::Router<Vec<usize>>>,
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
        let router = self.router.clone();
        let mut inner = self.inner.clone();
        let method = req.method().clone();
        let path = req.uri().path().to_string();

        Box::pin(async move {
            let (mut parts, body) = req.into_parts();

            // Strip all x-wr-* headers to prevent external callers from spoofing
            // internal routing identity.
            strip_external_spoofable_headers(&mut parts.headers);

            // Match the request against the configured public routes.
            let matched = match router.at(&path) {
                Ok(m) => m,
                Err(_) => {
                    return Ok(error_response(
                        StatusCode::NOT_FOUND,
                        "no public route for this path",
                    ));
                }
            };

            let route = match matched
                .value
                .iter()
                .find(|&&idx| routes[idx].methods().allows(&method))
            {
                Some(&idx) => &routes[idx],
                None => {
                    return Ok(error_response(
                        StatusCode::NOT_FOUND,
                        "no public route for this path",
                    ));
                }
            };

            let target = route.target();

            // Set routing headers and pass through to the inner stack.
            let dest = format!("http://{}.{}/", target.namespace(), target.module());
            if let Ok(v) = http::HeaderValue::from_str(&dest) {
                parts.headers.insert(WR_DESTINATION, v);
            }
            parts
                .headers
                .insert(WR_SOURCE, http::HeaderValue::from_static("external"));
            inner.call(Request::from_parts(parts, body)).await
        })
    }
}

#[cfg(test)]
mod tests {
    fn make_router(routes: &[(&str, &str)]) -> matchit::Router<usize> {
        let mut router = matchit::Router::new();
        for (i, (path, _)) in routes.iter().enumerate() {
            router.insert(*path, i).unwrap();
        }
        router
    }

    #[test]
    fn exact_match() {
        let router = make_router(&[("/items", "items")]);
        assert!(router.at("/items").is_ok());
        assert!(router.at("/orders").is_err());
    }

    #[test]
    fn wildcard_segment() {
        let router = make_router(&[("/items/{id}", "item")]);
        assert!(router.at("/items/123").is_ok());
        assert!(router.at("/items/123/extra").is_err());
    }

    #[test]
    fn root_path() {
        let router = make_router(&[("/", "root")]);
        assert!(router.at("/").is_ok());
        assert!(router.at("/items").is_err());
    }

    #[test]
    fn segment_count_mismatch() {
        let router = make_router(&[("/items/{id}", "item")]);
        assert!(router.at("/items").is_err());
    }
}
