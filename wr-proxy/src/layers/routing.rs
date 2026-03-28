use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http::{Request, StatusCode};
use tower::{Layer, Service};

use super::{ResBody, ResolvedDestination, error_response};
use crate::routing::CachedRoutingTable;

pub struct RoutingLayer {
    table: CachedRoutingTable,
}

impl RoutingLayer {
    pub fn new(table: CachedRoutingTable) -> Self {
        Self { table }
    }
}

impl<S> Layer<S> for RoutingLayer {
    type Service = RoutingService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        RoutingService { inner, table: self.table.clone() }
    }
}

#[derive(Clone)]
pub struct RoutingService<S> {
    inner: S,
    table: CachedRoutingTable,
}

impl<S> Service<Request<Bytes>> for RoutingService<S>
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

    fn call(&mut self, mut req: Request<Bytes>) -> Self::Future {
        let table = self.table.clone();
        let mut inner = self.inner.clone();

        Box::pin(async move {
            // Extract destination module name from the x-wr-destination host
            let dest_uri: Option<http::Uri> = req
                .headers()
                .get("x-wr-destination")
                .and_then(|v: &http::HeaderValue| v.to_str().ok())
                .and_then(|s| s.parse().ok());

            let module_name = dest_uri
                .as_ref()
                .and_then(|u: &http::Uri| u.host())
                .unwrap_or("")
                .to_string();

            // Look up the engine address for this destination module
            let engine_address = {
                let t = table.read().await;
                t.rules
                    .iter()
                    .find(|r| r.destination_module == module_name)
                    .map(|r| r.engine_address.clone())
            };

            match engine_address {
                Some(addr) => {
                    // Inject x-wr-module so the destination engine knows which
                    // WASM module to dispatch the request to.
                    if !module_name.is_empty() {
                        if let Ok(v) = http::HeaderValue::from_str(&module_name) {
                            req.headers_mut().insert("x-wr-module", v);
                        }
                    }
                    req.extensions_mut().insert(ResolvedDestination(addr));
                    inner.call(req).await
                }
                None => Ok(error_response(
                    StatusCode::BAD_GATEWAY,
                    &format!("no route for module '{module_name}'"),
                )),
            }
        })
    }
}
