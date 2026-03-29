mod forward;
mod metrics;
mod routing;
mod schema;
mod tracing;

pub use forward::ForwardService;
pub use metrics::MetricsLayer;
pub use routing::RoutingLayer;
pub use schema::SchemaValidationLayer;
pub use tracing::TracingLayer;

use bytes::Bytes;
use http::Response;
use http_body_util::{combinators::BoxBody, Full};
use std::convert::Infallible;

/// Shared response body type used throughout the proxy Tower stack.
pub type ResBody = BoxBody<Bytes, Infallible>;

pub fn full_body(msg: impl Into<Bytes>) -> ResBody {
    BoxBody::new(Full::new(msg.into()))
}

pub fn error_response(status: http::StatusCode, msg: &str) -> Response<ResBody> {
    Response::builder()
        .status(status)
        .body(full_body(Bytes::from(msg.to_string())))
        .unwrap()
}

/// Routing decision made by [`RoutingLayer`]; consumed by [`ForwardService`].
#[derive(Clone)]
pub enum Destination {
    /// Forward directly to the local engine at this address.
    LocalEngine(String),
    /// Forward to a peer proxy at this address (cross-node hop).
    RemoteProxy(String),
}

/// Set by [`RoutingLayer`] on the request extensions; read by [`ForwardService`].
#[derive(Clone)]
pub struct ResolvedDestination(pub Destination);
