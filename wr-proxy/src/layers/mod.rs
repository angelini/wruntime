mod egress;
mod forward;
mod ingress;
mod routing;
mod tracing;

pub use egress::EgressLayer;
pub use forward::ForwardService;
pub use ingress::IngressLayer;
pub use routing::RoutingLayer;
pub use tracing::TracingLayer;

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http::Response;
use http_body::Body;
use http_body_util::{BodyExt as _, Full};

/// A streaming body type used throughout the proxy Tower stack.
///
/// Wraps any `Body<Data=Bytes, Error=hyper::Error> + Send` behind a pinned
/// box.  This is needed because `hyper::body::Incoming` is `Send + !Sync`
/// (ruling out `http_body_util::BoxBody` which requires `Sync`) and `!Unpin`
/// (ruling out direct use with the hyper-util legacy `Client`).
///
/// `ProxyBody` is always `Send + Unpin + 'static`, satisfying both the Tower
/// stack constraints and the hyper client bounds.
pub struct ProxyBody(Pin<Box<dyn Body<Data = Bytes, Error = hyper::Error> + Send + 'static>>);

impl ProxyBody {
    /// Wrap a streaming `Incoming` body (or any compatible body).
    pub fn streaming(body: hyper::body::Incoming) -> Self {
        Self(Box::pin(body))
    }

    /// Build a body from a contiguous byte buffer.
    pub fn full(bytes: impl Into<Bytes>) -> Self {
        Self(Box::pin(
            Full::new(bytes.into()).map_err(|never| match never {}),
        ))
    }
}

impl Body for ProxyBody {
    type Data = Bytes;
    type Error = hyper::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, hyper::Error>>> {
        self.0.as_mut().poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.0.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.0.size_hint()
    }
}

/// Shared response body type used throughout the proxy Tower stack.
pub type ResBody = ProxyBody;

pub fn full_body(msg: impl Into<Bytes>) -> ResBody {
    ProxyBody::full(msg)
}

pub fn error_response(status: http::StatusCode, msg: &str) -> Response<ResBody> {
    Response::builder()
        .status(status)
        .body(ProxyBody::full(Bytes::from(msg.to_string())))
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
/// Contains the single round-robin-selected candidate to forward to.
#[derive(Clone)]
pub struct ResolvedDestination(pub Destination);
