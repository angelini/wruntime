mod egress;
mod forward;
mod ingress;
mod routing;
mod schema;
mod tracing;

pub use egress::EgressLayer;
pub use forward::ForwardService;
pub use ingress::IngressLayer;
pub use routing::RoutingLayer;
pub use schema::SchemaValidationLayer;
pub use tracing::TracingLayer;

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use http::Response;
use http_body::Body;
use http_body_util::{BodyExt as _, Full};
use wr_common::lifecycle_service::AdmissionGuard;

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

    /// Keep admitted work counted until the streaming response finishes or is dropped.
    pub fn with_admission_guard(self, guard: AdmissionGuard) -> Self {
        Self(Box::pin(GuardedBody {
            inner: self,
            _guard: guard,
        }))
    }
}

struct GuardedBody {
    inner: ProxyBody,
    _guard: AdmissionGuard,
}

impl Body for GuardedBody {
    type Data = Bytes;
    type Error = hyper::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, hyper::Error>>> {
        Pin::new(&mut self.inner).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
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

/// Prepared forwarding target shared by all snapshots and selected requests.
#[derive(Clone)]
pub struct ForwardTarget {
    address: Arc<str>,
    base_uri: Option<http::Uri>,
}

impl ForwardTarget {
    pub(crate) fn new(address: Arc<str>) -> Self {
        let base_uri = address.parse::<http::Uri>().ok().filter(|uri| {
            uri.scheme().is_some()
                && uri.authority().is_some()
                && uri.query().is_none()
                && matches!(uri.path(), "" | "/")
        });
        Self { address, base_uri }
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub(crate) fn base_uri(&self) -> Option<&http::Uri> {
        self.base_uri.as_ref()
    }
}

/// Routing decision made by [`RoutingLayer`]; consumed by [`ForwardService`].
#[derive(Clone)]
pub enum Destination {
    /// Forward directly to the local engine at this target.
    LocalEngine(ForwardTarget),
    /// Forward to a peer proxy at this target (cross-node hop).
    RemoteProxy(ForwardTarget),
}

impl Destination {
    pub(crate) fn local(address: Arc<str>) -> Self {
        Self::LocalEngine(ForwardTarget::new(address))
    }

    pub(crate) fn remote(address: Arc<str>) -> Self {
        Self::RemoteProxy(ForwardTarget::new(address))
    }

    pub fn target(&self) -> &ForwardTarget {
        match self {
            Self::LocalEngine(target) | Self::RemoteProxy(target) => target,
        }
    }

    pub fn address(&self) -> &str {
        self.target().address()
    }
}

/// Set by [`RoutingLayer`] on the request extensions; read by [`ForwardService`].
#[derive(Clone)]
pub struct ResolvedRoute {
    pub destination: Destination,
    pub breaker: crate::circuit_breaker::EngineBreaker,
}
