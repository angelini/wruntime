use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use http::uri::PathAndQuery;
use http::Request;
use tower::Service;
use tracing::{info_span, warn, Instrument};
use wr_common::http_headers::{strip_before_engine, WR_VIA_PROXY};
use wr_common::http_pool::{HttpClientPool, DEFAULT_POOL_SIZE};

use super::{Destination, ForwardTarget, ProxyBody, ResBody, ResolvedRoute};

#[derive(Clone)]
pub struct ForwardService {
    pool: HttpClientPool<ProxyBody>,
    mtls_pool: wr_common::tls::HttpsClientPool<ProxyBody>,
    open_duration_secs: u64,
}

impl ForwardService {
    pub fn new(
        open_duration_secs: u64,
        mtls_pool: wr_common::tls::HttpsClientPool<ProxyBody>,
    ) -> Self {
        Self {
            pool: HttpClientPool::new(DEFAULT_POOL_SIZE),
            mtls_pool,
            open_duration_secs,
        }
    }
}

fn assemble_forward_uri(
    target: &ForwardTarget,
    path_and_query: PathAndQuery,
) -> anyhow::Result<http::Uri> {
    let base = target.base_uri().ok_or_else(|| {
        anyhow::anyhow!(
            "forward destination must be an absolute HTTP URI with a root path: {}",
            target.address()
        )
    })?;
    let mut parts = base.clone().into_parts();
    parts.path_and_query = Some(path_and_query);
    http::Uri::from_parts(parts).map_err(Into::into)
}

impl Service<Request<ProxyBody>> for ForwardService {
    type Response = http::Response<ResBody>;
    type Error = anyhow::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut req: Request<ProxyBody>) -> Self::Future {
        let local_client = self.pool.get().clone();
        let mtls_client = self.mtls_pool.get().clone();
        let open_duration_secs = self.open_duration_secs;

        Box::pin(async move {
            let resolved = req
                .extensions_mut()
                .remove::<ResolvedRoute>()
                .ok_or_else(|| anyhow::anyhow!("missing ResolvedRoute extension"))?;
            let path_and_query = req
                .uri()
                .path_and_query()
                .cloned()
                .unwrap_or_else(|| PathAndQuery::from_static("/"));
            let (mut parts, body) = req.into_parts();

            match &resolved.destination {
                Destination::LocalEngine(_) => strip_before_engine(&mut parts.headers),
                Destination::RemoteProxy(_) => {
                    parts
                        .headers
                        .insert(WR_VIA_PROXY, http::HeaderValue::from_static("1"));
                }
            }

            parts.uri = assemble_forward_uri(resolved.destination.target(), path_and_query)?;
            wr_common::telemetry::inject_context(&mut parts.headers);
            let forward_req = Request::from_parts(parts, body);
            let forward_addr = resolved.destination.address();

            let span = info_span!(
                "proxy.forward",
                wr.engine = %forward_addr,
                http.response.status_code = tracing::field::Empty,
                otel.status_code = tracing::field::Empty,
            );

            if !resolved.breaker.is_call_permitted() {
                warn!(parent: &span, engine = %forward_addr, "circuit open");
                span.record("otel.status_code", "circuit_open");
                let mut response =
                    super::error_response(http::StatusCode::SERVICE_UNAVAILABLE, "circuit open");
                if let Ok(value) = http::HeaderValue::from_str(&open_duration_secs.to_string()) {
                    response
                        .headers_mut()
                        .insert(http::header::RETRY_AFTER, value);
                }
                return Ok(response);
            }

            let result = async {
                match &resolved.destination {
                    Destination::LocalEngine(_) => local_client
                        .request(forward_req)
                        .await
                        .map_err(|error| anyhow::anyhow!("forward failed: {error}")),
                    Destination::RemoteProxy(_) => mtls_client
                        .request(forward_req)
                        .await
                        .map_err(|error| anyhow::anyhow!("forward failed: {error}")),
                }
            }
            .instrument(span.clone())
            .await;

            match result {
                Ok(response) => {
                    let (response_parts, response_body) = response.into_parts();
                    span.record("http.response.status_code", response_parts.status.as_u16());
                    if response_parts.status.is_server_error()
                        || response_parts.status == http::StatusCode::TOO_MANY_REQUESTS
                    {
                        resolved.breaker.on_error();
                        span.record("otel.status_code", "ERROR");
                    } else {
                        resolved.breaker.on_success();
                        span.record("otel.status_code", "OK");
                    }
                    Ok(http::Response::from_parts(
                        response_parts,
                        ProxyBody::streaming(response_body),
                    ))
                }
                Err(error) => {
                    resolved.breaker.on_error();
                    span.record("otel.status_code", "ERROR");
                    Ok(super::error_response(
                        http::StatusCode::SERVICE_UNAVAILABLE,
                        &format!("forward failed: {error}"),
                    ))
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn target(address: &str) -> ForwardTarget {
        ForwardTarget::new(Arc::from(address))
    }

    #[test]
    fn prepared_uri_preserves_path_query_encoding_and_ipv6() {
        for (base, path, expected) in [
            ("http://engine", "/", "http://engine/"),
            ("http://engine/", "/rpc?q=one", "http://engine/rpc?q=one"),
            (
                "https://[::1]:9443",
                "/a%2Fb?x=%2F",
                "https://[::1]:9443/a%2Fb?x=%2F",
            ),
        ] {
            let uri = assemble_forward_uri(&target(base), path.parse().unwrap()).unwrap();
            assert_eq!(uri, expected);
        }
    }

    #[test]
    fn non_root_base_is_rejected() {
        let error =
            assemble_forward_uri(&target("http://engine/base"), "/rpc?q=1".parse().unwrap())
                .expect_err("non-root destination must not be concatenated");
        assert!(error.to_string().contains("root path"));
    }
}
