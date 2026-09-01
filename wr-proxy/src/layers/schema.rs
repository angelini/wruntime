use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use http::header::{CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, TRANSFER_ENCODING};
use http::{HeaderValue, Request, StatusCode};
use http_body_util::{BodyExt as _, LengthLimitError, Limited};
use tower::{Layer, Service};
use tracing::warn;
use wr_common::http_headers::{WR_MODULE, WR_NAMESPACE, WR_VERSION};

use super::{error_response, ProxyBody, ResBody};
use crate::schema::SchemaCache;
use crate::transcoding::{select_request_representation, transcode_request, RequestRepresentation};

pub struct SchemaValidationLayer {
    cache: Arc<SchemaCache>,
    max_request_body_bytes: usize,
}

impl SchemaValidationLayer {
    pub fn new(cache: Arc<SchemaCache>, max_request_body_bytes: usize) -> Self {
        Self {
            cache,
            max_request_body_bytes,
        }
    }
}

impl<S> Layer<S> for SchemaValidationLayer {
    type Service = SchemaValidationService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        SchemaValidationService {
            inner,
            cache: self.cache.clone(),
            max_request_body_bytes: self.max_request_body_bytes,
        }
    }
}

#[derive(Clone)]
pub struct SchemaValidationService<S> {
    inner: S,
    cache: Arc<SchemaCache>,
    max_request_body_bytes: usize,
}

impl<S> Service<Request<ProxyBody>> for SchemaValidationService<S>
where
    S: Service<Request<ProxyBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Error: Send + 'static,
    S::Future: Send + 'static,
{
    type Response = http::Response<ResBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ProxyBody>) -> Self::Future {
        let cache = self.cache.clone();
        let max_request_body_bytes = self.max_request_body_bytes;
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let schema_identity = [WR_NAMESPACE, WR_MODULE, WR_VERSION].map(|name| {
                req.headers()
                    .get(name)
                    .and_then(|value| value.to_str().ok())
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned)
            });
            let [Some(namespace), Some(module), Some(version)] = schema_identity else {
                return Ok(error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "resolved route is missing schema identity",
                ));
            };
            let rpc_path = req.uri().path().to_owned();

            let (mut parts, body) = req.into_parts();
            let body = match Limited::new(body, max_request_body_bytes).collect().await {
                Ok(body) => body.to_bytes(),
                Err(error) if error.downcast_ref::<LengthLimitError>().is_some() => {
                    return Ok(error_response(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        "request body exceeds external.max_request_body_bytes",
                    ));
                }
                Err(error) => {
                    warn!(%error, "failed to read public ingress body");
                    return Ok(error_response(
                        StatusCode::BAD_REQUEST,
                        "failed to read request body",
                    ));
                }
            };

            let representation =
                match select_request_representation(&parts.headers, body.is_empty()) {
                    Ok(representation) => representation,
                    Err(_) => {
                        return Ok(error_response(
                            StatusCode::UNSUPPORTED_MEDIA_TYPE,
                            "request body has a missing, malformed, or unsupported media type",
                        ));
                    }
                };

            let input = match cache
                .input_descriptor(&namespace, &module, &version, &rpc_path)
                .await
            {
                Ok(Some(input)) => input,
                Ok(None) => {
                    return Ok(error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "configured rpc_path is not present in the module schema",
                    ));
                }
                Err(error) => {
                    warn!(
                        %namespace,
                        %module,
                        %version,
                        %rpc_path,
                        %error,
                        "public ingress schema unavailable"
                    );
                    return Ok(error_response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "module schema is unavailable",
                    ));
                }
            };

            let body = match transcode_request(input, representation, &body) {
                Ok(body) => body,
                Err(error) => {
                    let representation = match representation {
                        RequestRepresentation::Protobuf => "protobuf",
                        RequestRepresentation::Json => "JSON",
                        RequestRepresentation::Form => "form",
                    };
                    return Ok(error_response(
                        StatusCode::BAD_REQUEST,
                        &format!("request body failed {representation} schema validation: {error}"),
                    ));
                }
            };

            parts.headers.insert(
                CONTENT_TYPE,
                HeaderValue::from_static("application/x-protobuf"),
            );
            parts.headers.insert(
                CONTENT_LENGTH,
                HeaderValue::from_str(&body.len().to_string())
                    .expect("encoded body length is a valid header value"),
            );
            parts.headers.remove(CONTENT_ENCODING);
            parts.headers.remove(TRANSFER_ENCODING);
            for header in ["content-md5", "digest", "content-digest"] {
                parts.headers.remove(header);
            }

            inner
                .call(Request::from_parts(parts, ProxyBody::full(body)))
                .await
        })
    }
}
