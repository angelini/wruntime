use crate::bindings::wasi::http::{
    outgoing_handler,
    types::{Fields, Method as WasiMethod, OutgoingBody, OutgoingRequest, RequestOptions, Scheme},
};
use crate::bindings::wasi::io::streams::StreamError;
use std::time::Duration;

// ── Error type ──────────────────────────────────────────────────────────────

/// Errors from outbound HTTP requests.
#[derive(Debug)]
pub enum HttpError {
    /// The server returned a non-success HTTP status.
    Status { code: u16, body: Vec<u8> },
    /// A transport-level failure (DNS, connection refused, timeout, etc.).
    Transport(String),
    /// Failed to decode the response body.
    Decode(String),
}

impl std::fmt::Display for HttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HttpError::Status { code, body } => {
                let msg = String::from_utf8_lossy(body);
                write!(f, "rpc error: HTTP {code}: {msg}")
            }
            HttpError::Transport(msg) => write!(f, "transport error: {msg}"),
            HttpError::Decode(msg) => write!(f, "decode error: {msg}"),
        }
    }
}

impl HttpError {
    /// Returns the HTTP status code if this is a Status error.
    pub fn status_code(&self) -> Option<u16> {
        match self {
            HttpError::Status { code, .. } => Some(*code),
            _ => None,
        }
    }

    /// Returns true if this is a status error with the given code.
    pub fn is_status(&self, code: u16) -> bool {
        matches!(self, HttpError::Status { code: c, .. } if *c == code)
    }
}

// ── Request / Response types ────────────────────────────────────────────────

/// HTTP method for outbound requests.
pub enum Method {
    Get,
    Post,
    Put,
    Delete,
    Patch,
    Head,
    Options,
}

impl Method {
    fn to_wasi(&self) -> WasiMethod {
        match self {
            Method::Get => WasiMethod::Get,
            Method::Post => WasiMethod::Post,
            Method::Put => WasiMethod::Put,
            Method::Delete => WasiMethod::Delete,
            Method::Patch => WasiMethod::Patch,
            Method::Head => WasiMethod::Head,
            Method::Options => WasiMethod::Options,
        }
    }
}

/// An outbound HTTP request descriptor.
pub struct HttpRequest<'a> {
    pub authority: &'a str,
    pub path: &'a str,
    pub method: Method,
    pub headers: &'a [(&'a str, &'a [u8])],
    pub body: &'a [u8],
}

/// An HTTP response with status and body.
pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

impl HttpResponse {
    /// Interpret the body as UTF-8 text.
    pub fn text(&self) -> Result<&str, HttpError> {
        std::str::from_utf8(&self.body)
            .map_err(|e| HttpError::Decode(format!("invalid UTF-8: {e}")))
    }

    /// Decode the body as a protobuf message.
    #[cfg(feature = "prost")]
    pub fn decode<T: prost::Message + Default>(&self) -> Result<T, HttpError> {
        T::decode(self.body.as_slice()).map_err(|e| HttpError::Decode(e.to_string()))
    }

    /// Return `Err(HttpError::Status { ... })` if the status is not 2xx.
    pub fn error_for_status(self) -> Result<Self, HttpError> {
        if self.status >= 200 && self.status < 300 {
            Ok(self)
        } else {
            Err(HttpError::Status {
                code: self.status,
                body: self.body,
            })
        }
    }
}

// ── Timeout configuration ───────────────────────────────────────────────────

/// Timeout configuration for HTTP requests.
pub struct Timeouts {
    /// Timeout for the initial TCP/TLS connection.
    pub connect: Option<Duration>,
    /// Timeout for receiving the first byte of the response.
    pub first_byte: Option<Duration>,
    /// Timeout between consecutive chunks of the response body.
    pub between_bytes: Option<Duration>,
}

impl Timeouts {
    /// Set all three timeouts to the same duration.
    pub fn uniform(d: Duration) -> Self {
        Self {
            connect: Some(d),
            first_byte: Some(d),
            between_bytes: Some(d),
        }
    }
}

// ── Public API ──────────────────────────────────────────────────────────────

/// Execute an HTTP request and return the response.
pub fn http_request(req: &HttpRequest) -> Result<HttpResponse, HttpError> {
    do_request(req, None)
}

/// Execute an HTTP request with timeout configuration.
pub fn http_request_with_timeouts(
    req: &HttpRequest,
    timeouts: &Timeouts,
) -> Result<HttpResponse, HttpError> {
    let opts = RequestOptions::new();
    if let Some(d) = timeouts.connect {
        opts.set_connect_timeout(Some(d.as_nanos() as u64))
            .map_err(|_| HttpError::Transport("connect timeout not supported".into()))?;
    }
    if let Some(d) = timeouts.first_byte {
        opts.set_first_byte_timeout(Some(d.as_nanos() as u64))
            .map_err(|_| HttpError::Transport("first-byte timeout not supported".into()))?;
    }
    if let Some(d) = timeouts.between_bytes {
        opts.set_between_bytes_timeout(Some(d.as_nanos() as u64))
            .map_err(|_| HttpError::Transport("between-bytes timeout not supported".into()))?;
    }
    do_request(req, Some(opts))
}

/// Make a unary protobuf RPC call over WASI HTTP.
///
/// Sends a POST to `http://{authority}{path}` with the protobuf-encoded `body`
/// and returns the HTTP status code and response bytes on success.
///
/// This is a convenience wrapper around [`http_request`]. New code should
/// prefer `http_request` for typed errors and method flexibility.
pub fn http_rpc(authority: &str, path: &str, body: &[u8]) -> Result<(u16, Vec<u8>), String> {
    let req = HttpRequest {
        authority,
        path,
        method: Method::Post,
        headers: &[("content-type", b"application/x-protobuf" as &[u8])],
        body,
    };
    http_request(&req)
        .map(|r| (r.status, r.body))
        .map_err(|e| e.to_string())
}

// ── Internal ────────────────────────────────────────────────────────────────

fn do_request(
    req: &HttpRequest,
    options: Option<RequestOptions>,
) -> Result<HttpResponse, HttpError> {
    let headers = Fields::new();
    for (name, value) in req.headers {
        headers
            .set(name, &[value.to_vec()])
            .map_err(|e| HttpError::Transport(format!("set header {name}: {e:?}")))?;
    }

    let out_req = OutgoingRequest::new(headers);
    out_req
        .set_method(&req.method.to_wasi())
        .map_err(|_| HttpError::Transport("set method".into()))?;
    out_req
        .set_scheme(Some(&Scheme::Http))
        .map_err(|_| HttpError::Transport("set scheme".into()))?;
    out_req
        .set_authority(Some(req.authority))
        .map_err(|_| HttpError::Transport("set authority".into()))?;
    out_req
        .set_path_with_query(Some(req.path))
        .map_err(|_| HttpError::Transport("set path".into()))?;

    let outgoing_body = out_req
        .body()
        .map_err(|_| HttpError::Transport("get body".into()))?;
    if !req.body.is_empty() {
        let stream = outgoing_body
            .write()
            .map_err(|_| HttpError::Transport("get write stream".into()))?;
        for chunk in req.body.chunks(4096) {
            stream
                .blocking_write_and_flush(chunk)
                .map_err(|e| HttpError::Transport(format!("write: {e:?}")))?;
        }
    }
    OutgoingBody::finish(outgoing_body, None)
        .map_err(|e| HttpError::Transport(format!("finish body: {e:?}")))?;

    let future_resp = outgoing_handler::handle(out_req, options)
        .map_err(|e| HttpError::Transport(format!("handle: {e:?}")))?;

    loop {
        match future_resp.get() {
            Some(result) => {
                let response = result
                    .map_err(|()| HttpError::Transport("response error".into()))?
                    .map_err(|e| HttpError::Transport(format!("http error: {e:?}")))?;

                let status = response.status();
                let incoming_body = response
                    .consume()
                    .map_err(|_| HttpError::Transport("consume response".into()))?;
                let stream = incoming_body
                    .stream()
                    .map_err(|_| HttpError::Transport("response body stream".into()))?;

                let mut resp_bytes = Vec::new();
                loop {
                    match stream.blocking_read(8192) {
                        Ok(chunk) if chunk.is_empty() => break,
                        Ok(chunk) => resp_bytes.extend_from_slice(&chunk),
                        Err(StreamError::Closed) => break,
                        Err(StreamError::LastOperationFailed(_)) => break,
                    }
                }

                return Ok(HttpResponse {
                    status,
                    body: resp_bytes,
                });
            }
            None => {
                future_resp.subscribe().block();
            }
        }
    }
}
