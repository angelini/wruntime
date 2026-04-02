use crate::bindings::wasi::http::{
    outgoing_handler,
    types::{Fields, Method, OutgoingBody, OutgoingRequest, Scheme},
};
use crate::bindings::wasi::io::streams::StreamError;

/// Make a unary protobuf RPC call over WASI HTTP.
///
/// Sends a POST to `http://{authority}{path}` with the protobuf-encoded `body`
/// and returns the HTTP status code and response bytes on success.
pub fn http_rpc(authority: &str, path: &str, body: &[u8]) -> Result<(u16, Vec<u8>), String> {
    let headers = Fields::new();
    headers
        .set("content-type", &[b"application/x-protobuf".to_vec()])
        .map_err(|e| format!("set header: {:?}", e))?;

    let req = OutgoingRequest::new(headers);
    req.set_method(&Method::Post).map_err(|_| "set method")?;
    req.set_scheme(Some(&Scheme::Http))
        .map_err(|_| "set scheme")?;
    req.set_authority(Some(authority))
        .map_err(|_| "set authority")?;
    req.set_path_with_query(Some(path))
        .map_err(|_| "set path")?;

    let outgoing_body = req.body().map_err(|_| "get body")?;
    {
        let stream = outgoing_body.write().map_err(|_| "get write stream")?;
        for chunk in body.chunks(4096) {
            stream
                .blocking_write_and_flush(chunk)
                .map_err(|e| format!("write: {:?}", e))?;
        }
    }
    OutgoingBody::finish(outgoing_body, None).map_err(|e| format!("finish body: {:?}", e))?;

    let future_resp =
        outgoing_handler::handle(req, None).map_err(|e| format!("handle: {:?}", e))?;

    loop {
        match future_resp.get() {
            Some(result) => {
                let response = result
                    .map_err(|()| "response error".to_string())?
                    .map_err(|e| format!("http error: {:?}", e))?;

                let status = response.status();
                let incoming_body = response.consume().map_err(|_| "consume response")?;
                let stream = incoming_body.stream().map_err(|_| "response body stream")?;

                let mut resp_bytes = Vec::new();
                loop {
                    match stream.blocking_read(8192) {
                        Ok(chunk) if chunk.is_empty() => break,
                        Ok(chunk) => resp_bytes.extend_from_slice(&chunk),
                        Err(StreamError::Closed) => break,
                        Err(StreamError::LastOperationFailed(_)) => break,
                    }
                }

                return Ok((status, resp_bytes));
            }
            None => {
                future_resp.subscribe().block();
            }
        }
    }
}
