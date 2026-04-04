use crate::bindings::wasi::http::types::{
    Fields, IncomingBody, OutgoingBody, OutgoingResponse, ResponseOutparam,
};
use crate::bindings::wasi::io::streams::StreamError;

/// Drain an `IncomingBody` into a `Vec<u8>`.
pub fn read_body(incoming: IncomingBody) -> Vec<u8> {
    let stream = incoming.stream().unwrap();
    let mut bytes = Vec::new();
    loop {
        match stream.blocking_read(8192) {
            Ok(chunk) if chunk.is_empty() => break,
            Ok(chunk) => bytes.extend_from_slice(&chunk),
            Err(StreamError::Closed) => break,
            Err(_) => break,
        }
    }
    drop(stream);
    IncomingBody::finish(incoming);
    bytes
}

/// Write a response with the given status and body bytes.
pub fn send_response(response_out: ResponseOutparam, status: u16, body: Vec<u8>) {
    let headers = Fields::new();
    let _ = headers.set("content-type", &[b"application/x-protobuf".to_vec()]);

    let resp = OutgoingResponse::new(headers);
    let _ = resp.set_status_code(status);

    let out_body = resp.body().unwrap();
    {
        let stream = out_body.write().unwrap();
        for chunk in body.chunks(4096) {
            let _ = stream.blocking_write_and_flush(chunk);
        }
    }

    ResponseOutparam::set(response_out, Ok(resp));
    let _ = OutgoingBody::finish(out_body, None);
}

/// Write a response with the given status, content-type, and body bytes.
pub fn send_json_response(response_out: ResponseOutparam, status: u16, body: Vec<u8>) {
    send_response_with_content_type(response_out, status, body, "application/json");
}

/// Write a response with a custom content-type header.
pub fn send_response_with_content_type(
    response_out: ResponseOutparam,
    status: u16,
    body: Vec<u8>,
    content_type: &str,
) {
    let headers = Fields::new();
    let _ = headers.set("content-type", &[content_type.as_bytes().to_vec()]);

    let resp = OutgoingResponse::new(headers);
    let _ = resp.set_status_code(status);

    let out_body = resp.body().unwrap();
    {
        let stream = out_body.write().unwrap();
        for chunk in body.chunks(4096) {
            let _ = stream.blocking_write_and_flush(chunk);
        }
    }

    ResponseOutparam::set(response_out, Ok(resp));
    let _ = OutgoingBody::finish(out_body, None);
}

/// Return a JSON error body with the given status code.
pub fn err_body(status: u16, msg: &str) -> (u16, Vec<u8>) {
    (status, format!(r#"{{"error":"{}"}}"#, msg).into_bytes())
}
