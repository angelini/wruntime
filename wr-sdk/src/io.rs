use crate::bindings::wasi::http::types::{
    Fields, IncomingBody, OutgoingBody, OutgoingResponse, ResponseOutparam,
};
use crate::bindings::wasi::io::streams::StreamError;

pub const CONTENT_TYPE_PROTOBUF: &str = "application/x-protobuf";
pub const CONTENT_TYPE_JSON: &str = "application/json";

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
    send_response_with_content_type(response_out, status, body, CONTENT_TYPE_PROTOBUF);
}

/// Write a response with the given status, content-type, and body bytes.
pub fn send_json_response(response_out: ResponseOutparam, status: u16, body: Vec<u8>) {
    send_response_with_content_type(response_out, status, body, CONTENT_TYPE_JSON);
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

/// Response returned by generated service routers.
pub struct ServiceResponse {
    pub status: u16,
    pub body: Vec<u8>,
    pub content_type: &'static str,
}

impl ServiceResponse {
    pub fn new(status: u16, body: Vec<u8>, content_type: &'static str) -> Self {
        Self {
            status,
            body,
            content_type,
        }
    }

    pub fn protobuf(status: u16, body: Vec<u8>) -> Self {
        Self::new(status, body, CONTENT_TYPE_PROTOBUF)
    }

    pub fn json(status: u16, body: Vec<u8>) -> Self {
        Self::new(status, body, CONTENT_TYPE_JSON)
    }

    pub fn json_error(status: u16, msg: &str) -> Self {
        Self::json(status, json_error_body(msg))
    }
}

/// Write a generated service response with its declared content-type.
pub fn send_service_response(response_out: ResponseOutparam, response: ServiceResponse) {
    send_response_with_content_type(
        response_out,
        response.status,
        response.body,
        response.content_type,
    );
}

fn json_error_body(msg: &str) -> Vec<u8> {
    let mut out = String::with_capacity(msg.len() + 12);
    out.push_str("{\"error\":\"");
    push_json_escaped_string(&mut out, msg);
    out.push_str("\"}");
    out.into_bytes()
}

fn push_json_escaped_string(out: &mut String, value: &str) {
    use core::fmt::Write;

    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if c <= '\u{1f}' => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
}

/// Return a JSON error body with the given status code.
pub fn err_body(status: u16, msg: &str) -> (u16, Vec<u8>) {
    (status, json_error_body(msg))
}

/// Serialize a value as JSON and return it as a `(status, body)` tuple.
///
/// Fits the router return convention for modules that serve JSON:
/// ```rust,ignore
/// fn handle_list() -> (u16, Vec<u8>) {
///     let items = vec!["a", "b"];
///     json_body(200, &items)
/// }
/// ```
#[cfg(feature = "serde")]
pub fn json_body(status: u16, value: &impl serde::Serialize) -> (u16, Vec<u8>) {
    match serde_json::to_vec(value) {
        Ok(bytes) => (status, bytes),
        Err(e) => err_body(500, &format!("json serialize: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn err_body_escapes_json_strings() {
        let (_, body) = err_body(
            400,
            "quote \" slash \\ newline \n tab \t bell \u{0007} snowman ☃",
        );
        assert_eq!(
            String::from_utf8(body).unwrap(),
            "{\"error\":\"quote \\\" slash \\\\ newline \\n tab \\t bell \\u0007 snowman ☃\"}"
        );
    }

    #[test]
    fn service_json_error_matches_err_body() {
        let msg = "bad \"request\"\\input";
        let (_, tuple_body) = err_body(418, msg);
        let resp = ServiceResponse::json_error(418, msg);
        assert_eq!(resp.status, 418);
        assert_eq!(resp.content_type, CONTENT_TYPE_JSON);
        assert_eq!(resp.body, tuple_body);
    }
}
