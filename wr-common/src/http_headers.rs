/// Extracts a header value as a borrowed `&str`, returning `"unknown"` if
/// the header is missing or not valid UTF-8.
pub fn header_str<'a>(headers: &'a http::HeaderMap, name: &str) -> &'a str {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown")
}

/// Extracts a header value as an owned `String`, returning `"unknown"` if
/// the header is missing or not valid UTF-8.
pub fn header_owned(headers: &http::HeaderMap, name: &str) -> String {
    header_str(headers, name).to_owned()
}
