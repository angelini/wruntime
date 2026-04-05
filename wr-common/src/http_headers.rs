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

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, HeaderValue};

    #[test]
    fn header_str_returns_value_when_present() {
        let mut headers = HeaderMap::new();
        headers.insert("x-wr-source", HeaderValue::from_static("ns.mod"));
        assert_eq!(header_str(&headers, "x-wr-source"), "ns.mod");
    }

    #[test]
    fn header_str_returns_unknown_when_missing() {
        let headers = HeaderMap::new();
        assert_eq!(header_str(&headers, "x-wr-source"), "unknown");
    }

    #[test]
    fn header_str_returns_unknown_for_non_utf8() {
        let mut headers = HeaderMap::new();
        // bytes 0x80..0xFF are not valid UTF-8
        headers.insert("x-bad", HeaderValue::from_bytes(&[0x80, 0x81]).unwrap());
        assert_eq!(header_str(&headers, "x-bad"), "unknown");
    }

    #[test]
    fn header_owned_returns_owned_string() {
        let mut headers = HeaderMap::new();
        headers.insert("x-wr-module", HeaderValue::from_static("inventory"));
        let val: String = header_owned(&headers, "x-wr-module");
        assert_eq!(val, "inventory");
    }

    #[test]
    fn header_owned_returns_unknown_when_missing() {
        let headers = HeaderMap::new();
        assert_eq!(header_owned(&headers, "x-nothing"), "unknown");
    }

    #[test]
    fn header_str_is_case_insensitive() {
        let mut headers = HeaderMap::new();
        headers.insert("Content-Type", HeaderValue::from_static("text/plain"));
        // HTTP headers are case-insensitive per spec; HeaderMap normalizes to lowercase
        assert_eq!(header_str(&headers, "content-type"), "text/plain");
    }
}
