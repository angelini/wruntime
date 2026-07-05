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

/// Reserved `x-wr-*` header names used for internal routing and identity.
/// Stripped at trust boundaries (see the `strip_*` helpers) so external callers
/// cannot spoof internal routing identity.
pub const WR_DESTINATION: &str = "x-wr-destination";
pub const WR_SOURCE: &str = "x-wr-source";
pub const WR_SOURCE_NS: &str = "x-wr-source-ns";
pub const WR_MODULE: &str = "x-wr-module";
pub const WR_NAMESPACE: &str = "x-wr-namespace";
pub const WR_VERSION: &str = "x-wr-version";
pub const WR_VIA_PROXY: &str = "x-wr-via-proxy";

/// Remove the engine-internal routing headers before forwarding to a local
/// engine (the `Destination::LocalEngine` boundary in the proxy forward layer).
pub fn strip_before_engine(headers: &mut http::HeaderMap) {
    headers.remove(WR_DESTINATION);
    headers.remove(WR_SOURCE);
    headers.remove(WR_SOURCE_NS);
    headers.remove(WR_VIA_PROXY);
}

/// Remove every reserved `x-wr-*` header from an inbound external request so
/// external callers cannot spoof internal routing identity (ingress boundary).
pub fn strip_external_spoofable_headers(headers: &mut http::HeaderMap) {
    headers.remove(WR_DESTINATION);
    headers.remove(WR_SOURCE);
    headers.remove(WR_SOURCE_NS);
    headers.remove(WR_MODULE);
    headers.remove(WR_NAMESPACE);
    headers.remove(WR_VERSION);
    headers.remove(WR_VIA_PROXY);
}

/// Remove every reserved `x-wr-*` header before forwarding out to an external
/// host (egress boundary). Kept distinct from
/// [`strip_external_spoofable_headers`] so the two lifecycles can diverge later.
pub fn strip_before_egress(headers: &mut http::HeaderMap) {
    headers.remove(WR_DESTINATION);
    headers.remove(WR_SOURCE);
    headers.remove(WR_SOURCE_NS);
    headers.remove(WR_MODULE);
    headers.remove(WR_NAMESPACE);
    headers.remove(WR_VERSION);
    headers.remove(WR_VIA_PROXY);
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

    fn all_wr_headers_plus_content_type() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(WR_DESTINATION, HeaderValue::from_static("http://ns.mod/"));
        headers.insert(WR_SOURCE, HeaderValue::from_static("ns.mod"));
        headers.insert(WR_SOURCE_NS, HeaderValue::from_static("ns"));
        headers.insert(WR_MODULE, HeaderValue::from_static("mod"));
        headers.insert(WR_NAMESPACE, HeaderValue::from_static("ns"));
        headers.insert(WR_VERSION, HeaderValue::from_static("1.0.0"));
        headers.insert(WR_VIA_PROXY, HeaderValue::from_static("1"));
        headers.insert("content-type", HeaderValue::from_static("text/plain"));
        headers
    }

    #[test]
    fn strip_before_engine_removes_only_engine_internal_headers() {
        let mut headers = all_wr_headers_plus_content_type();
        strip_before_engine(&mut headers);
        assert!(headers.get(WR_DESTINATION).is_none());
        assert!(headers.get(WR_SOURCE).is_none());
        assert!(headers.get(WR_SOURCE_NS).is_none());
        assert!(headers.get(WR_VIA_PROXY).is_none());
        // Not part of the 4-header engine set — must survive.
        assert!(headers.get(WR_MODULE).is_some());
        assert!(headers.get(WR_NAMESPACE).is_some());
        assert!(headers.get(WR_VERSION).is_some());
        assert_eq!(header_str(&headers, "content-type"), "text/plain");
    }

    #[test]
    fn strip_external_spoofable_headers_removes_all_wr_headers() {
        let mut headers = all_wr_headers_plus_content_type();
        strip_external_spoofable_headers(&mut headers);
        for name in [
            WR_DESTINATION,
            WR_SOURCE,
            WR_SOURCE_NS,
            WR_MODULE,
            WR_NAMESPACE,
            WR_VERSION,
            WR_VIA_PROXY,
        ] {
            assert!(headers.get(name).is_none(), "{name} should be stripped");
        }
        assert_eq!(header_str(&headers, "content-type"), "text/plain");
    }

    #[test]
    fn strip_before_egress_removes_all_wr_headers() {
        let mut headers = all_wr_headers_plus_content_type();
        strip_before_egress(&mut headers);
        for name in [
            WR_DESTINATION,
            WR_SOURCE,
            WR_SOURCE_NS,
            WR_MODULE,
            WR_NAMESPACE,
            WR_VERSION,
            WR_VIA_PROXY,
        ] {
            assert!(headers.get(name).is_none(), "{name} should be stripped");
        }
        assert_eq!(header_str(&headers, "content-type"), "text/plain");
    }
}
