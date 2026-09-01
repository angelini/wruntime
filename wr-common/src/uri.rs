/// Return a URI-like value suitable for telemetry by removing its complete query.
///
/// This intentionally operates on text instead of parsing. Telemetry must not fall
/// back to the original value when an upstream header contains a malformed URI, and
/// routing continues to use the untouched request URI/header separately.
pub fn uri_for_telemetry(value: &str) -> &str {
    value.split_once('?').map_or(value, |(base, _)| base)
}

#[cfg(test)]
mod tests {
    use super::uri_for_telemetry;

    #[test]
    fn preserves_uri_without_query() {
        assert_eq!(
            uri_for_telemetry("https://objects.example/reports/file.pdf"),
            "https://objects.example/reports/file.pdf"
        );
    }

    #[test]
    fn removes_complete_signed_and_unrelated_query() {
        let value = "https://objects.example/bucket/key?X-Amz-Credential=secret&X-Amz-Signature=signature&token=also-secret";
        let redacted = uri_for_telemetry(value);
        assert_eq!(redacted, "https://objects.example/bucket/key");
        assert!(!redacted.contains("X-Amz-"));
        assert!(!redacted.contains("secret"));
        assert!(!redacted.contains('?'));
    }

    #[test]
    fn safely_redacts_malformed_uri_text() {
        assert_eq!(
            uri_for_telemetry("not a valid uri?api_key=secret"),
            "not a valid uri"
        );
        assert_eq!(uri_for_telemetry("?secret"), "");
    }
}
