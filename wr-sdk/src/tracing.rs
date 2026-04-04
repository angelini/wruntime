use crate::bindings::wruntime::tracing::span;

pub fn start(name: &str, attrs: &[(&str, &str)]) -> span::ActiveSpan {
    let owned: Vec<(String, String)> = attrs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    span::start(name, &owned)
}

/// Set a span attribute, converting the value to a string via `Display`.
pub fn set_attr(span: &span::ActiveSpan, key: &str, value: impl std::fmt::Display) {
    span.set_attribute(key, &value.to_string());
}

pub fn record_event(span: &span::ActiveSpan, name: &str, attrs: &[(&str, &str)]) {
    let owned: Vec<(String, String)> = attrs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    span.record_event(name, &owned);
}

pub fn set_error(span: &span::ActiveSpan, message: &str) {
    span.set_error(message);
}
