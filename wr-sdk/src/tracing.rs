use crate::bindings::wruntime::tracing::span;

pub fn start(name: &str, attrs: &[(&str, &str)]) -> span::ActiveSpan {
    let owned: Vec<(String, String)> = attrs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    span::start(name, &owned)
}

pub fn set_attribute(span: &span::ActiveSpan, key: &str, value: &str) {
    span.set_attribute(key, value);
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
