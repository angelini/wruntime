use crate::bindings::wruntime::tracing::span;

pub fn start(name: &str, attrs: &[(&str, &str)]) -> span::ActiveSpan {
    let owned: Vec<(String, String)> = attrs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    span::start(name, &owned)
}

/// Start a new root span (fresh trace). All subsequent outbound HTTP
/// requests will be parented to this span until it is dropped.
/// Use this to group a batch of related outbound calls under one trace.
pub fn start_root(name: &str, attrs: &[(&str, &str)]) -> span::ActiveSpan {
    let owned: Vec<(String, String)> = attrs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    span::start_root(name, &owned)
}

/// Start a span from pre-built owned attr pairs. Used by the `span!` macro.
#[doc(hidden)]
pub fn start_owned(name: &str, attrs: Vec<(String, String)>) -> span::ActiveSpan {
    span::start(name, &attrs)
}

/// Start a root span from pre-built owned attr pairs. Used by the `root_span!` macro.
#[doc(hidden)]
pub fn start_root_owned(name: &str, attrs: Vec<(String, String)>) -> span::ActiveSpan {
    span::start_root(name, &attrs)
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

/// Start a span with attributes that accept any `Display` value.
///
/// ```rust,ignore
/// let sp = wr_sdk::span!("inventory.buy",
///     "product.id" => req.product_id.as_str(),
///     "product.quantity" => req.quantity,
/// );
/// ```
#[macro_export]
macro_rules! span {
    ($name:expr $(, $key:expr => $val:expr)* $(,)?) => {
        $crate::tracing::start_owned(
            $name,
            vec![$(($key.to_string(), ::std::format!("{}", $val)),)*],
        )
    };
}

/// Start a root span (fresh trace) with attributes. All subsequent outbound
/// HTTP requests will be parented to this span until it is dropped.
///
/// ```rust,ignore
/// let _trace = wr_sdk::root_span!("simulation.order",
///     "trader.id" => trader_id,
/// );
/// // outbound calls here share this trace
/// ```
#[macro_export]
macro_rules! root_span {
    ($name:expr $(, $key:expr => $val:expr)* $(,)?) => {
        $crate::tracing::start_root_owned(
            $name,
            vec![$(($key.to_string(), ::std::format!("{}", $val)),)*],
        )
    };
}
