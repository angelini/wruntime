use crate::bindings::wruntime::tracing::span;

pub use span::AttributeValue;

/// Convert a Rust value into a typed OpenTelemetry attribute.
pub trait IntoAttributeValue {
    fn into_attribute_value(self) -> AttributeValue;
}

impl IntoAttributeValue for AttributeValue {
    fn into_attribute_value(self) -> AttributeValue {
        self
    }
}

impl IntoAttributeValue for &str {
    fn into_attribute_value(self) -> AttributeValue {
        AttributeValue::Text(self.to_string())
    }
}

impl IntoAttributeValue for String {
    fn into_attribute_value(self) -> AttributeValue {
        AttributeValue::Text(self)
    }
}

impl IntoAttributeValue for &String {
    fn into_attribute_value(self) -> AttributeValue {
        AttributeValue::Text(self.clone())
    }
}

impl IntoAttributeValue for bool {
    fn into_attribute_value(self) -> AttributeValue {
        AttributeValue::Boolean(self)
    }
}

macro_rules! signed_attribute {
    ($($ty:ty),+ $(,)?) => {$ (
        impl IntoAttributeValue for $ty {
            fn into_attribute_value(self) -> AttributeValue {
                AttributeValue::Signed(self as i64)
            }
        }
    )+ };
}

signed_attribute!(i8, i16, i32, i64, isize, u8, u16, u32);

impl IntoAttributeValue for usize {
    fn into_attribute_value(self) -> AttributeValue {
        i64::try_from(self)
            .map(AttributeValue::Signed)
            .unwrap_or_else(|_| AttributeValue::Float(self as f64))
    }
}

impl IntoAttributeValue for u64 {
    fn into_attribute_value(self) -> AttributeValue {
        i64::try_from(self)
            .map(AttributeValue::Signed)
            .unwrap_or_else(|_| AttributeValue::Float(self as f64))
    }
}

impl IntoAttributeValue for f32 {
    fn into_attribute_value(self) -> AttributeValue {
        AttributeValue::Float(self as f64)
    }
}

impl IntoAttributeValue for f64 {
    fn into_attribute_value(self) -> AttributeValue {
        AttributeValue::Float(self)
    }
}

fn string_attrs(attrs: &[(&str, &str)]) -> Vec<(String, AttributeValue)> {
    attrs
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).into_attribute_value()))
        .collect()
}

pub fn start(name: &str, attrs: &[(&str, &str)]) -> span::ActiveSpan {
    span::start(name, &string_attrs(attrs))
}

/// Start a new root span (fresh trace). All subsequent outbound HTTP
/// requests will be parented to this span until it is dropped.
pub fn start_root(name: &str, attrs: &[(&str, &str)]) -> span::ActiveSpan {
    span::start_root(name, &string_attrs(attrs))
}

/// Start a span from pre-built typed attribute pairs. Used by the `span!` macro.
#[doc(hidden)]
pub fn start_owned(name: &str, attrs: Vec<(String, AttributeValue)>) -> span::ActiveSpan {
    span::start(name, &attrs)
}

/// Start a root span from pre-built typed attribute pairs. Used by `root_span!`.
#[doc(hidden)]
pub fn start_root_owned(name: &str, attrs: Vec<(String, AttributeValue)>) -> span::ActiveSpan {
    span::start_root(name, &attrs)
}

pub fn set_attr(span: &span::ActiveSpan, key: &str, value: impl IntoAttributeValue) {
    span.set_attribute(key, &value.into_attribute_value());
}

pub fn record_event(span: &span::ActiveSpan, name: &str, attrs: &[(&str, &str)]) {
    span.record_event(name, &string_attrs(attrs));
}

pub fn set_error(span: &span::ActiveSpan, message: &str) {
    span.set_error(message);
}

#[macro_export]
macro_rules! span {
    ($name:expr $(, $key:expr => $val:expr)* $(,)?) => {
        $crate::tracing::start_owned(
            $name,
            vec![$(($key.to_string(), $crate::tracing::IntoAttributeValue::into_attribute_value($val)),)*],
        )
    };
}

#[macro_export]
macro_rules! root_span {
    ($name:expr $(, $key:expr => $val:expr)* $(,)?) => {
        $crate::tracing::start_root_owned(
            $name,
            vec![$(($key.to_string(), $crate::tracing::IntoAttributeValue::into_attribute_value($val)),)*],
        )
    };
}
