use crate::bindings::wruntime::tracing::span;

pub use span::AttributeValue;

/// One owned tracing attribute suitable for the dynamic tracing helpers.
pub type Attribute = (String, AttributeValue);

/// Convert a Rust value into a typed OpenTelemetry attribute.
///
/// Unsigned values whose full range does not fit in `i64` are deliberately
/// unsupported. Convert them explicitly so overflow handling remains visible:
///
/// ```
/// use wr_sdk::tracing::{IntoAttributeValue as _, AttributeValue};
///
/// let signed = i64::try_from(42_u64).expect("attribute fits in i64");
/// assert!(matches!(signed.into_attribute_value(), AttributeValue::Signed(42)));
/// ```
///
/// ```compile_fail
/// use wr_sdk::tracing::IntoAttributeValue as _;
/// let _ = 42_u64.into_attribute_value();
/// ```
///
/// ```compile_fail
/// use wr_sdk::tracing::IntoAttributeValue as _;
/// let _ = 42_usize.into_attribute_value();
/// ```
///
/// ```compile_fail
/// use wr_sdk::tracing::IntoAttributeValue as _;
/// let _ = vec![1_u64, 2].into_attribute_value();
/// ```
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

impl IntoAttributeValue for Vec<String> {
    fn into_attribute_value(self) -> AttributeValue {
        AttributeValue::TextArray(self)
    }
}

impl IntoAttributeValue for &[String] {
    fn into_attribute_value(self) -> AttributeValue {
        AttributeValue::TextArray(self.to_vec())
    }
}

impl IntoAttributeValue for Vec<&str> {
    fn into_attribute_value(self) -> AttributeValue {
        AttributeValue::TextArray(self.into_iter().map(str::to_string).collect())
    }
}

impl IntoAttributeValue for &[&str] {
    fn into_attribute_value(self) -> AttributeValue {
        AttributeValue::TextArray(self.iter().map(|value| (*value).to_string()).collect())
    }
}

impl IntoAttributeValue for Vec<bool> {
    fn into_attribute_value(self) -> AttributeValue {
        AttributeValue::BooleanArray(self)
    }
}

impl IntoAttributeValue for &[bool] {
    fn into_attribute_value(self) -> AttributeValue {
        AttributeValue::BooleanArray(self.to_vec())
    }
}

impl IntoAttributeValue for Vec<i64> {
    fn into_attribute_value(self) -> AttributeValue {
        AttributeValue::SignedArray(self)
    }
}

impl IntoAttributeValue for &[i64] {
    fn into_attribute_value(self) -> AttributeValue {
        AttributeValue::SignedArray(self.to_vec())
    }
}

impl IntoAttributeValue for Vec<f64> {
    fn into_attribute_value(self) -> AttributeValue {
        AttributeValue::FloatArray(self)
    }
}

impl IntoAttributeValue for &[f64] {
    fn into_attribute_value(self) -> AttributeValue {
        AttributeValue::FloatArray(self.to_vec())
    }
}

/// Start a child span from dynamic typed attributes.
pub fn start(name: &str, attrs: &[Attribute]) -> span::ActiveSpan {
    span::start(name, attrs)
}

/// Start a new root span from dynamic typed attributes.
pub fn start_root(name: &str, attrs: &[Attribute]) -> span::ActiveSpan {
    span::start_root(name, attrs)
}

/// Start a span from macro-owned attributes.
#[doc(hidden)]
pub fn start_owned(name: &str, attrs: Vec<Attribute>) -> span::ActiveSpan {
    start(name, &attrs)
}

/// Start a root span from macro-owned attributes.
#[doc(hidden)]
pub fn start_root_owned(name: &str, attrs: Vec<Attribute>) -> span::ActiveSpan {
    start_root(name, &attrs)
}

fn set_attrs_with(
    span: &span::ActiveSpan,
    attrs: &[Attribute],
    finish: impl FnOnce(&span::ActiveSpan, &[Attribute]),
) {
    finish(span, attrs);
}

/// Set dynamic typed attributes in one guest/host crossing.
pub fn set_attrs(span: &span::ActiveSpan, attrs: &[Attribute]) {
    set_attrs_with(span, attrs, |span, attrs| span.set_attributes(attrs));
}

fn set_attr_with(
    span: &span::ActiveSpan,
    key: &str,
    value: impl IntoAttributeValue,
    finish: impl FnOnce(&span::ActiveSpan, &[Attribute]),
) {
    let attrs = [(key.to_string(), value.into_attribute_value())];
    set_attrs_with(span, &attrs, finish);
}

/// Set one attribute through the bulk transport operation.
pub fn set_attr(span: &span::ActiveSpan, key: &str, value: impl IntoAttributeValue) {
    set_attr_with(span, key, value, set_attrs);
}

/// Record an event with dynamic typed attributes.
pub fn record_event(span: &span::ActiveSpan, name: &str, attrs: &[Attribute]) {
    span.record_event(name, attrs);
}

pub fn set_error(span: &span::ActiveSpan, message: &str) {
    span.set_error(message);
}

#[doc(hidden)]
#[macro_export]
macro_rules! __wr_tracing_with_name_attrs {
    ($name:expr, [$($key:expr => $val:expr),* $(,)?], $finish:expr) => {{
        let __wr_name = $name;
        let __wr_attrs = vec![$(($key.to_string(), $crate::tracing::IntoAttributeValue::into_attribute_value($val))),*];
        ($finish)(__wr_name, __wr_attrs)
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __wr_tracing_with_span_attrs {
    ($span:expr, [$($key:expr => $val:expr),* $(,)?], $finish:expr) => {{
        let __wr_span = &$span;
        let __wr_attrs = vec![$(($key.to_string(), $crate::tracing::IntoAttributeValue::into_attribute_value($val))),*];
        ($finish)(__wr_span, &__wr_attrs)
    }};
}

#[macro_export]
macro_rules! set_attrs {
    ($span:expr $(, $key:expr => $val:expr)* $(,)?) => {
        $crate::__wr_tracing_with_span_attrs!(
            $span,
            [$($key => $val,)*],
            $crate::tracing::set_attrs
        )
    };
}

#[macro_export]
macro_rules! span {
    ($name:expr $(, $key:expr => $val:expr)* $(,)?) => {
        $crate::__wr_tracing_with_name_attrs!(
            $name,
            [$($key => $val,)*],
            $crate::tracing::start_owned
        )
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __wr_tracing_with_event_attrs {
    ($span:expr, $name:expr, [$($key:expr => $val:expr),* $(,)?], $finish:expr) => {{
        let __wr_span = &$span;
        let __wr_name = $name;
        let __wr_attrs = vec![$(($key.to_string(), $crate::tracing::IntoAttributeValue::into_attribute_value($val))),*];
        ($finish)(__wr_span, __wr_name, &__wr_attrs)
    }};
}

#[macro_export]
macro_rules! event {
    ($span:expr, $name:expr $(, $key:expr => $val:expr)* $(,)?) => {
        $crate::__wr_tracing_with_event_attrs!(
            $span,
            $name,
            [$($key => $val,)*],
            $crate::tracing::record_event
        )
    };
}

#[macro_export]
macro_rules! root_span {
    ($name:expr $(, $key:expr => $val:expr)* $(,)?) => {
        $crate::__wr_tracing_with_name_attrs!(
            $name,
            [$($key => $val,)*],
            $crate::tracing::start_root_owned
        )
    };
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::mem::ManuallyDrop;

    use super::{span, AttributeValue, IntoAttributeValue as _};

    #[test]
    fn typed_conversions_preserve_supported_values() {
        assert!(matches!(
            "value".into_attribute_value(),
            AttributeValue::Text(value) if value == "value"
        ));
        assert!(matches!(
            true.into_attribute_value(),
            AttributeValue::Boolean(true)
        ));
        assert!(matches!(
            (-42_i64).into_attribute_value(),
            AttributeValue::Signed(-42)
        ));
        assert!(matches!(
            1.5_f64.into_attribute_value(),
            AttributeValue::Float(value) if value == 1.5
        ));
        assert!(matches!(
            vec!["a", "b"].into_attribute_value(),
            AttributeValue::TextArray(values) if values == ["a", "b"]
        ));
        assert!(matches!(
            vec![true, false].into_attribute_value(),
            AttributeValue::BooleanArray(values) if values == [true, false]
        ));
        assert!(matches!(
            vec![-1_i64, 2].into_attribute_value(),
            AttributeValue::SignedArray(values) if values == [-1, 2]
        ));
        assert!(matches!(
            vec![1.25_f64, 2.5].into_attribute_value(),
            AttributeValue::FloatArray(values) if values == [1.25, 2.5]
        ));
        assert!(matches!(
            Vec::<i64>::new().into_attribute_value(),
            AttributeValue::SignedArray(values) if values.is_empty()
        ));
    }

    #[test]
    fn checked_unsigned_conversion_is_lossless() {
        let signed = i64::try_from(u64::MAX / 2).expect("value fits");
        assert!(matches!(
            signed.into_attribute_value(),
            AttributeValue::Signed(value) if value == signed
        ));
        assert!(i64::try_from(u64::MAX).is_err());
    }

    #[test]
    fn singular_and_empty_late_attributes_use_bulk_seam() {
        let span = ManuallyDrop::new(unsafe { span::ActiveSpan::from_handle(2) });

        let empty_calls = Cell::new(0);
        super::set_attrs_with(&span, &[], |_, attrs| {
            empty_calls.set(empty_calls.get() + 1);
            assert!(attrs.is_empty());
        });
        assert_eq!(empty_calls.get(), 1);

        let singular_calls = Cell::new(0);
        super::set_attr_with(&span, "single", 42_i64, |_, attrs| {
            singular_calls.set(singular_calls.get() + 1);
            assert_eq!(attrs.len(), 1);
            assert_eq!(attrs[0].0.as_str(), "single");
            assert!(matches!(&attrs[0].1, AttributeValue::Signed(42)));
        });
        assert_eq!(singular_calls.get(), 1);
    }

    #[test]
    fn macros_evaluate_span_name_and_fields_once() {
        let name_count = Cell::new(0);
        let key_count = Cell::new(0);
        let value_count = Cell::new(0);
        crate::__wr_tracing_with_name_attrs!(
            {
                name_count.set(name_count.get() + 1);
                "test-span"
            },
            [{
                key_count.set(key_count.get() + 1);
                "key"
            } => {
                value_count.set(value_count.get() + 1);
                true
            }],
            |name, attrs: Vec<super::Attribute>| {
                assert_eq!(name, "test-span");
                assert_eq!(attrs.len(), 1);
            }
        );
        assert_eq!(name_count.get(), 1);
        assert_eq!(key_count.get(), 1);
        assert_eq!(value_count.get(), 1);

        let span_count = Cell::new(0);
        let key_count = Cell::new(0);
        let value_count = Cell::new(0);
        let span = ManuallyDrop::new(unsafe { span::ActiveSpan::from_handle(1) });
        crate::__wr_tracing_with_span_attrs!(
            {
                span_count.set(span_count.get() + 1);
                &*span
            },
            [{
                key_count.set(key_count.get() + 1);
                "late.key"
            } => {
                value_count.set(value_count.get() + 1);
                42_i64
            }],
            |_, attrs: &[super::Attribute]| assert_eq!(attrs.len(), 1)
        );
        assert_eq!(span_count.get(), 1);
        assert_eq!(key_count.get(), 1);
        assert_eq!(value_count.get(), 1);

        let event_span_count = Cell::new(0);
        let event_name_count = Cell::new(0);
        let event_key_count = Cell::new(0);
        let event_value_count = Cell::new(0);
        crate::__wr_tracing_with_event_attrs!(
            {
                event_span_count.set(event_span_count.get() + 1);
                &*span
            },
            {
                event_name_count.set(event_name_count.get() + 1);
                "test.event"
            },
            [{
                event_key_count.set(event_key_count.get() + 1);
                "event.key"
            } => {
                event_value_count.set(event_value_count.get() + 1);
                false
            }],
            |_, name: &str, attrs: &[super::Attribute]| {
                assert_eq!(name, "test.event");
                assert_eq!(attrs.len(), 1);
            }
        );
        assert_eq!(event_span_count.get(), 1);
        assert_eq!(event_name_count.get(), 1);
        assert_eq!(event_key_count.get(), 1);
        assert_eq!(event_value_count.get(), 1);
    }
}
