mod helpers;

use std::sync::OnceLock;

use helpers::wasm::tracing_state;
use opentelemetry::{trace::TracerProvider as _, Array, KeyValue, Value};
use opentelemetry_sdk::trace::{
    in_memory_exporter::InMemorySpanExporter, SdkTracerProvider, SpanData,
};
use tracing_subscriber::layer::SubscriberExt as _;
use wr_engine::tracing::wruntime::tracing::span::AttributeValue;

struct CapturedTelemetry {
    exporter: InMemorySpanExporter,
    provider: SdkTracerProvider,
}

static CAPTURED_TELEMETRY: OnceLock<CapturedTelemetry> = OnceLock::new();

fn captured_telemetry() -> &'static CapturedTelemetry {
    CAPTURED_TELEMETRY.get_or_init(|| {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("wr-tests-tracing");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
        let dispatch = tracing::Dispatch::new(subscriber);
        tracing::dispatcher::set_global_default(dispatch)
            .expect("tracing_test installs the global tracing dispatcher once");
        CapturedTelemetry { exporter, provider }
    })
}

fn attribute<'a>(span: &'a SpanData, key: &str) -> Option<&'a Value> {
    key_value(&span.attributes, key)
}

fn key_value<'a>(attributes: &'a [KeyValue], key: &str) -> Option<&'a Value> {
    attributes
        .iter()
        .find(|attribute| attribute.key.as_str() == key)
        .map(|attribute| &attribute.value)
}

#[tokio::test]
async fn test_tracing_span_start_and_drop() {
    use wr_engine::tracing::wruntime::tracing::span::{Host, HostActiveSpan};

    let mut state = tracing_state();

    let span = Host::start(&mut state, "my-operation".into(), vec![])
        .await
        .expect("start");
    HostActiveSpan::drop(&mut state, span)
        .await
        .expect("drop span");
}

#[tokio::test]
async fn test_tracing_span_set_attributes_bulk() {
    use wr_engine::tracing::wruntime::tracing::span::{Host, HostActiveSpan};

    let telemetry = captured_telemetry();
    let mut state = tracing_state();

    let span = Host::start(&mut state, "bulk-late-attributes".into(), vec![])
        .await
        .expect("start");
    let rep = span.rep();
    HostActiveSpan::set_attributes(
        &mut state,
        wasmtime::component::Resource::new_borrow(rep),
        vec![
            ("text".into(), AttributeValue::Text("users".into())),
            ("boolean".into(), AttributeValue::Boolean(true)),
            ("signed".into(), AttributeValue::Signed(-42)),
            ("float".into(), AttributeValue::Float(1.5)),
            (
                "text-array".into(),
                AttributeValue::TextArray(vec!["a".into(), "b".into()]),
            ),
            (
                "boolean-array".into(),
                AttributeValue::BooleanArray(vec![true, false]),
            ),
            (
                "signed-array".into(),
                AttributeValue::SignedArray(vec![-1, 2]),
            ),
            (
                "float-array".into(),
                AttributeValue::FloatArray(vec![1.25, 2.5]),
            ),
        ],
    )
    .await
    .expect("set_attributes");
    HostActiveSpan::record_event(
        &mut state,
        wasmtime::component::Resource::new_borrow(rep),
        "bulk.event".into(),
        vec![
            ("event.text".into(), AttributeValue::Text("detail".into())),
            ("event.boolean".into(), AttributeValue::Boolean(false)),
            ("event.signed".into(), AttributeValue::Signed(7)),
            ("event.float".into(), AttributeValue::Float(2.5)),
            (
                "event.text-array".into(),
                AttributeValue::TextArray(vec!["x".into(), "y".into()]),
            ),
            (
                "event.boolean-array".into(),
                AttributeValue::BooleanArray(vec![false, true]),
            ),
            (
                "event.signed-array".into(),
                AttributeValue::SignedArray(vec![3, 4]),
            ),
            (
                "event.float-array".into(),
                AttributeValue::FloatArray(vec![3.5, 4.5]),
            ),
        ],
    )
    .await
    .expect("record_event");
    HostActiveSpan::set_attributes(
        &mut state,
        wasmtime::component::Resource::new_borrow(rep),
        vec![],
    )
    .await
    .expect("empty set_attributes");
    HostActiveSpan::set_attributes(
        &mut state,
        wasmtime::component::Resource::new_borrow(u32::MAX),
        vec![("ignored".into(), AttributeValue::Boolean(true))],
    )
    .await
    .expect("missing span is ignored");
    HostActiveSpan::drop(&mut state, span).await.expect("drop");
    drop(state);
    telemetry.provider.force_flush().expect("flush spans");

    let spans = telemetry
        .exporter
        .get_finished_spans()
        .expect("captured spans");
    let span = spans
        .iter()
        .find(|span| span.name == "bulk-late-attributes")
        .expect("completed bulk span");
    assert_eq!(
        attribute(span, "text"),
        Some(&Value::String("users".into()))
    );
    assert_eq!(attribute(span, "boolean"), Some(&Value::Bool(true)));
    assert_eq!(attribute(span, "signed"), Some(&Value::I64(-42)));
    assert_eq!(attribute(span, "float"), Some(&Value::F64(1.5)));
    assert_eq!(
        attribute(span, "text-array"),
        Some(&Value::Array(Array::String(vec!["a".into(), "b".into()])))
    );
    assert_eq!(
        attribute(span, "boolean-array"),
        Some(&Value::Array(Array::Bool(vec![true, false])))
    );
    assert_eq!(
        attribute(span, "signed-array"),
        Some(&Value::Array(Array::I64(vec![-1, 2])))
    );
    assert_eq!(
        attribute(span, "float-array"),
        Some(&Value::Array(Array::F64(vec![1.25, 2.5])))
    );

    let event = span
        .events
        .iter()
        .find(|event| event.name == "bulk.event")
        .expect("completed bulk event");
    assert_eq!(
        key_value(&event.attributes, "event.text"),
        Some(&Value::String("detail".into()))
    );
    assert_eq!(
        key_value(&event.attributes, "event.boolean"),
        Some(&Value::Bool(false))
    );
    assert_eq!(
        key_value(&event.attributes, "event.signed"),
        Some(&Value::I64(7))
    );
    assert_eq!(
        key_value(&event.attributes, "event.float"),
        Some(&Value::F64(2.5))
    );
    assert_eq!(
        key_value(&event.attributes, "event.text-array"),
        Some(&Value::Array(Array::String(vec!["x".into(), "y".into()])))
    );
    assert_eq!(
        key_value(&event.attributes, "event.boolean-array"),
        Some(&Value::Array(Array::Bool(vec![false, true])))
    );
    assert_eq!(
        key_value(&event.attributes, "event.signed-array"),
        Some(&Value::Array(Array::I64(vec![3, 4])))
    );
    assert_eq!(
        key_value(&event.attributes, "event.float-array"),
        Some(&Value::Array(Array::F64(vec![3.5, 4.5])))
    );
}

#[tokio::test]
async fn test_tracing_span_record_event() {
    use wr_engine::tracing::wruntime::tracing::span::{Host, HostActiveSpan};

    let mut state = tracing_state();

    let span = Host::start(&mut state, "op".into(), vec![])
        .await
        .expect("start");
    let rep = span.rep();
    HostActiveSpan::record_event(
        &mut state,
        wasmtime::component::Resource::new_borrow(rep),
        "cache.miss".into(),
        vec![("key".into(), AttributeValue::Text("user:42".into()))],
    )
    .await
    .expect("record_event");
    HostActiveSpan::drop(&mut state, span).await.expect("drop");
}

#[tokio::test]
async fn test_tracing_span_set_error() {
    use wr_engine::tracing::wruntime::tracing::span::{Host, HostActiveSpan};

    let mut state = tracing_state();

    let span = Host::start(&mut state, "op".into(), vec![])
        .await
        .expect("start");
    let rep = span.rep();
    HostActiveSpan::set_error(
        &mut state,
        wasmtime::component::Resource::new_borrow(rep),
        "connection refused".into(),
    )
    .await
    .expect("set_error");
    HostActiveSpan::drop(&mut state, span).await.expect("drop");
}
