mod helpers;
use helpers::wasm::tracing_state;
use wr_engine::tracing::wruntime::tracing::span::AttributeValue;

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
async fn test_tracing_span_set_attribute() {
    use wr_engine::tracing::wruntime::tracing::span::{Host, HostActiveSpan};

    let mut state = tracing_state();

    let span = Host::start(&mut state, "op".into(), vec![])
        .await
        .expect("start");
    let rep = span.rep();
    HostActiveSpan::set_attribute(
        &mut state,
        wasmtime::component::Resource::new_borrow(rep),
        "db.table".into(),
        AttributeValue::Text("users".into()),
    )
    .await
    .expect("set_attribute");
    HostActiveSpan::drop(&mut state, span).await.expect("drop");
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
