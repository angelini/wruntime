#[allow(dead_code, unused_imports)]
mod helpers;
use helpers::*;

#[tokio::test]
async fn test_tracing_span_start_and_drop() {
    use wr_engine::tracing::wruntime::tracing::span::{Host, HostActiveSpan};

    let mut state = tracing_state();

    let span = Host::start(&mut state, "my-operation".into(), vec![]).await;
    HostActiveSpan::drop(&mut state, span)
        .await
        .expect("drop span");
}

#[tokio::test]
async fn test_tracing_span_set_attribute() {
    use wr_engine::tracing::wruntime::tracing::span::{Host, HostActiveSpan};

    let mut state = tracing_state();

    let span = Host::start(&mut state, "op".into(), vec![]).await;
    let rep = span.rep();
    HostActiveSpan::set_attribute(
        &mut state,
        wasmtime::component::Resource::new_borrow(rep),
        "db.table".into(),
        "users".into(),
    )
    .await;
    HostActiveSpan::drop(&mut state, span).await.expect("drop");
}

#[tokio::test]
async fn test_tracing_span_record_event() {
    use wr_engine::tracing::wruntime::tracing::span::{Host, HostActiveSpan};

    let mut state = tracing_state();

    let span = Host::start(&mut state, "op".into(), vec![]).await;
    let rep = span.rep();
    HostActiveSpan::record_event(
        &mut state,
        wasmtime::component::Resource::new_borrow(rep),
        "cache.miss".into(),
        vec![("key".into(), "user:42".into())],
    )
    .await;
    HostActiveSpan::drop(&mut state, span).await.expect("drop");
}

#[tokio::test]
async fn test_tracing_span_set_error() {
    use wr_engine::tracing::wruntime::tracing::span::{Host, HostActiveSpan};

    let mut state = tracing_state();

    let span = Host::start(&mut state, "op".into(), vec![]).await;
    let rep = span.rep();
    HostActiveSpan::set_error(
        &mut state,
        wasmtime::component::Resource::new_borrow(rep),
        "connection refused".into(),
    )
    .await;
    HostActiveSpan::drop(&mut state, span).await.expect("drop");
}
