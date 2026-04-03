#[allow(dead_code, unused_imports)]
mod helpers;
use helpers::*;

use wr_engine::state::ModuleState;

#[test]
fn test_tracing_span_start_and_drop() {
    use wr_engine::tracing::wruntime::tracing::span::{Host, HostActiveSpan};

    let mut state = ModuleState::new(
        "test".into(),
        "test-ns".into(),
        "http://127.0.0.1:9001".parse().unwrap(),
        http_client(),
        Default::default(),
    )
    .expect("ModuleState");

    let span = Host::start(&mut state, "my-operation".into(), vec![]);
    HostActiveSpan::drop(&mut state, span).expect("drop span");
}

#[test]
fn test_tracing_span_set_attribute() {
    use wr_engine::tracing::wruntime::tracing::span::{Host, HostActiveSpan};

    let mut state = ModuleState::new(
        "test".into(),
        "test-ns".into(),
        "http://127.0.0.1:9001".parse().unwrap(),
        http_client(),
        Default::default(),
    )
    .expect("ModuleState");

    let span = Host::start(&mut state, "op".into(), vec![]);
    let rep = span.rep();
    HostActiveSpan::set_attribute(
        &mut state,
        wasmtime::component::Resource::new_borrow(rep),
        "db.table".into(),
        "users".into(),
    );
    HostActiveSpan::drop(&mut state, span).expect("drop");
}

#[test]
fn test_tracing_span_record_event() {
    use wr_engine::tracing::wruntime::tracing::span::{Host, HostActiveSpan};

    let mut state = ModuleState::new(
        "test".into(),
        "test-ns".into(),
        "http://127.0.0.1:9001".parse().unwrap(),
        http_client(),
        Default::default(),
    )
    .expect("ModuleState");

    let span = Host::start(&mut state, "op".into(), vec![]);
    let rep = span.rep();
    HostActiveSpan::record_event(
        &mut state,
        wasmtime::component::Resource::new_borrow(rep),
        "cache.miss".into(),
        vec![("key".into(), "user:42".into())],
    );
    HostActiveSpan::drop(&mut state, span).expect("drop");
}

#[test]
fn test_tracing_span_set_error() {
    use wr_engine::tracing::wruntime::tracing::span::{Host, HostActiveSpan};

    let mut state = ModuleState::new(
        "test".into(),
        "test-ns".into(),
        "http://127.0.0.1:9001".parse().unwrap(),
        http_client(),
        Default::default(),
    )
    .expect("ModuleState");

    let span = Host::start(&mut state, "op".into(), vec![]);
    let rep = span.rep();
    HostActiveSpan::set_error(
        &mut state,
        wasmtime::component::Resource::new_borrow(rep),
        "connection refused".into(),
    );
    HostActiveSpan::drop(&mut state, span).expect("drop");
}
