mod helpers;

use anyhow::Result;
use prost::Message;
use wr_engine::config::ResourceLimits;

use helpers::{
    proto,
    wasm::{tracing_state, tracing_state_with_limits, GuestHarness, TestGuest},
};

#[tokio::test]
async fn wasm_tracing_start_span() -> Result<()> {
    let Some(harness) = GuestHarness::load(TestGuest::Tracing).await? else {
        return Ok(());
    };
    let state = tracing_state();

    let req = proto::StartSpanRequest {
        name: "test-span".into(),
        attrs: [("key".into(), "value".into())].into(),
    };
    let resp = harness.dispatch(state, "/StartSpan", req).await?;
    assert_eq!(resp.status(), 200);

    let body = proto::StartSpanResponse::decode(resp.into_body())?;
    assert!(body.ok);
    Ok(())
}

#[tokio::test]
async fn wasm_tracing_span_attributes() -> Result<()> {
    let Some(harness) = GuestHarness::load(TestGuest::Tracing).await? else {
        return Ok(());
    };
    let state = tracing_state();

    let req = proto::SpanAttributesRequest {
        span_name: "attr-span".into(),
        attrs: [("a".into(), "1".into()), ("b".into(), "2".into())].into(),
    };
    let resp = harness.dispatch(state, "/SpanAttributes", req).await?;
    assert_eq!(resp.status(), 200);

    let body = proto::SpanAttributesResponse::decode(resp.into_body())?;
    assert!(body.ok);
    Ok(())
}

#[tokio::test]
async fn wasm_tracing_span_event() -> Result<()> {
    let Some(harness) = GuestHarness::load(TestGuest::Tracing).await? else {
        return Ok(());
    };
    let state = tracing_state();

    let req = proto::SpanEventRequest {
        span_name: "event-span".into(),
        event_name: "my-event".into(),
        event_attrs: [("detail".into(), "test".into())].into(),
    };
    let resp = harness.dispatch(state, "/SpanEvent", req).await?;
    assert_eq!(resp.status(), 200);

    let body = proto::SpanEventResponse::decode(resp.into_body())?;
    assert!(body.ok);
    Ok(())
}

#[tokio::test]
async fn wasm_tracing_span_error() -> Result<()> {
    let Some(harness) = GuestHarness::load(TestGuest::Tracing).await? else {
        return Ok(());
    };
    let state = tracing_state();

    let req = proto::SpanErrorRequest {
        span_name: "error-span".into(),
        message: "something went wrong".into(),
    };
    let resp = harness.dispatch(state, "/SpanError", req).await?;
    assert_eq!(resp.status(), 200);

    let body = proto::SpanErrorResponse::decode(resp.into_body())?;
    assert!(body.ok);
    Ok(())
}

#[tokio::test]
async fn wasm_tracing_nested_spans() -> Result<()> {
    let Some(harness) = GuestHarness::load(TestGuest::Tracing).await? else {
        return Ok(());
    };
    let state = tracing_state();

    let req = proto::NestedSpansRequest {
        outer_name: "outer".into(),
        inner_name: "inner".into(),
    };
    let resp = harness.dispatch(state, "/NestedSpans", req).await?;
    assert_eq!(resp.status(), 200);

    let body = proto::NestedSpansResponse::decode(resp.into_body())?;
    assert!(body.ok);
    Ok(())
}

#[tokio::test]
async fn wasm_tracing_span_cap() -> Result<()> {
    let Some(harness) = GuestHarness::load(TestGuest::Tracing).await? else {
        return Ok(());
    };
    let limits = ResourceLimits {
        max_spans: 2,
        ..Default::default()
    };

    // Exactly at cap succeeds.
    let state = tracing_state_with_limits(limits.clone());
    let req = proto::AllocSpansRequest {
        initial: 2,
        drop_count: 0,
        additional: 0,
    };
    let resp = harness.dispatch(state, "/AllocSpans", req).await?;
    assert_eq!(resp.status(), 200);
    let body = proto::AllocSpansResponse::decode(resp.into_body())?;
    assert_eq!(body.held, 2);

    // Cap + 1 traps the store — dispatch returns Err.
    let state = tracing_state_with_limits(limits.clone());
    let req = proto::AllocSpansRequest {
        initial: 3,
        drop_count: 0,
        additional: 0,
    };
    let result = harness.dispatch(state, "/AllocSpans", req).await;
    assert!(result.is_err(), "expected trap when exceeding span cap");

    // Engine survives the trap — a fresh request for the same module still works.
    let state = tracing_state_with_limits(limits.clone());
    let req = proto::AllocSpansRequest {
        initial: 1,
        drop_count: 0,
        additional: 0,
    };
    let resp = harness.dispatch(state, "/AllocSpans", req).await?;
    assert_eq!(resp.status(), 200);

    // Dropping a span frees a live slot so a later `start` succeeds again.
    let state = tracing_state_with_limits(limits);
    let req = proto::AllocSpansRequest {
        initial: 2,
        drop_count: 1,
        additional: 1,
    };
    let resp = harness.dispatch(state, "/AllocSpans", req).await?;
    assert_eq!(resp.status(), 200);
    let body = proto::AllocSpansResponse::decode(resp.into_body())?;
    assert_eq!(body.held, 2);

    Ok(())
}
