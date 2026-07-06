mod helpers;

use anyhow::Result;
use prost::Message;
use wr_engine::config::ResourceLimits;

use helpers::{
    llm::{llm_state, llm_state_with_limits, mock_llm_runtime, spawn_mock_llm_server, MockLlmMode},
    proto,
    wasm::{GuestHarness, TestGuest},
};

#[tokio::test]
async fn wasm_llm_complete() -> Result<()> {
    let Some(harness) = GuestHarness::load(TestGuest::Llm).await? else {
        return Ok(());
    };
    let (base_url, _shutdown) = spawn_mock_llm_server(MockLlmMode::Text {
        text: "Hello from mock Claude!".into(),
        input_tokens: 10,
        output_tokens: 7,
    })
    .await?;
    let llm = mock_llm_runtime(&base_url);
    let state = llm_state(llm);

    let req = proto::CompleteRequest {
        model: "claude-sonnet-4-6".into(),
        system: "You are a test assistant.".into(),
        user_message: "Say hello".into(),
        max_tokens: 100,
    };
    let resp = harness.dispatch(state, "/Complete", req).await?;
    assert_eq!(resp.status(), 200);

    let body = proto::CompleteResponse::decode(resp.into_body())?;
    assert_eq!(body.text, "Hello from mock Claude!");
    assert_eq!(body.stop_reason, "end_turn");
    assert_eq!(body.input_tokens, 10);
    assert_eq!(body.output_tokens, 7);
    Ok(())
}

#[tokio::test]
async fn wasm_llm_complete_text() -> Result<()> {
    let Some(harness) = GuestHarness::load(TestGuest::Llm).await? else {
        return Ok(());
    };
    let (base_url, _shutdown) = spawn_mock_llm_server(MockLlmMode::Text {
        text: "Short answer".into(),
        input_tokens: 5,
        output_tokens: 2,
    })
    .await?;
    let llm = mock_llm_runtime(&base_url);
    let state = llm_state(llm);

    let req = proto::CompleteTextRequest {
        user_message: "Give me a short answer".into(),
    };
    let resp = harness.dispatch(state, "/CompleteText", req).await?;
    assert_eq!(resp.status(), 200);

    let body = proto::CompleteTextResponse::decode(resp.into_body())?;
    assert_eq!(body.text, "Short answer");
    Ok(())
}

#[tokio::test]
async fn wasm_llm_tool_use() -> Result<()> {
    let Some(harness) = GuestHarness::load(TestGuest::Llm).await? else {
        return Ok(());
    };
    let (base_url, _shutdown) = spawn_mock_llm_server(MockLlmMode::ToolUse {
        tool_id: "toolu_mock_001".into(),
        tool_name: "get_weather".into(),
        tool_input: r#"{"location":"San Francisco"}"#.into(),
    })
    .await?;
    let llm = mock_llm_runtime(&base_url);
    let state = llm_state(llm);

    let req = proto::ToolUseRequest {
        user_message: "What's the weather in San Francisco?".into(),
        tool_name: "get_weather".into(),
        tool_description: "Get current weather for a location".into(),
        tool_schema: r#"{"type":"object","properties":{"location":{"type":"string"}},"required":["location"]}"#.into(),
    };
    let resp = harness.dispatch(state, "/ToolUse", req).await?;
    assert_eq!(resp.status(), 200);

    let body = proto::ToolUseResponse::decode(resp.into_body())?;
    assert_eq!(body.tool_name, "get_weather");
    assert_eq!(body.tool_id, "toolu_mock_001");
    assert!(body.tool_input.contains("San Francisco"));
    assert_eq!(body.stop_reason, "tool_use");
    Ok(())
}

#[tokio::test]
async fn wasm_llm_error() -> Result<()> {
    let Some(harness) = GuestHarness::load(TestGuest::Llm).await? else {
        return Ok(());
    };
    let (base_url, _shutdown) = spawn_mock_llm_server(MockLlmMode::Error {
        status: 401,
        body: r#"{"error":{"type":"authentication_error","message":"invalid api key"}}"#.into(),
    })
    .await?;
    let llm = mock_llm_runtime(&base_url);
    let state = llm_state(llm);

    let req = proto::LlmErrorRequest {
        user_message: "This should fail".into(),
    };
    let resp = harness.dispatch(state, "/Error", req).await?;
    assert_eq!(resp.status(), 200);

    let body = proto::LlmErrorResponse::decode(resp.into_body())?;
    assert_eq!(body.error_kind, "auth");
    assert!(!body.error_message.is_empty());
    Ok(())
}

#[tokio::test]
async fn wasm_llm_stream() -> Result<()> {
    let Some(harness) = GuestHarness::load(TestGuest::Llm).await? else {
        return Ok(());
    };
    let (base_url, _shutdown) = spawn_mock_llm_server(MockLlmMode::Stream {
        chunks: vec!["Hello".into(), " from".into(), " streaming!".into()],
    })
    .await?;
    let llm = mock_llm_runtime(&base_url);
    let state = llm_state(llm);

    let req = proto::StreamRequest {
        user_message: "Stream me a response".into(),
        with_tools: false,
    };
    let resp = harness.dispatch(state, "/Stream", req).await?;
    assert_eq!(resp.status(), 200);

    let body = proto::StreamResponse::decode(resp.into_body())?;
    assert_eq!(body.text, "Hello from streaming!");
    assert_eq!(body.chunk_count, 3);
    assert_eq!(
        body.events,
        vec!["text-delta", "text-delta", "text-delta", "usage", "stop"]
    );
    assert_eq!(body.input_tokens, 25);
    assert_eq!(body.output_tokens, 21); // "Hello"(5) + " from"(5) + " streaming!"(11)
    assert_eq!(body.stop_reason, "end_turn");
    assert!(body.usage_mid_none, "usage() must be None mid-stream");
    assert!(body.usage_present_after, "usage() must be Some after drain");
    assert!(body.error_kind.is_empty());
    Ok(())
}

#[tokio::test]
async fn wasm_llm_stream_error() -> Result<()> {
    let Some(harness) = GuestHarness::load(TestGuest::Llm).await? else {
        return Ok(());
    };
    let (base_url, _shutdown) = spawn_mock_llm_server(MockLlmMode::StreamError).await?;
    let llm = mock_llm_runtime(&base_url);
    let state = llm_state(llm);

    let req = proto::StreamRequest {
        user_message: "Stream me a response".into(),
        with_tools: false,
    };
    let resp = harness.dispatch(state, "/Stream", req).await?;
    assert_eq!(resp.status(), 200);

    let body = proto::StreamResponse::decode(resp.into_body())?;
    // Partial text arrives before the error frame.
    assert_eq!(body.text, "partial");
    assert_eq!(body.chunk_count, 1);
    // The stream-level error surfaces as an llm-error, not a silent truncation.
    assert_eq!(body.error_kind, "api");
    assert!(body.error_message.contains("overloaded"));
    Ok(())
}

#[tokio::test]
async fn wasm_llm_stream_tool_use_rejected() -> Result<()> {
    let Some(harness) = GuestHarness::load(TestGuest::Llm).await? else {
        return Ok(());
    };
    // Mock is spawned but never hit — the request is pre-rejected before any upstream call.
    let (base_url, _shutdown) = spawn_mock_llm_server(MockLlmMode::Stream {
        chunks: vec!["unused".into()],
    })
    .await?;
    let llm = mock_llm_runtime(&base_url);
    let state = llm_state(llm);

    let req = proto::StreamRequest {
        user_message: "Use a tool while streaming".into(),
        with_tools: true,
    };
    let resp = harness.dispatch(state, "/Stream", req).await?;
    assert_eq!(resp.status(), 200);

    let body = proto::StreamResponse::decode(resp.into_body())?;
    assert_eq!(body.error_kind, "invalid-request");
    assert!(body.error_message.contains("streaming"));
    // No stream was produced.
    assert_eq!(body.chunk_count, 0);
    assert!(body.text.is_empty());
    Ok(())
}

#[tokio::test]
async fn wasm_llm_stream_cap() -> Result<()> {
    let Some(harness) = GuestHarness::load(TestGuest::Llm).await? else {
        return Ok(());
    };
    let (base_url, _shutdown) = spawn_mock_llm_server(MockLlmMode::Stream {
        chunks: vec!["hi".into()],
    })
    .await?;
    let limits = ResourceLimits {
        max_llm_streams: 2,
        ..Default::default()
    };

    // Cap + 1 rejected via `LlmError::Api` — the 3rd `stream()` is rejected by
    // `try_track` before any upstream request, so the mock is only hit for the
    // successful opens.
    let llm = mock_llm_runtime(&base_url);
    let state = llm_state_with_limits(llm, limits.clone());
    let req = proto::AllocStreamsRequest {
        initial: 3,
        drop_count: 0,
        additional: 0,
    };
    let resp = harness.dispatch(state, "/AllocStreams", req).await?;
    assert_eq!(resp.status(), 200);
    let body = proto::AllocStreamsResponse::decode(resp.into_body())?;
    assert_eq!(body.held, 2);
    assert!(body.hit_cap);
    assert_eq!(body.error_kind, "api");

    // Dropping ALL held streams frees the count so a full re-allocation to cap
    // succeeds.
    let llm = mock_llm_runtime(&base_url);
    let state = llm_state_with_limits(llm, limits);
    let req = proto::AllocStreamsRequest {
        initial: 2,
        drop_count: 2,
        additional: 2,
    };
    let resp = harness.dispatch(state, "/AllocStreams", req).await?;
    assert_eq!(resp.status(), 200);
    let body = proto::AllocStreamsResponse::decode(resp.into_body())?;
    assert_eq!(body.held, 2);
    assert!(!body.hit_cap);

    Ok(())
}
