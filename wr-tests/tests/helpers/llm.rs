use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use http_body_util::Full;

use wr_engine::llm::LlmRuntime;

use super::db::{ModuleServices, ModuleState};
use super::proxy::http_pool;

#[derive(Clone)]
pub enum MockLlmMode {
    /// Return a simple text completion.
    Text {
        text: String,
        input_tokens: u32,
        output_tokens: u32,
    },
    /// Return a tool_use response.
    ToolUse {
        tool_id: String,
        tool_name: String,
        tool_input: String,
    },
    /// Return an HTTP error status.
    Error { status: u16, body: String },
    /// Return a streaming SSE response with the given text chunks.
    Stream { chunks: Vec<String> },
    /// Return a streaming SSE response that emits partial text then a stream-level `error` event.
    StreamError,
}

/// Spawn a mock Claude API HTTP server that returns canned responses.
/// Returns the base URL (e.g. "http://127.0.0.1:PORT") and a shutdown handle.
pub async fn spawn_mock_llm_server(
    mode: MockLlmMode,
) -> Result<(String, tokio::sync::oneshot::Sender<()>)> {
    use hyper::service::service_fn;
    use hyper_util::rt::TokioIo;
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        loop {
            let mode = mode.clone();
            tokio::select! {
                accepted = listener.accept() => {
                    let (stream, _) = match accepted {
                        Ok(v) => v,
                        Err(_) => continue,
                    };
                    let io = TokioIo::new(stream);
                    let mode = mode.clone();
                    tokio::spawn(async move {
                        let _ = hyper::server::conn::http1::Builder::new()
                            .serve_connection(
                                io,
                                service_fn(move |req| {
                                    let mode = mode.clone();
                                    async move {
                                        handle_mock_llm_request(req, mode).await
                                    }
                                }),
                            )
                            .await;
                    });
                }
                _ = &mut shutdown_rx => break,
            }
        }
    });

    Ok((format!("http://127.0.0.1:{}", addr.port()), shutdown_tx))
}

async fn handle_mock_llm_request(
    _req: hyper::Request<hyper::body::Incoming>,
    mode: MockLlmMode,
) -> Result<hyper::Response<http_body_util::Full<Bytes>>, std::convert::Infallible> {
    match mode {
        MockLlmMode::Text {
            text,
            input_tokens,
            output_tokens,
        } => {
            let body = serde_json::json!({
                "id": "msg_mock_001",
                "type": "message",
                "role": "assistant",
                "content": [{"type": "text", "text": text}],
                "model": "claude-sonnet-4-6",
                "stop_reason": "end_turn",
                "usage": {
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens
                }
            });
            Ok(hyper::Response::builder()
                .status(200)
                .header("content-type", "application/json")
                .body(Full::new(Bytes::from(serde_json::to_vec(&body).unwrap())))
                .unwrap())
        }
        MockLlmMode::ToolUse {
            tool_id,
            tool_name,
            tool_input,
        } => {
            let input_value: serde_json::Value =
                serde_json::from_str(&tool_input).unwrap_or(serde_json::json!({}));
            let body = serde_json::json!({
                "id": "msg_mock_002",
                "type": "message",
                "role": "assistant",
                "content": [{
                    "type": "tool_use",
                    "id": tool_id,
                    "name": tool_name,
                    "input": input_value
                }],
                "model": "claude-sonnet-4-6",
                "stop_reason": "tool_use",
                "usage": {"input_tokens": 50, "output_tokens": 30}
            });
            Ok(hyper::Response::builder()
                .status(200)
                .header("content-type", "application/json")
                .body(Full::new(Bytes::from(serde_json::to_vec(&body).unwrap())))
                .unwrap())
        }
        MockLlmMode::Error { status, body } => Ok(hyper::Response::builder()
            .status(status)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(body)))
            .unwrap()),
        MockLlmMode::Stream { chunks } => {
            let mut sse = String::new();
            // message_start — CRLF line endings (exercises CRLF normalization). Carries input_tokens.
            sse.push_str("event: message_start\r\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_mock_003\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"claude-sonnet-4-6\",\"stop_reason\":null,\"usage\":{\"input_tokens\":25,\"output_tokens\":0}}}\r\n\r\n");
            // content_block_start
            sse.push_str("event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n");
            // ping — CRLF, must be skipped (no guest event)
            sse.push_str("event: ping\r\ndata: {\"type\":\"ping\"}\r\n\r\n");
            // content_block_delta per chunk, JSON split across two data: lines (multiline accumulation)
            for chunk in &chunks {
                let escaped = chunk.replace('\\', "\\\\").replace('"', "\\\"");
                sse.push_str(&format!(
                    "event: content_block_delta\ndata: {{\"type\":\"content_block_delta\",\"index\":0,\ndata: \"delta\":{{\"type\":\"text_delta\",\"text\":\"{escaped}\"}}}}\n\n"
                ));
            }
            // content_block_stop
            sse.push_str("event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n");
            // message_delta with stop_reason + cumulative output_tokens
            let output_tokens = chunks.iter().map(|c| c.len() as u32).sum::<u32>();
            sse.push_str(&format!(
                "event: message_delta\ndata: {{\"type\":\"message_delta\",\"delta\":{{\"stop_reason\":\"end_turn\"}},\"usage\":{{\"output_tokens\":{output_tokens}}}}}\n\n"
            ));
            // message_stop
            sse.push_str("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n");

            Ok(hyper::Response::builder()
                .status(200)
                .header("content-type", "text/event-stream")
                .body(Full::new(Bytes::from(sse)))
                .unwrap())
        }
        MockLlmMode::StreamError => {
            let mut sse = String::new();
            sse.push_str("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_mock_004\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"claude-sonnet-4-6\",\"stop_reason\":null,\"usage\":{\"input_tokens\":10,\"output_tokens\":0}}}\n\n");
            sse.push_str("event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n");
            sse.push_str("event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"partial\"}}\n\n");
            sse.push_str("event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"server overloaded\"}}\n\n");

            Ok(hyper::Response::builder()
                .status(200)
                .header("content-type", "text/event-stream")
                .body(Full::new(Bytes::from(sse)))
                .unwrap())
        }
    }
}

/// Build an `LlmRuntime` pointing at the given mock base URL.
pub fn mock_llm_runtime(base_url: &str) -> Arc<LlmRuntime> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    use wr_engine::config::{LlmConfig, LlmProvider};
    // Set a temp env var for the API key
    std::env::set_var("WRT_TEST_LLM_KEY", "mock-key");
    let config = LlmConfig {
        provider: LlmProvider::Anthropic,
        api_key_env: "WRT_TEST_LLM_KEY".into(),
        base_url: base_url.into(),
        max_tokens_limit: 8192,
    };
    Arc::new(LlmRuntime::new(&config).expect("LlmRuntime"))
}

/// Build a `ModuleState` with an LLM runtime for WASM guest tests.
pub fn llm_state(llm: Arc<LlmRuntime>) -> ModuleState {
    ModuleState::new(
        "llm-test".into(),
        "test-ns".into(),
        "http://127.0.0.1:9001".parse().unwrap(),
        http_pool(),
        ModuleServices {
            llm: Some(llm),
            ..Default::default()
        },
    )
    .expect("ModuleState")
}

pub fn llm_state_with_limits(
    llm: Arc<LlmRuntime>,
    limits: wr_engine::config::ResourceLimits,
) -> ModuleState {
    ModuleState::new(
        "llm-test".into(),
        "test-ns".into(),
        "http://127.0.0.1:9001".parse().unwrap(),
        http_pool(),
        ModuleServices {
            llm: Some(llm),
            limits,
            ..Default::default()
        },
    )
    .expect("ModuleState")
}
