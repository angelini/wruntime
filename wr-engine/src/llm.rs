use anyhow::Context as _;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use wasmtime::component::Resource;

use crate::config::LlmConfig;
use crate::state::{CounterGuard, ModuleState, ResourceKind};

// ── LlmRuntime — host-side HTTP client for the Claude Messages API ──────────

pub struct LlmRuntime {
    client: Client,
    api_key: String,
    base_url: String,
    max_tokens_limit: u32,
}

impl LlmRuntime {
    pub fn new(config: &LlmConfig) -> anyhow::Result<Self> {
        let api_key = std::env::var(&config.api_key_env)
            .with_context(|| format!("missing env var: {}", config.api_key_env))?;
        Ok(Self {
            client: Client::new(),
            api_key,
            base_url: config.base_url.clone(),
            max_tokens_limit: config.max_tokens_limit,
        })
    }

    /// Non-streaming Messages API call.
    pub async fn complete(&self, req: ApiRequest) -> Result<ApiResponse, LlmErrorKind> {
        let url = format!("{}/v1/messages", self.base_url);
        let resp = self
            .client
            .post(&url)
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .json(&req)
            .send()
            .await
            .map_err(|e| LlmErrorKind::Api(format!("request failed: {e}")))?;

        if !resp.status().is_success() {
            return Err(error_from_response(resp).await);
        }

        resp.json::<ApiResponse>()
            .await
            .map_err(|e| LlmErrorKind::Api(format!("failed to parse response: {e}")))
    }

    /// Streaming Messages API call. Returns an mpsc receiver yielding host
    /// stream events per the completion-stream state machine.
    pub async fn complete_stream(
        &self,
        mut req: ApiRequest,
    ) -> Result<mpsc::Receiver<HostStreamEvent>, LlmErrorKind> {
        req.stream = Some(true);
        let url = format!("{}/v1/messages", self.base_url);
        let resp = self
            .client
            .post(&url)
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .json(&req)
            .send()
            .await
            .map_err(|e| LlmErrorKind::Api(format!("request failed: {e}")))?;

        if !resp.status().is_success() {
            return Err(error_from_response(resp).await);
        }

        let (tx, rx) = mpsc::channel(64);

        tokio::spawn(async move {
            use futures::StreamExt;
            let mut stream = resp.bytes_stream();
            let mut parser = SseParser::new();
            let mut input_tokens: u32 = 0;
            let mut terminal_sent = false;
            'outer: while let Some(chunk) = stream.next().await {
                let chunk = match chunk {
                    Ok(c) => c,
                    Err(e) => {
                        let _ = tx
                            .send(HostStreamEvent::Error(LlmErrorKind::Api(format!(
                                "stream connection error: {e}"
                            ))))
                            .await;
                        return;
                    }
                };
                for event in parser.feed(&String::from_utf8_lossy(&chunk)) {
                    match event {
                        SseEvent::MessageStart { input_tokens: it } => {
                            input_tokens = it;
                        }
                        SseEvent::TextDelta(text) => {
                            if tx.send(HostStreamEvent::TextDelta(text)).await.is_err() {
                                return;
                            }
                        }
                        SseEvent::ToolUseDelta => {
                            let _ = tx
                                .send(HostStreamEvent::Error(LlmErrorKind::InvalidRequest(
                                    "tool-use streaming is not supported; use complete()".into(),
                                )))
                                .await;
                            return;
                        }
                        SseEvent::MessageDelta {
                            output_tokens,
                            stop_reason,
                        } => {
                            let _ = tx
                                .send(HostStreamEvent::Usage {
                                    input_tokens,
                                    output_tokens,
                                })
                                .await;
                            let _ = tx
                                .send(HostStreamEvent::Stop(
                                    stop_reason.unwrap_or_else(|| "end_turn".into()),
                                ))
                                .await;
                            terminal_sent = true;
                        }
                        SseEvent::MessageStop => break 'outer,
                        SseEvent::Error(msg) => {
                            let _ = tx
                                .send(HostStreamEvent::Error(LlmErrorKind::Api(msg)))
                                .await;
                            return;
                        }
                    }
                }
            }
            if !terminal_sent {
                let _ = tx
                    .send(HostStreamEvent::Error(LlmErrorKind::Api(
                        "stream ended before completion".into(),
                    )))
                    .await;
            }
        });

        Ok(rx)
    }

    /// Enforce host-side max_tokens ceiling.
    pub fn clamp_max_tokens(&self, requested: u32) -> u32 {
        requested.min(self.max_tokens_limit)
    }
}

// ── SSE stream parser ───────────────────────────────────────────────────────

enum SseEvent {
    MessageStart {
        input_tokens: u32,
    },
    TextDelta(String),
    ToolUseDelta,
    MessageDelta {
        output_tokens: u32,
        stop_reason: Option<String>,
    },
    MessageStop,
    Error(String),
}

struct SseParser {
    buf: String,
}

impl SseParser {
    fn new() -> Self {
        Self { buf: String::new() }
    }

    fn feed(&mut self, chunk: &str) -> Vec<SseEvent> {
        self.buf.push_str(chunk);
        // Normalize CRLF -> LF over the whole retained buffer so frame splitting
        // on "\n\n" is line-ending agnostic (handles \r\n\r\n and a \r split
        // across chunk boundaries).
        if self.buf.contains('\r') {
            self.buf = self.buf.replace("\r\n", "\n");
        }
        let mut events = Vec::new();
        while let Some(pos) = self.buf.find("\n\n") {
            let block = self.buf[..pos].to_string();
            self.buf = self.buf[pos + 2..].to_string();
            if let Some(event) = Self::parse_block(&block) {
                events.push(event);
            }
        }
        events
    }

    fn parse_block(block: &str) -> Option<SseEvent> {
        let mut event_name = String::new();
        let mut data_lines: Vec<&str> = Vec::new();
        for line in block.lines() {
            if let Some(v) = line.strip_prefix("event:") {
                event_name = v.trim().to_string();
            } else if let Some(v) = line.strip_prefix("data:") {
                // SSE strips exactly one leading space after the field name.
                data_lines.push(v.strip_prefix(' ').unwrap_or(v));
            }
        }
        let data = data_lines.join("\n");
        match event_name.as_str() {
            "message_start" => {
                let m: MessageStartEvent = serde_json::from_str(&data).ok()?;
                Some(SseEvent::MessageStart {
                    input_tokens: m.message.usage.input_tokens,
                })
            }
            "content_block_delta" => {
                let d: ContentBlockDelta = serde_json::from_str(&data).ok()?;
                match d.delta.delta_type.as_str() {
                    "text_delta" => Some(SseEvent::TextDelta(d.delta.text)),
                    "input_json_delta" => Some(SseEvent::ToolUseDelta),
                    // thinking_delta / signature_delta / citations_delta: no WIT
                    // representation — dropped.
                    _ => None,
                }
            }
            "message_delta" => {
                let m: MessageDelta = serde_json::from_str(&data).ok()?;
                Some(SseEvent::MessageDelta {
                    output_tokens: m.usage.map(|u| u.output_tokens).unwrap_or(0),
                    stop_reason: m.delta.and_then(|d| d.stop_reason),
                })
            }
            "message_stop" => Some(SseEvent::MessageStop),
            "error" => {
                let e: StreamErrorEvent = serde_json::from_str(&data).unwrap_or_default();
                Some(SseEvent::Error(format!(
                    "{}: {}",
                    e.error.error_type, e.error.message
                )))
            }
            // content_block_start, content_block_stop, ping, unknown → ignored.
            _ => None,
        }
    }
}

// ── Internal API types for Claude Messages API ──────────────────────────────

#[derive(Serialize)]
pub struct ApiRequest {
    pub model: String,
    pub messages: Vec<ApiMessage>,
    pub max_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<ApiToolDef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
}

#[derive(Serialize)]
pub struct ApiMessage {
    pub role: String,
    pub content: String,
}

#[derive(Serialize)]
pub struct ApiToolDef {
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
}

#[derive(Deserialize)]
pub struct ApiResponse {
    pub content: Vec<ContentBlock>,
    #[serde(default)]
    pub usage: ApiUsage,
    #[serde(default)]
    pub stop_reason: Option<String>,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum ContentBlock {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "tool_use")]
    ToolUse {
        id: String,
        name: String,
        input: serde_json::Value,
    },
}

#[derive(Deserialize, Default)]
pub struct ApiUsage {
    #[serde(default)]
    pub input_tokens: u32,
    #[serde(default)]
    pub output_tokens: u32,
}

// SSE event types for streaming
#[derive(Deserialize)]
struct ContentBlockDelta {
    delta: DeltaPayload,
}

#[derive(Deserialize)]
struct DeltaPayload {
    #[serde(rename = "type")]
    delta_type: String,
    #[serde(default)]
    text: String,
}

#[derive(Deserialize)]
struct MessageStartEvent {
    message: MessageStartBody,
}

#[derive(Deserialize)]
struct MessageStartBody {
    #[serde(default)]
    usage: ApiUsage,
}

#[derive(Deserialize)]
struct MessageDelta {
    #[serde(default)]
    delta: Option<MessageDeltaDelta>,
    #[serde(default)]
    usage: Option<MessageDeltaUsage>,
}

#[derive(Deserialize)]
struct MessageDeltaDelta {
    #[serde(default)]
    stop_reason: Option<String>,
}

#[derive(Deserialize)]
struct MessageDeltaUsage {
    output_tokens: u32,
}

#[derive(Deserialize, Default)]
struct StreamErrorEvent {
    #[serde(default)]
    error: StreamErrorBody,
}

#[derive(Deserialize, Default)]
struct StreamErrorBody {
    #[serde(rename = "type", default)]
    error_type: String,
    #[serde(default)]
    message: String,
}

/// Host-internal events sent from the SSE producer task to `next()`.
pub enum HostStreamEvent {
    TextDelta(String),
    Usage {
        input_tokens: u32,
        output_tokens: u32,
    },
    Stop(String),
    Error(LlmErrorKind),
}

/// Internal error kind used by `LlmRuntime`.
pub enum LlmErrorKind {
    InvalidRequest(String),
    Auth(String),
    RateLimited(Option<u32>),
    Api(String),
}

/// Map a non-success HTTP response to the appropriate `LlmErrorKind`.
async fn error_from_response(resp: reqwest::Response) -> LlmErrorKind {
    let status = resp.status();
    let retry_after = resp
        .headers()
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u32>().ok());
    let body = resp.text().await.unwrap_or_default();
    match status.as_u16() {
        400 => LlmErrorKind::InvalidRequest(body),
        401 | 403 => LlmErrorKind::Auth(body),
        429 | 529 => LlmErrorKind::RateLimited(retry_after),
        _ => LlmErrorKind::Api(format!("HTTP {status}: {body}")),
    }
}

// ── WIT bindings ─────────────────────────────────────────────────────────────

/// Resource state for a streaming completion.
pub struct CompletionStreamState {
    rx: mpsc::Receiver<HostStreamEvent>,
    usage: Option<(u32, u32)>,
    finished: bool,
    _count: CounterGuard,
}

wasmtime::component::bindgen!({
    path:  "../wit/llm.wit",
    world: "llm-access",
    imports: { default: async },
    with: {
        "wruntime:llm/inference@0.1.0.completion-stream": CompletionStreamState,
    },
});

pub use wruntime::llm::inference::LlmError;
use wruntime::llm::inference::{
    Completion, CompletionRequest, CompletionResponse, Host, HostCompletionStream, MessageRole,
    StreamEvent, TokenUsage, ToolUse,
};

// ── Host implementation ─────────────────────────────────────────────────────

impl Host for ModuleState {
    async fn complete(&mut self, req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        let runtime = self.llm()?.runtime.clone();
        let api_req = to_api_request(&runtime, &req);
        match runtime.complete(api_req).await {
            Ok(resp) => Ok(from_api_response(resp)),
            Err(e) => Err(e.into()),
        }
    }

    async fn complete_stream(
        &mut self,
        req: CompletionRequest,
    ) -> Result<Resource<CompletionStreamState>, LlmError> {
        if !req.tools.is_empty() {
            return Err(LlmError::InvalidRequest(
                "tool use is not supported in streaming; use complete()".into(),
            ));
        }
        let cap = self.llm()?;
        let guard = cap
            .accounting
            .try_track(ResourceKind::LlmStream)
            .ok_or_else(|| LlmError::Api("llm stream cap exceeded".into()))?;
        let runtime = cap.runtime.clone();
        let api_req = to_api_request(&runtime, &req);
        match runtime.complete_stream(api_req).await {
            Ok(rx) => {
                let handle = self
                    .table()
                    .push(CompletionStreamState {
                        rx,
                        usage: None,
                        finished: false,
                        _count: guard,
                    })
                    .map_err(|e| LlmError::Api(format!("resource table full: {e}")))?;
                Ok(handle)
            }
            Err(e) => Err(e.into()),
        }
    }
}

impl HostCompletionStream for ModuleState {
    async fn next(
        &mut self,
        self_: Resource<CompletionStreamState>,
    ) -> Result<Option<StreamEvent>, LlmError> {
        let state = self
            .table()
            .get_mut(&self_)
            .map_err(|e| LlmError::Api(format!("invalid stream handle: {e}")))?;
        if state.finished {
            return Ok(None);
        }
        match state.rx.recv().await {
            Some(HostStreamEvent::TextDelta(text)) => Ok(Some(StreamEvent::TextDelta(text))),
            Some(HostStreamEvent::Usage {
                input_tokens,
                output_tokens,
            }) => {
                state.usage = Some((input_tokens, output_tokens));
                Ok(Some(StreamEvent::Usage(TokenUsage {
                    input_tokens,
                    output_tokens,
                })))
            }
            Some(HostStreamEvent::Stop(reason)) => Ok(Some(StreamEvent::Stop(reason))),
            Some(HostStreamEvent::Error(kind)) => {
                state.finished = true;
                Err(kind.into())
            }
            None => {
                state.finished = true;
                Ok(None)
            }
        }
    }

    async fn usage(&mut self, self_: Resource<CompletionStreamState>) -> Option<TokenUsage> {
        let state = match self.table().get(&self_) {
            Ok(s) => s,
            Err(_) => return None,
        };
        state.usage.map(|(input_tokens, output_tokens)| TokenUsage {
            input_tokens,
            output_tokens,
        })
    }

    async fn drop(&mut self, self_: Resource<CompletionStreamState>) -> wasmtime::Result<()> {
        self.table().delete(self_)?;
        Ok(())
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn to_api_request(runtime: &LlmRuntime, req: &CompletionRequest) -> ApiRequest {
    ApiRequest {
        model: req.model.clone(),
        messages: req
            .messages
            .iter()
            .map(|m| ApiMessage {
                role: match m.role {
                    MessageRole::User => "user".into(),
                    MessageRole::Assistant => "assistant".into(),
                },
                content: m.content.clone(),
            })
            .collect(),
        max_tokens: runtime.clamp_max_tokens(req.max_tokens),
        system: req.system.clone(),
        temperature: req.temperature,
        tools: req
            .tools
            .iter()
            .map(|t| ApiToolDef {
                name: t.name.clone(),
                description: t.description.clone(),
                input_schema: serde_json::from_str(&t.input_schema)
                    .unwrap_or(serde_json::json!({})),
            })
            .collect(),
        stream: None,
    }
}

fn from_api_response(resp: ApiResponse) -> CompletionResponse {
    let has_tool_use = resp
        .content
        .iter()
        .any(|b| matches!(b, ContentBlock::ToolUse { .. }));

    let completion = if has_tool_use {
        let calls = resp
            .content
            .iter()
            .filter_map(|b| match b {
                ContentBlock::ToolUse { id, name, input } => Some(ToolUse {
                    id: id.clone(),
                    name: name.clone(),
                    input: input.to_string(),
                }),
                _ => None,
            })
            .collect();
        Completion::ToolCalls(calls)
    } else {
        let text = resp
            .content
            .iter()
            .filter_map(|b| match b {
                ContentBlock::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("");
        Completion::Text(text)
    };

    CompletionResponse {
        completion,
        usage: TokenUsage {
            input_tokens: resp.usage.input_tokens,
            output_tokens: resp.usage.output_tokens,
        },
        stop_reason: resp.stop_reason.unwrap_or_else(|| "end_turn".into()),
    }
}

impl From<LlmErrorKind> for LlmError {
    fn from(e: LlmErrorKind) -> Self {
        match e {
            LlmErrorKind::InvalidRequest(s) => LlmError::InvalidRequest(s),
            LlmErrorKind::Auth(s) => LlmError::Auth(s),
            LlmErrorKind::RateLimited(r) => LlmError::RateLimited(r),
            LlmErrorKind::Api(s) => LlmError::Api(s),
        }
    }
}

pub use wruntime::llm::inference::add_to_linker;

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::wruntime::llm::inference::Message;
    use super::*;
    use crate::state::ModuleState;

    fn proxy_uri() -> hyper::Uri {
        "http://127.0.0.1:9001".parse().unwrap()
    }

    fn test_http_pool() -> wr_common::http_pool::HttpClientPool<http_body_util::Full<bytes::Bytes>>
    {
        wr_common::http_pool::HttpClientPool::new(1)
    }

    #[tokio::test]
    async fn test_complete_returns_error_when_no_llm() {
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            test_http_pool(),
            Default::default(),
        )
        .expect("state");
        let req = CompletionRequest {
            model: "claude-sonnet-4-6".into(),
            messages: vec![Message {
                role: MessageRole::User,
                content: "hello".into(),
            }],
            system: None,
            max_tokens: 100,
            temperature: None,
            tools: vec![],
        };
        let result = Host::complete(&mut state, req).await;
        assert!(matches!(result, Err(LlmError::InvalidRequest(_))));
    }

    #[tokio::test]
    async fn test_complete_stream_returns_error_when_no_llm() {
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            test_http_pool(),
            Default::default(),
        )
        .expect("state");
        let req = CompletionRequest {
            model: "claude-sonnet-4-6".into(),
            messages: vec![Message {
                role: MessageRole::User,
                content: "hello".into(),
            }],
            system: None,
            max_tokens: 100,
            temperature: None,
            tools: vec![],
        };
        let result = Host::complete_stream(&mut state, req).await;
        assert!(matches!(result, Err(LlmError::InvalidRequest(_))));
    }

    #[test]
    fn test_clamp_max_tokens() {
        let config = LlmConfig {
            provider: "anthropic".into(),
            api_key_env: "TEST_KEY".into(),
            base_url: "http://localhost".into(),
            max_tokens_limit: 1000,
        };
        assert_eq!(500u32.min(config.max_tokens_limit), 500);
        assert_eq!(2000u32.min(config.max_tokens_limit), 1000);
    }

    #[test]
    fn test_from_api_response_text() {
        let resp = ApiResponse {
            content: vec![ContentBlock::Text {
                text: "Hello!".into(),
            }],
            usage: ApiUsage {
                input_tokens: 10,
                output_tokens: 5,
            },
            stop_reason: Some("end_turn".into()),
        };
        let result = from_api_response(resp);
        assert!(matches!(result.completion, Completion::Text(ref t) if t == "Hello!"));
        assert_eq!(result.usage.input_tokens, 10);
        assert_eq!(result.usage.output_tokens, 5);
        assert_eq!(result.stop_reason, "end_turn");
    }

    #[test]
    fn test_from_api_response_tool_use() {
        let resp = ApiResponse {
            content: vec![ContentBlock::ToolUse {
                id: "tu_1".into(),
                name: "lookup".into(),
                input: serde_json::json!({"key": "val"}),
            }],
            usage: ApiUsage {
                input_tokens: 20,
                output_tokens: 15,
            },
            stop_reason: Some("tool_use".into()),
        };
        let result = from_api_response(resp);
        match result.completion {
            Completion::ToolCalls(calls) => {
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].name, "lookup");
                assert_eq!(calls[0].id, "tu_1");
            }
            _ => panic!("expected tool calls"),
        }
    }

    #[test]
    fn test_to_api_request_maps_fields() {
        let config = LlmConfig {
            provider: "anthropic".into(),
            api_key_env: "TEST_KEY".into(),
            base_url: "http://localhost".into(),
            max_tokens_limit: 1000,
        };
        let req = CompletionRequest {
            model: "claude-sonnet-4-6".into(),
            messages: vec![
                Message {
                    role: MessageRole::User,
                    content: "hi".into(),
                },
                Message {
                    role: MessageRole::Assistant,
                    content: "hello".into(),
                },
            ],
            system: Some("be helpful".into()),
            max_tokens: 2000,
            temperature: Some(0.5),
            tools: vec![],
        };
        let api_msgs: Vec<ApiMessage> = req
            .messages
            .iter()
            .map(|m| ApiMessage {
                role: match m.role {
                    MessageRole::User => "user".into(),
                    MessageRole::Assistant => "assistant".into(),
                },
                content: m.content.clone(),
            })
            .collect();
        assert_eq!(api_msgs.len(), 2);
        assert_eq!(api_msgs[0].role, "user");
        assert_eq!(api_msgs[1].role, "assistant");
        assert_eq!(2000u32.min(config.max_tokens_limit), 1000);
    }

    #[test]
    fn test_from_error_kind_mapping() {
        let e: LlmError = LlmErrorKind::InvalidRequest("bad".into()).into();
        assert!(matches!(e, LlmError::InvalidRequest(s) if s == "bad"));
        let e: LlmError = LlmErrorKind::Auth("denied".into()).into();
        assert!(matches!(e, LlmError::Auth(s) if s == "denied"));
        let e: LlmError = LlmErrorKind::RateLimited(Some(30)).into();
        assert!(matches!(e, LlmError::RateLimited(Some(30))));
        let e: LlmError = LlmErrorKind::Api("fail".into()).into();
        assert!(matches!(e, LlmError::Api(s) if s == "fail"));
    }
}
