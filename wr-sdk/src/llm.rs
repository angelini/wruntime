use crate::bindings::wruntime::llm::inference::*;
use crate::ServiceError;

impl From<LlmError> for ServiceError {
    fn from(e: LlmError) -> Self {
        match e {
            LlmError::InvalidRequest(msg) => ServiceError::bad_request(format!("llm: {msg}")),
            LlmError::Auth(msg) => ServiceError::internal(format!("llm auth: {msg}")),
            LlmError::RateLimited(retry_after) => {
                let msg = match retry_after {
                    Some(secs) => format!("llm rate limited (retry after {secs}s)"),
                    None => "llm rate limited".into(),
                };
                ServiceError {
                    status: 429,
                    message: msg,
                }
            }
            LlmError::Api(msg) => ServiceError::internal(format!("llm api: {msg}")),
        }
    }
}

/// Builder for completion requests.
pub struct CompletionBuilder {
    model: String,
    messages: Vec<Message>,
    system: Option<String>,
    max_tokens: u32,
    temperature: Option<f32>,
    tools: Vec<ToolDef>,
}

impl CompletionBuilder {
    pub fn new(model: &str) -> Self {
        Self {
            model: model.into(),
            messages: vec![],
            system: None,
            max_tokens: 4096,
            temperature: None,
            tools: vec![],
        }
    }

    /// Shorthand for claude-sonnet-4-6.
    pub fn sonnet() -> Self {
        Self::new("claude-sonnet-4-6")
    }

    /// Shorthand for claude-haiku-4-5-20251001.
    pub fn haiku() -> Self {
        Self::new("claude-haiku-4-5-20251001")
    }

    pub fn system(mut self, s: impl Into<String>) -> Self {
        self.system = Some(s.into());
        self
    }

    pub fn user(mut self, content: impl Into<String>) -> Self {
        self.messages.push(Message {
            role: MessageRole::User,
            content: content.into(),
        });
        self
    }

    pub fn assistant(mut self, content: impl Into<String>) -> Self {
        self.messages.push(Message {
            role: MessageRole::Assistant,
            content: content.into(),
        });
        self
    }

    pub fn max_tokens(mut self, n: u32) -> Self {
        self.max_tokens = n;
        self
    }

    pub fn temperature(mut self, t: f32) -> Self {
        self.temperature = Some(t);
        self
    }

    pub fn tool(mut self, name: &str, description: &str, schema: &str) -> Self {
        self.tools.push(ToolDef {
            name: name.into(),
            description: description.into(),
            input_schema: schema.into(),
        });
        self
    }

    /// Send and get the full response.
    pub fn complete(self) -> Result<CompletionResponse, LlmError> {
        complete(&self.into_request())
    }

    /// Send and get a streaming cursor.
    pub fn stream(self) -> Result<CompletionStream, LlmError> {
        complete_stream(&self.into_request())
    }

    /// Send, expect text, return just the string.
    pub fn complete_text(self) -> Result<String, LlmError> {
        let resp = self.complete()?;
        match resp.completion {
            Completion::Text(s) => Ok(s),
            Completion::ToolCalls(_) => Err(LlmError::InvalidRequest(
                "expected text but got tool_use".into(),
            )),
        }
    }

    fn into_request(self) -> CompletionRequest {
        CompletionRequest {
            model: self.model,
            messages: self.messages,
            system: self.system,
            max_tokens: self.max_tokens,
            temperature: self.temperature,
            tools: self.tools,
        }
    }
}

/// Collect a stream's text into a single string (ignores usage/stop events).
pub fn collect_stream(stream: CompletionStream) -> Result<String, LlmError> {
    let mut buf = String::new();
    loop {
        match stream.next()? {
            Some(StreamEvent::TextDelta(s)) => buf.push_str(&s),
            Some(StreamEvent::Usage(_)) | Some(StreamEvent::Stop(_)) => {}
            None => return Ok(buf),
        }
    }
}
