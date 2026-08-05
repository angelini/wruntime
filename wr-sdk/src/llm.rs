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

#[derive(Clone, Debug)]
pub struct ModelName(String);
impl ModelName {
    pub fn parse(value: impl Into<String>) -> Result<Self, LlmError> {
        let value = value.into();
        if value.trim().is_empty() {
            Err(LlmError::InvalidRequest("model must not be empty".into()))
        } else {
            Ok(Self(value))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug)]
pub struct MaxTokens(std::num::NonZeroU32);
impl MaxTokens {
    pub fn new(value: u32) -> Result<Self, LlmError> {
        std::num::NonZeroU32::new(value)
            .map(Self)
            .ok_or_else(|| LlmError::InvalidRequest("max_tokens must be > 0".into()))
    }
    pub fn get(self) -> u32 {
        self.0.get()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Temperature(f32);
impl Temperature {
    pub fn new(value: f32) -> Result<Self, LlmError> {
        if value.is_finite() && (0.0..=1.0).contains(&value) {
            Ok(Self(value))
        } else {
            Err(LlmError::InvalidRequest(
                "temperature must be finite and between 0.0 and 1.0".into(),
            ))
        }
    }
    pub fn get(self) -> f32 {
        self.0
    }
}

#[derive(Clone, Debug)]
pub struct ToolSchema(String);
impl ToolSchema {
    pub fn parse(value: &str) -> Result<Self, LlmError> {
        let parsed: serde_json::Value = serde_json::from_str(value)
            .map_err(|e| LlmError::InvalidRequest(format!("invalid tool schema JSON: {e}")))?;
        if !parsed.is_object() {
            return Err(LlmError::InvalidRequest(
                "tool schema must be a JSON object".into(),
            ));
        }
        Ok(Self(value.to_string()))
    }
    pub fn as_str(&self) -> &str {
        &self.0
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
    pub fn new(model: ModelName) -> Self {
        Self {
            model: model.0,
            messages: vec![],
            system: None,
            max_tokens: 4096,
            temperature: None,
            tools: vec![],
        }
    }

    /// Shorthand for claude-sonnet-4-6.
    pub fn sonnet() -> Self {
        Self::new(ModelName("claude-sonnet-4-6".into()))
    }

    /// Shorthand for claude-haiku-4-5-20251001.
    pub fn haiku() -> Self {
        Self::new(ModelName("claude-haiku-4-5-20251001".into()))
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

    pub fn max_tokens(mut self, value: MaxTokens) -> Self {
        self.max_tokens = value.get();
        self
    }

    pub fn temperature(mut self, value: Temperature) -> Self {
        self.temperature = Some(value.get());
        self
    }

    pub fn tool(mut self, name: &str, description: &str, schema: ToolSchema) -> Self {
        self.tools.push(ToolDef {
            name: name.into(),
            description: description.into(),
            input_schema: schema.0,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_llm_values_validate_boundaries_before_builder_use() {
        assert!(ModelName::parse(" ").is_err());
        assert!(MaxTokens::new(0).is_err());
        assert_eq!(MaxTokens::new(1).expect("lower boundary").get(), 1);
        assert_eq!(
            MaxTokens::new(u32::MAX).expect("upper boundary").get(),
            u32::MAX
        );
        assert!(Temperature::new(f32::NEG_INFINITY).is_err());
        assert!(Temperature::new(f32::NAN).is_err());
        assert!(Temperature::new(-f32::EPSILON).is_err());
        assert!(Temperature::new(1.0 + f32::EPSILON).is_err());
        assert_eq!(Temperature::new(0.0).expect("lower boundary").get(), 0.0);
        assert_eq!(Temperature::new(1.0).expect("upper boundary").get(), 1.0);
        assert!(ToolSchema::parse("not-json").is_err());
        assert!(ToolSchema::parse("[]").is_err());
        assert!(ToolSchema::parse(r#"{"type":"object"}"#).is_ok());
    }

    #[test]
    fn typed_setters_store_validated_values_in_one_signature_family() {
        let request = CompletionBuilder::new(ModelName::parse("model").expect("valid model"))
            .max_tokens(MaxTokens::new(1).expect("valid tokens"))
            .temperature(Temperature::new(1.0).expect("valid temperature"))
            .tool(
                "lookup",
                "Lookup a value",
                ToolSchema::parse(r#"{"type":"object"}"#).expect("valid schema"),
            )
            .into_request();

        assert_eq!(request.model, "model");
        assert_eq!(request.max_tokens, 1);
        assert_eq!(request.temperature, Some(1.0));
        assert_eq!(request.tools.len(), 1);
        assert_eq!(request.tools[0].name, "lookup");
        assert_eq!(request.tools[0].input_schema, r#"{"type":"object"}"#);
    }
}
