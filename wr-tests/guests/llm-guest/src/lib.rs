#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/test.rs"));
}

#[allow(dead_code, unused_imports)]
mod bindings;

use wr_sdk::bindings::wruntime::llm::inference;
use wr_sdk::llm::CompletionBuilder;
use wr_sdk::prelude::*;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        proto::llm_test_service_handle(&Component, request, response_out);
    }
}

impl proto::LlmTestService for Component {
    fn complete(
        &self,
        req: proto::CompleteRequest,
    ) -> Result<proto::CompleteResponse, ServiceError> {
        let mut builder = CompletionBuilder::new(&req.model);
        if !req.system.is_empty() {
            builder = builder.system(&req.system);
        }
        builder = builder.user(&req.user_message).max_tokens(req.max_tokens);

        let resp = builder.complete()?;

        let text = match resp.completion {
            inference::Completion::Text(t) => t,
            inference::Completion::ToolCalls(_) => String::new(),
        };

        Ok(proto::CompleteResponse {
            text,
            stop_reason: resp.stop_reason,
            input_tokens: resp.usage.input_tokens,
            output_tokens: resp.usage.output_tokens,
        })
    }

    fn complete_text(
        &self,
        req: proto::CompleteTextRequest,
    ) -> Result<proto::CompleteTextResponse, ServiceError> {
        let text = CompletionBuilder::sonnet()
            .user(&req.user_message)
            .complete_text()?;

        Ok(proto::CompleteTextResponse { text })
    }

    fn tool_use(
        &self,
        req: proto::ToolUseRequest,
    ) -> Result<proto::ToolUseResponse, ServiceError> {
        let resp = CompletionBuilder::sonnet()
            .user(&req.user_message)
            .tool(&req.tool_name, &req.tool_description, &req.tool_schema)
            .complete()?;

        match resp.completion {
            inference::Completion::ToolCalls(calls) => {
                let call = calls
                    .first()
                    .ok_or_else(|| ServiceError::internal("no tool calls returned"))?;
                Ok(proto::ToolUseResponse {
                    tool_name: call.name.clone(),
                    tool_id: call.id.clone(),
                    tool_input: call.input.clone(),
                    stop_reason: resp.stop_reason,
                })
            }
            inference::Completion::Text(_) => {
                Err(ServiceError::internal("expected tool_use but got text"))
            }
        }
    }

    fn error(
        &self,
        req: proto::LlmErrorRequest,
    ) -> Result<proto::LlmErrorResponse, ServiceError> {
        let result = CompletionBuilder::sonnet()
            .user(&req.user_message)
            .complete();

        match result {
            Ok(_) => Ok(proto::LlmErrorResponse {
                error_kind: "none".into(),
                error_message: "no error".into(),
            }),
            Err(e) => {
                let (kind, msg) = match e {
                    inference::LlmError::InvalidRequest(m) => ("invalid-request", m),
                    inference::LlmError::Auth(m) => ("auth", m),
                    inference::LlmError::RateLimited(_) => {
                        ("rate-limited", "rate limited".into())
                    }
                    inference::LlmError::Api(m) => ("api", m),
                };
                Ok(proto::LlmErrorResponse {
                    error_kind: kind.into(),
                    error_message: msg,
                })
            }
        }
    }

    fn stream(
        &self,
        req: proto::StreamRequest,
    ) -> Result<proto::StreamResponse, ServiceError> {
        let stream = CompletionBuilder::sonnet()
            .user(&req.user_message)
            .stream()?;

        let mut text = String::new();
        let mut chunk_count: u32 = 0;
        loop {
            match stream.next() {
                Ok(Some(chunk)) => {
                    text.push_str(&chunk);
                    chunk_count += 1;
                }
                Ok(None) => break,
                Err(e) => return Err(ServiceError::from(e)),
            }
        }

        Ok(proto::StreamResponse { text, chunk_count })
    }
}
