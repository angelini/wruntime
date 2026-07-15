#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/test.rs"));
}

#[allow(dead_code, unused_imports)]
mod bindings {
    wit_bindgen::generate!({
        path: "wit",
        world: "llm-guest",
        generate_all,
    });
}

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
            stop_reason: stop_reason(&resp.stop_reason),
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

    fn tool_use(&self, req: proto::ToolUseRequest) -> Result<proto::ToolUseResponse, ServiceError> {
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
                    stop_reason: stop_reason(&resp.stop_reason),
                })
            }
            inference::Completion::Text(_) => {
                Err(ServiceError::internal("expected tool_use but got text"))
            }
        }
    }

    fn error(&self, req: proto::LlmErrorRequest) -> Result<proto::LlmErrorResponse, ServiceError> {
        let result = CompletionBuilder::sonnet()
            .user(&req.user_message)
            .complete();

        match result {
            Ok(_) => Ok(proto::LlmErrorResponse {
                error_kind: proto::LlmErrorKind::None as i32,
                error_message: "no error".into(),
            }),
            Err(e) => {
                let (kind, msg) = llm_error_parts(e);
                Ok(proto::LlmErrorResponse {
                    error_kind: kind,
                    error_message: msg,
                })
            }
        }
    }

    fn stream(&self, req: proto::StreamRequest) -> Result<proto::StreamResponse, ServiceError> {
        let mut builder = CompletionBuilder::sonnet().user(&req.user_message);
        if req.with_tools {
            builder = builder.tool("dummy", "dummy tool", r#"{"type":"object"}"#);
        }

        let stream = match builder.stream() {
            Ok(s) => s,
            Err(e) => {
                let (kind, msg) = llm_error_parts(e);
                return Ok(proto::StreamResponse {
                    error_kind: kind,
                    error_message: msg,
                    ..Default::default()
                });
            }
        };

        let mut resp = proto::StreamResponse::default();
        loop {
            match stream.next() {
                Ok(Some(inference::StreamEvent::TextDelta(chunk))) => {
                    resp.text.push_str(&chunk);
                    resp.chunk_count += 1;
                    resp.events.push(proto::StreamEventKind::TextDelta as i32);
                    if resp.chunk_count == 1 {
                        resp.usage_mid_none = stream.usage().is_none();
                    }
                }
                Ok(Some(inference::StreamEvent::Usage(u))) => {
                    resp.events.push(proto::StreamEventKind::Usage as i32);
                    resp.input_tokens = u.input_tokens;
                    resp.output_tokens = u.output_tokens;
                }
                Ok(Some(inference::StreamEvent::Stop(reason))) => {
                    resp.events.push(proto::StreamEventKind::Stop as i32);
                    resp.stop_reason = stop_reason(&reason);
                }
                Ok(None) => {
                    resp.usage_present_after = stream.usage().is_some();
                    break;
                }
                Err(e) => {
                    let (kind, msg) = llm_error_parts(e);
                    resp.error_kind = kind;
                    resp.error_message = msg;
                    break;
                }
            }
        }
        Ok(resp)
    }

    fn alloc_streams(
        &self,
        req: proto::AllocStreamsRequest,
    ) -> Result<proto::AllocStreamsResponse, ServiceError> {
        let mut held = Vec::new();
        let mut resp = proto::AllocStreamsResponse::default();
        let open = |resp: &mut proto::AllocStreamsResponse, held: &mut Vec<_>, n: u32| {
            for _ in 0..n {
                match CompletionBuilder::sonnet().user("hi").stream() {
                    Ok(s) => held.push(s),
                    Err(inference::LlmError::Api(m)) => {
                        resp.hit_cap = true;
                        resp.error_kind = proto::LlmErrorKind::Api as i32;
                        resp.error_message = m;
                        break;
                    }
                    Err(e) => {
                        resp.error_kind = proto::LlmErrorKind::Other as i32;
                        resp.error_message = format!("{e:?}");
                        break;
                    }
                }
            }
        };
        open(&mut resp, &mut held, req.initial);
        for _ in 0..req.drop_count {
            held.pop(); // CompletionStream dropped -> host drop -> live-count decrement
        }
        open(&mut resp, &mut held, req.additional);
        resp.held = held.len() as u32;
        Ok(resp)
    }
}

fn stop_reason(value: &str) -> i32 {
    match value {
        "end_turn" => proto::StopReason::EndTurn as i32,
        "tool_use" => proto::StopReason::ToolUse as i32,
        "max_tokens" => proto::StopReason::MaxTokens as i32,
        "" => proto::StopReason::Unspecified as i32,
        _ => proto::StopReason::Other as i32,
    }
}

fn llm_error_parts(e: inference::LlmError) -> (i32, String) {
    match e {
        inference::LlmError::InvalidRequest(m) => (proto::LlmErrorKind::InvalidRequest as i32, m),
        inference::LlmError::Auth(m) => (proto::LlmErrorKind::Auth as i32, m),
        inference::LlmError::RateLimited(_) => (
            proto::LlmErrorKind::RateLimited as i32,
            "rate limited".into(),
        ),
        inference::LlmError::Api(m) => (proto::LlmErrorKind::Api as i32, m),
    }
}
