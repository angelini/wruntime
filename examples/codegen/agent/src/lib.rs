#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/codegen.rs"));
}

#[allow(dead_code, unused_imports)]
mod bindings {
    wit_bindgen::generate!({
        path: "wit",
        world: "agent",
        generate_all,
    });
}

use serde::Deserialize;
use wr_sdk::bindings::wasi::clocks::monotonic_clock;
use wr_sdk::bindings::wruntime::llm::inference::{CompletionResponse, LlmError};
use wr_sdk::llm::{CompletionBuilder, MaxTokens};
use wr_sdk::prelude::*;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

const BUCKET: &str = "codegen";
const MAX_CONTEXT_BYTES: usize = 300 * 1024; // ~300 KB context budget
const MAX_RETRIES: u32 = 3;

/// Call a builder-producing closure, retrying on rate-limit errors with a
/// sleep based on the retry-after hint from the API.
fn complete_with_retry(
    mut build: impl FnMut() -> CompletionBuilder,
) -> Result<CompletionResponse, LlmError> {
    for attempt in 0..=MAX_RETRIES {
        match build().complete() {
            Ok(resp) => return Ok(resp),
            Err(LlmError::RateLimited(retry_after)) if attempt < MAX_RETRIES => {
                let secs = retry_after.unwrap_or(30);
                wr_sdk::log!(
                    "rate limited, retrying in {secs}s (attempt {}/{})",
                    attempt + 1,
                    MAX_RETRIES
                );
                let nanos = secs as u64 * 1_000_000_000;
                monotonic_clock::subscribe_duration(nanos).block();
            }
            other => return other,
        }
    }
    unreachable!()
}

impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        proto::agent_service_handle(&Component, request, response_out);
    }
}

// ── Manifest type (matches collector's output) ──────────────────────────────

#[derive(Deserialize)]
struct Manifest {
    files: Vec<ManifestEntry>,
}

#[derive(Deserialize)]
struct ManifestEntry {
    key: String,
    size: u64,
}

#[derive(FromRow)]
struct SessionRow {
    status: String,
    latest_diff: String,
}

#[derive(FromRow)]
struct TurnRow {
    turn_number: i32,
    user_prompt: String,
    assistant_response: String,
    input_tokens: i32,
    output_tokens: i32,
}

// ── Service implementation ───────────────────────────────────────────────────

impl proto::AgentService for Component {
    fn run_task(&self, req: proto::RunTaskRequest) -> Result<proto::RunTaskResponse, ServiceError> {
        let session_id = &req.session_id;
        let max_turns = if req.max_turns > 0 { req.max_turns } else { 3 };
        let doc_prefix_count =
            i64::try_from(req.doc_prefixes.len()).expect("doc prefix count fits in i64");

        let span = wr_sdk::span!(
            "agent.run_task",
            "session.id" => session_id.as_str(),
            "agent.max_turns" => max_turns,
            "agent.doc_prefixes" => doc_prefix_count
        );

        // Create session in DB.
        query(
            "INSERT INTO sessions (session_id, status) VALUES ($1, 'active') \
             ON CONFLICT (session_id) DO UPDATE SET status = 'active', updated_at = now()",
        )
        .bind(session_id.clone())
        .execute()?;

        // Store doc prefix associations.
        for prefix in &req.doc_prefixes {
            query(
                "INSERT INTO session_doc_prefixes (session_id, doc_prefix) VALUES ($1, $2) \
                 ON CONFLICT DO NOTHING",
            )
            .bind(session_id.clone())
            .bind(prefix.clone())
            .execute()?;
        }

        // Load relevant documentation from blobstore.
        let ctx_span = wr_sdk::span!("agent.build_context");
        let context = build_context(&req.doc_prefixes, &req.task_description)?;
        wr_sdk::set_attrs!(
            ctx_span,
            "context.length" => i64::try_from(context.len()).expect("context length fits in i64")
        );
        drop(ctx_span);

        let system_prompt = format!(
            "You are a code generation agent. You produce unified diffs (patches) for code changes.\n\n\
             ## Context\n\
             The context below includes both source code files (from the repository) and \
             documentation. Use the source code to understand the existing codebase and produce \
             accurate diffs against the actual file contents.\n\n\
             {context}\n\n\
             ## Output Format\n\
             Produce a unified diff that can be applied with `patch -p1`. \
             Include file paths relative to the repository root. \
             Only output the diff, no explanations."
        );

        // Multi-turn LLM loop.
        let mut total_input: u32 = 0;
        let mut total_output: u32 = 0;
        let mut latest_diff;
        let mut turn: u32 = 0;

        // Turn 1: initial generation.
        let user_prompt = format!("## Task\n{}", req.task_description);

        let turn_span = wr_sdk::span!("agent.llm_turn", "turn" => 1_i64);
        wr_sdk::log!(
            "llm call: turn=1 model=claude-sonnet-4-6 messages=1 max_tokens=8192 system_len={}",
            system_prompt.len()
        );
        let resp = complete_with_retry(|| {
            CompletionBuilder::sonnet()
                .system(&system_prompt)
                .max_tokens(MaxTokens::new(8192).expect("8192 is a valid token limit"))
                .user(&user_prompt)
        })
        .map_err(|e| {
            tracing::set_error(&turn_span, &format!("llm complete: {e:?}"));
            ServiceError::internal(format!("llm complete: {e:?}"))
        })?;

        let text = match resp.completion {
            wr_sdk::bindings::wruntime::llm::inference::Completion::Text(s) => s,
            wr_sdk::bindings::wruntime::llm::inference::Completion::ToolCalls(_) => {
                return Err(ServiceError::internal("unexpected tool_use response"));
            }
        };

        total_input = total_input.saturating_add(resp.usage.input_tokens);
        total_output = total_output.saturating_add(resp.usage.output_tokens);
        latest_diff = text.clone();
        turn += 1;

        wr_sdk::log!(
            "llm response: turn=1 input_tokens={} output_tokens={} response_len={}",
            resp.usage.input_tokens,
            resp.usage.output_tokens,
            text.len()
        );
        wr_sdk::set_attrs!(
            turn_span,
            "tokens.input" => resp.usage.input_tokens,
            "tokens.output" => resp.usage.output_tokens
        );
        drop(turn_span);

        store_turn(session_id, turn, &user_prompt, &text, &resp.usage)?;

        // Subsequent turns: review and refine.
        let mut prev_assistant = text;
        while turn < max_turns {
            let refine_prompt = "Review the diff you just produced. \
                Check for correctness, missing changes, and consistency with the documentation. \
                If improvements are needed, produce an updated unified diff. \
                If the diff is already correct, respond with exactly: LGTM";

            let turn_span = wr_sdk::span!("agent.llm_turn", "turn" => turn + 1, "type" => "refine");
            wr_sdk::log!(
                "llm call: turn={} type=refine model=claude-sonnet-4-6 messages=2 max_tokens=8192",
                turn + 1,
            );
            // Refinement turns don't resend the full context — the model's
            // own diff output contains everything needed for self-review.
            let resp = complete_with_retry(|| {
                CompletionBuilder::sonnet()
                    .max_tokens(MaxTokens::new(8192).expect("8192 is a valid token limit"))
                    .user(format!(
                        "Here is a unified diff I produced:\n\n{prev_assistant}"
                    ))
                    .user(refine_prompt)
            })
            .map_err(|e| {
                tracing::set_error(&turn_span, &format!("llm refine: {e:?}"));
                ServiceError::internal(format!("llm refine: {e:?}"))
            })?;

            let text = match resp.completion {
                wr_sdk::bindings::wruntime::llm::inference::Completion::Text(s) => s,
                wr_sdk::bindings::wruntime::llm::inference::Completion::ToolCalls(_) => {
                    wr_sdk::log!(
                        "llm response: turn={} unexpected tool_use, stopping",
                        turn + 1
                    );
                    drop(turn_span);
                    break;
                }
            };

            total_input = total_input.saturating_add(resp.usage.input_tokens);
            total_output = total_output.saturating_add(resp.usage.output_tokens);
            turn += 1;

            wr_sdk::log!(
                "llm response: turn={turn} type=refine input_tokens={} output_tokens={} response_len={} lgtm={}",
                resp.usage.input_tokens, resp.usage.output_tokens, text.len(), text.trim() == "LGTM"
            );
            wr_sdk::set_attrs!(
                turn_span,
                "tokens.input" => resp.usage.input_tokens,
                "tokens.output" => resp.usage.output_tokens
            );

            store_turn(session_id, turn, refine_prompt, &text, &resp.usage)?;

            if text.trim() == "LGTM" {
                wr_sdk::event!(turn_span, "lgtm");
                drop(turn_span);
                break;
            }
            drop(turn_span);

            latest_diff = text.clone();
            prev_assistant = text;
        }

        // Update session with final result.
        query(
            "UPDATE sessions SET status = 'complete', latest_diff = $2, updated_at = now() \
             WHERE session_id = $1",
        )
        .bind(session_id.clone())
        .bind(latest_diff.clone())
        .execute()?;

        wr_sdk::set_attrs!(
            span,
            "agent.turns_used" => turn,
            "agent.total_input_tokens" => total_input,
            "agent.total_output_tokens" => total_output,
            "agent.diff_bytes" => i64::try_from(latest_diff.len())
                .expect("diff length fits in i64")
        );
        drop(span);

        Ok(proto::RunTaskResponse {
            session_id: session_id.clone(),
            unified_diff: latest_diff,
            turns_used: turn,
            input_tokens: total_input,
            output_tokens: total_output,
            status: proto::AgentRunStatus::Complete as i32,
            message: format!("completed in {turn} turn(s)"),
        })
    }

    fn get_session(
        &self,
        req: proto::GetSessionRequest,
    ) -> Result<proto::GetSessionResponse, ServiceError> {
        let session = query_as::<SessionRow>(
            "SELECT status, latest_diff FROM sessions WHERE session_id = $1",
        )
        .bind(req.session_id.clone())
        .fetch_optional()?
        .ok_or_else(|| ServiceError::not_found("session not found"))?;

        let status = match session.status.as_str() {
            "active" => proto::SessionStatus::Active,
            "complete" => proto::SessionStatus::Complete,
            "error" => proto::SessionStatus::Error,
            value => {
                return Err(ServiceError::internal(format!(
                    "invalid session status in database: {value}"
                )))
            }
        } as i32;

        let turns = query_as::<TurnRow>(
            "SELECT turn_number, user_prompt, assistant_resp AS assistant_response, \
             input_tokens, output_tokens \
             FROM conversation_turns WHERE session_id = $1 ORDER BY turn_number",
        )
        .bind(req.session_id.clone())
        .fetch_all()?
        .into_iter()
        .map(|row| {
            Ok(proto::ConversationTurn {
                turn_number: u32::try_from(row.turn_number)
                    .map_err(|_| ServiceError::internal("negative turn number in database"))?,
                user_prompt: row.user_prompt,
                assistant_response: row.assistant_response,
                input_tokens: u32::try_from(row.input_tokens).map_err(|_| {
                    ServiceError::internal("negative input token count in database")
                })?,
                output_tokens: u32::try_from(row.output_tokens).map_err(|_| {
                    ServiceError::internal("negative output token count in database")
                })?,
            })
        })
        .collect::<Result<Vec<_>, ServiceError>>()?;
        let turn_count = u32::try_from(turns.len())
            .map_err(|_| ServiceError::internal("turn count exceeds response range"))?;

        Ok(proto::GetSessionResponse {
            session_id: req.session_id,
            turn_count,
            latest_diff: session.latest_diff,
            status,
            turns,
        })
    }
}

// ── Context builder ──────────────────────────────────────────────────────────

fn build_context(doc_prefixes: &[String], task_description: &str) -> Result<String, ServiceError> {
    let task_lower = task_description.to_lowercase();
    let mut context = String::new();
    let mut total_size: usize = 0;
    let docs = bucket(BUCKET)?;

    for prefix in doc_prefixes {
        let manifest_key = format!("{prefix}/manifest.json");
        let manifest_data = docs.get(&manifest_key)?;

        let manifest: Manifest = serde_json::from_slice(&manifest_data)
            .map_err(|e| ServiceError::internal(format!("parse manifest: {e}")))?;

        // Score and sort files by relevance.
        let mut scored: Vec<(i32, &ManifestEntry)> = manifest
            .files
            .iter()
            .map(|entry| {
                let key_lower = entry.key.to_lowercase();
                let mut score: i32 = 0;

                // Prioritize README and main entry points.
                if key_lower.contains("readme") {
                    score += 10;
                }
                if key_lower.ends_with("lib.rs") || key_lower.ends_with("mod.rs") {
                    score += 8;
                }
                if key_lower.ends_with(".md") {
                    score += 5;
                }
                // Boost source code files so the agent has real code to diff against.
                if key_lower.ends_with(".rs")
                    || key_lower.ends_with(".toml")
                    || key_lower.ends_with(".proto")
                {
                    score += 6;
                }

                // Boost files whose names match task keywords.
                for word in task_lower.split_whitespace() {
                    if word.len() > 2 && key_lower.contains(word) {
                        score += 3;
                    }
                }

                // Penalize very large files.
                if entry.size > 100_000 {
                    score -= 2;
                }

                (score, entry)
            })
            .collect();

        scored.sort_by_key(|item| std::cmp::Reverse(item.0));

        for (_, entry) in scored {
            if total_size + entry.size as usize > MAX_CONTEXT_BYTES {
                continue;
            }

            let data = docs.get(&entry.key)?;
            let text = String::from_utf8(data).map_err(|error| {
                ServiceError::internal(format!("{} is not UTF-8: {error}", entry.key))
            })?;
            let label = match entry.key.find("/files/") {
                Some(index) => &entry.key[index + 7..],
                None => &entry.key,
            };

            context.push_str(&format!("<file: {label}>\n{text}\n</file>\n\n"));
            total_size = total_size
                .checked_add(text.len())
                .ok_or_else(|| ServiceError::internal("context byte count overflow"))?;
        }
    }

    Ok(context)
}

// ── DB helpers ───────────────────────────────────────────────────────────────

fn store_turn(
    session_id: &str,
    turn_number: u32,
    user_prompt: &str,
    assistant_resp: &str,
    usage: &wr_sdk::bindings::wruntime::llm::inference::TokenUsage,
) -> Result<(), ServiceError> {
    query(
        "INSERT INTO conversation_turns \
         (session_id, turn_number, user_prompt, assistant_resp, input_tokens, output_tokens) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(session_id.to_owned())
    .bind(
        i32::try_from(turn_number)
            .map_err(|_| ServiceError::bad_request("turn number exceeds database range"))?,
    )
    .bind(user_prompt.to_owned())
    .bind(assistant_resp.to_owned())
    .bind(
        i32::try_from(usage.input_tokens)
            .map_err(|_| ServiceError::bad_request("input token count exceeds database range"))?,
    )
    .bind(
        i32::try_from(usage.output_tokens)
            .map_err(|_| ServiceError::bad_request("output token count exceeds database range"))?,
    )
    .execute()?;
    Ok(())
}
