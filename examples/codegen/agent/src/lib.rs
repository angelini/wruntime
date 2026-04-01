#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/codegen.rs"));
}

#[allow(dead_code, unused_imports)]
mod bindings;

use serde::Deserialize;
use wr_sdk::bindings::wasi::http::types::{IncomingRequest, ResponseOutparam};
use wr_sdk::bindings::wruntime::blobstore::store;
use wr_sdk::bindings::wruntime::db::database::{self, PgValue};
use wr_sdk::io::{read_body, send_response};
use wr_sdk::llm::CompletionBuilder;
use wr_sdk::ServiceError;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

const BUCKET: &str = "codegen";
const MAX_CONTEXT_BYTES: usize = 500 * 1024; // ~500 KB context budget

impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let path = request.path_with_query().unwrap_or_default();
        let body = read_body(request.consume().unwrap());
        let (status, resp) = proto::agent_service_router(&Component, &path, &body);
        send_response(response_out, status, resp);
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

// ── Service implementation ───────────────────────────────────────────────────

impl proto::AgentService for Component {
    fn run_task(
        &self,
        req: proto::RunTaskRequest,
    ) -> Result<proto::RunTaskResponse, ServiceError> {
        let session_id = &req.session_id;
        let max_turns = if req.max_turns > 0 { req.max_turns } else { 3 };

        // Create session in DB.
        database::execute(
            "INSERT INTO sessions (session_id, status) VALUES ($1, 'active') \
             ON CONFLICT (session_id) DO UPDATE SET status = 'active', updated_at = now()",
            &[PgValue::Text(session_id.clone())],
        )
        .map_err(|e| ServiceError::internal(format!("insert session: {e:?}")))?;

        // Store doc prefix associations.
        for prefix in &req.doc_prefixes {
            database::execute(
                "INSERT INTO session_doc_prefixes (session_id, doc_prefix) VALUES ($1, $2) \
                 ON CONFLICT DO NOTHING",
                &[
                    PgValue::Text(session_id.clone()),
                    PgValue::Text(prefix.clone()),
                ],
            )
            .map_err(|e| ServiceError::internal(format!("insert doc prefix: {e:?}")))?;
        }

        // Load relevant documentation from blobstore.
        let context = build_context(&req.doc_prefixes, &req.task_description)?;

        let system_prompt = format!(
            "You are a code generation agent. You produce unified diffs (patches) for code changes.\n\n\
             ## Documentation Context\n{context}\n\n\
             ## Output Format\n\
             Produce a unified diff that can be applied with `patch -p1`. \
             Include file paths relative to the repository root. \
             Only output the diff, no explanations."
        );

        // Multi-turn LLM loop.
        let mut total_input: i32 = 0;
        let mut total_output: i32 = 0;
        let mut latest_diff;
        let mut turn = 0;

        // Build conversation history for multi-turn.
        let mut builder = CompletionBuilder::sonnet()
            .system(&system_prompt)
            .max_tokens(8192);

        // Turn 1: initial generation.
        let user_prompt = format!("## Task\n{}", req.task_description);
        builder = builder.user(&user_prompt);

        let resp = builder.complete().map_err(|e| {
            ServiceError::internal(format!("llm complete: {e:?}"))
        })?;

        let text = match resp.completion {
            wr_sdk::bindings::wruntime::llm::inference::Completion::Text(s) => s,
            wr_sdk::bindings::wruntime::llm::inference::Completion::ToolCalls(_) => {
                return Err(ServiceError::internal("unexpected tool_use response"));
            }
        };

        total_input += resp.usage.input_tokens as i32;
        total_output += resp.usage.output_tokens as i32;
        latest_diff = text.clone();
        turn += 1;

        store_turn(session_id, turn, &user_prompt, &text, &resp.usage)?;

        // Subsequent turns: review and refine.
        let mut prev_assistant = text;
        while turn < max_turns {
            let refine_prompt = "Review the diff you just produced. \
                Check for correctness, missing changes, and consistency with the documentation. \
                If improvements are needed, produce an updated unified diff. \
                If the diff is already correct, respond with exactly: LGTM";

            // Rebuild with full conversation history.
            let refine_builder = CompletionBuilder::sonnet()
                .system(&system_prompt)
                .max_tokens(8192)
                .user(&user_prompt)
                .assistant(&prev_assistant)
                .user(refine_prompt);

            let resp = refine_builder.complete().map_err(|e| {
                ServiceError::internal(format!("llm refine: {e:?}"))
            })?;

            let text = match resp.completion {
                wr_sdk::bindings::wruntime::llm::inference::Completion::Text(s) => s,
                wr_sdk::bindings::wruntime::llm::inference::Completion::ToolCalls(_) => {
                    break;
                }
            };

            total_input += resp.usage.input_tokens as i32;
            total_output += resp.usage.output_tokens as i32;
            turn += 1;

            store_turn(session_id, turn, refine_prompt, &text, &resp.usage)?;

            if text.trim() == "LGTM" {
                break;
            }

            latest_diff = text.clone();
            prev_assistant = text;
        }

        // Update session with final result.
        database::execute(
            "UPDATE sessions SET status = 'complete', latest_diff = $2, updated_at = now() \
             WHERE session_id = $1",
            &[
                PgValue::Text(session_id.clone()),
                PgValue::Text(latest_diff.clone()),
            ],
        )
        .map_err(|e| ServiceError::internal(format!("update session: {e:?}")))?;

        Ok(proto::RunTaskResponse {
            session_id: session_id.clone(),
            unified_diff: latest_diff,
            turns_used: turn,
            input_tokens: total_input,
            output_tokens: total_output,
            status: "complete".into(),
            message: format!("completed in {turn} turn(s)"),
        })
    }

    fn get_session(
        &self,
        req: proto::GetSessionRequest,
    ) -> Result<proto::GetSessionResponse, ServiceError> {
        let rows = database::query(
            "SELECT status, latest_diff FROM sessions WHERE session_id = $1",
            &[PgValue::Text(req.session_id.clone())],
        )
        .map_err(|e| ServiceError::internal(format!("query session: {e:?}")))?;

        if rows.is_empty() {
            return Err(ServiceError::not_found("session not found"));
        }

        let status = match &rows[0].columns[0].value {
            PgValue::Text(s) => s.clone(),
            _ => String::new(),
        };
        let latest_diff = match &rows[0].columns[1].value {
            PgValue::Text(s) => s.clone(),
            _ => String::new(),
        };

        let turn_rows = database::query(
            "SELECT turn_number, user_prompt, assistant_resp, input_tokens, output_tokens \
             FROM conversation_turns WHERE session_id = $1 ORDER BY turn_number",
            &[PgValue::Text(req.session_id.clone())],
        )
        .map_err(|e| ServiceError::internal(format!("query turns: {e:?}")))?;

        let turns = turn_rows
            .into_iter()
            .map(|row| {
                let turn_number = match &row.columns[0].value {
                    PgValue::Int4(n) => *n,
                    _ => 0,
                };
                let user_prompt = match &row.columns[1].value {
                    PgValue::Text(s) => s.clone(),
                    _ => String::new(),
                };
                let assistant_response = match &row.columns[2].value {
                    PgValue::Text(s) => s.clone(),
                    _ => String::new(),
                };
                let input_tokens = match &row.columns[3].value {
                    PgValue::Int4(n) => *n,
                    _ => 0,
                };
                let output_tokens = match &row.columns[4].value {
                    PgValue::Int4(n) => *n,
                    _ => 0,
                };
                proto::ConversationTurn {
                    turn_number,
                    user_prompt,
                    assistant_response,
                    input_tokens,
                    output_tokens,
                }
            })
            .collect::<Vec<_>>();

        Ok(proto::GetSessionResponse {
            session_id: req.session_id,
            turn_count: turns.len() as i32,
            latest_diff,
            status,
            turns,
        })
    }
}

// ── Context builder ──────────────────────────────────────────────────────────

fn build_context(
    doc_prefixes: &[String],
    task_description: &str,
) -> Result<String, ServiceError> {
    let task_lower = task_description.to_lowercase();
    let mut context = String::new();
    let mut total_size: usize = 0;

    for prefix in doc_prefixes {
        let manifest_key = format!("{prefix}/manifest.json");
        let manifest_data = store::get_object(BUCKET, &manifest_key)
            .map_err(|e| ServiceError::internal(format!("get manifest: {e:?}")))?;

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

        scored.sort_by(|a, b| b.0.cmp(&a.0));

        for (_, entry) in scored {
            if total_size + entry.size as usize > MAX_CONTEXT_BYTES {
                continue;
            }

            match store::get_object(BUCKET, &entry.key) {
                Ok(data) => {
                    if let Ok(text) = String::from_utf8(data.clone()) {
                        // Extract a short label from the key.
                        let label = entry
                            .key
                            .rsplit('/')
                            .next()
                            .unwrap_or(&entry.key);

                        context.push_str(&format!("<doc: {label}>\n{text}\n</doc>\n\n"));
                        total_size += data.len();
                    }
                }
                Err(_) => continue,
            }
        }
    }

    Ok(context)
}

// ── DB helpers ───────────────────────────────────────────────────────────────

fn store_turn(
    session_id: &str,
    turn_number: i32,
    user_prompt: &str,
    assistant_resp: &str,
    usage: &wr_sdk::bindings::wruntime::llm::inference::TokenUsage,
) -> Result<(), ServiceError> {
    database::execute(
        "INSERT INTO conversation_turns \
         (session_id, turn_number, user_prompt, assistant_resp, input_tokens, output_tokens) \
         VALUES ($1, $2, $3, $4, $5, $6)",
        &[
            PgValue::Text(session_id.into()),
            PgValue::Int4(turn_number),
            PgValue::Text(user_prompt.into()),
            PgValue::Text(assistant_resp.into()),
            PgValue::Int4(usage.input_tokens as i32),
            PgValue::Int4(usage.output_tokens as i32),
        ],
    )
    .map_err(|e| ServiceError::internal(format!("insert turn: {e:?}")))?;
    Ok(())
}
