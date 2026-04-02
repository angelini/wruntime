#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/codegen.rs"));
}

#[allow(dead_code, unused_imports)]
mod bindings;

use proto::{AgentServiceClient, CollectorServiceClient};
use serde::{Deserialize, Serialize};
use wr_sdk::bindings::wasi::http::types::{IncomingRequest, Method, ResponseOutparam};
use wr_sdk::bindings::wruntime::db::database::{self, PgValue};
use wr_sdk::io::{read_body, send_response};
use wr_sdk::ServiceError;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let method = request.method();
        let path = request.path_with_query().unwrap_or_default();
        let body = read_body(request.consume().unwrap());

        // External ingress routes (JSON) vs internal RPC routes (protobuf).
        let (status, resp) = if path.starts_with("/tasks") {
            handle_external(&method, &path, &body)
        } else {
            proto::coordinator_service_router(&Component, &path, &body)
        };
        send_response(response_out, status, resp);
    }
}

// ── External ingress (JSON API) ──────────────────────────────────────────────

#[derive(Deserialize)]
struct CreateTaskJson {
    repo_url: String,
    #[serde(rename = "ref", default)]
    git_ref: String,
    #[serde(default)]
    doc_sources: Vec<DocSourceJson>,
    task_description: String,
    #[serde(default = "default_max_turns")]
    max_agent_turns: i32,
}

fn default_max_turns() -> i32 {
    3
}

#[derive(Deserialize, Serialize, Clone)]
struct DocSourceJson {
    source_type: String,
    #[serde(default)]
    owner: String,
    #[serde(default)]
    repo: String,
    #[serde(default)]
    ref_or_ver: String,
}

#[derive(Serialize)]
struct TaskResponseJson {
    task_id: String,
    status: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    unified_diff: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    message: String,
    agent_turns: i32,
    total_input_tokens: i32,
    total_output_tokens: i32,
    created_at: String,
    updated_at: String,
}

#[derive(Serialize)]
struct CreateResponseJson {
    task_id: String,
    status: String,
}

#[derive(Serialize)]
struct ErrorJson {
    error: String,
}

fn json_response(status: u16, body: &impl Serialize) -> (u16, Vec<u8>) {
    (status, serde_json::to_vec(body).unwrap_or_default())
}

fn handle_external(method: &Method, path: &str, body: &[u8]) -> (u16, Vec<u8>) {
    match (method, path) {
        (Method::Post, "/tasks") => handle_create_task_json(body),
        (Method::Get, p) if p.starts_with("/tasks/") => {
            let task_id = &p[7..]; // strip "/tasks/"
            handle_get_task_json(task_id)
        }
        _ => json_response(404, &ErrorJson {
            error: "not found".into(),
        }),
    }
}

fn handle_create_task_json(body: &[u8]) -> (u16, Vec<u8>) {
    let req: CreateTaskJson = match serde_json::from_slice(body) {
        Ok(r) => r,
        Err(e) => {
            return json_response(400, &ErrorJson {
                error: format!("invalid JSON: {e}"),
            })
        }
    };

    let proto_req = proto::CreateTaskRequest {
        repo_url: req.repo_url,
        r#ref: req.git_ref,
        doc_sources: req
            .doc_sources
            .into_iter()
            .map(|s| proto::DocSourceSpec {
                source_type: s.source_type,
                owner: s.owner,
                repo: s.repo,
                ref_or_ver: s.ref_or_ver,
            })
            .collect(),
        task_description: req.task_description,
        max_agent_turns: req.max_agent_turns,
    };

    match Component.create_task_inner(proto_req) {
        Ok(resp) => json_response(201, &CreateResponseJson {
            task_id: resp.task_id,
            status: resp.status,
        }),
        Err(e) => json_response(e.status, &ErrorJson {
            error: e.message,
        }),
    }
}

fn handle_get_task_json(task_id: &str) -> (u16, Vec<u8>) {
    match Component.get_task_inner(task_id) {
        Ok(resp) => json_response(200, &TaskResponseJson {
            task_id: resp.task_id,
            status: resp.status,
            unified_diff: resp.unified_diff,
            message: resp.message,
            agent_turns: resp.agent_turns,
            total_input_tokens: resp.total_input_tokens,
            total_output_tokens: resp.total_output_tokens,
            created_at: resp.created_at,
            updated_at: resp.updated_at,
        }),
        Err(e) => json_response(e.status, &ErrorJson {
            error: e.message,
        }),
    }
}

// ── Proto service implementation ─────────────────────────────────────────────

impl proto::CoordinatorService for Component {
    fn create_task(
        &self,
        req: proto::CreateTaskRequest,
    ) -> Result<proto::CreateTaskResponse, ServiceError> {
        self.create_task_inner(req)
    }

    fn get_task(
        &self,
        req: proto::GetTaskRequest,
    ) -> Result<proto::GetTaskResponse, ServiceError> {
        self.get_task_inner(&req.task_id)
    }

    fn list_tasks(
        &self,
        req: proto::ListTasksRequest,
    ) -> Result<proto::ListTasksResponse, ServiceError> {
        let limit = if req.limit > 0 { req.limit } else { 50 };
        let offset = if req.offset > 0 { req.offset } else { 0 };

        let rows = database::query(
            "SELECT task_id, status, unified_diff, message, agent_turns, \
             total_input_tokens, total_output_tokens, \
             created_at::text, updated_at::text \
             FROM tasks ORDER BY created_at DESC LIMIT $1 OFFSET $2",
            &[PgValue::Int4(limit), PgValue::Int4(offset)],
        )
        .map_err(|e| {
            wr_sdk::log::log(&format!("query tasks failed: {e:?}"));
            ServiceError::internal(format!("query tasks: {e:?}"))
        })?;

        let tasks = rows.into_iter().map(row_to_task_response).collect();
        Ok(proto::ListTasksResponse { tasks })
    }
}

impl Component {
    fn create_task_inner(
        &self,
        req: proto::CreateTaskRequest,
    ) -> Result<proto::CreateTaskResponse, ServiceError> {
        // Generate task ID from random bytes.
        let task_id = generate_id("task");
        let session_id = generate_id("sess");

        let doc_sources_json = serde_json::to_string(
            &req.doc_sources
                .iter()
                .map(|s| DocSourceJson {
                    source_type: s.source_type.clone(),
                    owner: s.owner.clone(),
                    repo: s.repo.clone(),
                    ref_or_ver: s.ref_or_ver.clone(),
                })
                .collect::<Vec<_>>(),
        )
        .unwrap_or_else(|_| "[]".into());

        let max_turns = if req.max_agent_turns > 0 {
            req.max_agent_turns
        } else {
            3
        };

        // Insert task record.
        database::execute(
            "INSERT INTO tasks (task_id, repo_url, \"ref\", doc_sources, task_description, \
             max_agent_turns, status, session_id) \
             VALUES ($1, $2, $3, $4::jsonb, $5, $6, 'pending', $7)",
            &[
                PgValue::Text(task_id.clone()),
                PgValue::Text(req.repo_url.clone()),
                PgValue::Text(req.r#ref.clone()),
                PgValue::Text(doc_sources_json),
                PgValue::Text(req.task_description.clone()),
                PgValue::Int4(max_turns),
                PgValue::Text(session_id.clone()),
            ],
        )
        .map_err(|e| {
            wr_sdk::log::log(&format!("insert task failed: {e:?}"));
            ServiceError::internal(format!("insert task: {e:?}"))
        })?;

        // Phase 1: Collect docs.
        update_task_status(&task_id, "collecting")?;

        let collector = CollectorServiceClient::new("codegen.collector");
        let fetch_resp = collector
            .fetch_docs(proto::FetchDocsRequest {
                sources: req
                    .doc_sources
                    .into_iter()
                    .map(|s| proto::DocSource {
                        source_type: s.source_type,
                        owner: s.owner,
                        repo: s.repo,
                        ref_or_ver: s.ref_or_ver,
                    })
                    .collect(),
            })
            .map_err(|e| {
                let _ = update_task_status_with_message(&task_id, "error", &format!("collector: {e}"));
                ServiceError::internal(format!("collector failed: {e}"))
            })?;

        wr_sdk::log::log(&format!(
            "collected {} source(s), {} bytes",
            fetch_resp.sources_fetched, fetch_resp.total_bytes
        ));

        // Phase 2: Run agent.
        update_task_status(&task_id, "generating")?;

        let agent = AgentServiceClient::new("codegen.agent");
        let agent_resp = agent
            .run_task(proto::RunTaskRequest {
                session_id: session_id.clone(),
                task_description: req.task_description,
                doc_prefixes: fetch_resp.doc_prefixes,
                max_turns,
            })
            .map_err(|e| {
                let _ = update_task_status_with_message(&task_id, "error", &format!("agent: {e}"));
                ServiceError::internal(format!("agent failed: {e}"))
            })?;

        // Store result.
        database::execute(
            "UPDATE tasks SET status = 'complete', unified_diff = $2, message = $3, \
             agent_turns = $4, total_input_tokens = $5, total_output_tokens = $6, \
             updated_at = now() WHERE task_id = $1",
            &[
                PgValue::Text(task_id.clone()),
                PgValue::Text(agent_resp.unified_diff.clone()),
                PgValue::Text(agent_resp.message.clone()),
                PgValue::Int4(agent_resp.turns_used),
                PgValue::Int4(agent_resp.input_tokens),
                PgValue::Int4(agent_resp.output_tokens),
            ],
        )
        .map_err(|e| {
            wr_sdk::log::log(&format!("update task failed: {e:?}"));
            ServiceError::internal(format!("update task: {e:?}"))
        })?;

        Ok(proto::CreateTaskResponse {
            task_id,
            status: "complete".into(),
        })
    }

    fn get_task_inner(&self, task_id: &str) -> Result<proto::GetTaskResponse, ServiceError> {
        let rows = database::query(
            "SELECT task_id, status, unified_diff, message, agent_turns, \
             total_input_tokens, total_output_tokens, \
             created_at::text, updated_at::text \
             FROM tasks WHERE task_id = $1",
            &[PgValue::Text(task_id.into())],
        )
        .map_err(|e| {
            wr_sdk::log::log(&format!("query task failed: {e:?}"));
            ServiceError::internal(format!("query task: {e:?}"))
        })?;

        if rows.is_empty() {
            return Err(ServiceError::not_found("task not found"));
        }

        Ok(row_to_task_response(rows.into_iter().next().unwrap()))
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn row_to_task_response(row: database::Row) -> proto::GetTaskResponse {
    let text = |i: usize| -> String {
        match &row.columns[i].value {
            PgValue::Text(s) => s.clone(),
            _ => String::new(),
        }
    };
    let int = |i: usize| -> i32 {
        match &row.columns[i].value {
            PgValue::Int4(n) => *n,
            _ => 0,
        }
    };

    proto::GetTaskResponse {
        task_id: text(0),
        status: text(1),
        unified_diff: text(2),
        message: text(3),
        agent_turns: int(4),
        total_input_tokens: int(5),
        total_output_tokens: int(6),
        created_at: text(7),
        updated_at: text(8),
    }
}

fn generate_id(prefix: &str) -> String {
    use bindings::wasi::random::random::get_random_u64;
    let r = get_random_u64();
    format!("{prefix}-{r:016x}")
}

fn update_task_status(task_id: &str, status: &str) -> Result<(), ServiceError> {
    database::execute(
        "UPDATE tasks SET status = $2, updated_at = now() WHERE task_id = $1",
        &[
            PgValue::Text(task_id.into()),
            PgValue::Text(status.into()),
        ],
    )
    .map_err(|e| {
        wr_sdk::log::log(&format!("update status failed: {e:?}"));
        ServiceError::internal(format!("update status: {e:?}"))
    })?;
    Ok(())
}

fn update_task_status_with_message(
    task_id: &str,
    status: &str,
    message: &str,
) -> Result<(), ServiceError> {
    database::execute(
        "UPDATE tasks SET status = $2, message = $3, updated_at = now() WHERE task_id = $1",
        &[
            PgValue::Text(task_id.into()),
            PgValue::Text(status.into()),
            PgValue::Text(message.into()),
        ],
    )
    .map_err(|e| {
        wr_sdk::log::log(&format!("update status failed: {e:?}"));
        ServiceError::internal(format!("update status: {e:?}"))
    })?;
    Ok(())
}
