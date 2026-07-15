#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/codegen.rs"));
}

#[allow(dead_code, unused_imports)]
mod bindings {
    wit_bindgen::generate!({
        path: "wit",
        world: "coordinator",
        generate_all,
    });
}

use proto::CoordinatorService;
use serde::{Deserialize, Serialize};
use wr_sdk::prelude::*;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let method = request.method();
        let path = request.path_with_query().unwrap_or_default();
        let body = read_body(request.consume().unwrap());

        let response = if path.starts_with("/tasks") {
            let (status, body) = handle_external(&method, &path, &body);
            ServiceResponse::json(status, body)
        } else {
            proto::coordinator_service_router(&Component, &path, &body)
        };
        send_service_response(response_out, response);
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
    max_agent_turns: u32,
}

fn default_max_turns() -> u32 {
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
    agent_turns: u32,
    total_input_tokens: u32,
    total_output_tokens: u32,
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
    json_body(status, body)
}

fn handle_external(method: &Method, path: &str, body: &[u8]) -> (u16, Vec<u8>) {
    match (method, path) {
        (Method::Post, "/tasks") => handle_create_task_json(body),
        (Method::Get, p) if p.starts_with("/tasks/") => {
            let task_id = &p[7..];
            handle_get_task_json(task_id)
        }
        _ => json_response(
            404,
            &ErrorJson {
                error: "not found".into(),
            },
        ),
    }
}

fn handle_create_task_json(body: &[u8]) -> (u16, Vec<u8>) {
    let req: CreateTaskJson = match serde_json::from_slice(body) {
        Ok(r) => r,
        Err(e) => {
            return json_response(
                400,
                &ErrorJson {
                    error: format!("invalid JSON: {e}"),
                },
            )
        }
    };

    let doc_sources = match req
        .doc_sources
        .into_iter()
        .map(|source| {
            parse_doc_source_type(&source.source_type).map(|source_type| proto::DocSourceSpec {
                source_type: source_type as i32,
                owner: source.owner,
                repo: source.repo,
                ref_or_ver: source.ref_or_ver,
            })
        })
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(sources) => sources,
        Err(error) => return json_response(400, &ErrorJson { error }),
    };

    let proto_req = proto::CreateTaskRequest {
        repo_url: req.repo_url,
        r#ref: req.git_ref,
        doc_sources,
        task_description: req.task_description,
        max_agent_turns: req.max_agent_turns,
    };

    match Component.create_task(proto_req) {
        Ok(resp) => json_response(
            201,
            &CreateResponseJson {
                task_id: resp.task_id,
                status: task_status_name(resp.status).to_string(),
            },
        ),
        Err(e) => json_response(e.status, &ErrorJson { error: e.message }),
    }
}

fn handle_get_task_json(task_id: &str) -> (u16, Vec<u8>) {
    match Component.get_task_inner(task_id) {
        Ok(resp) => json_response(
            200,
            &TaskResponseJson {
                task_id: resp.task_id,
                status: task_status_name(resp.status).to_string(),
                unified_diff: resp.unified_diff,
                message: resp.message,
                agent_turns: resp.agent_turns,
                total_input_tokens: resp.total_input_tokens,
                total_output_tokens: resp.total_output_tokens,
                created_at: resp.created_at,
                updated_at: resp.updated_at,
            },
        ),
        Err(e) => json_response(e.status, &ErrorJson { error: e.message }),
    }
}

// ── Proto service implementation ─────────────────────────────────────────────

impl proto::CoordinatorService for Component {
    fn create_task(
        &self,
        req: proto::CreateTaskRequest,
    ) -> Result<proto::CreateTaskResponse, ServiceError> {
        let task_id = generate_id("task");
        let session_id = generate_id("sess");

        for source in &req.doc_sources {
            parse_doc_source_type(doc_source_type_name(source.source_type))
                .map_err(ServiceError::bad_request)?;
        }

        let doc_sources_json = serde_json::to_string(
            &req.doc_sources
                .iter()
                .map(|s| DocSourceJson {
                    source_type: doc_source_type_name(s.source_type).to_string(),
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

        database::execute(
            "INSERT INTO tasks (task_id, repo_url, \"ref\", doc_sources, task_description, \
             max_agent_turns, status, session_id) \
             VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7)",
            &[
                PgValue::Text(task_id.clone()),
                PgValue::Text(req.repo_url.clone()),
                PgValue::Text(req.r#ref.clone()),
                PgValue::Jsonb(doc_sources_json),
                PgValue::Text(req.task_description.clone()),
                PgValue::Int4(to_db_i32(max_turns, "max_agent_turns")?),
                PgValue::Text(session_id.clone()),
            ],
        )?;

        // Submit a job to the engine's worker queue.
        let worker = proto::WorkerServiceClient::new("codegen.worker", "1.0.0");
        if let Err(e) = worker.process_task(proto::ProcessTaskRequest {
            task_id: task_id.clone(),
            session_id,
            repo_url: req.repo_url,
            r#ref: req.r#ref,
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
            max_agent_turns: max_turns,
        }) {
            wr_sdk::log::log(&format!("failed to submit worker job: {e}"));
        }

        Ok(proto::CreateTaskResponse {
            task_id,
            status: proto::TaskStatus::Pending as i32,
        })
    }

    fn get_task(&self, req: proto::GetTaskRequest) -> Result<proto::GetTaskResponse, ServiceError> {
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
            &[
                PgValue::Int4(to_db_i32(limit, "limit")?),
                PgValue::Int4(to_db_i32(offset, "offset")?),
            ],
        )?;

        let tasks = rows.into_iter().map(row_to_task_response).collect();
        Ok(proto::ListTasksResponse { tasks })
    }

    fn claim_task(
        &self,
        _req: proto::ClaimTaskRequest,
    ) -> Result<proto::ClaimTaskResponse, ServiceError> {
        // Atomically claim one pending task.
        let rows = database::query(
            "UPDATE tasks SET status = 'claimed', updated_at = now() \
             WHERE task_id = ( \
               SELECT task_id FROM tasks WHERE status = 'pending' \
               ORDER BY created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED \
             ) RETURNING task_id, session_id, repo_url, \"ref\", \
               doc_sources, task_description, max_agent_turns",
            &[],
        )?;

        if rows.is_empty() {
            return Ok(proto::ClaimTaskResponse {
                found: false,
                ..Default::default()
            });
        }

        let row = &rows[0];
        let text = |i: usize| -> String {
            match &row.columns[i].value {
                PgValue::Text(s) => s.clone(),
                PgValue::Jsonb(s) => s.clone(),
                _ => String::new(),
            }
        };
        let count = |i: usize| -> u32 {
            match &row.columns[i].value {
                PgValue::Int4(n) => u32::try_from(*n).unwrap_or_default(),
                _ => 0,
            }
        };

        let doc_sources_json = text(4);
        let doc_source_specs: Vec<DocSourceJson> =
            serde_json::from_str(&doc_sources_json).unwrap_or_default();
        let doc_sources = doc_source_specs
            .into_iter()
            .map(|source| {
                let source_type = parse_doc_source_type(&source.source_type)
                    .map_err(ServiceError::bad_request)?;
                Ok(proto::DocSourceSpec {
                    source_type: source_type as i32,
                    owner: source.owner,
                    repo: source.repo,
                    ref_or_ver: source.ref_or_ver,
                })
            })
            .collect::<Result<Vec<_>, ServiceError>>()?;

        Ok(proto::ClaimTaskResponse {
            found: true,
            task_id: text(0),
            session_id: text(1),
            repo_url: text(2),
            r#ref: text(3),
            doc_sources,
            task_description: text(5),
            max_agent_turns: count(6),
        })
    }

    fn update_task_status(
        &self,
        req: proto::UpdateTaskStatusRequest,
    ) -> Result<proto::UpdateTaskStatusResponse, ServiceError> {
        let status = validated_task_status(req.status)?;
        database::execute(
            "UPDATE tasks SET status = $2, updated_at = now() WHERE task_id = $1",
            &[
                PgValue::Text(req.task_id),
                PgValue::Text(task_status_name(status as i32).to_string()),
            ],
        )?;
        Ok(proto::UpdateTaskStatusResponse {})
    }

    fn complete_task(
        &self,
        req: proto::CompleteTaskRequest,
    ) -> Result<proto::CompleteTaskResponse, ServiceError> {
        let status = validated_task_status(req.status)?;
        if !matches!(
            status,
            proto::TaskStatus::Complete | proto::TaskStatus::Error
        ) {
            return Err(ServiceError::bad_request(
                "complete_task status must be complete or error",
            ));
        }
        database::execute(
            "UPDATE tasks SET status = $2, unified_diff = $3, message = $4, \
             agent_turns = $5, total_input_tokens = $6, total_output_tokens = $7, \
             updated_at = now() WHERE task_id = $1",
            &[
                PgValue::Text(req.task_id),
                PgValue::Text(task_status_name(status as i32).to_string()),
                PgValue::Text(req.unified_diff),
                PgValue::Text(req.message),
                PgValue::Int4(to_db_i32(req.agent_turns, "agent_turns")?),
                PgValue::Int4(to_db_i32(req.total_input_tokens, "total_input_tokens")?),
                PgValue::Int4(to_db_i32(req.total_output_tokens, "total_output_tokens")?),
            ],
        )?;
        Ok(proto::CompleteTaskResponse {})
    }
}

impl Component {
    fn get_task_inner(&self, task_id: &str) -> Result<proto::GetTaskResponse, ServiceError> {
        let span = tracing::start("coordinator.get_task", &[("task.id", task_id)]);
        let rows = database::query(
            "SELECT task_id, status, unified_diff, message, agent_turns, \
             total_input_tokens, total_output_tokens, \
             created_at::text, updated_at::text \
             FROM tasks WHERE task_id = $1",
            &[PgValue::Text(task_id.into())],
        )?;

        if rows.is_empty() {
            tracing::set_error(&span, "task not found");
            return Err(ServiceError::not_found("task not found"));
        }

        drop(span);
        Ok(row_to_task_response(rows.into_iter().next().unwrap()))
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn parse_doc_source_type(value: &str) -> Result<proto::DocSourceType, String> {
    match value {
        "github_tarball" => Ok(proto::DocSourceType::GithubTarball),
        "docs_rs" => Ok(proto::DocSourceType::DocsRs),
        "crates_io" => Ok(proto::DocSourceType::CratesIo),
        other => Err(format!("unsupported source_type: {other}")),
    }
}

fn doc_source_type_name(value: i32) -> &'static str {
    match proto::DocSourceType::try_from(value) {
        Ok(proto::DocSourceType::GithubTarball) => "github_tarball",
        Ok(proto::DocSourceType::DocsRs) => "docs_rs",
        Ok(proto::DocSourceType::CratesIo) => "crates_io",
        _ => "unspecified",
    }
}

fn task_status_from_name(value: &str) -> proto::TaskStatus {
    match value {
        "pending" => proto::TaskStatus::Pending,
        "claimed" => proto::TaskStatus::Claimed,
        "collecting" => proto::TaskStatus::Collecting,
        "generating" => proto::TaskStatus::Generating,
        "complete" => proto::TaskStatus::Complete,
        "error" => proto::TaskStatus::Error,
        _ => proto::TaskStatus::Unspecified,
    }
}

fn task_status_name(value: i32) -> &'static str {
    match proto::TaskStatus::try_from(value) {
        Ok(proto::TaskStatus::Pending) => "pending",
        Ok(proto::TaskStatus::Claimed) => "claimed",
        Ok(proto::TaskStatus::Collecting) => "collecting",
        Ok(proto::TaskStatus::Generating) => "generating",
        Ok(proto::TaskStatus::Complete) => "complete",
        Ok(proto::TaskStatus::Error) => "error",
        _ => "unspecified",
    }
}

fn validated_task_status(value: i32) -> Result<proto::TaskStatus, ServiceError> {
    let status = proto::TaskStatus::try_from(value)
        .map_err(|_| ServiceError::bad_request("unknown task status"))?;
    if status == proto::TaskStatus::Unspecified {
        return Err(ServiceError::bad_request("task status is required"));
    }
    Ok(status)
}

fn to_db_i32(value: u32, field: &str) -> Result<i32, ServiceError> {
    i32::try_from(value)
        .map_err(|_| ServiceError::bad_request(format!("{field} exceeds database range")))
}

fn row_to_task_response(row: database::Row) -> proto::GetTaskResponse {
    let text = |i: usize| -> String {
        match &row.columns[i].value {
            PgValue::Text(s) => s.clone(),
            _ => String::new(),
        }
    };
    let count = |i: usize| -> u32 {
        match &row.columns[i].value {
            PgValue::Int4(n) => u32::try_from(*n).unwrap_or_default(),
            _ => 0,
        }
    };

    proto::GetTaskResponse {
        task_id: text(0),
        status: task_status_from_name(&text(1)) as i32,
        unified_diff: text(2),
        message: text(3),
        agent_turns: count(4),
        total_input_tokens: count(5),
        total_output_tokens: count(6),
        created_at: text(7),
        updated_at: text(8),
    }
}

fn generate_id(prefix: &str) -> String {
    use bindings::wasi::random::random::get_random_u64;
    let r = get_random_u64();
    format!("{prefix}-{r:016x}")
}
