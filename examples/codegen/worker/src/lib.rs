#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/codegen.rs"));
}

#[allow(dead_code, unused_imports)]
mod bindings;

use proto::{AgentServiceClient, CollectorServiceClient, CoordinatorServiceClient};
use wr_sdk::bindings::wasi::http::types::{IncomingRequest, ResponseOutparam};
use wr_sdk::io::{read_body, send_response};
use wr_sdk::tracing;
use wr_sdk::ServiceError;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let path = request.path_with_query().unwrap_or_default();
        let body = read_body(request.consume().unwrap());
        let (status, resp) = proto::worker_service_router(&Component, &path, &body);
        send_response(response_out, status, resp);
    }
}

impl proto::WorkerService for Component {
    fn process_task(
        &self,
        req: proto::ProcessTaskRequest,
    ) -> Result<proto::ProcessTaskResponse, ServiceError> {
        let task_id = &req.task_id;
        let span = tracing::start("worker.process_task", &[("task.id", task_id.as_str())]);

        let coordinator = CoordinatorServiceClient::new("codegen.coordinator");
        let collector = CollectorServiceClient::new("codegen.collector");
        let agent = AgentServiceClient::new("codegen.agent");

        // Phase 1: Collect docs + source code.
        let _ = coordinator.update_task_status(proto::UpdateTaskStatusRequest {
            task_id: task_id.clone(),
            status: "collecting".into(),
        });

        let collect_span = tracing::start("worker.collect_docs", &[("task.id", task_id.as_str())]);

        let mut sources: Vec<proto::DocSourceSpec> = req
            .doc_sources
            .into_iter()
            .map(|s| proto::DocSourceSpec {
                source_type: s.source_type,
                owner: s.owner,
                repo: s.repo,
                ref_or_ver: s.ref_or_ver,
            })
            .collect();

        // Add the repo itself as a github_tarball source.
        if !req.repo_url.is_empty() {
            if let Some((owner, repo)) = parse_github_url(&req.repo_url) {
                sources.push(proto::DocSourceSpec {
                    source_type: "github_tarball".into(),
                    owner,
                    repo,
                    ref_or_ver: req.r#ref.clone(),
                });
            }
        }

        // The collector proto uses its own DocSource type.
        let collector_sources = sources
            .iter()
            .map(|s| proto::DocSource {
                source_type: s.source_type.clone(),
                owner: s.owner.clone(),
                repo: s.repo.clone(),
                ref_or_ver: s.ref_or_ver.clone(),
            })
            .collect();

        let fetch_resp = match collector.fetch_docs(proto::FetchDocsRequest {
            sources: collector_sources,
        }) {
            Ok(r) => r,
            Err(e) => {
                tracing::set_error(&collect_span, &format!("collector: {e}"));
                fail_task(&coordinator, task_id, &format!("collector: {e}"));
                drop(collect_span);
                drop(span);
                return Err(ServiceError::internal(format!("collector: {e}")));
            }
        };

        tracing::set_attribute(
            &collect_span,
            "collector.sources_fetched",
            &fetch_resp.sources_fetched.to_string(),
        );
        drop(collect_span);

        wr_sdk::log::log(&format!(
            "collected {} source(s), {} bytes",
            fetch_resp.sources_fetched, fetch_resp.total_bytes
        ));

        // Phase 2: Run agent.
        let _ = coordinator.update_task_status(proto::UpdateTaskStatusRequest {
            task_id: task_id.clone(),
            status: "generating".into(),
        });

        let agent_span = tracing::start(
            "worker.run_agent",
            &[
                ("task.id", task_id.as_str()),
                ("agent.max_turns", &req.max_agent_turns.to_string()),
            ],
        );

        let agent_resp = match agent.run_task(proto::RunTaskRequest {
            session_id: req.session_id,
            task_description: req.task_description,
            doc_prefixes: fetch_resp.doc_prefixes,
            max_turns: req.max_agent_turns,
        }) {
            Ok(r) => r,
            Err(e) => {
                tracing::set_error(&agent_span, &format!("agent: {e}"));
                fail_task(&coordinator, task_id, &format!("agent: {e}"));
                drop(agent_span);
                drop(span);
                return Err(ServiceError::internal(format!("agent: {e}")));
            }
        };

        tracing::set_attribute(
            &agent_span,
            "agent.turns_used",
            &agent_resp.turns_used.to_string(),
        );
        drop(agent_span);

        // Store result via coordinator.
        let _ = coordinator.complete_task(proto::CompleteTaskRequest {
            task_id: task_id.clone(),
            status: "complete".into(),
            unified_diff: agent_resp.unified_diff.clone(),
            message: agent_resp.message.clone(),
            agent_turns: agent_resp.turns_used,
            total_input_tokens: agent_resp.input_tokens,
            total_output_tokens: agent_resp.output_tokens,
        });

        tracing::set_attribute(&span, "task.status", "complete");
        drop(span);
        wr_sdk::log::log(&format!("task {} complete", task_id));

        Ok(proto::ProcessTaskResponse {
            unified_diff: agent_resp.unified_diff,
            message: agent_resp.message,
            agent_turns: agent_resp.turns_used,
            total_input_tokens: agent_resp.input_tokens,
            total_output_tokens: agent_resp.output_tokens,
        })
    }
}

fn fail_task(coordinator: &CoordinatorServiceClient, task_id: &str, message: &str) {
    wr_sdk::log::log(&format!("task {} failed: {}", task_id, message));
    let _ = coordinator.complete_task(proto::CompleteTaskRequest {
        task_id: task_id.into(),
        status: "error".into(),
        message: message.into(),
        ..Default::default()
    });
}

fn parse_github_url(url: &str) -> Option<(String, String)> {
    let path = url
        .strip_prefix("https://github.com/")
        .or_else(|| url.strip_prefix("http://github.com/"))?;
    let parts: Vec<&str> = path.trim_end_matches('/').splitn(3, '/').collect();
    if parts.len() >= 2 && !parts[0].is_empty() && !parts[1].is_empty() {
        let repo = parts[1].strip_suffix(".git").unwrap_or(parts[1]);
        Some((parts[0].to_string(), repo.to_string()))
    } else {
        None
    }
}
