use std::time::Duration;

use bytes::Bytes;
use deadpool_postgres::Pool;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::{TokioExecutor, TokioIo};
use prost::Message;
use tokio::net::TcpStream;
use tracing::{debug, info, warn};
use wr_common::wruntime::{SubmitJobRequest, SubmitJobResponse};

use crate::db;

/// Submissions must return quickly (the engine only enqueues the job and returns
/// a job_id); the job's own `timeout_secs` is enforced engine-side and is
/// independent of this. Kept below any sane `scheduler_lease_secs` so a hung
/// proxy connection cannot silently outlive the lease.
const SUBMIT_TIMEOUT: Duration = Duration::from_secs(10);
const SUBMIT_JOB_PATH: &str = "/wruntime.WorkerService/SubmitJob";

/// Background task. Every `interval`: claim due schedules (short txn), submit each
/// through the local proxy (no txn), then finalize each with a fenced update.
/// Delivery is at-least-once — jobs must be idempotent.
#[allow(clippy::too_many_arguments)]
pub async fn run_scheduler(
    pool: Pool,
    manager_id: String,
    interval: Duration,
    lease_secs: f64,
    retry_base_secs: f64,
    retry_cap_secs: f64,
    local_proxy_address: String,
) {
    let mut tick = tokio::time::interval(interval);
    loop {
        tick.tick().await;
        if let Err(e) = evaluate_schedules(
            &pool,
            &manager_id,
            lease_secs,
            retry_base_secs,
            retry_cap_secs,
            &local_proxy_address,
        )
        .await
        {
            warn!(error = %e, "scheduler tick failed");
        }
    }
}

async fn evaluate_schedules(
    pool: &Pool,
    manager_id: &str,
    lease_secs: f64,
    retry_base_secs: f64,
    retry_cap_secs: f64,
    local_proxy_address: &str,
) -> Result<(), anyhow::Error> {
    // ── Phase 1: claim (short txn) ───────────────────────────────────────────
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    let due = db::claim_due_schedules(&txn, manager_id, lease_secs)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    txn.commit().await?;

    if due.is_empty() {
        return Ok(());
    }

    // ── Phase 2 + 3: submit (no txn) then fenced finalize (own connection) ────
    for schedule in &due {
        let Some(claim_id) = schedule.claim_id.as_deref() else {
            warn!(schedule_id = %schedule.schedule_id, "claimed schedule missing claim_id, skipping");
            continue;
        };

        match submit_job(local_proxy_address, schedule).await {
            Ok(body) => {
                let job_id = SubmitJobResponse::decode(body.as_ref())
                    .map(|r| r.job_id)
                    .unwrap_or_default();
                info!(
                    schedule_id = %schedule.schedule_id,
                    job_id,
                    "scheduled job submitted"
                );
                let n = db::mark_schedule_succeeded(pool, &schedule.schedule_id, claim_id)
                    .await
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                if n == 0 {
                    debug!(schedule_id = %schedule.schedule_id, "dropped stale success finalize (reclaimed)");
                }
            }
            Err(e) => {
                let backoff = (retry_base_secs * 2f64.powi(schedule.consecutive_failures))
                    .min(retry_cap_secs);
                warn!(
                    schedule_id = %schedule.schedule_id,
                    error = %e,
                    backoff_secs = backoff,
                    "scheduled job submission failed, will retry"
                );
                let n = db::mark_schedule_failed(
                    pool,
                    &schedule.schedule_id,
                    claim_id,
                    &e.to_string(),
                    backoff,
                )
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
                if n == 0 {
                    debug!(schedule_id = %schedule.schedule_id, "dropped stale failure finalize (reclaimed)");
                }
            }
        }
    }

    Ok(())
}

/// Submit a scheduled job through the local proxy loopback.
///
/// Uses `POST /wruntime.WorkerService/SubmitJob` with
/// `x-wr-destination: http://{ns}.{module}/wruntime.WorkerService/SubmitJob`.
/// Returns the response body on HTTP 2xx; `Err` on connect/timeout/non-2xx.
pub async fn submit_job(
    local_proxy_address: &str,
    schedule: &db::ScheduleRow,
) -> Result<Bytes, anyhow::Error> {
    let req = SubmitJobRequest {
        worker_namespace: schedule.worker_namespace.clone(),
        worker_name: schedule.worker_name.clone(),
        worker_version: schedule.worker_version.clone(),
        job_type: schedule.job_type.clone(),
        payload: schedule.payload.clone(),
        timeout_secs: schedule.timeout_secs,
        max_attempts: schedule.max_attempts,
    };
    let body = req.encode_to_vec();

    // Proxy loopback speaks HTTP/2 prior knowledge (h2c) — matches wr-cli invoke
    // and the proxy's serve_connection. Strip scheme for the raw TCP connect.
    let addr = local_proxy_address
        .trim_start_matches("http://")
        .trim_start_matches("https://");

    let destination = format!(
        "http://{}.{}{}",
        schedule.worker_namespace, schedule.worker_name, SUBMIT_JOB_PATH
    );

    let do_submit = async {
        let stream = TcpStream::connect(addr).await?;
        let io = TokioIo::new(stream);
        let (mut sender, conn) =
            hyper::client::conn::http2::handshake(TokioExecutor::new(), io).await?;
        tokio::spawn(conn);

        let http_req = http::Request::builder()
            .method("POST")
            .uri(SUBMIT_JOB_PATH)
            .header("content-type", "application/x-protobuf")
            .header("x-wr-destination", &destination)
            .header("x-wr-version", &schedule.worker_version)
            .header("x-wr-source", "wr-manager-scheduler")
            .body(Full::new(Bytes::from(body)))?;

        let resp = sender.send_request(http_req).await?;
        let status = resp.status();
        let resp_body = resp.into_body().collect().await?.to_bytes();
        if !status.is_success() {
            anyhow::bail!(
                "proxy returned {}: {}",
                status,
                String::from_utf8_lossy(&resp_body)
            );
        }
        Ok::<Bytes, anyhow::Error>(resp_body)
    };

    match tokio::time::timeout(SUBMIT_TIMEOUT, do_submit).await {
        Ok(result) => result,
        Err(_) => anyhow::bail!("submission timed out after {SUBMIT_TIMEOUT:?}"),
    }
}
