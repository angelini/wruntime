use std::time::Duration;

use bytes::Bytes;
use deadpool_postgres::Pool;
use http_body_util::BodyExt;
use http_body_util::Full;
use hyper_util::rt::{TokioExecutor, TokioIo};
use prost::Message;
use tokio::net::TcpStream;
use tracing::{info, warn};
use wr_common::wruntime::{SubmitJobRequest, SubmitJobResponse};

use crate::db;

/// Background task: every `interval`, claim due schedules from the DB
/// using SKIP LOCKED, submit jobs to the appropriate engine, and mark fired.
pub async fn run_scheduler(pool: Pool, interval: Duration) {
    let mut tick = tokio::time::interval(interval);
    loop {
        tick.tick().await;
        if let Err(e) = evaluate_schedules(&pool).await {
            warn!(error = %e, "scheduler tick failed");
        }
    }
}

async fn evaluate_schedules(pool: &Pool) -> Result<(), anyhow::Error> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;

    let due = db::claim_due_schedules(&txn)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    if due.is_empty() {
        txn.commit().await?;
        return Ok(());
    }

    for schedule in &due {
        let engine_addr = db::resolve_engine_for_worker(
            pool,
            &schedule.worker_namespace,
            &schedule.worker_name,
            &schedule.worker_version,
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

        let engine_addr = match engine_addr {
            Some(addr) => addr,
            None => {
                warn!(
                    schedule_id = %schedule.schedule_id,
                    worker = %format!(
                        "{}/{}/{}",
                        schedule.worker_namespace, schedule.worker_name, schedule.worker_version
                    ),
                    "no healthy engine found, skipping"
                );
                continue;
            }
        };

        match submit_job_to_engine(&engine_addr, schedule).await {
            Ok(job_id) => {
                info!(
                    schedule_id = %schedule.schedule_id,
                    job_id,
                    engine = %engine_addr,
                    "schedule fired"
                );
                db::mark_schedule_fired(&txn, &schedule.schedule_id)
                    .await
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
            }
            Err(e) => {
                warn!(
                    schedule_id = %schedule.schedule_id,
                    engine = %engine_addr,
                    error = %e,
                    "failed to submit scheduled job, will retry next tick"
                );
            }
        }
    }

    txn.commit().await?;
    Ok(())
}

async fn submit_job_to_engine(
    engine_addr: &str,
    schedule: &db::ScheduleRow,
) -> Result<String, anyhow::Error> {
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

    // engine_addr is "http://host:port" — strip scheme for TCP connect
    let addr = engine_addr
        .trim_start_matches("http://")
        .trim_start_matches("https://");

    let stream = TcpStream::connect(addr).await?;
    let io = TokioIo::new(stream);

    let (mut sender, conn) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), io).await?;
    tokio::spawn(conn);

    let http_req = http::Request::builder()
        .method("POST")
        .uri("/wruntime.WorkerService/SubmitJob")
        .header("content-type", "application/x-protobuf")
        .body(Full::new(Bytes::from(body)))?;

    let resp = sender.send_request(http_req).await?;
    let status = resp.status();
    let resp_body = resp.into_body().collect().await?.to_bytes();

    if !status.is_success() {
        anyhow::bail!(
            "engine returned {}: {}",
            status,
            String::from_utf8_lossy(&resp_body)
        );
    }

    let submit_resp = SubmitJobResponse::decode(resp_body.as_ref())?;
    Ok(submit_resp.job_id)
}
