use std::future::Future;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use deadpool_postgres::Pool;
use tokio::time::sleep;
use tonic::transport::Channel;

use wr_common::wruntime::{
    manager_service_client::ManagerServiceClient, ListManagersRequest, ManagerInfo,
};
use wr_engine::worker::JobStatus;

pub const DEFAULT_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(50);

pub async fn eventually<T, F, Fut>(
    context: impl Into<String>,
    timeout: Duration,
    interval: Duration,
    mut check: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Option<T>>>,
{
    let context = context.into();
    let started = Instant::now();
    let deadline = started + timeout;
    let mut last_error: Option<String> = None;

    loop {
        match check().await {
            Ok(Some(value)) => return Ok(value),
            Ok(None) => {}
            Err(err) => last_error = Some(format!("{err:#}")),
        }

        if Instant::now() >= deadline {
            let elapsed = started.elapsed();
            if let Some(last_error) = last_error {
                bail!(
                    "timed out after {elapsed:?} waiting for {context}; last error: {last_error}"
                );
            }
            bail!("timed out after {elapsed:?} waiting for {context}");
        }

        sleep(interval).await;
    }
}

pub async fn wait_for_rule_health(
    mgr: &mut ManagerServiceClient<Channel>,
    destination_module: &str,
    expected_healthy: bool,
    timeout: Duration,
) -> Result<(bool, u64)> {
    let context = format!("routing rule {destination_module} healthy={expected_healthy}");
    let started = Instant::now();
    let deadline = started + timeout;
    let mut last_seen: Option<(bool, u64)> = None;

    loop {
        match super::manager::get_rule_health(mgr, destination_module).await {
            Ok((healthy, version)) if healthy == expected_healthy => return Ok((healthy, version)),
            Ok((healthy, version)) => last_seen = Some((healthy, version)),
            Err(err) if Instant::now() >= deadline => {
                bail!(
                    "timed out after {:?} waiting for {context}; last error: {err:#}",
                    started.elapsed()
                );
            }
            Err(_) => {}
        }

        if Instant::now() >= deadline {
            bail!(
                "timed out after {:?} waiting for {context}; last seen: {:?}",
                started.elapsed(),
                last_seen
            );
        }
        sleep(DEFAULT_POLL_INTERVAL).await;
    }
}

pub async fn wait_for_routing_table_version_gt(
    mgr: &mut ManagerServiceClient<Channel>,
    baseline: u64,
    timeout: Duration,
) -> Result<u64> {
    let started = Instant::now();
    let deadline = started + timeout;
    let mut last_seen = None;
    loop {
        let version = super::manager::get_routing_table_version(mgr).await?;
        if version > baseline {
            return Ok(version);
        }
        last_seen = Some(version);
        if Instant::now() >= deadline {
            bail!(
                "timed out after {:?} waiting for routing table version > {baseline}; last seen: {:?}",
                started.elapsed(),
                last_seen
            );
        }
        sleep(DEFAULT_POLL_INTERVAL).await;
    }
}

pub async fn wait_for_manager_count(
    mgr: &mut ManagerServiceClient<Channel>,
    expected: usize,
    timeout: Duration,
) -> Result<Vec<ManagerInfo>> {
    let started = Instant::now();
    let deadline = started + timeout;
    let mut last_seen = None;
    loop {
        let managers = mgr
            .list_managers(ListManagersRequest {})
            .await?
            .into_inner()
            .managers;
        if managers.len() == expected {
            return Ok(managers);
        }
        last_seen = Some(managers.len());
        if Instant::now() >= deadline {
            bail!(
                "timed out after {:?} waiting for manager count == {expected}; last seen: {:?}",
                started.elapsed(),
                last_seen
            );
        }
        sleep(DEFAULT_POLL_INTERVAL).await;
    }
}

pub async fn wait_for_manager_absent(
    mgr: &mut ManagerServiceClient<Channel>,
    manager_id: &str,
    timeout: Duration,
) -> Result<Vec<ManagerInfo>> {
    let started = Instant::now();
    let deadline = started + timeout;
    let mut last_ids: Vec<String> = Vec::new();
    loop {
        let managers = mgr
            .list_managers(ListManagersRequest {})
            .await?
            .into_inner()
            .managers;
        if !managers.iter().any(|m| m.manager_id == manager_id) {
            return Ok(managers);
        }
        last_ids = managers.iter().map(|m| m.manager_id.clone()).collect();
        if Instant::now() >= deadline {
            bail!(
                "timed out after {:?} waiting for manager {manager_id} absent from ListManagers; last ids: {:?}",
                started.elapsed(),
                last_ids
            );
        }
        sleep(DEFAULT_POLL_INTERVAL).await;
    }
}

pub async fn wait_for_job_status(
    pool: &Pool,
    job_id: &str,
    expected_status: &str,
    timeout: Duration,
) -> Result<JobStatus> {
    eventually(
        format!("job {job_id} status == {expected_status}"),
        timeout,
        DEFAULT_POLL_INTERVAL,
        || async {
            let status = wr_engine::worker::get_job_status(pool, job_id).await?;
            Ok(status.filter(|s| s.status == expected_status))
        },
    )
    .await
}
