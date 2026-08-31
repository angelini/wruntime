use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use deadpool_postgres::Pool;
use http_body_util::BodyExt as _;
use tokio::sync::Notify;
use tracing::{error, info, warn};

use crate::{InboundRequest, ModuleTx};
use wr_common::lifecycle::{
    AttemptCount, JobState, JobTimeoutSecs, MaxAttempts, WorkerConcurrency,
};
use wr_common::lifecycle_service::AdmissionGate;
use wr_common::task_group::{TaskCancellation, TaskExit, TaskGroup};

/// Insert a job into the queue. Returns the generated job_id.
#[allow(clippy::too_many_arguments)]
pub async fn insert_job(
    pool: &Pool,
    namespace: &str,
    name: &str,
    version: &str,
    job_type: &str,
    payload: &[u8],
    timeout_secs: u32,
    max_attempts: u32,
    source_namespace: &str,
    source_module: &str,
) -> anyhow::Result<String> {
    let client = pool.get().await?;
    let job_id = uuid::Uuid::new_v4().to_string();
    let timeout = JobTimeoutSecs::new(if timeout_secs == 0 { 300 } else { timeout_secs })?;
    let attempts = MaxAttempts::new(if max_attempts == 0 { 3 } else { max_attempts })?;
    let timeout = i32::try_from(timeout.get())?;
    let attempts = i32::try_from(attempts.get())?;

    client
        .execute(
            "INSERT INTO wr__jobs.jobs \
             (job_id, worker_namespace, worker_name, worker_version, job_type, payload, \
              timeout_secs, max_attempts, source_namespace, source_module) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            &[
                &job_id,
                &namespace,
                &name,
                &version,
                &job_type,
                &payload,
                &timeout,
                &attempts,
                &source_namespace,
                &source_module,
            ],
        )
        .await?;

    Ok(job_id)
}

/// Query a job's current status.
pub async fn get_job_status(pool: &Pool, job_id: &str) -> anyhow::Result<Option<JobStatus>> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "SELECT job_id, status, result, error_message, attempt, max_attempts \
             FROM wr__jobs.jobs WHERE job_id = $1",
            &[&job_id],
        )
        .await?;

    row.map(|r| {
        let state = JobState::try_from(r.get::<_, &str>(1))?;
        let attempt_raw = r.get::<_, i32>(4);
        let max_raw = r.get::<_, i32>(5);
        let max_attempts = MaxAttempts::new(u32::try_from(max_raw)?)?;
        let attempt = AttemptCount::new(u32::try_from(attempt_raw)?).validate(max_attempts)?;
        Ok(JobStatus {
            job_id: r.get(0),
            status: state,
            result: r.get::<_, Option<Vec<u8>>>(2).unwrap_or_default(),
            error_message: r.get::<_, Option<String>>(3).unwrap_or_default(),
            attempt,
            max_attempts,
        })
    })
    .transpose()
}

pub struct JobStatus {
    pub job_id: String,
    pub status: JobState,
    pub result: Vec<u8>,
    pub error_message: String,
    pub attempt: AttemptCount,
    pub max_attempts: MaxAttempts,
}

/// A claimed job ready for dispatch.
pub struct ClaimedJob {
    pub job_id: String,
    pub job_type: String,
    pub payload: Vec<u8>,
    pub claim_id: uuid::Uuid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Finalization {
    Applied,
    Stale,
}

impl Finalization {
    fn from_affected_rows(rows: u64) -> Self {
        if rows == 1 {
            Self::Applied
        } else {
            Self::Stale
        }
    }
}

/// Claim one pending job for the given worker module.
/// Uses `FOR UPDATE SKIP LOCKED` to guarantee exclusive access across engines.
pub async fn claim_job(
    pool: &Pool,
    namespace: &str,
    name: &str,
    version: &str,
    engine_id: &str,
) -> anyhow::Result<Option<ClaimedJob>> {
    let client = pool.get().await?;
    let claim_id = uuid::Uuid::new_v4();
    let row = client
        .query_opt(
            "UPDATE wr__jobs.jobs SET status = 'running', claimed_at = now(), \
             lease_expires_at = now() + timeout_secs * interval '1 second', \
             claimed_by = $4, attempt = attempt + 1, claim_id = $5, updated_at = now() \
             WHERE job_id = ( \
               SELECT job_id FROM wr__jobs.jobs \
               WHERE worker_namespace = $1 \
                  AND worker_name = $2 \
                  AND (worker_version = $3 OR worker_version = '') \
                  AND status = 'pending' \
               ORDER BY created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED \
             ) RETURNING job_id, job_type, payload, claim_id",
            &[&namespace, &name, &version, &engine_id, &claim_id],
        )
        .await?;

    Ok(row.map(|r| ClaimedJob {
        job_id: r.get(0),
        job_type: r.get(1),
        payload: r.get::<_, Vec<u8>>(2),
        claim_id: r.get(3),
    }))
}

/// Mark a job as complete with a result if this is still its active claim.
pub async fn complete_job(
    pool: &Pool,
    job_id: &str,
    claim_id: uuid::Uuid,
    result: &[u8],
) -> anyhow::Result<Finalization> {
    let client = pool.get().await?;
    let rows = client
        .execute(
            "UPDATE wr__jobs.jobs SET status = 'complete', result = $3, \
             claimed_at = NULL, claimed_by = NULL, claim_id = NULL, lease_expires_at = NULL, \
             completed_at = now(), updated_at = now() \
             WHERE job_id = $1 AND status = 'running' AND claim_id = $2",
            &[&job_id, &claim_id, &result],
        )
        .await?;
    Ok(Finalization::from_affected_rows(rows))
}

/// Mark a job as failed if this is still its active claim.
pub async fn fail_job(
    pool: &Pool,
    job_id: &str,
    claim_id: uuid::Uuid,
    error_msg: &str,
) -> anyhow::Result<Finalization> {
    let client = pool.get().await?;
    let rows = client
        .execute(
            "UPDATE wr__jobs.jobs SET \
               status = CASE WHEN attempt < max_attempts THEN 'pending' ELSE 'dead' END, \
               error_message = $3, \
               claimed_at = NULL, \
               claimed_by = NULL, \
               claim_id = NULL, \
               lease_expires_at = NULL, \
               updated_at = now() \
             WHERE job_id = $1 AND status = 'running' AND claim_id = $2",
            &[&job_id, &claim_id, &error_msg],
        )
        .await?;
    Ok(Finalization::from_affected_rows(rows))
}

/// Recover expired leases with the active claim fence in the update predicate.
/// `SKIP LOCKED` keeps multiple engine coordinators safe and non-blocking.
pub async fn recover_stale_jobs(pool: &Pool) -> anyhow::Result<u64> {
    let client = pool.get().await?;
    let count = client
        .execute(
            "WITH expired AS ( \
               SELECT job_id, claim_id FROM wr__jobs.jobs \
               WHERE status = 'running' AND lease_expires_at <= now() \
               FOR UPDATE SKIP LOCKED \
             ) \
             UPDATE wr__jobs.jobs AS jobs SET \
               status = CASE WHEN jobs.attempt < jobs.max_attempts THEN 'pending' ELSE 'dead' END, \
               error_message = COALESCE(jobs.error_message, '') || ' [stale recovery]', \
               claimed_at = NULL, claimed_by = NULL, claim_id = NULL, lease_expires_at = NULL, \
               updated_at = now() \
             FROM expired \
             WHERE jobs.job_id = expired.job_id \
               AND jobs.status = 'running' \
               AND jobs.claim_id = expired.claim_id",
            &[],
        )
        .await?;
    Ok(count)
}

/// Register the engine-level stale lease recovery coordinator as owned work.
pub fn spawn_recovery_coordinator(tasks: &mut TaskGroup, pool: Arc<Pool>) {
    tasks.spawn("engine-job-recovery", move |mut cancellation| async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => return Ok(TaskExit::Cancelled),
                _ = interval.tick() => {}
            }
            let started = std::time::Instant::now();
            match recover_stale_jobs(&pool).await {
                Ok(recovered) => info!(
                    scans = 1_u64,
                    recovered,
                    duration_ms = started.elapsed().as_millis() as u64,
                    "stale job recovery scan complete"
                ),
                Err(error) => warn!(
                    scans = 1_u64,
                    duration_ms = started.elapsed().as_millis() as u64,
                    %error,
                    "stale job recovery failed"
                ),
            }
        }
    });
}

/// Configuration for a worker pool.
pub struct WorkerPoolConfig {
    pub namespace: String,
    pub name: String,
    pub version: String,
    pub engine_id: String,
    pub concurrency: WorkerConcurrency,
    pub poll_interval: Duration,
    pub job_timeout: Duration,
    /// Raw database URL for the LISTEN connection (outside of deadpool).
    pub database_url: String,
}

const LONG_IDENTITY_WORKER_CHANNEL: &str = "wr_jobs_long_identity";
const MAX_POSTGRES_CHANNEL_BYTES: usize = 63;

fn worker_channel(namespace: &str, name: &str, version: &str) -> String {
    let channel = if version.is_empty() {
        format!("wr_jobs_{namespace}_{name}_unversioned")
    } else {
        format!("wr_jobs_{namespace}_{name}_{version}")
    };
    if channel.len() > MAX_POSTGRES_CHANNEL_BYTES {
        LONG_IDENTITY_WORKER_CHANNEL.to_string()
    } else {
        channel
    }
}

/// Register N module-specific worker loops and one LISTEN task as owned work.
pub fn spawn_worker_pool(
    tasks: &mut TaskGroup,
    pool: Arc<Pool>,
    config: WorkerPoolConfig,
    tx: ModuleTx,
    admission: AdmissionGate,
) {
    let notify = Arc::new(Notify::new());
    let channels = vec![
        worker_channel(&config.namespace, &config.name, &config.version),
        worker_channel(&config.namespace, &config.name, ""),
    ];

    {
        let notify = notify.clone();
        let channels = channels.clone();
        let ns = config.namespace.clone();
        let name = config.name.clone();
        let version = config.version.clone();
        let db_url = config.database_url.clone();
        let task_name = format!("worker-listen-{ns}-{name}-{version}");
        tasks.spawn(task_name, move |cancellation| async move {
            listen_task(
                &db_url,
                &channels,
                notify,
                &ns,
                &name,
                &version,
                cancellation,
            )
            .await
        });
    }

    for worker_id in 0..config.concurrency.get() {
        let pool = pool.clone();
        let tx = tx.clone();
        let notify = notify.clone();
        let ns = config.namespace.clone();
        let name = config.name.clone();
        let version = config.version.clone();
        let engine_id = config.engine_id.clone();
        let poll_interval = config.poll_interval;
        let job_timeout = config.job_timeout;
        let admission = admission.clone();
        let task_name = format!("worker-{ns}-{name}-{version}-{worker_id}");
        tasks.spawn(task_name, move |cancellation| async move {
            worker_loop(
                worker_id,
                &pool,
                &tx,
                &notify,
                &ns,
                &name,
                &version,
                &engine_id,
                poll_interval,
                job_timeout,
                admission,
                cancellation,
            )
            .await
        });
    }

    info!(
        namespace = %config.namespace,
        module = %config.name,
        version = %config.version,
        concurrency = config.concurrency.get(),
        "worker pool started",
    );
}

async fn listen_task(
    db_url: &str,
    channels: &[String],
    notify: Arc<Notify>,
    ns: &str,
    name: &str,
    version: &str,
    mut cancellation: TaskCancellation,
) -> anyhow::Result<TaskExit> {
    loop {
        match listen_loop(db_url, channels, &notify, cancellation.clone()).await {
            Ok(exit) => return Ok(exit),
            Err(error) => {
                warn!(
                    namespace = %ns,
                    module = %name,
                    version = %version,
                    %error,
                    "LISTEN connection lost, reconnecting in 2s",
                );
                tokio::select! {
                    _ = cancellation.cancelled() => return Ok(TaskExit::Cancelled),
                    _ = tokio::time::sleep(Duration::from_secs(2)) => {}
                }
            }
        }
    }
}

async fn listen_loop(
    db_url: &str,
    channels: &[String],
    notify: &Arc<Notify>,
    cancellation: TaskCancellation,
) -> anyhow::Result<TaskExit> {
    let (client, mut connection) = tokio_postgres::connect(db_url, tokio_postgres::NoTls).await?;
    let driver_notify = Arc::clone(notify);
    let mut driver_cancellation = cancellation.clone();
    let driver = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = driver_cancellation.cancelled() => return Ok(TaskExit::Cancelled),
                message = std::future::poll_fn(|cx| connection.poll_message(cx)) => {
                    match message {
                        Some(Ok(tokio_postgres::AsyncMessage::Notification(_))) => {
                            driver_notify.notify_waiters();
                        }
                        Some(Ok(_)) => {}
                        Some(Err(error)) => return Err(error.into()),
                        None => anyhow::bail!("LISTEN connection closed"),
                    }
                }
            }
        }
    });

    let listen_sql = channels
        .iter()
        .map(|channel| format!("LISTEN \"{}\"", channel.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join("; ");
    if let Err(error) = client.batch_execute(&listen_sql).await {
        driver.abort();
        let _ = driver.await;
        return Err(error.into());
    }
    info!(channels = ?channels, "LISTEN active");

    let result = driver.await?;
    drop(client);
    result
}

async fn finalize_failure(pool: &Pool, job_id: &str, claim_id: uuid::Uuid, message: &str) {
    match fail_job(pool, job_id, claim_id, message).await {
        Ok(Finalization::Applied) => {}
        Ok(Finalization::Stale) => warn!(job_id, "ignored stale job failure"),
        Err(error) => error!(job_id, %error, "failed to mark job failed"),
    }
}

/// Dispatch a single claimed job: build an HTTP request, send it through the
/// module channel, wait for the response, and update job status accordingly.
async fn dispatch_job(pool: &Pool, tx: &ModuleTx, job: ClaimedJob, job_timeout: Duration) {
    let job_id = job.job_id.clone();
    let claim_id = job.claim_id;

    // Build HTTP request: POST /{job_type} with payload body.
    let request = match http::Request::builder()
        .method("POST")
        .uri(format!("http://localhost{}", job.job_type))
        .header("x-wr-job-id", &job.job_id)
        .header("x-wr-timeout", job_timeout.as_secs().to_string())
        .header("content-type", "application/x-protobuf")
        .body(crate::inbound_full(Bytes::from(job.payload)))
    {
        Ok(r) => r,
        Err(e) => {
            let msg = format!("build request: {e}");
            warn!(job_id = %job_id, error = %msg, "failed to build job request");
            finalize_failure(pool, &job_id, claim_id, &msg).await;
            return;
        }
    };

    // Dispatch through the module's channel.
    let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
    let inbound = InboundRequest {
        request,
        response_tx: resp_tx,
        span: tracing::Span::current(),
    };

    if tx.send(inbound).await.is_err() {
        let msg = "module channel closed";
        warn!(job_id = %job_id, msg);
        finalize_failure(pool, &job_id, claim_id, msg).await;
        return;
    }

    // Worker results are persisted bytes, so this boundary deliberately buffers
    // the streamed guest response under the existing job timeout.
    let response = tokio::time::timeout(job_timeout, async {
        let response = resp_rx
            .await
            .map_err(|_| "module dropped response".to_string())?;
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .map_err(|error| format!("response body: {error}"))?
            .to_bytes();
        Ok::<_, String>((status, body))
    })
    .await;

    match response {
        Ok(Ok((status, body))) if status.is_success() => {
            match complete_job(pool, &job_id, claim_id, &body).await {
                Ok(Finalization::Applied) => {}
                Ok(Finalization::Stale) => warn!(job_id = %job_id, "ignored stale job completion"),
                Err(e) => error!(job_id = %job_id, error = %e, "failed to mark job complete"),
            }
        }
        Ok(Ok((status, body))) => {
            let status = status.as_u16();
            let body = String::from_utf8_lossy(&body);
            let msg = format!("HTTP {status}: {body}");
            warn!(job_id = %job_id, status, "job failed");
            finalize_failure(pool, &job_id, claim_id, &msg).await;
        }
        Ok(Err(message)) => {
            warn!(job_id = %job_id, %message);
            finalize_failure(pool, &job_id, claim_id, &message).await;
        }
        Err(_) => {
            let msg = format!("job timed out after {}s", job_timeout.as_secs());
            warn!(job_id = %job_id, %msg);
            finalize_failure(pool, &job_id, claim_id, &msg).await;
        }
    }
}

/// Single worker loop: waits for notification, claims and dispatches jobs.
#[allow(clippy::too_many_arguments)]
async fn worker_loop(
    worker_id: usize,
    pool: &Pool,
    tx: &ModuleTx,
    notify: &Notify,
    namespace: &str,
    name: &str,
    version: &str,
    engine_id: &str,
    poll_interval: Duration,
    job_timeout: Duration,
    admission: AdmissionGate,
    mut cancellation: TaskCancellation,
) -> anyhow::Result<TaskExit> {
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return Ok(TaskExit::Cancelled),
            _ = notify.notified() => {}
            _ = tokio::time::sleep(poll_interval) => {}
        }

        loop {
            let Some(_claim_guard) = admission.try_enter() else {
                break;
            };
            let job = match claim_job(pool, namespace, name, version, engine_id).await {
                Ok(Some(job)) => job,
                Ok(None) => break,
                Err(error) => {
                    warn!(
                        worker_id,
                        namespace,
                        module = name,
                        version,
                        %error,
                        "claim_job failed",
                    );
                    break;
                }
            };

            info!(
                worker_id,
                namespace,
                module = name,
                version,
                job_id = %job.job_id,
                job_type = %job.job_type,
                "processing job",
            );
            dispatch_job(pool, tx, job, job_timeout).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn db_url() -> Option<String> {
        std::env::var("WRT_TEST_DB_URL").ok()
    }

    /// Returns a unique test prefix to isolate parallel tests.
    fn unique_prefix() -> String {
        let n = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("wt{n}_{ts}")
    }

    async fn test_pool() -> Option<Pool> {
        use tokio::sync::OnceCell;
        static PROVISIONED: OnceCell<()> = OnceCell::const_new();

        let url = db_url()?;
        let pool = crate::pool::build_pool(&url, 2).expect("build pool");

        // Provision the schema exactly once across all parallel tests.
        PROVISIONED
            .get_or_init(|| async {
                crate::job_migration::run_job_migrations(&pool)
                    .await
                    .expect("migrate job schema");
            })
            .await;

        Some(pool)
    }

    #[test]
    fn test_worker_channels_distinguish_exact_and_unversioned_jobs() {
        assert_eq!(
            worker_channel("shop", "processor", "1.0.0"),
            "wr_jobs_shop_processor_1.0.0"
        );
        assert_eq!(
            worker_channel("shop", "processor", ""),
            "wr_jobs_shop_processor_unversioned"
        );
        assert_eq!(
            worker_channel(&"n".repeat(45), "worker", ""),
            LONG_IDENTITY_WORKER_CHANNEL
        );
        assert_eq!(
            worker_channel(&"n".repeat(45), "worker", "123456789"),
            LONG_IDENTITY_WORKER_CHANNEL
        );
    }

    /// Helper macro: skip the test if no DB URL is set.
    macro_rules! require_pool {
        () => {
            match test_pool().await {
                Some(p) => p,
                None => {
                    eprintln!("skipping (no WRT_TEST_DB_URL)");
                    return;
                }
            }
        };
    }

    #[tokio::test]
    async fn test_insert_and_get_job_status() {
        let pool = require_pool!();
        let p = unique_prefix();
        let job_id = insert_job(
            &pool,
            &p,
            "mod",
            "1.0.0",
            "/test/Process",
            b"hello",
            60,
            3,
            "src-ns",
            "src-mod",
        )
        .await
        .expect("insert job");

        let status = get_job_status(&pool, &job_id)
            .await
            .expect("get status")
            .expect("job should exist");
        assert_eq!(status.job_id, job_id);
        assert_eq!(status.status, "pending");
        assert_eq!(status.attempt, 0);
        assert_eq!(status.max_attempts, 3);
    }

    #[tokio::test]
    async fn test_insert_unversioned_job_with_long_identity() {
        let pool = require_pool!();
        let namespace = "n".repeat(45);
        let name = format!("worker_{}", unique_prefix());
        let job_id = insert_job(
            &pool,
            &namespace,
            &name,
            "",
            "/test/Process",
            b"payload",
            60,
            3,
            "",
            "",
        )
        .await
        .expect("long unversioned identity must use the bounded fallback channel");
        assert!(get_job_status(&pool, &job_id).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_get_job_status_not_found() {
        let pool = require_pool!();
        let status = get_job_status(&pool, "nonexistent-id")
            .await
            .expect("get status");
        assert!(status.is_none());
    }

    #[tokio::test]
    async fn test_claim_job_returns_oldest_pending() {
        let pool = require_pool!();
        let p = unique_prefix();
        let id1 = insert_job(
            &pool, &p, "mod", "1.0.0", "/type/A", b"first", 60, 3, "", "",
        )
        .await
        .unwrap();
        let _id2 = insert_job(
            &pool, &p, "mod", "1.0.0", "/type/B", b"second", 60, 3, "", "",
        )
        .await
        .unwrap();

        let claimed = claim_job(&pool, &p, "mod", "1.0.0", "engine-1")
            .await
            .expect("claim")
            .expect("should claim a job");
        assert_eq!(claimed.job_id, id1);
        assert_eq!(claimed.job_type, "/type/A");
        assert_eq!(claimed.payload, b"first");

        let status = get_job_status(&pool, &id1).await.unwrap().unwrap();
        assert_eq!(status.status, "running");
        assert_eq!(status.attempt, 1);
    }

    #[tokio::test]
    async fn test_claim_job_is_version_scoped_and_preserves_order_within_version() {
        let pool = require_pool!();
        let p = unique_prefix();
        let id_v1_old = insert_job(
            &pool,
            &p,
            "mod",
            "1.0.0",
            "/type/v1-old",
            b"v1-old",
            60,
            3,
            "",
            "",
        )
        .await
        .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let id_v2 = insert_job(&pool, &p, "mod", "2.0.0", "/type/v2", b"v2", 60, 3, "", "")
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let id_v1_new = insert_job(
            &pool,
            &p,
            "mod",
            "1.0.0",
            "/type/v1-new",
            b"v1-new",
            60,
            3,
            "",
            "",
        )
        .await
        .unwrap();

        let claimed_v2 = claim_job(&pool, &p, "mod", "2.0.0", "engine-v2")
            .await
            .unwrap()
            .expect("v2 worker should claim v2 job");
        assert_eq!(claimed_v2.job_id, id_v2);
        assert_eq!(claimed_v2.job_type, "/type/v2");
        assert_eq!(claimed_v2.payload, b"v2");

        let claimed_v1_old = claim_job(&pool, &p, "mod", "1.0.0", "engine-v1")
            .await
            .unwrap()
            .expect("v1 worker should claim oldest v1 job");
        assert_eq!(claimed_v1_old.job_id, id_v1_old);

        let claimed_v1_new = claim_job(&pool, &p, "mod", "1.0.0", "engine-v1")
            .await
            .unwrap()
            .expect("v1 worker should claim second v1 job");
        assert_eq!(claimed_v1_new.job_id, id_v1_new);

        let no_v2_left = claim_job(&pool, &p, "mod", "2.0.0", "engine-v2")
            .await
            .unwrap();
        assert!(no_v2_left.is_none());
    }

    #[tokio::test]
    async fn test_claim_job_accepts_unversioned_but_not_other_exact_versions() {
        let pool = require_pool!();
        let p = unique_prefix();
        let unversioned = insert_job(
            &pool,
            &p,
            "mod",
            "",
            "/type/unversioned",
            b"any",
            60,
            3,
            "",
            "",
        )
        .await
        .unwrap();
        let other_version = insert_job(&pool, &p, "mod", "2.0.0", "/type/v2", b"v2", 60, 3, "", "")
            .await
            .unwrap();

        let claimed = claim_job(&pool, &p, "mod", "1.0.0", "engine-v1")
            .await
            .unwrap()
            .expect("v1 worker should claim the unversioned job");
        assert_eq!(claimed.job_id, unversioned);
        assert!(
            claim_job(&pool, &p, "mod", "1.0.0", "engine-v1")
                .await
                .unwrap()
                .is_none(),
            "v1 worker must not claim an exact v2 job"
        );
        let claimed_v2 = claim_job(&pool, &p, "mod", "2.0.0", "engine-v2")
            .await
            .unwrap()
            .expect("v2 worker should claim its exact job");
        assert_eq!(claimed_v2.job_id, other_version);
    }

    #[tokio::test]
    async fn test_claim_job_skips_other_modules() {
        let pool = require_pool!();
        let p = unique_prefix();
        let _id = insert_job(&pool, &p, "other", "1.0.0", "/test", b"", 60, 3, "", "")
            .await
            .unwrap();

        let claimed = claim_job(&pool, &p, "target", "1.0.0", "engine-1")
            .await
            .expect("claim");
        assert!(claimed.is_none(), "should not claim job for other module");
    }

    #[tokio::test]
    async fn test_claim_job_returns_none_when_empty() {
        let pool = require_pool!();
        let p = unique_prefix();
        let claimed = claim_job(&pool, &p, "mod", "1.0.0", "engine-1")
            .await
            .expect("claim");
        assert!(claimed.is_none());
    }

    #[tokio::test]
    async fn test_complete_job() {
        let pool = require_pool!();
        let p = unique_prefix();
        let id = insert_job(&pool, &p, "mod", "1.0.0", "/test", b"", 60, 3, "", "")
            .await
            .unwrap();
        let claim = claim_job(&pool, &p, "mod", "1.0.0", "engine-1")
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            complete_job(&pool, &id, claim.claim_id, b"result-data")
                .await
                .expect("complete"),
            Finalization::Applied
        );
        let status = get_job_status(&pool, &id).await.unwrap().unwrap();
        assert_eq!(status.status, "complete");
        assert_eq!(status.result, b"result-data");
    }

    #[tokio::test]
    async fn test_fail_job_retries() {
        let pool = require_pool!();
        let p = unique_prefix();
        let id = insert_job(&pool, &p, "mod", "1.0.0", "/test", b"", 60, 3, "", "")
            .await
            .unwrap();
        let claim = claim_job(&pool, &p, "mod", "1.0.0", "engine-1")
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            fail_job(&pool, &id, claim.claim_id, "oops")
                .await
                .expect("fail"),
            Finalization::Applied
        );
        let status = get_job_status(&pool, &id).await.unwrap().unwrap();
        assert_eq!(status.status, "pending");
        assert_eq!(status.error_message, "oops");
    }

    #[tokio::test]
    async fn test_fail_job_marks_dead_after_max_attempts() {
        let pool = require_pool!();
        let p = unique_prefix();
        let id = insert_job(&pool, &p, "mod", "1.0.0", "/test", b"", 60, 1, "", "")
            .await
            .unwrap();
        let claim = claim_job(&pool, &p, "mod", "1.0.0", "engine-1")
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            fail_job(&pool, &id, claim.claim_id, "final failure")
                .await
                .expect("fail"),
            Finalization::Applied
        );
        let status = get_job_status(&pool, &id).await.unwrap().unwrap();
        assert_eq!(status.status, "dead");
    }

    #[tokio::test]
    async fn claim_persists_complete_fixed_lease_metadata() {
        let pool = require_pool!();
        let namespace = unique_prefix();
        let id = insert_job(
            &pool, &namespace, "mod", "1.0.0", "/test", b"", 47, 3, "", "",
        )
        .await
        .unwrap();
        let claim = claim_job(&pool, &namespace, "mod", "1.0.0", "engine-lease")
            .await
            .unwrap()
            .unwrap();
        let row = pool
            .get()
            .await
            .unwrap()
            .query_one(
                "SELECT claim_id, claimed_by, \
                 extract(epoch FROM (lease_expires_at - claimed_at))::bigint \
                 FROM wr__jobs.jobs WHERE job_id = $1",
                &[&id],
            )
            .await
            .unwrap();
        assert_eq!(row.get::<_, uuid::Uuid>(0), claim.claim_id);
        assert_eq!(row.get::<_, String>(1), "engine-lease");
        assert_eq!(row.get::<_, i64>(2), 47);
    }

    #[tokio::test]
    async fn test_recover_stale_jobs() {
        let pool = require_pool!();
        let p = unique_prefix();
        let id = insert_job(&pool, &p, "mod", "1.0.0", "/test", b"", 1, 3, "", "")
            .await
            .unwrap();
        let _ = claim_job(&pool, &p, "mod", "1.0.0", "engine-1").await;

        let client = pool.get().await.unwrap();
        client
            .execute(
                "UPDATE wr__jobs.jobs SET lease_expires_at = now() - interval '10 seconds' WHERE job_id = $1",
                &[&id],
            )
            .await
            .unwrap();

        recover_stale_jobs(&pool).await.expect("recover");

        let status = get_job_status(&pool, &id).await.unwrap().unwrap();
        assert_eq!(status.status, "pending");
        assert!(status.error_message.contains("[stale recovery]"));
    }

    #[tokio::test]
    async fn concurrent_engine_recovery_applies_each_lease_once() {
        let pool = require_pool!();
        let namespace = unique_prefix();
        let id = insert_job(
            &pool, &namespace, "mod", "1.0.0", "/test", b"", 1, 3, "", "",
        )
        .await
        .unwrap();
        claim_job(&pool, &namespace, "mod", "1.0.0", "engine-a")
            .await
            .unwrap()
            .unwrap();
        pool.get()
            .await
            .unwrap()
            .execute(
                "UPDATE wr__jobs.jobs SET lease_expires_at = now() - interval '1 second' \
                 WHERE job_id = $1",
                &[&id],
            )
            .await
            .unwrap();

        let (left, right) = tokio::join!(recover_stale_jobs(&pool), recover_stale_jobs(&pool));
        left.unwrap();
        right.unwrap();
        let status = get_job_status(&pool, &id).await.unwrap().unwrap();
        assert_eq!(status.status, "pending");
        assert_eq!(status.error_message.matches("[stale recovery]").count(), 1);
    }

    #[tokio::test]
    async fn stale_claim_cannot_finalize_a_recovered_job() {
        let pool = require_pool!();
        let namespace = unique_prefix();
        let id = insert_job(
            &pool, &namespace, "mod", "1.0.0", "/test", b"payload", 1, 3, "", "",
        )
        .await
        .unwrap();
        let claim_a = claim_job(&pool, &namespace, "mod", "1.0.0", "engine-a")
            .await
            .unwrap()
            .unwrap();
        let client = pool.get().await.unwrap();
        client
            .execute(
                "UPDATE wr__jobs.jobs SET lease_expires_at = now() - interval '10 seconds' WHERE job_id = $1",
                &[&id],
            )
            .await
            .unwrap();
        recover_stale_jobs(&pool).await.unwrap();

        let claim_b = claim_job(&pool, &namespace, "mod", "1.0.0", "engine-b")
            .await
            .unwrap()
            .unwrap();
        assert_ne!(claim_a.claim_id, claim_b.claim_id);
        assert_eq!(
            complete_job(&pool, &id, claim_a.claim_id, b"stale")
                .await
                .unwrap(),
            Finalization::Stale
        );
        assert_eq!(
            fail_job(&pool, &id, claim_a.claim_id, "stale failure")
                .await
                .unwrap(),
            Finalization::Stale
        );
        let row = client
            .query_one(
                "SELECT status, claimed_by, result FROM wr__jobs.jobs WHERE job_id = $1",
                &[&id],
            )
            .await
            .unwrap();
        assert_eq!(row.get::<_, &str>(0), "running");
        assert_eq!(row.get::<_, &str>(1), "engine-b");
        assert_eq!(row.get::<_, Option<Vec<u8>>>(2), None);
        assert_eq!(
            complete_job(&pool, &id, claim_b.claim_id, b"fresh")
                .await
                .unwrap(),
            Finalization::Applied
        );
    }

    #[tokio::test]
    async fn test_insert_job_defaults() {
        let pool = require_pool!();
        let p = unique_prefix();
        let id = insert_job(&pool, &p, "mod", "1.0.0", "/test", b"", 0, 0, "", "")
            .await
            .unwrap();

        let client = pool.get().await.unwrap();
        let row = client
            .query_one(
                "SELECT timeout_secs, max_attempts FROM wr__jobs.jobs WHERE job_id = $1",
                &[&id],
            )
            .await
            .unwrap();
        let timeout: i32 = row.get(0);
        let max_attempts: i32 = row.get(1);
        assert_eq!(timeout, 300);
        assert_eq!(max_attempts, 3);
    }

    #[tokio::test]
    async fn test_claim_does_not_claim_running_jobs() {
        let pool = require_pool!();
        let p = unique_prefix();
        let id = insert_job(&pool, &p, "mod", "1.0.0", "/test", b"", 60, 3, "", "")
            .await
            .unwrap();
        let claimed = claim_job(&pool, &p, "mod", "1.0.0", "engine-1")
            .await
            .unwrap();
        assert!(claimed.is_some());
        assert_eq!(claimed.unwrap().job_id, id);

        let claimed2 = claim_job(&pool, &p, "mod", "1.0.0", "engine-2")
            .await
            .unwrap();
        assert!(claimed2.is_none());
    }

    #[tokio::test]
    async fn test_full_lifecycle_pending_running_complete() {
        let pool = require_pool!();
        let p = unique_prefix();
        let id = insert_job(
            &pool, &p, "mod", "1.0.0", "/test/Do", b"payload", 60, 3, "s", "m",
        )
        .await
        .unwrap();

        let s = get_job_status(&pool, &id).await.unwrap().unwrap();
        assert_eq!(s.status, "pending");

        let claimed = claim_job(&pool, &p, "mod", "1.0.0", "e1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claimed.job_id, id);
        assert_eq!(claimed.job_type, "/test/Do");
        assert_eq!(claimed.payload, b"payload");
        let s = get_job_status(&pool, &id).await.unwrap().unwrap();
        assert_eq!(s.status, "running");
        assert_eq!(s.attempt, 1);

        assert_eq!(
            complete_job(&pool, &id, claimed.claim_id, b"done")
                .await
                .unwrap(),
            Finalization::Applied
        );
        let s = get_job_status(&pool, &id).await.unwrap().unwrap();
        assert_eq!(s.status, "complete");
        assert_eq!(s.result, b"done");
    }

    #[tokio::test]
    async fn test_full_lifecycle_pending_running_fail_retry_complete() {
        let pool = require_pool!();
        let p = unique_prefix();
        let id = insert_job(&pool, &p, "mod", "1.0.0", "/test", b"", 60, 2, "", "")
            .await
            .unwrap();

        let claim1 = claim_job(&pool, &p, "mod", "1.0.0", "e1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            fail_job(&pool, &id, claim1.claim_id, "transient error")
                .await
                .unwrap(),
            Finalization::Applied
        );
        let s = get_job_status(&pool, &id).await.unwrap().unwrap();
        assert_eq!(s.status, "pending");
        assert_eq!(s.attempt, 1);

        let claim2 = claim_job(&pool, &p, "mod", "1.0.0", "e1")
            .await
            .unwrap()
            .unwrap();
        assert_ne!(claim1.claim_id, claim2.claim_id);
        assert_eq!(
            complete_job(&pool, &id, claim2.claim_id, b"ok")
                .await
                .unwrap(),
            Finalization::Applied
        );
        let s = get_job_status(&pool, &id).await.unwrap().unwrap();
        assert_eq!(s.status, "complete");
        assert_eq!(s.attempt, 2);
    }
}
