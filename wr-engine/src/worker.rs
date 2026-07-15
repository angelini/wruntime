use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use deadpool_postgres::Pool;
use tokio::sync::Notify;
use tracing::{error, info, warn};

use crate::{InboundRequest, ModuleTx};
use wr_common::lifecycle::{
    AttemptCount, JobState, JobTimeoutSecs, MaxAttempts, WorkerConcurrency,
};

/// Provision the `wr__jobs` schema, table, indexes, and NOTIFY trigger.
/// Idempotent — safe to call on every startup.
pub async fn provision_job_schema(pool: &Pool) -> anyhow::Result<()> {
    let client = pool.get().await?;

    client
        .batch_execute(
            r#"
CREATE SCHEMA IF NOT EXISTS wr__jobs;

CREATE TABLE IF NOT EXISTS wr__jobs.jobs (
    job_id            TEXT        PRIMARY KEY,
    worker_namespace  TEXT        NOT NULL,
    worker_name       TEXT        NOT NULL,
    worker_version    TEXT        NOT NULL,
    job_type          TEXT        NOT NULL DEFAULT '/',
    payload           BYTEA      NOT NULL DEFAULT '',
    status            TEXT        NOT NULL DEFAULT 'pending',
    result            BYTEA,
    error_message     TEXT,
    attempt           INT         NOT NULL DEFAULT 0,
    max_attempts      INT         NOT NULL DEFAULT 3,
    timeout_secs      INT         NOT NULL DEFAULT 300,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at        TIMESTAMPTZ,
    completed_at      TIMESTAMPTZ,
    claimed_by        TEXT,
    source_namespace  TEXT        NOT NULL DEFAULT '',
    source_module     TEXT        NOT NULL DEFAULT ''
);

DROP INDEX IF EXISTS wr__jobs.idx_jobs_pending;
CREATE INDEX IF NOT EXISTS idx_jobs_pending
    ON wr__jobs.jobs (worker_namespace, worker_name, worker_version, created_at)
    WHERE status = 'pending';

UPDATE wr__jobs.jobs SET status = 'dead' WHERE status NOT IN ('pending', 'running', 'complete', 'dead');
UPDATE wr__jobs.jobs SET timeout_secs = 300 WHERE timeout_secs <= 0;
UPDATE wr__jobs.jobs SET max_attempts = 3 WHERE max_attempts <= 0;
UPDATE wr__jobs.jobs SET attempt = 0 WHERE attempt < 0;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'jobs_status_valid' AND conrelid = 'wr__jobs.jobs'::regclass) THEN
        ALTER TABLE wr__jobs.jobs ADD CONSTRAINT jobs_status_valid CHECK (status IN ('pending', 'running', 'complete', 'dead')) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'jobs_timeout_positive' AND conrelid = 'wr__jobs.jobs'::regclass) THEN
        ALTER TABLE wr__jobs.jobs ADD CONSTRAINT jobs_timeout_positive CHECK (timeout_secs > 0) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'jobs_max_attempts_positive' AND conrelid = 'wr__jobs.jobs'::regclass) THEN
        ALTER TABLE wr__jobs.jobs ADD CONSTRAINT jobs_max_attempts_positive CHECK (max_attempts > 0) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'jobs_attempt_valid' AND conrelid = 'wr__jobs.jobs'::regclass) THEN
        ALTER TABLE wr__jobs.jobs ADD CONSTRAINT jobs_attempt_valid CHECK (attempt >= 0 AND attempt <= max_attempts) NOT VALID;
    END IF;
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
ALTER TABLE wr__jobs.jobs VALIDATE CONSTRAINT jobs_status_valid;
ALTER TABLE wr__jobs.jobs VALIDATE CONSTRAINT jobs_timeout_positive;
ALTER TABLE wr__jobs.jobs VALIDATE CONSTRAINT jobs_max_attempts_positive;
ALTER TABLE wr__jobs.jobs VALIDATE CONSTRAINT jobs_attempt_valid;

DROP INDEX IF EXISTS wr__jobs.idx_jobs_stale;
CREATE INDEX IF NOT EXISTS idx_jobs_stale
    ON wr__jobs.jobs (claimed_at)
    WHERE status = 'running';

CREATE OR REPLACE FUNCTION wr__jobs.notify_new_job() RETURNS trigger AS $$
DECLARE
    channel_name TEXT := CASE WHEN NEW.worker_version = ''
        THEN 'wr_jobs_' || NEW.worker_namespace || '_' || NEW.worker_name || '_unversioned'
        ELSE 'wr_jobs_' || NEW.worker_namespace || '_' || NEW.worker_name || '_' || NEW.worker_version
    END;
BEGIN
    IF octet_length(channel_name) > 63 THEN
        channel_name := 'wr_jobs_long_identity';
    END IF;
    PERFORM pg_notify(channel_name, NEW.job_id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_notify_new_job ON wr__jobs.jobs;
CREATE TRIGGER trg_notify_new_job
    AFTER INSERT ON wr__jobs.jobs
    FOR EACH ROW EXECUTE FUNCTION wr__jobs.notify_new_job();
"#,
        )
        .await?;

    info!("wr__jobs schema provisioned");
    Ok(())
}

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
    let row = client
        .query_opt(
            "UPDATE wr__jobs.jobs SET status = 'running', claimed_at = now(), \
             claimed_by = $4, attempt = attempt + 1, updated_at = now() \
             WHERE job_id = ( \
               SELECT job_id FROM wr__jobs.jobs \
               WHERE worker_namespace = $1 \
                  AND worker_name = $2 \
                  AND (worker_version = $3 OR worker_version = '') \
                  AND status = 'pending' \
               ORDER BY created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED \
             ) RETURNING job_id, job_type, payload",
            &[&namespace, &name, &version, &engine_id],
        )
        .await?;

    Ok(row.map(|r| ClaimedJob {
        job_id: r.get(0),
        job_type: r.get(1),
        payload: r.get::<_, Vec<u8>>(2),
    }))
}

/// Mark a job as complete with a result.
pub async fn complete_job(pool: &Pool, job_id: &str, result: &[u8]) -> anyhow::Result<()> {
    let client = pool.get().await?;
    client
        .execute(
            "UPDATE wr__jobs.jobs SET status = 'complete', result = $2, \
             completed_at = now(), updated_at = now() WHERE job_id = $1",
            &[&job_id, &result],
        )
        .await?;
    Ok(())
}

/// Mark a job as failed. If retries remain, reset to pending; otherwise mark dead.
pub async fn fail_job(pool: &Pool, job_id: &str, error_msg: &str) -> anyhow::Result<()> {
    let client = pool.get().await?;
    // Check if we can retry.
    client
        .execute(
            "UPDATE wr__jobs.jobs SET \
               status = CASE WHEN attempt < max_attempts THEN 'pending' ELSE 'dead' END, \
               error_message = $2, \
               claimed_at = NULL, \
               claimed_by = NULL, \
               updated_at = now() \
             WHERE job_id = $1",
            &[&job_id, &error_msg],
        )
        .await?;
    Ok(())
}

/// Reset jobs that have been running longer than their timeout.
pub async fn recover_stale_jobs(pool: &Pool) -> anyhow::Result<u64> {
    let client = pool.get().await?;
    let count = client
        .execute(
            "UPDATE wr__jobs.jobs SET \
               status = CASE WHEN attempt < max_attempts THEN 'pending' ELSE 'dead' END, \
               error_message = COALESCE(error_message, '') || ' [stale recovery]', \
               claimed_at = NULL, \
               claimed_by = NULL, \
               updated_at = now() \
             WHERE status = 'running' \
               AND claimed_at < now() - (timeout_secs || ' seconds')::interval",
            &[],
        )
        .await?;
    Ok(count)
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

/// Spawn the worker pool: N worker loops + a LISTEN task + a stale recovery task.
/// The worker loops dispatch jobs as HTTP requests through the provided `ModuleTx`.
pub fn spawn_worker_pool(pool: Arc<Pool>, config: WorkerPoolConfig, tx: ModuleTx) {
    let notify = Arc::new(Notify::new());
    let channels = vec![
        worker_channel(&config.namespace, &config.name, &config.version),
        worker_channel(&config.namespace, &config.name, ""),
    ];

    // Spawn LISTEN task with a raw tokio-postgres connection.
    {
        let notify = notify.clone();
        let channels = channels.clone();
        let ns = config.namespace.clone();
        let name = config.name.clone();
        let version = config.version.clone();
        let db_url = config.database_url.clone();
        tokio::spawn(async move {
            listen_task(&db_url, &channels, notify, &ns, &name, &version).await;
        });
    }

    // Spawn worker loops.
    for i in 0..config.concurrency.get() {
        let pool = pool.clone();
        let tx = tx.clone();
        let notify = notify.clone();
        let ns = config.namespace.clone();
        let name = config.name.clone();
        let version = config.version.clone();
        let engine_id = config.engine_id.clone();
        let poll_interval = config.poll_interval;
        let job_timeout = config.job_timeout;
        tokio::spawn(async move {
            worker_loop(
                i,
                &pool,
                &tx,
                &notify,
                &ns,
                &name,
                &version,
                &engine_id,
                poll_interval,
                job_timeout,
            )
            .await;
        });
    }

    // Spawn stale recovery task.
    {
        let pool = pool.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                interval.tick().await;
                match recover_stale_jobs(&pool).await {
                    Ok(0) => {}
                    Ok(n) => info!(recovered = n, "recovered stale jobs"),
                    Err(e) => warn!(error = %e, "stale job recovery failed"),
                }
            }
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

/// Dedicated connection that runs LISTEN and wakes worker loops via Notify.
async fn listen_task(
    db_url: &str,
    channels: &[String],
    notify: Arc<Notify>,
    ns: &str,
    name: &str,
    version: &str,
) {
    loop {
        match listen_loop(db_url, channels, &notify).await {
            Ok(()) => break,
            Err(e) => {
                warn!(
                    namespace = %ns,
                    module = %name,
                    version = %version,
                    error = %e,
                    "LISTEN connection lost, reconnecting in 2s",
                );
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

async fn listen_loop(
    db_url: &str,
    channels: &[String],
    notify: &Arc<Notify>,
) -> anyhow::Result<()> {
    let (client, mut connection) = tokio_postgres::connect(db_url, tokio_postgres::NoTls).await?;

    // Drive the connection manually so we can intercept notifications.
    let notify = Arc::clone(notify);
    let conn_handle = tokio::spawn(async move {
        loop {
            match std::future::poll_fn(|cx| connection.poll_message(cx)).await {
                Some(Ok(tokio_postgres::AsyncMessage::Notification(_))) => {
                    notify.notify_waiters();
                }
                Some(Ok(_)) => {} // parameter status, etc.
                Some(Err(e)) => {
                    warn!(error = %e, "LISTEN connection error");
                    break;
                }
                None => break,
            }
        }
    });

    let listen_sql = channels
        .iter()
        .map(|channel| format!("LISTEN \"{}\"", channel.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join("; ");
    client.batch_execute(&listen_sql).await?;

    info!(channels = ?channels, "LISTEN active");

    // Wait for the connection task to finish (connection lost).
    conn_handle.await?;
    anyhow::bail!("LISTEN connection closed")
}

/// Dispatch a single claimed job: build an HTTP request, send it through the
/// module channel, wait for the response, and update job status accordingly.
async fn dispatch_job(pool: &Pool, tx: &ModuleTx, job: ClaimedJob, job_timeout: Duration) {
    let job_id = job.job_id.clone();

    // Build HTTP request: POST /{job_type} with payload body.
    let request = match http::Request::builder()
        .method("POST")
        .uri(format!("http://localhost{}", job.job_type))
        .header("x-wr-job-id", &job.job_id)
        .header("x-wr-timeout", job_timeout.as_secs().to_string())
        .header("content-type", "application/x-protobuf")
        .body(Bytes::from(job.payload))
    {
        Ok(r) => r,
        Err(e) => {
            let msg = format!("build request: {e}");
            warn!(job_id = %job_id, error = %msg, "failed to build job request");
            let _ = fail_job(pool, &job_id, &msg).await;
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
        let _ = fail_job(pool, &job_id, msg).await;
        return;
    }

    // Wait for response with timeout.
    match tokio::time::timeout(job_timeout, resp_rx).await {
        Ok(Ok(resp)) if resp.status().is_success() => {
            let body = resp.into_body();
            if let Err(e) = complete_job(pool, &job_id, body.as_ref()).await {
                error!(job_id = %job_id, error = %e, "failed to mark job complete");
            }
        }
        Ok(Ok(resp)) => {
            let status = resp.status().as_u16();
            let body = String::from_utf8_lossy(resp.body().as_ref()).to_string();
            let msg = format!("HTTP {status}: {body}");
            warn!(job_id = %job_id, status, "job failed");
            let _ = fail_job(pool, &job_id, &msg).await;
        }
        Ok(Err(_)) => {
            let msg = "module dropped response";
            warn!(job_id = %job_id, msg);
            let _ = fail_job(pool, &job_id, msg).await;
        }
        Err(_) => {
            let msg = format!("job timed out after {}s", job_timeout.as_secs());
            warn!(job_id = %job_id, %msg);
            let _ = fail_job(pool, &job_id, &msg).await;
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
) {
    loop {
        tokio::select! {
            _ = notify.notified() => {}
            _ = tokio::time::sleep(poll_interval) => {}
        }

        // Drain: keep claiming until no more pending jobs.
        loop {
            let job = match claim_job(pool, namespace, name, version, engine_id).await {
                Ok(Some(job)) => job,
                Ok(None) => break,
                Err(e) => {
                    warn!(
                        worker_id,
                        namespace,
                        module = name,
                        version,
                        error = %e,
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
                provision_job_schema(&pool).await.expect("provision schema");
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
        let _ = claim_job(&pool, &p, "mod", "1.0.0", "engine-1").await;

        complete_job(&pool, &id, b"result-data")
            .await
            .expect("complete");
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
        let _ = claim_job(&pool, &p, "mod", "1.0.0", "engine-1").await;

        fail_job(&pool, &id, "oops").await.expect("fail");
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
        let _ = claim_job(&pool, &p, "mod", "1.0.0", "engine-1").await;

        fail_job(&pool, &id, "final failure").await.expect("fail");
        let status = get_job_status(&pool, &id).await.unwrap().unwrap();
        assert_eq!(status.status, "dead");
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
                "UPDATE wr__jobs.jobs SET claimed_at = now() - interval '10 seconds' WHERE job_id = $1",
                &[&id],
            )
            .await
            .unwrap();

        let recovered = recover_stale_jobs(&pool).await.expect("recover");
        assert!(recovered >= 1);

        let status = get_job_status(&pool, &id).await.unwrap().unwrap();
        assert_eq!(status.status, "pending");
        assert!(status.error_message.contains("[stale recovery]"));
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

        complete_job(&pool, &id, b"done").await.unwrap();
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

        let _ = claim_job(&pool, &p, "mod", "1.0.0", "e1").await;
        fail_job(&pool, &id, "transient error").await.unwrap();
        let s = get_job_status(&pool, &id).await.unwrap().unwrap();
        assert_eq!(s.status, "pending");
        assert_eq!(s.attempt, 1);

        let _ = claim_job(&pool, &p, "mod", "1.0.0", "e1").await;
        complete_job(&pool, &id, b"ok").await.unwrap();
        let s = get_job_status(&pool, &id).await.unwrap().unwrap();
        assert_eq!(s.status, "complete");
        assert_eq!(s.attempt, 2);
    }
}
