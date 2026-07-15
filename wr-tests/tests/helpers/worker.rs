use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Result};
use bytes::Bytes;
use deadpool_postgres::Pool;
use http::{Request, Response};
use http_body_util::{BodyExt, Full};
use hyper::server::conn::http2;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use wr_engine::worker::JobStatus;

use super::db::require_db_url;

pub async fn worker_pool() -> deadpool_postgres::Pool {
    use tokio::sync::OnceCell;
    static PROVISIONED: OnceCell<()> = OnceCell::const_new();

    let url = require_db_url();
    let pool = wr_engine::pool::build_pool(&url, 2)
        .expect("WRT_TEST_DB_URL is set but unusable: failed to build worker test pool");

    PROVISIONED
        .get_or_init(|| async {
            wr_engine::worker::provision_job_schema(&pool)
                .await
                .expect("provision wr__jobs");
        })
        .await;

    pool
}

static WORKER_TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn unique_worker_namespace(_test_name: &str) -> String {
    let n = WORKER_TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_nanos() as u64;
    format!("wt{:016x}", ts ^ n.rotate_left(32))
}

pub struct WorkerPoolHarness {
    pub pool: Arc<Pool>,
    pub namespace: String,
    pub name: String,
    pub version: String,
    pub engine_id: String,
    db_url: String,
    tx: mpsc::Sender<wr_engine::InboundRequest>,
    rx: mpsc::Receiver<wr_engine::InboundRequest>,
}

impl WorkerPoolHarness {
    pub async fn new(test_name: &str, name: &str) -> Option<Self> {
        if super::db::skip_without_db(test_name) {
            return None;
        }
        let db_url = super::db::require_db_url();
        let pool = Arc::new(worker_pool().await);
        let (tx, rx) = mpsc::channel::<wr_engine::InboundRequest>(32);
        Some(Self {
            pool,
            namespace: unique_worker_namespace(test_name),
            name: name.to_string(),
            version: "1.0.0".to_string(),
            engine_id: format!("{test_name}-engine"),
            db_url,
            tx,
            rx,
        })
    }

    pub async fn insert_job(
        &self,
        job_type: &str,
        payload: impl AsRef<[u8]>,
        timeout_secs: i32,
        max_attempts: i32,
    ) -> Result<String> {
        wr_engine::worker::insert_job(
            &self.pool,
            &self.namespace,
            &self.name,
            &self.version,
            job_type,
            payload.as_ref(),
            timeout_secs,
            max_attempts,
            "",
            "",
        )
        .await
    }

    pub async fn insert_job_with_source(
        &self,
        job_type: &str,
        payload: impl AsRef<[u8]>,
        timeout_secs: i32,
        max_attempts: i32,
        source_namespace: &str,
        source_module: &str,
    ) -> Result<String> {
        wr_engine::worker::insert_job(
            &self.pool,
            &self.namespace,
            &self.name,
            &self.version,
            job_type,
            payload.as_ref(),
            timeout_secs,
            max_attempts,
            source_namespace,
            source_module,
        )
        .await
    }

    pub fn spawn(&self, concurrency: usize, poll_interval: Duration, job_timeout: Duration) {
        wr_engine::worker::spawn_worker_pool(
            self.pool.clone(),
            wr_engine::worker::WorkerPoolConfig {
                namespace: self.namespace.clone(),
                name: self.name.clone(),
                version: self.version.clone(),
                engine_id: self.engine_id.clone(),
                concurrency,
                poll_interval,
                job_timeout,
                database_url: self.db_url.clone(),
            },
            self.tx.clone(),
        );
    }

    pub async fn wait_for_listener(&self, timeout: Duration) -> Result<()> {
        let channel = format!("wr_jobs_{}_{}_{}", self.namespace, self.name, self.version);
        let expected_query = format!("%LISTEN \"{channel}\"%");
        super::wait::eventually(
            format!("worker LISTEN active for {channel}"),
            timeout,
            super::wait::DEFAULT_POLL_INTERVAL,
            || {
                let pool = self.pool.clone();
                let expected_query = expected_query.clone();
                async move {
                    let client = pool.get().await?;
                    let row = client
                        .query_opt(
                            "SELECT 1 FROM pg_stat_activity WHERE query LIKE $1 LIMIT 1",
                            &[&expected_query],
                        )
                        .await?;
                    Ok(row.map(|_| ()))
                }
            },
        )
        .await
    }

    pub async fn recv_dispatch(&mut self, timeout: Duration) -> Result<wr_engine::InboundRequest> {
        tokio::time::timeout(timeout, self.rx.recv())
            .await
            .map_err(|_| anyhow::anyhow!("timeout waiting for worker dispatch"))?
            .ok_or_else(|| anyhow::anyhow!("worker dispatch channel closed"))
    }

    pub async fn expect_no_dispatch(&mut self, timeout: Duration) -> Result<()> {
        match tokio::time::timeout(timeout, self.rx.recv()).await {
            Err(_) => Ok(()),
            Ok(None) => bail!("worker dispatch channel closed"),
            Ok(Some(inbound)) => bail!(
                "unexpected worker dispatch for {}",
                inbound.request.uri().path()
            ),
        }
    }

    pub fn respond(inbound: wr_engine::InboundRequest, status: u16, body: impl Into<Bytes>) {
        let response = Response::builder()
            .status(status)
            .body(body.into())
            .expect("build worker response");
        inbound
            .response_tx
            .send(response)
            .expect("send worker response");
    }

    pub async fn wait_for_status(
        &self,
        job_id: &str,
        expected_status: &str,
        timeout: Duration,
    ) -> Result<JobStatus> {
        super::wait::wait_for_job_status(&self.pool, job_id, expected_status, timeout).await
    }

    pub async fn wait_for_status_matching(
        &self,
        job_id: &str,
        context: &str,
        timeout: Duration,
        predicate: impl Fn(&JobStatus) -> bool,
    ) -> Result<JobStatus> {
        let started = std::time::Instant::now();
        loop {
            if let Some(status) = wr_engine::worker::get_job_status(&self.pool, job_id).await? {
                if predicate(&status) {
                    return Ok(status);
                }
            }
            if started.elapsed() >= timeout {
                bail!(
                    "timed out after {:?} waiting for {context}",
                    started.elapsed()
                );
            }
            tokio::time::sleep(super::wait::DEFAULT_POLL_INTERVAL).await;
        }
    }
}

/// Spawn a stub engine that processes worker job requests.
///
/// For each inbound request, the stub reads the path (job_type) and body (payload),
/// then responds with 200 OK and the body `"processed:{path}:{payload_len}"`.
/// If the path contains "fail", responds with 500 instead.
pub async fn spawn_worker_stub_engine() -> Result<(String, oneshot::Sender<()>)> {
    let (tx, rx) = oneshot::channel::<()>();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = format!("http://{}", listener.local_addr()?);
    tokio::spawn(async move {
        tokio::select! {
            _ = rx => {}
            _ = worker_stub_engine(listener) => {}
        }
    });
    Ok((addr, tx))
}

async fn worker_stub_engine(listener: TcpListener) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            break;
        };
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc =
                hyper::service::service_fn(|req: Request<hyper::body::Incoming>| async move {
                    let path = req.uri().path().to_string();
                    let status = if path.contains("fail") { 500 } else { 200 };
                    let body_bytes = BodyExt::collect(req.into_body())
                        .await
                        .map(|c| c.to_bytes())
                        .unwrap_or_default();
                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(status)
                            .body(Full::new(Bytes::from(format!(
                                "processed:{}:{}",
                                path,
                                body_bytes.len()
                            ))))
                            .unwrap(),
                    )
                });
            let _ = http2::Builder::new(TokioExecutor::new())
                .serve_connection(io, svc)
                .await;
        });
    }
}
