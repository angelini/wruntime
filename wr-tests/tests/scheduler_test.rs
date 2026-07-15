mod helpers;
use helpers::{
    manager::{manager_trio, register_test_module, synced_routing_table},
    proxy::start_proxy,
    worker::spawn_worker_stub_engine,
};

use anyhow::Result;
use wr_common::wruntime::UpsertScheduleRequest;

async fn due_schedule(
    c: &mut wr_common::wruntime::manager_service_client::ManagerServiceClient<
        tonic::transport::Channel,
    >,
    ns: &str,
    name: &str,
    ver: &str,
) -> Result<String> {
    Ok(c.upsert_schedule(UpsertScheduleRequest {
        worker_namespace: ns.into(),
        worker_name: name.into(),
        worker_version: ver.into(),
        job_type: "/Run".into(),
        interval_secs: 300,
        immediate: true,
        payload: vec![],
        timeout_secs: 30,
        max_attempts: 1,
    })
    .await?
    .into_inner()
    .schedule_id)
}

#[tokio::test]
async fn test_no_duplicate_concurrent_claim() -> Result<()> {
    let (pool, _addr, mut c) = manager_trio().await?;

    due_schedule(&mut c, "ns", "mod", "1.0.0").await?;

    let mut cl1 = pool.get().await?;
    let t1 = cl1.transaction().await?;
    let r1 = wr_manager::db::claim_due_schedules(&t1, "m1", 60.0).await?;

    let mut cl2 = pool.get().await?;
    let t2 = cl2.transaction().await?;
    let r2 = wr_manager::db::claim_due_schedules(&t2, "m2", 60.0).await?;

    t1.commit().await?;
    t2.commit().await?;

    assert_eq!(r1.len() + r2.len(), 1);

    Ok(())
}

#[tokio::test]
async fn test_lease_expiry_reclaim() -> Result<()> {
    let (pool, _addr, mut c) = manager_trio().await?;

    let sid = due_schedule(&mut c, "ns", "mod", "1.0.0").await?;

    let mut cl1 = pool.get().await?;
    let t1 = cl1.transaction().await?;
    let r1 = wr_manager::db::claim_due_schedules(&t1, "m1", 60.0).await?;
    t1.commit().await?;
    let claim_a = r1
        .iter()
        .find(|r| r.schedule_id == sid)
        .and_then(|r| r.claim_id.clone())
        .expect("claim_a");

    let raw = pool.get().await?;
    raw.execute(
        "UPDATE wr_schedules SET claimed_until = NOW() - INTERVAL '1 hour', \
         next_fire_at = NOW() - INTERVAL '1 minute' WHERE schedule_id = $1",
        &[&sid],
    )
    .await?;

    let mut cl2 = pool.get().await?;
    let t2 = cl2.transaction().await?;
    let r2 = wr_manager::db::claim_due_schedules(&t2, "m2", 60.0).await?;
    t2.commit().await?;
    let claim_b = r2
        .iter()
        .find(|r| r.schedule_id == sid)
        .and_then(|r| r.claim_id.clone())
        .expect("claim_b");

    assert_ne!(claim_a, claim_b);

    let row = raw
        .query_one(
            "SELECT claimed_by FROM wr_schedules WHERE schedule_id = $1",
            &[&sid],
        )
        .await?;
    let claimed_by: String = row.get(0);
    assert_eq!(claimed_by, "m2");

    Ok(())
}

#[tokio::test]
async fn test_fencing_prevents_stale_finalize() -> Result<()> {
    let (pool, _addr, mut c) = manager_trio().await?;

    let sid = due_schedule(&mut c, "ns", "mod", "1.0.0").await?;

    let mut cl1 = pool.get().await?;
    let t1 = cl1.transaction().await?;
    let r1 = wr_manager::db::claim_due_schedules(&t1, "m1", 60.0).await?;
    t1.commit().await?;
    let claim_a = r1
        .iter()
        .find(|r| r.schedule_id == sid)
        .and_then(|r| r.claim_id.clone())
        .expect("claim_a");

    let raw = pool.get().await?;
    raw.execute(
        "UPDATE wr_schedules SET claimed_until = NOW() - INTERVAL '1 hour', \
         next_fire_at = NOW() - INTERVAL '1 minute' WHERE schedule_id = $1",
        &[&sid],
    )
    .await?;

    let mut cl2 = pool.get().await?;
    let t2 = cl2.transaction().await?;
    let r2 = wr_manager::db::claim_due_schedules(&t2, "m2", 60.0).await?;
    t2.commit().await?;
    let claim_b = r2
        .iter()
        .find(|r| r.schedule_id == sid)
        .and_then(|r| r.claim_id.clone())
        .expect("claim_b");

    let n = wr_manager::db::mark_schedule_succeeded(&pool, &sid, &claim_a).await?;
    assert_eq!(n, 0);

    let row = raw
        .query_one(
            "SELECT claim_id::text, last_fired_at IS NULL FROM wr_schedules WHERE schedule_id = $1",
            &[&sid],
        )
        .await?;
    let claim_id: String = row.get(0);
    let last_fired_at_is_null: bool = row.get(1);
    assert_eq!(claim_id, claim_b);
    assert!(last_fired_at_is_null);

    let n2 = wr_manager::db::mark_schedule_succeeded(&pool, &sid, &claim_b).await?;
    assert_eq!(n2, 1);

    Ok(())
}

#[tokio::test]
async fn test_failed_then_success_state_transitions() -> Result<()> {
    let (pool, _addr, mut c) = manager_trio().await?;

    let sid = due_schedule(&mut c, "ns", "mod", "1.0.0").await?;

    let mut cl = pool.get().await?;
    let txn = cl.transaction().await?;
    let due = wr_manager::db::claim_due_schedules(&txn, "m1", 60.0).await?;
    txn.commit().await?;
    let claim = due
        .iter()
        .find(|r| r.schedule_id == sid)
        .and_then(|r| r.claim_id.clone())
        .expect("claim");

    let n = wr_manager::db::mark_schedule_failed(&pool, &sid, &claim, "boom", 3600.0).await?;
    assert_eq!(n, 1);

    let raw = pool.get().await?;
    let row = raw
        .query_one(
            "SELECT consecutive_failures, last_error, next_fire_at > NOW() \
             FROM wr_schedules WHERE schedule_id = $1",
            &[&sid],
        )
        .await?;
    let consecutive_failures: i32 = row.get(0);
    let last_error: Option<String> = row.get(1);
    let not_yet_due: bool = row.get(2);
    assert_eq!(consecutive_failures, 1);
    assert_eq!(last_error.as_deref(), Some("boom"));
    assert!(not_yet_due);

    let mut cl2 = pool.get().await?;
    let txn2 = cl2.transaction().await?;
    let due2 = wr_manager::db::claim_due_schedules(&txn2, "m2", 60.0).await?;
    txn2.commit().await?;
    assert!(!due2.iter().any(|r| r.schedule_id == sid));

    raw.execute(
        "UPDATE wr_schedules SET next_fire_at = NOW() WHERE schedule_id = $1",
        &[&sid],
    )
    .await?;

    let mut cl3 = pool.get().await?;
    let txn3 = cl3.transaction().await?;
    let due3 = wr_manager::db::claim_due_schedules(&txn3, "m3", 60.0).await?;
    txn3.commit().await?;
    let claim2 = due3
        .iter()
        .find(|r| r.schedule_id == sid)
        .and_then(|r| r.claim_id.clone())
        .expect("claim2");

    let n2 = wr_manager::db::mark_schedule_succeeded(&pool, &sid, &claim2).await?;
    assert_eq!(n2, 1);

    let row = raw
        .query_one(
            "SELECT consecutive_failures, last_error, next_fire_at > NOW() \
             FROM wr_schedules WHERE schedule_id = $1",
            &[&sid],
        )
        .await?;
    let consecutive_failures: i32 = row.get(0);
    let last_error: Option<String> = row.get(1);
    let now_due: bool = row.get(2);
    assert_eq!(consecutive_failures, 0);
    assert!(last_error.is_none());
    assert!(now_due);

    Ok(())
}

#[tokio::test]
async fn test_submit_job_unreachable_proxy_errors() -> Result<()> {
    let (pool, _addr, mut c) = manager_trio().await?;

    let sid = due_schedule(&mut c, "ns", "mod", "1.0.0").await?;

    let mut cl = pool.get().await?;
    let txn = cl.transaction().await?;
    let due = wr_manager::db::claim_due_schedules(&txn, "m1", 60.0).await?;
    txn.commit().await?;
    let row = due.into_iter().find(|r| r.schedule_id == sid).expect("row");

    let res = wr_manager::scheduler::submit_job("http://127.0.0.1:1", &row).await;
    assert!(res.is_err());

    Ok(())
}

async fn capture_scheduler_submit_server() -> Result<(
    std::net::SocketAddr,
    tokio::sync::oneshot::Receiver<(String, Option<String>, String)>,
)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let io = hyper_util::rt::TokioIo::new(stream);
        let tx = std::sync::Arc::new(std::sync::Mutex::new(Some(tx)));
        let svc = hyper::service::service_fn(move |req: http::Request<hyper::body::Incoming>| {
            let tx = tx.clone();
            async move {
                use http_body_util::BodyExt;
                use prost::Message;

                let path = req.uri().path().to_string();
                let header = req
                    .headers()
                    .get("x-wr-version")
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_owned);
                let body = req
                    .into_body()
                    .collect()
                    .await
                    .map(|c| c.to_bytes())
                    .unwrap_or_default();
                let decoded = wr_common::wruntime::SubmitJobRequest::decode(&body[..])
                    .map(|req| req.worker_version)
                    .unwrap_or_default();
                if let Some(tx) = tx.lock().unwrap().take() {
                    let _ = tx.send((path, header, decoded));
                }
                let resp = wr_common::wruntime::SubmitJobResponse {
                    job_id: "job-1".into(),
                };
                Ok::<_, std::convert::Infallible>(
                    http::Response::builder()
                        .status(200)
                        .body(http_body_util::Full::new(bytes::Bytes::from(
                            resp.encode_to_vec(),
                        )))
                        .unwrap(),
                )
            }
        });
        let _ = hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
            .serve_connection(io, svc)
            .await;
    });
    Ok((addr, rx))
}

#[tokio::test]
async fn test_scheduler_submit_job_sends_version_header_and_body() -> Result<()> {
    let (addr, captured) = capture_scheduler_submit_server().await?;
    let row = wr_manager::db::ScheduleRow {
        schedule_id: "sched-1".into(),
        worker_namespace: "sched".into(),
        worker_name: "worker".into(),
        worker_version: "1.2.3".into(),
        job_type: "/Run".into(),
        interval_secs: 300,
        immediate: true,
        payload: vec![],
        timeout_secs: 30,
        max_attempts: 1,
        enabled: true,
        last_fired_at: None,
        next_fire_at: None,
        last_error: None,
        consecutive_failures: 0,
        claim_id: Some("claim".into()),
    };

    wr_manager::scheduler::submit_job(&format!("http://{addr}"), &row)
        .await
        .unwrap();
    let (path, header, body_version) = captured.await.unwrap();
    assert_eq!(path, "/wruntime.WorkerService/SubmitJob");
    assert_eq!(header, Some("1.2.3".to_string()));
    assert_eq!(body_version, "1.2.3");

    Ok(())
}

#[tokio::test]
async fn test_upsert_next_fire_at_due_semantics() -> Result<()> {
    let (pool, _addr, mut c) = manager_trio().await?;

    // Immediate schedule is due now.
    let sid_imm = c
        .upsert_schedule(UpsertScheduleRequest {
            worker_namespace: "ns1".into(),
            worker_name: "mod".into(),
            worker_version: "1.0.0".into(),
            job_type: "/Run".into(),
            interval_secs: 300,
            immediate: true,
            payload: vec![],
            timeout_secs: 30,
            max_attempts: 1,
        })
        .await?
        .into_inner()
        .schedule_id;

    let mut cl1 = pool.get().await?;
    let t1 = cl1.transaction().await?;
    let r1 = wr_manager::db::claim_due_schedules(&t1, "m1", 60.0).await?;
    t1.commit().await?;
    assert!(r1.iter().any(|r| r.schedule_id == sid_imm));

    // Non-immediate schedule is not yet due.
    let sid_ni = c
        .upsert_schedule(UpsertScheduleRequest {
            worker_namespace: "ns2".into(),
            worker_name: "mod".into(),
            worker_version: "1.0.0".into(),
            job_type: "/Run".into(),
            interval_secs: 300,
            immediate: false,
            payload: vec![],
            timeout_secs: 30,
            max_attempts: 1,
        })
        .await?
        .into_inner()
        .schedule_id;

    let mut cl2 = pool.get().await?;
    let t2 = cl2.transaction().await?;
    let r2 = wr_manager::db::claim_due_schedules(&t2, "m2", 60.0).await?;
    t2.commit().await?;
    assert!(!r2.iter().any(|r| r.schedule_id == sid_ni));

    let raw = pool.get().await?;
    let before: String = raw
        .query_one(
            "SELECT next_fire_at::text FROM wr_schedules WHERE schedule_id = $1",
            &[&sid_ni],
        )
        .await?
        .get(0);

    raw.execute(
        "UPDATE wr_schedules SET enabled = FALSE WHERE schedule_id = $1",
        &[&sid_ni],
    )
    .await?;

    c.upsert_schedule(UpsertScheduleRequest {
        worker_namespace: "ns2".into(),
        worker_name: "mod".into(),
        worker_version: "1.0.0".into(),
        job_type: "/Run".into(),
        interval_secs: 60,
        immediate: false,
        payload: vec![],
        timeout_secs: 30,
        max_attempts: 1,
    })
    .await?;

    let row = raw
        .query_one(
            "SELECT enabled, next_fire_at > NOW(), next_fire_at::text FROM wr_schedules WHERE schedule_id = $1",
            &[&sid_ni],
        )
        .await?;
    let enabled: bool = row.get(0);
    let not_yet_due: bool = row.get(1);
    let after: String = row.get(2);
    assert!(enabled);
    assert!(not_yet_due);
    assert_ne!(before, after);

    let mut cl3 = pool.get().await?;
    let t3 = cl3.transaction().await?;
    let r3 = wr_manager::db::claim_due_schedules(&t3, "m3", 60.0).await?;
    t3.commit().await?;
    assert!(!r3.iter().any(|r| r.schedule_id == sid_ni));

    Ok(())
}

#[tokio::test]
async fn test_v9_backfill_due_logic() -> Result<()> {
    let (pool, _addr, _c) = manager_trio().await?;
    let cl = pool.get().await?;

    let sid_imm: String = cl
        .query_one(
            "INSERT INTO wr_schedules
                (worker_namespace, worker_name, worker_version, job_type,
                 interval_secs, immediate, next_fire_at)
             VALUES ('ns', 'mod', '1.0.0', '/A', 3600, TRUE, NULL)
             RETURNING schedule_id",
            &[],
        )
        .await?
        .get(0);

    let sid_ni: String = cl
        .query_one(
            "INSERT INTO wr_schedules
                (worker_namespace, worker_name, worker_version, job_type,
                 interval_secs, immediate, created_at, next_fire_at)
             VALUES ('ns', 'mod2', '1.0.0', '/B', 3600, FALSE, NOW() - INTERVAL '2 hours', NULL)
             RETURNING schedule_id",
            &[],
        )
        .await?
        .get(0);

    let sid_fired: String = cl
        .query_one(
            "INSERT INTO wr_schedules
                (worker_namespace, worker_name, worker_version, job_type,
                 interval_secs, immediate, last_fired_at, next_fire_at)
             VALUES ('ns', 'mod3', '1.0.0', '/C', 3600, FALSE, NOW() - INTERVAL '2 hours', NULL)
             RETURNING schedule_id",
            &[],
        )
        .await?
        .get(0);

    cl.execute(
        "UPDATE wr_schedules
         SET next_fire_at = CASE
             WHEN last_fired_at IS NULL AND immediate THEN NOW()
             WHEN last_fired_at IS NULL             THEN created_at   + make_interval(secs => interval_secs::double precision)
             ELSE                                        last_fired_at + make_interval(secs => interval_secs::double precision)
           END
         WHERE next_fire_at IS NULL",
        &[],
    )
    .await?;

    for sid in [&sid_imm, &sid_ni, &sid_fired] {
        let due: bool = cl
            .query_one(
                "SELECT next_fire_at <= NOW() FROM wr_schedules WHERE schedule_id = $1",
                &[sid],
            )
            .await?
            .get(0);
        assert!(due, "schedule {sid} expected due after V9 backfill");
    }

    Ok(())
}

#[tokio::test]
async fn test_routed_firing_reaches_worker_via_proxy() -> Result<()> {
    let (pool, mgr_addr, mut c) = manager_trio().await?;
    let (engine_addr, _stub_tx) = spawn_worker_stub_engine().await?;

    register_test_module(&mut c, "eng1", &engine_addr, "sched", "worker", "1.0.0").await?;

    let table = synced_routing_table(&mgr_addr).await?;
    let proxy_addr = start_proxy(table).await?;

    let sid = due_schedule(&mut c, "sched", "worker", "1.0.0").await?;

    let mut cl = pool.get().await?;
    let txn = cl.transaction().await?;
    let due = wr_manager::db::claim_due_schedules(&txn, "m1", 60.0).await?;
    txn.commit().await?;
    let row = due
        .into_iter()
        .find(|r| r.schedule_id == sid)
        .expect("claimed");

    let body = wr_manager::scheduler::submit_job(&format!("http://{proxy_addr}"), &row).await?;
    assert!(
        String::from_utf8_lossy(&body).starts_with("processed:/wruntime.WorkerService/SubmitJob:")
    );

    Ok(())
}
