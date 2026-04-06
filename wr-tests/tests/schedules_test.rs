#[allow(dead_code, unused_imports)]
mod helpers;
use helpers::*;

use anyhow::Result;

use wr_common::wruntime::{DeleteScheduleRequest, ListSchedulesRequest, UpsertScheduleRequest};

#[tokio::test]
async fn test_upsert_and_list_schedules() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    // Upsert two schedules.
    let resp = c
        .upsert_schedule(UpsertScheduleRequest {
            worker_namespace: "codegen".into(),
            worker_name: "worker".into(),
            worker_version: "1.0.0".into(),
            job_type: "/Cleanup/Run".into(),
            interval_secs: 300,
            immediate: false,
            payload: vec![],
            timeout_secs: 60,
            max_attempts: 3,
        })
        .await?
        .into_inner();
    assert!(!resp.schedule_id.is_empty());

    c.upsert_schedule(UpsertScheduleRequest {
        worker_namespace: "codegen".into(),
        worker_name: "worker".into(),
        worker_version: "1.0.0".into(),
        job_type: "/Index/Rebuild".into(),
        interval_secs: 600,
        immediate: true,
        payload: b"rebuild-all".to_vec(),
        timeout_secs: 120,
        max_attempts: 1,
    })
    .await?;

    // A schedule in a different namespace.
    c.upsert_schedule(UpsertScheduleRequest {
        worker_namespace: "payments".into(),
        worker_name: "reconciler".into(),
        worker_version: "2.0.0".into(),
        job_type: "/Reconcile".into(),
        interval_secs: 3600,
        immediate: false,
        payload: vec![],
        timeout_secs: 300,
        max_attempts: 5,
    })
    .await?;

    // List all — should see 3.
    let resp = c
        .list_schedules(ListSchedulesRequest {
            worker_namespace: String::new(),
        })
        .await?
        .into_inner();
    assert_eq!(resp.schedules.len(), 3);

    // List by namespace — should see only 2 codegen schedules.
    let resp = c
        .list_schedules(ListSchedulesRequest {
            worker_namespace: "codegen".into(),
        })
        .await?
        .into_inner();
    assert_eq!(resp.schedules.len(), 2);
    assert!(resp
        .schedules
        .iter()
        .all(|s| s.worker_namespace == "codegen"));

    // List nonexistent namespace — empty.
    let resp = c
        .list_schedules(ListSchedulesRequest {
            worker_namespace: "nonexistent".into(),
        })
        .await?
        .into_inner();
    assert!(resp.schedules.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_upsert_schedule_is_idempotent() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    let req = UpsertScheduleRequest {
        worker_namespace: "ns".into(),
        worker_name: "mod".into(),
        worker_version: "1.0.0".into(),
        job_type: "/Run".into(),
        interval_secs: 60,
        immediate: false,
        payload: vec![],
        timeout_secs: 30,
        max_attempts: 1,
    };

    let id1 = c
        .upsert_schedule(req.clone())
        .await?
        .into_inner()
        .schedule_id;

    // Upsert again with different interval — same schedule_id, updated fields.
    let mut req2 = req.clone();
    req2.interval_secs = 120;
    let id2 = c.upsert_schedule(req2).await?.into_inner().schedule_id;
    assert_eq!(id1, id2);

    // Should still be exactly one schedule.
    let resp = c
        .list_schedules(ListSchedulesRequest {
            worker_namespace: "ns".into(),
        })
        .await?
        .into_inner();
    assert_eq!(resp.schedules.len(), 1);
    assert_eq!(resp.schedules[0].interval_secs, 120);

    Ok(())
}

#[tokio::test]
async fn test_delete_schedule() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    c.upsert_schedule(UpsertScheduleRequest {
        worker_namespace: "ns".into(),
        worker_name: "mod".into(),
        worker_version: "1.0.0".into(),
        job_type: "/Task".into(),
        interval_secs: 60,
        immediate: false,
        payload: vec![],
        timeout_secs: 30,
        max_attempts: 1,
    })
    .await?;

    // Verify it exists.
    let resp = c
        .list_schedules(ListSchedulesRequest {
            worker_namespace: "ns".into(),
        })
        .await?
        .into_inner();
    assert_eq!(resp.schedules.len(), 1);

    // Delete it.
    c.delete_schedule(DeleteScheduleRequest {
        worker_namespace: "ns".into(),
        worker_name: "mod".into(),
        worker_version: "1.0.0".into(),
        job_type: "/Task".into(),
    })
    .await?;

    // Verify it's gone.
    let resp = c
        .list_schedules(ListSchedulesRequest {
            worker_namespace: "ns".into(),
        })
        .await?
        .into_inner();
    assert!(resp.schedules.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_delete_nonexistent_schedule_succeeds() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    // Deleting a schedule that was never created should not error.
    c.delete_schedule(DeleteScheduleRequest {
        worker_namespace: "ns".into(),
        worker_name: "mod".into(),
        worker_version: "1.0.0".into(),
        job_type: "/Never".into(),
    })
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_upsert_schedule_empty_fields_rejected() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    // Empty namespace.
    let result = c
        .upsert_schedule(UpsertScheduleRequest {
            worker_namespace: String::new(),
            worker_name: "mod".into(),
            worker_version: "1.0.0".into(),
            job_type: "/Run".into(),
            interval_secs: 60,
            ..Default::default()
        })
        .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);

    // Empty name.
    let result = c
        .upsert_schedule(UpsertScheduleRequest {
            worker_namespace: "ns".into(),
            worker_name: String::new(),
            worker_version: "1.0.0".into(),
            job_type: "/Run".into(),
            interval_secs: 60,
            ..Default::default()
        })
        .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);

    // Empty version.
    let result = c
        .upsert_schedule(UpsertScheduleRequest {
            worker_namespace: "ns".into(),
            worker_name: "mod".into(),
            worker_version: String::new(),
            job_type: "/Run".into(),
            interval_secs: 60,
            ..Default::default()
        })
        .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);

    // Empty job_type.
    let result = c
        .upsert_schedule(UpsertScheduleRequest {
            worker_namespace: "ns".into(),
            worker_name: "mod".into(),
            worker_version: "1.0.0".into(),
            job_type: String::new(),
            interval_secs: 60,
            ..Default::default()
        })
        .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);

    Ok(())
}

#[tokio::test]
async fn test_upsert_schedule_zero_interval_rejected() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    let result = c
        .upsert_schedule(UpsertScheduleRequest {
            worker_namespace: "ns".into(),
            worker_name: "mod".into(),
            worker_version: "1.0.0".into(),
            job_type: "/Run".into(),
            interval_secs: 0,
            ..Default::default()
        })
        .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);

    Ok(())
}

#[tokio::test]
async fn test_delete_schedule_empty_fields_rejected() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    let result = c
        .delete_schedule(DeleteScheduleRequest {
            worker_namespace: String::new(),
            worker_name: "mod".into(),
            worker_version: "1.0.0".into(),
            job_type: "/Run".into(),
        })
        .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);

    let result = c
        .delete_schedule(DeleteScheduleRequest {
            worker_namespace: "ns".into(),
            worker_name: String::new(),
            worker_version: "1.0.0".into(),
            job_type: "/Run".into(),
        })
        .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);

    Ok(())
}

#[tokio::test]
async fn test_schedule_fields_preserved() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    c.upsert_schedule(UpsertScheduleRequest {
        worker_namespace: "ns".into(),
        worker_name: "mod".into(),
        worker_version: "2.0.0".into(),
        job_type: "/Process".into(),
        interval_secs: 120,
        immediate: true,
        payload: b"hello".to_vec(),
        timeout_secs: 45,
        max_attempts: 7,
    })
    .await?;

    let resp = c
        .list_schedules(ListSchedulesRequest {
            worker_namespace: "ns".into(),
        })
        .await?
        .into_inner();
    assert_eq!(resp.schedules.len(), 1);

    let s = &resp.schedules[0];
    assert!(!s.schedule_id.is_empty());
    assert_eq!(s.worker_namespace, "ns");
    assert_eq!(s.worker_name, "mod");
    assert_eq!(s.worker_version, "2.0.0");
    assert_eq!(s.job_type, "/Process");
    assert_eq!(s.interval_secs, 120);
    assert!(s.immediate);
    assert_eq!(s.payload, b"hello");
    assert_eq!(s.timeout_secs, 45);
    assert_eq!(s.max_attempts, 7);
    assert!(s.enabled);
    assert!(s.last_fired_at.is_empty());

    Ok(())
}
