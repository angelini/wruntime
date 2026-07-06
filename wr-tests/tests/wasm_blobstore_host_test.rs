mod helpers;

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use prost::Message;

use helpers::{
    blobstore::{blobstore_client, blobstore_state, blobstore_state_with_limits},
    proto,
    wasm::{GuestHarness, TestGuest},
};

fn unique_prefix(test_name: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("wasm-test/{test_name}/{ts}-{n}")
}

#[tokio::test]
async fn wasm_blobstore_put_get() -> Result<()> {
    let Some(harness) = GuestHarness::load(TestGuest::Blobstore).await? else {
        return Ok(());
    };
    let bs = blobstore_client();
    let key = unique_prefix("put-get");

    // Put
    let state = blobstore_state(bs.clone());
    let req = proto::PutRequest {
        bucket: "test-bucket".into(),
        key: key.clone(),
        data: b"hello wasm blobstore".to_vec(),
    };
    let resp = harness.dispatch(state, "/Put", req).await?;
    assert_eq!(resp.status(), 200);

    // Get
    let state = blobstore_state(bs.clone());
    let req = proto::GetRequest {
        bucket: "test-bucket".into(),
        key: key.clone(),
    };
    let resp = harness.dispatch(state, "/Get", req).await?;
    assert_eq!(resp.status(), 200);

    let body = proto::GetResponse::decode(resp.into_body())?;
    assert_eq!(body.data, b"hello wasm blobstore");
    Ok(())
}

#[tokio::test]
async fn wasm_blobstore_delete() -> Result<()> {
    let Some(harness) = GuestHarness::load(TestGuest::Blobstore).await? else {
        return Ok(());
    };
    let bs = blobstore_client();
    let key = unique_prefix("delete-me");

    // Put first
    let state = blobstore_state(bs.clone());
    let req = proto::PutRequest {
        bucket: "test-bucket".into(),
        key: key.clone(),
        data: b"temp".to_vec(),
    };
    let resp = harness.dispatch(state, "/Put", req).await?;
    assert_eq!(resp.status(), 200);

    // Delete
    let state = blobstore_state(bs.clone());
    let req = proto::DeleteRequest {
        bucket: "test-bucket".into(),
        key: key.clone(),
    };
    let resp = harness.dispatch(state, "/Delete", req).await?;
    assert_eq!(resp.status(), 200);

    // Verify deleted — get should fail
    let state = blobstore_state(bs.clone());
    let req = proto::NotFoundRequest {
        bucket: "test-bucket".into(),
        key: key.clone(),
    };
    let resp = harness.dispatch(state, "/NotFound", req).await?;
    assert_eq!(resp.status(), 200);

    let body = proto::NotFoundResponse::decode(resp.into_body())?;
    assert_eq!(body.error_kind, "not-found");
    Ok(())
}

#[tokio::test]
async fn wasm_blobstore_list() -> Result<()> {
    let Some(harness) = GuestHarness::load(TestGuest::Blobstore).await? else {
        return Ok(());
    };
    let bs = blobstore_client();
    let prefix = unique_prefix("list");

    // Put 3 objects with a common prefix
    for i in 0..3 {
        let state = blobstore_state(bs.clone());
        let req = proto::PutRequest {
            bucket: "test-bucket".into(),
            key: format!("{prefix}/item-{i}"),
            data: format!("data-{i}").into_bytes(),
        };
        let resp = harness.dispatch(state, "/Put", req).await?;
        assert_eq!(resp.status(), 200);
    }

    // List with prefix
    let state = blobstore_state(bs.clone());
    let req = proto::ListRequest {
        bucket: "test-bucket".into(),
        prefix: format!("{prefix}/"),
    };
    let resp = harness.dispatch(state, "/List", req).await?;
    assert_eq!(resp.status(), 200);

    let body = proto::ListResponse::decode(resp.into_body())?;
    assert_eq!(
        body.objects.len(),
        3,
        "expected exactly 3 objects, got {}",
        body.objects.len()
    );
    Ok(())
}

#[tokio::test]
async fn wasm_blobstore_head() -> Result<()> {
    let Some(harness) = GuestHarness::load(TestGuest::Blobstore).await? else {
        return Ok(());
    };
    let bs = blobstore_client();

    let key = unique_prefix("head-obj");
    let data = b"head-test-data";
    let state = blobstore_state(bs.clone());
    let req = proto::PutRequest {
        bucket: "test-bucket".into(),
        key: key.clone(),
        data: data.to_vec(),
    };
    let resp = harness.dispatch(state, "/Put", req).await?;
    assert_eq!(resp.status(), 200);

    let state = blobstore_state(bs.clone());
    let req = proto::HeadRequest {
        bucket: "test-bucket".into(),
        key: key.clone(),
    };
    let resp = harness.dispatch(state, "/Head", req).await?;
    assert_eq!(resp.status(), 200);

    let body = proto::HeadResponse::decode(resp.into_body())?;
    assert_eq!(body.key, key);
    assert_eq!(body.size, data.len() as u64);
    Ok(())
}

#[tokio::test]
async fn wasm_blobstore_round_trip() -> Result<()> {
    let Some(harness) = GuestHarness::load(TestGuest::Blobstore).await? else {
        return Ok(());
    };
    let bs = blobstore_client();
    let key = unique_prefix("round-trip");
    let state = blobstore_state(bs.clone());

    let req = proto::RoundTripRequest {
        bucket: "test-bucket".into(),
        key,
        data: b"round-trip-payload".to_vec(),
    };
    let resp = harness.dispatch(state, "/RoundTrip", req).await?;
    assert_eq!(resp.status(), 200);

    let body = proto::RoundTripResponse::decode(resp.into_body())?;
    assert!(body.matches);
    assert_eq!(body.data, b"round-trip-payload");
    Ok(())
}

#[tokio::test]
async fn wasm_blobstore_not_found() -> Result<()> {
    let Some(harness) = GuestHarness::load(TestGuest::Blobstore).await? else {
        return Ok(());
    };
    let bs = blobstore_client();
    let state = blobstore_state(bs.clone());

    let req = proto::NotFoundRequest {
        bucket: "test-bucket".into(),
        key: unique_prefix("nonexistent"),
    };
    let resp = harness.dispatch(state, "/NotFound", req).await?;
    assert_eq!(resp.status(), 200);

    let body = proto::NotFoundResponse::decode(resp.into_body())?;
    assert_eq!(body.error_kind, "not-found");
    Ok(())
}

#[tokio::test]
async fn wasm_blobstore_put_too_large_rejected() -> Result<()> {
    let Some(harness) = GuestHarness::load(TestGuest::Blobstore).await? else {
        return Ok(());
    };
    let bs = blobstore_client();
    let key = unique_prefix("put-too-large");
    let limits = wr_engine::config::BlobstoreLimits {
        max_object_size: 1024,
        ..wr_engine::config::BlobstoreLimits::default()
    };

    // Exactly at cap (1024 bytes) is accepted.
    let state = blobstore_state_with_limits(bs.clone(), limits);
    let req = proto::PutRequest {
        bucket: "test-bucket".into(),
        key: format!("{key}/at-cap"),
        data: vec![b'x'; 1024],
    };
    let resp = harness.dispatch(state, "/Put", req).await?;
    assert_eq!(resp.status(), 200, "at-cap upload should be accepted");

    // One byte over cap is rejected with too-large.
    let state = blobstore_state_with_limits(bs.clone(), limits);
    let req = proto::PutRequest {
        bucket: "test-bucket".into(),
        key: format!("{key}/over-cap"),
        data: vec![b'x'; 1025],
    };
    let resp = harness.dispatch(state, "/Put", req).await?;
    assert_ne!(resp.status(), 200, "over-cap upload should be rejected");
    let body = String::from_utf8_lossy(&resp.into_body()).into_owned();
    assert!(
        body.contains("too large"),
        "expected too-large error, got: {body}"
    );
    Ok(())
}

#[tokio::test]
async fn wasm_blobstore_get_too_large_rejected() -> Result<()> {
    let Some(harness) = GuestHarness::load(TestGuest::Blobstore).await? else {
        return Ok(());
    };
    let bs = blobstore_client();
    let key = unique_prefix("get-too-large");

    // Store a 2 KiB object using a default-limit state (upload allowed).
    let state = blobstore_state(bs.clone());
    let req = proto::PutRequest {
        bucket: "test-bucket".into(),
        key: key.clone(),
        data: vec![b'y'; 2048],
    };
    let resp = harness.dispatch(state, "/Put", req).await?;
    assert_eq!(resp.status(), 200);

    // Download it under a 1 KiB cap → rejected mid-stream with too-large.
    let limits = wr_engine::config::BlobstoreLimits {
        max_object_size: 1024,
        ..wr_engine::config::BlobstoreLimits::default()
    };
    let state = blobstore_state_with_limits(bs.clone(), limits);
    let req = proto::GetRequest {
        bucket: "test-bucket".into(),
        key: key.clone(),
    };
    let resp = harness.dispatch(state, "/Get", req).await?;
    assert_ne!(resp.status(), 200, "over-cap download should be rejected");
    let body = String::from_utf8_lossy(&resp.into_body()).into_owned();
    assert!(
        body.contains("too large"),
        "expected too-large error, got: {body}"
    );
    Ok(())
}

#[tokio::test]
async fn wasm_blobstore_list_too_large_rejected() -> Result<()> {
    let Some(harness) = GuestHarness::load(TestGuest::Blobstore).await? else {
        return Ok(());
    };
    let bs = blobstore_client();
    let prefix = unique_prefix("list-cap");

    // Seed 3 objects under a shared prefix (default-limit state).
    for i in 0..3 {
        let state = blobstore_state(bs.clone());
        let req = proto::PutRequest {
            bucket: "test-bucket".into(),
            key: format!("{prefix}/item-{i}"),
            data: format!("data-{i}").into_bytes(),
        };
        let resp = harness.dispatch(state, "/Put", req).await?;
        assert_eq!(resp.status(), 200);
    }

    // Cap = 3 → all 3 returned (at-cap ok).
    let limits_ok = wr_engine::config::BlobstoreLimits {
        max_list_objects: 3,
        ..wr_engine::config::BlobstoreLimits::default()
    };
    let state = blobstore_state_with_limits(bs.clone(), limits_ok);
    let req = proto::ListRequest {
        bucket: "test-bucket".into(),
        prefix: format!("{prefix}/"),
    };
    let resp = harness.dispatch(state, "/List", req).await?;
    assert_eq!(resp.status(), 200, "listing at cap should succeed");
    let body = proto::ListResponse::decode(resp.into_body())?;
    assert_eq!(body.objects.len(), 3);

    // Cap = 2 → over cap → rejected with too-large.
    let limits_over = wr_engine::config::BlobstoreLimits {
        max_list_objects: 2,
        ..wr_engine::config::BlobstoreLimits::default()
    };
    let state = blobstore_state_with_limits(bs.clone(), limits_over);
    let req = proto::ListRequest {
        bucket: "test-bucket".into(),
        prefix: format!("{prefix}/"),
    };
    let resp = harness.dispatch(state, "/List", req).await?;
    assert_ne!(resp.status(), 200, "listing over cap should be rejected");
    let body = String::from_utf8_lossy(&resp.into_body()).into_owned();
    assert!(
        body.contains("too large"),
        "expected too-large error, got: {body}"
    );
    Ok(())
}
