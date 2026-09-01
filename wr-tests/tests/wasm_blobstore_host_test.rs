mod helpers;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use http::{Method, Request, StatusCode};
use http_body_util::{BodyExt, Full};
use prost::Message;
use wr_engine::blobstore::BlobstoreRuntime;

use helpers::{
    blobstore::{
        blobstore_client, blobstore_state, blobstore_state_for_namespace,
        blobstore_state_with_limits,
    },
    proto,
    proxy::{http_client, http_pool, start_egress_proxy, EgressConfig, TEST_SELF_PEER},
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

async fn put_blob(
    harness: &GuestHarness,
    blobstore: Arc<BlobstoreRuntime>,
    key: &str,
    data: &[u8],
) -> Result<()> {
    let response = harness
        .dispatch(
            blobstore_state(blobstore),
            "/Put",
            proto::PutRequest {
                bucket: "test-bucket".into(),
                key: key.into(),
                data: data.to_vec(),
            },
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    Ok(())
}

async fn issue_download_url(
    harness: &GuestHarness,
    blobstore: Arc<BlobstoreRuntime>,
    key: &str,
    expires_in_seconds: u32,
) -> Result<proto::CreateDownloadUrlResponse> {
    let response = harness
        .dispatch(
            blobstore_state(blobstore),
            "/CreateDownloadUrl",
            proto::CreateDownloadUrlRequest {
                bucket: "test-bucket".into(),
                key: key.into(),
                expires_in_seconds,
            },
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    Ok(proto::CreateDownloadUrlResponse::decode(
        response.into_body(),
    )?)
}

async fn egress_signed_request(
    proxy_addr: SocketAddr,
    method: Method,
    destination: &str,
    body: &[u8],
) -> Result<(StatusCode, Vec<u8>)> {
    let request = Request::builder()
        .method(method)
        .uri(format!("http://{proxy_addr}/"))
        .header("x-wr-destination", destination)
        .header("x-wr-source", "signed-url-test")
        .body(Full::new(Bytes::copy_from_slice(body)))
        .map_err(|_| anyhow!("failed to build signed URL test request"))?;
    let response = http_client()
        .request(request)
        .await
        .map_err(|_| anyhow!("signed URL test request failed"))?;
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .map_err(|_| anyhow!("failed to collect signed URL test response"))?
        .to_bytes()
        .to_vec();
    Ok((status, body))
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
        key: format!("{key}//./object"),
        data: b"hello wasm blobstore".to_vec(),
    };
    let resp = harness.dispatch(state, "/Put", req).await?;
    assert_eq!(resp.status(), 200);

    // Get
    let state = blobstore_state(bs.clone());
    let req = proto::GetRequest {
        bucket: "test-bucket".into(),
        key: format!("{key}/object"),
    };
    let resp = harness.dispatch(state, "/Get", req).await?;
    assert_eq!(resp.status(), 200);

    let body = proto::GetResponse::decode(resp.into_body())?;
    assert_eq!(body.data, b"hello wasm blobstore");
    Ok(())
}

#[tokio::test]
async fn wasm_blobstore_create_download_url_for_namespace_scoped_missing_key() -> Result<()> {
    let Some(harness) = GuestHarness::load(TestGuest::Blobstore).await? else {
        return Ok(());
    };
    let requested_at = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let response = harness
        .dispatch(
            blobstore_state(blobstore_client()),
            "/CreateDownloadUrl",
            proto::CreateDownloadUrlRequest {
                bucket: "test-bucket".into(),
                key: unique_prefix("signed-missing"),
                expires_in_seconds: 300,
            },
        )
        .await?;
    assert_eq!(response.status(), 200);
    let link = proto::CreateDownloadUrlResponse::decode(response.into_body())?;
    let uri: http::Uri = link
        .url
        .parse()
        .map_err(|_| anyhow!("signer returned an invalid URL"))?;
    let configured_endpoint: http::Uri = std::env::var("WRT_TEST_S3_ENDPOINT")?.parse()?;
    assert_eq!(uri.scheme_str(), configured_endpoint.scheme_str());
    assert_eq!(uri.authority(), configured_endpoint.authority());
    assert!(uri.path().contains("/test-bucket/wr/test-ns/wasm-test/"));
    assert!(uri.query().is_some_and(|query| {
        query
            .split('&')
            .any(|part| part.eq_ignore_ascii_case("X-Amz-Expires=300"))
    }));
    let completed_at = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    assert!((requested_at + 300..=completed_at + 300).contains(&link.expires_at));
    Ok(())
}

#[tokio::test]
async fn wasm_blobstore_signed_url_downloads_through_egress_across_namespaces() -> Result<()> {
    let Some(blob_harness) = GuestHarness::load(TestGuest::Blobstore).await? else {
        return Ok(());
    };
    let Some(http_harness) = GuestHarness::load(TestGuest::Http).await? else {
        return Ok(());
    };
    let blobstore = blobstore_client();
    let key = unique_prefix("signed-cross-namespace");
    let expected = b"direct bearer download".to_vec();

    let put = blob_harness
        .dispatch(
            blobstore_state(blobstore.clone()),
            "/Put",
            proto::PutRequest {
                bucket: "test-bucket".into(),
                key: key.clone(),
                data: expected.clone(),
            },
        )
        .await?;
    assert_eq!(put.status(), 200);

    let issued = blob_harness
        .dispatch(
            blobstore_state(blobstore.clone()),
            "/CreateDownloadUrl",
            proto::CreateDownloadUrlRequest {
                bucket: "test-bucket".into(),
                key: key.clone(),
                expires_in_seconds: 300,
            },
        )
        .await?;
    assert_eq!(issued.status(), 200);
    let link = proto::CreateDownloadUrlResponse::decode(issued.into_body())?;
    let signed_uri: http::Uri = link
        .url
        .parse()
        .map_err(|_| anyhow!("signer returned an invalid URL"))?;
    let signed_host = signed_uri
        .host()
        .ok_or_else(|| anyhow!("signed URL has no hostname"))?
        .to_string();

    let table = wr_proxy::routing::new_routing_table(Default::default(), TEST_SELF_PEER);
    let proxy_addr = start_egress_proxy(
        Some(EgressConfig {
            allowed_domains: vec![signed_host],
        }),
        table,
    )
    .await?;
    let recipient = wr_engine::state::ModuleState::new(
        "http-recipient".into(),
        "recipient-ns".into(),
        format!("http://{proxy_addr}").parse()?,
        http_pool(),
        Default::default(),
    )?;
    let fetched = http_harness
        .dispatch(recipient, "/GetUrl", proto::GetUrlRequest { url: link.url })
        .await?;
    assert_eq!(fetched.status(), 200);
    let fetched = proto::GetUrlResponse::decode(fetched.into_body())?;
    assert_eq!(fetched.status, 200);
    assert_eq!(fetched.body, expected);

    let isolated = blob_harness
        .dispatch(
            blobstore_state_for_namespace(blobstore, "recipient-ns"),
            "/Get",
            proto::GetRequest {
                bucket: "test-bucket".into(),
                key,
            },
        )
        .await?;
    assert_eq!(isolated.status(), 404);
    Ok(())
}

#[tokio::test]
async fn wasm_blobstore_signed_url_enforces_signature_expiry_and_key_mutations() -> Result<()> {
    let Some(harness) = GuestHarness::load(TestGuest::Blobstore).await? else {
        return Ok(());
    };
    let blobstore = blobstore_client();
    let key = unique_prefix("signed-bearer-behavior");
    put_blob(&harness, blobstore.clone(), &key, b"original").await?;
    let link = issue_download_url(&harness, blobstore.clone(), &key, 60).await?;
    let signed_uri: http::Uri = link
        .url
        .parse()
        .map_err(|_| anyhow!("signer returned an invalid URL"))?;
    let signed_host = signed_uri
        .host()
        .ok_or_else(|| anyhow!("signed URL has no hostname"))?
        .to_string();
    let table = wr_proxy::routing::new_routing_table(Default::default(), TEST_SELF_PEER);
    let proxy_addr = start_egress_proxy(
        Some(EgressConfig {
            allowed_domains: vec![signed_host],
        }),
        table,
    )
    .await?;

    for _ in 0..2 {
        let (status, body) = egress_signed_request(proxy_addr, Method::GET, &link.url, &[]).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, b"original");
    }

    let (status, _) =
        egress_signed_request(proxy_addr, Method::PUT, &link.url, b"tampered").await?;
    assert!(!status.is_success(), "a GET signature must reject PUT");

    let (base, query) = link
        .url
        .split_once('?')
        .ok_or_else(|| anyhow!("signed URL has no query"))?;
    let tampered_path = format!("{base}-tampered?{query}");
    let (status, _) = egress_signed_request(proxy_addr, Method::GET, &tampered_path, &[]).await?;
    assert!(
        !status.is_success(),
        "a signature must reject path tampering"
    );
    let tampered_query = format!("{}&unrelated-secret=tampered", link.url);
    let (status, _) = egress_signed_request(proxy_addr, Method::GET, &tampered_query, &[]).await?;
    assert!(
        !status.is_success(),
        "a signature must reject query tampering"
    );

    put_blob(&harness, blobstore.clone(), &key, b"overwritten").await?;
    let (status, body) = egress_signed_request(proxy_addr, Method::GET, &link.url, &[]).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"overwritten");

    let deleted = harness
        .dispatch(
            blobstore_state(blobstore.clone()),
            "/Delete",
            proto::DeleteRequest {
                bucket: "test-bucket".into(),
                key: key.clone(),
            },
        )
        .await?;
    assert_eq!(deleted.status(), StatusCode::OK);
    let (status, _) = egress_signed_request(proxy_addr, Method::GET, &link.url, &[]).await?;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let expiry_key = unique_prefix("signed-expiry");
    put_blob(&harness, blobstore.clone(), &expiry_key, b"expires").await?;
    let expiring = issue_download_url(&harness, blobstore, &expiry_key, 1).await?;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let (status, _) = egress_signed_request(proxy_addr, Method::GET, &expiring.url, &[]).await?;
    assert!(
        !status.is_success(),
        "an expired signature must be rejected"
    );
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
async fn wasm_blobstore_facade_rejects_invalid_names_before_host_calls() -> Result<()> {
    let Some(harness) = GuestHarness::load(TestGuest::Blobstore).await? else {
        return Ok(());
    };
    for (bucket, key) in [
        ("Bad_Bucket", "valid/key"),
        ("test-bucket", "../secret"),
        ("test-bucket", ""),
    ] {
        let response = harness
            .dispatch(
                blobstore_state(blobstore_client()),
                "/Put",
                proto::PutRequest {
                    bucket: bucket.into(),
                    key: key.into(),
                    data: b"must not be written".to_vec(),
                },
            )
            .await?;
        assert_eq!(response.status(), 400, "bucket={bucket}, key={key}");
    }
    Ok(())
}

#[tokio::test]
async fn wasm_blobstore_facade_missing_object_maps_to_not_found() -> Result<()> {
    let Some(harness) = GuestHarness::load(TestGuest::Blobstore).await? else {
        return Ok(());
    };
    let response = harness
        .dispatch(
            blobstore_state(blobstore_client()),
            "/Get",
            proto::GetRequest {
                bucket: "test-bucket".into(),
                key: unique_prefix("facade-missing"),
            },
        )
        .await?;
    assert_eq!(response.status(), 404);
    Ok(())
}

#[tokio::test]
async fn wasm_blobstore_delete_missing_returns_not_found() -> Result<()> {
    let Some(harness) = GuestHarness::load(TestGuest::Blobstore).await? else {
        return Ok(());
    };
    let state = blobstore_state(blobstore_client());
    let req = proto::DeleteRequest {
        bucket: "test-bucket".into(),
        key: unique_prefix("delete-missing"),
    };
    let resp = harness.dispatch(state, "/DeleteMissing", req).await?;
    assert_eq!(resp.status(), 200);

    let body = proto::NotFoundResponse::decode(resp.into_body())?;
    assert_eq!(body.error_kind, "not-found");
    Ok(())
}

#[tokio::test]
async fn wasm_blobstore_rejects_bucket_outside_allowlist() -> Result<()> {
    let Some(harness) = GuestHarness::load(TestGuest::Blobstore).await? else {
        return Ok(());
    };
    let state = blobstore_state(blobstore_client());
    let req = proto::NotFoundRequest {
        bucket: "unauthorized-bucket".into(),
        key: unique_prefix("denied-bucket"),
    };
    let resp = harness.dispatch(state, "/NotFound", req).await?;
    assert_eq!(resp.status(), 200);

    let body = proto::NotFoundResponse::decode(resp.into_body())?;
    assert_eq!(body.error_kind, "access-denied");
    assert!(body.error_message.contains("unauthorized-bucket"));
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
        prefix: format!("{prefix}//./"),
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
