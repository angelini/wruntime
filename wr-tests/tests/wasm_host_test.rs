/// WASM guest test harness — exercises host bindings (DB, tracing, blobstore)
/// through real WASM components using protobuf-encoded requests/responses.
#[allow(dead_code, unused_imports)]
mod helpers;
use helpers::*;

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use bytes::Bytes;
use prost::Message;

/// Proto types generated from the test .proto files (message types only).
#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/test.rs"));
}

// ── Path constants ───────────────────────────────────────────────────────────

const DB_GUEST_WASM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/guests/db-guest/target/wasm32-wasip2/release/db_guest.wasm"
);
const TRACING_GUEST_WASM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/guests/tracing-guest/target/wasm32-wasip2/release/tracing_guest.wasm"
);
const BLOBSTORE_GUEST_WASM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/guests/blobstore-guest/target/wasm32-wasip2/release/blobstore_guest.wasm"
);

/// Build an HTTP POST request targeting a generated router path.
fn rpc_request(path: &str, body: Vec<u8>) -> http::Request<Bytes> {
    http::Request::builder()
        .method("POST")
        .uri(format!("http://localhost{path}"))
        .body(Bytes::from(body))
        .unwrap()
}

// ── DB guest tests ───────────────────────────────────────────────────────────

fn skip_if_no_db_wasm() -> bool {
    if !std::path::Path::new(DB_GUEST_WASM).exists() {
        eprintln!("SKIP: db-guest WASM not built — run `just build-test-guests`");
        return true;
    }
    false
}

#[tokio::test]
async fn wasm_db_execute() -> Result<()> {
    if skip_if_no_db_wasm() {
        return Ok(());
    }
    let Some(state) = db_state_for_module(1, "test-ns", "db-test").await else {
        eprintln!("SKIP: WRUNTIME_TEST_DB_URL not set");
        return Ok(());
    };
    let (engine, pre) = wasm_module_pre(DB_GUEST_WASM)?;

    // Create a table via Execute
    let req = proto::ExecuteRequest {
        sql: "CREATE TEMP TABLE IF NOT EXISTS exec_test (id integer)".into(),
        params_json: "".into(),
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/test.db_test/Execute", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    let body = proto::ExecuteResponse::decode(resp.into_body())?;
    // CREATE TABLE doesn't affect rows
    assert_eq!(body.affected, 0);
    Ok(())
}

#[tokio::test]
async fn wasm_db_query() -> Result<()> {
    if skip_if_no_db_wasm() {
        return Ok(());
    }
    let Some(state) = db_state_for_module(1, "test-ns", "db-query-test").await else {
        eprintln!("SKIP: WRUNTIME_TEST_DB_URL not set");
        return Ok(());
    };
    let (engine, pre) = wasm_module_pre(DB_GUEST_WASM)?;

    let req = proto::QueryRequest {
        sql: "SELECT 42 as num".into(),
        params_json: "".into(),
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/test.db_test/Query", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    let body = proto::QueryResponse::decode(resp.into_body())?;
    assert_eq!(body.rows.len(), 1);
    assert!(!body.rows[0].columns_json.is_empty());
    // The column JSON should contain "42"
    assert!(body.rows[0].columns_json[0].contains("42"));
    Ok(())
}

#[tokio::test]
async fn wasm_db_query_types() -> Result<()> {
    if skip_if_no_db_wasm() {
        return Ok(());
    }
    let Some(state) = db_state_for_module(1, "test-ns", "db-types-test").await else {
        eprintln!("SKIP: WRUNTIME_TEST_DB_URL not set");
        return Ok(());
    };
    let (engine, pre) = wasm_module_pre(DB_GUEST_WASM)?;

    let req = proto::QueryTypesRequest {};
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/test.db_test/QueryTypes", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    let body = proto::QueryTypesResponse::decode(resp.into_body())?;
    // row_json should contain entries for all the typed columns
    assert!(body.row_json.contains("boolean"));
    assert!(body.row_json.contains("int4"));
    assert!(body.row_json.contains("int8"));
    assert!(body.row_json.contains("float8"));
    assert!(body.row_json.contains("text"));
    assert!(body.row_json.contains("hello"));
    Ok(())
}

#[tokio::test]
async fn wasm_db_transaction_commit() -> Result<()> {
    if skip_if_no_db_wasm() {
        return Ok(());
    }
    let Some(state) = db_state_for_module(1, "test-ns", "db-txcommit-test").await else {
        eprintln!("SKIP: WRUNTIME_TEST_DB_URL not set");
        return Ok(());
    };
    let (engine, pre) = wasm_module_pre(DB_GUEST_WASM)?;

    let req = proto::TransactionCommitRequest {
        table_name: "tx_commit_wasm".into(),
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/test.db_test/TransactionCommit", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    let body = proto::TransactionCommitResponse::decode(resp.into_body())?;
    assert_eq!(body.count, 1, "committed row should be visible");
    Ok(())
}

#[tokio::test]
async fn wasm_db_transaction_rollback() -> Result<()> {
    if skip_if_no_db_wasm() {
        return Ok(());
    }
    let Some(state) = db_state_for_module(1, "test-ns", "db-txrollback-test").await else {
        eprintln!("SKIP: WRUNTIME_TEST_DB_URL not set");
        return Ok(());
    };
    let (engine, pre) = wasm_module_pre(DB_GUEST_WASM)?;

    let req = proto::TransactionRollbackRequest {
        table_name: "tx_rollback_wasm".into(),
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/test.db_test/TransactionRollback", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    let body = proto::TransactionRollbackResponse::decode(resp.into_body())?;
    assert_eq!(body.count, 0, "rolled-back row should not be visible");
    Ok(())
}

#[tokio::test]
async fn wasm_db_transaction_drop() -> Result<()> {
    if skip_if_no_db_wasm() {
        return Ok(());
    }
    let Some(state) = db_state_for_module(1, "test-ns", "db-txdrop-test").await else {
        eprintln!("SKIP: WRUNTIME_TEST_DB_URL not set");
        return Ok(());
    };
    let (engine, pre) = wasm_module_pre(DB_GUEST_WASM)?;

    let req = proto::TransactionDropRequest {
        table_name: "tx_drop_wasm".into(),
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/test.db_test/TransactionDrop", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    let body = proto::TransactionDropResponse::decode(resp.into_body())?;
    assert_eq!(
        body.count, 0,
        "dropped transaction should implicitly rollback"
    );
    Ok(())
}

#[tokio::test]
async fn wasm_db_error() -> Result<()> {
    if skip_if_no_db_wasm() {
        return Ok(());
    }
    let Some(state) = db_state_for_module(1, "test-ns", "db-error-test").await else {
        eprintln!("SKIP: WRUNTIME_TEST_DB_URL not set");
        return Ok(());
    };
    let (engine, pre) = wasm_module_pre(DB_GUEST_WASM)?;

    let req = proto::ErrorRequest {
        sql: "SELECT * FROM nonexistent_table_xyz".into(),
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/test.db_test/Error", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    let body = proto::ErrorResponse::decode(resp.into_body())?;
    assert_eq!(body.error_kind, "query");
    assert!(!body.error_message.is_empty());
    Ok(())
}

// ── Tracing guest tests ──────────────────────────────────────────────────────

fn skip_if_no_tracing_wasm() -> bool {
    if !std::path::Path::new(TRACING_GUEST_WASM).exists() {
        eprintln!("SKIP: tracing-guest WASM not built — run `just build-test-guests`");
        return true;
    }
    false
}

#[tokio::test]
async fn wasm_tracing_start_span() -> Result<()> {
    if skip_if_no_tracing_wasm() {
        return Ok(());
    }
    let (engine, pre) = wasm_module_pre(TRACING_GUEST_WASM)?;
    let state = tracing_state();

    let req = proto::StartSpanRequest {
        name: "test-span".into(),
        attrs: [("key".into(), "value".into())].into(),
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/test.tracing_test/StartSpan", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    let body = proto::StartSpanResponse::decode(resp.into_body())?;
    assert!(body.ok);
    Ok(())
}

#[tokio::test]
async fn wasm_tracing_span_attributes() -> Result<()> {
    if skip_if_no_tracing_wasm() {
        return Ok(());
    }
    let (engine, pre) = wasm_module_pre(TRACING_GUEST_WASM)?;
    let state = tracing_state();

    let req = proto::SpanAttributesRequest {
        span_name: "attr-span".into(),
        attrs: [("a".into(), "1".into()), ("b".into(), "2".into())].into(),
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/test.tracing_test/SpanAttributes", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    let body = proto::SpanAttributesResponse::decode(resp.into_body())?;
    assert!(body.ok);
    Ok(())
}

#[tokio::test]
async fn wasm_tracing_span_event() -> Result<()> {
    if skip_if_no_tracing_wasm() {
        return Ok(());
    }
    let (engine, pre) = wasm_module_pre(TRACING_GUEST_WASM)?;
    let state = tracing_state();

    let req = proto::SpanEventRequest {
        span_name: "event-span".into(),
        event_name: "my-event".into(),
        event_attrs: [("detail".into(), "test".into())].into(),
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/test.tracing_test/SpanEvent", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    let body = proto::SpanEventResponse::decode(resp.into_body())?;
    assert!(body.ok);
    Ok(())
}

#[tokio::test]
async fn wasm_tracing_span_error() -> Result<()> {
    if skip_if_no_tracing_wasm() {
        return Ok(());
    }
    let (engine, pre) = wasm_module_pre(TRACING_GUEST_WASM)?;
    let state = tracing_state();

    let req = proto::SpanErrorRequest {
        span_name: "error-span".into(),
        message: "something went wrong".into(),
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/test.tracing_test/SpanError", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    let body = proto::SpanErrorResponse::decode(resp.into_body())?;
    assert!(body.ok);
    Ok(())
}

#[tokio::test]
async fn wasm_tracing_nested_spans() -> Result<()> {
    if skip_if_no_tracing_wasm() {
        return Ok(());
    }
    let (engine, pre) = wasm_module_pre(TRACING_GUEST_WASM)?;
    let state = tracing_state();

    let req = proto::NestedSpansRequest {
        outer_name: "outer".into(),
        inner_name: "inner".into(),
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/test.tracing_test/NestedSpans", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    let body = proto::NestedSpansResponse::decode(resp.into_body())?;
    assert!(body.ok);
    Ok(())
}

// ── Blobstore guest tests ────────────────────────────────────────────────────

/// Generate a unique key prefix for each test invocation to avoid cross-run contamination.
fn unique_prefix(test_name: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("wasm-test/{test_name}/{ts}-{n}")
}

fn skip_if_no_blobstore_wasm() -> bool {
    if !std::path::Path::new(BLOBSTORE_GUEST_WASM).exists() {
        eprintln!("SKIP: blobstore-guest WASM not built — run `just build-test-guests`");
        return true;
    }
    false
}

#[tokio::test]
async fn wasm_blobstore_put_get() -> Result<()> {
    if skip_if_no_blobstore_wasm() {
        return Ok(());
    }
    let Some(bs) = blobstore_client() else {
        eprintln!("SKIP: WRUNTIME_TEST_S3_* env vars not set");
        return Ok(());
    };
    let (engine, pre) = wasm_module_pre(BLOBSTORE_GUEST_WASM)?;
    let key = unique_prefix("put-get");

    // Put
    let state = blobstore_state(bs.clone());
    let req = proto::PutRequest {
        bucket: "test-bucket".into(),
        key: key.clone(),
        data: b"hello wasm blobstore".to_vec(),
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/test.blobstore_test/Put", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    // Get
    let state = blobstore_state(bs.clone());
    let req = proto::GetRequest {
        bucket: "test-bucket".into(),
        key: key.clone(),
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/test.blobstore_test/Get", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    let body = proto::GetResponse::decode(resp.into_body())?;
    assert_eq!(body.data, b"hello wasm blobstore");
    Ok(())
}

#[tokio::test]
async fn wasm_blobstore_delete() -> Result<()> {
    if skip_if_no_blobstore_wasm() {
        return Ok(());
    }
    let Some(bs) = blobstore_client() else {
        eprintln!("SKIP: WRUNTIME_TEST_S3_* env vars not set");
        return Ok(());
    };
    let (engine, pre) = wasm_module_pre(BLOBSTORE_GUEST_WASM)?;
    let key = unique_prefix("delete-me");

    // Put first
    let state = blobstore_state(bs.clone());
    let req = proto::PutRequest {
        bucket: "test-bucket".into(),
        key: key.clone(),
        data: b"temp".to_vec(),
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/test.blobstore_test/Put", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    // Delete
    let state = blobstore_state(bs.clone());
    let req = proto::DeleteRequest {
        bucket: "test-bucket".into(),
        key: key.clone(),
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/test.blobstore_test/Delete", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    // Verify deleted — get should fail
    let state = blobstore_state(bs.clone());
    let req = proto::NotFoundRequest {
        bucket: "test-bucket".into(),
        key: key.clone(),
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/test.blobstore_test/NotFound", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    let body = proto::NotFoundResponse::decode(resp.into_body())?;
    assert_eq!(body.error_kind, "not-found");
    Ok(())
}

#[tokio::test]
async fn wasm_blobstore_list() -> Result<()> {
    if skip_if_no_blobstore_wasm() {
        return Ok(());
    }
    let Some(bs) = blobstore_client() else {
        eprintln!("SKIP: WRUNTIME_TEST_S3_* env vars not set");
        return Ok(());
    };
    let (engine, pre) = wasm_module_pre(BLOBSTORE_GUEST_WASM)?;
    let prefix = unique_prefix("list");

    // Put 3 objects with a common prefix
    for i in 0..3 {
        let state = blobstore_state(bs.clone());
        let req = proto::PutRequest {
            bucket: "test-bucket".into(),
            key: format!("{prefix}/item-{i}"),
            data: format!("data-{i}").into_bytes(),
        };
        let resp = dispatch_to_wasm(
            &engine,
            &pre,
            state,
            rpc_request("/test.blobstore_test/Put", req.encode_to_vec()),
        )
        .await?;
        assert_eq!(resp.status(), 200);
    }

    // List with prefix
    let state = blobstore_state(bs.clone());
    let req = proto::ListRequest {
        bucket: "test-bucket".into(),
        prefix: format!("{prefix}/"),
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/test.blobstore_test/List", req.encode_to_vec()),
    )
    .await?;
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
    if skip_if_no_blobstore_wasm() {
        return Ok(());
    }
    let Some(bs) = blobstore_client() else {
        eprintln!("SKIP: WRUNTIME_TEST_S3_* env vars not set");
        return Ok(());
    };
    let (engine, pre) = wasm_module_pre(BLOBSTORE_GUEST_WASM)?;

    let key = unique_prefix("head-obj");
    let data = b"head-test-data";
    let state = blobstore_state(bs.clone());
    let req = proto::PutRequest {
        bucket: "test-bucket".into(),
        key: key.clone(),
        data: data.to_vec(),
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/test.blobstore_test/Put", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    let state = blobstore_state(bs.clone());
    let req = proto::HeadRequest {
        bucket: "test-bucket".into(),
        key: key.clone(),
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/test.blobstore_test/Head", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    let body = proto::HeadResponse::decode(resp.into_body())?;
    assert_eq!(body.key, key);
    assert_eq!(body.size, data.len() as u64);
    Ok(())
}

#[tokio::test]
async fn wasm_blobstore_round_trip() -> Result<()> {
    if skip_if_no_blobstore_wasm() {
        return Ok(());
    }
    let Some(bs) = blobstore_client() else {
        eprintln!("SKIP: WRUNTIME_TEST_S3_* env vars not set");
        return Ok(());
    };
    let (engine, pre) = wasm_module_pre(BLOBSTORE_GUEST_WASM)?;
    let key = unique_prefix("round-trip");
    let state = blobstore_state(bs.clone());

    let req = proto::RoundTripRequest {
        bucket: "test-bucket".into(),
        key,
        data: b"round-trip-payload".to_vec(),
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/test.blobstore_test/RoundTrip", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    let body = proto::RoundTripResponse::decode(resp.into_body())?;
    assert!(body.matches);
    assert_eq!(body.data, b"round-trip-payload");
    Ok(())
}

#[tokio::test]
async fn wasm_blobstore_not_found() -> Result<()> {
    if skip_if_no_blobstore_wasm() {
        return Ok(());
    }
    let Some(bs) = blobstore_client() else {
        eprintln!("SKIP: WRUNTIME_TEST_S3_* env vars not set");
        return Ok(());
    };
    let (engine, pre) = wasm_module_pre(BLOBSTORE_GUEST_WASM)?;
    let state = blobstore_state(bs.clone());

    let req = proto::NotFoundRequest {
        bucket: "test-bucket".into(),
        key: unique_prefix("nonexistent"),
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/test.blobstore_test/NotFound", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    let body = proto::NotFoundResponse::decode(resp.into_body())?;
    assert_eq!(body.error_kind, "not-found");
    Ok(())
}
