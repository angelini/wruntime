/// WASM guest test harness — exercises host bindings and WASI HTTP through real
/// WASM components using protobuf-encoded requests/responses.
#[allow(dead_code, unused_imports)]
mod helpers;
use helpers::*;

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use prost::Message;
use wr_engine::config::ResourceLimits;

/// Proto types generated from the test .proto files (message types only).
#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/test.rs"));
}

// ── Path constants ───────────────────────────────────────────────────────────

const DB_GUEST_WASM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/guests/db-guest/target/wasm32-wasip2/debug/db_guest.wasm"
);
const TRACING_GUEST_WASM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/guests/tracing-guest/target/wasm32-wasip2/debug/tracing_guest.wasm"
);
const BLOBSTORE_GUEST_WASM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/guests/blobstore-guest/target/wasm32-wasip2/debug/blobstore_guest.wasm"
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
    let state = db_state_for_module(1, "test-ns", "db-test").await;
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
        rpc_request("/Execute", req.encode_to_vec()),
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
    let state = db_state_for_module(1, "test-ns", "db-query-test").await;
    let (engine, pre) = wasm_module_pre(DB_GUEST_WASM)?;

    let req = proto::QueryRequest {
        sql: "SELECT 42 as num".into(),
        params_json: "".into(),
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/Query", req.encode_to_vec()),
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
    let state = db_state_for_module(1, "test-ns", "db-types-test").await;
    let (engine, pre) = wasm_module_pre(DB_GUEST_WASM)?;

    let req = proto::QueryTypesRequest {};
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/QueryTypes", req.encode_to_vec()),
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
    let state = db_state_for_module(2, "test-ns", "db-txcommit-test").await;
    let (engine, pre) = wasm_module_pre(DB_GUEST_WASM)?;

    let req = proto::TransactionCommitRequest {
        table_name: "tx_commit_wasm".into(),
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/TransactionCommit", req.encode_to_vec()),
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
    let state = db_state_for_module(2, "test-ns", "db-txrollback-test").await;
    let (engine, pre) = wasm_module_pre(DB_GUEST_WASM)?;

    let req = proto::TransactionRollbackRequest {
        table_name: "tx_rollback_wasm".into(),
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/TransactionRollback", req.encode_to_vec()),
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
    let state = db_state_for_module(2, "test-ns", "db-txdrop-test").await;
    let (engine, pre) = wasm_module_pre(DB_GUEST_WASM)?;

    let req = proto::TransactionDropRequest {
        table_name: "tx_drop_wasm".into(),
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/TransactionDrop", req.encode_to_vec()),
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
    let state = db_state_for_module(1, "test-ns", "db-error-test").await;
    let (engine, pre) = wasm_module_pre(DB_GUEST_WASM)?;

    let req = proto::ErrorRequest {
        sql: "SELECT * FROM nonexistent_table_xyz".into(),
        params_json: "".into(),
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/Error", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    let body = proto::ErrorResponse::decode(resp.into_body())?;
    assert_eq!(body.error_kind, "query");
    assert!(!body.error_message.is_empty());
    Ok(())
}

#[tokio::test]
async fn wasm_db_invalid_param() -> Result<()> {
    if skip_if_no_db_wasm() {
        return Ok(());
    }
    let state = db_state_for_module(1, "test-ns", "db-invalid-param-test").await;
    let (engine, pre) = wasm_module_pre(DB_GUEST_WASM)?;

    let req = proto::ErrorRequest {
        sql: "SELECT $1::numeric AS n".into(),
        params_json: r#"[{"type":"numeric","value":"not-a-number"}]"#.into(),
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/Error", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    let body = proto::ErrorResponse::decode(resp.into_body())?;
    assert_eq!(body.error_kind, "query");
    assert!(!body.error_message.is_empty());
    Ok(())
}

#[tokio::test]
async fn wasm_db_query_stream() -> Result<()> {
    if skip_if_no_db_wasm() {
        return Ok(());
    }
    let state = db_state_for_module(1, "test-ns", "db-stream-test").await;
    let (engine, pre) = wasm_module_pre(DB_GUEST_WASM)?;

    let req = proto::QueryStreamRequest {
        sql: "SELECT generate_series(1, 5) AS n".into(),
        params_json: "".into(),
        batch_size: 2,
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/QueryStream", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    let body = proto::QueryStreamResponse::decode(resp.into_body())?;
    assert_eq!(body.rows.len(), 5);
    // With batch_size=2 and 5 rows: batches of 2, 2, 1, then empty = 4 batches
    assert_eq!(body.batch_count, 4);
    Ok(())
}

#[tokio::test]
async fn wasm_db_query_stream_drop() -> Result<()> {
    if skip_if_no_db_wasm() {
        return Ok(());
    }
    let state = db_state_for_module(1, "test-ns", "db-stream-drop-test").await;
    let (engine, pre) = wasm_module_pre(DB_GUEST_WASM)?;

    let req = proto::QueryStreamDropRequest {
        sql: "SELECT generate_series(1, 100) AS n".into(),
        fetch_count: 5,
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/QueryStreamDrop", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    let body = proto::QueryStreamDropResponse::decode(resp.into_body())?;
    assert_eq!(body.fetched, 5);
    Ok(())
}

#[tokio::test]
async fn wasm_db_resource_caps() -> Result<()> {
    if skip_if_no_db_wasm() {
        return Ok(());
    }
    let (engine, pre) = wasm_module_pre(DB_GUEST_WASM)?;
    let limits = ResourceLimits {
        max_db_transactions: 2,
        max_db_cursors: 2,
        ..Default::default()
    };

    for path in ["/AllocTransactions", "/AllocCursors"] {
        // Cap + 1 rejected as a normal error, not a trap.
        let state =
            db_state_for_module_with_limits(5, "test-ns", "db-cap-test", limits.clone()).await;
        let req = proto::AllocResourcesRequest {
            initial: 3,
            drop_count: 0,
            additional: 0,
        };
        let resp =
            dispatch_to_wasm(&engine, &pre, state, rpc_request(path, req.encode_to_vec())).await?;
        assert_eq!(resp.status(), 200);
        let body = proto::AllocResourcesResponse::decode(resp.into_body())?;
        assert_eq!(body.held, 2, "path={path}");
        assert!(body.hit_cap, "path={path}");
        assert_eq!(body.error_kind, "connection", "path={path}");

        // Dropping ALL held resources frees the count so a full re-allocation
        // to cap succeeds — proves the decrement-on-drop invariant holds.
        let state =
            db_state_for_module_with_limits(5, "test-ns", "db-cap-test", limits.clone()).await;
        let req = proto::AllocResourcesRequest {
            initial: 2,
            drop_count: 2,
            additional: 2,
        };
        let resp =
            dispatch_to_wasm(&engine, &pre, state, rpc_request(path, req.encode_to_vec())).await?;
        assert_eq!(resp.status(), 200);
        let body = proto::AllocResourcesResponse::decode(resp.into_body())?;
        assert_eq!(body.held, 2, "path={path}");
        assert!(!body.hit_cap, "path={path}");
    }

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
        rpc_request("/StartSpan", req.encode_to_vec()),
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
        rpc_request("/SpanAttributes", req.encode_to_vec()),
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
        rpc_request("/SpanEvent", req.encode_to_vec()),
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
        rpc_request("/SpanError", req.encode_to_vec()),
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
        rpc_request("/NestedSpans", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    let body = proto::NestedSpansResponse::decode(resp.into_body())?;
    assert!(body.ok);
    Ok(())
}

#[tokio::test]
async fn wasm_tracing_span_cap() -> Result<()> {
    if skip_if_no_tracing_wasm() {
        return Ok(());
    }
    let (engine, pre) = wasm_module_pre(TRACING_GUEST_WASM)?;
    let limits = ResourceLimits {
        max_spans: 2,
        ..Default::default()
    };

    // Exactly at cap succeeds.
    let state = tracing_state_with_limits(limits.clone());
    let req = proto::AllocSpansRequest {
        initial: 2,
        drop_count: 0,
        additional: 0,
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/AllocSpans", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);
    let body = proto::AllocSpansResponse::decode(resp.into_body())?;
    assert_eq!(body.held, 2);

    // Cap + 1 traps the store — dispatch returns Err.
    let state = tracing_state_with_limits(limits.clone());
    let req = proto::AllocSpansRequest {
        initial: 3,
        drop_count: 0,
        additional: 0,
    };
    let result = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/AllocSpans", req.encode_to_vec()),
    )
    .await;
    assert!(result.is_err(), "expected trap when exceeding span cap");

    // Engine survives the trap — a fresh request for the same module still works.
    let state = tracing_state_with_limits(limits.clone());
    let req = proto::AllocSpansRequest {
        initial: 1,
        drop_count: 0,
        additional: 0,
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/AllocSpans", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    // Dropping a span frees a live slot so a later `start` succeeds again.
    let state = tracing_state_with_limits(limits);
    let req = proto::AllocSpansRequest {
        initial: 2,
        drop_count: 1,
        additional: 1,
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/AllocSpans", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);
    let body = proto::AllocSpansResponse::decode(resp.into_body())?;
    assert_eq!(body.held, 2);

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
    let bs = blobstore_client();
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
        rpc_request("/Put", req.encode_to_vec()),
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
        rpc_request("/Get", req.encode_to_vec()),
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
    let bs = blobstore_client();
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
        rpc_request("/Put", req.encode_to_vec()),
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
        rpc_request("/Delete", req.encode_to_vec()),
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
        rpc_request("/NotFound", req.encode_to_vec()),
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
    let bs = blobstore_client();
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
            rpc_request("/Put", req.encode_to_vec()),
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
        rpc_request("/List", req.encode_to_vec()),
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
    let bs = blobstore_client();
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
        rpc_request("/Put", req.encode_to_vec()),
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
        rpc_request("/Head", req.encode_to_vec()),
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
    let bs = blobstore_client();
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
        rpc_request("/RoundTrip", req.encode_to_vec()),
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
    let bs = blobstore_client();
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
        rpc_request("/NotFound", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    let body = proto::NotFoundResponse::decode(resp.into_body())?;
    assert_eq!(body.error_kind, "not-found");
    Ok(())
}

#[tokio::test]
async fn wasm_blobstore_put_too_large_rejected() -> Result<()> {
    if skip_if_no_blobstore_wasm() {
        return Ok(());
    }
    let bs = blobstore_client();
    let (engine, pre) = wasm_module_pre(BLOBSTORE_GUEST_WASM)?;
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
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/Put", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200, "at-cap upload should be accepted");

    // One byte over cap is rejected with too-large.
    let state = blobstore_state_with_limits(bs.clone(), limits);
    let req = proto::PutRequest {
        bucket: "test-bucket".into(),
        key: format!("{key}/over-cap"),
        data: vec![b'x'; 1025],
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/Put", req.encode_to_vec()),
    )
    .await?;
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
    if skip_if_no_blobstore_wasm() {
        return Ok(());
    }
    let bs = blobstore_client();
    let (engine, pre) = wasm_module_pre(BLOBSTORE_GUEST_WASM)?;
    let key = unique_prefix("get-too-large");

    // Store a 2 KiB object using a default-limit state (upload allowed).
    let state = blobstore_state(bs.clone());
    let req = proto::PutRequest {
        bucket: "test-bucket".into(),
        key: key.clone(),
        data: vec![b'y'; 2048],
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/Put", req.encode_to_vec()),
    )
    .await?;
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
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/Get", req.encode_to_vec()),
    )
    .await?;
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
    if skip_if_no_blobstore_wasm() {
        return Ok(());
    }
    let bs = blobstore_client();
    let (engine, pre) = wasm_module_pre(BLOBSTORE_GUEST_WASM)?;
    let prefix = unique_prefix("list-cap");

    // Seed 3 objects under a shared prefix (default-limit state).
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
            rpc_request("/Put", req.encode_to_vec()),
        )
        .await?;
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
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/List", req.encode_to_vec()),
    )
    .await?;
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
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/List", req.encode_to_vec()),
    )
    .await?;
    assert_ne!(resp.status(), 200, "listing over cap should be rejected");
    let body = String::from_utf8_lossy(&resp.into_body()).into_owned();
    assert!(
        body.contains("too large"),
        "expected too-large error, got: {body}"
    );
    Ok(())
}

// ── LLM guest tests ─────────────────────────────────────────────────────────

const LLM_GUEST_WASM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/guests/llm-guest/target/wasm32-wasip2/debug/llm_guest.wasm"
);

fn skip_if_no_llm_wasm() -> bool {
    if !std::path::Path::new(LLM_GUEST_WASM).exists() {
        eprintln!("SKIP: llm-guest WASM not built — run `just build-test-guests`");
        return true;
    }
    false
}

#[tokio::test]
async fn wasm_llm_complete() -> Result<()> {
    if skip_if_no_llm_wasm() {
        return Ok(());
    }
    let (base_url, _shutdown) = spawn_mock_llm_server(MockLlmMode::Text {
        text: "Hello from mock Claude!".into(),
        input_tokens: 10,
        output_tokens: 7,
    })
    .await?;
    let llm = mock_llm_runtime(&base_url);
    let (engine, pre) = wasm_module_pre(LLM_GUEST_WASM)?;
    let state = llm_state(llm);

    let req = proto::CompleteRequest {
        model: "claude-sonnet-4-6".into(),
        system: "You are a test assistant.".into(),
        user_message: "Say hello".into(),
        max_tokens: 100,
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/Complete", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    let body = proto::CompleteResponse::decode(resp.into_body())?;
    assert_eq!(body.text, "Hello from mock Claude!");
    assert_eq!(body.stop_reason, "end_turn");
    assert_eq!(body.input_tokens, 10);
    assert_eq!(body.output_tokens, 7);
    Ok(())
}

#[tokio::test]
async fn wasm_llm_complete_text() -> Result<()> {
    if skip_if_no_llm_wasm() {
        return Ok(());
    }
    let (base_url, _shutdown) = spawn_mock_llm_server(MockLlmMode::Text {
        text: "Short answer".into(),
        input_tokens: 5,
        output_tokens: 2,
    })
    .await?;
    let llm = mock_llm_runtime(&base_url);
    let (engine, pre) = wasm_module_pre(LLM_GUEST_WASM)?;
    let state = llm_state(llm);

    let req = proto::CompleteTextRequest {
        user_message: "Give me a short answer".into(),
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/CompleteText", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    let body = proto::CompleteTextResponse::decode(resp.into_body())?;
    assert_eq!(body.text, "Short answer");
    Ok(())
}

#[tokio::test]
async fn wasm_llm_tool_use() -> Result<()> {
    if skip_if_no_llm_wasm() {
        return Ok(());
    }
    let (base_url, _shutdown) = spawn_mock_llm_server(MockLlmMode::ToolUse {
        tool_id: "toolu_mock_001".into(),
        tool_name: "get_weather".into(),
        tool_input: r#"{"location":"San Francisco"}"#.into(),
    })
    .await?;
    let llm = mock_llm_runtime(&base_url);
    let (engine, pre) = wasm_module_pre(LLM_GUEST_WASM)?;
    let state = llm_state(llm);

    let req = proto::ToolUseRequest {
        user_message: "What's the weather in San Francisco?".into(),
        tool_name: "get_weather".into(),
        tool_description: "Get current weather for a location".into(),
        tool_schema: r#"{"type":"object","properties":{"location":{"type":"string"}},"required":["location"]}"#.into(),
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/ToolUse", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    let body = proto::ToolUseResponse::decode(resp.into_body())?;
    assert_eq!(body.tool_name, "get_weather");
    assert_eq!(body.tool_id, "toolu_mock_001");
    assert!(body.tool_input.contains("San Francisco"));
    assert_eq!(body.stop_reason, "tool_use");
    Ok(())
}

#[tokio::test]
async fn wasm_llm_error() -> Result<()> {
    if skip_if_no_llm_wasm() {
        return Ok(());
    }
    let (base_url, _shutdown) = spawn_mock_llm_server(MockLlmMode::Error {
        status: 401,
        body: r#"{"error":{"type":"authentication_error","message":"invalid api key"}}"#.into(),
    })
    .await?;
    let llm = mock_llm_runtime(&base_url);
    let (engine, pre) = wasm_module_pre(LLM_GUEST_WASM)?;
    let state = llm_state(llm);

    let req = proto::LlmErrorRequest {
        user_message: "This should fail".into(),
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/Error", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    let body = proto::LlmErrorResponse::decode(resp.into_body())?;
    assert_eq!(body.error_kind, "auth");
    assert!(!body.error_message.is_empty());
    Ok(())
}

#[tokio::test]
async fn wasm_llm_stream() -> Result<()> {
    if skip_if_no_llm_wasm() {
        return Ok(());
    }
    let (base_url, _shutdown) = spawn_mock_llm_server(MockLlmMode::Stream {
        chunks: vec!["Hello".into(), " from".into(), " streaming!".into()],
    })
    .await?;
    let llm = mock_llm_runtime(&base_url);
    let (engine, pre) = wasm_module_pre(LLM_GUEST_WASM)?;
    let state = llm_state(llm);

    let req = proto::StreamRequest {
        user_message: "Stream me a response".into(),
        with_tools: false,
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/Stream", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    let body = proto::StreamResponse::decode(resp.into_body())?;
    assert_eq!(body.text, "Hello from streaming!");
    assert_eq!(body.chunk_count, 3);
    assert_eq!(
        body.events,
        vec!["text-delta", "text-delta", "text-delta", "usage", "stop"]
    );
    assert_eq!(body.input_tokens, 25);
    assert_eq!(body.output_tokens, 21); // "Hello"(5) + " from"(5) + " streaming!"(11)
    assert_eq!(body.stop_reason, "end_turn");
    assert!(body.usage_mid_none, "usage() must be None mid-stream");
    assert!(body.usage_present_after, "usage() must be Some after drain");
    assert!(body.error_kind.is_empty());
    Ok(())
}

#[tokio::test]
async fn wasm_llm_stream_error() -> Result<()> {
    if skip_if_no_llm_wasm() {
        return Ok(());
    }
    let (base_url, _shutdown) = spawn_mock_llm_server(MockLlmMode::StreamError).await?;
    let llm = mock_llm_runtime(&base_url);
    let (engine, pre) = wasm_module_pre(LLM_GUEST_WASM)?;
    let state = llm_state(llm);

    let req = proto::StreamRequest {
        user_message: "Stream me a response".into(),
        with_tools: false,
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/Stream", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    let body = proto::StreamResponse::decode(resp.into_body())?;
    // Partial text arrives before the error frame.
    assert_eq!(body.text, "partial");
    assert_eq!(body.chunk_count, 1);
    // The stream-level error surfaces as an llm-error, not a silent truncation.
    assert_eq!(body.error_kind, "api");
    assert!(body.error_message.contains("overloaded"));
    Ok(())
}

#[tokio::test]
async fn wasm_llm_stream_tool_use_rejected() -> Result<()> {
    if skip_if_no_llm_wasm() {
        return Ok(());
    }
    // Mock is spawned but never hit — the request is pre-rejected before any upstream call.
    let (base_url, _shutdown) = spawn_mock_llm_server(MockLlmMode::Stream {
        chunks: vec!["unused".into()],
    })
    .await?;
    let llm = mock_llm_runtime(&base_url);
    let (engine, pre) = wasm_module_pre(LLM_GUEST_WASM)?;
    let state = llm_state(llm);

    let req = proto::StreamRequest {
        user_message: "Use a tool while streaming".into(),
        with_tools: true,
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/Stream", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);

    let body = proto::StreamResponse::decode(resp.into_body())?;
    assert_eq!(body.error_kind, "invalid-request");
    assert!(body.error_message.contains("streaming"));
    // No stream was produced.
    assert_eq!(body.chunk_count, 0);
    assert!(body.text.is_empty());
    Ok(())
}

#[tokio::test]
async fn wasm_llm_stream_cap() -> Result<()> {
    if skip_if_no_llm_wasm() {
        return Ok(());
    }
    let (base_url, _shutdown) = spawn_mock_llm_server(MockLlmMode::Stream {
        chunks: vec!["hi".into()],
    })
    .await?;
    let (engine, pre) = wasm_module_pre(LLM_GUEST_WASM)?;
    let limits = ResourceLimits {
        max_llm_streams: 2,
        ..Default::default()
    };

    // Cap + 1 rejected via `LlmError::Api` — the 3rd `stream()` is rejected by
    // `try_track` before any upstream request, so the mock is only hit for the
    // successful opens.
    let llm = mock_llm_runtime(&base_url);
    let state = llm_state_with_limits(llm, limits.clone());
    let req = proto::AllocStreamsRequest {
        initial: 3,
        drop_count: 0,
        additional: 0,
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/AllocStreams", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);
    let body = proto::AllocStreamsResponse::decode(resp.into_body())?;
    assert_eq!(body.held, 2);
    assert!(body.hit_cap);
    assert_eq!(body.error_kind, "api");

    // Dropping ALL held streams frees the count so a full re-allocation to cap
    // succeeds.
    let llm = mock_llm_runtime(&base_url);
    let state = llm_state_with_limits(llm, limits);
    let req = proto::AllocStreamsRequest {
        initial: 2,
        drop_count: 2,
        additional: 2,
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        state,
        rpc_request("/AllocStreams", req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);
    let body = proto::AllocStreamsResponse::decode(resp.into_body())?;
    assert_eq!(body.held, 2);
    assert!(!body.hit_cap);

    Ok(())
}

// ── HTTP guest tests (egress & ingress) ─────────────────────────────────────

const HTTP_GUEST_WASM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/guests/http-guest/target/wasm32-wasip2/debug/http_guest.wasm"
);

fn skip_if_no_http_wasm() -> bool {
    if !std::path::Path::new(HTTP_GUEST_WASM).exists() {
        eprintln!("SKIP: http-guest WASM not built — run `just build-test-guests`");
        return true;
    }
    false
}

/// Verify a WASM guest can make an outbound HTTP request that exits the
/// network via the proxy egress layer to an allowed external domain.
#[tokio::test]
async fn wasm_http_egress() -> Result<()> {
    if skip_if_no_http_wasm() {
        return Ok(());
    }

    // External HTTP/1.1 stub (stands in for example.com).
    let (ext_addr, _ext_shutdown) = spawn_http1_stub().await?;
    let ext_uri: http::Uri = ext_addr.parse()?;
    let ext_authority = ext_uri.authority().unwrap().to_string();

    // Egress proxy with 127.0.0.1 in the allowlist.
    let table = wr_proxy::routing::new_routing_table();
    let egress_cfg = EgressConfig {
        allowed_domains: vec!["127.0.0.1".into()],
    };
    let proxy_addr = start_egress_proxy(Some(egress_cfg), table).await?;
    let proxy_uri: hyper::Uri = format!("http://{proxy_addr}").parse()?;

    let (engine, pre) = wasm_module_pre(HTTP_GUEST_WASM)?;

    // Small outbound-body cap so the over-cap case is cheap.
    let cap = 1024usize;

    // Under the cap: succeeds and returns the stub's echo body.
    let under_state = ModuleState::new(
        "http-test".into(),
        "test-ns".into(),
        proxy_uri.clone(),
        http_pool(),
        ModuleServices {
            max_outbound_body_bytes: cap,
            ..ModuleServices::default()
        },
    )?;
    let under_req = proto::EgressRequest {
        authority: ext_authority.clone(),
        path: "/hello-egress".into(),
        body: vec![b'x'; 16],
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        under_state,
        rpc_request("/Egress", under_req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 200);
    let body = proto::EgressResponse::decode(resp.into_body())?;
    assert_eq!(body.status, 200);
    assert_eq!(body.body, "egress:/hello-egress");

    // Over the cap: the outbound body exceeds `cap`, so send_request returns
    // HttpRequestBodySize; the guest maps the failed http_rpc to a 500.
    let over_state = ModuleState::new(
        "http-test".into(),
        "test-ns".into(),
        proxy_uri.clone(),
        http_pool(),
        ModuleServices {
            max_outbound_body_bytes: cap,
            ..ModuleServices::default()
        },
    )?;
    let over_req = proto::EgressRequest {
        authority: ext_authority.clone(),
        path: "/hello-egress".into(),
        body: vec![b'x'; 4096],
    };
    let resp = dispatch_to_wasm(
        &engine,
        &pre,
        over_state,
        rpc_request("/Egress", over_req.encode_to_vec()),
    )
    .await?;
    assert_eq!(resp.status(), 500);
    let err_body = String::from_utf8_lossy(resp.into_body().as_ref()).into_owned();
    assert!(
        err_body.contains("egress call failed"),
        "unexpected error body: {err_body}"
    );

    Ok(())
}

/// Verify an external HTTP client (no x-wr-* headers) can reach a WASM guest
/// through the proxy ingress layer.
#[tokio::test]
async fn wasm_http_ingress() -> Result<()> {
    if skip_if_no_http_wasm() {
        return Ok(());
    }

    let (engine, pre) = wasm_module_pre(HTTP_GUEST_WASM)?;

    // WASM-backed HTTP/2 engine.
    let (engine_addr, _engine_shutdown) =
        spawn_wasm_stub_engine(engine, pre, "http://127.0.0.1:9001", "http-svc", "test-ns").await?;

    // Manager + registration.
    let (_pool, mgr_addr, mut client) = manager_trio().await?;
    register_test_module(
        &mut client,
        "wasm-engine-1",
        &engine_addr,
        "test-ns",
        "http-svc",
        "1.0.0",
    )
    .await?;

    // Ingress proxy with a public route for Echo.
    let table = synced_routing_table(&mgr_addr).await?;
    let ingress_addr = start_ingress_proxy(
        table,
        vec![ExternalRoute {
            path: "/Echo".into(),
            methods: vec!["POST".into()],
            module: "http-svc".into(),
            namespace: "test-ns".into(),
        }],
    )
    .await?;

    // Plain HTTP request — no x-wr-* headers — simulates external caller.
    let req_body = proto::EchoRequest {
        message: "hello from outside".into(),
    };
    let resp = http_client()
        .request(
            http::Request::builder()
                .method("POST")
                .uri(format!("http://{ingress_addr}/Echo"))
                .body(Full::new(Bytes::from(req_body.encode_to_vec())))?,
        )
        .await?;

    assert_eq!(resp.status(), 200);
    let body_bytes = resp.into_body().collect().await?.to_bytes();
    let echo_resp = proto::EchoResponse::decode(body_bytes)?;
    assert_eq!(echo_resp.message, "echo:hello from outside");
    Ok(())
}
