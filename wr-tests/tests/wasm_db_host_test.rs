mod helpers;

use anyhow::Result;
use prost::Message;
use wr_engine::config::ResourceLimits;

use helpers::{
    db::{db_state_for_module, db_state_for_module_with_limits, skip_without_db},
    proto,
    wasm::{GuestHarness, TestGuest},
};

#[tokio::test]
async fn wasm_db_execute() -> Result<()> {
    if skip_without_db("wasm_db_execute") {
        return Ok(());
    }
    let Some(harness) = GuestHarness::load(TestGuest::Db).await? else {
        return Ok(());
    };
    let state = db_state_for_module(1, "test-ns", "db-test").await;

    // Create a table via Execute
    let req = proto::ExecuteRequest {
        sql: "CREATE TEMP TABLE IF NOT EXISTS exec_test (id integer)".into(),
        params_json: "".into(),
    };
    let resp = harness.dispatch(state, "/Execute", req).await?;
    assert_eq!(resp.status(), 200);

    let body = proto::ExecuteResponse::decode(resp.into_body())?;
    // CREATE TABLE doesn't affect rows
    assert_eq!(body.affected, 0);
    Ok(())
}

#[tokio::test]
async fn wasm_db_query() -> Result<()> {
    if skip_without_db("wasm_db_query") {
        return Ok(());
    }
    let Some(harness) = GuestHarness::load(TestGuest::Db).await? else {
        return Ok(());
    };
    let state = db_state_for_module(1, "test-ns", "db-query-test").await;

    let req = proto::QueryRequest {
        sql: "SELECT 42 as num".into(),
        params_json: "".into(),
    };
    let resp = harness.dispatch(state, "/Query", req).await?;
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
    if skip_without_db("wasm_db_query_types") {
        return Ok(());
    }
    let Some(harness) = GuestHarness::load(TestGuest::Db).await? else {
        return Ok(());
    };
    let state = db_state_for_module(1, "test-ns", "db-types-test").await;

    let req = proto::QueryTypesRequest {};
    let resp = harness.dispatch(state, "/QueryTypes", req).await?;
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
    if skip_without_db("wasm_db_transaction_commit") {
        return Ok(());
    }
    let Some(harness) = GuestHarness::load(TestGuest::Db).await? else {
        return Ok(());
    };
    let state = db_state_for_module(2, "test-ns", "db-txcommit-test").await;

    let req = proto::TransactionCommitRequest {
        table_name: "tx_commit_wasm".into(),
    };
    let resp = harness.dispatch(state, "/TransactionCommit", req).await?;
    assert_eq!(resp.status(), 200);

    let body = proto::TransactionCommitResponse::decode(resp.into_body())?;
    assert_eq!(body.count, 1, "committed row should be visible");
    Ok(())
}

#[tokio::test]
async fn wasm_db_transaction_rollback() -> Result<()> {
    if skip_without_db("wasm_db_transaction_rollback") {
        return Ok(());
    }
    let Some(harness) = GuestHarness::load(TestGuest::Db).await? else {
        return Ok(());
    };
    let state = db_state_for_module(2, "test-ns", "db-txrollback-test").await;

    let req = proto::TransactionRollbackRequest {
        table_name: "tx_rollback_wasm".into(),
    };
    let resp = harness.dispatch(state, "/TransactionRollback", req).await?;
    assert_eq!(resp.status(), 200);

    let body = proto::TransactionRollbackResponse::decode(resp.into_body())?;
    assert_eq!(body.count, 0, "rolled-back row should not be visible");
    Ok(())
}

#[tokio::test]
async fn wasm_db_transaction_drop() -> Result<()> {
    if skip_without_db("wasm_db_transaction_drop") {
        return Ok(());
    }
    let Some(harness) = GuestHarness::load(TestGuest::Db).await? else {
        return Ok(());
    };
    let state = db_state_for_module(2, "test-ns", "db-txdrop-test").await;

    let req = proto::TransactionDropRequest {
        table_name: "tx_drop_wasm".into(),
    };
    let resp = harness.dispatch(state, "/TransactionDrop", req).await?;
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
    if skip_without_db("wasm_db_error") {
        return Ok(());
    }
    let Some(harness) = GuestHarness::load(TestGuest::Db).await? else {
        return Ok(());
    };
    let state = db_state_for_module(1, "test-ns", "db-error-test").await;

    let req = proto::ErrorRequest {
        sql: "SELECT * FROM nonexistent_table_xyz".into(),
        params_json: "".into(),
    };
    let resp = harness.dispatch(state, "/Error", req).await?;
    assert_eq!(resp.status(), 200);

    let body = proto::ErrorResponse::decode(resp.into_body())?;
    assert_eq!(body.error_kind, "query");
    assert!(!body.error_message.is_empty());
    Ok(())
}

#[tokio::test]
async fn wasm_db_invalid_param() -> Result<()> {
    if skip_without_db("wasm_db_invalid_param") {
        return Ok(());
    }
    let Some(harness) = GuestHarness::load(TestGuest::Db).await? else {
        return Ok(());
    };
    let state = db_state_for_module(1, "test-ns", "db-invalid-param-test").await;

    let req = proto::ErrorRequest {
        sql: "SELECT $1::numeric AS n".into(),
        params_json: r#"[{"type":"numeric","value":"not-a-number"}]"#.into(),
    };
    let resp = harness.dispatch(state, "/Error", req).await?;
    assert_eq!(resp.status(), 200);

    let body = proto::ErrorResponse::decode(resp.into_body())?;
    assert_eq!(body.error_kind, "query");
    assert!(!body.error_message.is_empty());
    Ok(())
}

#[tokio::test]
async fn wasm_db_query_stream() -> Result<()> {
    if skip_without_db("wasm_db_query_stream") {
        return Ok(());
    }
    let Some(harness) = GuestHarness::load(TestGuest::Db).await? else {
        return Ok(());
    };
    let state = db_state_for_module(1, "test-ns", "db-stream-test").await;

    let req = proto::QueryStreamRequest {
        sql: "SELECT generate_series(1, 5) AS n".into(),
        params_json: "".into(),
        batch_size: 2,
    };
    let resp = harness.dispatch(state, "/QueryStream", req).await?;
    assert_eq!(resp.status(), 200);

    let body = proto::QueryStreamResponse::decode(resp.into_body())?;
    assert_eq!(body.rows.len(), 5);
    // With batch_size=2 and 5 rows: batches of 2, 2, 1, then empty = 4 batches
    assert_eq!(body.batch_count, 4);
    Ok(())
}

#[tokio::test]
async fn wasm_db_query_stream_drop() -> Result<()> {
    if skip_without_db("wasm_db_query_stream_drop") {
        return Ok(());
    }
    let Some(harness) = GuestHarness::load(TestGuest::Db).await? else {
        return Ok(());
    };
    let state = db_state_for_module(1, "test-ns", "db-stream-drop-test").await;

    let req = proto::QueryStreamDropRequest {
        sql: "SELECT generate_series(1, 100) AS n".into(),
        fetch_count: 5,
    };
    let resp = harness.dispatch(state, "/QueryStreamDrop", req).await?;
    assert_eq!(resp.status(), 200);

    let body = proto::QueryStreamDropResponse::decode(resp.into_body())?;
    assert_eq!(body.fetched, 5);
    Ok(())
}

#[tokio::test]
async fn wasm_db_resource_caps() -> Result<()> {
    if skip_without_db("wasm_db_resource_caps") {
        return Ok(());
    }
    let Some(harness) = GuestHarness::load(TestGuest::Db).await? else {
        return Ok(());
    };
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
        let resp = harness.dispatch(state, path, req).await?;
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
        let resp = harness.dispatch(state, path, req).await?;
        assert_eq!(resp.status(), 200);
        let body = proto::AllocResourcesResponse::decode(resp.into_body())?;
        assert_eq!(body.held, 2, "path={path}");
        assert!(!body.hit_cap, "path={path}");
    }

    Ok(())
}
