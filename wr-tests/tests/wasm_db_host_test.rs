mod helpers;

use std::sync::OnceLock;

use anyhow::Result;
use opentelemetry::{trace::TracerProvider as _, Value};
use opentelemetry_sdk::trace::{
    in_memory_exporter::InMemorySpanExporter, SdkTracerProvider, SpanData,
};
use prost::Message;
use tracing_subscriber::layer::SubscriberExt as _;
use wr_engine::config::ResourceLimits;

use helpers::{
    db::{
        db_state_for_module, db_state_for_module_with_active_span, db_state_for_module_with_limits,
        skip_without_db,
    },
    proto,
    wasm::{GuestHarness, RpcPath, TestGuest},
};

struct CapturedTelemetry {
    exporter: InMemorySpanExporter,
    provider: SdkTracerProvider,
    dispatch: tracing::Dispatch,
}

static CAPTURED_TELEMETRY: OnceLock<CapturedTelemetry> = OnceLock::new();

fn captured_telemetry() -> &'static CapturedTelemetry {
    CAPTURED_TELEMETRY.get_or_init(|| {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("wr-tests-wasm-db");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
        let dispatch = tracing::Dispatch::new(subscriber);
        tracing::dispatcher::set_global_default(dispatch.clone())
            .expect("wasm_db_host_test installs the global tracing dispatcher once");
        CapturedTelemetry {
            exporter,
            provider,
            dispatch,
        }
    })
}

fn attribute<'a>(span: &'a SpanData, key: &str) -> Option<&'a Value> {
    span.attributes
        .iter()
        .find(|attribute| attribute.key.as_str() == key)
        .map(|attribute| &attribute.value)
}

fn string_attribute<'a>(span: &'a SpanData, key: &str) -> Option<&'a str> {
    match attribute(span, key) {
        Some(Value::String(value)) => Some(value.as_str()),
        _ => None,
    }
}

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
        params: vec![],
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
        params: vec![],
    };
    let body: proto::QueryResponse = harness
        .dispatch_typed(state, RpcPath::new("/Query")?, req)
        .await?;
    assert_eq!(body.rows.len(), 1);
    assert_eq!(body.rows[0].columns.len(), 1);
    assert_eq!(
        body.rows[0].columns[0].value,
        Some(proto::db_column::Value::Integer(42))
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn wasm_db_builder_api() -> Result<()> {
    if skip_without_db("wasm_db_builder_api") {
        return Ok(());
    }
    let Some(harness) = GuestHarness::load(TestGuest::Db).await? else {
        return Ok(());
    };
    let telemetry = captured_telemetry();
    let (raw_parent, builder_parent) =
        tracing::dispatcher::with_default(&telemetry.dispatch, || {
            (
                tracing::info_span!("raw-db-request", "otel.name" = "raw-db-request"),
                tracing::info_span!("builder-db-request", "otel.name" = "builder-db-request"),
            )
        });

    let raw_state = db_state_for_module_with_active_span(
        2,
        "test-ns",
        "db-builder-raw-test",
        raw_parent.clone(),
    )
    .await;
    let raw: proto::QueryResponse = harness
        .dispatch_typed(
            raw_state,
            RpcPath::new("/Query")?,
            proto::QueryRequest {
                sql: "SELECT 42 AS value".into(),
                params: vec![],
            },
        )
        .await?;
    assert_eq!(raw.rows.len(), 1);

    let builder_state = db_state_for_module_with_active_span(
        2,
        "test-ns",
        "db-builder-api-test",
        builder_parent.clone(),
    )
    .await;
    let body: proto::BuilderApiResponse = harness
        .dispatch_typed(
            builder_state,
            RpcPath::new("/BuilderApi")?,
            proto::BuilderApiRequest {
                table_name: "builder_api_wasm".into(),
            },
        )
        .await?;

    assert_eq!(body.raw_value, 7);
    assert_eq!(body.typed_label, "typed");
    assert_eq!(
        body.scalar_value, 42,
        "bind order must follow placeholder order"
    );
    assert!(body.optional_missing);
    assert_eq!(body.first_value, 11);
    assert_eq!(body.all_count, 3);
    assert_eq!(body.exactly_one_actual, 2);
    assert!(body.execution_failed);
    assert_eq!(body.bind_error_parameter, 1);
    assert_eq!(
        body.side_effect_count, 0,
        "local encoding failure must not execute its INSERT"
    );
    assert_eq!(body.transaction_count, 1);
    assert_eq!(body.streamed_before_drop, 1);

    drop(raw_parent);
    drop(builder_parent);
    telemetry.provider.force_flush().expect("flush spans");
    let spans = telemetry
        .exporter
        .get_finished_spans()
        .expect("captured spans");
    let raw_parent = spans
        .iter()
        .find(|span| span.name == "raw-db-request")
        .expect("raw request span");
    let builder_parent = spans
        .iter()
        .find(|span| span.name == "builder-db-request")
        .expect("builder request span");
    let database_spans: Vec<_> = spans
        .iter()
        .filter(|span| string_attribute(span, "db.system.name") == Some("postgresql"))
        .collect();
    let raw_spans: Vec<_> = database_spans
        .iter()
        .copied()
        .filter(|span| span.parent_span_id == raw_parent.span_context.span_id())
        .collect();
    let builder_spans: Vec<_> = database_spans
        .iter()
        .copied()
        .filter(|span| span.parent_span_id == builder_parent.span_context.span_id())
        .collect();
    let captured = spans
        .iter()
        .map(|span| {
            format!(
                "name={:?} span_id={:?} parent_span_id={:?} attributes={:?} status={:?}",
                span.name,
                span.span_context.span_id(),
                span.parent_span_id,
                span.attributes,
                span.status
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    assert_eq!(
        raw_spans.len(),
        1,
        "one raw-WIT query host span; raw_parent_span_id={:?}; \
         builder_parent_span_id={:?}; captured spans:\n{captured}",
        raw_parent.span_context.span_id(),
        builder_parent.span_context.span_id()
    );
    assert_eq!(
        raw_spans[0].parent_span_id,
        raw_parent.span_context.span_id()
    );
    assert_eq!(
        string_attribute(raw_spans[0], "db.operation.name"),
        Some("query")
    );
    assert_eq!(
        attribute(raw_spans[0], "db.response.returned_rows"),
        Some(&Value::I64(1))
    );
    assert!(attribute(raw_spans[0], "db.query.text").is_none());
    assert!(
        builder_spans.len() >= 10,
        "builder calls must reach shared host seams; raw_parent_span_id={:?}; \
         builder_parent_span_id={:?}; captured spans:\n{captured}",
        raw_parent.span_context.span_id(),
        builder_parent.span_context.span_id()
    );
    for span in &builder_spans {
        assert_eq!(span.parent_span_id, builder_parent.span_context.span_id());
        assert!(attribute(span, "db.query.text").is_none());
        assert_eq!(
            string_attribute(span, "db.namespace"),
            Some("wr__test_ns__db_builder_api_test")
        );
    }
    for operation in [
        "query",
        "execute",
        "transaction.execute",
        "transaction.stream",
    ] {
        assert!(
            builder_spans
                .iter()
                .any(|span| string_attribute(span, "db.operation.name") == Some(operation)),
            "missing builder host operation span: {operation}"
        );
    }
    let partial_stream = builder_spans
        .iter()
        .find(|span| string_attribute(span, "db.operation.name") == Some("transaction.stream"))
        .expect("transaction stream span");
    assert_eq!(
        attribute(partial_stream, "db.response.returned_rows"),
        Some(&Value::I64(2)),
        "host telemetry counts the full two-row batch delivered before guest drop"
    );
    assert_eq!(
        string_attribute(partial_stream, "error.type"),
        Some("cancelled")
    );
    assert!(builder_spans.iter().any(|span| {
        string_attribute(span, "error.type") == Some("query")
            && matches!(&span.status, opentelemetry::trace::Status::Error { .. })
    }));
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
    let row = body.row.expect("typed row");
    let types: Vec<_> = row
        .columns
        .iter()
        .map(|column| column.type_name.as_str())
        .collect();
    assert!(types.contains(&"boolean"));
    assert!(types.contains(&"int4"));
    assert!(types.contains(&"int8"));
    assert!(types.contains(&"float8"));
    assert!(row
        .columns
        .iter()
        .any(|column| { column.value == Some(proto::db_column::Value::Text("hello".into())) }));
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
async fn wasm_db_transaction_rejects_use_after_completion() -> Result<()> {
    if skip_without_db("wasm_db_transaction_rejects_use_after_completion") {
        return Ok(());
    }
    let Some(harness) = GuestHarness::load(TestGuest::Db).await? else {
        return Ok(());
    };

    for rollback in [false, true] {
        for operation in ["query", "execute", "query-stream", "commit", "rollback"] {
            let state = db_state_for_module(2, "test-ns", "db-txdone-test").await;
            let response: proto::TransactionAfterCompleteResponse = harness
                .dispatch_typed(
                    state,
                    RpcPath::new("/TransactionAfterComplete")?,
                    proto::TransactionAfterCompleteRequest {
                        rollback,
                        operation: operation.into(),
                    },
                )
                .await?;
            assert!(
                response
                    .error_message
                    .contains("transaction already completed"),
                "{operation} after completion returned: {}",
                response.error_message
            );
        }
    }
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
        params: vec![],
        operation: "query".into(),
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
        params: vec![proto::DbParam {
            value: Some(proto::db_param::Value::Numeric("not-a-number".into())),
        }],
        operation: "query".into(),
    };
    let resp = harness.dispatch(state, "/Error", req).await?;
    assert_eq!(resp.status(), 200);

    let body = proto::ErrorResponse::decode(resp.into_body())?;
    assert_eq!(body.error_kind, "query");
    assert!(!body.error_message.is_empty());
    Ok(())
}

#[tokio::test]
async fn wasm_db_typed_null_roundtrip() -> Result<()> {
    if skip_without_db("wasm_db_typed_null_roundtrip") {
        return Ok(());
    }
    let Some(harness) = GuestHarness::load(TestGuest::Db).await? else {
        return Ok(());
    };
    let state = db_state_for_module(1, "test-ns", "db-null-test").await;
    let response: proto::QueryResponse = harness
        .dispatch_typed(
            state,
            RpcPath::new("/Query")?,
            proto::QueryRequest {
                sql: "SELECT $1::text AS value".into(),
                params: vec![proto::DbParam {
                    value: Some(proto::db_param::Value::NullType("text".into())),
                }],
            },
        )
        .await?;

    let column = &response.rows[0].columns[0];
    assert_eq!(column.type_name, "null");
    assert_eq!(
        column.value,
        Some(proto::db_column::Value::NullType("text".into()))
    );
    Ok(())
}

#[tokio::test]
async fn wasm_db_unsupported_result_type_is_contextual_on_every_query_path() -> Result<()> {
    if skip_without_db("wasm_db_unsupported_result_type_is_contextual_on_every_query_path") {
        return Ok(());
    }
    let Some(harness) = GuestHarness::load(TestGuest::Db).await? else {
        return Ok(());
    };

    for operation in ["query", "transaction", "stream", "transaction-stream"] {
        let state = db_state_for_module(2, "test-ns", "db-unsupported-test").await;
        let response: proto::ErrorResponse = harness
            .dispatch_typed(
                state,
                RpcPath::new("/Error")?,
                proto::ErrorRequest {
                    sql: "SELECT '127.0.0.1'::inet AS network".into(),
                    params: vec![],
                    operation: operation.into(),
                },
            )
            .await?;

        assert_eq!(
            response.error_kind, "unsupported-result-type",
            "{operation}"
        );
        assert_eq!(response.column_name, "network", "{operation}");
        assert_eq!(response.column_index, 0, "{operation}");
        assert_eq!(response.postgres_type_name, "inet", "{operation}");
        assert_eq!(response.postgres_type_oid, 869, "{operation}");
        assert!(response.error_message.contains("network"), "{operation}");
        assert!(response.error_message.contains("inet"), "{operation}");
    }
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
        params: vec![],
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
