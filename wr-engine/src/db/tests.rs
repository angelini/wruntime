use std::sync::{Arc, OnceLock};

use opentelemetry::{trace::TracerProvider as _, Value};
use opentelemetry_sdk::trace::{
    in_memory_exporter::InMemorySpanExporter, SdkTracerProvider, SpanData,
};
use tracing_subscriber::layer::SubscriberExt as _;

use super::wruntime::db::database::{DbError, Host, HostRowCursor, PgInterval, PgType, PgValue};
use crate::state::{ModuleServices, ModuleState};

fn proxy_uri() -> hyper::Uri {
    "http://127.0.0.1:9001".parse().unwrap()
}

fn test_http_pool() -> wr_common::http_pool::HttpClientPool<http_body_util::Full<bytes::Bytes>> {
    wr_common::http_pool::HttpClientPool::new(1)
}

struct CapturedTelemetry {
    exporter: InMemorySpanExporter,
    provider: SdkTracerProvider,
    dispatch: tracing::Dispatch,
}

static CAPTURED_TELEMETRY: OnceLock<CapturedTelemetry> = OnceLock::new();
static CAPTURED_TELEMETRY_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn captured_telemetry() -> &'static CapturedTelemetry {
    CAPTURED_TELEMETRY.get_or_init(|| {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("wr-engine-db-tests");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
        let dispatch = tracing::Dispatch::new(subscriber);
        tracing::dispatcher::set_global_default(dispatch.clone())
            .expect("wr-engine lib tests install the global tracing dispatcher once");
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

// ── no-pool tests ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_query_returns_error_when_no_pool() {
    let mut state = ModuleState::new(
        "test".into(),
        "test".into(),
        proxy_uri(),
        test_http_pool(),
        Default::default(),
    )
    .expect("state");
    let result = state.query("SELECT 1".into(), vec![]).await;
    assert!(
        matches!(result, Err(DbError::Connection(_))),
        "expected Connection error, got {result:?}",
    );
}

#[tokio::test]
async fn test_execute_returns_error_when_no_pool() {
    let mut state = ModuleState::new(
        "test".into(),
        "test".into(),
        proxy_uri(),
        test_http_pool(),
        Default::default(),
    )
    .expect("state");
    let result = state.execute("SELECT 1".into(), vec![]).await;
    assert!(
        matches!(result, Err(DbError::Connection(_))),
        "expected Connection error, got {result:?}",
    );
}

#[tokio::test]
async fn test_begin_transaction_returns_error_when_no_pool() {
    let mut state = ModuleState::new(
        "test".into(),
        "test".into(),
        proxy_uri(),
        test_http_pool(),
        Default::default(),
    )
    .expect("state");
    let result = state.begin_transaction().await;
    assert!(
        matches!(result, Err(DbError::Connection(_))),
        "expected Connection error, got {result:?}",
    );
}

#[tokio::test]
async fn test_query_stream_returns_error_when_no_pool() {
    let mut state = ModuleState::new(
        "test".into(),
        "test".into(),
        proxy_uri(),
        test_http_pool(),
        Default::default(),
    )
    .expect("state");
    let result = state.query_stream("SELECT 1".into(), vec![]).await;
    assert!(
        matches!(result, Err(DbError::Connection(_))),
        "expected Connection error, got {result:?}",
    );
}

#[tokio::test(flavor = "current_thread")]
async fn database_capability_error_is_parented_and_query_text_defaults_off() {
    let _capture_guard = CAPTURED_TELEMETRY_LOCK.lock().await;
    let telemetry = captured_telemetry();
    let request = tracing::dispatcher::with_default(&telemetry.dispatch, || {
        tracing::info_span!(
            "db-capability-request",
            "otel.name" = "db-capability-request"
        )
    });
    let mut state = ModuleState::new(
        "test".into(),
        "test".into(),
        proxy_uri(),
        test_http_pool(),
        ModuleServices {
            active_span: request.clone(),
            ..Default::default()
        },
    )
    .expect("state");

    let result = state.query("SELECT secret_literal".into(), vec![]).await;
    assert!(matches!(result, Err(DbError::Connection(_))));
    drop(state);
    drop(request);
    telemetry.provider.force_flush().expect("flush spans");

    let spans = telemetry
        .exporter
        .get_finished_spans()
        .expect("captured spans");
    let request = spans
        .iter()
        .find(|span| span.name == "db-capability-request")
        .expect("capability request span");
    let query = spans
        .iter()
        .find(|span| {
            span.parent_span_id == request.span_context.span_id()
                && string_attribute(span, "db.operation.name") == Some("query")
        })
        .unwrap_or_else(|| {
            let captured = spans
                .iter()
                .map(|span| {
                    format!(
                        "name={:?} attributes={:?} status={:?}",
                        span.name, span.attributes, span.status
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");
            panic!(
                "query operation span missing; captured spans (name, attributes, status):\n{captured}"
            );
        });
    assert_eq!(query.parent_span_id, request.span_context.span_id());
    assert_eq!(
        string_attribute(query, "db.system.name"),
        Some("postgresql")
    );
    assert_eq!(string_attribute(query, "db.operation.name"), Some("query"));
    assert_eq!(string_attribute(query, "error.type"), Some("connection"));
    assert_eq!(
        attribute(query, "db.response.returned_rows"),
        Some(&Value::I64(0))
    );
    assert!(attribute(query, "db.query.text").is_none());
    assert!(matches!(
        &query.status,
        opentelemetry::trace::Status::Error { .. }
    ));
}

// ── real-Postgres tests ───────────────────────────────────────────────────

/// Skip the test if `WRT_TEST_DB_URL` is not set.
fn db_url() -> Option<String> {
    std::env::var("WRT_TEST_DB_URL").ok()
}

#[tokio::test(flavor = "multi_thread")]
async fn guest_pool_recycling_cleans_or_replaces_sessions() {
    let Some(url) = db_url() else { return };
    let admin_pool = crate::pool::build_pool(&url, 2).expect("admin pool");
    let admin = admin_pool.get().await.expect("admin connection");
    let role = format!("wr_clean_{}", uuid::Uuid::new_v4().simple());
    let password = "wr-clean-test-password";
    admin
        .batch_execute(&format!(
            "CREATE ROLE \"{role}\" LOGIN PASSWORD '{password}'"
        ))
        .await
        .expect("create guest role");

    let pool = crate::pool::build_guest_pool(&url, &role, password, 1).expect("guest pool");
    let schema = Some(Arc::<str>::from("public"));
    let timeouts = crate::state::DbTimeouts {
        statement_timeout_secs: 7,
        idle_in_transaction_timeout_secs: 11,
    };
    let first = pool.get().await.expect("first checkout");
    first
        .batch_execute(
            "BEGIN; SET application_name = 'dirty'; \
             CREATE TEMP TABLE wr_open_transaction (value INT)",
        )
        .await
        .expect("dirty transaction");
    drop(first);
    let replacement = super::connection::get_prepared_connection(&pool, &schema, &timeouts)
        .await
        .expect("checkout after raw transaction");
    assert!(replacement
        .query_one(
            "SELECT to_regclass('pg_temp.wr_open_transaction') IS NULL",
            &[],
        )
        .await
        .unwrap()
        .get::<_, bool>(0));
    replacement
        .batch_execute(
            "SET application_name = 'dirty'; \
             LISTEN wr_clean_test; \
             SELECT pg_advisory_lock(987654321); \
             CREATE TEMP TABLE wr_clean_temp (value INT); \
             CREATE TEMP SEQUENCE wr_clean_sequence",
        )
        .await
        .expect("dirty session state");
    drop(replacement);

    let clean = super::connection::get_prepared_connection(&pool, &schema, &timeouts)
        .await
        .expect("clean checkout");
    assert_eq!(
        clean
            .query_one("SHOW application_name", &[])
            .await
            .unwrap()
            .get::<_, &str>(0),
        ""
    );
    assert!(clean
        .query_one("SELECT pg_try_advisory_lock(987654321)", &[])
        .await
        .unwrap()
        .get::<_, bool>(0));
    assert_eq!(
        clean
            .query_one("SELECT count(*) FROM pg_listening_channels()", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    assert!(clean
        .query_one("SELECT to_regclass('pg_temp.wr_clean_temp') IS NULL", &[])
        .await
        .unwrap()
        .get::<_, bool>(0));
    clean.batch_execute("RESET ALL").await.unwrap();
    drop(clean);

    let prepared = super::connection::get_prepared_connection(&pool, &schema, &timeouts)
        .await
        .expect("prepared checkout");
    assert_eq!(
        prepared
            .query_one("SHOW search_path", &[])
            .await
            .unwrap()
            .get::<_, &str>(0),
        "public"
    );
    assert_eq!(
        prepared
            .query_one("SHOW statement_timeout", &[])
            .await
            .unwrap()
            .get::<_, &str>(0),
        "7s"
    );
    assert_eq!(
        prepared
            .query_one("SHOW idle_in_transaction_session_timeout", &[])
            .await
            .unwrap()
            .get::<_, &str>(0),
        "11s"
    );
    drop(prepared);
    drop(pool);
    admin
        .execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = $1 AND pid <> pg_backend_pid()",
            &[&role],
        )
        .await
        .unwrap();
    admin
        .batch_execute(&format!("DROP ROLE \"{role}\""))
        .await
        .expect("drop guest role");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_with_postgres() {
    let url = match db_url() {
        Some(u) => u,
        None => return,
    };

    let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
    let mut state = ModuleState::new(
        "test".into(),
        "test".into(),
        proxy_uri(),
        test_http_pool(),
        ModuleServices {
            db_pool: Some(Arc::new(pool)),
            db_schema: Some(Arc::from("public")),
            ..Default::default()
        },
    )
    .expect("state");

    let rows = state
        .query(
            "SELECT $1::text AS echo".into(),
            vec![PgValue::Text("hello".into())],
        )
        .await
        .expect("query");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns[0].name, "echo");
    assert_eq!(rows[0].columns[0].value, PgValue::Text("hello".into()));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_execute_with_postgres() {
    let url = match db_url() {
        Some(u) => u,
        None => return,
    };

    let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
    let mut state = ModuleState::new(
        "test".into(),
        "test".into(),
        proxy_uri(),
        test_http_pool(),
        ModuleServices {
            db_pool: Some(Arc::new(pool)),
            db_schema: Some(Arc::from("public")),
            ..Default::default()
        },
    )
    .expect("state");

    // DDL returns 0 rows affected.
    let n = state
        .execute("CREATE TEMP TABLE _wr_db_test (id INT)".into(), vec![])
        .await
        .expect("create table");
    assert_eq!(n, 0);

    // DML returns the actual affected-row count.
    let n = state
        .execute("INSERT INTO _wr_db_test VALUES (1)".into(), vec![])
        .await
        .expect("insert");
    assert_eq!(n, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_parameterised_with_postgres() {
    let url = match db_url() {
        Some(u) => u,
        None => return,
    };

    let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
    let mut state = ModuleState::new(
        "test".into(),
        "test".into(),
        proxy_uri(),
        test_http_pool(),
        ModuleServices {
            db_pool: Some(Arc::new(pool)),
            db_schema: Some(Arc::from("public")),
            ..Default::default()
        },
    )
    .expect("state");

    let rows = state
        .query(
            "SELECT $1::text AS a, $2::text AS b".into(),
            vec![PgValue::Text("foo".into()), PgValue::Text("bar".into())],
        )
        .await
        .expect("query");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns[0].name, "a");
    assert_eq!(rows[0].columns[0].value, PgValue::Text("foo".into()));
    assert_eq!(rows[0].columns[1].name, "b");
    assert_eq!(rows[0].columns[1].value, PgValue::Text("bar".into()));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_typed_columns_with_postgres() {
    let url = match db_url() {
        Some(u) => u,
        None => return,
    };

    let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
    let mut state = ModuleState::new(
        "test".into(),
        "test".into(),
        proxy_uri(),
        test_http_pool(),
        ModuleServices {
            db_pool: Some(Arc::new(pool)),
            db_schema: Some(Arc::from("public")),
            ..Default::default()
        },
    )
    .expect("state");

    let rows = state
        .query(
            "SELECT \
                true::bool       AS b, \
                42::int2         AS i2, \
                1000::int4       AS i4, \
                9999999999::int8 AS i8, \
                1.5::float4      AS f4, \
                2.5::float8      AS f8, \
                NULL::text       AS n"
                .into(),
            vec![],
        )
        .await
        .expect("query");

    assert_eq!(rows.len(), 1);
    let cols = &rows[0].columns;
    assert_eq!(cols[0].value, PgValue::Boolean(true));
    assert_eq!(cols[1].value, PgValue::Int2(42));
    assert_eq!(cols[2].value, PgValue::Int4(1000));
    assert_eq!(cols[3].value, PgValue::Int8(9_999_999_999));
    assert_eq!(cols[4].value, PgValue::Float4(1.5));
    assert_eq!(cols[5].value, PgValue::Float8(2.5));
    assert_eq!(cols[6].value, PgValue::Null(PgType::Text));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_every_supported_typed_null_roundtrips_with_its_postgres_type() {
    let url = match db_url() {
        Some(url) => url,
        None => return,
    };
    let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
    let mut state = ModuleState::new(
        "test".into(),
        "test".into(),
        proxy_uri(),
        test_http_pool(),
        ModuleServices {
            db_pool: Some(Arc::new(pool)),
            db_schema: Some(Arc::from("public")),
            ..Default::default()
        },
    )
    .expect("state");

    let cases = [
        (PgType::Boolean, "bool"),
        (PgType::Int2, "int2"),
        (PgType::Int4, "int4"),
        (PgType::Int8, "int8"),
        (PgType::Float4, "float4"),
        (PgType::Float8, "float8"),
        (PgType::Text, "text"),
        (PgType::Bytea, "bytea"),
        (PgType::Timestamptz, "timestamptz"),
        (PgType::Timestamp, "timestamp"),
        (PgType::Date, "date"),
        (PgType::Time, "time"),
        (PgType::Interval, "interval"),
        (PgType::Numeric, "numeric"),
        (PgType::Uuid, "uuid"),
        (PgType::Jsonb, "jsonb"),
        (PgType::Oid, "oid"),
        (PgType::BoolArray, "bool[]"),
        (PgType::Int2Array, "int2[]"),
        (PgType::Int4Array, "int4[]"),
        (PgType::Int8Array, "int8[]"),
        (PgType::Float4Array, "float4[]"),
        (PgType::Float8Array, "float8[]"),
        (PgType::TextArray, "text[]"),
        (PgType::TimestamptzArray, "timestamptz[]"),
        (PgType::TimestampArray, "timestamp[]"),
        (PgType::UuidArray, "uuid[]"),
        (PgType::JsonbArray, "jsonb[]"),
    ];

    for (pg_type, postgres_type) in cases {
        let rows = state
            .query(
                format!("SELECT $1::{postgres_type} AS value"),
                vec![PgValue::Null(pg_type)],
            )
            .await
            .unwrap_or_else(|error| panic!("{postgres_type} typed null failed: {error:?}"));
        assert_eq!(rows[0].columns[0].value, PgValue::Null(pg_type));
    }

    let error = state
        .query(
            "SELECT $1::int4 AS value".into(),
            vec![PgValue::Null(PgType::Text)],
        )
        .await
        .expect_err("text null must not bind in an int4 context");
    assert!(matches!(error, DbError::Query(_)));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_unsupported_result_context_propagates_and_cursors_remain_droppable() {
    let url = match db_url() {
        Some(url) => url,
        None => return,
    };
    let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
    let mut state = ModuleState::new(
        "test".into(),
        "test".into(),
        proxy_uri(),
        test_http_pool(),
        ModuleServices {
            db_pool: Some(Arc::new(pool)),
            db_schema: Some(Arc::from("public")),
            ..Default::default()
        },
    )
    .expect("state");

    let domain_rows = state
        .query(
            "SELECT 'safe-domain'::information_schema.sql_identifier AS value".into(),
            vec![],
        )
        .await
        .expect("supported domain query");
    assert_eq!(
        domain_rows[0].columns[0].value,
        PgValue::Text("safe-domain".into())
    );

    let assert_context = |error: DbError| match error {
        DbError::UnsupportedResultType(details) => {
            assert_eq!(details.column_name.as_deref(), Some("network"));
            assert_eq!(details.column_index, 0);
            assert_eq!(details.postgres_type_name, "inet");
            assert_eq!(details.postgres_type_oid, 869);
        }
        other => panic!("expected unsupported result type, got {other:?}"),
    };

    let error = state
        .query("SELECT '127.0.0.1'::inet AS network".into(), vec![])
        .await
        .expect_err("ordinary query must reject inet");
    assert_context(error);

    let cursor = state
        .query_stream("SELECT '127.0.0.1'::inet AS network".into(), vec![])
        .await
        .expect("open ordinary cursor");
    let cursor_rep = cursor.rep();
    let error = HostRowCursor::next_batch(
        &mut state,
        wasmtime::component::Resource::new_borrow(cursor_rep),
        1,
    )
    .await
    .expect_err("ordinary stream must reject inet");
    assert_context(error);
    HostRowCursor::drop(&mut state, cursor)
        .await
        .expect("drop failed ordinary cursor");

    let tx = state.begin_transaction().await.expect("begin transaction");
    let tx_rep = tx.rep();
    let error = super::wruntime::db::database::HostTransaction::query(
        &mut state,
        wasmtime::component::Resource::new_borrow(tx_rep),
        "SELECT '127.0.0.1'::inet AS network".into(),
        vec![],
    )
    .await
    .expect_err("transaction query must reject inet");
    assert_context(error);

    let cursor = super::wruntime::db::database::HostTransaction::query_stream(
        &mut state,
        wasmtime::component::Resource::new_borrow(tx_rep),
        "SELECT '127.0.0.1'::inet AS network".into(),
        vec![],
    )
    .await
    .expect("open transaction cursor");
    let cursor_rep = cursor.rep();
    let error = HostRowCursor::next_batch(
        &mut state,
        wasmtime::component::Resource::new_borrow(cursor_rep),
        1,
    )
    .await
    .expect_err("transaction stream must reject inet");
    assert_context(error);
    HostRowCursor::drop(&mut state, cursor)
        .await
        .expect("drop failed transaction cursor");
    super::wruntime::db::database::HostTransaction::rollback(
        &mut state,
        wasmtime::component::Resource::new_borrow(tx_rep),
    )
    .await
    .expect("rollback transaction");
    super::wruntime::db::database::HostTransaction::drop(&mut state, tx)
        .await
        .expect("drop transaction");

    let rows = state
        .query("SELECT 1::int4 AS value".into(), vec![])
        .await
        .expect("connection remains usable after conversion failures");
    assert_eq!(rows[0].columns[0].value, PgValue::Int4(1));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_unsupported_domain_reports_domain_metadata() {
    let url = match db_url() {
        Some(url) => url,
        None => return,
    };
    let pool = crate::pool::build_pool(&url, 1).expect("build_pool");
    let mut state = ModuleState::new(
        "test".into(),
        "test".into(),
        proxy_uri(),
        test_http_pool(),
        ModuleServices {
            db_pool: Some(Arc::new(pool)),
            db_schema: Some(Arc::from("public")),
            ..Default::default()
        },
    )
    .expect("state");

    let suffix = std::process::id();
    let domain_name = format!("_wr_unsupported_inet_domain_{suffix}");
    let table_name = format!("_wr_unsupported_domain_table_{suffix}");
    state
        .execute(format!("DROP TABLE IF EXISTS public.{table_name}"), vec![])
        .await
        .expect("remove stale unsupported-domain table");
    state
        .execute(
            format!("DROP DOMAIN IF EXISTS public.{domain_name}"),
            vec![],
        )
        .await
        .expect("remove stale unsupported domain");
    state
        .execute(
            format!("CREATE DOMAIN public.{domain_name} AS inet"),
            vec![],
        )
        .await
        .expect("create unsupported domain");
    state
        .execute(
            format!("CREATE TABLE public.{table_name} (value public.{domain_name} NOT NULL)"),
            vec![],
        )
        .await
        .expect("create unsupported-domain table");
    state
        .execute(
            format!("INSERT INTO public.{table_name} (value) VALUES ('127.0.0.1')"),
            vec![],
        )
        .await
        .expect("insert unsupported-domain value");

    let domain_result = state
        .query(
            format!("SELECT value AS domain_network FROM public.{table_name}"),
            vec![],
        )
        .await;

    state
        .execute(format!("DROP TABLE IF EXISTS public.{table_name}"), vec![])
        .await
        .expect("clean up unsupported-domain table");
    state
        .execute(
            format!("DROP DOMAIN IF EXISTS public.{domain_name}"),
            vec![],
        )
        .await
        .expect("clean up unsupported domain");

    let error = domain_result.expect_err("unsupported domain must not produce a row");
    match error {
        DbError::UnsupportedResultType(details) => {
            assert_eq!(details.column_name.as_deref(), Some("domain_network"));
            assert_eq!(details.column_index, 0);
            assert_eq!(details.postgres_type_name, "inet");
            assert_eq!(details.postgres_type_oid, 869);
        }
        other => panic!("expected unsupported domain result type, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_transaction_commit() {
    use super::wruntime::db::database::{Host, HostTransaction};

    let url = match db_url() {
        Some(u) => u,
        None => return,
    };

    let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
    let mut state = ModuleState::new(
        "test".into(),
        "test".into(),
        proxy_uri(),
        test_http_pool(),
        ModuleServices {
            db_pool: Some(Arc::new(pool)),
            db_schema: Some(Arc::from("public")),
            ..Default::default()
        },
    )
    .expect("state");

    // Setup: create a temp table outside the transaction.
    Host::execute(
        &mut state,
        "CREATE TEMP TABLE _wr_tx_commit_test (val INT)".into(),
        vec![],
    )
    .await
    .expect("create table");

    let tx = state.begin_transaction().await.expect("begin");
    let rep = tx.rep();

    HostTransaction::execute(
        &mut state,
        wasmtime::component::Resource::new_borrow(rep),
        "INSERT INTO _wr_tx_commit_test VALUES (42)".into(),
        vec![],
    )
    .await
    .expect("insert");

    HostTransaction::commit(&mut state, wasmtime::component::Resource::new_borrow(rep))
        .await
        .expect("commit");

    // Release the resource first so its connection is returned to the pool.
    // done=true means no ROLLBACK is issued.
    HostTransaction::drop(&mut state, tx).await.expect("drop");

    // After the connection is back in the pool, Host::query reacquires it
    // and can see the TEMP TABLE (TEMP tables are connection-scoped).
    let rows = Host::query(
        &mut state,
        "SELECT val FROM _wr_tx_commit_test".into(),
        vec![],
    )
    .await
    .expect("query after commit");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns[0].value, PgValue::Int4(42));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_transaction_rollback() {
    use super::wruntime::db::database::{Host, HostTransaction};

    let url = match db_url() {
        Some(u) => u,
        None => return,
    };

    let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
    let mut state = ModuleState::new(
        "test".into(),
        "test".into(),
        proxy_uri(),
        test_http_pool(),
        ModuleServices {
            db_pool: Some(Arc::new(pool)),
            db_schema: Some(Arc::from("public")),
            ..Default::default()
        },
    )
    .expect("state");

    Host::execute(
        &mut state,
        "CREATE TEMP TABLE _wr_tx_rollback_test (val INT)".into(),
        vec![],
    )
    .await
    .expect("create table");

    let tx = state.begin_transaction().await.expect("begin");
    let rep = tx.rep();

    HostTransaction::execute(
        &mut state,
        wasmtime::component::Resource::new_borrow(rep),
        "INSERT INTO _wr_tx_rollback_test VALUES (99)".into(),
        vec![],
    )
    .await
    .expect("insert");

    HostTransaction::rollback(&mut state, wasmtime::component::Resource::new_borrow(rep))
        .await
        .expect("rollback");

    // Release the resource first so its connection is returned to the pool.
    HostTransaction::drop(&mut state, tx).await.expect("drop");

    // After the connection is back in the pool, Host::query reacquires it
    // and can see the TEMP TABLE with the rolled-back INSERT absent.
    let rows = Host::query(
        &mut state,
        "SELECT val FROM _wr_tx_rollback_test".into(),
        vec![],
    )
    .await
    .expect("query after rollback");
    assert_eq!(rows.len(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn postgres_error_poisoning_prevents_false_commit_success() {
    use super::wruntime::db::database::{Host, HostTransaction};

    let Some(url) = db_url() else { return };
    let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
    let mut state = ModuleState::new(
        "test".into(),
        "test".into(),
        proxy_uri(),
        test_http_pool(),
        ModuleServices {
            db_pool: Some(Arc::new(pool)),
            db_schema: Some(Arc::from("public")),
            ..Default::default()
        },
    )
    .expect("state");
    Host::execute(
        &mut state,
        "CREATE TEMP TABLE _wr_tx_poison_test (val INT)".into(),
        vec![],
    )
    .await
    .expect("create table");

    let tx = state.begin_transaction().await.expect("begin");
    let rep = tx.rep();
    HostTransaction::execute(
        &mut state,
        wasmtime::component::Resource::new_borrow(rep),
        "INSERT INTO _wr_tx_poison_test VALUES (1)".into(),
        vec![],
    )
    .await
    .expect("insert");
    assert!(matches!(
        HostTransaction::query(
            &mut state,
            wasmtime::component::Resource::new_borrow(rep),
            "SELECT 1 / 0".into(),
            vec![],
        )
        .await,
        Err(DbError::Query(_))
    ));
    let commit =
        HostTransaction::commit(&mut state, wasmtime::component::Resource::new_borrow(rep)).await;
    assert!(matches!(commit, Err(DbError::Query(message)) if message.contains("rolled back")));
    HostTransaction::drop(&mut state, tx).await.expect("drop");

    let rows = Host::query(
        &mut state,
        "SELECT val FROM _wr_tx_poison_test".into(),
        vec![],
    )
    .await
    .expect("query after aborted commit");
    assert!(rows.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn local_parameter_and_result_errors_do_not_poison_transactions() {
    use super::wruntime::db::database::{Host, HostTransaction};

    let Some(url) = db_url() else { return };
    let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
    let mut state = ModuleState::new(
        "test".into(),
        "test".into(),
        proxy_uri(),
        test_http_pool(),
        ModuleServices {
            db_pool: Some(Arc::new(pool)),
            db_schema: Some(Arc::from("public")),
            ..Default::default()
        },
    )
    .expect("state");
    Host::execute(
        &mut state,
        "CREATE TEMP TABLE _wr_tx_local_error_test (val INT)".into(),
        vec![],
    )
    .await
    .expect("create table");

    let tx = state.begin_transaction().await.expect("begin");
    let rep = tx.rep();
    assert!(HostTransaction::query(
        &mut state,
        wasmtime::component::Resource::new_borrow(rep),
        "SELECT $1::numeric".into(),
        vec![PgValue::Numeric("not-a-number".into())],
    )
    .await
    .is_err());
    assert!(matches!(
        HostTransaction::query(
            &mut state,
            wasmtime::component::Resource::new_borrow(rep),
            "SELECT '127.0.0.1'::inet".into(),
            vec![],
        )
        .await,
        Err(DbError::UnsupportedResultType(_))
    ));
    HostTransaction::execute(
        &mut state,
        wasmtime::component::Resource::new_borrow(rep),
        "INSERT INTO _wr_tx_local_error_test VALUES (2)".into(),
        vec![],
    )
    .await
    .expect("valid insert");
    HostTransaction::commit(&mut state, wasmtime::component::Resource::new_borrow(rep))
        .await
        .expect("commit");
    HostTransaction::drop(&mut state, tx).await.expect("drop");
    assert_eq!(
        Host::query(
            &mut state,
            "SELECT val FROM _wr_tx_local_error_test".into(),
            vec![],
        )
        .await
        .unwrap()
        .len(),
        1
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_transaction_implicit_rollback_on_drop() {
    use super::wruntime::db::database::{Host, HostTransaction};

    let url = match db_url() {
        Some(u) => u,
        None => return,
    };

    let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
    let mut state = ModuleState::new(
        "test".into(),
        "test".into(),
        proxy_uri(),
        test_http_pool(),
        ModuleServices {
            db_pool: Some(Arc::new(pool)),
            db_schema: Some(Arc::from("public")),
            ..Default::default()
        },
    )
    .expect("state");

    Host::execute(
        &mut state,
        "CREATE TEMP TABLE _wr_tx_drop_test (val INT)".into(),
        vec![],
    )
    .await
    .expect("create table");

    let tx = state.begin_transaction().await.expect("begin");
    let rep = tx.rep();

    HostTransaction::execute(
        &mut state,
        wasmtime::component::Resource::new_borrow(rep),
        "INSERT INTO _wr_tx_drop_test VALUES (7)".into(),
        vec![],
    )
    .await
    .expect("insert");

    // Drop without committing — host must issue implicit ROLLBACK.
    HostTransaction::drop(&mut state, tx).await.expect("drop");

    let rows = Host::query(
        &mut state,
        "SELECT val FROM _wr_tx_drop_test".into(),
        vec![],
    )
    .await
    .expect("query after implicit rollback");
    assert_eq!(rows.len(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_stream_with_postgres() {
    let url = match db_url() {
        Some(u) => u,
        None => return,
    };

    let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
    let mut state = ModuleState::new(
        "test".into(),
        "test".into(),
        proxy_uri(),
        test_http_pool(),
        ModuleServices {
            db_pool: Some(Arc::new(pool)),
            db_schema: Some(Arc::from("public")),
            ..Default::default()
        },
    )
    .expect("state");

    let cursor = state
        .query_stream("SELECT generate_series(1, 5) AS n".into(), vec![])
        .await
        .expect("query_stream");
    let rep = cursor.rep();

    // Fetch in batches of 2
    let batch1 = HostRowCursor::next_batch(
        &mut state,
        wasmtime::component::Resource::new_borrow(rep),
        2,
    )
    .await
    .expect("batch1");
    assert_eq!(batch1.len(), 2);

    let batch2 = HostRowCursor::next_batch(
        &mut state,
        wasmtime::component::Resource::new_borrow(rep),
        2,
    )
    .await
    .expect("batch2");
    assert_eq!(batch2.len(), 2);

    let batch3 = HostRowCursor::next_batch(
        &mut state,
        wasmtime::component::Resource::new_borrow(rep),
        2,
    )
    .await
    .expect("batch3");
    assert_eq!(batch3.len(), 1);

    // Stream exhausted
    let batch4 = HostRowCursor::next_batch(
        &mut state,
        wasmtime::component::Resource::new_borrow(rep),
        2,
    )
    .await
    .expect("batch4");
    assert!(batch4.is_empty());

    HostRowCursor::drop(&mut state, cursor).await.expect("drop");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_stream_drop_mid_iteration() {
    let url = match db_url() {
        Some(u) => u,
        None => return,
    };

    let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
    let mut state = ModuleState::new(
        "test".into(),
        "test".into(),
        proxy_uri(),
        test_http_pool(),
        ModuleServices {
            db_pool: Some(Arc::new(pool)),
            db_schema: Some(Arc::from("public")),
            ..Default::default()
        },
    )
    .expect("state");

    let cursor = state
        .query_stream("SELECT generate_series(1, 100) AS n".into(), vec![])
        .await
        .expect("query_stream");
    let rep = cursor.rep();

    // Fetch only the first batch, then drop
    let batch = HostRowCursor::next_batch(
        &mut state,
        wasmtime::component::Resource::new_borrow(rep),
        5,
    )
    .await
    .expect("batch");
    assert_eq!(batch.len(), 5);

    HostRowCursor::drop(&mut state, cursor).await.expect("drop");

    // Verify the connection is usable again by running another query
    let rows = state
        .query("SELECT 1 AS ok".into(), vec![])
        .await
        .expect("query after drop");
    assert_eq!(rows.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_stream_in_transaction() {
    use super::wruntime::db::database::{Host, HostTransaction};

    let url = match db_url() {
        Some(u) => u,
        None => return,
    };

    let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
    let mut state = ModuleState::new(
        "test".into(),
        "test".into(),
        proxy_uri(),
        test_http_pool(),
        ModuleServices {
            db_pool: Some(Arc::new(pool)),
            db_schema: Some(Arc::from("public")),
            ..Default::default()
        },
    )
    .expect("state");

    let tx = state.begin_transaction().await.expect("begin");
    let tx_rep = tx.rep();

    let cursor = HostTransaction::query_stream(
        &mut state,
        wasmtime::component::Resource::new_borrow(tx_rep),
        "SELECT generate_series(1, 3) AS n".into(),
        vec![],
    )
    .await
    .expect("query_stream in tx");
    let cursor_rep = cursor.rep();

    assert!(matches!(
        HostRowCursor::next_batch(
            &mut state,
            wasmtime::component::Resource::new_borrow(cursor_rep),
            super::cursor::MAX_CURSOR_BATCH_ROWS + 1,
        )
        .await,
        Err(DbError::Query(_))
    ));
    assert!(matches!(
        HostTransaction::query(
            &mut state,
            wasmtime::component::Resource::new_borrow(tx_rep),
            "SELECT 1".into(),
            vec![],
        )
        .await,
        Err(DbError::Query(_))
    ));
    assert!(matches!(
        HostTransaction::execute(
            &mut state,
            wasmtime::component::Resource::new_borrow(tx_rep),
            "SELECT 1".into(),
            vec![],
        )
        .await,
        Err(DbError::Query(_))
    ));
    assert!(HostTransaction::query_stream(
        &mut state,
        wasmtime::component::Resource::new_borrow(tx_rep),
        "SELECT 1".into(),
        vec![],
    )
    .await
    .is_err());
    assert!(matches!(
        HostTransaction::commit(
            &mut state,
            wasmtime::component::Resource::new_borrow(tx_rep),
        )
        .await,
        Err(DbError::Query(_))
    ));
    assert!(matches!(
        HostTransaction::rollback(
            &mut state,
            wasmtime::component::Resource::new_borrow(tx_rep),
        )
        .await,
        Err(DbError::Query(_))
    ));
    assert!(
        HostTransaction::drop(&mut state, wasmtime::component::Resource::new_own(tx_rep),)
            .await
            .is_err()
    );

    let batch = HostRowCursor::next_batch(
        &mut state,
        wasmtime::component::Resource::new_borrow(cursor_rep),
        10,
    )
    .await
    .expect("batch");
    assert_eq!(batch.len(), 3);

    // Drain the cursor
    let empty = HostRowCursor::next_batch(
        &mut state,
        wasmtime::component::Resource::new_borrow(cursor_rep),
        10,
    )
    .await
    .expect("empty");
    assert!(empty.is_empty());

    HostRowCursor::drop(&mut state, cursor)
        .await
        .expect("drop cursor");

    HostTransaction::commit(
        &mut state,
        wasmtime::component::Resource::new_borrow(tx_rep),
    )
    .await
    .expect("commit");
    HostTransaction::drop(&mut state, tx)
        .await
        .expect("drop tx");
}

#[tokio::test(flavor = "multi_thread")]
async fn transaction_stream_server_error_poisoning_releases_cursor_ordering() {
    use super::wruntime::db::database::{Host, HostTransaction};

    let Some(url) = db_url() else { return };
    let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
    let mut state = ModuleState::new(
        "test".into(),
        "test".into(),
        proxy_uri(),
        test_http_pool(),
        ModuleServices {
            db_pool: Some(Arc::new(pool)),
            db_schema: Some(Arc::from("public")),
            ..Default::default()
        },
    )
    .expect("state");
    let tx = state.begin_transaction().await.expect("begin");
    let tx_rep = tx.rep();
    let cursor = HostTransaction::query_stream(
        &mut state,
        wasmtime::component::Resource::new_borrow(tx_rep),
        "SELECT 1 / (n - 2) FROM generate_series(1, 3) AS n".into(),
        vec![],
    )
    .await
    .expect("open stream");
    let cursor_rep = cursor.rep();
    assert!(matches!(
        HostRowCursor::next_batch(
            &mut state,
            wasmtime::component::Resource::new_borrow(cursor_rep),
            10,
        )
        .await,
        Err(DbError::Query(_))
    ));
    HostRowCursor::drop(&mut state, cursor)
        .await
        .expect("drop synchronized cursor");
    assert!(matches!(
        HostTransaction::commit(
            &mut state,
            wasmtime::component::Resource::new_borrow(tx_rep),
        )
        .await,
        Err(DbError::Query(message)) if message.contains("rolled back")
    ));
    HostTransaction::drop(&mut state, tx)
        .await
        .expect("drop transaction");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_naive_timestamp_with_postgres() {
    let url = match db_url() {
        Some(u) => u,
        None => return,
    };

    let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
    let mut state = ModuleState::new(
        "test".into(),
        "test".into(),
        proxy_uri(),
        test_http_pool(),
        ModuleServices {
            db_pool: Some(Arc::new(pool)),
            db_schema: Some(Arc::from("public")),
            ..Default::default()
        },
    )
    .expect("state");

    // Use epoch to avoid timezone ambiguity.
    let rows = state
        .query(
            "SELECT '2000-01-01 00:00:00'::timestamp AS ts".into(),
            vec![],
        )
        .await
        .expect("query");

    assert_eq!(rows.len(), 1);
    // Should be Timestamp, not Timestamptz
    match &rows[0].columns[0].value {
        PgValue::Timestamp(micros) => {
            // 2000-01-01 00:00:00 UTC = 946684800 seconds since Unix epoch
            assert_eq!(*micros, 946_684_800_000_000);
        }
        other => panic!("expected Timestamp, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_naive_timestamp_param_roundtrip() {
    let url = match db_url() {
        Some(u) => u,
        None => return,
    };

    let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
    let mut state = ModuleState::new(
        "test".into(),
        "test".into(),
        proxy_uri(),
        test_http_pool(),
        ModuleServices {
            db_pool: Some(Arc::new(pool)),
            db_schema: Some(Arc::from("public")),
            ..Default::default()
        },
    )
    .expect("state");

    let micros: i64 = 1_718_451_000_000_000;
    let rows = state
        .query(
            "SELECT $1::timestamp AS ts".into(),
            vec![PgValue::Timestamp(micros)],
        )
        .await
        .expect("query");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns[0].value, PgValue::Timestamp(micros));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_interval_with_postgres() {
    let url = match db_url() {
        Some(u) => u,
        None => return,
    };

    let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
    let mut state = ModuleState::new(
        "test".into(),
        "test".into(),
        proxy_uri(),
        test_http_pool(),
        ModuleServices {
            db_pool: Some(Arc::new(pool)),
            db_schema: Some(Arc::from("public")),
            ..Default::default()
        },
    )
    .expect("state");

    let rows = state
        .query(
            "SELECT '1 year 2 months 3 days 4 hours 5 minutes 6 seconds'::interval AS iv".into(),
            vec![],
        )
        .await
        .expect("query");

    assert_eq!(rows.len(), 1);
    match &rows[0].columns[0].value {
        PgValue::Interval(iv) => {
            assert_eq!(iv.months, 14); // 1 year + 2 months
            assert_eq!(iv.days, 3);
            // 4h5m6s = 14706 seconds = 14706000000 microseconds
            assert_eq!(iv.microseconds, 14_706_000_000);
        }
        other => panic!("expected Interval, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_interval_param_roundtrip() {
    let url = match db_url() {
        Some(u) => u,
        None => return,
    };

    let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
    let mut state = ModuleState::new(
        "test".into(),
        "test".into(),
        proxy_uri(),
        test_http_pool(),
        ModuleServices {
            db_pool: Some(Arc::new(pool)),
            db_schema: Some(Arc::from("public")),
            ..Default::default()
        },
    )
    .expect("state");

    let iv = PgInterval {
        months: 14,
        days: 3,
        microseconds: 14_706_000_000,
    };
    let rows = state
        .query(
            "SELECT $1::interval AS iv".into(),
            vec![PgValue::Interval(iv)],
        )
        .await
        .expect("query");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns[0].value, PgValue::Interval(iv));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_int4_array_with_postgres() {
    let url = match db_url() {
        Some(u) => u,
        None => return,
    };

    let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
    let mut state = ModuleState::new(
        "test".into(),
        "test".into(),
        proxy_uri(),
        test_http_pool(),
        ModuleServices {
            db_pool: Some(Arc::new(pool)),
            db_schema: Some(Arc::from("public")),
            ..Default::default()
        },
    )
    .expect("state");

    let rows = state
        .query("SELECT ARRAY[1, 2, NULL, 4]::int4[] AS arr".into(), vec![])
        .await
        .expect("query");

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].columns[0].value,
        PgValue::Int4Array(vec![Some(1), Some(2), None, Some(4)])
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_text_array_with_postgres() {
    let url = match db_url() {
        Some(u) => u,
        None => return,
    };

    let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
    let mut state = ModuleState::new(
        "test".into(),
        "test".into(),
        proxy_uri(),
        test_http_pool(),
        ModuleServices {
            db_pool: Some(Arc::new(pool)),
            db_schema: Some(Arc::from("public")),
            ..Default::default()
        },
    )
    .expect("state");

    let rows = state
        .query(
            "SELECT ARRAY['hello', NULL, 'world']::text[] AS arr".into(),
            vec![],
        )
        .await
        .expect("query");

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].columns[0].value,
        PgValue::TextArray(vec![Some("hello".into()), None, Some("world".into()),])
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_array_param_roundtrip() {
    let url = match db_url() {
        Some(u) => u,
        None => return,
    };

    let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
    let mut state = ModuleState::new(
        "test".into(),
        "test".into(),
        proxy_uri(),
        test_http_pool(),
        ModuleServices {
            db_pool: Some(Arc::new(pool)),
            db_schema: Some(Arc::from("public")),
            ..Default::default()
        },
    )
    .expect("state");

    let arr = vec![Some(10), None, Some(30)];
    let rows = state
        .query(
            "SELECT $1::int4[] AS arr".into(),
            vec![PgValue::Int4Array(arr.clone())],
        )
        .await
        .expect("query");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns[0].value, PgValue::Int4Array(arr));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_array_any_query() {
    let url = match db_url() {
        Some(u) => u,
        None => return,
    };

    let pool = crate::pool::build_pool(&url, 2).expect("build_pool");
    let mut state = ModuleState::new(
        "test".into(),
        "test".into(),
        proxy_uri(),
        test_http_pool(),
        ModuleServices {
            db_pool: Some(Arc::new(pool)),
            db_schema: Some(Arc::from("public")),
            ..Default::default()
        },
    )
    .expect("state");

    // Common pattern: WHERE id = ANY($1::int4[])
    let rows = state
        .query(
            "SELECT unnest($1::int4[]) AS n".into(),
            vec![PgValue::Int4Array(vec![Some(1), Some(2), Some(3)])],
        )
        .await
        .expect("query");

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].columns[0].value, PgValue::Int4(1));
    assert_eq!(rows[1].columns[0].value, PgValue::Int4(2));
    assert_eq!(rows[2].columns[0].value, PgValue::Int4(3));
}

#[tokio::test(flavor = "current_thread")]
async fn database_spans_cover_guest_parent_operations_errors_and_cursor_lifetimes() {
    use crate::tracing::wruntime::tracing::span::{Host as TracingHost, HostActiveSpan};

    let _capture_guard = CAPTURED_TELEMETRY_LOCK.lock().await;
    let url = match db_url() {
        Some(url) => url,
        None => return,
    };
    let telemetry = captured_telemetry();
    let request = tracing::dispatcher::with_default(&telemetry.dispatch, || {
        tracing::info_span!("db-lifecycle-request", "otel.name" = "db-lifecycle-request")
    });
    let pool = Arc::new(crate::pool::build_pool(&url, 3).expect("build_pool"));
    let mut state = ModuleState::new(
        "test".into(),
        "test".into(),
        proxy_uri(),
        test_http_pool(),
        ModuleServices {
            db_pool: Some(pool.clone()),
            db_schema: Some(Arc::from("public")),
            db_telemetry_include_query_text: true,
            active_span: request.clone(),
            ..Default::default()
        },
    )
    .expect("state");
    let guest = TracingHost::start(&mut state, "guest-db".into(), vec![])
        .await
        .expect("guest span");
    let rows = state
        .query(
            "/* telemetry-query */\n SELECT $1::text AS value".into(),
            vec![PgValue::Text("bind-secret".into())],
        )
        .await
        .expect("query");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        state
            .query("/* telemetry-zero */ SELECT 1 WHERE false".into(), vec![])
            .await
            .expect("zero-row query")
            .len(),
        0
    );
    assert_eq!(
        state
            .execute("/* telemetry-execute */ SELECT 1".into(), vec![])
            .await
            .expect("execute"),
        1
    );

    let exhausted = state
        .query_stream(
            "/* telemetry-stream-exhausted */ SELECT generate_series(1, 3)".into(),
            vec![],
        )
        .await
        .expect("exhausted cursor");
    let exhausted_rep = exhausted.rep();
    assert_eq!(
        HostRowCursor::next_batch(
            &mut state,
            wasmtime::component::Resource::new_borrow(exhausted_rep),
            2,
        )
        .await
        .expect("first exhausted-stream batch")
        .len(),
        2
    );
    assert_eq!(
        HostRowCursor::next_batch(
            &mut state,
            wasmtime::component::Resource::new_borrow(exhausted_rep),
            2,
        )
        .await
        .expect("final exhausted-stream batch")
        .len(),
        1
    );
    HostRowCursor::drop(&mut state, exhausted)
        .await
        .expect("drop exhausted cursor");

    let empty = state
        .query_stream(
            "/* telemetry-stream-empty */ SELECT 1 WHERE false".into(),
            vec![],
        )
        .await
        .expect("empty cursor");
    let empty_rep = empty.rep();
    assert!(HostRowCursor::next_batch(
        &mut state,
        wasmtime::component::Resource::new_borrow(empty_rep),
        2,
    )
    .await
    .expect("empty stream")
    .is_empty());
    HostRowCursor::drop(&mut state, empty)
        .await
        .expect("drop empty cursor");

    let partial = state
        .query_stream(
            "/* telemetry-stream-partial */ SELECT generate_series(1, 100)".into(),
            vec![],
        )
        .await
        .expect("partial cursor");
    let partial_rep = partial.rep();
    assert_eq!(
        HostRowCursor::next_batch(
            &mut state,
            wasmtime::component::Resource::new_borrow(partial_rep),
            2,
        )
        .await
        .expect("partial stream")
        .len(),
        2
    );
    HostRowCursor::drop(&mut state, partial)
        .await
        .expect("drop partial cursor");

    let discarded = state
        .query_stream(
            "/* telemetry-stream-discarded */ \
             SELECT 1 / (n - 3) FROM generate_series(1, 3) AS n"
                .into(),
            vec![],
        )
        .await
        .expect("discarded-batch cursor");
    let discarded_rep = discarded.rep();
    assert!(matches!(
        HostRowCursor::next_batch(
            &mut state,
            wasmtime::component::Resource::new_borrow(discarded_rep),
            10,
        )
        .await,
        Err(DbError::Query(_))
    ));
    HostRowCursor::drop(&mut state, discarded)
        .await
        .expect("drop discarded-batch cursor");

    let decode = state
        .query_stream(
            "/* telemetry-stream-decode */ SELECT '127.0.0.1'::inet AS network".into(),
            vec![],
        )
        .await
        .expect("decode cursor");
    let decode_rep = decode.rep();
    assert!(matches!(
        HostRowCursor::next_batch(
            &mut state,
            wasmtime::component::Resource::new_borrow(decode_rep),
            1,
        )
        .await,
        Err(DbError::UnsupportedResultType(_))
    ));
    HostRowCursor::drop(&mut state, decode)
        .await
        .expect("drop decode cursor");

    let transaction = state.begin_transaction().await.expect("transaction");
    let transaction_rep = transaction.rep();
    let transaction_rows = super::wruntime::db::database::HostTransaction::query(
        &mut state,
        wasmtime::component::Resource::new_borrow(transaction_rep),
        "/* telemetry-transaction-query */ SELECT 1".into(),
        vec![],
    )
    .await
    .expect("transaction query");
    assert_eq!(transaction_rows.len(), 1);
    assert_eq!(
        super::wruntime::db::database::HostTransaction::execute(
            &mut state,
            wasmtime::component::Resource::new_borrow(transaction_rep),
            "/* telemetry-transaction-execute */ SELECT 1".into(),
            vec![],
        )
        .await
        .expect("transaction execute"),
        1
    );
    let transaction_cursor = super::wruntime::db::database::HostTransaction::query_stream(
        &mut state,
        wasmtime::component::Resource::new_borrow(transaction_rep),
        "/* telemetry-transaction-stream */ SELECT generate_series(1, 2)".into(),
        vec![],
    )
    .await
    .expect("transaction stream");
    let transaction_cursor_rep = transaction_cursor.rep();
    assert_eq!(
        HostRowCursor::next_batch(
            &mut state,
            wasmtime::component::Resource::new_borrow(transaction_cursor_rep),
            10,
        )
        .await
        .expect("transaction stream rows")
        .len(),
        2
    );
    HostRowCursor::drop(&mut state, transaction_cursor)
        .await
        .expect("drop transaction cursor");
    super::wruntime::db::database::HostTransaction::commit(
        &mut state,
        wasmtime::component::Resource::new_borrow(transaction_rep),
    )
    .await
    .expect("commit");
    assert!(super::wruntime::db::database::HostTransaction::query(
        &mut state,
        wasmtime::component::Resource::new_borrow(transaction_rep),
        "/* telemetry-completed */ SELECT 1".into(),
        vec![],
    )
    .await
    .is_err());
    super::wruntime::db::database::HostTransaction::drop(&mut state, transaction)
        .await
        .expect("drop transaction");

    HostActiveSpan::drop(&mut state, guest)
        .await
        .expect("drop guest span");

    let mut capped = ModuleState::new(
        "test".into(),
        "test".into(),
        proxy_uri(),
        test_http_pool(),
        ModuleServices {
            db_pool: Some(pool),
            db_schema: Some(Arc::from("public")),
            active_span: request.clone(),
            limits: crate::config::ResourceLimits {
                max_db_cursors: 0,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .expect("capped state");
    assert!(capped
        .query_stream("/* telemetry-cap */ SELECT 1".into(), vec![])
        .await
        .is_err());

    drop(capped);
    drop(state);
    drop(request);
    telemetry.provider.force_flush().expect("flush spans");
    let spans = telemetry
        .exporter
        .get_finished_spans()
        .expect("captured spans");
    let request = spans
        .iter()
        .find(|span| span.name == "db-lifecycle-request")
        .expect("lifecycle request span");
    let guest = spans
        .iter()
        .find(|span| {
            span.name == "guest-db" && span.parent_span_id == request.span_context.span_id()
        })
        .expect("guest DB span");
    let db_spans: Vec<_> = spans
        .iter()
        .filter(|span| {
            string_attribute(span, "db.system.name") == Some("postgresql")
                && (span.parent_span_id == guest.span_context.span_id()
                    || span.parent_span_id == request.span_context.span_id())
        })
        .collect();
    assert!(db_spans.len() >= 13);
    for span in db_spans.iter().filter(|span| {
        string_attribute(span, "db.query.text").is_some_and(|text| !text.contains("telemetry-cap"))
    }) {
        assert_eq!(span.parent_span_id, guest.span_context.span_id());
    }
    assert!(db_spans.iter().all(|span| {
        span.attributes
            .iter()
            .all(|attribute| !attribute.value.to_string().contains("bind-secret"))
    }));

    let find = |marker: &str| {
        db_spans
            .iter()
            .copied()
            .find(|span| {
                string_attribute(span, "db.query.text").is_some_and(|text| text.contains(marker))
            })
            .unwrap()
    };
    let query = find("telemetry-query");
    assert_eq!(
        string_attribute(query, "db.query.text"),
        Some("/* telemetry-query */ SELECT $1::text AS value")
    );
    assert_eq!(string_attribute(query, "db.namespace"), Some("public"));
    assert_eq!(
        attribute(query, "db.response.returned_rows"),
        Some(&Value::I64(1))
    );
    assert_eq!(
        attribute(find("telemetry-zero"), "db.response.returned_rows"),
        Some(&Value::I64(0))
    );
    let exhausted = find("telemetry-stream-exhausted");
    assert_eq!(
        attribute(exhausted, "db.response.returned_rows"),
        Some(&Value::I64(3))
    );
    assert!(string_attribute(exhausted, "error.type").is_none());
    let empty = find("telemetry-stream-empty");
    assert_eq!(
        attribute(empty, "db.response.returned_rows"),
        Some(&Value::I64(0))
    );
    assert!(string_attribute(empty, "error.type").is_none());
    let partial = find("telemetry-stream-partial");
    assert_eq!(
        attribute(partial, "db.response.returned_rows"),
        Some(&Value::I64(2))
    );
    assert_eq!(string_attribute(partial, "error.type"), Some("cancelled"));
    let discarded = find("telemetry-stream-discarded");
    assert_eq!(
        attribute(discarded, "db.response.returned_rows"),
        Some(&Value::I64(0))
    );
    assert_eq!(string_attribute(discarded, "error.type"), Some("query"));
    let decode = find("telemetry-stream-decode");
    assert_eq!(
        string_attribute(decode, "error.type"),
        Some("unsupported_result_type")
    );
    assert_eq!(
        string_attribute(find("telemetry-transaction-query"), "db.operation.name"),
        Some("transaction.query")
    );
    assert_eq!(
        string_attribute(find("telemetry-transaction-execute"), "db.operation.name"),
        Some("transaction.execute")
    );
    assert_eq!(
        string_attribute(find("telemetry-transaction-stream"), "db.operation.name"),
        Some("transaction.stream")
    );
    assert_eq!(
        string_attribute(find("telemetry-completed"), "error.type"),
        Some("query")
    );
    let cap = db_spans
        .iter()
        .copied()
        .find(|span| {
            string_attribute(span, "db.operation.name") == Some("stream")
                && string_attribute(span, "error.type") == Some("connection")
                && attribute(span, "db.query.text").is_none()
        })
        .expect("resource-cap span");
    assert_eq!(cap.parent_span_id, request.span_context.span_id());
    assert!(matches!(
        &cap.status,
        opentelemetry::trace::Status::Error { .. }
    ));
}
