use std::sync::Arc;

use super::wruntime::db::database::{DbError, Host, HostRowCursor, PgInterval, PgValue};
use crate::state::{ModuleServices, ModuleState};

fn proxy_uri() -> hyper::Uri {
    "http://127.0.0.1:9001".parse().unwrap()
}

fn test_http_pool() -> wr_common::http_pool::HttpClientPool<http_body_util::Full<bytes::Bytes>> {
    wr_common::http_pool::HttpClientPool::new(1)
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

// ── real-Postgres tests ───────────────────────────────────────────────────

/// Skip the test if `WRT_TEST_DB_URL` is not set.
fn db_url() -> Option<String> {
    std::env::var("WRT_TEST_DB_URL").ok()
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
    assert_eq!(cols[6].value, PgValue::Null);
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
