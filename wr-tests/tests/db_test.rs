mod helpers;
use helpers::{
    db::{db_state, DbError, DbHost as _, ModuleState, PgValue},
    proxy::http_pool,
};

// ── DB integration tests ──────────────────────────────────────────────────────
//
// Config-parsing tests run unconditionally.
// Host-trait tests that hit a real Postgres instance require
// WRT_TEST_DB_URL — `db_state()` panics when it is absent.

// ─ EngineConfig / DatabaseConfig parsing ─────────────────────────────────────

#[test]
fn test_engine_config_database_section_parses() {
    use wr_engine::config::EngineConfig;
    let toml = r#"
        listen_address  = "0.0.0.0:9100"
        [node]
        proxy_address   = "http://127.0.0.1:9001"
        control_address = "http://127.0.0.1:9002"
        [node.tls]
        cert_path    = "c.crt"
        key_path     = "c.key"
        ca_cert_path = "ca.crt"
        [database]
        url             = "postgres://user:pass@localhost:5432/mydb"
        max_connections = 4
    "#;
    let cfg: EngineConfig = toml::from_str(toml).unwrap();
    let db = cfg.database.expect("database section should be present");
    assert_eq!(db.url, "postgres://user:pass@localhost:5432/mydb");
    assert_eq!(db.max_connections, 4);
}

#[test]
fn test_engine_config_database_max_connections_default() {
    use wr_engine::config::EngineConfig;
    let toml = r#"
        listen_address  = "0.0.0.0:9100"
        [node]
        proxy_address   = "http://127.0.0.1:9001"
        control_address = "http://127.0.0.1:9002"
        [node.tls]
        cert_path    = "c.crt"
        key_path     = "c.key"
        ca_cert_path = "ca.crt"
        [database]
        url = "postgres://user:pass@localhost:5432/mydb"
    "#;
    let cfg: EngineConfig = toml::from_str(toml).unwrap();
    let db = cfg.database.expect("database section should be present");
    assert_eq!(db.max_connections, 20);
}

#[test]
fn test_engine_config_module_database_flag_parses() {
    use wr_engine::config::EngineConfig;
    // database = true on a module is parsed correctly; EngineConfig::validate()
    // (called via load()) would reject this if [database] were absent.
    let toml = r#"
        listen_address  = "0.0.0.0:9100"
        [node]
        proxy_address   = "http://127.0.0.1:9001"
        control_address = "http://127.0.0.1:9002"
        [node.tls]
        cert_path    = "c.crt"
        key_path     = "c.key"
        ca_cert_path = "ca.crt"
        [database]
        url = "postgres://user:pass@localhost:5432/mydb"
        [[module]]
        name        = "svc"
        namespace   = "my-ns"
        version     = "1.0.0"
        wasm_path   = "/nonexistent/svc.wasm"
        schema_path = "/nonexistent/svc.binpb"
        database    = true
    "#;
    let cfg: EngineConfig = toml::from_str(toml).unwrap();
    assert!(
        cfg.modules[0].database,
        "database flag should parse as true"
    );
    assert!(cfg.database.is_some(), "database section should be present");
}

#[test]
fn test_engine_config_module_database_flag_defaults_to_false() {
    use wr_engine::config::EngineConfig;
    let toml = r#"
        listen_address  = "0.0.0.0:9100"
        [node]
        proxy_address   = "http://127.0.0.1:9001"
        control_address = "http://127.0.0.1:9002"
        [node.tls]
        cert_path    = "c.crt"
        key_path     = "c.key"
        ca_cert_path = "ca.crt"
        [[module]]
        name        = "svc"
        namespace   = "my-ns"
        version     = "1.0.0"
        wasm_path   = "/nonexistent/svc.wasm"
        schema_path = "/nonexistent/svc.binpb"
    "#;
    let cfg: EngineConfig = toml::from_str(toml).unwrap();
    assert!(!cfg.modules[0].database, "database should default to false");
}

// ─ Host trait — no pool ───────────────────────────────────────────────────────

#[tokio::test]
async fn test_db_query_without_pool_returns_connection_error() {
    let mut state = ModuleState::new(
        "test".into(),
        "test-ns".into(),
        "http://127.0.0.1:9001".parse().unwrap(),
        http_pool(),
        Default::default(),
    )
    .expect("ModuleState");
    let err = state.query("SELECT 1".into(), vec![]).await.unwrap_err();
    assert!(
        matches!(err, DbError::Connection(_)),
        "expected Connection error, got {err:?}",
    );
}

#[tokio::test]
async fn test_db_execute_without_pool_returns_connection_error() {
    let mut state = ModuleState::new(
        "test".into(),
        "test-ns".into(),
        "http://127.0.0.1:9001".parse().unwrap(),
        http_pool(),
        Default::default(),
    )
    .expect("ModuleState");
    let err = state
        .execute("INSERT INTO t VALUES (1)".into(), vec![])
        .await
        .unwrap_err();
    assert!(
        matches!(err, DbError::Connection(_)),
        "expected Connection error, got {err:?}",
    );
}

// ─ Host trait — real Postgres ─────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn test_db_bytea_roundtrip() {
    let mut state = db_state(2);
    let payload = vec![0u8, 1, 127, 128, 255];
    let rows = state
        .query(
            "SELECT $1::bytea AS b".into(),
            vec![PgValue::Bytea(payload.clone())],
        )
        .await
        .expect("query");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns[0].value, PgValue::Bytea(payload));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_db_uuid_roundtrip() {
    let mut state = db_state(2);
    // UUID 550e8400-e29b-41d4-a716-446655440000 split into (hi, lo) at bit 64.
    let hi: u64 = 0x550e_8400_e29b_41d4;
    let lo: u64 = 0xa716_4466_5544_0000;
    let rows = state
        .query("SELECT $1::uuid AS u".into(), vec![PgValue::Uuid((hi, lo))])
        .await
        .expect("query");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns[0].value, PgValue::Uuid((hi, lo)));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_db_timestamptz_roundtrip() {
    let mut state = db_state(2);
    // 2001-09-09 01:46:40 UTC — a clean million-second boundary.
    let micros: i64 = 1_000_000_000 * 1_000_000;
    let rows = state
        .query(
            "SELECT $1::timestamptz AS ts".into(),
            vec![PgValue::Timestamptz(micros)],
        )
        .await
        .expect("query");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns[0].value, PgValue::Timestamptz(micros));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_db_date_roundtrip() {
    let mut state = db_state(2);
    // 10957 days since 1970-01-01 = 2000-01-01.
    let rows = state
        .query("SELECT $1::date AS d".into(), vec![PgValue::Date(10957)])
        .await
        .expect("query");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns[0].value, PgValue::Date(10957));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_db_time_roundtrip() {
    let mut state = db_state(2);
    // 14:30:00.000000 — 52 200 seconds from midnight in microseconds.
    let micros: i64 = 52_200 * 1_000_000;
    let rows = state
        .query("SELECT $1::time AS t".into(), vec![PgValue::Time(micros)])
        .await
        .expect("query");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns[0].value, PgValue::Time(micros));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_db_numeric_roundtrip() {
    let mut state = db_state(2);
    let rows = state
        .query(
            "SELECT $1::numeric AS n".into(),
            vec![PgValue::Numeric("123.456".into())],
        )
        .await
        .expect("query");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns[0].value, PgValue::Numeric("123.456".into()));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_db_jsonb_roundtrip() {
    let mut state = db_state(2);
    let input = r#"{"key":"value","num":42}"#;
    let rows = state
        .query(
            "SELECT $1::jsonb AS j".into(),
            vec![PgValue::Jsonb(input.into())],
        )
        .await
        .expect("query");
    assert_eq!(rows.len(), 1);
    // JSONB may reorder keys; compare structurally.
    let PgValue::Jsonb(got) = &rows[0].columns[0].value else {
        panic!("expected Jsonb, got {:?}", rows[0].columns[0].value);
    };
    let want: serde_json::Value = serde_json::from_str(input).unwrap();
    let got_val: serde_json::Value = serde_json::from_str(got.as_str()).unwrap();
    assert_eq!(got_val, want);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_db_null_param_passes_through_as_null_column() {
    let mut state = db_state(2);
    let rows = state
        .query("SELECT $1::text AS v".into(), vec![PgValue::Null])
        .await
        .expect("query");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns[0].value, PgValue::Null);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_db_query_error_on_invalid_sql() {
    let mut state = db_state(2);
    let err = state
        .query("THIS IS NOT VALID SQL".into(), vec![])
        .await
        .unwrap_err();
    assert!(
        matches!(err, DbError::Query(_)),
        "expected Query error, got {err:?}",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_db_execute_insert_and_query_roundtrip() {
    // Pool size 1: TEMP TABLEs are connection-local, so all operations must
    // share the same underlying connection.
    let mut state = db_state(1);

    state
        .execute(
            "CREATE TEMP TABLE _wr_roundtrip (name TEXT, score INT4)".into(),
            vec![],
        )
        .await
        .expect("create table");

    let n = state
        .execute(
            "INSERT INTO _wr_roundtrip VALUES ($1, $2)".into(),
            vec![PgValue::Text("alice".into()), PgValue::Int4(99)],
        )
        .await
        .expect("insert");
    assert_eq!(n, 1, "one row should have been inserted");

    let rows = state
        .query("SELECT name, score FROM _wr_roundtrip".into(), vec![])
        .await
        .expect("select");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns[0].value, PgValue::Text("alice".into()));
    assert_eq!(rows[0].columns[1].value, PgValue::Int4(99));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_db_execute_returns_affected_row_count() {
    // Pool size 1 for TEMP TABLE visibility (see above).
    let mut state = db_state(1);

    state
        .execute("CREATE TEMP TABLE _wr_update (v INT4)".into(), vec![])
        .await
        .expect("create table");
    state
        .execute("INSERT INTO _wr_update VALUES (1), (2), (3)".into(), vec![])
        .await
        .expect("insert rows");

    let n = state
        .execute(
            "UPDATE _wr_update SET v = v + 10 WHERE v < 3".into(),
            vec![],
        )
        .await
        .expect("update");
    assert_eq!(n, 2, "two rows should have v < 3");

    let deleted = state
        .execute("DELETE FROM _wr_update WHERE v > 10".into(), vec![])
        .await
        .expect("delete");
    assert_eq!(deleted, 2, "two updated rows should be deleted");
}
