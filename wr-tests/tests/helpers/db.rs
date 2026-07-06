use std::sync::Arc;

pub use wr_engine::db::wruntime::db::database::{DbError, Host as DbHost, PgValue};
pub use wr_engine::state::{ModuleServices, ModuleState};

use super::proxy::http_pool;

pub const TEST_DB_URL_ENV: &str = "WRT_TEST_DB_URL";

pub fn test_db_url() -> Option<String> {
    match std::env::var_os(TEST_DB_URL_ENV) {
        None => None,
        Some(raw) => Some(
            raw.into_string()
                .expect("WRT_TEST_DB_URL must be valid UTF-8 when set"),
        ),
    }
}

pub fn skip_without_db(test_name: &str) -> bool {
    if test_db_url().is_none() {
        eprintln!("skipping {test_name} (no WRT_TEST_DB_URL)");
        true
    } else {
        false
    }
}

pub fn require_db_url() -> String {
    let url = test_db_url().expect("WRT_TEST_DB_URL must be set for this test");
    assert!(!url.is_empty(), "WRT_TEST_DB_URL is set but empty");
    url
}

pub async fn manager_pool() -> deadpool_postgres::Pool {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    static CLEANED: AtomicBool = AtomicBool::new(false);
    static CLEANUP_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    let base_url = require_db_url();
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let schema = format!("mgr_test_{n}");

    // Create the schema using a one-shot connection to the base DB (no search_path override).
    let setup_pool = wr_common::pool::build_pool(&base_url, 1).expect("failed to build setup pool");
    let client = setup_pool.get().await.expect("setup connection");

    // On the first call only, drop all leftover mgr_test_* schemas from
    // previous (possibly failed) test runs.
    if !CLEANED.load(Ordering::SeqCst) {
        let _guard = CLEANUP_LOCK.lock().await;
        if !CLEANED.load(Ordering::SeqCst) {
            let rows = client
                .query(
                    "SELECT schema_name FROM information_schema.schemata
                     WHERE schema_name LIKE 'mgr_test_%'",
                    &[],
                )
                .await
                .expect("list mgr_test schemas");
            for row in &rows {
                let name: &str = row.get(0);
                client
                    .batch_execute(&format!("DROP SCHEMA \"{name}\" CASCADE"))
                    .await
                    .expect("drop leftover schema");
            }
            // Ensure wr_system schema exists before any migrations run.
            // Done once under the lock to avoid races between parallel tests.
            client
                .batch_execute("CREATE SCHEMA IF NOT EXISTS wr_system")
                .await
                .expect("create wr_system schema");
            CLEANED.store(true, Ordering::SeqCst);
        }
    }

    client
        .batch_execute(&format!("CREATE SCHEMA \"{schema}\""))
        .await
        .expect("create schema");
    drop(client);
    drop(setup_pool);

    // Build the real pool with search_path pinned to the test schema.
    let pool = wr_common::pool::build_pool_with_search_path(&base_url, 5, &schema)
        .expect("failed to build manager test pool");

    let mut client = pool.get().await.expect("migration connection");
    wr_manager::migrate::run_migrations(&mut client)
        .await
        .expect("manager migrations failed");
    drop(client);

    pool
}

/// Drop-and-recreate `schema` (leaving it empty), ensure `wr_system` exists, and
/// return a 1-connection pool whose `search_path` is pinned to `schema`.
/// Unlike [`manager_pool`], migrations are NOT run — the caller runs them, which
/// lets a test point multiple pools at the SAME schema to exercise concurrent
/// `run_migrations`.
pub async fn manager_pool_in_schema(schema: &str) -> deadpool_postgres::Pool {
    let base_url = require_db_url();
    let setup_pool = wr_common::pool::build_pool(&base_url, 1).expect("failed to build setup pool");
    let client = setup_pool.get().await.expect("setup connection");
    client
        .batch_execute("CREATE SCHEMA IF NOT EXISTS wr_system")
        .await
        .expect("create wr_system schema");
    client
        .batch_execute(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .await
        .expect("drop test schema");
    client
        .batch_execute(&format!("CREATE SCHEMA \"{schema}\""))
        .await
        .expect("create test schema");
    drop(client);
    drop(setup_pool);

    wr_common::pool::build_pool_with_search_path(&base_url, 1, schema)
        .expect("failed to build manager test pool")
}

pub fn db_state(pool_size: usize) -> ModuleState {
    let url = require_db_url();
    let pool = Arc::new(wr_engine::pool::build_pool(&url, pool_size).expect("build_pool"));
    ModuleState::new(
        "test".into(),
        "test-ns".into(),
        "http://127.0.0.1:9001".parse().unwrap(),
        http_pool(),
        ModuleServices {
            db_pool: Some(pool),
            ..Default::default()
        },
    )
    .expect("ModuleState")
}

/// Build a `ModuleState` for a specific `(namespace, name)` pair, provisioning
/// the module's Postgres schema (`wr__{sanitized_namespace}__{sanitized_name}`)
/// if it does not
/// already exist. Panics if `WRT_TEST_DB_URL` is not set.
pub async fn db_state_for_module(pool_size: usize, namespace: &str, name: &str) -> ModuleState {
    let url = require_db_url();
    let schema = wr_engine::pool::module_schema(namespace, name);
    let pool = Arc::new(wr_engine::pool::build_pool(&url, pool_size).expect("build_pool"));
    let client = pool
        .get()
        .await
        .expect("get connection for schema provisioning");
    if let Err(e) = client
        .simple_query(&format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\""))
        .await
    {
        // Ignore unique_violation (23505) — a concurrent test may have created
        // the schema between our IF NOT EXISTS check and the actual CREATE.
        let is_duplicate = e
            .as_db_error()
            .is_some_and(|db| db.code().code() == "23505");
        if !is_duplicate {
            panic!("provision schema: {e}");
        }
    }
    drop(client);
    ModuleState::new(
        name.into(),
        namespace.into(),
        "http://127.0.0.1:9001".parse().unwrap(),
        http_pool(),
        ModuleServices {
            db_pool: Some(pool),
            db_schema: Some(Arc::from(schema)),
            ..Default::default()
        },
    )
    .expect("ModuleState")
}

/// Same schema-provisioning body as `db_state_for_module`, but with `limits`.
pub async fn db_state_for_module_with_limits(
    pool_size: usize,
    namespace: &str,
    name: &str,
    limits: wr_engine::config::ResourceLimits,
) -> ModuleState {
    let url = require_db_url();
    let schema = wr_engine::pool::module_schema(namespace, name);
    let pool = Arc::new(wr_engine::pool::build_pool(&url, pool_size).expect("build_pool"));
    let client = pool
        .get()
        .await
        .expect("get connection for schema provisioning");
    if let Err(e) = client
        .simple_query(&format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\""))
        .await
    {
        let is_duplicate = e
            .as_db_error()
            .is_some_and(|db| db.code().code() == "23505");
        if !is_duplicate {
            panic!("provision schema: {e}");
        }
    }
    drop(client);
    ModuleState::new(
        name.into(),
        namespace.into(),
        "http://127.0.0.1:9001".parse().unwrap(),
        http_pool(),
        ModuleServices {
            db_pool: Some(pool),
            db_schema: Some(Arc::from(schema)),
            limits,
            ..Default::default()
        },
    )
    .expect("ModuleState")
}
