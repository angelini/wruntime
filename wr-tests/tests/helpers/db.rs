use std::sync::Arc;

use deadpool_postgres::tokio_postgres;

#[allow(unused_imports)]
pub use wr_engine::db::wruntime::db::database::{DbError, Host as DbHost, PgType, PgValue};
pub use wr_engine::state::{ModuleServices, ModuleState};

use super::proxy::http_pool;

pub const TEST_DB_URL_ENV: &str = "WRT_TEST_DB_URL";

pub fn test_db_url() -> Option<String> {
    std::env::var_os(TEST_DB_URL_ENV).map(|raw| {
        raw.into_string()
            .expect("WRT_TEST_DB_URL must be valid UTF-8 when set")
    })
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
    let base_url = require_db_url();
    let schema = format!("mgr_test_{}", uuid::Uuid::new_v4().simple());

    // Create the schema using a one-shot connection to the base DB (no search_path override).
    // A UUID keeps independently launched integration-test processes isolated. Do not clean up
    // other mgr_test_* schemas here: without shared liveness metadata they may still be in use.
    let setup_pool = wr_common::pool::build_pool(&base_url, 1).expect("failed to build setup pool");
    let client = setup_pool.get().await.expect("setup connection");
    wr_manager::migrate::ensure_system_schema(&client)
        .await
        .expect("create wr_system schema");
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
    wr_manager::migrate::ensure_system_schema(&client)
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
            db_schema: Some(Arc::from("public")),
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
    db_state_for_module_with_active_span(pool_size, namespace, name, tracing::Span::none()).await
}

pub async fn db_state_for_module_with_active_span(
    pool_size: usize,
    namespace: &str,
    name: &str,
    active_span: tracing::Span,
) -> ModuleState {
    let url = require_db_url();
    let schema = wr_engine::pool::module_schema(namespace, name);
    let pool = Arc::new(wr_engine::pool::build_pool(&url, pool_size).expect("build_pool"));
    let client = pool
        .get()
        .await
        .expect("get connection for schema provisioning");
    if let Err(error) = client
        .simple_query(&format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\""))
        .await
    {
        let is_duplicate = error
            .as_db_error()
            .is_some_and(|database| database.code().code() == "23505");
        if !is_duplicate {
            panic!("provision schema: {error}");
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
            active_span,
            ..Default::default()
        },
    )
    .expect("ModuleState")
}

/// Test-owned private PostgreSQL databases for offline provisioner coverage.
/// Cleanup is restricted to UUID-derived names held by this fixture.
pub struct PrivateDatabaseFixture {
    pub platform_database: String,
    pub retained_database: String,
}

impl PrivateDatabaseFixture {
    pub async fn create() -> anyhow::Result<Self> {
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let fixture = Self {
            platform_database: format!("wr_test_platform_{suffix}"),
            retained_database: format!("wr_test_retained_{suffix}"),
        };
        let (client, connection) =
            tokio_postgres::connect(&require_db_url(), tokio_postgres::NoTls).await?;
        let task = tokio::spawn(connection);
        client
            .batch_execute(&format!("CREATE DATABASE {}", fixture.platform_database))
            .await?;
        client
            .batch_execute(&format!("CREATE DATABASE {}", fixture.retained_database))
            .await?;
        task.abort();
        Ok(fixture)
    }

    pub async fn cleanup(
        self,
        manifest: &wr_common::postgres::PostgresProvisioningManifest,
    ) -> anyhow::Result<()> {
        let derived = manifest.derive_namespaces();
        let (client, connection) =
            tokio_postgres::connect(&require_db_url(), tokio_postgres::NoTls).await?;
        let task = tokio::spawn(connection);
        let mut databases = derived
            .iter()
            .map(|namespace| namespace.database.clone())
            .collect::<Vec<_>>();
        databases.extend([self.platform_database, self.retained_database]);
        for database in databases {
            client
                .execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1 AND pid <> pg_backend_pid()",
                    &[&database],
                )
                .await?;
            client
                .batch_execute(&format!("DROP DATABASE IF EXISTS {database}"))
                .await?;
        }
        for namespace in derived {
            for login in namespace.node_logins {
                client
                    .batch_execute(&format!(
                        "DROP ROLE IF EXISTS {}; DROP ROLE IF EXISTS {}",
                        login.runtime, login.readiness
                    ))
                    .await?;
            }
            client
                .batch_execute(&format!(
                    "DROP ROLE IF EXISTS {}; DROP ROLE IF EXISTS {}; DROP ROLE IF EXISTS {}",
                    namespace.runtime_group, namespace.owner, namespace.maintenance_role
                ))
                .await?;
        }
        task.abort();
        Ok(())
    }
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
