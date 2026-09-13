mod helpers;
use helpers::{
    db::{
        db_state_for_module, require_db_url, skip_without_db, DbHost, PgValue,
        PrivateDatabaseFixture,
    },
    manager::{manager_trio, register_test_module_ready, synced_routing_table},
    proxy::{http_client, proxy_get, start_proxy, TEST_SELF_PEER},
    stubs::spawn_identified_stub,
    wasm::invalid_protobuf,
};

use anyhow::Result;
use deadpool_postgres::tokio_postgres;
use http::{Request, StatusCode};
use http_body_util::Full;

use wr_common::postgres::{
    PlatformDatabases, PostgresProvisioningLimits, PostgresProvisioningManifest,
    PostgresProvisioningNamespace, PostgresProvisioningNode,
};
use wr_common::wruntime::{EngineRegistration, ModuleDescriptor, RegisterEngineRequest};

#[tokio::test]
async fn test_proxy_namespaces_are_isolated() -> Result<()> {
    // Two engines host the same module name in different namespaces.
    let (pool, mgr_addr, mut mgr) = manager_trio().await?;

    let (e_alpha_addr, e_alpha_shutdown) = spawn_identified_stub("engine-alpha").await?;
    let (e_beta_addr, e_beta_shutdown) = spawn_identified_stub("engine-beta").await?;

    register_test_module_ready(
        &pool,
        &mut mgr,
        "ea",
        &e_alpha_addr,
        "ns-alpha",
        "shared-service",
        "1.0.0",
    )
    .await?;
    register_test_module_ready(
        &pool,
        &mut mgr,
        "eb",
        &e_beta_addr,
        "ns-beta",
        "shared-service",
        "1.0.0",
    )
    .await?;

    let table = synced_routing_table(&mgr_addr).await?;
    let proxy = start_proxy(table).await?;

    // ns-alpha routes to engine-alpha, not engine-beta.
    let (s, body) = proxy_get(proxy, "ns-alpha", "shared-service", Some("1.0.0")).await?;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(
        body, "engine-alpha",
        "ns-alpha should route to engine-alpha"
    );

    // ns-beta routes to engine-beta, not engine-alpha.
    let (s, body) = proxy_get(proxy, "ns-beta", "shared-service", Some("1.0.0")).await?;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body, "engine-beta", "ns-beta should route to engine-beta");

    let _ = e_alpha_shutdown.send(());
    let _ = e_beta_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn test_proxy_returns_400_when_namespace_missing() -> Result<()> {
    let proxy_addr = start_proxy(wr_proxy::routing::new_routing_table(
        Default::default(),
        TEST_SELF_PEER,
    ))
    .await?;

    // Host has no dot — no namespace.
    let req = Request::builder()
        .uri(format!("http://{proxy_addr}/rpc"))
        .header("x-wr-destination", "http://some-service/rpc")
        .header("x-wr-source", "test")
        .body(Full::new(invalid_protobuf()))?;

    let resp = http_client().request(req).await?;
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "missing namespace in destination host should give 400"
    );

    Ok(())
}

#[tokio::test]
async fn test_manager_rejects_module_without_namespace() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    let result = c
        .register_engine(RegisterEngineRequest {
            registration: Some(EngineRegistration {
                engine_id: "e1".into(),
                address: "http://127.0.0.1:9100".into(),
                proxy_address: TEST_SELF_PEER.into(),
                peer_address: TEST_SELF_PEER.into(),
                modules: vec![ModuleDescriptor {
                    name: "svc".into(),
                    namespace: String::new(), // empty namespace → should be rejected
                    version: "1.0.0".into(),
                    proto_schema: vec![],
                }],
                secrets: vec![],
                db_namespaces: vec![],
                deployment: None,
                job_queue_id: String::new(),
                job_admin_address: String::new(),
            }),

            activation_id: uuid::Uuid::new_v4().to_string(),
        })
        .await;

    assert!(result.is_err(), "manager should reject empty namespace");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn database_grants_enforce_namespace_not_module_authorization() -> Result<()> {
    if skip_without_db("database_grants_enforce_namespace_not_module_authorization") {
        return Ok(());
    }
    use std::str::FromStr as _;
    let fixture = PrivateDatabaseFixture::create().await?;
    let suffix = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();
    let namespace_one = format!("boundary-{suffix}");
    let namespace_two = format!("other-{suffix}");
    let manifest = PostgresProvisioningManifest {
        format_version: 1,
        cluster_id: "cluster-a".into(),
        generation: 1,
        postgres_major: 18,
        postgres_ca_sha256: format!("sha256:{}", "a".repeat(64)),
        ident_map_name: "wruntime_nodes".into(),
        platform_databases: PlatformDatabases {
            manager: fixture.platform_database.clone(),
            job_queues: vec![],
        },
        extension_allowlist: vec![],
        tenant_client_cidrs: vec!["127.0.0.1/32".into()],
        limits: PostgresProvisioningLimits {
            namespace_database_connections: 20,
            runtime_login_connections: 5,
            readiness_verifier_connections: 1,
            statement_timeout_ms: 30_000,
            lock_timeout_ms: 5_000,
            idle_in_transaction_timeout_ms: 60_000,
        },
        nodes: vec![PostgresProvisioningNode {
            node_id: "node-a".into(),
            certificate_pem_path: "/unused/test.pem".into(),
            certificate_sha256: format!("sha256:{}", "b".repeat(64)),
            certificate_common_name: "wr-db-node-a".into(),
        }],
        namespaces: vec![
            PostgresProvisioningNamespace {
                namespace: namespace_one.clone(),
                modules: vec!["a".into(), "b".into()],
            },
            PostgresProvisioningNamespace {
                namespace: namespace_two.clone(),
                modules: vec!["c".into()],
            },
        ],
    };
    let derived = wr_cli::postgres::converge_sql_state(&require_db_url(), &manifest).await?;
    let one = &derived[0];
    let two = &derived[1];
    let mut admin_config = tokio_postgres::Config::from_str(&require_db_url())?;
    admin_config.dbname(&one.database);
    let (admin, driver) = admin_config.connect(tokio_postgres::NoTls).await?;
    let admin_task = tokio::spawn(driver);
    admin
        .batch_execute(&format!(
            "CREATE TABLE {}.shared_table (id bigint); CREATE TABLE {}.other_module_table (id bigint)",
            one.schemas[0], one.schemas[1]
        ))
        .await?;
    wr_cli::postgres::converge_sql_state(&require_db_url(), &manifest).await?;

    let mut runtime_config = tokio_postgres::Config::from_str(&require_db_url())?;
    runtime_config.dbname(&one.database);
    let (runtime, runtime_driver) = runtime_config.connect(tokio_postgres::NoTls).await?;
    let runtime_task = tokio::spawn(runtime_driver);
    let runtime_role: String = runtime
        .query_one("SELECT quote_ident($1)", &[&one.node_logins[0].runtime])
        .await?
        .get(0);
    runtime
        .batch_execute(&format!("SET SESSION AUTHORIZATION {runtime_role}"))
        .await?;
    runtime
        .batch_execute(&format!(
            "INSERT INTO {}.shared_table VALUES (1); SELECT * FROM {}.other_module_table",
            one.schemas[0], one.schemas[1]
        ))
        .await?;
    assert!(runtime
        .batch_execute(&format!(
            "CREATE TABLE {}.forbidden (id bigint)",
            one.schemas[0]
        ))
        .await
        .is_err());
    assert!(runtime.batch_execute("SET ROLE postgres").await.is_err());

    let cross_database_connect: bool = admin
        .query_one(
            "SELECT has_database_privilege($1,$2,'CONNECT')",
            &[&one.node_logins[0].runtime, &two.database],
        )
        .await?
        .get(0);
    assert!(!cross_database_connect);
    drop(runtime);
    runtime_task.await??;
    drop(admin);
    admin_task.await??;
    fixture.cleanup(&manifest).await?;
    Ok(())
}

// ── per-module default-schema tests ───────────────────────────────────────────
//
// These tests require WRT_TEST_DB_URL; they panic when it is absent.

/// `foo.bar` and `foo.other` each get their own default Postgres schema.
/// Unqualified SQL from `foo.other` must not resolve a table in `foo.bar`.
#[tokio::test(flavor = "multi_thread")]
async fn test_db_schema_isolation_between_modules() {
    const TABLE: &str = "_wr_isol_items";

    let mut bar = db_state_for_module(1, "foo", "bar").await;
    let mut other = db_state_for_module(1, "foo", "other").await;

    // Drop any table left by a previous test run.
    let _ = DbHost::execute(&mut bar, format!("DROP TABLE IF EXISTS {TABLE}"), vec![]).await;

    // foo.bar creates and populates its own table.
    DbHost::execute(&mut bar, format!("CREATE TABLE {TABLE} (id INT4)"), vec![])
        .await
        .expect("create table in foo.bar schema");
    DbHost::execute(&mut bar, format!("INSERT INTO {TABLE} VALUES (1)"), vec![])
        .await
        .expect("insert into foo.bar schema");

    // foo.other's schema has no such table — the query must fail.
    let result = DbHost::query(&mut other, format!("SELECT id FROM {TABLE}"), vec![]).await;
    assert!(
        result.is_err(),
        "foo.other must not see foo.bar's table; got: {result:?}",
    );

    // Clean up.
    DbHost::execute(&mut bar, format!("DROP TABLE {TABLE}"), vec![])
        .await
        .expect("drop");
}

/// Two engine instances of the same module share the same Postgres schema.
/// A row written by instance 1 must be readable by instance 2.
#[tokio::test(flavor = "multi_thread")]
async fn test_db_schema_shared_across_module_instances() {
    const TABLE: &str = "_wr_shared_items";

    // Two separate pools simulate two independent engine processes.
    let mut inst1 = db_state_for_module(1, "foo", "bar").await;
    let mut inst2 = db_state_for_module(1, "foo", "bar").await;

    // Drop any table left by a previous test run.
    let _ = DbHost::execute(&mut inst1, format!("DROP TABLE IF EXISTS {TABLE}"), vec![]).await;

    // Instance 1 creates the table and inserts a row.
    DbHost::execute(
        &mut inst1,
        format!("CREATE TABLE {TABLE} (val INT4)"),
        vec![],
    )
    .await
    .expect("create table");
    DbHost::execute(
        &mut inst1,
        format!("INSERT INTO {TABLE} VALUES (42)"),
        vec![],
    )
    .await
    .expect("insert");

    // Instance 2 reads from the same schema and must see the row.
    let rows = DbHost::query(&mut inst2, format!("SELECT val FROM {TABLE}"), vec![])
        .await
        .expect("query");
    assert_eq!(
        rows.len(),
        1,
        "instance 2 should see the row written by instance 1"
    );
    assert_eq!(rows[0].columns[0].value, PgValue::Int4(42));

    // Clean up.
    DbHost::execute(&mut inst1, format!("DROP TABLE {TABLE}"), vec![])
        .await
        .expect("drop");
}
