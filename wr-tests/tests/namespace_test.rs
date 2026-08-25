mod helpers;
use helpers::{
    db::{db_state_for_module, require_db_url, skip_without_db, DbHost, PgValue},
    manager::{manager_trio, register_test_module_ready, synced_routing_table},
    proxy::{http_client, proxy_get, start_proxy, TEST_SELF_PEER},
    stubs::spawn_identified_stub,
    wasm::invalid_protobuf,
};

use anyhow::Result;
use http::{Request, StatusCode};
use http_body_util::Full;

use wr_common::wruntime::{EngineRegistration, ModuleDescriptor, RegisterEngineRequest};
use wr_engine::provisioning::{provision_namespaces, NamespaceProvisioning};

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
            }),
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
    let url = require_db_url();
    let admin_pool = wr_engine::pool::build_pool(&url, 4)?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let role_one = format!("wr_boundary_one_{suffix}");
    let role_two = format!("wr_boundary_two_{suffix}");
    let password_one = format!("one{suffix}");
    let password_two = format!("two{suffix}");
    let schema_a = format!("wr__boundary_{suffix}__a");
    let schema_b = format!("wr__boundary_{suffix}__b");
    let schema_other = format!("wr__other_{suffix}__c");
    provision_namespaces(
        &admin_pool,
        &[
            NamespaceProvisioning {
                namespace: format!("boundary_{suffix}"),
                role: role_one.clone(),
                password: password_one.clone(),
                schemas: vec![schema_a.clone(), schema_b.clone()],
            },
            NamespaceProvisioning {
                namespace: format!("other_{suffix}"),
                role: role_two.clone(),
                password: password_two.clone(),
                schemas: vec![schema_other.clone()],
            },
        ],
    )
    .await?;
    let admin = admin_pool.get().await?;
    admin
        .batch_execute(&format!(
            "CREATE SCHEMA IF NOT EXISTS wr_system; \
             CREATE TABLE \"{schema_a}\".admin_table (id INT); \
             CREATE SEQUENCE \"{schema_a}\".admin_sequence; \
             CREATE FUNCTION \"{schema_a}\".admin_function() RETURNS INT LANGUAGE SQL AS 'SELECT 1'; \
             CREATE TABLE IF NOT EXISTS wr_system.boundary_secret (id INT)"
        ))
        .await?;

    let one_pool = wr_engine::pool::build_guest_pool(&url, &role_one, &password_one, 1)?;
    let one = one_pool.get().await?;
    one.batch_execute(&format!("SET search_path = \"{schema_a}\""))
        .await?;
    one.batch_execute("CREATE TABLE unqualified_table (id INT)")
        .await?;
    assert!(one
        .query_one(
            "SELECT to_regclass($1) IS NOT NULL",
            &[&format!("{schema_a}.unqualified_table")],
        )
        .await?
        .get::<_, bool>(0));
    one.batch_execute(&format!(
        "CREATE TABLE \"{schema_b}\".qualified_table (id INT); \
         SELECT nextval('\"{schema_a}\".admin_sequence'); \
         SELECT \"{schema_a}\".admin_function(); \
         SELECT * FROM \"{schema_a}\".admin_table"
    ))
    .await?;
    assert!(one
        .query("SELECT * FROM wr_system.boundary_secret", &[])
        .await
        .is_err());
    assert!(one
        .batch_execute("CREATE TABLE wr_system.guest_forbidden (id INT)")
        .await
        .is_err());
    drop(one);
    drop(one_pool);

    let two_pool = wr_engine::pool::build_guest_pool(&url, &role_two, &password_two, 1)?;
    let two = two_pool.get().await?;
    assert!(two
        .query(&format!("SELECT * FROM \"{schema_a}\".admin_table"), &[])
        .await
        .is_err());
    drop(two);
    drop(two_pool);

    admin
        .execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = ANY($1) AND pid <> pg_backend_pid()",
            &[&vec![role_one.clone(), role_two.clone()]],
        )
        .await?;
    admin
        .batch_execute(&format!(
            "DROP SCHEMA \"{schema_a}\" CASCADE; \
             DROP SCHEMA \"{schema_b}\" CASCADE; \
             DROP SCHEMA \"{schema_other}\" CASCADE; \
             DROP TABLE wr_system.boundary_secret; \
             DROP ROLE \"{role_one}\"; \
             DROP ROLE \"{role_two}\""
        ))
        .await?;
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
