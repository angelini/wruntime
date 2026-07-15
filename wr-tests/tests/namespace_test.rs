mod helpers;
use helpers::{
    db::{db_state_for_module, DbHost, PgValue},
    manager::{manager_trio, register_test_module_ready, synced_routing_table},
    proxy::{http_client, proxy_get, start_proxy, TEST_SELF_PEER},
    stubs::spawn_identified_stub,
    wasm::invalid_protobuf,
};

use anyhow::Result;
use http::{Request, StatusCode};
use http_body_util::Full;

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
    let proxy_addr = start_proxy(wr_proxy::routing::new_routing_table()).await?;

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
            }),
        })
        .await;

    assert!(result.is_err(), "manager should reject empty namespace");
    Ok(())
}

// ── per-module DB schema isolation tests ──────────────────────────────────────
//
// These tests require WRT_TEST_DB_URL; they panic when it is absent.

/// `foo.bar` and `foo.other` each get their own Postgres schema.
/// A table created by `foo.bar` must not be visible to `foo.other`.
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
