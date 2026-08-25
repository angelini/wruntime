mod helpers;

use anyhow::Result;
use wr_engine::provisioning::{provision_namespaces, NamespaceProvisioning};

use helpers::db::{require_db_url, skip_without_db};

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_namespace_provisioning_converges() -> Result<()> {
    if skip_without_db("concurrent_namespace_provisioning_converges") {
        return Ok(());
    }
    let url = require_db_url();
    let pool = wr_engine::pool::build_pool(&url, 4)?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let namespace = format!("provision_{suffix}");
    let role = format!("wr_provision_{suffix}");
    let schema_a = format!("wr__provision_{suffix}__a");
    let schema_b = format!("wr__provision_{suffix}__b");
    let password = format!("pw{suffix}");
    let specification = NamespaceProvisioning {
        namespace,
        role: role.clone(),
        password: password.clone(),
        schemas: vec![schema_a.clone(), schema_b.clone()],
    };

    let (left, right) = tokio::join!(
        provision_namespaces(&pool, std::slice::from_ref(&specification)),
        provision_namespaces(&pool, std::slice::from_ref(&specification)),
    );
    left?;
    right?;

    let guest = wr_engine::pool::build_guest_pool(&url, &role, &password, 1)?;
    let client = guest.get().await?;
    assert_eq!(
        client
            .query_one("SELECT current_user", &[])
            .await?
            .get::<_, &str>(0),
        role
    );
    client
        .batch_execute(&format!(
            "CREATE TABLE \"{schema_a}\".current_grant_test (id INT); \
             CREATE SEQUENCE \"{schema_b}\".default_grant_test"
        ))
        .await?;
    let drop_error = client
        .batch_execute(&format!("DROP SCHEMA \"{schema_a}\" CASCADE"))
        .await
        .expect_err("namespace role must not own its module schema");
    assert_eq!(
        drop_error.as_db_error().map(|error| error.code().code()),
        Some("42501")
    );
    drop(client);
    drop(guest);

    let admin = pool.get().await?;
    let owner: String = admin
        .query_one(
            "SELECT schema_owner FROM information_schema.schemata WHERE schema_name = $1",
            &[&schema_a],
        )
        .await?
        .get(0);
    let current_user: String = admin.query_one("SELECT current_user", &[]).await?.get(0);
    assert_eq!(owner, current_user);
    admin
        .batch_execute(&format!(
            "DROP SCHEMA \"{schema_a}\" CASCADE; \
             DROP SCHEMA \"{schema_b}\" CASCADE; \
             DROP ROLE \"{role}\""
        ))
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn provisioning_failure_rolls_back_namespace_contract() -> Result<()> {
    if skip_without_db("provisioning_failure_rolls_back_namespace_contract") {
        return Ok(());
    }
    let url = require_db_url();
    let pool = wr_engine::pool::build_pool(&url, 2)?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let role = format!("wr_provision_fail_{suffix}");
    let schema = format!("wr__provision_fail_{suffix}");
    let specification = NamespaceProvisioning {
        namespace: format!("provision_fail_{suffix}"),
        role: role.clone(),
        password: format!("pw{suffix}"),
        schemas: vec![schema.clone(), "pg_catalog".into()],
    };

    assert!(provision_namespaces(&pool, &[specification]).await.is_err());
    let admin = pool.get().await?;
    assert!(!admin
        .query_one(
            "SELECT EXISTS (SELECT FROM pg_roles WHERE rolname = $1)",
            &[&role]
        )
        .await?
        .get::<_, bool>(0));
    assert!(!admin
        .query_one(
            "SELECT EXISTS (SELECT FROM information_schema.schemata WHERE schema_name = $1)",
            &[&schema],
        )
        .await?
        .get::<_, bool>(0));
    Ok(())
}
