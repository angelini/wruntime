mod helpers;

use anyhow::Result;
use deadpool_postgres::tokio_postgres;
use helpers::db::{require_db_url, skip_without_db, PrivateDatabaseFixture};
use wr_common::postgres::{
    PlatformDatabases, PostgresProvisioningLimits, PostgresProvisioningManifest,
    PostgresProvisioningNamespace, PostgresProvisioningNode, PLATFORM_SCHEMA,
};

fn manifest(
    platform_database: String,
    namespace: String,
    other_namespace: String,
) -> PostgresProvisioningManifest {
    PostgresProvisioningManifest {
        format_version: 1,
        cluster_id: "cluster-a".into(),
        generation: 7,
        postgres_major: 18,
        postgres_ca_sha256:
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
        ident_map_name: "wruntime_nodes".into(),
        platform_databases: PlatformDatabases {
            manager: platform_database,
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
            certificate_pem_path: "/unused/in/sql-only-test.pem".into(),
            certificate_sha256:
                "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
            certificate_common_name: "wr-db-node-a".into(),
        }],
        namespaces: vec![
            PostgresProvisioningNamespace {
                namespace,
                modules: vec!["catalog".into(), "orders".into()],
            },
            PostgresProvisioningNamespace {
                namespace: other_namespace,
                modules: vec!["private".into()],
            },
        ],
    }
}

async fn connect_database(admin_url: &str, database: &str) -> Result<tokio_postgres::Client> {
    use std::str::FromStr as _;
    let mut config = tokio_postgres::Config::from_str(admin_url)?;
    config.dbname(database);
    let (client, connection) = config.connect(tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

async fn connect_as_role(
    admin_url: &str,
    database: &str,
    role: &str,
) -> Result<tokio_postgres::Client> {
    let client = connect_database(admin_url, database).await?;
    let role: String = client
        .query_one("SELECT quote_ident($1)", &[&role])
        .await?
        .get(0);
    client
        .batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await?;
    Ok(client)
}

#[tokio::test(flavor = "multi_thread")]
async fn offline_sql_provisioning_repairs_hostile_state_and_retains_unreferenced_database(
) -> Result<()> {
    if skip_without_db(
        "offline_sql_provisioning_repairs_hostile_state_and_retains_unreferenced_database",
    ) {
        return Ok(());
    }
    let fixture = PrivateDatabaseFixture::create().await?;
    let suffix = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();
    let manifest = manifest(
        fixture.platform_database.clone(),
        format!("ptest-{suffix}"),
        format!("other-{suffix}"),
    );
    manifest.validate()?;
    let admin_url = require_db_url();

    let derived = wr_cli::postgres::converge_sql_state(&admin_url, &manifest).await?;
    wr_cli::postgres::publish_sql_generation(&admin_url, &manifest).await?;
    wr_cli::postgres::publish_sql_generation(&admin_url, &manifest).await?;
    let mut changed_replay = manifest.clone();
    changed_replay.extension_allowlist.push("pgcrypto".into());
    assert!(
        wr_cli::postgres::publish_sql_generation(&admin_url, &changed_replay)
            .await
            .is_err()
    );
    let mut regressed = manifest.clone();
    regressed.generation -= 1;
    assert!(
        wr_cli::postgres::publish_sql_generation(&admin_url, &regressed)
            .await
            .is_err()
    );
    let namespace = &derived[0];
    let runtime = &namespace.node_logins[0].runtime;
    let readiness = &namespace.node_logins[0].readiness;

    let admin = connect_database(&admin_url, &namespace.database).await?;
    admin
        .batch_execute(&format!(
            "ALTER ROLE {runtime} CREATEROLE INHERIT CONNECTION LIMIT 99; \
             GRANT CREATE ON SCHEMA {} TO {runtime}; \
             GRANT ALL ON DATABASE {} TO PUBLIC; \
             CREATE TABLE {}.hostile_owner (id bigint)",
            namespace.schemas[0], namespace.database, namespace.schemas[0]
        ))
        .await?;

    wr_cli::postgres::converge_sql_state(&admin_url, &manifest).await?;

    let row = admin
        .query_one(
            "SELECT rolcreaterole,rolsuper,rolinherit,rolconnlimit FROM pg_roles WHERE rolname=$1",
            &[runtime],
        )
        .await?;
    assert!(!row.get::<_, bool>(0));
    assert!(!row.get::<_, bool>(1));
    assert!(!row.get::<_, bool>(2));
    assert_eq!(row.get::<_, i32>(3), 5);
    let database_limit: i32 = admin
        .query_one(
            "SELECT datconnlimit FROM pg_database WHERE datname=$1",
            &[&namespace.database],
        )
        .await?
        .get(0);
    assert_eq!(database_limit, 20);
    let settings: String = admin
        .query_one(
            "SELECT array_to_string(setting.setconfig,',') FROM pg_db_role_setting setting JOIN pg_roles role ON role.oid=setting.setrole JOIN pg_database database ON database.oid=setting.setdatabase WHERE role.rolname=$1 AND database.datname=$2",
            &[runtime, &namespace.database],
        )
        .await?
        .get(0);
    assert!(settings.contains("statement_timeout=30000ms"));
    assert!(settings.contains("lock_timeout=5000ms"));
    assert!(settings.contains("idle_in_transaction_session_timeout=60000ms"));
    let default_acl_count: i64 = admin
        .query_one(
            "SELECT count(*) FROM pg_default_acl defaults JOIN pg_roles owner ON owner.oid=defaults.defaclrole JOIN pg_namespace schema ON schema.oid=defaults.defaclnamespace WHERE owner.rolname=$1 AND schema.nspname=$2",
            &[&namespace.owner, &namespace.schemas[0]],
        )
        .await?
        .get(0);
    assert!(default_acl_count >= 2);
    let public_routine_default: bool = admin
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_default_acl defaults CROSS JOIN LATERAL aclexplode(defaults.defaclacl) acl JOIN pg_roles owner ON owner.oid=defaults.defaclrole WHERE owner.rolname=$1 AND defaults.defaclobjtype='f' AND defaults.defaclnamespace=0 AND acl.grantee=0 AND acl.privilege_type='EXECUTE')",
            &[&namespace.owner],
        )
        .await?
        .get(0);
    assert!(!public_routine_default);
    assert!(admin
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)",
            &[&fixture.retained_database],
        )
        .await?
        .get::<_, bool>(0));

    let runtime_client = connect_as_role(&admin_url, &namespace.database, runtime).await?;
    assert!(!runtime_client
        .query_one(
            "SELECT has_schema_privilege(current_user,$1,'CREATE')",
            &[&namespace.schemas[0]],
        )
        .await?
        .get::<_, bool>(0));
    let owner: String = admin
        .query_one(
            "SELECT tableowner FROM pg_tables WHERE schemaname=$1 AND tablename='hostile_owner'",
            &[&namespace.schemas[0]],
        )
        .await?
        .get(0);
    assert_eq!(owner, namespace.owner);
    assert!(runtime_client
        .query_one(
            "SELECT has_schema_privilege(current_user,$1,'USAGE')",
            &[&namespace.schemas[0]],
        )
        .await?
        .get::<_, bool>(0));
    runtime_client
        .batch_execute(&format!(
            "INSERT INTO {}.hostile_owner VALUES (1); SELECT * FROM {}.hostile_owner",
            namespace.schemas[0], namespace.schemas[0]
        ))
        .await?;
    assert!(runtime_client
        .query(
            &format!("SELECT * FROM {PLATFORM_SCHEMA}.namespace_state"),
            &[]
        )
        .await
        .is_err());
    let runtime_platform_connect: bool = admin
        .query_one(
            "SELECT has_database_privilege($1,$2,'CONNECT')",
            &[runtime, &fixture.platform_database],
        )
        .await?
        .get(0);
    assert!(!runtime_platform_connect);
    let runtime_other_namespace_connect: bool = admin
        .query_one(
            "SELECT has_database_privilege($1,$2,'CONNECT')",
            &[runtime, &derived[1].database],
        )
        .await?
        .get(0);
    assert!(!runtime_other_namespace_connect);

    let unsupported_target = tempfile::NamedTempFile::new()?;
    std::fs::set_permissions(
        unsupported_target.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o600),
    )?;
    assert!(wr_cli::postgres::validate_server_topology(
        &admin_url,
        &manifest,
        unsupported_target.path(),
    )
    .await
    .is_err());

    let readiness_client = connect_as_role(&admin_url, &namespace.database, readiness).await?;
    let generation: i64 = readiness_client
        .query_one(
            &format!("SELECT generation FROM {PLATFORM_SCHEMA}.namespace_state"),
            &[],
        )
        .await?
        .get(0);
    assert_eq!(generation, manifest.generation as i64);
    readiness_client
        .query(
            &format!("SELECT namespace,module,migration_version,attempt,filename,content_hash,byte_length,bundle_digest,deployment_digest,state FROM {PLATFORM_SCHEMA}.migration_attempts"),
            &[],
        )
        .await?;
    assert!(readiness_client
        .query(
            &format!("SELECT * FROM {PLATFORM_SCHEMA}.migration_retry_approvals"),
            &[],
        )
        .await
        .is_err());
    assert!(readiness_client
        .execute(
            &format!("UPDATE {PLATFORM_SCHEMA}.namespace_state SET generation=generation"),
            &[],
        )
        .await
        .is_err());
    assert!(readiness_client
        .query(
            &format!(
                "SELECT * FROM {}.missing_tenant_table",
                namespace.schemas[0]
            ),
            &[]
        )
        .await
        .is_err());

    fixture.cleanup(&manifest).await?;
    Ok(())
}
