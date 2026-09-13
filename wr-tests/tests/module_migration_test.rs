mod helpers;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use deadpool_postgres::tokio_postgres;
use helpers::db::{require_db_url, skip_without_db, PrivateDatabaseFixture};
use wr_common::migration_bundle::{MigrationBundle, MigrationBundleManifest, MigrationLimits};
use wr_common::postgres::{
    PlatformDatabases, PostgresProvisioningLimits, PostgresProvisioningManifest,
    PostgresProvisioningNamespace, PostgresProvisioningNode, PLATFORM_SCHEMA,
};

fn provisioning(
    platform_database: String,
    namespace: String,
    modules: Vec<String>,
) -> PostgresProvisioningManifest {
    PostgresProvisioningManifest {
        format_version: 1,
        cluster_id: "cluster-a".into(),
        generation: 1,
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
            certificate_pem_path: "/unused/offline-test.pem".into(),
            certificate_sha256:
                "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
            certificate_common_name: "wr-db-node-a".into(),
        }],
        namespaces: vec![PostgresProvisioningNamespace { namespace, modules }],
    }
}

fn migration_bundle(
    namespace: &str,
    sql: &str,
    deadline_ms: u64,
) -> Result<(tempfile::TempDir, MigrationBundle)> {
    let directory = tempfile::tempdir()?;
    std::fs::write(directory.path().join("V1__migration.sql"), sql)?;
    let captured = MigrationBundleManifest::capture_sources(
        format!("sha256:{}", "d".repeat(64)),
        MigrationLimits {
            max_migrations_per_namespace: 8,
            max_file_bytes: 64 * 1024,
            max_startup_bytes: 128 * 1024,
            file_deadline_ms: deadline_ms,
            cancellation_grace_ms: 500,
        },
        &[(
            namespace.to_string(),
            "catalog".into(),
            PathBuf::from(directory.path()),
        )],
    )?;
    Ok((directory, captured))
}

fn migration_bundle_sources(
    namespace: &str,
    modules: &[(&str, &[(&str, &str)])],
    deadline_ms: u64,
) -> Result<(Vec<tempfile::TempDir>, MigrationBundle)> {
    let mut directories = Vec::with_capacity(modules.len());
    for (_, files) in modules {
        let directory = tempfile::tempdir()?;
        for (filename, sql) in *files {
            std::fs::write(directory.path().join(filename), sql)?;
        }
        directories.push(directory);
    }
    let sources = modules
        .iter()
        .zip(&directories)
        .map(|((module, _), directory)| {
            (
                namespace.to_string(),
                (*module).to_string(),
                directory.path().to_path_buf(),
            )
        })
        .collect::<Vec<_>>();
    let captured = MigrationBundleManifest::capture_sources(
        format!("sha256:{}", "e".repeat(64)),
        MigrationLimits {
            max_migrations_per_namespace: 32,
            max_file_bytes: 256 * 1024,
            max_startup_bytes: 2 * 1024 * 1024,
            file_deadline_ms: deadline_ms,
            cancellation_grace_ms: 500,
        },
        &sources,
    )?;
    Ok((directories, captured))
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

async fn verify_with_trust_fixture(
    access: wr_engine::tenant_db::VerifiedNamespaceAccess,
    bundle: &MigrationBundle,
) -> Result<()> {
    let base_url = require_db_url();
    wr_engine::tenant_db::verify_readiness_with(&[access], Some(bundle), move |access| {
        let base_url = base_url.clone();
        async move {
            use std::str::FromStr as _;
            let mut config = tokio_postgres::Config::from_str(&base_url)?;
            config.dbname(&access.descriptor.database);
            let (client, connection) = config.connect(tokio_postgres::NoTls).await?;
            let driver = tokio::spawn(connection);
            let authorize = async {
                let role: String = client
                    .query_one(
                        "SELECT quote_ident($1)",
                        &[&access.descriptor.readiness_role],
                    )
                    .await?
                    .get(0);
                client
                    .batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
                    .await?;
                Ok::<(), anyhow::Error>(())
            }
            .await;
            if let Err(error) = authorize {
                driver.abort();
                return Err(error);
            }
            Ok((client, driver))
        }
    })
    .await
}

fn provisioning_test_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

async fn setup_with_modules(
    modules: Vec<String>,
) -> Result<(
    PrivateDatabaseFixture,
    PostgresProvisioningManifest,
    wr_common::postgres::DerivedNamespace,
)> {
    let _guard = provisioning_test_lock().lock().await;
    let fixture = PrivateDatabaseFixture::create().await?;
    let suffix = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();
    let manifest = provisioning(
        fixture.platform_database.clone(),
        format!("migration-{suffix}"),
        modules,
    );
    let namespace = wr_cli::postgres::converge_sql_state(&require_db_url(), &manifest)
        .await?
        .remove(0);
    Ok((fixture, manifest, namespace))
}

async fn setup() -> Result<(
    PrivateDatabaseFixture,
    PostgresProvisioningManifest,
    wr_common::postgres::DerivedNamespace,
)> {
    setup_with_modules(vec!["catalog".into()]).await
}

#[tokio::test(flavor = "multi_thread")]
async fn offline_migration_uses_module_schema_owner_and_removes_executor() -> Result<()> {
    if skip_without_db("offline_migration_uses_module_schema_owner_and_removes_executor") {
        return Ok(());
    }
    let (fixture, manifest, namespace) = setup().await?;
    let (_directory, bundle) = migration_bundle(
        &namespace.namespace,
        "CREATE TABLE inventory (id bigint PRIMARY KEY);",
        5_000,
    )?;
    wr_cli::postgres::migration::migrate_bundle(&require_db_url(), &manifest, bundle.clone())
        .await?;
    // A successful exact replay is a no-op; deployment receipt emission separately verifies
    // this same ledger before atomically writing the deterministic receipt.
    wr_cli::postgres::migration::migrate_bundle(&require_db_url(), &manifest, bundle.clone())
        .await?;

    let admin = connect_database(&require_db_url(), &namespace.database).await?;
    let row = admin
        .query_one(
            "SELECT schemaname,tableowner FROM pg_catalog.pg_tables WHERE tablename='inventory'",
            &[],
        )
        .await?;
    assert_eq!(row.get::<_, String>(0), namespace.schemas[0]);
    assert_eq!(row.get::<_, String>(1), namespace.owner);
    let executor_prefix = wr_common::naming::migration_executor_prefix(&namespace.namespace);
    assert!(!admin
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_roles WHERE rolname LIKE $1) OR EXISTS(SELECT 1 FROM pg_catalog.pg_stat_activity WHERE usename LIKE $1)",
            &[&format!("{executor_prefix}%")],
        )
        .await?
        .get::<_, bool>(0));

    // The engine-side verifier consumes the immutable ledger through a direct,
    // single-use connection. This trust-backed SQL-only seam authenticates as
    // the test administrator and assumes the readiness role over NoTls;
    // production configuration has no insecure connector selection.
    wr_cli::postgres::publish_sql_generation(&require_db_url(), &manifest).await?;
    let readiness_role = namespace.node_logins[0].readiness.clone();
    let access = wr_engine::tenant_db::VerifiedNamespaceAccess {
        descriptor: wr_common::wruntime::NamespaceAccessDescriptor {
            namespace: namespace.namespace.clone(),
            database: namespace.database.clone(),
            runtime_role: namespace.node_logins[0].runtime.clone(),
            readiness_role: readiness_role.clone(),
        },
        expectation: wr_engine::config::NamespaceDatabaseExpectation {
            namespace: namespace.namespace.clone(),
            generation: manifest.generation,
            deployment_digest: bundle.manifest.deployment_digest.clone(),
            bundle_digest: bundle.manifest.bundle_digest.clone(),
            migrations: vec![],
        },
    };
    verify_with_trust_fixture(access.clone(), &bundle).await?;

    // A succeeded row retains the release digests from the deployment that
    // applied it. A later authenticated bundle with the same immutable file
    // must accept that historical row rather than demanding rewritten history.
    let mut next_bundle = bundle.clone();
    next_bundle.manifest.deployment_digest = format!("sha256:{}", "e".repeat(64));
    next_bundle.manifest.bundle_digest = next_bundle.manifest.computed_digest()?;
    let mut next_access = access.clone();
    next_access.expectation.deployment_digest = next_bundle.manifest.deployment_digest.clone();
    next_access.expectation.bundle_digest = next_bundle.manifest.bundle_digest.clone();
    verify_with_trust_fixture(next_access, &next_bundle).await?;

    let readiness_connections: i64 = admin
        .query_one(
            "SELECT count(*) FROM pg_stat_activity WHERE usename=$1",
            &[&readiness_role],
        )
        .await?
        .get(0);
    assert_eq!(readiness_connections, 0);

    admin
        .execute(
            &format!("UPDATE {PLATFORM_SCHEMA}.namespace_state SET generation=generation+1"),
            &[],
        )
        .await?;
    assert!(verify_with_trust_fixture(access.clone(), &bundle)
        .await
        .is_err());
    admin
        .execute(
            &format!("UPDATE {PLATFORM_SCHEMA}.namespace_state SET generation=$1"),
            &[&(manifest.generation as i64)],
        )
        .await?;
    admin
        .execute(
            &format!("INSERT INTO {PLATFORM_SCHEMA}.migration_attempts(namespace,module,migration_version,attempt,filename,content_hash,byte_length,bundle_digest,deployment_digest,state) VALUES($1,'unexpected',1,1,'V1__unexpected.sql',$2,0,$3,$4,'started')"),
            &[&namespace.namespace, &format!("sha256:{}", "f".repeat(64)), &bundle.manifest.bundle_digest, &bundle.manifest.deployment_digest],
        )
        .await?;
    assert!(verify_with_trust_fixture(access, &bundle).await.is_err());

    let changed = migration_bundle(
        &namespace.namespace,
        "CREATE TABLE replacement(id int);",
        5_000,
    )?
    .1;
    assert!(
        wr_cli::postgres::migration::migrate_bundle(&require_db_url(), &manifest, changed)
            .await
            .is_err()
    );
    fixture.cleanup(&manifest).await
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_guard_and_reset_role_are_fail_closed() -> Result<()> {
    if skip_without_db("failed_guard_and_reset_role_are_fail_closed") {
        return Ok(());
    }
    for scenario in [
        "security-definer",
        "unsafe-routine-path",
        "reset-role",
        "public-grant",
        "pg-catalog-create",
        "public-create",
        "platform-create",
    ] {
        let (fixture, manifest, namespace) = setup().await?;
        let schema = &namespace.schemas[0];
        let sql = match scenario {
            "security-definer" => format!(
                "CREATE FUNCTION \"{schema}\".unsafe() RETURNS int LANGUAGE sql SECURITY DEFINER AS 'SELECT 1';"
            ),
            "unsafe-routine-path" => format!(
                "CREATE FUNCTION \"{schema}\".unsafe_path() RETURNS int LANGUAGE sql AS 'SELECT 1';"
            ),
            "reset-role" => format!("RESET ROLE; CREATE TABLE \"{schema}\".escaped(id int);"),
            "public-grant" => format!(
                "CREATE TABLE \"{schema}\".public_grant(id int); GRANT SELECT ON \"{schema}\".public_grant TO PUBLIC;"
            ),
            "pg-catalog-create" => "CREATE TABLE pg_catalog.forbidden(id int);".into(),
            "public-create" => "CREATE TABLE public.forbidden(id int);".into(),
            "platform-create" => {
                format!("CREATE TABLE {PLATFORM_SCHEMA}.forbidden(id int);")
            }
            _ => unreachable!(),
        };
        let (_directory, bundle) = migration_bundle(&namespace.namespace, &sql, 5_000)?;
        assert!(wr_cli::postgres::migration::migrate_bundle(
            &require_db_url(),
            &manifest,
            bundle.clone()
        )
        .await
        .is_err());
        assert!(
            wr_cli::postgres::migration::migrate_bundle(&require_db_url(), &manifest, bundle)
                .await
                .is_err()
        );
        let admin = connect_database(&require_db_url(), &namespace.database).await?;
        let row = admin
            .query_one(
                &format!("SELECT state,failure_code FROM {PLATFORM_SCHEMA}.migration_attempts"),
                &[],
            )
            .await?;
        assert_eq!(row.get::<_, String>(0), "failed");
        assert!(!row.get::<_, String>(1).is_empty());
        fixture.cleanup(&manifest).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn module_order_multi_version_and_catalog_pinned_guard_are_stable() -> Result<()> {
    if skip_without_db("module_order_multi_version_and_catalog_pinned_guard_are_stable") {
        return Ok(());
    }
    let (fixture, manifest, namespace) =
        setup_with_modules(vec!["alpha".into(), "beta".into()]).await?;
    let v1 = [("V1__shared.sql", "CREATE TABLE shared_name(id int);")];
    let (_directories, first) = migration_bundle_sources(
        &namespace.namespace,
        &[("beta", &v1), ("alpha", &v1)],
        5_000,
    )?;
    assert_eq!(first.files[0].manifest.module, "alpha");
    assert_eq!(first.files[1].manifest.module, "beta");
    wr_cli::postgres::migration::migrate_bundle(&require_db_url(), &manifest, first).await?;

    let alpha_schema = wr_common::naming::module_schema(&namespace.namespace, "alpha");
    let shadow_sql = format!(
        "CREATE TABLE pg_roles(marker text); SELECT pg_catalog.set_config('search_path','\"{alpha_schema}\"',true); CREATE TABLE guard_shadow_passed(id int);"
    );
    let alpha_files = [
        ("V1__shared.sql", "CREATE TABLE shared_name(id int);"),
        ("V2__shadow.sql", shadow_sql.as_str()),
    ];
    let (_directories, second) = migration_bundle_sources(
        &namespace.namespace,
        &[("alpha", &alpha_files), ("beta", &v1)],
        5_000,
    )?;
    wr_cli::postgres::migration::migrate_bundle(&require_db_url(), &manifest, second).await?;

    let admin = connect_database(&require_db_url(), &namespace.database).await?;
    for module in ["alpha", "beta"] {
        let schema = wr_common::naming::module_schema(&namespace.namespace, module);
        assert!(admin
            .query_one(
                "SELECT pg_catalog.to_regclass($1) IS NOT NULL",
                &[&format!("{schema}.shared_name")],
            )
            .await?
            .get::<_, bool>(0));
    }
    assert!(admin
        .query_one(
            "SELECT pg_catalog.to_regclass($1) IS NOT NULL AND pg_catalog.to_regclass($2) IS NOT NULL",
            &[&format!("{alpha_schema}.pg_roles"), &format!("{alpha_schema}.guard_shadow_passed")],
        )
        .await?
        .get::<_, bool>(0));
    let attempts: i64 = admin
        .query_one(
            &format!(
                "SELECT count(*) FROM {PLATFORM_SCHEMA}.migration_attempts WHERE state='succeeded'"
            ),
            &[],
        )
        .await?
        .get(0);
    assert_eq!(attempts, 3);
    fixture.cleanup(&manifest).await
}

#[tokio::test(flavor = "multi_thread")]
async fn example_migration_bundles_run_offline_in_derived_schemas() -> Result<()> {
    if skip_without_db("example_migration_bundles_run_offline_in_derived_schemas") {
        return Ok(());
    }
    let modules = vec![
        "agent".to_string(),
        "coordinator".to_string(),
        "exchange".to_string(),
        "inventory".to_string(),
        "ledger".to_string(),
    ];
    let (fixture, manifest, namespace) = setup_with_modules(modules).await?;
    let sources = [
        ("agent", "examples/codegen/agent/migrations"),
        ("coordinator", "examples/codegen/coordinator/migrations"),
        ("exchange", "examples/stockmarket/exchange/migrations"),
        ("inventory", "examples/ecommerce/inventory/migrations"),
        ("ledger", "examples/stockmarket/ledger/migrations"),
    ]
    .map(|(module, path)| {
        (
            namespace.namespace.clone(),
            module.to_string(),
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("..")
                .join(path),
        )
    });
    let bundle = MigrationBundleManifest::capture_sources(
        format!("sha256:{}", "f".repeat(64)),
        MigrationLimits {
            max_migrations_per_namespace: 16,
            max_file_bytes: 256 * 1024,
            max_startup_bytes: 2 * 1024 * 1024,
            file_deadline_ms: 5_000,
            cancellation_grace_ms: 500,
        },
        &sources,
    )?;
    wr_cli::postgres::migration::migrate_bundle(&require_db_url(), &manifest, bundle).await?;

    let admin = connect_database(&require_db_url(), &namespace.database).await?;
    for (module, table) in [
        ("agent", "sessions"),
        ("coordinator", "tasks"),
        ("exchange", "orders"),
        ("inventory", "inventory"),
        ("ledger", "trades"),
    ] {
        let schema = wr_common::naming::module_schema(&namespace.namespace, module);
        let row = admin
            .query_one(
                "SELECT pg_catalog.to_regclass($1) IS NOT NULL, tableowner FROM pg_catalog.pg_tables WHERE schemaname=$2 AND tablename=$3",
                &[&format!("{schema}.{table}"), &schema, &table],
            )
            .await?;
        assert!(row.get::<_, bool>(0));
        assert_eq!(row.get::<_, String>(1), namespace.owner);
    }
    fixture.cleanup(&manifest).await
}

#[tokio::test(flavor = "multi_thread")]
async fn timeout_and_crash_require_fresh_single_use_exact_approval() -> Result<()> {
    if skip_without_db("timeout_and_crash_require_fresh_single_use_exact_approval") {
        return Ok(());
    }
    let (fixture, manifest, namespace) = setup().await?;
    let (_directory, bundle) = migration_bundle(&namespace.namespace, "SELECT pg_sleep(10);", 100)?;
    assert!(wr_cli::postgres::migration::migrate_bundle(
        &require_db_url(),
        &manifest,
        bundle.clone()
    )
    .await
    .is_err());

    let file = &bundle.files[0].manifest;
    let approval = wr_cli::postgres::migration::RetryApproval {
        namespace: file.namespace.clone(),
        module: file.module.clone(),
        version: file.version,
        attempt: 1,
        state: "failed".into(),
        content_hash: file.content_hash.clone(),
        operator: "test-operator".into(),
        reason: "exercise one exact retry".into(),
    };
    wr_cli::postgres::migration::approve_retry(&require_db_url(), &manifest, &approval).await?;
    assert!(wr_cli::postgres::migration::migrate_bundle(
        &require_db_url(),
        &manifest,
        bundle.clone()
    )
    .await
    .is_err());
    assert!(wr_cli::postgres::migration::migrate_bundle(
        &require_db_url(),
        &manifest,
        bundle.clone()
    )
    .await
    .is_err());

    let admin = connect_database(&require_db_url(), &namespace.database).await?;
    let consumed: bool = admin
        .query_one(
            &format!(
                "SELECT consumed_at IS NOT NULL FROM {PLATFORM_SCHEMA}.migration_retry_approvals"
            ),
            &[],
        )
        .await?
        .get(0);
    assert!(consumed);
    let attempts: i64 = admin
        .query_one(
            &format!("SELECT count(*) FROM {PLATFORM_SCHEMA}.migration_attempts"),
            &[],
        )
        .await?
        .get(0);
    assert_eq!(attempts, 2);

    // Aborting after admission simulates process loss. The next invocation must preserve the
    // nonterminal row, remove any orphan executor, and still refuse normal execution.
    let approval = wr_cli::postgres::migration::RetryApproval {
        attempt: 2,
        state: "failed".into(),
        reason: "create an interrupted third attempt".into(),
        ..approval
    };
    wr_cli::postgres::migration::approve_retry(&require_db_url(), &manifest, &approval).await?;
    let run_manifest = manifest.clone();
    let run_bundle = bundle.clone();
    let admin_url = require_db_url();
    let task = tokio::spawn(async move {
        wr_cli::postgres::migration::migrate_bundle(&admin_url, &run_manifest, run_bundle).await
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let state: Option<String> = admin
            .query_opt(
                &format!("SELECT state FROM {PLATFORM_SCHEMA}.migration_attempts WHERE attempt=3"),
                &[],
            )
            .await?
            .map(|row| row.get(0));
        if state.as_deref() == Some("started") {
            break;
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "third attempt did not start"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    task.abort();
    let _ = task.await;
    assert!(
        wr_cli::postgres::migration::migrate_bundle(&require_db_url(), &manifest, bundle)
            .await
            .context("ambiguous attempt should block")
            .is_err()
    );
    let executor_prefix = wr_common::naming::migration_executor_prefix(&namespace.namespace);
    assert!(!admin
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_roles WHERE rolname LIKE $1) OR EXISTS(SELECT 1 FROM pg_catalog.pg_stat_activity WHERE usename LIKE $1)",
            &[&format!("{executor_prefix}%")],
        )
        .await?
        .get::<_, bool>(0));
    fixture.cleanup(&manifest).await
}
