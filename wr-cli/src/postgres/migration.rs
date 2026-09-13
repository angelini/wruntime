use std::str::FromStr;
use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
use tokio_postgres::{Client, Config, NoTls};
use wr_common::migration_bundle::{CapturedMigrationFile, MigrationBundle};
use wr_common::naming::{migration_executor_prefix, module_schema};
use wr_common::postgres::{
    DerivedNamespace, PostgresProvisioningManifest, MIGRATION_EXECUTOR_AUTH_MARKER_ROLE,
    PLATFORM_SCHEMA,
};

const MIGRATION_LOCK_DOMAIN: &[u8] = b"wruntime-namespace-migration-lock-v1\0";
const FAILURE_CODE_LIMIT: usize = 64;

#[derive(Clone, Debug)]
pub struct RetryApproval {
    pub namespace: String,
    pub module: String,
    pub version: u64,
    pub attempt: i64,
    pub state: String,
    pub content_hash: String,
    pub operator: String,
    pub reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum AttemptOutcome {
    Succeeded,
    Failed(String),
    Ambiguous,
}

pub async fn migrate_bundle(
    admin_url: &str,
    provisioning: &PostgresProvisioningManifest,
    bundle: MigrationBundle,
) -> Result<()> {
    provisioning.validate()?;
    bundle.manifest.validate()?;
    let desired = provisioning.derive_namespaces();
    for file in &bundle.files {
        ensure!(
            desired
                .iter()
                .any(|namespace| namespace.namespace == file.manifest.namespace),
            "migration namespace is absent from provisioning manifest"
        );
    }

    let mut offset = 0;
    while offset < bundle.files.len() {
        let namespace_name = &bundle.files[offset].manifest.namespace;
        let end = bundle.files[offset..]
            .iter()
            .position(|file| file.manifest.namespace != *namespace_name)
            .map_or(bundle.files.len(), |relative| offset + relative);
        let namespace = desired
            .iter()
            .find(|namespace| namespace.namespace == *namespace_name)
            .expect("validated namespace");
        migrate_namespace(
            admin_url,
            provisioning,
            namespace,
            &bundle,
            &bundle.files[offset..end],
        )
        .await
        .with_context(|| format!("migrating namespace {namespace_name}"))?;
        offset = end;
    }
    Ok(())
}

pub async fn approve_retry(
    admin_url: &str,
    provisioning: &PostgresProvisioningManifest,
    approval: &RetryApproval,
) -> Result<()> {
    ensure!(!approval.operator.trim().is_empty(), "operator is required");
    ensure!(
        !approval.reason.trim().is_empty(),
        "approval reason is required"
    );
    ensure!(
        approval.reason.len() <= 1_024,
        "approval reason is too long"
    );
    ensure!(
        matches!(approval.state.as_str(), "started" | "failed"),
        "approval state must be started or failed"
    );
    let namespace = provisioning
        .derive_namespaces()
        .into_iter()
        .find(|namespace| namespace.namespace == approval.namespace)
        .context("approval namespace is absent from provisioning manifest")?;
    let (client, driver) = connect_database(admin_url, &namespace.database).await?;
    let task = tokio::spawn(driver);
    ensure_platform_version(&client).await?;
    let row = client
        .query_opt(
            &format!("SELECT attempt,state,content_hash FROM {PLATFORM_SCHEMA}.migration_attempts WHERE namespace=$1 AND module=$2 AND migration_version=$3 ORDER BY attempt DESC LIMIT 1"),
            &[&approval.namespace, &approval.module, &(approval.version as i64)],
        )
        .await?
        .context("migration attempt does not exist")?;
    ensure!(
        row.get::<_, i64>(0) == approval.attempt,
        "stale retry approval attempt"
    );
    ensure!(
        row.get::<_, String>(1) == approval.state,
        "stale retry approval state"
    );
    ensure!(
        row.get::<_, String>(2) == approval.content_hash,
        "retry approval hash mismatch"
    );
    client.execute(
        &format!("INSERT INTO {PLATFORM_SCHEMA}.migration_retry_approvals(namespace,module,migration_version,attempt,state,content_hash,operator_name,reason) VALUES($1,$2,$3,$4,$5,$6,$7,$8)"),
        &[&approval.namespace, &approval.module, &(approval.version as i64), &approval.attempt, &approval.state, &approval.content_hash, &approval.operator, &approval.reason],
    ).await.context("recording single-use retry approval")?;
    drop(client);
    let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
    Ok(())
}

async fn migrate_namespace(
    admin_url: &str,
    provisioning: &PostgresProvisioningManifest,
    namespace: &DerivedNamespace,
    bundle: &MigrationBundle,
    files: &[CapturedMigrationFile],
) -> Result<()> {
    let (mut platform, platform_driver) = connect_database(admin_url, &namespace.database).await?;
    let platform_task = tokio::spawn(platform_driver);
    ensure_platform_version(&platform).await?;
    let lock_key = namespace_lock_key(&namespace.namespace);
    platform
        .execute("SELECT pg_advisory_lock($1)", &[&lock_key])
        .await?;
    cleanup_orphan_executors(&platform, namespace).await?;

    for file in files {
        let Some(attempt) = admit_attempt(&mut platform, bundle, file).await? else {
            continue;
        };
        let mut executor = create_executor(&platform, namespace).await?;
        let execution = execute_file(
            admin_url,
            &platform,
            ExecutionPolicy {
                provisioning,
                namespace,
                deadline: bundle.manifest.limits.file_deadline(),
                grace: bundle.manifest.limits.cancellation_grace(),
            },
            &mut executor,
            file,
        )
        .await;
        let outcome = match execution {
            Ok(outcome) => outcome,
            Err(_) => AttemptOutcome::Ambiguous,
        };
        cleanup_executor(
            &platform,
            namespace,
            &executor.role,
            executor.backend_pid,
            bundle.manifest.limits.cancellation_grace(),
        )
        .await?;
        match outcome {
            AttemptOutcome::Succeeded => {
                terminalize(&platform, file, attempt, "succeeded", None).await?;
            }
            AttemptOutcome::Failed(code) => {
                terminalize(&platform, file, attempt, "failed", Some(&code)).await?;
                bail!("migration failed with confirmed rollback; explicit exact-artifact approval is required");
            }
            AttemptOutcome::Ambiguous => {
                bail!("migration outcome is ambiguous; started ledger state is retained and explicit exact-artifact approval is required");
            }
        }
    }

    platform
        .execute("SELECT pg_advisory_unlock($1)", &[&lock_key])
        .await?;
    drop(platform);
    let _ = tokio::time::timeout(Duration::from_secs(2), platform_task).await;
    Ok(())
}

async fn ensure_platform_version(client: &Client) -> Result<()> {
    let version: i32 = client
        .query_one("SELECT current_setting('server_version_num')::int", &[])
        .await?
        .get(0);
    ensure!(
        version / 10_000 == wr_common::postgres::SUPPORTED_POSTGRES_MAJOR as i32,
        "unsupported PostgreSQL server major version"
    );
    let present: bool = client.query_one(
        "SELECT to_regclass('wr__platform.migration_attempts') IS NOT NULL AND to_regclass('wr__platform.migration_retry_approvals') IS NOT NULL",
        &[],
    ).await?.get(0);
    ensure!(present, "platform migration ledger is not provisioned");
    Ok(())
}

async fn admit_attempt(
    platform: &mut Client,
    bundle: &MigrationBundle,
    file: &CapturedMigrationFile,
) -> Result<Option<i64>> {
    let tx = platform.transaction().await?;
    let row = tx.query_opt(
        &format!("SELECT attempt,state,filename,content_hash,bundle_digest,deployment_digest FROM {PLATFORM_SCHEMA}.migration_attempts WHERE namespace=$1 AND module=$2 AND migration_version=$3 ORDER BY attempt DESC LIMIT 1 FOR UPDATE"),
        &[&file.manifest.namespace, &file.manifest.module, &(file.manifest.version as i64)],
    ).await?;
    let attempt = if let Some(row) = row {
        let previous_attempt: i64 = row.get(0);
        let state: String = row.get(1);
        let immutable_identity = row.get::<_, String>(2) == file.manifest.filename
            && row.get::<_, String>(3) == file.manifest.content_hash;
        if state == "succeeded" {
            ensure!(
                immutable_identity,
                "successful migration identity is immutable"
            );
            tx.commit().await?;
            return Ok(None);
        }
        let exact_attempt = immutable_identity
            && row.get::<_, String>(4) == bundle.manifest.bundle_digest
            && row.get::<_, String>(5) == bundle.manifest.deployment_digest;
        ensure!(
            exact_attempt,
            "blocked migration may only retry the exact immutable artifact"
        );
        ensure!(
            matches!(state.as_str(), "started" | "failed"),
            "invalid migration ledger state"
        );
        let approval = tx.query_opt(
            &format!("SELECT approval_id FROM {PLATFORM_SCHEMA}.migration_retry_approvals WHERE namespace=$1 AND module=$2 AND migration_version=$3 AND attempt=$4 AND state=$5 AND content_hash=$6 AND consumed_at IS NULL ORDER BY approval_id LIMIT 1 FOR UPDATE"),
            &[&file.manifest.namespace, &file.manifest.module, &(file.manifest.version as i64), &previous_attempt, &state, &file.manifest.content_hash],
        ).await?.context("migration is blocked; a new exact-artifact retry approval is required")?;
        let approval_id: i64 = approval.get(0);
        tx.execute(
            &format!("UPDATE {PLATFORM_SCHEMA}.migration_retry_approvals SET consumed_at=clock_timestamp() WHERE approval_id=$1 AND consumed_at IS NULL"),
            &[&approval_id],
        ).await?;
        previous_attempt
            .checked_add(1)
            .context("migration attempt overflow")?
    } else {
        1
    };
    tx.execute(
        &format!("INSERT INTO {PLATFORM_SCHEMA}.migration_attempts(namespace,module,migration_version,attempt,filename,content_hash,byte_length,bundle_digest,deployment_digest,state) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'started')"),
        &[&file.manifest.namespace, &file.manifest.module, &(file.manifest.version as i64), &attempt, &file.manifest.filename, &file.manifest.content_hash, &(file.manifest.byte_length as i64), &bundle.manifest.bundle_digest, &bundle.manifest.deployment_digest],
    ).await?;
    tx.commit().await?;
    Ok(Some(attempt))
}

struct Executor {
    role: String,
    password: String,
    backend_pid: Option<i32>,
}

async fn create_executor(platform: &Client, namespace: &DerivedNamespace) -> Result<Executor> {
    let role = format!(
        "{}{}",
        migration_executor_prefix(&namespace.namespace),
        &uuid::Uuid::new_v4().simple().to_string()[..8]
    );
    let password = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let role_q = quote_identifier(&role);
    let owner_q = quote_identifier(&namespace.owner);
    let password_q: String = platform
        .query_one("SELECT quote_literal($1)", &[&password])
        .await?
        .get(0);
    let database_q = quote_identifier(&namespace.database);
    let executor_auth_q = quote_identifier(MIGRATION_EXECUTOR_AUTH_MARKER_ROLE);
    platform.batch_execute(&format!(
        "CREATE ROLE {role_q} LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 1 PASSWORD {password_q}; \
         GRANT CONNECT ON DATABASE {database_q} TO {role_q}; \
         GRANT {executor_auth_q} TO {role_q} WITH INHERIT FALSE, SET FALSE, ADMIN FALSE; \
         GRANT {owner_q} TO {role_q} WITH INHERIT FALSE, SET TRUE, ADMIN FALSE"
    )).await?;
    Ok(Executor {
        role,
        password,
        backend_pid: None,
    })
}

struct ExecutionPolicy<'a> {
    provisioning: &'a PostgresProvisioningManifest,
    namespace: &'a DerivedNamespace,
    deadline: Duration,
    grace: Duration,
}

async fn execute_file(
    admin_url: &str,
    platform: &Client,
    policy: ExecutionPolicy<'_>,
    executor: &mut Executor,
    file: &CapturedMigrationFile,
) -> Result<AttemptOutcome> {
    let ExecutionPolicy {
        provisioning,
        namespace,
        deadline,
        grace,
    } = policy;
    let mut config = Config::from_str(admin_url)?;
    config.dbname(&namespace.database);
    config.user(&executor.role);
    config.password(&executor.password);
    let (mut client, driver) = config
        .connect(NoTls)
        .await
        .context("opening disposable migration executor")?;
    let driver_task = tokio::spawn(driver);
    let pid: i32 = client
        .query_one("SELECT pg_backend_pid()", &[])
        .await?
        .get(0);
    executor.backend_pid = Some(pid);
    let owner = quote_identifier(&namespace.owner);
    let schema = module_schema(&file.manifest.namespace, &file.manifest.module);
    let search_path = format!("{}, pg_catalog", quote_identifier(&schema));
    let sql = std::str::from_utf8(&file.bytes)?;
    let allowed_schemas = namespace.schemas.clone();
    let extensions = provisioning.extension_allowlist.clone();

    let executor_role = executor.role.clone();
    let run = async {
        let tx = client.transaction().await?;
        if let Err(error) = tx.batch_execute(&format!("SET LOCAL ROLE {owner}")).await {
            let code = redacted_postgres_error(&error);
            tx.rollback()
                .await
                .context("rolling back rejected SET ROLE")?;
            return Ok::<AttemptOutcome, anyhow::Error>(AttemptOutcome::Failed(code));
        }
        if let Err(error) = tx
            .execute(
                "SELECT pg_catalog.set_config('search_path',$1,true)",
                &[&search_path],
            )
            .await
        {
            let code = redacted_postgres_error(&error);
            tx.rollback()
                .await
                .context("rolling back rejected search path")?;
            return Ok(AttemptOutcome::Failed(code));
        }
        if let Err(error) = tx.batch_execute(sql).await {
            let code = redacted_postgres_error(&error);
            tx.rollback()
                .await
                .context("rolling back failed tenant SQL")?;
            return Ok(AttemptOutcome::Failed(code));
        }
        if let Err(error) = tx
            .batch_execute(&format!(
                "SET LOCAL ROLE {owner}; SELECT pg_catalog.set_config('search_path','pg_catalog',true)"
            ))
            .await
        {
            let code = redacted_postgres_error(&error);
            tx.rollback()
                .await
                .context("rolling back failed guard-context reset")?;
            return Ok(AttemptOutcome::Failed(code));
        }
        if let Err(error) = publication_guard(
            &tx,
            namespace,
            &executor_role,
            &allowed_schemas,
            &extensions,
        )
        .await
        {
            let code = redacted_error_code(&error);
            tx.rollback()
                .await
                .context("rolling back publication guard rejection")?;
            return Ok(AttemptOutcome::Failed(code));
        }
        match tx.commit().await {
            Ok(()) => Ok(AttemptOutcome::Succeeded),
            Err(_) => Ok(AttemptOutcome::Ambiguous),
        }
    };

    match tokio::time::timeout(deadline, run).await {
        Ok(result) => {
            drop(client);
            stop_connection_driver(driver_task, grace).await;
            result
        }
        Err(_) => {
            let cancellation = platform
                .execute("SELECT pg_cancel_backend($1)", &[&pid])
                .await;
            drop(client);
            stop_connection_driver(driver_task, grace).await;
            cancellation?;
            if wait_backend_absent(platform, pid, grace).await? {
                Ok(AttemptOutcome::Failed("deadline_cancelled".into()))
            } else {
                platform
                    .execute("SELECT pg_terminate_backend($1)", &[&pid])
                    .await?;
                if wait_backend_absent(platform, pid, grace).await? {
                    Ok(AttemptOutcome::Failed("deadline_terminated".into()))
                } else {
                    Ok(AttemptOutcome::Ambiguous)
                }
            }
        }
    }
}

async fn publication_guard(
    tx: &tokio_postgres::Transaction<'_>,
    namespace: &DerivedNamespace,
    executor: &str,
    allowed_schemas: &[String],
    extension_allowlist: &[String],
) -> Result<()> {
    let unsafe_found: bool = tx.query_one(
        "SELECT
          EXISTS (SELECT 1 FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_roles r ON r.oid=p.proowner JOIN pg_catalog.pg_language l ON l.oid=p.prolang WHERE r.rolname=$1 AND (p.prosecdef OR NOT ('search_path=pg_catalog'=ANY(COALESCE(p.proconfig,ARRAY[]::text[]))) OR l.lanname NOT IN ('sql','plpgsql')))
          OR EXISTS (SELECT 1 FROM pg_catalog.pg_language l JOIN pg_catalog.pg_roles r ON r.oid=l.lanowner WHERE r.rolname=$1 AND l.lanname NOT IN ('sql','plpgsql'))
          OR EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_roles r ON r.oid=c.relowner WHERE r.rolname=$1 AND (c.relkind='f' OR c.reltablespace<>0))
          OR EXISTS (SELECT 1 FROM pg_catalog.pg_largeobject_metadata l JOIN pg_catalog.pg_roles r ON r.oid=l.lomowner WHERE r.rolname=$1)
          OR EXISTS (SELECT 1 FROM pg_catalog.pg_extension e JOIN pg_catalog.pg_roles r ON r.oid=e.extowner WHERE r.rolname=$1 AND NOT (e.extname=ANY($5)))
          OR EXISTS (SELECT 1 FROM pg_catalog.pg_foreign_data_wrapper f JOIN pg_catalog.pg_roles r ON r.oid=f.fdwowner WHERE r.rolname=$1)
          OR EXISTS (SELECT 1 FROM pg_catalog.pg_foreign_server f JOIN pg_catalog.pg_roles r ON r.oid=f.srvowner WHERE r.rolname=$1)
          OR EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles member ON member.oid=m.member JOIN pg_catalog.pg_roles granted ON granted.oid=m.roleid WHERE (member.rolname=$1 OR granted.rolname=$1) AND member.rolname<>$4)
          OR pg_catalog.has_schema_privilege($1, 'wr__platform', 'USAGE')
          OR pg_catalog.has_schema_privilege($2, 'wr__platform', 'USAGE')
          OR EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n WHERE n.nspname<>'wr__platform' AND n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema' AND n.nspname<>'public' AND n.nspname<>ALL($3) AND pg_catalog.has_schema_privilege($2,n.oid,'USAGE'))
          OR EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_roles r ON r.oid=c.relowner WHERE r.rolname=$1 AND c.relacl IS NOT NULL AND pg_catalog.has_table_privilege('public',c.oid,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'))",
        &[&namespace.owner, &namespace.runtime_group, &allowed_schemas, &executor, &extension_allowlist],
    ).await?.get(0);
    ensure!(
        !unsafe_found,
        "post-migration publication safety guard rejected tenant DDL"
    );
    Ok(())
}

async fn terminalize(
    platform: &Client,
    file: &CapturedMigrationFile,
    attempt: i64,
    state: &str,
    failure_code: Option<&str>,
) -> Result<()> {
    platform.execute(
        &format!("UPDATE {PLATFORM_SCHEMA}.migration_attempts SET state=$5,finished_at=clock_timestamp(),failure_code=$6 WHERE namespace=$1 AND module=$2 AND migration_version=$3 AND attempt=$4 AND state='started'"),
        &[&file.manifest.namespace, &file.manifest.module, &(file.manifest.version as i64), &attempt, &state, &failure_code],
    ).await?;
    Ok(())
}

async fn cleanup_orphan_executors(platform: &Client, namespace: &DerivedNamespace) -> Result<()> {
    let prefix = migration_executor_prefix(&namespace.namespace);
    let pattern = format!("{prefix}%");
    let roles = platform
        .query(
            "SELECT rolname FROM pg_catalog.pg_roles WHERE rolname LIKE $1",
            &[&pattern],
        )
        .await?;
    for row in roles {
        let role: String = row.get(0);
        let pids = platform
            .query(
                "SELECT pid FROM pg_catalog.pg_stat_activity WHERE usename=$1",
                &[&role],
            )
            .await?;
        for row in pids {
            let pid: i32 = row.get(0);
            platform
                .execute("SELECT pg_terminate_backend($1)", &[&pid])
                .await?;
            ensure!(
                wait_backend_absent(platform, pid, Duration::from_secs(5)).await?,
                "could not terminate orphan migration executor"
            );
        }
        drop_executor_role(platform, namespace, &role).await?;
    }
    Ok(())
}

async fn cleanup_executor(
    platform: &Client,
    namespace: &DerivedNamespace,
    role: &str,
    known_pid: Option<i32>,
    grace: Duration,
) -> Result<()> {
    if let Some(pid) = known_pid {
        if !wait_backend_absent(platform, pid, grace).await? {
            platform
                .execute("SELECT pg_terminate_backend($1)", &[&pid])
                .await?;
            ensure!(
                wait_backend_absent(platform, pid, grace).await?,
                "migration executor backend survived termination"
            );
        }
    }
    let rows = platform
        .query(
            "SELECT pid FROM pg_catalog.pg_stat_activity WHERE usename=$1",
            &[&role],
        )
        .await?;
    for row in rows {
        let pid: i32 = row.get(0);
        platform
            .execute("SELECT pg_terminate_backend($1)", &[&pid])
            .await?;
        ensure!(
            wait_backend_absent(platform, pid, grace).await?,
            "migration executor backend survived cleanup"
        );
    }
    drop_executor_role(platform, namespace, role).await
}

async fn drop_executor_role(
    platform: &Client,
    namespace: &DerivedNamespace,
    role: &str,
) -> Result<()> {
    let role = quote_identifier(role);
    let owner = quote_identifier(&namespace.owner);
    let executor_auth = quote_identifier(MIGRATION_EXECUTOR_AUTH_MARKER_ROLE);
    let database = quote_identifier(&namespace.database);
    platform.batch_execute(&format!(
        "REVOKE {owner} FROM {role}; REVOKE {executor_auth} FROM {role}; REVOKE CONNECT ON DATABASE {database} FROM {role}; DROP ROLE {role}"
    )).await?;
    Ok(())
}

async fn stop_connection_driver(
    mut driver: tokio::task::JoinHandle<std::result::Result<(), tokio_postgres::Error>>,
    grace: Duration,
) {
    if tokio::time::timeout(grace, &mut driver).await.is_err() {
        driver.abort();
        let _ = driver.await;
    }
}

async fn wait_backend_absent(platform: &Client, pid: i32, grace: Duration) -> Result<bool> {
    let deadline = tokio::time::Instant::now() + grace;
    loop {
        let present: bool = platform
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid=$1)",
                &[&pid],
            )
            .await?
            .get(0);
        if !present {
            return Ok(true);
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn connect_database(
    admin_url: &str,
    database: &str,
) -> Result<(
    Client,
    impl std::future::Future<Output = std::result::Result<(), tokio_postgres::Error>>,
)> {
    let mut config = Config::from_str(admin_url)?;
    config.dbname(database);
    Ok(config.connect(NoTls).await?)
}

fn namespace_lock_key(namespace: &str) -> i64 {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest([MIGRATION_LOCK_DOMAIN, namespace.as_bytes()].concat());
    i64::from_be_bytes(digest[..8].try_into().expect("eight bytes"))
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn redacted_postgres_error(error: &tokio_postgres::Error) -> String {
    let mut code = error.code().map_or_else(
        || "migration_executor_error".to_string(),
        |code| format!("sqlstate:{}", code.code()),
    );
    code.truncate(FAILURE_CODE_LIMIT);
    code
}

fn redacted_error_code(error: &anyhow::Error) -> String {
    let mut code = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<tokio_postgres::Error>())
        .and_then(tokio_postgres::Error::code)
        .map_or_else(
            || "migration_executor_error".to_string(),
            |code| format!("sqlstate:{}", code.code()),
        );
    code.truncate(FAILURE_CODE_LIMIT);
    code
}
