use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use anyhow::{ensure, Context, Result};
use rustls::pki_types::CertificateDer;
use wr_common::migration_bundle::MigrationBundleManifest;
use wr_common::naming::{
    module_schema, namespace_database, namespace_owner, namespace_readiness_verifier,
    namespace_runtime_login,
};
use wr_common::postgres::PostgresProvisioningManifest;
use wr_common::wruntime::NamespaceAccessDescriptor;
use wr_engine::config::{ExpectedMigration, NamespaceDatabaseExpectation, TenantDatabaseConfig};
use wr_engine::tenant_db::{verify_readiness, VerifiedNamespaceAccess};

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("wr-tests has repository parent")
        .to_path_buf()
}

fn worktree_state_root(root: &Path) -> Result<PathBuf> {
    let output = Command::new("git")
        .args([
            "-C",
            root.to_str().context("repository path is not UTF-8")?,
            "rev-parse",
            "--path-format=absolute",
            "--absolute-git-dir",
        ])
        .output()?;
    ensure!(
        output.status.success(),
        "cannot derive worktree Git directory"
    );
    Ok(std::fs::canonicalize(String::from_utf8(output.stdout)?.trim())?.join("wruntime-dev-state"))
}

fn first_certificate(path: &Path) -> Result<CertificateDer<'static>> {
    let mut reader = BufReader::new(File::open(path)?);
    let certificate = rustls_pemfile::certs(&mut reader)
        .next()
        .transpose()?
        .context("certificate file is empty")?;
    Ok(certificate)
}

fn run_psql(connection: &str, sql: &str) -> Result<Output> {
    Command::new("psql")
        .args([
            "-X",
            "-A",
            "-t",
            "-v",
            "ON_ERROR_STOP=1",
            connection,
            "-c",
            sql,
        ])
        .output()
        .context("executing worktree development fixture probe")
}

fn tenant_connection(
    state_root: &Path,
    host_addr: &str,
    port: u16,
    user: &str,
    database: &str,
) -> String {
    format!(
        "host=postgres.internal hostaddr={host_addr} port={port} user={user} dbname={database} sslmode=verify-full sslrootcert={} sslcert={} sslkey={}",
        state_root.join("pki/root/ca.crt").display(),
        state_root.join("pki/node-a/leaf.pem").display(),
        state_root.join("pki/node-a/key.pem").display()
    )
}

#[test]
fn worktree_native_fixture_enforces_behavior_and_engine_readiness_without_admin_inputs(
) -> Result<()> {
    let root = repository_root();
    let state_root = worktree_state_root(&root)?;
    let fixture = state_root.join("fixture");
    let ready = fixture.join("ready.json");
    ensure!(
        ready.is_file(),
        "development PostgreSQL fixture is not ready; run host `just dev-up`"
    );
    let marker: serde_json::Value = serde_json::from_slice(&std::fs::read(&ready)?)?;
    ensure!(marker["schema_version"] == 3);
    ensure!(marker["compose_project"]
        .as_str()
        .is_some_and(|value| value.starts_with("wruntime-dev-")));
    ensure!(marker["server_name"] == "postgres.internal");
    let host_addr = marker["host_addr"]
        .as_str()
        .context("fixture host address is absent")?;
    let port = u16::try_from(marker["port"].as_u64().context("fixture port is absent")?)?;
    ensure!(host_addr == "127.0.0.1" && port > 0);
    let provisioning = PostgresProvisioningManifest::parse_toml(&std::fs::read_to_string(
        fixture.join("provisioning.toml"),
    )?)?;
    let migration = MigrationBundleManifest::parse_toml(&std::fs::read_to_string(
        fixture.join("migration-bundle.toml"),
    )?)?;
    migration.capture(&fixture.join("migrations"))?;
    ensure!(marker["provision_generation"] == provisioning.generation);
    ensure!(marker["provisioning_manifest_digest"] == provisioning.normalized_digest());
    ensure!(marker["migration_bundle_digest"] == migration.bundle_digest);
    ensure!(marker["successful_migrations"] == serde_json::to_value(&migration.files)?);
    ensure!(
        marker["postgres_ca_sha256"]
            == wr_common::tls::certificate_fingerprint_sha256(
                first_certificate(&state_root.join("pki/root/ca.crt"))?.as_ref(),
            )
    );
    ensure!(
        marker["postgres_client_leaf_fingerprint"]
            == wr_common::tls::certificate_fingerprint_sha256(
                first_certificate(&state_root.join("pki/node-a/leaf.pem"))?.as_ref(),
            )
    );
    ensure!(provisioning.namespaces.len() >= 3);
    let stock = provisioning
        .namespaces
        .iter()
        .find(|value| value.namespace == "stockmarket")
        .context("stockmarket namespace missing")?;
    ensure!(stock.modules.len() >= 2);
    ensure!(
        provisioning.platform_databases.manager != provisioning.platform_databases.job_queues[0]
    );

    let runtime = namespace_runtime_login("node-a", "stockmarket");
    let database = namespace_database("stockmarket");
    let connection = tenant_connection(&state_root, host_addr, port, &runtime, &database);
    let exchange = module_schema("stockmarket", "exchange");
    let ledger = module_schema("stockmarket", "ledger");
    let output = run_psql(
        &connection,
        &format!("SELECT current_user=$${runtime}$$, has_schema_privilege(current_user,$${exchange}$$,'USAGE'), has_schema_privilege(current_user,$${ledger}$$,'USAGE'), (SELECT count(*) >= 0 FROM {exchange}.orders), (SELECT count(*) >= 0 FROM {ledger}.trades)"),
    )?;
    ensure!(
        output.status.success(),
        "mapped native login failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    ensure!(String::from_utf8(output.stdout)?.trim() == "t|t|t|t|t");

    let ddl = run_psql(
        &connection,
        &format!("CREATE TABLE {exchange}.runtime_forbidden(id int)"),
    )?;
    ensure!(
        !ddl.status.success(),
        "runtime role unexpectedly performed DDL"
    );
    let escalate = run_psql(
        &connection,
        &format!("SET ROLE {}", namespace_owner("stockmarket")),
    )?;
    ensure!(
        !escalate.status.success(),
        "runtime role assumed namespace owner"
    );
    let cross = run_psql(
        &tenant_connection(
            &state_root,
            host_addr,
            port,
            &runtime,
            &namespace_database("ecommerce"),
        ),
        "SELECT 1",
    )?;
    ensure!(
        !cross.status.success(),
        "runtime role connected to another namespace database"
    );
    let platform = run_psql(
        &tenant_connection(
            &state_root,
            host_addr,
            port,
            &runtime,
            &provisioning.platform_databases.manager,
        ),
        "SELECT 1",
    )?;
    ensure!(
        !platform.status.success(),
        "runtime role connected to manager platform database"
    );
    let fresh = run_psql(
        &connection,
        "SELECT current_user, current_setting('search_path')",
    )?;
    ensure!(fresh.status.success());
    ensure!(String::from_utf8(fresh.stdout)?.contains(&runtime));

    let expectation = NamespaceDatabaseExpectation {
        namespace: "stockmarket".into(),
        generation: provisioning.generation,
        deployment_digest: migration.deployment_digest.clone(),
        bundle_digest: migration.bundle_digest.clone(),
        migrations: migration
            .files
            .iter()
            .filter(|file| file.namespace == "stockmarket")
            .map(|file| ExpectedMigration {
                module: file.module.clone(),
                version: file.version,
                filename: file.filename.clone(),
                content_hash: file.content_hash.clone(),
                byte_length: file.byte_length,
            })
            .collect(),
    };
    let tenant = TenantDatabaseConfig {
        server_name: "postgres.internal".into(),
        host_addr: Some(host_addr.into()),
        port,
        trust_root_path: state_root.join("pki/root/ca.crt").display().to_string(),
        client_cert_path: state_root.join("pki/node-a/leaf.pem").display().to_string(),
        client_key_path: state_root.join("pki/node-a/key.pem").display().to_string(),
        connect_timeout_secs: 10,
        expected_namespaces: vec![expectation.clone()],
    };
    let access = VerifiedNamespaceAccess {
        descriptor: NamespaceAccessDescriptor {
            namespace: "stockmarket".into(),
            database,
            runtime_role: runtime,
            readiness_role: namespace_readiness_verifier("node-a", "stockmarket"),
        },
        expectation,
    };
    tokio::runtime::Runtime::new()?.block_on(verify_readiness(&tenant, &[access], None))?;
    Ok(())
}
