use std::collections::BTreeSet;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use anyhow::{ensure, Context, Result};
use rustls::pki_types::CertificateDer;
use serde::Deserialize;
use wr_cli::cmd::config::{EngineConfig, NamespaceDatabaseExpectation, TenantDatabaseConfig};
use wr_common::migration_bundle::{MigrationBundleManifest, MigrationFileManifest};
use wr_common::postgres::{PostgresProvisioningManifest, SUPPORTED_POSTGRES_MAJOR};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadyV3 {
    schema_version: u32,
    owner_worktree: String,
    git_common_dir: String,
    git_dir: String,
    worktree_slot: u16,
    source_digest: String,
    fixture_artifact_digest: String,
    compose_project: String,
    postgres_image: String,
    postgres_image_id: String,
    daemon_architecture: String,
    daemon_platform: String,
    rust_target: String,
    cargo_version: String,
    rustc_version: String,
    cargo_zigbuild_version: String,
    zig_version: String,
    wr_cli_binary_sha256: String,
    minimal_context_sha256: String,
    provisioner_image_tag: String,
    provisioner_image_id: String,
    base_postgres_image_id: String,
    image_smoke_passed: bool,
    provision_generation: u64,
    provisioning_manifest_digest: String,
    migration_bundle_digest: String,
    successful_migrations: Vec<MigrationFileManifest>,
    postgres_major: u16,
    postgres_ca_sha256: String,
    postgres_client_leaf_fingerprint: String,
    server_name: String,
    host_addr: String,
    port: u16,
    s3_port: u16,
    connect_timeout_secs: u64,
}

fn first_certificate(path: &Path) -> Result<CertificateDer<'static>> {
    let mut reader = BufReader::new(File::open(path)?);
    let certificate = rustls_pemfile::certs(&mut reader)
        .next()
        .transpose()?
        .context("certificate file is empty")?;
    Ok(certificate)
}

fn main() -> Result<()> {
    let mut values = std::env::args().skip(1);
    let mut fixture = None;
    let mut configs = Vec::new();
    let mut verify_only = false;
    while let Some(flag) = values.next() {
        match flag.as_str() {
            "--verify-only" if !verify_only => verify_only = true,
            "--fixture" if fixture.is_none() => {
                fixture = Some(PathBuf::from(
                    values.next().context("missing value for --fixture")?,
                ));
            }
            "--config" => configs.push(PathBuf::from(
                values.next().context("missing value for --config")?,
            )),
            _ => anyhow::bail!("unexpected or duplicate argument {flag}"),
        }
    }
    let fixture = fixture.context("missing --fixture")?;
    ensure!(
        verify_only || !configs.is_empty(),
        "at least one --config is required"
    );
    let ready_path = fixture.join("ready.json");
    let ready: ReadyV3 =
        serde_json::from_slice(&std::fs::read(&ready_path).with_context(|| {
            format!(
                "development PostgreSQL fixture is not ready at {}; run `just dev-up` on the host",
                ready_path.display()
            )
        })?)?;
    ensure!(
        ready.schema_version == 3,
        "unsupported development fixture marker"
    );
    ensure!(
        ready.server_name == "postgres.internal"
            && ready.host_addr == "127.0.0.1"
            && ready.port > 0
            && ready.s3_port > 0
            && ready.port != ready.s3_port
            && ready.connect_timeout_secs == 10,
        "development fixture endpoint binding is invalid"
    );
    ensure!(ready.postgres_major == SUPPORTED_POSTGRES_MAJOR);
    ensure!(
        ready
            .compose_project
            .strip_prefix("wruntime-dev-")
            .is_some_and(
                |value| value.len() == 12 && value.chars().all(|ch| ch.is_ascii_hexdigit())
            )
            && ready.worktree_slot < 128
            && ready.postgres_image == "postgres:18-alpine"
            && ready.postgres_image_id.starts_with("sha256:")
            && ready.source_digest.starts_with("sha256:")
            && ready.fixture_artifact_digest.starts_with("sha256:")
            && ready.postgres_image_id == ready.base_postgres_image_id
            && ready.wr_cli_binary_sha256.starts_with("sha256:")
            && ready.minimal_context_sha256.starts_with("sha256:")
            && ready.provisioner_image_id.starts_with("sha256:")
            && ready.provisioner_image_tag
                == format!(
                    "wruntime-dev-postgres-provisioner:{}",
                    ready.minimal_context_sha256.trim_start_matches("sha256:")
                )
            && ready.image_smoke_passed
            && matches!(
                (
                    ready.daemon_architecture.as_str(),
                    ready.daemon_platform.as_str(),
                    ready.rust_target.as_str()
                ),
                ("amd64", "linux/amd64", "x86_64-unknown-linux-musl")
                    | ("arm64", "linux/arm64", "aarch64-unknown-linux-musl")
            )
            && !ready.cargo_version.is_empty()
            && !ready.rustc_version.is_empty()
            && !ready.cargo_zigbuild_version.is_empty()
            && !ready.zig_version.is_empty()
            && !ready.owner_worktree.is_empty()
            && !ready.git_common_dir.is_empty()
            && !ready.git_dir.is_empty(),
        "development fixture ownership binding is invalid"
    );

    let provisioning = PostgresProvisioningManifest::parse_toml(&std::fs::read_to_string(
        fixture.join("provisioning.toml"),
    )?)?;
    let migration = MigrationBundleManifest::parse_toml(&std::fs::read_to_string(
        fixture.join("migration-bundle.toml"),
    )?)?;
    let captured = migration.capture(&fixture.join("migrations"))?;
    ensure!(
        ready.provision_generation == provisioning.generation
            && ready.provisioning_manifest_digest == provisioning.normalized_digest()
            && ready.migration_bundle_digest == migration.bundle_digest
            && ready.successful_migrations == migration.files,
        "development fixture marker is stale; rerun host `just dev-up`"
    );
    ensure!(
        captured.manifest == migration,
        "development migration bytes are stale"
    );
    let state_root = fixture
        .parent()
        .context("fixture has no worktree-state parent")?;
    ensure!(
        state_root
            .parent()
            .is_some_and(|path| path == Path::new(&ready.git_dir)),
        "development fixture Git-directory binding is stale"
    );
    let root = state_root.join("pki/root/ca.crt");
    let client = state_root.join("pki/node-a/leaf.pem");
    ensure!(
        root.is_file() && client.is_file(),
        "development PostgreSQL PKI is incomplete"
    );
    ensure!(
        ready.postgres_ca_sha256
            == wr_common::tls::certificate_fingerprint_sha256(first_certificate(&root)?.as_ref())
            && ready.postgres_client_leaf_fingerprint
                == wr_common::tls::certificate_fingerprint_sha256(
                    first_certificate(&client)?.as_ref()
                ),
        "development fixture certificate binding is stale"
    );
    if verify_only {
        return Ok(());
    }

    let mut engines = configs
        .iter()
        .map(|path| EngineConfig::from_file(path.to_str().context("config path is not UTF-8")?))
        .collect::<Result<Vec<_>>>()?;
    let declared = provisioning
        .namespaces
        .iter()
        .map(|value| value.namespace.as_str())
        .collect::<BTreeSet<_>>();
    for engine in &mut engines {
        let required = engine
            .modules
            .iter()
            .filter(|module| module.database)
            .map(|module| module.namespace.clone())
            .collect::<BTreeSet<_>>();
        if required.is_empty() {
            continue;
        }
        ensure!(
            required
                .iter()
                .all(|namespace| declared.contains(namespace.as_str())),
            "engine requires namespace absent from development fixture"
        );
        engine
            .database
            .as_mut()
            .context("database-enabled config lacks [database]")?
            .tenant = Some(TenantDatabaseConfig {
            server_name: "postgres.internal".into(),
            host_addr: Some(ready.host_addr.clone()),
            port: ready.port,
            trust_root_path: state_root.join("pki/root/ca.crt").display().to_string(),
            client_cert_path: state_root.join("pki/node-a/leaf.pem").display().to_string(),
            client_key_path: state_root.join("pki/node-a/key.pem").display().to_string(),
            connect_timeout_secs: 10,
            expected_namespaces: required
                .into_iter()
                .map(|namespace| NamespaceDatabaseExpectation {
                    namespace,
                    generation: provisioning.generation,
                    deployment_digest: migration.deployment_digest.clone(),
                    bundle_digest: migration.bundle_digest.clone(),
                    migrations: vec![],
                })
                .collect(),
        });
    }
    EngineConfig::populate_receipt_tenant_expectations(
        &mut engines,
        &migration.deployment_digest,
        &migration.bundle_digest,
        Some(provisioning.generation),
        &migration.files,
    )?;
    for (path, engine) in configs.iter().zip(engines) {
        std::fs::write(path, engine.to_toml()?)?;
    }
    Ok(())
}
