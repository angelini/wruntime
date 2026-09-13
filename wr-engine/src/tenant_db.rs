use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::BufReader;
use std::net::IpAddr;
use std::time::Duration;

use anyhow::{ensure, Context, Result};
use deadpool_postgres::Pool;
use rustls::{ClientConfig, RootCertStore};
use tokio_postgres::{config::SslMode, error::SqlState};
use tokio_postgres_rustls::MakeRustlsConnect;
use wr_common::migration_bundle::MigrationBundle;
use wr_common::naming::{
    namespace_database, namespace_readiness_verifier, namespace_runtime_login,
};
use wr_common::wruntime::NamespaceAccessDescriptor;

use crate::config::{NamespaceDatabaseExpectation, TenantDatabaseConfig};

const PLATFORM_SCHEMA: &str = "wr__platform";

/// Descriptor plus node-local expected state. This contains no credential
/// material and is safe to retain for the life of the engine.
#[derive(Clone, Debug)]
pub struct VerifiedNamespaceAccess {
    pub descriptor: NamespaceAccessDescriptor,
    pub expectation: NamespaceDatabaseExpectation,
}

pub fn reconcile_descriptors(
    node_id: &str,
    tenant: &TenantDatabaseConfig,
    descriptors: &[NamespaceAccessDescriptor],
    required_namespaces: impl Iterator<Item = String>,
    platform_databases: &BTreeSet<String>,
) -> Result<Vec<VerifiedNamespaceAccess>> {
    let expected = tenant
        .expected_namespaces
        .iter()
        .map(|value| (value.namespace.as_str(), value))
        .collect::<BTreeMap<_, _>>();
    ensure!(
        expected.len() == tenant.expected_namespaces.len(),
        "duplicate local namespace database expectation"
    );
    let required = required_namespaces.collect::<BTreeSet<_>>();
    ensure!(
        expected.keys().copied().collect::<BTreeSet<_>>()
            == required.iter().map(String::as_str).collect(),
        "local namespace database expectations do not match configured namespaces"
    );

    let mut seen = BTreeSet::new();
    let mut verified = Vec::with_capacity(descriptors.len());
    for descriptor in descriptors {
        ensure!(
            seen.insert(descriptor.namespace.as_str()),
            "manager returned duplicate namespace access descriptor"
        );
        let expectation = expected
            .get(descriptor.namespace.as_str())
            .context("manager returned an extra namespace access descriptor")?;
        ensure!(
            descriptor.database == namespace_database(&descriptor.namespace)
                && descriptor.runtime_role
                    == namespace_runtime_login(node_id, &descriptor.namespace)
                && descriptor.readiness_role
                    == namespace_readiness_verifier(node_id, &descriptor.namespace),
            "manager namespace access descriptor identity mismatch"
        );
        ensure!(
            !platform_databases.contains(&descriptor.database),
            "namespace access descriptor targets a platform database"
        );
        verified.push(VerifiedNamespaceAccess {
            descriptor: descriptor.clone(),
            expectation: (*expectation).clone(),
        });
    }
    ensure!(
        seen.len() == required.len(),
        "manager omitted namespace access descriptor"
    );
    verified.sort_by(|left, right| left.descriptor.namespace.cmp(&right.descriptor.namespace));
    Ok(verified)
}

pub fn tls_connector(config: &TenantDatabaseConfig) -> Result<MakeRustlsConnect> {
    let mut roots = RootCertStore::empty();
    let mut root_reader = BufReader::new(
        File::open(&config.trust_root_path).context("opening PostgreSQL trust root")?,
    );
    let certificates = rustls_pemfile::certs(&mut root_reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("reading PostgreSQL trust root")?;
    ensure!(
        !certificates.is_empty(),
        "PostgreSQL trust root contains no certificates"
    );
    let (added, _) = roots.add_parsable_certificates(certificates);
    ensure!(
        added > 0,
        "PostgreSQL trust root contains no usable certificates"
    );

    let mut cert_reader = BufReader::new(
        File::open(&config.client_cert_path).context("opening PostgreSQL client certificate")?,
    );
    let client_chain = rustls_pemfile::certs(&mut cert_reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("reading PostgreSQL client certificate")?;
    ensure!(
        !client_chain.is_empty(),
        "PostgreSQL client certificate is empty"
    );
    let mut key_reader = BufReader::new(
        File::open(&config.client_key_path).context("opening PostgreSQL client private key")?,
    );
    let key = rustls_pemfile::private_key(&mut key_reader)
        .context("reading PostgreSQL client private key")?
        .context("PostgreSQL client private key is empty")?;
    let tls = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(client_chain, key)
        .context("invalid PostgreSQL client credential")?;
    Ok(MakeRustlsConnect::new(tls))
}

fn connection_config(
    tenant: &TenantDatabaseConfig,
    database: &str,
    role: &str,
    application_name: &str,
) -> Result<tokio_postgres::Config> {
    let mut config = tokio_postgres::Config::new();
    config
        .host(&tenant.server_name)
        .port(tenant.port)
        .dbname(database)
        .user(role)
        .ssl_mode(SslMode::Require)
        .connect_timeout(Duration::from_secs(tenant.connect_timeout_secs))
        .application_name(application_name);
    if let Some(address) = &tenant.host_addr {
        config.hostaddr(
            address
                .parse::<IpAddr>()
                .context("invalid PostgreSQL host_addr")?,
        );
    }
    Ok(config)
}

/// Verify every namespace through a direct, bounded connection and fully join
/// its connection driver before returning.
pub async fn verify_readiness(
    tenant: &TenantDatabaseConfig,
    accesses: &[VerifiedNamespaceAccess],
    bundle: Option<&MigrationBundle>,
) -> Result<()> {
    let connector = tls_connector(tenant)?;
    let tenant = tenant.clone();
    verify_readiness_with(accesses, bundle, move |access| {
        let connector = connector.clone();
        let tenant = tenant.clone();
        async move {
            let config = connection_config(
                &tenant,
                &access.descriptor.database,
                &access.descriptor.readiness_role,
                "wruntime-readiness",
            )?;
            let deadline =
                tokio::time::Instant::now() + Duration::from_secs(tenant.connect_timeout_secs);
            let (client, connection) = loop {
                match config.connect(connector.clone()).await {
                    Ok(connection) => break connection,
                    Err(error)
                        if error.code() == Some(&SqlState::TOO_MANY_CONNECTIONS)
                            && tokio::time::Instant::now() < deadline =>
                    {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Err(error) => {
                        return Err(error).context("connecting namespace readiness verifier")
                    }
                }
            };
            Ok((client, tokio::spawn(connection)))
        }
    })
    .await
}

/// Narrow connector seam used by trust-backed tests. Production configuration
/// always calls [`verify_readiness`] and therefore cannot select `NoTls`.
#[doc(hidden)]
pub async fn verify_readiness_with<F, Fut>(
    accesses: &[VerifiedNamespaceAccess],
    bundle: Option<&MigrationBundle>,
    mut connect: F,
) -> Result<()>
where
    F: FnMut(VerifiedNamespaceAccess) -> Fut,
    Fut: std::future::Future<
        Output = Result<(
            tokio_postgres::Client,
            tokio::task::JoinHandle<std::result::Result<(), tokio_postgres::Error>>,
        )>,
    >,
{
    for access in accesses {
        let (mut client, driver) = connect(access.clone()).await?;
        let result = verify_one(&mut client, access, bundle).await;
        drop(client);
        let disconnected = tokio::time::timeout(Duration::from_secs(5), driver)
            .await
            .context("namespace readiness connection did not disconnect")?
            .context("namespace readiness connection driver panicked")?;
        disconnected.context("namespace readiness connection driver failed")?;
        result?;
    }
    Ok(())
}

async fn verify_one(
    client: &mut tokio_postgres::Client,
    access: &VerifiedNamespaceAccess,
    bundle: Option<&MigrationBundle>,
) -> Result<()> {
    let transaction = client
        .build_transaction()
        .read_only(true)
        .start()
        .await
        .context("starting namespace readiness transaction")?;
    let row = transaction
        .query_opt(
            &format!("SELECT generation FROM {PLATFORM_SCHEMA}.namespace_state WHERE singleton"),
            &[],
        )
        .await?
        .context("namespace provisioning state is missing")?;
    let generation: i64 = row.get(0);
    ensure!(
        generation > 0 && generation as u64 == access.expectation.generation,
        "namespace provisioning generation mismatch"
    );

    let rows = transaction
        .query(
            &format!("SELECT DISTINCT ON(namespace,module,migration_version) namespace,module,migration_version,attempt,filename,content_hash,byte_length,bundle_digest,deployment_digest,state FROM {PLATFORM_SCHEMA}.migration_attempts ORDER BY namespace,module,migration_version,attempt DESC"),
            &[],
        )
        .await?;
    let expected_files = if access.expectation.migrations.is_empty() {
        bundle
            .map(|bundle| {
                bundle
                    .files
                    .iter()
                    .filter(|file| file.manifest.namespace == access.descriptor.namespace)
                    .map(|file| {
                        (
                            (file.manifest.module.clone(), file.manifest.version),
                            (
                                file.manifest.filename.clone(),
                                file.manifest.content_hash.clone(),
                                file.manifest.byte_length,
                            ),
                        )
                    })
                    .collect::<BTreeMap<_, _>>()
            })
            .unwrap_or_default()
    } else {
        access
            .expectation
            .migrations
            .iter()
            .map(|file| {
                (
                    (file.module.clone(), file.version),
                    (
                        file.filename.clone(),
                        file.content_hash.clone(),
                        file.byte_length,
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>()
    };
    ensure!(
        expected_files.len() == access.expectation.migrations.len()
            || access.expectation.migrations.is_empty(),
        "local migration expectation contains duplicate identities"
    );
    if access.expectation.migrations.is_empty() {
        if let Some(bundle) = bundle {
            ensure!(
                bundle.manifest.bundle_digest == access.expectation.bundle_digest,
                "local migration bundle digest mismatch"
            );
            ensure!(
                bundle.manifest.deployment_digest == access.expectation.deployment_digest,
                "local migration deployment digest mismatch"
            );
        }
    }
    let mut completed = BTreeSet::<(String, u64)>::new();
    for row in rows {
        let namespace: String = row.get(0);
        ensure!(
            namespace == access.descriptor.namespace,
            "migration ledger contains a foreign namespace"
        );
        let module: String = row.get(1);
        let version_i64: i64 = row.get(2);
        ensure!(version_i64 > 0, "migration ledger version is invalid");
        let version = version_i64 as u64;
        let expected = expected_files
            .get(&(module.clone(), version))
            .context("migration ledger contains an unexpected migration")?;
        let attempt: i64 = row.get(3);
        ensure!(attempt > 0, "migration ledger attempt is invalid");
        let filename: String = row.get(4);
        let content_hash: String = row.get(5);
        let byte_length: i64 = row.get(6);
        let state: String = row.get(9);
        ensure!(
            state == "succeeded",
            "migration ledger has failed or ambiguous state"
        );
        ensure!(
            filename == expected.0
                && content_hash == expected.1
                && byte_length >= 0
                && byte_length as u64 == expected.2,
            "migration ledger immutable identity mismatch"
        );
        completed.insert((module, version));
    }
    ensure!(
        completed.len() == expected_files.len(),
        "migration ledger is missing a migration"
    );
    transaction.commit().await?;
    Ok(())
}

pub fn build_runtime_pool(
    tenant: &TenantDatabaseConfig,
    access: &VerifiedNamespaceAccess,
    max_size: usize,
) -> Result<Pool> {
    let config = connection_config(
        tenant,
        &access.descriptor.database,
        &access.descriptor.runtime_role,
        "wruntime-guest",
    )?;
    wr_common::pool::build_guest_pool_with_connector(config, tls_connector(tenant)?, max_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tenant() -> TenantDatabaseConfig {
        TenantDatabaseConfig {
            server_name: "postgres.internal".into(),
            host_addr: None,
            port: 5432,
            trust_root_path: "/not/read".into(),
            client_cert_path: "/not/read".into(),
            client_key_path: "/not/read".into(),
            connect_timeout_secs: 1,
            expected_namespaces: vec![NamespaceDatabaseExpectation {
                namespace: "shop".into(),
                generation: 1,
                deployment_digest: format!("sha256:{}", "a".repeat(64)),
                bundle_digest: format!("sha256:{}", "b".repeat(64)),
                migrations: vec![],
            }],
        }
    }

    #[test]
    fn descriptor_reconciliation_is_exact_and_node_bound() {
        let descriptor = NamespaceAccessDescriptor {
            namespace: "shop".into(),
            database: namespace_database("shop"),
            runtime_role: namespace_runtime_login("node-a", "shop"),
            readiness_role: namespace_readiness_verifier("node-a", "shop"),
        };
        assert!(reconcile_descriptors(
            "node-a",
            &tenant(),
            std::slice::from_ref(&descriptor),
            ["shop".into()].into_iter(),
            &BTreeSet::new(),
        )
        .is_ok());
        assert!(reconcile_descriptors(
            "node-b",
            &tenant(),
            &[descriptor],
            ["shop".into()].into_iter(),
            &BTreeSet::new(),
        )
        .is_err());
    }
}
