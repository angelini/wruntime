use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{ensure, Context, Result};
use rustls::pki_types::CertificateDer;
use wr_cli::cmd::config::{EngineConfig, NamespaceDatabaseExpectation, TenantDatabaseConfig};
use wr_common::migration_bundle::{MigrationBundleManifest, MigrationLimits};
use wr_common::postgres::{
    PlatformDatabases, PostgresProvisioningLimits, PostgresProvisioningManifest,
    PostgresProvisioningNamespace, PostgresProvisioningNode, POSTGRES_PROVISIONING_FORMAT_VERSION,
    SUPPORTED_POSTGRES_MAJOR,
};

const PROVISIONER_NODE_CERTIFICATE_PATH: &str = "/wr-input/node-a-public.pem";

struct Args {
    output: PathBuf,
    contract_out: Option<PathBuf>,
    generation: Option<u64>,
    client_cert: PathBuf,
    client_key: PathBuf,
    server_ca: PathBuf,
    configs: Vec<PathBuf>,
}

fn args() -> Result<Args> {
    let mut values = std::env::args().skip(1);
    let mut map = BTreeMap::<String, Vec<String>>::new();
    while let Some(flag) = values.next() {
        let value = values
            .next()
            .with_context(|| format!("missing value for {flag}"))?;
        map.entry(flag).or_default().push(value);
    }
    let one = |name: &str| -> Result<PathBuf> {
        let values = map.get(name).with_context(|| format!("missing {name}"))?;
        ensure!(values.len() == 1, "{name} must be supplied once");
        Ok(values[0].clone().into())
    };
    Ok(Args {
        output: one("--output")?,
        contract_out: map
            .get("--contract-out")
            .map(|values| {
                ensure!(values.len() == 1, "--contract-out must be supplied once");
                Ok(PathBuf::from(&values[0]))
            })
            .transpose()?,
        generation: map
            .get("--generation")
            .map(|values| {
                ensure!(values.len() == 1, "--generation must be supplied once");
                values[0].parse::<u64>().context("invalid --generation")
            })
            .transpose()?,
        client_cert: one("--client-cert")?,
        client_key: one("--client-key")?,
        server_ca: one("--server-ca")?,
        configs: map
            .get("--config")
            .context("at least one --config is required")?
            .iter()
            .map(PathBuf::from)
            .collect(),
    })
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
    let args = args()?;
    ensure!(
        !args.output.exists(),
        "native staging output already exists"
    );
    std::fs::create_dir_all(&args.output)?;
    let mut configs = args
        .configs
        .iter()
        .map(|path| EngineConfig::from_file(path.to_str().context("config path is not UTF-8")?))
        .collect::<Result<Vec<_>>>()?;
    let namespaces = configs
        .iter()
        .flat_map(|config| &config.modules)
        .filter(|module| module.database)
        .fold(
            BTreeMap::<String, BTreeSet<String>>::new(),
            |mut all, module| {
                all.entry(module.namespace.clone())
                    .or_default()
                    .insert(module.name.clone());
                all
            },
        );
    ensure!(
        !namespaces.is_empty(),
        "staging requires database-enabled modules"
    );
    let generation = args
        .generation
        .unwrap_or(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64);
    let tenant_server_name = "postgres.internal".to_string();
    let tenant_host_addr = Some("127.0.0.1".to_string());
    let tenant_port = 5433;
    let deployment_digest = format!("sha256:{}", "0".repeat(64));
    let mut sources = BTreeMap::new();
    for module in configs
        .iter()
        .flat_map(|config| &config.modules)
        .filter(|module| module.database)
    {
        if let Some(path) = module.migrations_path.as_deref() {
            sources.insert(
                (module.namespace.clone(), module.name.clone()),
                std::fs::canonicalize(path)?,
            );
        }
    }
    let sources = sources
        .into_iter()
        .map(|((namespace, module), path)| (namespace, module, path))
        .collect::<Vec<_>>();
    let bundle = MigrationBundleManifest::capture_sources(
        deployment_digest.clone(),
        MigrationLimits {
            max_migrations_per_namespace: 1024,
            max_file_bytes: 2 * 1024 * 1024,
            max_startup_bytes: 64 * 1024 * 1024,
            file_deadline_ms: 30_000,
            cancellation_grace_ms: 5_000,
        },
        &sources,
    )?;
    let client = first_certificate(&args.client_cert)?;
    let ca = first_certificate(&args.server_ca)?;
    let manifest = PostgresProvisioningManifest {
        format_version: POSTGRES_PROVISIONING_FORMAT_VERSION,
        cluster_id: "default".into(),
        generation,
        postgres_major: SUPPORTED_POSTGRES_MAJOR,
        postgres_ca_sha256: wr_common::tls::certificate_fingerprint_sha256(ca.as_ref()),
        ident_map_name: "wruntime_nodes".into(),
        platform_databases: PlatformDatabases {
            manager: "wruntime_manager".into(),
            job_queues: vec!["wruntime_jobs".into()],
        },
        extension_allowlist: vec![],
        tenant_client_cidrs: vec!["127.0.0.1/32".into(), "172.16.0.0/12".into()],
        limits: PostgresProvisioningLimits {
            namespace_database_connections: 100,
            runtime_login_connections: 40,
            readiness_verifier_connections: 1,
            statement_timeout_ms: 30_000,
            lock_timeout_ms: 5_000,
            idle_in_transaction_timeout_ms: 60_000,
        },
        nodes: vec![PostgresProvisioningNode {
            node_id: "node-a".into(),
            certificate_pem_path: PROVISIONER_NODE_CERTIFICATE_PATH.into(),
            certificate_sha256: wr_common::tls::certificate_fingerprint_sha256(client.as_ref()),
            certificate_common_name: "urn:wruntime:default:postgres-client:node-a".into(),
        }],
        namespaces: namespaces
            .iter()
            .map(|(namespace, modules)| PostgresProvisioningNamespace {
                namespace: namespace.clone(),
                modules: modules.iter().cloned().collect(),
            })
            .collect(),
    };
    manifest.validate()?;
    if let Some(path) = &args.contract_out {
        std::fs::write(
            path,
            serde_json::to_vec_pretty(&serde_json::json!({
                "provision_generation": generation,
                "provisioning_manifest_digest": manifest.normalized_digest(),
                "migration_bundle_digest": bundle.manifest.bundle_digest.clone(),
            }))?,
        )?;
    }
    std::fs::write(
        args.output.join("provisioning.toml"),
        toml::to_string_pretty(&manifest)?,
    )?;
    std::fs::write(
        args.output.join("migration-bundle.toml"),
        toml::to_string_pretty(&bundle.manifest)?,
    )?;
    let migration_root = args.output.join("migrations");
    for file in &bundle.files {
        let path = migration_root
            .join(&file.manifest.namespace)
            .join(&file.manifest.module)
            .join(&file.manifest.filename);
        std::fs::create_dir_all(path.parent().expect("migration path parent"))?;
        std::fs::write(path, &file.bytes)?;
    }
    for config in &mut configs {
        let required = config
            .modules
            .iter()
            .filter(|module| module.database)
            .map(|module| module.namespace.clone())
            .collect::<BTreeSet<_>>();
        if required.is_empty() {
            continue;
        }
        let database = config
            .database
            .as_mut()
            .context("database-enabled config lacks [database]")?;
        database.tenant = Some(TenantDatabaseConfig {
            server_name: tenant_server_name.clone(),
            host_addr: tenant_host_addr.clone(),
            port: tenant_port,
            trust_root_path: args.server_ca.display().to_string(),
            client_cert_path: args.client_cert.display().to_string(),
            client_key_path: args.client_key.display().to_string(),
            connect_timeout_secs: 10,
            expected_namespaces: required
                .into_iter()
                .map(|namespace| NamespaceDatabaseExpectation {
                    namespace,
                    generation,
                    deployment_digest: deployment_digest.clone(),
                    bundle_digest: bundle.manifest.bundle_digest.clone(),
                    migrations: vec![],
                })
                .collect(),
        });
    }
    EngineConfig::populate_receipt_tenant_expectations(
        &mut configs,
        &deployment_digest,
        &bundle.manifest.bundle_digest,
        Some(generation),
        &bundle.manifest.files,
    )?;
    for (path, config) in args.configs.iter().zip(configs) {
        std::fs::write(path, config.to_toml()?)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::PROVISIONER_NODE_CERTIFICATE_PATH;

    #[test]
    fn provisioning_uses_public_combined_certificate_outside_private_node_directory() {
        assert_eq!(
            PROVISIONER_NODE_CERTIFICATE_PATH,
            "/wr-input/node-a-public.pem"
        );
        assert!(!PROVISIONER_NODE_CERTIFICATE_PATH.contains("/node-a/"));
        assert!(!PROVISIONER_NODE_CERTIFICATE_PATH.ends_with("/leaf.pem"));
    }
}
