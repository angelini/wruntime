use std::collections::{BTreeMap, BTreeSet};

use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::identity::{ClusterId, Namespace, NodeId};
use crate::naming::{
    module_schema, namespace_database, namespace_maintenance_role, namespace_owner,
    namespace_readiness_verifier, namespace_runtime_group, namespace_runtime_login,
};

pub const POSTGRES_PROVISIONING_FORMAT_VERSION: u32 = 1;
pub const SUPPORTED_POSTGRES_MAJOR: u16 = 18;
pub const MIGRATION_EXECUTOR_AUTH_MARKER_ROLE: &str = "wr__migration_executor_auth";
pub const TENANT_AUTH_MARKER_ROLE: &str = "wr__tenant_client_auth";
pub const PLATFORM_SCHEMA: &str = "wr__platform";
pub const IDENT_HEADER: &str = "# wruntime postgres tenant mappings v1";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresProvisioningManifest {
    pub format_version: u32,
    pub cluster_id: String,
    pub generation: u64,
    pub postgres_major: u16,
    pub postgres_ca_sha256: String,
    pub ident_map_name: String,
    pub platform_databases: PlatformDatabases,
    pub extension_allowlist: Vec<String>,
    pub tenant_client_cidrs: Vec<String>,
    pub limits: PostgresProvisioningLimits,
    pub nodes: Vec<PostgresProvisioningNode>,
    pub namespaces: Vec<PostgresProvisioningNamespace>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PlatformDatabases {
    pub manager: String,
    pub job_queues: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresProvisioningLimits {
    pub namespace_database_connections: u32,
    pub runtime_login_connections: u32,
    pub readiness_verifier_connections: u32,
    pub statement_timeout_ms: u32,
    pub lock_timeout_ms: u32,
    pub idle_in_transaction_timeout_ms: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresProvisioningNode {
    pub node_id: String,
    pub certificate_pem_path: String,
    pub certificate_sha256: String,
    pub certificate_common_name: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresProvisioningNamespace {
    pub namespace: String,
    pub modules: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DerivedNamespace {
    pub namespace: String,
    pub database: String,
    pub owner: String,
    pub runtime_group: String,
    pub maintenance_role: String,
    pub schemas: Vec<String>,
    pub node_logins: Vec<DerivedNodeLogins>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DerivedNodeLogins {
    pub node_id: String,
    pub runtime: String,
    pub readiness: String,
}

impl PostgresProvisioningManifest {
    pub fn parse_toml(input: &str) -> Result<Self> {
        let manifest: Self =
            toml::from_str(input).context("invalid PostgreSQL provisioning manifest")?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.format_version == POSTGRES_PROVISIONING_FORMAT_VERSION,
            "unsupported PostgreSQL provisioning manifest format_version"
        );
        ClusterId::parse(self.cluster_id.clone())?;
        ensure!(self.generation > 0, "generation must be nonzero");
        ensure!(
            self.postgres_major == SUPPORTED_POSTGRES_MAJOR,
            "postgres_major must be {SUPPORTED_POSTGRES_MAJOR}"
        );
        ensure!(
            self.ident_map_name == "wruntime_nodes",
            "ident_map_name must be wruntime_nodes"
        );
        validate_sha256(&self.postgres_ca_sha256, "postgres_ca_sha256")?;
        ensure!(
            !self.platform_databases.manager.is_empty(),
            "manager database is required"
        );
        let mut platform_databases = BTreeSet::from([self.platform_databases.manager.clone()]);
        for database in &self.platform_databases.job_queues {
            ensure!(!database.is_empty(), "job queue database name is required");
            ensure!(
                platform_databases.insert(database.clone()),
                "platform databases must be distinct"
            );
        }
        ensure!(
            !self.tenant_client_cidrs.is_empty(),
            "tenant_client_cidrs must not be empty"
        );
        for cidr in &self.tenant_client_cidrs {
            validate_cidr(cidr)?;
        }
        for extension in &self.extension_allowlist {
            validate_sql_token(extension, "extension")?;
        }
        let values = [
            self.limits.namespace_database_connections,
            self.limits.runtime_login_connections,
            self.limits.readiness_verifier_connections,
            self.limits.statement_timeout_ms,
            self.limits.lock_timeout_ms,
            self.limits.idle_in_transaction_timeout_ms,
        ];
        ensure!(
            values.iter().all(|value| *value > 0),
            "all limits must be nonzero"
        );
        ensure!(
            self.limits.namespace_database_connections >= self.limits.runtime_login_connections,
            "namespace database connection ceiling is below one runtime-login ceiling"
        );

        let mut nodes = BTreeSet::new();
        let mut common_names = BTreeSet::new();
        let mut fingerprints = BTreeSet::new();
        ensure!(!self.nodes.is_empty(), "nodes must not be empty");
        for node in &self.nodes {
            NodeId::parse(node.node_id.clone())?;
            ensure!(
                nodes.insert(&node.node_id),
                "duplicate node_id: {}",
                node.node_id
            );
            validate_common_name(&node.certificate_common_name)?;
            ensure!(
                common_names.insert(&node.certificate_common_name),
                "duplicate certificate Common Name"
            );
            validate_sha256(&node.certificate_sha256, "certificate_sha256")?;
            ensure!(
                fingerprints.insert(&node.certificate_sha256),
                "duplicate node certificate fingerprint"
            );
            ensure!(
                !node.certificate_pem_path.is_empty(),
                "certificate_pem_path is required"
            );
        }

        let mut namespaces = BTreeSet::new();
        let mut physical = BTreeMap::<String, String>::new();
        ensure!(!self.namespaces.is_empty(), "namespaces must not be empty");
        for namespace in &self.namespaces {
            Namespace::parse(namespace.namespace.clone())?;
            ensure!(
                namespaces.insert(&namespace.namespace),
                "duplicate namespace: {}",
                namespace.namespace
            );
            ensure!(
                !namespace.modules.is_empty(),
                "namespace modules must not be empty"
            );
            let mut modules = BTreeSet::new();
            for module in &namespace.modules {
                crate::identity::validate_name(module, "module name")?;
                ensure!(modules.insert(module), "duplicate module in namespace");
            }
            let tenant_database = namespace_database(&namespace.namespace);
            ensure!(
                !platform_databases.contains(&tenant_database),
                "tenant and platform databases must be distinct"
            );
            for name in [
                tenant_database,
                namespace_owner(&namespace.namespace),
                namespace_runtime_group(&namespace.namespace),
                namespace_maintenance_role(&namespace.namespace),
            ] {
                if let Some(previous) = physical.insert(name.clone(), namespace.namespace.clone()) {
                    bail!(
                        "derived PostgreSQL identity collision between {previous} and {}",
                        namespace.namespace
                    );
                }
            }
        }
        Ok(())
    }

    pub fn derive_namespaces(&self) -> Vec<DerivedNamespace> {
        self.namespaces
            .iter()
            .map(|namespace| DerivedNamespace {
                namespace: namespace.namespace.clone(),
                database: namespace_database(&namespace.namespace),
                owner: namespace_owner(&namespace.namespace),
                runtime_group: namespace_runtime_group(&namespace.namespace),
                maintenance_role: namespace_maintenance_role(&namespace.namespace),
                schemas: namespace
                    .modules
                    .iter()
                    .map(|module| module_schema(&namespace.namespace, module))
                    .collect(),
                node_logins: self
                    .nodes
                    .iter()
                    .map(|node| DerivedNodeLogins {
                        node_id: node.node_id.clone(),
                        runtime: namespace_runtime_login(&node.node_id, &namespace.namespace),
                        readiness: namespace_readiness_verifier(
                            &node.node_id,
                            &namespace.namespace,
                        ),
                    })
                    .collect(),
            })
            .collect()
    }

    pub fn normalized_digest(&self) -> String {
        let mut normalized = self.clone();
        normalized.platform_databases.job_queues.sort();
        normalized.extension_allowlist.sort();
        normalized.tenant_client_cidrs.sort();
        normalized
            .nodes
            .sort_by(|left, right| left.node_id.cmp(&right.node_id));
        normalized
            .namespaces
            .sort_by(|left, right| left.namespace.cmp(&right.namespace));
        let encoded = toml::to_string(&normalized).expect("validated manifest serializes");
        format!("sha256:{:x}", Sha256::digest(encoded.as_bytes()))
    }
}

pub fn validate_common_name(value: &str) -> Result<()> {
    ensure!(
        !value.is_empty() && value.len() <= 128,
        "certificate Common Name has invalid length"
    );
    ensure!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric()
                || matches!(byte, b'.' | b'_' | b':' | b'@' | b'-')),
        "certificate Common Name contains pg_ident syntax"
    );
    ensure!(
        !value.starts_with('/'),
        "certificate Common Name must be literal"
    );
    Ok(())
}

fn validate_cidr(value: &str) -> Result<()> {
    let (address, prefix) = value
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("invalid tenant client CIDR: {value}"))?;
    let address = address
        .parse::<std::net::IpAddr>()
        .with_context(|| format!("invalid tenant client CIDR: {value}"))?;
    let prefix = prefix
        .parse::<u8>()
        .with_context(|| format!("invalid tenant client CIDR: {value}"))?;
    let valid = match address {
        std::net::IpAddr::V4(_) => prefix <= 32,
        std::net::IpAddr::V6(_) => prefix <= 128,
    };
    ensure!(valid, "invalid tenant client CIDR: {value}");
    Ok(())
}

fn validate_sha256(value: &str, field: &str) -> Result<()> {
    ensure!(
        value.len() == 71
            && value.starts_with("sha256:")
            && value[7..]
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()),
        "{field} must be a lowercase sha256 fingerprint"
    );
    Ok(())
}

fn validate_sql_token(value: &str, kind: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 63
            && value
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'),
        "invalid {kind} name"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest() -> String {
        r#"
format_version = 1
cluster_id = "cluster-a"
generation = 1
postgres_major = 18
postgres_ca_sha256 = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
ident_map_name = "wruntime_nodes"
extension_allowlist = ["pgcrypto"]
tenant_client_cidrs = ["10.0.0.0/24"]
[platform_databases]
manager = "manager"
job_queues = ["jobs"]
[limits]
namespace_database_connections = 20
runtime_login_connections = 5
readiness_verifier_connections = 1
statement_timeout_ms = 30000
lock_timeout_ms = 5000
idle_in_transaction_timeout_ms = 60000
[[nodes]]
node_id = "node-a"
certificate_pem_path = "/run/certs/node-a.pem"
certificate_sha256 = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
certificate_common_name = "wr-db-node-a"
[[namespaces]]
namespace = "shop"
modules = ["catalog", "orders"]
"#
        .into()
    }

    #[test]
    fn strict_versioned_manifest_derives_cartesian_logins() {
        let parsed = PostgresProvisioningManifest::parse_toml(&manifest()).unwrap();
        let derived = parsed.derive_namespaces();
        assert_eq!(derived[0].database, "wr_db_shop");
        assert_eq!(derived[0].node_logins.len(), 1);
        assert!(derived[0].node_logins[0].runtime.starts_with("wr_runtime_"));
    }

    #[test]
    fn rejects_unknown_and_secret_fields() {
        assert!(PostgresProvisioningManifest::parse_toml(
            &(manifest() + "\nprivate_key_path = \"secret\"\n")
        )
        .is_err());
        assert!(PostgresProvisioningManifest::parse_toml(
            &manifest().replace("format_version = 1", "format_version = 2")
        )
        .is_err());
    }

    #[test]
    fn rejects_common_name_injection() {
        assert!(validate_common_name("node-a # /regex").is_err());
    }
}
