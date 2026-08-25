use std::collections::{btree_map::Entry, BTreeMap};
use std::path::PathBuf;

use anyhow::{Context, Result};

use crate::config::{EngineConfig, ModuleMode};
use crate::pool::module_schema;

/// Database startup work shared by every configured instance of one module schema.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaStartup {
    pub namespace: String,
    pub module: String,
    pub schema: String,
    pub migrations_path: Option<PathBuf>,
}

/// Validated database startup topology.
///
/// Module descriptors remain per config entry, while provisioning and migrations
/// are represented once per `(namespace, module)` schema. Namespace capacities
/// retain every DB-enabled config entry's contribution.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StartupDbManifest {
    pub schemas: Vec<SchemaStartup>,
    pub namespace_capacities: BTreeMap<String, usize>,
    pub has_workers: bool,
}

impl StartupDbManifest {
    pub fn build(config: &EngineConfig) -> Result<Self> {
        let Some(database) = config.database.as_ref() else {
            return Ok(Self {
                has_workers: config
                    .modules
                    .iter()
                    .any(|module| module.mode == ModuleMode::Worker),
                ..Self::default()
            });
        };

        let mut schemas = BTreeMap::<(String, String), SchemaStartup>::new();
        let mut namespace_capacities = BTreeMap::<String, usize>::new();

        for module in config.modules.iter().filter(|module| module.database) {
            let contribution = module
                .db_max_connections
                .unwrap_or(database.max_connections);
            anyhow::ensure!(
                contribution > 0,
                "module '{}.{}' database connection contribution must be > 0",
                module.namespace,
                module.name
            );
            let total = namespace_capacities
                .entry(module.namespace.clone())
                .or_default();
            *total = total.checked_add(contribution).ok_or_else(|| {
                anyhow::anyhow!(
                    "database connection capacity overflow for namespace '{}'",
                    module.namespace
                )
            })?;

            let migrations_path = module
                .migrations_path
                .as_deref()
                .map(std::fs::canonicalize)
                .transpose()
                .with_context(|| {
                    format!(
                        "failed to canonicalize migrations_path for module '{}.{}'",
                        module.namespace, module.name
                    )
                })?;
            let key = (module.namespace.clone(), module.name.clone());
            let candidate = SchemaStartup {
                namespace: module.namespace.clone(),
                module: module.name.clone(),
                schema: module_schema(&module.namespace, &module.name),
                migrations_path,
            };
            match schemas.entry(key) {
                Entry::Vacant(entry) => {
                    entry.insert(candidate);
                }
                Entry::Occupied(entry) => {
                    anyhow::ensure!(
                        entry.get().migrations_path == candidate.migrations_path,
                        "conflicting migration sources for module schema '{}': {:?} versus {:?}",
                        candidate.schema,
                        entry.get().migrations_path,
                        candidate.migrations_path
                    );
                }
            }
        }

        Ok(Self {
            schemas: schemas.into_values().collect(),
            namespace_capacities,
            has_workers: config
                .modules
                .iter()
                .any(|module| module.mode == ModuleMode::Worker),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_with_modules(modules: &str) -> EngineConfig {
        toml::from_str(&format!(
            r#"
listen_address = "127.0.0.1:9100"
[node]
proxy_address = "http://127.0.0.1:9001"
control_address = "http://127.0.0.1:9002"
peer_address = "https://127.0.0.1:9443"
[node.tls]
cert_path = "c.crt"
key_path = "c.key"
ca_cert_path = "ca.crt"
[database]
url = "postgres://localhost/test"
max_connections = 5
{modules}
"#
        ))
        .expect("test engine config")
    }

    #[test]
    fn duplicate_versions_share_schema_work_and_sum_capacity() {
        let config = config_with_modules(
            r#"
[[module]]
name = "orders"
namespace = "shop"
version = "1.0.0"
wasm_path = "orders.wasm"
database = true
db_max_connections = 2
[[module]]
name = "orders"
namespace = "shop"
version = "2.0.0"
wasm_path = "orders.wasm"
database = true
db_max_connections = 3
"#,
        );
        let manifest = StartupDbManifest::build(&config).expect("manifest");
        assert_eq!(manifest.schemas.len(), 1);
        assert_eq!(manifest.namespace_capacities["shop"], 5);
    }

    #[test]
    fn duplicate_schema_rejects_none_some_migration_conflict() {
        let migrations = tempfile::tempdir().expect("migration dir");
        let mut config = config_with_modules(
            r#"
[[module]]
name = "orders"
namespace = "shop"
version = "1.0.0"
wasm_path = "orders.wasm"
database = true
[[module]]
name = "orders"
namespace = "shop"
version = "2.0.0"
wasm_path = "orders.wasm"
database = true
"#,
        );
        config.modules[1].migrations_path = Some(
            migrations
                .path()
                .to_str()
                .expect("UTF-8 temp path")
                .to_string(),
        );
        let error = StartupDbManifest::build(&config).expect_err("conflict must fail");
        assert!(error.to_string().contains("conflicting migration sources"));
    }

    #[test]
    fn namespace_capacity_overflow_is_rejected() {
        let mut config = config_with_modules(
            r#"
[[module]]
name = "a"
namespace = "shop"
version = "1.0.0"
wasm_path = "a.wasm"
database = true
[[module]]
name = "b"
namespace = "shop"
version = "1.0.0"
wasm_path = "b.wasm"
database = true
"#,
        );
        config.modules[0].db_max_connections = Some(usize::MAX);
        config.modules[1].db_max_connections = Some(1);
        let error = StartupDbManifest::build(&config).expect_err("overflow must fail");
        assert!(error.to_string().contains("capacity overflow"));
    }
}
