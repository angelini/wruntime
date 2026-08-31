use std::collections::HashMap;
use std::sync::Arc;

use prost_reflect::{DescriptorPool, MessageDescriptor};
use tokio::sync::RwLock;
use wr_common::discovery::ManagerDiscovery;
use wr_common::wruntime::GetSchemaRequest;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SchemaKey {
    namespace: String,
    module: String,
    version: String,
}

impl SchemaKey {
    fn new(namespace: &str, module: &str, version: &str) -> Self {
        Self {
            namespace: namespace.to_owned(),
            module: module.to_owned(),
            version: version.to_owned(),
        }
    }
}

/// Lazily loads and caches immutable module schemas by exact routed version.
pub struct SchemaCache {
    pools: RwLock<HashMap<SchemaKey, DescriptorPool>>,
    discovery: Option<Arc<ManagerDiscovery>>,
}

impl SchemaCache {
    pub fn new(discovery: Arc<ManagerDiscovery>) -> Self {
        Self {
            pools: RwLock::new(HashMap::new()),
            discovery: Some(discovery),
        }
    }

    pub async fn insert(
        &self,
        namespace: &str,
        module: &str,
        version: &str,
        schema_bytes: &[u8],
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            !schema_bytes.is_empty(),
            "schema bytes for {namespace}.{module}@{version} must not be empty"
        );
        let pool = DescriptorPool::decode(schema_bytes).map_err(|error| {
            anyhow::anyhow!("invalid descriptor set for {namespace}.{module}@{version}: {error}")
        })?;
        self.pools
            .write()
            .await
            .insert(SchemaKey::new(namespace, module, version), pool);
        Ok(())
    }

    pub async fn input_descriptor(
        &self,
        namespace: &str,
        module: &str,
        version: &str,
        rpc_path: &str,
    ) -> anyhow::Result<Option<MessageDescriptor>> {
        let pool = self.pool(namespace, module, version).await?;
        Ok(resolve_input_message(&pool, rpc_path))
    }

    async fn pool(
        &self,
        namespace: &str,
        module: &str,
        version: &str,
    ) -> anyhow::Result<DescriptorPool> {
        let key = SchemaKey::new(namespace, module, version);
        if let Some(pool) = self.pools.read().await.get(&key).cloned() {
            return Ok(pool);
        }

        let discovery = self
            .discovery
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("schema is not cached"))?;
        let mut client = discovery
            .get_client()
            .await
            .map_err(|error| anyhow::anyhow!("manager unavailable: {error}"))?;
        let schema_bytes = client
            .get_schema(GetSchemaRequest {
                namespace: namespace.to_owned(),
                module: module.to_owned(),
                version: version.to_owned(),
            })
            .await
            .map_err(|error| anyhow::anyhow!("schema fetch failed: {error}"))?
            .into_inner()
            .proto_schema;

        self.insert(namespace, module, version, &schema_bytes)
            .await?;
        self.pools
            .read()
            .await
            .get(&key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("schema cache insert failed"))
    }
}

impl Default for SchemaCache {
    fn default() -> Self {
        Self {
            pools: RwLock::new(HashMap::new()),
            discovery: None,
        }
    }
}

fn resolve_input_message(pool: &DescriptorPool, rpc_path: &str) -> Option<MessageDescriptor> {
    let mut segments = rpc_path.trim_start_matches('/').split('/');
    let service_name = segments.next()?;
    let method_name = segments.next()?;
    if service_name.is_empty() || method_name.is_empty() || segments.next().is_some() {
        return None;
    }

    pool.get_service_by_name(service_name)
        .and_then(|service| {
            service
                .methods()
                .find(|method| method.name() == method_name)
        })
        .map(|method| method.input())
}

#[cfg(test)]
mod tests {
    use prost::Message as _;
    use prost_types::{
        DescriptorProto, FileDescriptorProto, FileDescriptorSet, MethodDescriptorProto,
        ServiceDescriptorProto,
    };

    use super::*;

    fn descriptor_set() -> Vec<u8> {
        FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("test.proto".into()),
                package: Some("test".into()),
                message_type: vec![DescriptorProto {
                    name: Some("Request".into()),
                    ..Default::default()
                }],
                service: vec![ServiceDescriptorProto {
                    name: Some("Service".into()),
                    method: vec![MethodDescriptorProto {
                        name: Some("Call".into()),
                        input_type: Some(".test.Request".into()),
                        output_type: Some(".test.Request".into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                syntax: Some("proto3".into()),
                ..Default::default()
            }],
        }
        .encode_to_vec()
    }

    #[tokio::test]
    async fn resolves_exact_service_and_method_path() -> anyhow::Result<()> {
        let cache = SchemaCache::default();
        cache
            .insert("ns", "module", "1.0.0", &descriptor_set())
            .await?;

        assert!(cache
            .input_descriptor("ns", "module", "1.0.0", "/test.Service/Call")
            .await?
            .is_some());
        assert!(cache
            .input_descriptor("ns", "module", "1.0.0", "/other.Service/Call")
            .await?
            .is_none());
        Ok(())
    }
}
