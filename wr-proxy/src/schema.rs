use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use tracing::{info, warn};

use wr_common::wruntime::{
    manager_service_client::ManagerServiceClient, GetSchemaRequest, ListEnginesRequest,
};

use crate::routing::CachedRoutingTable;

/// Caches a `DescriptorPool` per (namespace, module) pair, built from the
/// `FileDescriptorSet` bytes registered by each engine on startup.
pub struct SchemaCache {
    pools: RwLock<HashMap<(String, String), DescriptorPool>>,
}

impl SchemaCache {
    pub fn new() -> Self {
        Self {
            pools: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for SchemaCache {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaCache {
    /// Store a compiled schema for `(namespace, module)`. `schema_bytes` must
    /// be a serialised `FileDescriptorSet` as produced by protoc
    /// (`--descriptor_set_out`).
    pub async fn insert(
        &self,
        namespace: &str,
        module: &str,
        schema_bytes: &[u8],
    ) -> anyhow::Result<()> {
        if schema_bytes.is_empty() {
            return Ok(());
        }
        let pool = DescriptorPool::decode(schema_bytes)
            .map_err(|e| anyhow::anyhow!("bad FileDescriptorSet for {namespace}.{module}: {e}"))?;
        self.pools
            .write()
            .await
            .insert((namespace.to_string(), module.to_string()), pool);
        Ok(())
    }

    /// Validate `body` against the protobuf schema registered for `(namespace, module)`.
    ///
    /// `path` should be a gRPC-style method path, e.g.
    /// `/inventory.InventoryService/GetItems`, used to resolve the expected
    /// input message type.
    ///
    /// Returns `Some(error_message)` if validation fails. Returns `None` when
    /// the body is valid, the schema is absent, or the path cannot be resolved.
    pub async fn validate(
        &self,
        namespace: &str,
        module: &str,
        path: &str,
        body: &[u8],
    ) -> Option<String> {
        if body.is_empty() {
            return None;
        }

        let pools = self.pools.read().await;
        let pool = pools.get(&(namespace.to_string(), module.to_string()))?;

        let message_desc = resolve_input_message(pool, path)?;

        match DynamicMessage::decode(message_desc, body) {
            Ok(_) => None,
            Err(e) => Some(format!(
                "schema validation failed for {namespace}.{module}{path}: {e}"
            )),
        }
    }
}

/// Background task: periodically fetches schemas from wr-manager for every
/// module registered across all known engines and stores them in the cache.
pub async fn sync_schemas(
    mut client: ManagerServiceClient<tonic::transport::Channel>,
    _table: CachedRoutingTable,
    cache: Arc<SchemaCache>,
    ttl_secs: u64,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(ttl_secs));
    loop {
        interval.tick().await;

        // Enumerate all registered engines to find (module, version) pairs.
        let engines = match client.list_engines(ListEnginesRequest {}).await {
            Ok(r) => r.into_inner().engines,
            Err(e) => {
                warn!(error = %e, "schema sync — list_engines failed");
                continue;
            }
        };

        for engine in engines {
            for module_desc in engine.modules {
                let module = &module_desc.name;
                let namespace = &module_desc.namespace;
                let version = &module_desc.version;

                // Skip if already cached.
                if cache
                    .pools
                    .read()
                    .await
                    .contains_key(&(namespace.clone(), module.clone()))
                {
                    continue;
                }

                match client
                    .get_schema(GetSchemaRequest {
                        namespace: namespace.clone(),
                        module: module.clone(),
                        version: version.clone(),
                    })
                    .await
                {
                    Ok(resp) => {
                        let bytes = resp.into_inner().proto_schema;
                        if let Err(e) = cache.insert(namespace, module, &bytes).await {
                            warn!(namespace, module, version, error = %e, "schema decode error");
                        } else {
                            info!(namespace, module, version, "schema cached");
                        }
                    }
                    Err(e) if e.code() == tonic::Code::NotFound => {}
                    Err(e) => {
                        warn!(namespace, module, version, error = %e, "schema fetch error");
                    }
                }
            }
        }
    }
}

/// Parse a gRPC path (`/package.ServiceName/MethodName`) and return the input
/// `MessageDescriptor` for that RPC.
fn resolve_input_message(pool: &DescriptorPool, path: &str) -> Option<MessageDescriptor> {
    let path = path.trim_start_matches('/');
    let (service_name, method_name) = path.split_once('/')?;
    let service = pool.get_service_by_name(service_name)?;
    let method = service.methods().find(|m| m.name() == method_name)?;
    Some(method.input())
}
