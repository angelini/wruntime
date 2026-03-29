use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, RwLock};

use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use tracing::{info, warn};

use wr_common::wruntime::{
    manager_service_client::ManagerServiceClient, GetSchemaRequest, ListEnginesRequest,
};

use crate::routing::CachedRoutingTable;

/// Result of looking up a message descriptor by fully-qualified type name.
pub enum MessageLookup {
    /// Descriptor found.
    Found(MessageDescriptor),
    /// No schema has been synced yet for this (namespace, module) pair.
    SchemaNotCached,
    /// Schema is cached but the requested type name is not present.
    TypeNotFound,
}

/// Outcome of a schema validation check.
pub enum ValidationOutcome {
    /// Body decoded successfully against the expected message type.
    Pass,
    /// Body failed protobuf decoding against the expected message type.
    Fail(String),
    /// No schema is cached for this module yet (proxy still syncing).
    SchemaNotCached,
    /// Path does not map to any RPC declared in the module's schema.
    /// All inter-service traffic must target a known gRPC method.
    MethodNotFound(String),
}

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
            anyhow::bail!("schema bytes for {module}.{namespace} must not be empty");
        }
        let pool = DescriptorPool::decode(schema_bytes)
            .map_err(|e| anyhow::anyhow!("bad FileDescriptorSet for {module}.{namespace}: {e}"))?;
        self.pools
            .write()
            .await
            .insert((namespace.to_string(), module.to_string()), pool);
        Ok(())
    }

    /// Look up a message descriptor by its fully-qualified type name
    /// (e.g. `"inventory.GetItemRequest"`) within the schema cached for
    /// `(namespace, module)`.
    pub async fn message_descriptor(
        &self,
        namespace: &str,
        module: &str,
        type_name: &str,
    ) -> MessageLookup {
        let pools = self.pools.read().await;
        let Some(pool) = pools.get(&(namespace.to_string(), module.to_string())) else {
            return MessageLookup::SchemaNotCached;
        };
        match pool.get_message_by_name(type_name) {
            Some(desc) => MessageLookup::Found(desc),
            None => MessageLookup::TypeNotFound,
        }
    }

    /// Validate `body` against the protobuf schema registered for `(namespace, module)`.
    ///
    /// - Returns [`ValidationOutcome::Pass`] when the body is empty or when
    ///   the path cannot be resolved to a known RPC (schema does not cover it).
    /// - Returns [`ValidationOutcome::Fail`] when the body is present but
    ///   does not decode against the expected message type.
    /// - Returns [`ValidationOutcome::SchemaNotCached`] when no schema has
    ///   been synced yet for this module.
    pub async fn validate(
        &self,
        namespace: &str,
        module: &str,
        path: &str,
        body: &[u8],
    ) -> ValidationOutcome {
        // Note: empty body is NOT skipped. An empty proto3 message encodes to
        // zero bytes, so an empty body is still validated against the schema.

        let pools = self.pools.read().await;
        let Some(pool) = pools.get(&(namespace.to_string(), module.to_string())) else {
            return ValidationOutcome::SchemaNotCached;
        };

        let Some(message_desc) = resolve_input_message(pool, path) else {
            return ValidationOutcome::MethodNotFound(format!(
                "path '{path}' does not match any RPC in the schema for \
                 {module}.{namespace} — all inter-service calls must use \
                 gRPC paths (/package.Service/Method)"
            ));
        };

        match DynamicMessage::decode(message_desc, body) {
            Ok(_) => ValidationOutcome::Pass,
            Err(e) => ValidationOutcome::Fail(format!(
                "schema validation failed for {module}.{namespace}{path}: {e}"
            )),
        }
    }
}

/// Background task: periodically fetches schemas from wr-manager for every
/// module registered across all known engines and stores them in the cache.
///
/// Also wakes immediately when `trigger` is notified (fired by
/// `sync_routing_table` on every routing table version advance), reducing the
/// window between a new engine registering and its schema being cached.
pub async fn sync_schemas(
    mut client: ManagerServiceClient<tonic::transport::Channel>,
    _table: CachedRoutingTable,
    cache: Arc<SchemaCache>,
    ttl_secs: u64,
    trigger: Arc<Notify>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(ttl_secs));
    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = trigger.notified() => {
                // Reset so the interval doesn't fire again shortly after this
                // triggered sync.
                interval.reset();
            }
        }

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
                    Err(e) if e.code() == tonic::Code::NotFound => {
                        warn!(
                            namespace,
                            module,
                            version,
                            "schema not found on manager — module must declare a schema"
                        );
                    }
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
