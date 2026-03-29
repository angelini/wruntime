use anyhow::{Context, Result};
use serde::Deserialize;
use wr_common::node::NodeConfig;

#[derive(Deserialize, Clone)]
pub struct ProxyConfig {
    /// TCP address to listen on for inbound HTTP, e.g. "0.0.0.0:9001"
    pub listen_address: String,
    /// gRPC address of wr-manager, e.g. "http://127.0.0.1:9000"
    pub manager_address: String,
    /// Node configuration — this proxy's own address as reachable by peer proxies.
    pub node: NodeConfig,
    #[serde(default)]
    pub cache: CacheConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
    /// Optional external-facing listener with a restricted set of public routes.
    pub external: Option<ExternalConfig>,
}

/// Configuration for the external-facing HTTP listener.
#[derive(Deserialize, Clone)]
pub struct ExternalConfig {
    /// TCP address to bind the external listener, e.g. "0.0.0.0:8080"
    pub listen_address: String,
    /// Routes accessible to external callers.
    #[serde(default)]
    pub routes: Vec<ExternalRoute>,
}

/// A single publicly-exposed route mapping an HTTP path to an internal module.
#[derive(Deserialize, Clone, Default)]
pub struct ExternalRoute {
    /// URL path pattern, e.g. "/items" or "/items/{id}".
    /// Segments wrapped in `{braces}` match any single path segment.
    pub path: String,
    /// Allowed HTTP methods (case-insensitive). Empty means all methods are allowed.
    #[serde(default)]
    pub methods: Vec<String>,
    /// Target module name.
    pub module: String,
    /// Target namespace.
    pub namespace: String,
    /// RPC method path to forward to, e.g. "/ecommerce.inventory/GetItem".
    /// Uses the `{namespace}.{module}/MethodName` format, consistent with the
    /// HTTP hostname used for inter-module addressing.
    /// When set together with `request_type` and `response_type`, the ingress
    /// layer transcodes the JSON request body to protobuf before forwarding and
    /// transcodes the protobuf response back to JSON before returning.
    pub grpc_path: Option<String>,
    /// Fully-qualified protobuf message type for the request body,
    /// e.g. "ecommerce.GetItemRequest". Required when `grpc_path` is set.
    pub request_type: Option<String>,
    /// Fully-qualified protobuf message type for the response body,
    /// e.g. "ecommerce.GetItemResponse". Required when `grpc_path` is set.
    pub response_type: Option<String>,
}

#[derive(Deserialize, Clone)]
pub struct CacheConfig {
    /// How often (seconds) to poll wr-manager for routing table updates
    pub routing_table_ttl_secs: u64,
    /// How often (seconds) to re-fetch module schemas from wr-manager
    pub schema_ttl_secs: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            routing_table_ttl_secs: 5,
            schema_ttl_secs: 60,
        }
    }
}

#[derive(Deserialize, Clone)]
pub struct MetricsConfig {
    /// How often (seconds) to flush buffered metrics to wr-manager
    pub flush_interval_secs: u64,
    /// Capacity of the in-process metrics channel
    pub queue_depth: usize,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            flush_interval_secs: 10,
            queue_depth: 1000,
        }
    }
}

impl ProxyConfig {
    pub fn load(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config: {path}"))?;
        let config: ProxyConfig =
            toml::from_str(&content).context("failed to parse proxy config")?;
        config.validate().context("invalid proxy config")?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            !self.listen_address.is_empty(),
            "listen_address is required"
        );
        anyhow::ensure!(
            !self.manager_address.is_empty(),
            "manager_address is required"
        );
        anyhow::ensure!(
            !self.node.proxy_address.is_empty(),
            "node.proxy_address is required"
        );
        anyhow::ensure!(
            self.cache.routing_table_ttl_secs > 0,
            "cache.routing_table_ttl_secs must be > 0"
        );
        anyhow::ensure!(
            self.cache.schema_ttl_secs > 0,
            "cache.schema_ttl_secs must be > 0"
        );
        anyhow::ensure!(
            self.metrics.flush_interval_secs > 0,
            "metrics.flush_interval_secs must be > 0"
        );
        anyhow::ensure!(
            self.metrics.queue_depth > 0,
            "metrics.queue_depth must be > 0"
        );
        if let Some(ext) = &self.external {
            anyhow::ensure!(
                !ext.listen_address.is_empty(),
                "external.listen_address is required"
            );
            for (i, route) in ext.routes.iter().enumerate() {
                anyhow::ensure!(
                    !route.path.is_empty(),
                    "external.routes[{i}].path is required"
                );
                anyhow::ensure!(
                    !route.module.is_empty(),
                    "external.routes[{i}].module is required"
                );
                anyhow::ensure!(
                    !route.namespace.is_empty(),
                    "external.routes[{i}].namespace is required"
                );
                let has_grpc = route.grpc_path.is_some();
                let has_req = route.request_type.is_some();
                let has_resp = route.response_type.is_some();
                anyhow::ensure!(
                    has_grpc == has_req && has_req == has_resp,
                    "external.routes[{i}]: grpc_path, request_type, and response_type \
                     must all be set together or all omitted"
                );
                if let Some(p) = &route.grpc_path {
                    anyhow::ensure!(
                        p.starts_with('/'),
                        "external.routes[{i}].grpc_path must start with '/'"
                    );
                }
            }
        }
        Ok(())
    }
}
