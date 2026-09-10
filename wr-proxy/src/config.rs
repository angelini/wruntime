use std::collections::HashMap;

use anyhow::{Context, Result};
use http::Method;
use serde::{de::Error as _, Deserialize, Deserializer};
use wr_common::identity::{ModuleName, Namespace};
use wr_common::node::{is_loopback_addr, ClientTlsConfig, NodeConfig, ServerTlsConfig};

#[derive(Deserialize, Clone)]
pub struct ProxyConfig {
    /// Loopback TCP address to listen on for inbound HTTP, e.g. "127.0.0.1:9001".
    pub listen_address: String,
    /// gRPC listen address for the NodeService control plane (engines connect here).
    pub control_address: String,
    /// Transport-neutral local node and peer endpoint metadata.
    pub node: NodeConfig,
    /// Server identity and client roots for the peer-proxy acceptor.
    pub endpoint_tls: ServerTlsConfig,
    /// Enrolled proxy identity and server roots for manager and peer connectors.
    pub client_tls: ClientTlsConfig,
    /// PostgreSQL connection for manager discovery via `wr_managers` table.
    pub database: DatabaseConfig,
    #[serde(default)]
    pub cache: CacheConfig,
    #[serde(default)]
    pub circuit_breaker: CircuitBreakerConfig,
    #[serde(default)]
    pub status: StatusConfig,
    /// Complete managed deployment identity, absent only for local development.
    #[serde(default)]
    pub deployment: Option<ProxyDeploymentConfig>,
    /// Optional external-facing listener with a restricted set of public routes.
    pub external: Option<ExternalConfig>,
    /// Optional egress allowlist — controls which external domains WASM modules may call.
    #[serde(default)]
    pub egress: Option<EgressConfig>,
}

#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ProxyDeploymentConfig {
    pub node_id: String,
    #[serde(deserialize_with = "deserialize_revision")]
    pub revision: u64,
    pub bundle_digest: String,
    pub operation_id: String,
    pub revision_digest: String,
}

#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct StatusConfig {
    #[serde(default = "default_report_interval_secs")]
    pub report_interval_secs: u64,
}

fn deserialize_revision<'de, D>(deserializer: D) -> std::result::Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Revision {
        Number(u64),
        String(String),
    }
    match Revision::deserialize(deserializer)? {
        Revision::Number(value) => Ok(value),
        Revision::String(value) => value.parse().map_err(D::Error::custom),
    }
}

fn default_report_interval_secs() -> u64 {
    5
}

impl Default for StatusConfig {
    fn default() -> Self {
        Self {
            report_interval_secs: default_report_interval_secs(),
        }
    }
}

#[derive(Deserialize, Clone)]
pub struct DatabaseConfig {
    /// `postgres://user:pass@host:port/dbname` connection string.
    pub url: String,
    /// Maximum number of pooled connections. Defaults to 2.
    #[serde(default = "default_discovery_max_connections")]
    pub max_connections: usize,
    /// Manager lease freshness used by direct-PostgreSQL discovery fallback.
    /// Must match every manager's configured cluster liveness threshold.
    #[serde(default = "default_manager_liveness_threshold_secs")]
    pub manager_liveness_threshold_secs: u64,
}

fn default_manager_liveness_threshold_secs() -> u64 {
    wr_common::DEFAULT_MANAGER_LIVENESS_THRESHOLD_SECS
}

fn default_discovery_max_connections() -> usize {
    2
}

fn valid_sha256_digest(value: &str) -> bool {
    value.len() == 71
        && value.starts_with("sha256:")
        && value[7..]
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

/// Configuration for the external-facing HTTP listener.
#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ExternalConfig {
    /// TCP address to bind the external listener, e.g. "0.0.0.0:8080"
    pub listen_address: String,
    /// Maximum public request body buffered for transcoding and schema validation.
    #[serde(default = "default_external_max_request_body_bytes")]
    pub max_request_body_bytes: usize,
    /// Routes accessible to external callers.
    #[serde(default)]
    pub routes: Vec<ExternalRoute>,
}

fn default_external_max_request_body_bytes() -> usize {
    16 * 1024 * 1024
}

/// A validated external route pattern accepted by `matchit`.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RoutePattern(String);

impl RoutePattern {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for RoutePattern {
    type Error = anyhow::Error;

    fn try_from(path: String) -> Result<Self> {
        if !path.starts_with('/') {
            anyhow::bail!("route path must start with '/'");
        }
        let mut router = matchit::Router::new();
        router
            .insert(path.clone(), ())
            .with_context(|| format!("invalid route pattern '{path}'"))?;
        Ok(Self(path))
    }
}

/// Method filter normalized once at config/construction time.
#[derive(Clone, Debug)]
pub enum MethodSet {
    All,
    Only(Vec<Method>),
}

impl MethodSet {
    pub fn allows(&self, method: &Method) -> bool {
        match self {
            Self::All => true,
            Self::Only(methods) => methods.iter().any(|allowed| allowed == method),
        }
    }

    fn try_from_strings(methods: Vec<String>) -> Result<Self> {
        if methods.is_empty() {
            return Ok(Self::All);
        }

        let mut parsed = Vec::with_capacity(methods.len());
        for method in methods {
            let normalized = method.to_ascii_uppercase();
            let parsed_method = Method::from_bytes(normalized.as_bytes())
                .with_context(|| format!("invalid HTTP method '{method}'"))?;
            if !parsed.contains(&parsed_method) {
                parsed.push(parsed_method);
            }
        }
        Ok(Self::Only(parsed))
    }
}

#[derive(Clone, Debug)]
pub struct ModuleTarget {
    namespace: Namespace,
    module: ModuleName,
}

impl ModuleTarget {
    pub fn namespace(&self) -> &str {
        self.namespace.as_str()
    }

    pub fn module(&self) -> &str {
        self.module.as_str()
    }

    fn new(namespace: String, module: String) -> Result<Self> {
        Ok(Self {
            namespace: Namespace::parse(namespace.trim())?,
            module: ModuleName::parse(module.trim())?,
        })
    }
}

/// A canonical protobuf service method path.
#[derive(Clone, Debug)]
pub struct RpcPath(String);

impl RpcPath {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for RpcPath {
    type Error = anyhow::Error;

    fn try_from(path: String) -> Result<Self> {
        let mut segments = path.strip_prefix('/').unwrap_or_default().split('/');
        let service = segments.next().unwrap_or_default();
        let method = segments.next().unwrap_or_default();
        anyhow::ensure!(
            !service.is_empty()
                && !method.is_empty()
                && segments.next().is_none()
                && !path.contains(['?', '#', '{', '}']),
            "rpc_path must use the canonical '/package.Service/Method' form"
        );
        Ok(Self(path))
    }
}

/// A single publicly-exposed route mapping an HTTP path to a protobuf RPC.
#[derive(Clone, Debug)]
pub struct ExternalRoute {
    path: RoutePattern,
    rpc_path: RpcPath,
    methods: MethodSet,
    target: ModuleTarget,
}

impl ExternalRoute {
    pub fn new(
        path: impl Into<String>,
        rpc_path: impl Into<String>,
        methods: Vec<String>,
        module: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Result<Self> {
        Ok(Self {
            path: RoutePattern::try_from(path.into())?,
            rpc_path: RpcPath::try_from(rpc_path.into())?,
            methods: MethodSet::try_from_strings(methods)?,
            target: ModuleTarget::new(namespace.into(), module.into())?,
        })
    }

    pub fn path(&self) -> &RoutePattern {
        &self.path
    }

    pub fn rpc_path(&self) -> &RpcPath {
        &self.rpc_path
    }

    pub fn methods(&self) -> &MethodSet {
        &self.methods
    }

    pub fn target(&self) -> &ModuleTarget {
        &self.target
    }
}

#[derive(Deserialize)]
struct RawExternalRoute {
    path: String,
    rpc_path: String,
    #[serde(default)]
    methods: Vec<String>,
    module: String,
    namespace: String,
}

impl<'de> Deserialize<'de> for ExternalRoute {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawExternalRoute::deserialize(deserializer)?;
        Self::new(
            raw.path,
            raw.rpc_path,
            raw.methods,
            raw.module,
            raw.namespace,
        )
        .map_err(D::Error::custom)
    }
}

pub(crate) fn build_external_route_index(
    routes: &[ExternalRoute],
) -> Result<matchit::Router<Vec<usize>>> {
    let mut path_map: HashMap<&str, Vec<usize>> = HashMap::new();
    for (index, route) in routes.iter().enumerate() {
        path_map
            .entry(route.path().as_str())
            .or_default()
            .push(index);
    }

    let mut router = matchit::Router::new();
    for (path, indices) in path_map {
        router
            .insert(path, indices)
            .with_context(|| format!("conflicting external route pattern '{path}'"))?;
    }
    Ok(router)
}

#[derive(Deserialize, Clone)]
pub struct CacheConfig {
    /// How often (seconds) to poll wr-manager for routing table updates
    pub routing_table_ttl_secs: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            routing_table_ttl_secs: 2,
        }
    }
}

#[derive(Deserialize, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failures before the breaker opens.
    pub failure_threshold: u32,
    /// How long (seconds) the breaker stays open before entering half-open.
    pub open_duration_secs: u64,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            open_duration_secs: 30,
        }
    }
}

/// Controls which external domains WASM modules are permitted to call via egress.
#[derive(Deserialize, Clone, Default)]
pub struct EgressConfig {
    /// Domains that WASM modules may reach directly.
    /// Supports a single leading wildcard label: `*.openai.com` matches
    /// `api.openai.com` but not `openai.com` or `a.b.openai.com`.
    #[serde(default)]
    pub allowed_domains: Vec<String>,
}

impl wr_common::config::Validatable for ProxyConfig {
    fn validate(&self) -> Result<()> {
        self.validate_inner()
    }
}

impl ProxyConfig {
    pub fn load(path: &str) -> Result<Self> {
        wr_common::config::load(path)
    }

    fn validate_inner(&self) -> Result<()> {
        use wr_common::config::Validator;
        let mut v = Validator::new();

        v.check(
            !self.listen_address.is_empty(),
            "listen_address is required",
        );
        v.check(
            !self.control_address.is_empty(),
            "control_address is required",
        );
        v.check(
            is_loopback_addr(&self.listen_address),
            "listen_address must bind to loopback (127.0.0.1, ::1, or localhost); \
             network traffic uses the mTLS peer listener",
        );
        v.check(
            is_loopback_addr(&self.control_address),
            "control_address must bind to loopback (127.0.0.1, ::1, or localhost)",
        );
        v.check(
            self.node.proxy_address.starts_with("http://")
                && is_loopback_addr(&self.node.proxy_address),
            "node.proxy_address must be an absolute loopback HTTP URL",
        );
        v.check(
            self.node.control_address.starts_with("http://")
                && is_loopback_addr(&self.node.control_address),
            "node.control_address must be an absolute loopback HTTP URL",
        );
        v.check(!self.database.url.is_empty(), "database.url is required");
        v.check(
            self.database.manager_liveness_threshold_secs > 0,
            "database.manager_liveness_threshold_secs must be > 0",
        );
        if let Err(error) = self.node.peer_address() {
            v.check(false, format!("invalid node configuration: {error}"));
        }
        v.check(
            !self.endpoint_tls.cert_path.is_empty(),
            "endpoint_tls.cert_path is required",
        );
        v.check(
            !self.endpoint_tls.key_path.is_empty(),
            "endpoint_tls.key_path is required",
        );
        v.check(
            !self.endpoint_tls.client_ca_cert_path.is_empty(),
            "endpoint_tls.client_ca_cert_path is required",
        );
        v.check(
            !self.client_tls.cert_path.is_empty(),
            "client_tls.cert_path is required",
        );
        v.check(
            !self.client_tls.key_path.is_empty(),
            "client_tls.key_path is required",
        );
        v.check(
            !self.client_tls.server_ca_cert_path.is_empty(),
            "client_tls.server_ca_cert_path is required",
        );
        v.check(
            self.cache.routing_table_ttl_secs > 0,
            "cache.routing_table_ttl_secs must be > 0",
        );
        v.check(
            (1..=300).contains(&self.status.report_interval_secs),
            "status.report_interval_secs must be in 1..=300",
        );
        if let Some(deployment) = &self.deployment {
            v.check(
                !deployment.node_id.is_empty(),
                "deployment.node_id is required",
            );
            v.check(deployment.revision > 0, "deployment.revision must be > 0");
            v.check(
                valid_sha256_digest(&deployment.bundle_digest),
                "deployment.bundle_digest must be sha256:<lowercase hex>",
            );
            v.check(
                !deployment.operation_id.is_empty(),
                "deployment.operation_id is required",
            );
            v.check(
                valid_sha256_digest(&deployment.revision_digest),
                "deployment.revision_digest must be sha256:<lowercase hex>",
            );
        }
        v.check(
            self.circuit_breaker.failure_threshold > 0,
            "circuit_breaker.failure_threshold must be > 0",
        );
        v.check(
            self.circuit_breaker.open_duration_secs > 0,
            "circuit_breaker.open_duration_secs must be > 0",
        );

        if let Some(egress) = &self.egress {
            for (i, pattern) in egress.allowed_domains.iter().enumerate() {
                v.check(
                    !pattern.is_empty(),
                    format!("egress.allowed_domains[{i}] must not be empty"),
                );
                v.check(
                    !pattern.starts_with('.') && !pattern.ends_with('.'),
                    format!("egress.allowed_domains[{i}] must not start or end with '.'"),
                );
                v.check(
                    !pattern.contains(".."),
                    format!("egress.allowed_domains[{i}] must not contain '..'"),
                );
                for (j, label) in pattern.split('.').enumerate() {
                    if label.contains('*') {
                        v.check(
                            j == 0 && label == "*",
                            format!(
                                "egress.allowed_domains[{i}]: '*' may only appear as \
                                     the entire first label (e.g. '*.example.com')"
                            ),
                        );
                    }
                }
            }
        }
        if let Some(ext) = &self.external {
            v.check(
                !ext.listen_address.is_empty(),
                "external.listen_address is required",
            );
            v.check(
                ext.max_request_body_bytes > 0,
                "external.max_request_body_bytes must be > 0",
            );
            if let Err(error) = build_external_route_index(&ext.routes) {
                v.check(false, format!("invalid external routes: {error:#}"));
            }
        }

        v.finish()
    }
}

#[cfg(test)]
mod status_config_tests {
    use super::*;

    #[derive(Deserialize)]
    struct OptionalDeployment {
        deployment: Option<ProxyDeploymentConfig>,
    }

    #[test]
    fn unmanaged_deployment_may_be_absent_but_partial_managed_identity_is_rejected() {
        let unmanaged: OptionalDeployment = serde_json::from_str("{}").unwrap();
        assert!(unmanaged.deployment.is_none());
        let partial = r#"{"deployment":{"node_id":"node-a","revision":1}}"#;
        assert!(serde_json::from_str::<OptionalDeployment>(partial).is_err());
    }

    #[test]
    fn status_interval_has_a_safe_nonzero_default() {
        assert_eq!(StatusConfig::default().report_interval_secs, 5);
        assert!(valid_sha256_digest(&format!("sha256:{}", "a".repeat(64))));
        assert!(!valid_sha256_digest("sha256:ABC"));
    }
}
