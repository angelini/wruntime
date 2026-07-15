use std::collections::HashMap;

use anyhow::{Context, Result};
use http::Method;
use serde::{de::Error as _, Deserialize, Deserializer};
use wr_common::identity::{ModuleName, Namespace};
use wr_common::node::{is_loopback_addr, NodeConfig};

#[derive(Deserialize, Clone)]
pub struct ProxyConfig {
    /// Loopback TCP address to listen on for inbound HTTP, e.g. "127.0.0.1:9001".
    pub listen_address: String,
    /// gRPC listen address for the NodeService control plane (engines connect here).
    pub control_address: String,
    /// Node configuration — this proxy's own address as reachable by peer proxies.
    pub node: NodeConfig,
    /// PostgreSQL connection for manager discovery via `wr_managers` table.
    pub database: DatabaseConfig,
    #[serde(default)]
    pub cache: CacheConfig,
    #[serde(default)]
    pub circuit_breaker: CircuitBreakerConfig,
    /// Optional external-facing listener with a restricted set of public routes.
    pub external: Option<ExternalConfig>,
    /// Optional egress allowlist — controls which external domains WASM modules may call.
    #[serde(default)]
    pub egress: Option<EgressConfig>,
}

#[derive(Deserialize, Clone)]
pub struct DatabaseConfig {
    /// `postgres://user:pass@host:port/dbname` connection string.
    pub url: String,
    /// Maximum number of pooled connections. Defaults to 2.
    #[serde(default = "default_discovery_max_connections")]
    pub max_connections: usize,
}

fn default_discovery_max_connections() -> usize {
    2
}

/// Configuration for the external-facing HTTP listener.
#[derive(Deserialize, Clone)]
pub struct ExternalConfig {
    /// TCP address to bind the external listener, e.g. "0.0.0.0:8080"
    pub listen_address: String,
    /// Routes accessible to external callers.
    #[serde(default, alias = "route")]
    pub routes: Vec<ExternalRoute>,
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

/// A single publicly-exposed route mapping an HTTP path to an internal module.
#[derive(Clone, Debug)]
pub struct ExternalRoute {
    path: RoutePattern,
    methods: MethodSet,
    target: ModuleTarget,
}

impl ExternalRoute {
    pub fn new(
        path: impl Into<String>,
        methods: Vec<String>,
        module: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Result<Self> {
        Ok(Self {
            path: RoutePattern::try_from(path.into())?,
            methods: MethodSet::try_from_strings(methods)?,
            target: ModuleTarget::new(namespace.into(), module.into())?,
        })
    }

    pub fn path(&self) -> &RoutePattern {
        &self.path
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
        Self::new(raw.path, raw.methods, raw.module, raw.namespace).map_err(D::Error::custom)
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
        v.check(!self.database.url.is_empty(), "database.url is required");
        if let Err(error) = self.node.peer_address() {
            v.check(false, format!("invalid node configuration: {error}"));
        }
        v.check(
            !self.node.tls.cert_path.is_empty(),
            "node.tls.cert_path is required",
        );
        v.check(
            !self.node.tls.key_path.is_empty(),
            "node.tls.key_path is required",
        );
        v.check(
            !self.node.tls.ca_cert_path.is_empty(),
            "node.tls.ca_cert_path is required",
        );
        v.check(
            self.cache.routing_table_ttl_secs > 0,
            "cache.routing_table_ttl_secs must be > 0",
        );
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
            if let Err(error) = build_external_route_index(&ext.routes) {
                v.check(false, format!("invalid external routes: {error:#}"));
            }
        }

        v.finish()
    }
}
