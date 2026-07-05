use anyhow::Result;
use serde::Deserialize;
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
        v.check(
            !self.node.proxy_address.is_empty(),
            "node.proxy_address is required",
        );
        v.check(self.node.peer_port > 0, "node.peer_port must be > 0");
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
            for (i, route) in ext.routes.iter().enumerate() {
                v.check(
                    !route.path.is_empty(),
                    format!("external.routes[{i}].path is required"),
                );
                v.check(
                    !route.module.is_empty(),
                    format!("external.routes[{i}].module is required"),
                );
                v.check(
                    !route.namespace.is_empty(),
                    format!("external.routes[{i}].namespace is required"),
                );
            }
        }

        v.finish()
    }
}
