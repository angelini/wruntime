use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct NodeConfig {
    /// Externally-reachable HTTP address of the local proxy, e.g. "http://127.0.0.1:9001".
    /// Used as the node identity in routing table comparisons.
    pub proxy_address: String,
    /// gRPC address of the proxy's NodeService control plane, e.g. "http://127.0.0.1:9002".
    /// Engines use this for registration, heartbeats, and deregistration.
    #[serde(default)]
    pub control_address: String,
}
