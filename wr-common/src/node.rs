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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_full_config() {
        let toml = r#"
            proxy_address = "http://127.0.0.1:9001"
            control_address = "http://127.0.0.1:9002"
        "#;
        let cfg: NodeConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.proxy_address, "http://127.0.0.1:9001");
        assert_eq!(cfg.control_address, "http://127.0.0.1:9002");
    }

    #[test]
    fn control_address_defaults_to_empty() {
        let toml = r#"proxy_address = "http://127.0.0.1:9001""#;
        let cfg: NodeConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.control_address, "");
    }

    #[test]
    fn missing_proxy_address_fails() {
        let toml = r#"control_address = "http://127.0.0.1:9002""#;
        assert!(toml::from_str::<NodeConfig>(toml).is_err());
    }

    #[test]
    fn clone_is_independent() {
        let cfg = NodeConfig {
            proxy_address: "http://a:1".into(),
            control_address: "http://a:2".into(),
        };
        let clone = cfg.clone();
        assert_eq!(clone.proxy_address, "http://a:1");
        assert_eq!(clone.control_address, "http://a:2");
    }
}
