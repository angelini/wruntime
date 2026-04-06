use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct NodeConfig {
    /// Plain HTTP address of the local proxy, e.g. "http://127.0.0.1:9001".
    /// Engines on the same host use this for outbound WASM HTTP calls.
    /// Should bind to loopback only — network traffic uses the mTLS peer listener.
    pub proxy_address: String,
    /// gRPC address of the proxy's NodeService control plane, e.g. "http://127.0.0.1:9002".
    /// Engines use this for registration, heartbeats, and deregistration.
    #[serde(default)]
    pub control_address: String,
    /// mTLS peer listener port. Peer proxies connect here for cross-node traffic.
    /// The peer address is derived from `proxy_address` host + this port.
    #[serde(default = "default_peer_port")]
    pub peer_port: u16,
    /// TLS certificate configuration for mTLS.
    pub tls: TlsConfig,
}

fn default_peer_port() -> u16 {
    9443
}

/// TLS certificate paths for mutual TLS authentication.
#[derive(Debug, Deserialize, Clone)]
pub struct TlsConfig {
    /// PEM file containing this node's certificate chain.
    pub cert_path: String,
    /// PEM file containing this node's private key.
    pub key_path: String,
    /// PEM file containing the CA certificate used to verify peers.
    pub ca_cert_path: String,
}

impl NodeConfig {
    /// Derive the mTLS peer address from `proxy_address` host + `peer_port`.
    ///
    /// Example: `"http://10.0.1.5:9001"` + `peer_port=9443` → `"https://10.0.1.5:9443"`
    pub fn peer_address(&self) -> String {
        let uri: http::Uri = self
            .proxy_address
            .parse()
            .expect("proxy_address must be a valid URI");
        let host = uri.host().expect("proxy_address must have a host");
        format!("https://{}:{}", host, self.peer_port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_full_config() {
        let toml = r#"
            proxy_address = "http://127.0.0.1:9001"
            control_address = "http://127.0.0.1:9002"
            peer_port = 9443

            [tls]
            cert_path = "certs/node.crt"
            key_path = "certs/node.key"
            ca_cert_path = "certs/ca.crt"
        "#;
        let cfg: NodeConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.proxy_address, "http://127.0.0.1:9001");
        assert_eq!(cfg.control_address, "http://127.0.0.1:9002");
        assert_eq!(cfg.peer_port, 9443);
        assert_eq!(cfg.tls.cert_path, "certs/node.crt");
    }

    #[test]
    fn peer_port_defaults_to_9443() {
        let toml = r#"
            proxy_address = "http://127.0.0.1:9001"

            [tls]
            cert_path = "certs/node.crt"
            key_path = "certs/node.key"
            ca_cert_path = "certs/ca.crt"
        "#;
        let cfg: NodeConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.peer_port, 9443);
        assert_eq!(cfg.control_address, "");
    }

    #[test]
    fn peer_address_derived_correctly() {
        let toml = r#"
            proxy_address = "http://10.0.1.5:9001"
            peer_port = 8443

            [tls]
            cert_path = "c.crt"
            key_path = "c.key"
            ca_cert_path = "ca.crt"
        "#;
        let cfg: NodeConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.peer_address(), "https://10.0.1.5:8443");
    }

    #[test]
    fn missing_proxy_address_fails() {
        let toml = r#"
            control_address = "http://127.0.0.1:9002"

            [tls]
            cert_path = "c.crt"
            key_path = "c.key"
            ca_cert_path = "ca.crt"
        "#;
        assert!(toml::from_str::<NodeConfig>(toml).is_err());
    }

    #[test]
    fn missing_tls_fails() {
        let toml = r#"proxy_address = "http://127.0.0.1:9001""#;
        assert!(toml::from_str::<NodeConfig>(toml).is_err());
    }
}
