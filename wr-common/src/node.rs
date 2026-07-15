use std::net::{IpAddr, Ipv6Addr, SocketAddr};

use anyhow::{bail, Context, Result};
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

/// Returns `true` if `addr` binds a loopback interface.
///
/// Accepts an optional `http://`/`https://` scheme, an optional port, bracketed
/// IPv6 (`[::1]:9001`), and the literal host `localhost` (matched WITHOUT DNS
/// resolution). Any authority that fails to parse is treated as NON-loopback so
/// it becomes a hard config error rather than being silently accepted.
pub fn is_loopback_addr(addr: &str) -> bool {
    let authority = addr
        .strip_prefix("http://")
        .or_else(|| addr.strip_prefix("https://"))
        .unwrap_or(addr);

    if let Ok(sock) = authority.parse::<SocketAddr>() {
        return sock.ip().is_loopback();
    }
    if let Ok(ip) = authority.parse::<IpAddr>() {
        return ip.is_loopback();
    }

    // Not an IP literal: accept only the explicit host `localhost`
    // (with or without a port); never resolve DNS.
    let host = authority.rsplit_once(':').map_or(authority, |(h, _)| h);
    host == "localhost"
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
    pub fn peer_address(&self) -> Result<String> {
        let uri: http::Uri = self.proxy_address.parse().with_context(|| {
            format!("proxy_address '{}' is not a valid URI", self.proxy_address)
        })?;
        let scheme = uri
            .scheme_str()
            .ok_or_else(|| anyhow::anyhow!("proxy_address must include an http or https scheme"))?;
        if !matches!(scheme, "http" | "https") {
            bail!("proxy_address scheme must be http or https");
        }
        let host = uri
            .host()
            .filter(|host| !host.is_empty())
            .ok_or_else(|| anyhow::anyhow!("proxy_address must include a host"))?;
        if self.peer_port == 0 {
            bail!("peer_port must be > 0");
        }

        let peer_host = if host.parse::<Ipv6Addr>().is_ok() {
            format!("[{host}]")
        } else {
            host.to_string()
        };
        Ok(format!("https://{peer_host}:{}", self.peer_port))
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

    fn node_config(proxy_address: &str, peer_port: u16) -> NodeConfig {
        toml::from_str(&format!(
            r#"
            proxy_address = "{proxy_address}"
            peer_port = {peer_port}

            [tls]
            cert_path = "c.crt"
            key_path = "c.key"
            ca_cert_path = "ca.crt"
        "#
        ))
        .unwrap()
    }

    #[test]
    fn peer_address_derived_correctly() {
        for (proxy_address, expected) in [
            ("http://10.0.1.5:9001", "https://10.0.1.5:8443"),
            (
                "http://service.internal:9001",
                "https://service.internal:8443",
            ),
            ("http://localhost:9001", "https://localhost:8443"),
            ("http://[::1]:9001", "https://[::1]:8443"),
        ] {
            assert_eq!(
                node_config(proxy_address, 8443).peer_address().unwrap(),
                expected
            );
        }
    }

    #[test]
    fn peer_address_rejects_malformed_proxy_addresses() {
        for proxy_address in ["", "127.0.0.1:9001", "http://", "/relative/path"] {
            assert!(
                node_config(proxy_address, 9443).peer_address().is_err(),
                "proxy_address should be rejected: {proxy_address:?}"
            );
        }
    }

    #[test]
    fn peer_address_rejects_zero_port() {
        assert!(node_config("http://localhost:9001", 0)
            .peer_address()
            .is_err());
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

    #[test]
    fn is_loopback_addr_accepts_loopback() {
        assert!(is_loopback_addr("127.0.0.1:9001"));
        assert!(is_loopback_addr("http://127.0.0.1:9001"));
        assert!(is_loopback_addr("https://127.0.0.1:9001"));
        assert!(is_loopback_addr("[::1]:9001"));
        assert!(is_loopback_addr("localhost:9001"));
        assert!(is_loopback_addr("localhost"));
        assert!(is_loopback_addr("127.0.0.1"));
        assert!(is_loopback_addr("::1"));
    }

    #[test]
    fn is_loopback_addr_rejects_non_loopback() {
        assert!(!is_loopback_addr("0.0.0.0:9001"));
        assert!(!is_loopback_addr("192.168.1.5:9001"));
        assert!(!is_loopback_addr("::"));
    }

    #[test]
    fn is_loopback_addr_rejects_malformed() {
        assert!(!is_loopback_addr(""));
        assert!(!is_loopback_addr("http://"));
        assert!(!is_loopback_addr("nonsense"));
        assert!(!is_loopback_addr("example.com:80"));
    }
}
