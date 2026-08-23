use std::net::{IpAddr, SocketAddr};

use anyhow::{bail, Context, Result};
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    /// Plain HTTP URL of the local proxy used by engines for outbound calls.
    pub proxy_address: String,
    /// Plain HTTP URL of the local proxy NodeService used for registration and heartbeats.
    pub control_address: String,
    /// Explicit mTLS URL advertised to peer proxies, e.g. "https://node-a:9443".
    pub peer_address: String,
    /// TLS certificate configuration for mTLS.
    pub tls: TlsConfig,
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
    /// Validate and return the explicitly advertised mTLS peer URL.
    pub fn peer_address(&self) -> Result<String> {
        let uri: http::Uri = self
            .peer_address
            .parse()
            .with_context(|| format!("peer_address '{}' is not a valid URI", self.peer_address))?;
        if uri.scheme_str() != Some("https") {
            bail!("peer_address must use https");
        }
        if uri.host().filter(|host| !host.is_empty()).is_none() {
            bail!("peer_address must include a host");
        }
        if uri.port_u16().is_none() {
            bail!("peer_address must include a non-zero port");
        }
        Ok(self.peer_address.clone())
    }

    /// Port on which the proxy binds its mTLS peer listener.
    pub fn peer_port(&self) -> Result<u16> {
        let uri: http::Uri = self
            .peer_address
            .parse()
            .with_context(|| format!("peer_address '{}' is not a valid URI", self.peer_address))?;
        uri.port_u16()
            .filter(|port| *port > 0)
            .ok_or_else(|| anyhow::anyhow!("peer_address must include a non-zero port"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node_config(peer_address: &str) -> NodeConfig {
        toml::from_str(&format!(
            r#"
            proxy_address = "http://127.0.0.1:9001"
            control_address = "http://127.0.0.1:9002"
            peer_address = "{peer_address}"

            [tls]
            cert_path = "c.crt"
            key_path = "c.key"
            ca_cert_path = "ca.crt"
        "#
        ))
        .unwrap()
    }

    #[test]
    fn explicit_addresses_deserialize_and_validate() {
        let cfg = node_config("https://node-a.example:9443");
        assert_eq!(cfg.proxy_address, "http://127.0.0.1:9001");
        assert_eq!(cfg.control_address, "http://127.0.0.1:9002");
        assert_eq!(cfg.peer_address().unwrap(), "https://node-a.example:9443");
        assert_eq!(cfg.peer_port().unwrap(), 9443);
    }

    #[test]
    fn peer_address_rejects_malformed_or_non_mtls_values() {
        for peer_address in [
            "",
            "127.0.0.1:9443",
            "http://node-a:9443",
            "https://node-a",
            "/relative/path",
        ] {
            assert!(
                node_config(peer_address).peer_address().is_err(),
                "peer_address should be rejected: {peer_address:?}"
            );
        }
    }

    #[test]
    fn obsolete_derived_peer_shape_is_rejected() {
        let toml = r#"
            proxy_address = "http://127.0.0.1:9001"
            control_address = "http://127.0.0.1:9002"
            peer_port = 9443

            [tls]
            cert_path = "c.crt"
            key_path = "c.key"
            ca_cert_path = "ca.crt"
        "#;
        assert!(toml::from_str::<NodeConfig>(toml).is_err());
    }

    #[test]
    fn missing_proxy_address_fails() {
        let toml = r#"
            control_address = "http://127.0.0.1:9002"
            peer_address = "https://node-a:9443"

            [tls]
            cert_path = "c.crt"
            key_path = "c.key"
            ca_cert_path = "ca.crt"
        "#;
        assert!(toml::from_str::<NodeConfig>(toml).is_err());
    }

    #[test]
    fn missing_tls_fails() {
        let toml = r#"
            proxy_address = "http://127.0.0.1:9001"
            control_address = "http://127.0.0.1:9002"
            peer_address = "https://node-a:9443"
        "#;
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
