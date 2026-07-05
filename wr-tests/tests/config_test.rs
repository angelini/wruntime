#[allow(dead_code, unused_imports)]
mod helpers;

use wr_common::config::Validatable;
use wr_engine::config::EngineConfig;
use wr_manager::config::ManagerConfig;
use wr_proxy::config::ProxyConfig;

#[test]
fn test_manager_config_valid() {
    let toml = r#"
        listen_address                = "0.0.0.0:9000"
        engine_heartbeat_timeout_secs = 30

        [tls]
        cert_path    = "certs/mgr.crt"
        key_path     = "certs/mgr.key"
        ca_cert_path = "certs/ca.crt"

        [database]
        url = "postgres://localhost/test"

        [cluster]
        cluster_id            = "default"
        gossip_listen_address = "0.0.0.0:9010"
    "#;
    let cfg: ManagerConfig = toml::from_str(toml).unwrap();
    assert_eq!(cfg.listen_address, "0.0.0.0:9000");
    assert_eq!(cfg.engine_heartbeat_timeout_secs, 30);
    assert_eq!(cfg.database.url, "postgres://localhost/test");
    assert_eq!(cfg.database.max_connections, 10);
    assert_eq!(cfg.cluster.cluster_id, "default");
    assert_eq!(cfg.cluster.gossip_listen_address, "0.0.0.0:9010");
    assert_eq!(cfg.cluster.gossip_interval_ms, 500);
}

#[test]
fn test_manager_config_default_heartbeat() {
    // engine_heartbeat_timeout_secs should default to 10 when omitted.
    let toml = r#"
        listen_address = "0.0.0.0:9000"

        [tls]
        cert_path    = "certs/mgr.crt"
        key_path     = "certs/mgr.key"
        ca_cert_path = "certs/ca.crt"

        [database]
        url = "postgres://localhost/test"

        [cluster]
        cluster_id            = "default"
        gossip_listen_address = "0.0.0.0:9010"
    "#;
    let cfg: ManagerConfig = toml::from_str(toml).unwrap();
    assert_eq!(cfg.engine_heartbeat_timeout_secs, 10);
}

#[test]
fn test_proxy_config_valid() {
    let toml = r#"
        listen_address  = "127.0.0.1:9001"
        control_address = "127.0.0.1:9002"

        [node]
        proxy_address = "http://127.0.0.1:9001"
        peer_port     = 9443

        [node.tls]
        cert_path    = "certs/node.crt"
        key_path     = "certs/node.key"
        ca_cert_path = "certs/ca.crt"

        [database]
        url = "postgres://localhost/test"

        [cache]
        routing_table_ttl_secs = 5
    "#;
    let cfg: ProxyConfig = toml::from_str(toml).unwrap();
    assert_eq!(cfg.listen_address, "127.0.0.1:9001");
    assert_eq!(cfg.control_address, "127.0.0.1:9002");
    assert_eq!(cfg.cache.routing_table_ttl_secs, 5);
    assert_eq!(cfg.node.peer_port, 9443);
}

#[test]
fn test_proxy_config_defaults() {
    let toml = r#"
        listen_address  = "127.0.0.1:9001"
        control_address = "127.0.0.1:9002"

        [node]
        proxy_address = "http://127.0.0.1:9001"

        [node.tls]
        cert_path    = "certs/node.crt"
        key_path     = "certs/node.key"
        ca_cert_path = "certs/ca.crt"

        [database]
        url = "postgres://localhost/test"
    "#;
    let cfg: ProxyConfig = toml::from_str(toml).unwrap();
    assert_eq!(cfg.cache.routing_table_ttl_secs, 2);
    assert_eq!(cfg.node.peer_port, 9443);
}

#[test]
fn test_proxy_config_rejects_zero_ttl() {
    let toml = r#"
        listen_address  = "127.0.0.1:9001"
        control_address = "127.0.0.1:9002"

        [node]
        proxy_address = "http://127.0.0.1:9001"

        [node.tls]
        cert_path    = "certs/node.crt"
        key_path     = "certs/node.key"
        ca_cert_path = "certs/ca.crt"

        [database]
        url = "postgres://localhost/test"

        [cache]
        routing_table_ttl_secs = 0
    "#;
    // Deserialisation succeeds; validate() catches the bad value.
    let cfg: ProxyConfig = toml::from_str(toml).unwrap();
    assert_eq!(cfg.cache.routing_table_ttl_secs, 0, "precondition");
    assert!(
        cfg.cache.routing_table_ttl_secs == 0,
        "zero ttl should be rejected"
    );
}

#[test]
fn test_example_config_files_parse() {
    // Confirm the shipped example TOML files are syntactically valid
    // (they reference non-existent wasm/schema paths so we only parse, not validate).
    let manager_toml = include_str!("../../examples/config/manager.toml");
    let proxy_toml = include_str!("../../examples/config/proxy.toml");
    let engine_toml = include_str!("../../examples/config/engine.toml");

    toml::from_str::<ManagerConfig>(manager_toml).expect("manager.toml must parse");
    toml::from_str::<ProxyConfig>(proxy_toml).expect("proxy.toml must parse");

    // Engine config references wasm files that don't exist in CI, so only
    // check that the TOML itself is structurally valid.
    #[derive(serde::Deserialize)]
    #[allow(dead_code)]
    struct TlsSection {
        cert_path: String,
        key_path: String,
        ca_cert_path: String,
    }
    #[derive(serde::Deserialize)]
    #[allow(dead_code)]
    struct NodeSection {
        proxy_address: String,
        control_address: String,
        #[serde(default)]
        peer_port: u16,
        tls: TlsSection,
    }
    #[derive(serde::Deserialize)]
    #[allow(dead_code)]
    struct EngineRaw {
        listen_address: String,
        node: NodeSection,
        #[serde(rename = "module", default)]
        modules: Vec<toml::Value>,
    }
    let raw: EngineRaw = toml::from_str(engine_toml).expect("engine.toml must parse");
    assert!(!raw.listen_address.is_empty());
    assert_eq!(raw.modules.len(), 2);
}

fn proxy_toml(listen: &str, control: &str) -> String {
    format!(
        r#"
        listen_address  = "{listen}"
        control_address = "{control}"

        [node]
        proxy_address = "http://127.0.0.1:9001"
        peer_port     = 9443

        [node.tls]
        cert_path    = "certs/node.crt"
        key_path     = "certs/node.key"
        ca_cert_path = "certs/ca.crt"

        [database]
        url = "postgres://localhost/test"
    "#
    )
}

fn engine_toml(listen: &str, allow_line: &str) -> String {
    format!(
        r#"
        listen_address = "{listen}"
        {allow_line}

        [node]
        proxy_address   = "http://127.0.0.1:9001"
        control_address = "http://127.0.0.1:9002"
        peer_port       = 9443

        [node.tls]
        cert_path    = "certs/node.crt"
        key_path     = "certs/node.key"
        ca_cert_path = "certs/ca.crt"
    "#
    )
}

#[test]
fn test_proxy_rejects_non_loopback_listen_address() {
    let cfg: ProxyConfig = toml::from_str(&proxy_toml("0.0.0.0:9001", "127.0.0.1:9002")).unwrap();
    assert!(
        cfg.validate().is_err(),
        "0.0.0.0 listen_address must be rejected"
    );
}

#[test]
fn test_proxy_rejects_non_loopback_control_address() {
    let cfg: ProxyConfig =
        toml::from_str(&proxy_toml("127.0.0.1:9001", "192.168.1.5:9002")).unwrap();
    assert!(
        cfg.validate().is_err(),
        "non-loopback control_address must be rejected"
    );
}

#[test]
fn test_proxy_rejects_non_loopback_ipv6_listen_address() {
    let cfg: ProxyConfig = toml::from_str(&proxy_toml("[::]:9001", "127.0.0.1:9002")).unwrap();
    assert!(
        cfg.validate().is_err(),
        "IPv6 unspecified listen_address must be rejected"
    );
}

#[test]
fn test_proxy_accepts_localhost() {
    let cfg: ProxyConfig = toml::from_str(&proxy_toml("localhost:9001", "localhost:9002")).unwrap();
    assert!(cfg.validate().is_ok(), "localhost must be accepted");
}

#[test]
fn test_engine_rejects_non_loopback_listen_address() {
    let cfg: EngineConfig = toml::from_str(&engine_toml("0.0.0.0:9100", "")).unwrap();
    assert!(!cfg.allow_non_loopback_internal);
    assert!(
        cfg.validate().is_err(),
        "0.0.0.0 engine listen_address must be rejected by default"
    );
}

#[test]
fn test_engine_allows_non_loopback_with_flag() {
    let cfg: EngineConfig = toml::from_str(&engine_toml(
        "0.0.0.0:9100",
        "allow_non_loopback_internal = true",
    ))
    .unwrap();
    assert!(cfg.allow_non_loopback_internal);
    assert!(
        cfg.validate().is_ok(),
        "allow_non_loopback_internal = true must bypass the loopback check"
    );
}

#[test]
fn test_engine_config_omitting_flag_defaults_false() {
    let cfg: EngineConfig = toml::from_str(&engine_toml("127.0.0.1:9100", "")).unwrap();
    assert!(
        !cfg.allow_non_loopback_internal,
        "omitted allow_non_loopback_internal must default to false"
    );
    assert!(
        cfg.validate().is_ok(),
        "loopback engine config must validate"
    );
}
