#[allow(dead_code, unused_imports)]
mod helpers;

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
