#[allow(dead_code, unused_imports)]
mod helpers;

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use wr_common::config::Validatable;
use wr_engine::config::EngineConfig;
use wr_manager::config::ManagerConfig;
use wr_proxy::config::ProxyConfig;

fn unique_temp_path(name: &str, ext: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "wr-config-test-{name}-{}-{nanos}.{ext}",
        std::process::id()
    ))
}

fn unique_temp_dir(name: &str) -> PathBuf {
    let dir = unique_temp_path(name, "dir");
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_temp_config(name: &str, content: &str) -> PathBuf {
    let path = unique_temp_path(name, "toml");
    fs::write(&path, content).unwrap();
    path
}

fn load_config_from_toml<T>(
    name: &str,
    content: &str,
    load: impl FnOnce(&str) -> anyhow::Result<T>,
) -> T {
    let path = write_temp_config(name, content);
    let cfg = load(path.to_str().unwrap()).unwrap();
    let _ = fs::remove_file(path);
    cfg
}

fn engine_toml_with_temp_artifacts(content: &str, root: &Path) -> String {
    let mut value: toml::Value = toml::from_str(content).unwrap();
    let modules_dir = root.join("modules");
    let schemas_dir = root.join("schemas");
    let migrations_dir = root.join("migrations");
    fs::create_dir_all(&modules_dir).unwrap();
    fs::create_dir_all(&schemas_dir).unwrap();
    fs::create_dir_all(&migrations_dir).unwrap();

    let modules = value
        .get_mut("module")
        .and_then(toml::Value::as_array_mut)
        .expect("engine TOML must contain module tables");

    for module in modules {
        let table = module.as_table_mut().unwrap();
        let name = table
            .get("name")
            .and_then(toml::Value::as_str)
            .unwrap()
            .to_string();

        let wasm_path = modules_dir.join(format!("{name}.wasm"));
        fs::write(&wasm_path, b"wasm").unwrap();
        table.insert(
            "wasm_path".to_string(),
            toml::Value::String(wasm_path.to_string_lossy().into_owned()),
        );

        if table.contains_key("schema_path") {
            let schema_path = schemas_dir.join(format!("{name}.binpb"));
            fs::write(&schema_path, b"schema").unwrap();
            table.insert(
                "schema_path".to_string(),
                toml::Value::String(schema_path.to_string_lossy().into_owned()),
            );
        }

        if table.contains_key("migrations_path") {
            let migrations_path = migrations_dir.join(&name);
            fs::create_dir_all(&migrations_path).unwrap();
            table.insert(
                "migrations_path".to_string(),
                toml::Value::String(migrations_path.to_string_lossy().into_owned()),
            );
        }
    }

    toml::to_string_pretty(&value).unwrap()
}

#[test]
fn test_manager_config_valid() {
    let toml = r#"
        listen_address                = "0.0.0.0:9000"
        engine_heartbeat_timeout_secs = 30
        local_proxy_address           = "http://127.0.0.1:9001"

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
    let cfg = load_config_from_toml("manager-valid", toml, ManagerConfig::load);
    assert_eq!(cfg.listen_address, "0.0.0.0:9000");
    assert_eq!(cfg.engine_heartbeat_timeout_secs, 30);
    assert_eq!(cfg.module_heartbeat_timeout_secs, Some(30));
    assert_eq!(cfg.local_proxy_address, "http://127.0.0.1:9001");
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
        local_proxy_address = "http://127.0.0.1:9001"

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
    let cfg = load_config_from_toml("manager-default-heartbeat", toml, ManagerConfig::load);
    assert_eq!(cfg.engine_heartbeat_timeout_secs, 10);
    assert_eq!(cfg.module_heartbeat_timeout_secs, Some(10));
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
    let err = cfg.validate().unwrap_err();
    assert!(
        format!("{err:#}").contains("cache.routing_table_ttl_secs must be > 0"),
        "unexpected validation error: {err:#}"
    );
}

#[test]
fn test_example_config_files_parse() {
    let manager_toml = include_str!("../../examples/config/manager.toml");
    let proxy_toml = include_str!("../../examples/config/proxy.toml");
    let engine_toml = include_str!("../../examples/config/engine.toml");

    let manager = load_config_from_toml("example-manager", manager_toml, ManagerConfig::load);
    assert_eq!(manager.local_proxy_address, "http://127.0.0.1:9001");

    let proxy = load_config_from_toml("example-proxy", proxy_toml, ProxyConfig::load);
    assert_eq!(proxy.listen_address, "127.0.0.1:9001");

    let artifact_root = unique_temp_dir("example-engine-artifacts");
    let engine_toml = engine_toml_with_temp_artifacts(engine_toml, &artifact_root);
    let engine = load_config_from_toml("example-engine", &engine_toml, EngineConfig::load);
    assert_eq!(engine.modules.len(), 2);
    assert!(engine.database.is_some());
    let _ = fs::remove_dir_all(artifact_root);
}

#[test]
fn test_live_engine_examples_parse_structurally() {
    let codegen: EngineConfig = toml::from_str(include_str!("../../examples/codegen/engine.toml"))
        .expect("codegen engine.toml must parse");
    assert!(codegen.blobstore.is_some());
    assert!(codegen.llm.is_some());
    assert!(codegen.modules.iter().any(|m| m.fs.is_some()));
    assert!(codegen.modules.iter().any(|m| {
        m.name == "worker"
            && m.database
            && m.worker_concurrency == 1
            && m.worker_job_timeout_secs == 900
    }));

    let ledger: EngineConfig = toml::from_str(include_str!(
        "../../examples/stockmarket/engine-ledger.toml"
    ))
    .expect("stockmarket ledger engine.toml must parse");
    assert!(ledger.blobstore.is_some());
    assert!(ledger
        .modules
        .iter()
        .any(|m| m.name == "ledger" && m.database && m.blobstore));
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
