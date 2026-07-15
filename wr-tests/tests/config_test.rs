use std::collections::{BTreeMap, BTreeSet};
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

fn write_temp_config(name: &str, content: &str) -> PathBuf {
    let path = unique_temp_path(name, "toml");
    fs::write(&path, content).unwrap();
    path
}

fn write_temp_file(name: &str, ext: &str, content: &[u8]) -> PathBuf {
    let path = unique_temp_path(name, ext);
    fs::write(&path, content).unwrap();
    path
}

fn workspace_path(path: impl AsRef<Path>) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("wr-tests crate has a workspace parent")
        .join(path)
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
    assert_eq!(cfg.circuit_breaker.failure_threshold, 5);
    assert_eq!(cfg.circuit_breaker.open_duration_secs, 30);
}

#[test]
fn test_proxy_config_circuit_breaker_section_requires_both_fields() {
    let toml = format!(
        "{}\n[circuit_breaker]\nfailure_threshold = 2\n",
        proxy_toml("127.0.0.1:9001", "127.0.0.1:9002")
    );
    assert!(toml::from_str::<ProxyConfig>(&toml).is_err());
}

#[test]
fn test_proxy_config_rejects_zero_circuit_breaker_values() {
    let mut cfg: ProxyConfig =
        toml::from_str(&proxy_toml("127.0.0.1:9001", "127.0.0.1:9002")).unwrap();
    cfg.circuit_breaker.failure_threshold = 0;
    cfg.circuit_breaker.open_duration_secs = 0;
    let error = cfg.validate().unwrap_err().to_string();
    assert!(error.contains("circuit_breaker.failure_threshold must be > 0"));
    assert!(error.contains("circuit_breaker.open_duration_secs must be > 0"));
}

#[test]
fn test_proxy_config_rejects_malformed_node_proxy_address() {
    let toml = proxy_toml("127.0.0.1:9001", "127.0.0.1:9002")
        .replace("http://127.0.0.1:9001", "127.0.0.1:9001");
    let cfg: ProxyConfig = toml::from_str(&toml).unwrap();
    assert!(cfg.validate().is_err());
}

#[test]
fn test_engine_config_rejects_malformed_node_proxy_address() {
    let toml = engine_toml("127.0.0.1:9100", "").replace("http://127.0.0.1:9001", "127.0.0.1:9001");
    let cfg: EngineConfig = toml::from_str(&toml).unwrap();
    assert!(cfg.validate().is_err());
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
    let cfg: ProxyConfig = toml::from_str(toml).unwrap();
    assert_eq!(cfg.cache.routing_table_ttl_secs, 0, "precondition");
    let err = cfg
        .validate()
        .expect_err("zero TTL must be rejected by ProxyConfig::validate");
    assert!(
        format!("{err:#}").contains("cache.routing_table_ttl_secs must be > 0"),
        "unexpected validation error: {err:#}"
    );
}

#[test]
fn test_proxy_config_accepts_positive_ttl() {
    let mut cfg: ProxyConfig =
        toml::from_str(&proxy_toml("127.0.0.1:9001", "127.0.0.1:9002")).unwrap();
    cfg.cache.routing_table_ttl_secs = 1;
    cfg.validate()
        .expect("positive routing_table_ttl_secs must validate");
}

#[test]
fn test_example_config_files_parse() {
    let (path, toml) = (
        "examples/config/manager.toml",
        include_str!("../../examples/config/manager.toml"),
    );
    toml::from_str::<ManagerConfig>(toml).unwrap_or_else(|err| panic!("{path} must parse: {err}"));

    for (path, toml) in [
        (
            "examples/config/proxy.toml",
            include_str!("../../examples/config/proxy.toml"),
        ),
        (
            "examples/multi-node/node-a/proxy.toml",
            include_str!("../../examples/multi-node/node-a/proxy.toml"),
        ),
        (
            "examples/multi-node/node-b/proxy.toml",
            include_str!("../../examples/multi-node/node-b/proxy.toml"),
        ),
    ] {
        toml::from_str::<ProxyConfig>(toml)
            .unwrap_or_else(|err| panic!("{path} must parse: {err}"));
    }

    // Engine configs reference wasm/schema artifacts that may not exist before build recipes run,
    // so check parse and structural invariants without EngineConfig::validate().
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

    for (path, toml) in [
        (
            "examples/config/engine.toml",
            include_str!("../../examples/config/engine.toml"),
        ),
        (
            "examples/ecommerce/engine-client.toml",
            include_str!("../../examples/ecommerce/engine-client.toml"),
        ),
        (
            "examples/ecommerce/engine-inventory-1.toml",
            include_str!("../../examples/ecommerce/engine-inventory-1.toml"),
        ),
        (
            "examples/ecommerce/engine-inventory-2.toml",
            include_str!("../../examples/ecommerce/engine-inventory-2.toml"),
        ),
        (
            "examples/codegen/engine.toml",
            include_str!("../../examples/codegen/engine.toml"),
        ),
        (
            "examples/stockmarket/engine-exchange.toml",
            include_str!("../../examples/stockmarket/engine-exchange.toml"),
        ),
        (
            "examples/stockmarket/engine-ledger.toml",
            include_str!("../../examples/stockmarket/engine-ledger.toml"),
        ),
        (
            "examples/stockmarket/engine-simulator.toml",
            include_str!("../../examples/stockmarket/engine-simulator.toml"),
        ),
        (
            "examples/multi-node/node-a/engine-1.toml",
            include_str!("../../examples/multi-node/node-a/engine-1.toml"),
        ),
        (
            "examples/multi-node/node-a/engine-2.toml",
            include_str!("../../examples/multi-node/node-a/engine-2.toml"),
        ),
        (
            "examples/multi-node/node-b/engine-1.toml",
            include_str!("../../examples/multi-node/node-b/engine-1.toml"),
        ),
    ] {
        let raw: EngineRaw =
            toml::from_str(toml).unwrap_or_else(|err| panic!("{path} must parse: {err}"));
        assert!(!raw.listen_address.is_empty(), "{path} listen_address");
        assert!(
            !raw.node.proxy_address.is_empty(),
            "{path} node.proxy_address"
        );
        assert!(
            !raw.node.control_address.is_empty(),
            "{path} node.control_address"
        );
        if !path.starts_with("examples/multi-node/") {
            assert!(!raw.modules.is_empty(), "{path} modules");
        }
    }
}

#[derive(serde::Deserialize)]
struct BuildManifestRaw {
    #[serde(rename = "module", default)]
    modules: Vec<BuildModuleRaw>,
}

#[derive(serde::Deserialize)]
struct BuildModuleRaw {
    name: String,
    proto_path: String,
    schema_path: String,
    cargo_dir: String,
    wasm_path: String,
}

#[test]
fn test_test_guest_build_manifest_is_explicit_and_valid() {
    let manifest: BuildManifestRaw = toml::from_str(include_str!("../guests/build.toml"))
        .expect("test guest build manifest must parse");
    assert_eq!(manifest.modules.len(), 5);
    let names: BTreeSet<String> = manifest.modules.iter().map(|m| m.name.clone()).collect();
    assert_eq!(
        names,
        BTreeSet::from([
            "db-guest".to_string(),
            "tracing-guest".to_string(),
            "blobstore-guest".to_string(),
            "http-guest".to_string(),
            "llm-guest".to_string(),
        ])
    );
    for module in &manifest.modules {
        assert!(
            workspace_path(&module.proto_path).exists(),
            "{} proto_path",
            module.name
        );
        assert!(
            module.schema_path.ends_with(".binpb"),
            "{} schema_path",
            module.name
        );
        assert!(
            workspace_path(&module.cargo_dir)
                .join("Cargo.toml")
                .exists(),
            "{} cargo_dir",
            module.name
        );
        assert!(
            module
                .wasm_path
                .starts_with(&format!("{}/target/wasm32-wasip2/debug/", module.cargo_dir)),
            "{} wasm_path",
            module.name
        );
    }
}

#[test]
fn test_example_build_metadata_matches_engine_configs() {
    for (group, configs, expected_names) in [
        (
            "ecommerce",
            &[
                include_str!("../../examples/ecommerce/engine-client.toml"),
                include_str!("../../examples/ecommerce/engine-inventory-1.toml"),
                include_str!("../../examples/ecommerce/engine-inventory-2.toml"),
            ][..],
            BTreeSet::from(["client".to_string(), "inventory".to_string()]),
        ),
        (
            "stockmarket",
            &[
                include_str!("../../examples/stockmarket/engine-exchange.toml"),
                include_str!("../../examples/stockmarket/engine-ledger.toml"),
                include_str!("../../examples/stockmarket/engine-simulator.toml"),
            ][..],
            BTreeSet::from([
                "exchange".to_string(),
                "ledger".to_string(),
                "simulator".to_string(),
            ]),
        ),
        (
            "codegen",
            &[include_str!("../../examples/codegen/engine.toml")][..],
            BTreeSet::from([
                "collector".to_string(),
                "agent".to_string(),
                "coordinator".to_string(),
                "worker".to_string(),
            ]),
        ),
    ] {
        let modules = unique_example_modules(configs);
        let names: BTreeSet<String> = modules.values().map(|m| m.name.clone()).collect();
        assert_eq!(names, expected_names, "{group} unique module names");
        if group == "stockmarket" {
            assert!(modules
                .values()
                .all(|m| !m.wasm_path.contains("examples/stockmarket/schemas/ledger")));
        }
        for module in modules.values() {
            if let Some(schema_path) = &module.schema_path {
                let proto_path = schema_path.replace(".binpb", ".proto");
                assert!(workspace_path(&proto_path).exists(), "{group} {proto_path}");
            }
            let cargo_dir = module
                .wasm_path
                .split("/target/wasm32-wasip2/")
                .next()
                .expect("wasm_path must contain target directory");
            assert!(
                workspace_path(cargo_dir).join("Cargo.toml").exists(),
                "{group} {cargo_dir}/Cargo.toml"
            );
        }
    }
}

#[test]
fn test_engine_schema_path_first_occurrence_required() {
    let wasm_a = write_temp_file("schema-first-required-a", "wasm", b"wasm");
    let wasm_b = write_temp_file("schema-first-required-b", "wasm", b"wasm");
    let schema = write_temp_file("schema-first-required", "binpb", b"schema");
    let cfg: EngineConfig = toml::from_str(&engine_toml_with_modules(&format!(
        r#"
          [[module]]
          name = "inventory"
          namespace = "store"
          version = "1.0.0"
          wasm_path = "{}"

          [[module]]
          name = "inventory"
          namespace = "store"
          version = "1.0.0"
          wasm_path = "{}"
          schema_path = "{}"
        "#,
        wasm_a.display(),
        wasm_b.display(),
        schema.display()
    )))
    .unwrap();
    let err = cfg
        .validate()
        .expect_err("first duplicate occurrence without schema must fail");
    assert!(
        format!("{err:#}").contains("schema_path is required for first occurrence"),
        "unexpected validation error: {err:#}"
    );
    let _ = fs::remove_file(wasm_a);
    let _ = fs::remove_file(wasm_b);
    let _ = fs::remove_file(schema);
}

#[test]
fn test_engine_schema_path_duplicate_may_omit_after_first() {
    let wasm_a = write_temp_file("schema-dup-omit-a", "wasm", b"wasm");
    let wasm_b = write_temp_file("schema-dup-omit-b", "wasm", b"wasm");
    let schema = write_temp_file("schema-dup-omit", "binpb", b"schema");
    let cfg: EngineConfig = toml::from_str(&engine_toml_with_modules(&format!(
        r#"
          [[module]]
          name = "inventory"
          namespace = "store"
          version = "1.0.0"
          wasm_path = "{}"
          schema_path = "{}"

          [[module]]
          name = "inventory"
          namespace = "store"
          version = "1.0.0"
          wasm_path = "{}"
        "#,
        wasm_a.display(),
        schema.display(),
        wasm_b.display()
    )))
    .unwrap();
    cfg.validate()
        .expect("duplicate module may omit schema_path after first occurrence");
    let _ = fs::remove_file(wasm_a);
    let _ = fs::remove_file(wasm_b);
    let _ = fs::remove_file(schema);
}

#[test]
fn test_engine_schema_path_empty_fails() {
    let wasm = write_temp_file("schema-empty", "wasm", b"wasm");
    let cfg: EngineConfig = toml::from_str(&engine_toml_with_modules(&format!(
        r#"
          [[module]]
          name = "inventory"
          namespace = "store"
          version = "1.0.0"
          wasm_path = "{}"
          schema_path = ""
        "#,
        wasm.display()
    )))
    .unwrap();
    let err = cfg
        .validate()
        .expect_err("empty schema_path must fail validation");
    assert!(
        format!("{err:#}").contains("must not be empty"),
        "unexpected validation error: {err:#}"
    );
    let _ = fs::remove_file(wasm);
}

#[test]
fn test_engine_schema_path_existing_first_occurrence_passes() {
    let wasm = write_temp_file("schema-existing", "wasm", b"wasm");
    let schema = write_temp_file("schema-existing", "binpb", b"schema");
    let cfg: EngineConfig = toml::from_str(&engine_toml_with_modules(&format!(
        r#"
          [[module]]
          name = "inventory"
          namespace = "store"
          version = "1.0.0"
          wasm_path = "{}"
          schema_path = "{}"
        "#,
        wasm.display(),
        schema.display()
    )))
    .unwrap();
    cfg.validate()
        .expect("existing first-occurrence schema_path must validate");
    let _ = fs::remove_file(wasm);
    let _ = fs::remove_file(schema);
}

#[test]
fn test_engine_schema_path_present_missing_file_fails() {
    let wasm = write_temp_file("schema-missing", "wasm", b"wasm");
    let missing_schema = unique_temp_path("schema-missing", "binpb");
    let cfg: EngineConfig = toml::from_str(&engine_toml_with_modules(&format!(
        r#"
          [[module]]
          name = "inventory"
          namespace = "store"
          version = "1.0.0"
          wasm_path = "{}"
          schema_path = "{}"
        "#,
        wasm.display(),
        missing_schema.display()
    )))
    .unwrap();
    let err = cfg
        .validate()
        .expect_err("missing schema_path file must fail validation");
    assert!(
        format!("{err:#}").contains("schema_path not found"),
        "unexpected validation error: {err:#}"
    );
    let _ = fs::remove_file(wasm);
}

#[test]
fn test_example_engine_configs_validate_when_artifacts_are_built() {
    for (path, toml) in example_engine_configs() {
        let cfg: EngineConfig = toml::from_str(toml).unwrap_or_else(|err| panic!("{path}: {err}"));
        let artifacts_exist = cfg.modules.iter().all(|module| {
            Path::new(&module.wasm_path).exists()
                && module
                    .schema_path
                    .as_ref()
                    .is_none_or(|schema_path| Path::new(schema_path).exists())
        });
        if artifacts_exist {
            cfg.validate()
                .unwrap_or_else(|err| panic!("{path} must validate when artifacts exist: {err:#}"));
        }
    }
}

fn unique_example_modules(configs: &[&str]) -> BTreeMap<String, wr_engine::config::ModuleConfig> {
    let mut modules: BTreeMap<String, wr_engine::config::ModuleConfig> = BTreeMap::new();
    for toml in configs {
        let cfg: EngineConfig = toml::from_str(toml).expect("example engine config must parse");
        for module in cfg.modules {
            let wasm_path = module.wasm_path.clone();
            match modules.get_mut(&wasm_path) {
                Some(existing) => {
                    if existing.schema_path.is_none() && module.schema_path.is_some() {
                        existing.schema_path = module.schema_path.clone();
                    }
                }
                None => {
                    modules.insert(wasm_path, module);
                }
            }
        }
    }
    modules
}

fn example_engine_configs() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "examples/ecommerce/engine-client.toml",
            include_str!("../../examples/ecommerce/engine-client.toml"),
        ),
        (
            "examples/ecommerce/engine-inventory-1.toml",
            include_str!("../../examples/ecommerce/engine-inventory-1.toml"),
        ),
        (
            "examples/ecommerce/engine-inventory-2.toml",
            include_str!("../../examples/ecommerce/engine-inventory-2.toml"),
        ),
        (
            "examples/stockmarket/engine-exchange.toml",
            include_str!("../../examples/stockmarket/engine-exchange.toml"),
        ),
        (
            "examples/stockmarket/engine-ledger.toml",
            include_str!("../../examples/stockmarket/engine-ledger.toml"),
        ),
        (
            "examples/stockmarket/engine-simulator.toml",
            include_str!("../../examples/stockmarket/engine-simulator.toml"),
        ),
        (
            "examples/codegen/engine.toml",
            include_str!("../../examples/codegen/engine.toml"),
        ),
    ]
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

fn engine_toml_with_modules(module_blocks: &str) -> String {
    format!(
        r#"
          listen_address = "127.0.0.1:9100"

          [node]
          proxy_address   = "http://127.0.0.1:9001"
          control_address = "http://127.0.0.1:9002"
          peer_port       = 9443

          [node.tls]
          cert_path    = "certs/node.crt"
          key_path     = "certs/node.key"
          ca_cert_path = "certs/ca.crt"

          {module_blocks}
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
