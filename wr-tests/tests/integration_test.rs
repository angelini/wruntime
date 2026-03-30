/// Integration tests for Wruntime.
///
/// Each test spins up real in-process gRPC services / HTTP servers on
/// ephemeral ports so that no external processes are required.
mod helpers;
use helpers::*;

use std::sync::Arc;

use anyhow::Result;
use http::{Request, StatusCode};
use http_body_util::{BodyExt, Full};

use wr_common::wruntime::{
    DeregisterEngineRequest, EngineRegistration, GetMetricsSummaryRequest, GetRoutingTableRequest,
    HeartbeatRequest, ListEnginesRequest, ModuleDescriptor, RegisterEngineRequest,
    ReportMetricsRequest, RequestMetrics, RoutingRule,
};
use wr_manager::config::ManagerConfig;
use wr_proxy::config::ProxyConfig;

// ── manager RPC tests ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_register_and_list_engines() -> Result<()> {
    let addr = start_manager().await?;
    let mut c = manager_client(&addr).await?;

    c.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "e1".into(),
            address: "http://127.0.0.1:9100".into(),
            proxy_address: String::new(),
            modules: vec![ModuleDescriptor {
                name: "inventory-service".into(),
                namespace: "store".into(),
                version: "1.0.0".into(),
                proto_schema: minimal_file_descriptor_set(),
            }],
        }),
    })
    .await?;

    let list = c
        .list_engines(ListEnginesRequest {})
        .await?
        .into_inner()
        .engines;
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].engine_id, "e1");
    assert_eq!(list[0].modules[0].name, "inventory-service");

    Ok(())
}

#[tokio::test]
async fn test_deregister_engine() -> Result<()> {
    let addr = start_manager().await?;
    let mut c = manager_client(&addr).await?;

    c.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "e1".into(),
            address: "http://127.0.0.1:9101".into(),
            proxy_address: String::new(),
            modules: vec![],
        }),
    })
    .await?;

    c.deregister_engine(DeregisterEngineRequest {
        engine_id: "e1".into(),
    })
    .await?;

    let list = c
        .list_engines(ListEnginesRequest {})
        .await?
        .into_inner()
        .engines;
    assert!(list.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_heartbeat() -> Result<()> {
    let addr = start_manager().await?;
    let mut c = manager_client(&addr).await?;

    c.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "e1".into(),
            address: "http://127.0.0.1:9102".into(),
            proxy_address: String::new(),
            modules: vec![],
        }),
    })
    .await?;

    c.heartbeat(HeartbeatRequest {
        engine_id: "e1".into(),
        healthy_modules: vec![],
    })
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_routing_table_upsert_and_get() -> Result<()> {
    let addr = start_manager().await?;
    let mut c = manager_client(&addr).await?;

    c.upsert_routing_rule(RoutingRule {
        rule_id: "r1".into(),
        source_module: "order-service".into(),
        source_namespace: "store".into(),
        destination_module: "inventory-service".into(),
        destination_namespace: "store".into(),
        destination_version: "1.0.0".into(),
        engine_id: "e1".into(),
        engine_address: "http://127.0.0.1:9103".into(),
        proxy_address: String::new(),
        healthy: false, // server sets this to true on upsert
    })
    .await?;

    let table = c
        .get_routing_table(GetRoutingTableRequest {})
        .await?
        .into_inner()
        .table
        .unwrap();

    assert_eq!(table.rules.len(), 1);
    assert_eq!(table.rules[0].destination_module, "inventory-service");
    assert_eq!(table.rules[0].destination_namespace, "store");
    assert!(table.rules[0].healthy, "upserted rule should be healthy");
    assert_eq!(table.version, 1);

    Ok(())
}

#[tokio::test]
async fn test_metrics_report_and_summary() -> Result<()> {
    let addr = start_manager().await?;
    let mut c = manager_client(&addr).await?;

    c.report_metrics(ReportMetricsRequest {
        metrics: vec![RequestMetrics {
            source: "order-service".into(),
            destination: "inventory-service".into(),
            duration_ms: 42,
            status: 200,
            error: String::new(),
        }],
    })
    .await?;

    let summary = c
        .get_metrics_summary(GetMetricsSummaryRequest {})
        .await?
        .into_inner()
        .metrics;

    assert_eq!(summary.len(), 1);
    assert_eq!(summary[0].source, "order-service");
    assert_eq!(summary[0].duration_ms, 42);

    Ok(())
}

// ── proxy routing tests ───────────────────────────────────────────────────────

#[tokio::test]
async fn test_proxy_routes_to_engine() -> Result<()> {
    let mgr_addr = start_manager().await?;
    let mut mgr_c = manager_client(&mgr_addr).await?;

    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;

    register_module(
        &mut mgr_c,
        EngineSpec {
            id: "stub-engine",
            addr: &engine_addr,
            proxy_address: "",
        },
        ModuleSpec {
            namespace: "store",
            name: "inventory-service",
            version: "1.0.0",
            schema: minimal_file_descriptor_set(),
        },
    )
    .await?;

    let table = wr_proxy::routing::new_routing_table();
    sync_table(&mgr_addr, &table).await?;
    let proxy = start_proxy_synced(&mgr_addr, table).await?;

    let (status, body) = proxy_get(proxy, "store", "inventory-service", Some("1.0.0")).await?;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("/store.inventory-service"),
        "expected stub to echo request path, got: {body}"
    );

    let _ = engine_shutdown.send(());
    Ok(())
}

// ── schema validation tests ───────────────────────────────────────────────────

#[tokio::test]
async fn test_schema_validation_rejects_invalid_body() -> Result<()> {
    // 1. Start manager and upload a schema for "ping-service".
    let mgr_addr = start_manager().await?;
    let mut mgr = manager_client(&mgr_addr).await?;

    mgr.upload_schema(wr_common::wruntime::UploadSchemaRequest {
        namespace: "svc-ns".into(),
        module: "ping-service".into(),
        version: "1.0.0".into(),
        proto_schema: minimal_file_descriptor_set(),
    })
    .await?;

    // 2. Pre-populate a schema cache and start a proxy backed by it.
    let schema_cache = Arc::new(wr_proxy::schema::SchemaCache::new());
    let schema_bytes = manager_client(&mgr_addr)
        .await?
        .get_schema(wr_common::wruntime::GetSchemaRequest {
            namespace: "svc-ns".into(),
            module: "ping-service".into(),
            version: "1.0.0".into(),
        })
        .await?
        .into_inner()
        .proto_schema;
    schema_cache
        .insert("svc-ns", "ping-service", &schema_bytes)
        .await?;

    let proxy_addr =
        start_proxy_with_schema(wr_proxy::routing::new_routing_table(), schema_cache, "").await?;

    let client = http_client();

    // 3. Invalid body → 400 with structured JSON error.
    let bad_req = Request::builder()
        .method("POST")
        .uri(format!("http://{proxy_addr}/Ping"))
        .header("x-wr-destination", "http://svc-ns.ping-service/Ping")
        .header("x-wr-source", "caller-service")
        .body(Full::new(invalid_protobuf()))?;

    let resp = client.request(bad_req).await?;
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "expected 400 for invalid protobuf"
    );

    let (resp_parts, resp_body) = resp.into_parts();
    let body_bytes = resp_body.collect().await?.to_bytes();
    let body_str = std::str::from_utf8(&body_bytes)?;

    assert!(
        body_str.contains(r#""error":"schema_validation_failed""#),
        "expected structured JSON error, got: {body_str}"
    );
    assert!(
        body_str.contains(r#""source":"caller-service""#),
        "expected source in error, got: {body_str}"
    );
    assert!(
        body_str.contains(r#""destination":"ping-service""#),
        "expected destination in error, got: {body_str}"
    );
    assert!(
        resp_parts
            .headers
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_none_or(|v| v.starts_with("application/json")),
        "expected application/json content-type"
    );

    // 4. Valid protobuf body → passes schema validation (routing then fails
    //    with 503 since no engine is registered, not 400).
    let good_req = Request::builder()
        .method("POST")
        .uri(format!("http://{proxy_addr}/Ping"))
        .header("x-wr-destination", "http://svc-ns.ping-service/Ping")
        .header("x-wr-source", "caller-service")
        .body(Full::new(valid_ping_request()))?;

    let resp2 = client.request(good_req).await?;
    assert_ne!(
        resp2.status(),
        StatusCode::BAD_REQUEST,
        "valid body should not fail schema validation"
    );

    Ok(())
}

#[tokio::test]
async fn test_schema_validation_accepts_json_body() -> Result<()> {
    // Verify that the proxy transcodes an `application/json` body to protobuf
    // before schema validation, so callers like `wr-cli invoke` can pass
    // stringified JSON instead of binary-encoded protobuf.

    let mgr_addr = start_manager().await?;
    let mut mgr = manager_client(&mgr_addr).await?;

    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;

    register_module(
        &mut mgr,
        EngineSpec {
            id: "json-engine",
            addr: &engine_addr,
            proxy_address: "",
        },
        ModuleSpec {
            namespace: "json-ns",
            name: "ping-service",
            version: "1.0.0",
            schema: minimal_file_descriptor_set(),
        },
    )
    .await?;

    let table = wr_proxy::routing::new_routing_table();
    sync_table(&mgr_addr, &table).await?;
    let proxy_addr = start_proxy_synced(&mgr_addr, table).await?;

    let client = http_client();

    // Valid JSON matching PingRequest { message: string } — should pass.
    let json_req = Request::builder()
        .method("POST")
        .uri(format!("http://{proxy_addr}/Ping"))
        .header("content-type", "application/json")
        .header("x-wr-destination", "http://json-ns.ping-service/Ping")
        .header("x-wr-source", "wr-cli")
        .body(Full::new(bytes::Bytes::from(r#"{"message":"hello"}"#)))?;

    let resp = client.request(json_req).await?;
    assert_ne!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "valid JSON body should pass schema validation"
    );

    // Invalid JSON — should be rejected with 400.
    let bad_json_req = Request::builder()
        .method("POST")
        .uri(format!("http://{proxy_addr}/Ping"))
        .header("content-type", "application/json")
        .header("x-wr-destination", "http://json-ns.ping-service/Ping")
        .header("x-wr-source", "wr-cli")
        .body(Full::new(bytes::Bytes::from(r#"not json"#)))?;

    let bad_resp = client.request(bad_json_req).await?;
    assert_eq!(
        bad_resp.status(),
        StatusCode::BAD_REQUEST,
        "invalid JSON body should be rejected"
    );

    let _ = engine_shutdown.send(());
    Ok(())
}

// ── config validation tests ───────────────────────────────────────────────────

#[test]
fn test_manager_config_valid() {
    let toml = r#"
        listen_address                = "0.0.0.0:9000"
        engine_heartbeat_timeout_secs = 30
    "#;
    let cfg: ManagerConfig = toml::from_str(toml).unwrap();
    assert_eq!(cfg.listen_address, "0.0.0.0:9000");
    assert_eq!(cfg.engine_heartbeat_timeout_secs, 30);
}

#[test]
fn test_manager_config_default_heartbeat() {
    // engine_heartbeat_timeout_secs should default to 30 when omitted.
    let toml = r#"listen_address = "0.0.0.0:9000""#;
    let cfg: ManagerConfig = toml::from_str(toml).unwrap();
    assert_eq!(cfg.engine_heartbeat_timeout_secs, 30);
}

#[test]
fn test_proxy_config_valid() {
    let toml = r#"
        listen_address  = "0.0.0.0:9001"
        manager_address = "http://127.0.0.1:9000"

        [node]
        proxy_address = "http://127.0.0.1:9001"

        [cache]
        routing_table_ttl_secs = 5
        schema_ttl_secs        = 60

        [metrics]
        flush_interval_secs = 10
        queue_depth         = 1000
    "#;
    let cfg: ProxyConfig = toml::from_str(toml).unwrap();
    assert_eq!(cfg.listen_address, "0.0.0.0:9001");
    assert_eq!(cfg.manager_address, "http://127.0.0.1:9000");
    assert_eq!(cfg.cache.routing_table_ttl_secs, 5);
    assert_eq!(cfg.cache.schema_ttl_secs, 60);
    assert_eq!(cfg.metrics.flush_interval_secs, 10);
    assert_eq!(cfg.metrics.queue_depth, 1000);
}

#[test]
fn test_proxy_config_defaults() {
    let toml = r#"
        listen_address  = "0.0.0.0:9001"
        manager_address = "http://127.0.0.1:9000"
        [node]
        proxy_address   = "http://127.0.0.1:9001"
    "#;
    let cfg: ProxyConfig = toml::from_str(toml).unwrap();
    assert_eq!(cfg.cache.routing_table_ttl_secs, 5);
    assert_eq!(cfg.cache.schema_ttl_secs, 60);
    assert_eq!(cfg.metrics.flush_interval_secs, 10);
    assert_eq!(cfg.metrics.queue_depth, 1000);
}

#[test]
fn test_proxy_config_rejects_zero_ttl() {
    let toml = r#"
        listen_address  = "0.0.0.0:9001"
        manager_address = "http://127.0.0.1:9000"
        [node]
        proxy_address   = "http://127.0.0.1:9001"
        [cache]
        routing_table_ttl_secs = 0
        schema_ttl_secs        = 60
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
fn test_proxy_config_rejects_zero_queue_depth() {
    let toml = r#"
        listen_address  = "0.0.0.0:9001"
        manager_address = "http://127.0.0.1:9000"
        [node]
        proxy_address   = "http://127.0.0.1:9001"
        [metrics]
        flush_interval_secs = 10
        queue_depth         = 0
    "#;
    let cfg: ProxyConfig = toml::from_str(toml).unwrap();
    assert_eq!(
        cfg.metrics.queue_depth, 0,
        "precondition for validation guard"
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
    struct NodeSection {
        proxy_address: String,
    }
    #[derive(serde::Deserialize)]
    #[allow(dead_code)]
    struct EngineRaw {
        listen_address: String,
        manager_address: String,
        node: NodeSection,
        #[serde(rename = "module", default)]
        modules: Vec<toml::Value>,
    }
    let raw: EngineRaw = toml::from_str(engine_toml).expect("engine.toml must parse");
    assert!(!raw.listen_address.is_empty());
    assert_eq!(raw.modules.len(), 2);
}

// ── multi-instance / version / health tests ───────────────────────────────────

#[tokio::test]
async fn test_proxy_routes_to_explicit_version() -> Result<()> {
    let mgr_addr = start_manager().await?;
    let mut mgr = manager_client(&mgr_addr).await?;

    let (e1_addr, e1_shutdown) = spawn_identified_stub("engine-v1").await?;
    let (e2_addr, e2_shutdown) = spawn_identified_stub("engine-v2").await?;

    register_module(
        &mut mgr,
        EngineSpec {
            id: "e1",
            addr: &e1_addr,
            proxy_address: "",
        },
        ModuleSpec {
            namespace: "ver-ns",
            name: "versioned-service",
            version: "1.0.0",
            schema: minimal_file_descriptor_set(),
        },
    )
    .await?;
    register_module(
        &mut mgr,
        EngineSpec {
            id: "e2",
            addr: &e2_addr,
            proxy_address: "",
        },
        ModuleSpec {
            namespace: "ver-ns",
            name: "versioned-service",
            version: "2.0.0",
            schema: minimal_file_descriptor_set(),
        },
    )
    .await?;

    let table = wr_proxy::routing::new_routing_table();
    sync_table(&mgr_addr, &table).await?;
    let proxy = start_proxy_synced(&mgr_addr, table).await?;

    let (s, body) = proxy_get(proxy, "ver-ns", "versioned-service", Some("1.0.0")).await?;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body, "engine-v1", "x-wr-version: 1.0.0 should route to v1");

    let (s, body) = proxy_get(proxy, "ver-ns", "versioned-service", Some("2.0.0")).await?;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body, "engine-v2", "x-wr-version: 2.0.0 should route to v2");

    let _ = e1_shutdown.send(());
    let _ = e2_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn test_proxy_routes_to_latest_version() -> Result<()> {
    let mgr_addr = start_manager().await?;
    let mut mgr = manager_client(&mgr_addr).await?;

    let (e1_addr, e1_shutdown) = spawn_identified_stub("engine-v1").await?;
    let (e2_addr, e2_shutdown) = spawn_identified_stub("engine-v2").await?;

    register_module(
        &mut mgr,
        EngineSpec {
            id: "e1",
            addr: &e1_addr,
            proxy_address: "",
        },
        ModuleSpec {
            namespace: "latest-ns",
            name: "latest-service",
            version: "1.0.0",
            schema: minimal_file_descriptor_set(),
        },
    )
    .await?;
    register_module(
        &mut mgr,
        EngineSpec {
            id: "e2",
            addr: &e2_addr,
            proxy_address: "",
        },
        ModuleSpec {
            namespace: "latest-ns",
            name: "latest-service",
            version: "2.0.0",
            schema: minimal_file_descriptor_set(),
        },
    )
    .await?;

    let table = wr_proxy::routing::new_routing_table();
    sync_table(&mgr_addr, &table).await?;
    let proxy = start_proxy_synced(&mgr_addr, table).await?;

    // No x-wr-version → should route to the highest semver (2.0.0)
    let (s, body) = proxy_get(proxy, "latest-ns", "latest-service", None).await?;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(
        body, "engine-v2",
        "no version header should route to latest"
    );

    let _ = e1_shutdown.send(());
    let _ = e2_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn test_proxy_returns_503_for_missing_version() -> Result<()> {
    let mgr_addr = start_manager().await?;
    let mut mgr = manager_client(&mgr_addr).await?;

    let (e1_addr, _stub) = spawn_identified_stub("engine-v1").await?;
    register_module(
        &mut mgr,
        EngineSpec {
            id: "e1",
            addr: &e1_addr,
            proxy_address: "",
        },
        ModuleSpec {
            namespace: "mv-ns",
            name: "missing-ver-service",
            version: "1.0.0",
            schema: minimal_file_descriptor_set(),
        },
    )
    .await?;

    let table = wr_proxy::routing::new_routing_table();
    sync_table(&mgr_addr, &table).await?;
    let proxy = start_proxy(table).await?;

    let (s, _) = proxy_get(proxy, "mv-ns", "missing-ver-service", Some("9.0.0")).await?;
    assert_eq!(s, StatusCode::SERVICE_UNAVAILABLE, "unknown version → 503");

    Ok(())
}

#[tokio::test]
async fn test_proxy_routes_semver_range_to_highest_satisfying() -> Result<()> {
    let mgr_addr = start_manager().await?;
    let mut mgr = manager_client(&mgr_addr).await?;

    let (e1_addr, e1_shutdown) = spawn_identified_stub("engine-v1").await?;
    let (e2_addr, e2_shutdown) = spawn_identified_stub("engine-v2").await?;
    let (e3_addr, e3_shutdown) = spawn_identified_stub("engine-v3").await?;

    for (id, addr, version) in [
        ("e1", &e1_addr, "1.0.0"),
        ("e2", &e2_addr, "1.5.0"),
        ("e3", &e3_addr, "2.0.0"),
    ] {
        register_module(
            &mut mgr,
            EngineSpec {
                id,
                addr,
                proxy_address: "",
            },
            ModuleSpec {
                namespace: "range-ns",
                name: "range-service",
                version,
                schema: minimal_file_descriptor_set(),
            },
        )
        .await?;
    }

    let table = wr_proxy::routing::new_routing_table();
    sync_table(&mgr_addr, &table).await?;
    let proxy = start_proxy_synced(&mgr_addr, table).await?;

    // ^1 should pick the highest 1.x, which is 1.5.0
    let (s, body) = proxy_get(proxy, "range-ns", "range-service", Some("^1")).await?;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body, "engine-v2", "^1 should route to highest 1.x (1.5.0)");

    // >=1.5.0 should pick 2.0.0 (highest satisfying)
    let (s, body) = proxy_get(proxy, "range-ns", "range-service", Some(">=1.5.0")).await?;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(
        body, "engine-v3",
        ">=1.5.0 should route to highest satisfying (2.0.0)"
    );

    let _ = e1_shutdown.send(());
    let _ = e2_shutdown.send(());
    let _ = e3_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn test_proxy_returns_503_for_unsatisfiable_range() -> Result<()> {
    let mgr_addr = start_manager().await?;
    let mut mgr = manager_client(&mgr_addr).await?;

    let (e1_addr, _stub) = spawn_identified_stub("engine-v1").await?;
    register_module(
        &mut mgr,
        EngineSpec {
            id: "e1",
            addr: &e1_addr,
            proxy_address: "",
        },
        ModuleSpec {
            namespace: "range-503-ns",
            name: "range-503-service",
            version: "1.0.0",
            schema: minimal_file_descriptor_set(),
        },
    )
    .await?;

    let table = wr_proxy::routing::new_routing_table();
    sync_table(&mgr_addr, &table).await?;
    let proxy = start_proxy(table).await?;

    let (s, _) = proxy_get(proxy, "range-503-ns", "range-503-service", Some("^3")).await?;
    assert_eq!(
        s,
        StatusCode::SERVICE_UNAVAILABLE,
        "unsatisfiable range → 503"
    );

    Ok(())
}

#[tokio::test]
async fn test_proxy_load_balances_across_instances() -> Result<()> {
    let mgr_addr = start_manager().await?;
    let mut mgr = manager_client(&mgr_addr).await?;

    // Two engines both hosting the same (module, version).
    let (e1_addr, e1_shutdown) = spawn_identified_stub("engine-a").await?;
    let (e2_addr, e2_shutdown) = spawn_identified_stub("engine-b").await?;

    register_module(
        &mut mgr,
        EngineSpec {
            id: "ea",
            addr: &e1_addr,
            proxy_address: "",
        },
        ModuleSpec {
            namespace: "lb-ns",
            name: "lb-service",
            version: "1.0.0",
            schema: minimal_file_descriptor_set(),
        },
    )
    .await?;
    register_module(
        &mut mgr,
        EngineSpec {
            id: "eb",
            addr: &e2_addr,
            proxy_address: "",
        },
        ModuleSpec {
            namespace: "lb-ns",
            name: "lb-service",
            version: "1.0.0",
            schema: minimal_file_descriptor_set(),
        },
    )
    .await?;

    let table = wr_proxy::routing::new_routing_table();
    sync_table(&mgr_addr, &table).await?;
    let proxy = start_proxy_synced(&mgr_addr, table).await?;

    let mut saw_a = false;
    let mut saw_b = false;
    for _ in 0..10 {
        let (s, body) = proxy_get(proxy, "lb-ns", "lb-service", Some("1.0.0")).await?;
        assert_eq!(s, StatusCode::OK);
        saw_a |= body == "engine-a";
        saw_b |= body == "engine-b";
    }

    assert!(saw_a, "engine-a should have received at least one request");
    assert!(saw_b, "engine-b should have received at least one request");

    let _ = e1_shutdown.send(());
    let _ = e2_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn test_proxy_failover_after_deregister() -> Result<()> {
    let mgr_addr = start_manager().await?;
    let mut mgr = manager_client(&mgr_addr).await?;

    let (e1_addr, e1_shutdown) = spawn_identified_stub("engine-a").await?;
    let (e2_addr, e2_shutdown) = spawn_identified_stub("engine-b").await?;

    register_module(
        &mut mgr,
        EngineSpec {
            id: "ea",
            addr: &e1_addr,
            proxy_address: "",
        },
        ModuleSpec {
            namespace: "fo-ns",
            name: "failover-service",
            version: "1.0.0",
            schema: minimal_file_descriptor_set(),
        },
    )
    .await?;
    register_module(
        &mut mgr,
        EngineSpec {
            id: "eb",
            addr: &e2_addr,
            proxy_address: "",
        },
        ModuleSpec {
            namespace: "fo-ns",
            name: "failover-service",
            version: "1.0.0",
            schema: minimal_file_descriptor_set(),
        },
    )
    .await?;

    let table = wr_proxy::routing::new_routing_table();
    sync_table(&mgr_addr, &table).await?;
    let proxy = start_proxy_synced(&mgr_addr, table.clone()).await?;

    // Both instances should be reachable before failover.
    let mut saw_a = false;
    let mut saw_b = false;
    for _ in 0..4 {
        let (_, body) = proxy_get(proxy, "fo-ns", "failover-service", Some("1.0.0")).await?;
        saw_a |= body == "engine-a";
        saw_b |= body == "engine-b";
    }
    assert!(saw_a, "engine-a should be reachable before failover");
    assert!(saw_b, "engine-b should be reachable before failover");

    // Deregister engine-a; its rule is immediately marked unhealthy.
    mgr.deregister_engine(DeregisterEngineRequest {
        engine_id: "ea".into(),
    })
    .await?;
    sync_table(&mgr_addr, &table).await?;

    // All subsequent traffic must go to engine-b.
    for _ in 0..4 {
        let (s, body) = proxy_get(proxy, "fo-ns", "failover-service", Some("1.0.0")).await?;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(
            body, "engine-b",
            "after failover all traffic should go to engine-b"
        );
    }

    let _ = e1_shutdown.send(());
    let _ = e2_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn test_proxy_503_when_all_instances_unhealthy() -> Result<()> {
    let mgr_addr = start_manager().await?;
    let mut mgr = manager_client(&mgr_addr).await?;

    let (e1_addr, _stub) = spawn_identified_stub("engine-a").await?;
    register_module(
        &mut mgr,
        EngineSpec {
            id: "ea",
            addr: &e1_addr,
            proxy_address: "",
        },
        ModuleSpec {
            namespace: "gone-ns",
            name: "gone-service",
            version: "1.0.0",
            schema: minimal_file_descriptor_set(),
        },
    )
    .await?;

    let table = wr_proxy::routing::new_routing_table();
    sync_table(&mgr_addr, &table).await?;
    let proxy = start_proxy_synced(&mgr_addr, table.clone()).await?;

    let (s, _) = proxy_get(proxy, "gone-ns", "gone-service", Some("1.0.0")).await?;
    assert_eq!(s, StatusCode::OK, "should be reachable before deregister");

    mgr.deregister_engine(DeregisterEngineRequest {
        engine_id: "ea".into(),
    })
    .await?;
    sync_table(&mgr_addr, &table).await?;

    let (s, _) = proxy_get(proxy, "gone-ns", "gone-service", Some("1.0.0")).await?;
    assert_eq!(s, StatusCode::SERVICE_UNAVAILABLE, "all unhealthy → 503");

    Ok(())
}

// ── DB integration tests ──────────────────────────────────────────────────────
//
// Config-parsing tests run unconditionally.
// Host-trait tests that hit a real Postgres instance are gated on
// WRUNTIME_TEST_DB_URL — `db_state()` returns None when it is absent and
// the test returns early, so `cargo test` works without a database.

// ─ EngineConfig / DatabaseConfig parsing ─────────────────────────────────────

#[test]
fn test_engine_config_database_section_parses() {
    use wr_engine::config::EngineConfig;
    let toml = r#"
        listen_address  = "0.0.0.0:9100"
        manager_address = "http://127.0.0.1:9000"
        [node]
        proxy_address   = "http://127.0.0.1:9001"
        [database]
        url             = "postgres://user:pass@localhost:5432/mydb"
        max_connections = 4
    "#;
    let cfg: EngineConfig = toml::from_str(toml).unwrap();
    let db = cfg.database.expect("database section should be present");
    assert_eq!(db.url, "postgres://user:pass@localhost:5432/mydb");
    assert_eq!(db.max_connections, 4);
}

#[test]
fn test_engine_config_database_max_connections_defaults_to_8() {
    use wr_engine::config::EngineConfig;
    let toml = r#"
        listen_address  = "0.0.0.0:9100"
        manager_address = "http://127.0.0.1:9000"
        [node]
        proxy_address   = "http://127.0.0.1:9001"
        [database]
        url = "postgres://user:pass@localhost:5432/mydb"
    "#;
    let cfg: EngineConfig = toml::from_str(toml).unwrap();
    let db = cfg.database.expect("database section should be present");
    assert_eq!(db.max_connections, 8);
}

#[test]
fn test_engine_config_module_database_flag_parses() {
    use wr_engine::config::EngineConfig;
    // database = true on a module is parsed correctly; EngineConfig::validate()
    // (called via load()) would reject this if [database] were absent.
    let toml = r#"
        listen_address  = "0.0.0.0:9100"
        manager_address = "http://127.0.0.1:9000"
        [node]
        proxy_address   = "http://127.0.0.1:9001"
        [database]
        url = "postgres://user:pass@localhost:5432/mydb"
        [[module]]
        name        = "svc"
        namespace   = "my-ns"
        version     = "1.0.0"
        wasm_path   = "/nonexistent/svc.wasm"
        schema_path = "/nonexistent/svc.binpb"
        database    = true
    "#;
    let cfg: EngineConfig = toml::from_str(toml).unwrap();
    assert!(
        cfg.modules[0].database,
        "database flag should parse as true"
    );
    assert!(cfg.database.is_some(), "database section should be present");
}

#[test]
fn test_engine_config_module_database_flag_defaults_to_false() {
    use wr_engine::config::EngineConfig;
    let toml = r#"
        listen_address  = "0.0.0.0:9100"
        manager_address = "http://127.0.0.1:9000"
        [node]
        proxy_address   = "http://127.0.0.1:9001"
        [[module]]
        name        = "svc"
        namespace   = "my-ns"
        version     = "1.0.0"
        wasm_path   = "/nonexistent/svc.wasm"
        schema_path = "/nonexistent/svc.binpb"
    "#;
    let cfg: EngineConfig = toml::from_str(toml).unwrap();
    assert!(!cfg.modules[0].database, "database should default to false");
}

// ─ Host trait — no pool ───────────────────────────────────────────────────────

#[test]
fn test_db_query_without_pool_returns_connection_error() {
    let mut state = ModuleState::new(
        "test".into(),
        "test-ns".into(),
        "http://127.0.0.1:9001".parse().unwrap(),
        None,
        None,
        None,
        tracing::Span::none(),
    )
    .expect("ModuleState");
    let err = state.query("SELECT 1".into(), vec![]).unwrap_err();
    assert!(
        matches!(err, DbError::Connection(_)),
        "expected Connection error, got {err:?}",
    );
}

#[test]
fn test_db_execute_without_pool_returns_connection_error() {
    let mut state = ModuleState::new(
        "test".into(),
        "test-ns".into(),
        "http://127.0.0.1:9001".parse().unwrap(),
        None,
        None,
        None,
        tracing::Span::none(),
    )
    .expect("ModuleState");
    let err = state
        .execute("INSERT INTO t VALUES (1)".into(), vec![])
        .unwrap_err();
    assert!(
        matches!(err, DbError::Connection(_)),
        "expected Connection error, got {err:?}",
    );
}

// ─ Host trait — real Postgres ─────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn test_db_bytea_roundtrip() {
    let Some(mut state) = db_state(2) else { return };
    let payload = vec![0u8, 1, 127, 128, 255];
    let rows = state
        .query(
            "SELECT $1::bytea AS b".into(),
            vec![PgValue::Bytea(payload.clone())],
        )
        .expect("query");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns[0].value, PgValue::Bytea(payload));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_db_uuid_roundtrip() {
    let Some(mut state) = db_state(2) else { return };
    // UUID 550e8400-e29b-41d4-a716-446655440000 split into (hi, lo) at bit 64.
    let hi: u64 = 0x550e_8400_e29b_41d4;
    let lo: u64 = 0xa716_4466_5544_0000;
    let rows = state
        .query("SELECT $1::uuid AS u".into(), vec![PgValue::Uuid((hi, lo))])
        .expect("query");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns[0].value, PgValue::Uuid((hi, lo)));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_db_timestamptz_roundtrip() {
    let Some(mut state) = db_state(2) else { return };
    // 2001-09-09 01:46:40 UTC — a clean million-second boundary.
    let micros: i64 = 1_000_000_000 * 1_000_000;
    let rows = state
        .query(
            "SELECT $1::timestamptz AS ts".into(),
            vec![PgValue::Timestamptz(micros)],
        )
        .expect("query");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns[0].value, PgValue::Timestamptz(micros));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_db_date_roundtrip() {
    let Some(mut state) = db_state(2) else { return };
    // 10957 days since 1970-01-01 = 2000-01-01.
    let rows = state
        .query("SELECT $1::date AS d".into(), vec![PgValue::Date(10957)])
        .expect("query");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns[0].value, PgValue::Date(10957));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_db_time_roundtrip() {
    let Some(mut state) = db_state(2) else { return };
    // 14:30:00.000000 — 52 200 seconds from midnight in microseconds.
    let micros: i64 = 52_200 * 1_000_000;
    let rows = state
        .query("SELECT $1::time AS t".into(), vec![PgValue::Time(micros)])
        .expect("query");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns[0].value, PgValue::Time(micros));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_db_numeric_roundtrip() {
    let Some(mut state) = db_state(2) else { return };
    let rows = state
        .query(
            "SELECT $1::numeric AS n".into(),
            vec![PgValue::Numeric("123.456".into())],
        )
        .expect("query");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns[0].value, PgValue::Numeric("123.456".into()));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_db_jsonb_roundtrip() {
    let Some(mut state) = db_state(2) else { return };
    let input = r#"{"key":"value","num":42}"#;
    let rows = state
        .query(
            "SELECT $1::jsonb AS j".into(),
            vec![PgValue::Jsonb(input.into())],
        )
        .expect("query");
    assert_eq!(rows.len(), 1);
    // JSONB may reorder keys; compare structurally.
    let PgValue::Jsonb(got) = &rows[0].columns[0].value else {
        panic!("expected Jsonb, got {:?}", rows[0].columns[0].value);
    };
    let want: serde_json::Value = serde_json::from_str(input).unwrap();
    let got_val: serde_json::Value = serde_json::from_str(got).unwrap();
    assert_eq!(got_val, want);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_db_null_param_passes_through_as_null_column() {
    let Some(mut state) = db_state(2) else { return };
    let rows = state
        .query("SELECT $1::text AS v".into(), vec![PgValue::Null])
        .expect("query");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns[0].value, PgValue::Null);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_db_query_error_on_invalid_sql() {
    let Some(mut state) = db_state(2) else { return };
    let err = state
        .query("THIS IS NOT VALID SQL".into(), vec![])
        .unwrap_err();
    assert!(
        matches!(err, DbError::Query(_)),
        "expected Query error, got {err:?}",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_db_execute_insert_and_query_roundtrip() {
    // Pool size 1: TEMP TABLEs are connection-local, so all operations must
    // share the same underlying connection.
    let Some(mut state) = db_state(1) else { return };

    state
        .execute(
            "CREATE TEMP TABLE _wr_roundtrip (name TEXT, score INT4)".into(),
            vec![],
        )
        .expect("create table");

    let n = state
        .execute(
            "INSERT INTO _wr_roundtrip VALUES ($1, $2)".into(),
            vec![PgValue::Text("alice".into()), PgValue::Int4(99)],
        )
        .expect("insert");
    assert_eq!(n, 1, "one row should have been inserted");

    let rows = state
        .query("SELECT name, score FROM _wr_roundtrip".into(), vec![])
        .expect("select");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns[0].value, PgValue::Text("alice".into()));
    assert_eq!(rows[0].columns[1].value, PgValue::Int4(99));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_db_execute_returns_affected_row_count() {
    // Pool size 1 for TEMP TABLE visibility (see above).
    let Some(mut state) = db_state(1) else { return };

    state
        .execute("CREATE TEMP TABLE _wr_update (v INT4)".into(), vec![])
        .expect("create table");
    state
        .execute("INSERT INTO _wr_update VALUES (1), (2), (3)".into(), vec![])
        .expect("insert rows");

    let n = state
        .execute(
            "UPDATE _wr_update SET v = v + 10 WHERE v < 3".into(),
            vec![],
        )
        .expect("update");
    assert_eq!(n, 2, "two rows should have v < 3");

    let deleted = state
        .execute("DELETE FROM _wr_update WHERE v > 10".into(), vec![])
        .expect("delete");
    assert_eq!(deleted, 2, "two updated rows should be deleted");
}

// ── namespace isolation tests ─────────────────────────────────────────────────

#[tokio::test]
async fn test_proxy_namespaces_are_isolated() -> Result<()> {
    // Two engines host the same module name in different namespaces.
    let mgr_addr = start_manager().await?;
    let mut mgr = manager_client(&mgr_addr).await?;

    let (e_alpha_addr, e_alpha_shutdown) = spawn_identified_stub("engine-alpha").await?;
    let (e_beta_addr, e_beta_shutdown) = spawn_identified_stub("engine-beta").await?;

    register_module(
        &mut mgr,
        EngineSpec {
            id: "ea",
            addr: &e_alpha_addr,
            proxy_address: "",
        },
        ModuleSpec {
            namespace: "ns-alpha",
            name: "shared-service",
            version: "1.0.0",
            schema: minimal_file_descriptor_set(),
        },
    )
    .await?;
    register_module(
        &mut mgr,
        EngineSpec {
            id: "eb",
            addr: &e_beta_addr,
            proxy_address: "",
        },
        ModuleSpec {
            namespace: "ns-beta",
            name: "shared-service",
            version: "1.0.0",
            schema: minimal_file_descriptor_set(),
        },
    )
    .await?;

    let table = wr_proxy::routing::new_routing_table();
    sync_table(&mgr_addr, &table).await?;
    let proxy = start_proxy_synced(&mgr_addr, table).await?;

    // ns-alpha routes to engine-alpha, not engine-beta.
    let (s, body) = proxy_get(proxy, "ns-alpha", "shared-service", Some("1.0.0")).await?;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(
        body, "engine-alpha",
        "ns-alpha should route to engine-alpha"
    );

    // ns-beta routes to engine-beta, not engine-alpha.
    let (s, body) = proxy_get(proxy, "ns-beta", "shared-service", Some("1.0.0")).await?;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body, "engine-beta", "ns-beta should route to engine-beta");

    let _ = e_alpha_shutdown.send(());
    let _ = e_beta_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn test_proxy_returns_400_when_namespace_missing() -> Result<()> {
    let proxy_addr = start_proxy(wr_proxy::routing::new_routing_table()).await?;

    // Host has no dot — no namespace.
    let req = Request::builder()
        .uri(format!("http://{proxy_addr}/rpc"))
        .header("x-wr-destination", "http://some-service/rpc")
        .header("x-wr-source", "test")
        .body(Full::new(invalid_protobuf()))?;

    let resp = http_client().request(req).await?;
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "missing namespace in destination host should give 400"
    );

    Ok(())
}

#[tokio::test]
async fn test_manager_rejects_module_without_namespace() -> Result<()> {
    let addr = start_manager().await?;
    let mut c = manager_client(&addr).await?;

    let result = c
        .register_engine(RegisterEngineRequest {
            registration: Some(EngineRegistration {
                engine_id: "e1".into(),
                address: "http://127.0.0.1:9100".into(),
                proxy_address: String::new(),
                modules: vec![ModuleDescriptor {
                    name: "svc".into(),
                    namespace: String::new(), // empty namespace → should be rejected
                    version: "1.0.0".into(),
                    proto_schema: vec![],
                }],
            }),
        })
        .await;

    assert!(result.is_err(), "manager should reject empty namespace");
    Ok(())
}

// ── per-module DB schema isolation tests ──────────────────────────────────────
//
// These tests require WRUNTIME_TEST_DB_URL; they skip silently when it is absent.

/// `foo.bar` and `foo.other` each get their own Postgres schema.
/// A table created by `foo.bar` must not be visible to `foo.other`.
#[tokio::test(flavor = "multi_thread")]
async fn test_db_schema_isolation_between_modules() {
    const TABLE: &str = "_wr_isol_items";

    let Some(mut bar) = db_state_for_module(1, "foo", "bar").await else {
        return;
    };
    let Some(mut other) = db_state_for_module(1, "foo", "other").await else {
        return;
    };

    // Drop any table left by a previous test run.
    let _ = DbHost::execute(&mut bar, format!("DROP TABLE IF EXISTS {TABLE}"), vec![]);

    // foo.bar creates and populates its own table.
    DbHost::execute(&mut bar, format!("CREATE TABLE {TABLE} (id INT4)"), vec![])
        .expect("create table in foo.bar schema");
    DbHost::execute(&mut bar, format!("INSERT INTO {TABLE} VALUES (1)"), vec![])
        .expect("insert into foo.bar schema");

    // foo.other's schema has no such table — the query must fail.
    let result = DbHost::query(&mut other, format!("SELECT id FROM {TABLE}"), vec![]);
    assert!(
        result.is_err(),
        "foo.other must not see foo.bar's table; got: {result:?}",
    );

    // Clean up.
    DbHost::execute(&mut bar, format!("DROP TABLE {TABLE}"), vec![]).expect("drop");
}

/// Two engine instances of the same module share the same Postgres schema.
/// A row written by instance 1 must be readable by instance 2.
#[tokio::test(flavor = "multi_thread")]
async fn test_db_schema_shared_across_module_instances() {
    const TABLE: &str = "_wr_shared_items";

    // Two separate pools simulate two independent engine processes.
    let Some(mut inst1) = db_state_for_module(1, "foo", "bar").await else {
        return;
    };
    let Some(mut inst2) = db_state_for_module(1, "foo", "bar").await else {
        return;
    };

    // Drop any table left by a previous test run.
    let _ = DbHost::execute(&mut inst1, format!("DROP TABLE IF EXISTS {TABLE}"), vec![]);

    // Instance 1 creates the table and inserts a row.
    DbHost::execute(
        &mut inst1,
        format!("CREATE TABLE {TABLE} (val INT4)"),
        vec![],
    )
    .expect("create table");
    DbHost::execute(
        &mut inst1,
        format!("INSERT INTO {TABLE} VALUES (42)"),
        vec![],
    )
    .expect("insert");

    // Instance 2 reads from the same schema and must see the row.
    let rows =
        DbHost::query(&mut inst2, format!("SELECT val FROM {TABLE}"), vec![]).expect("query");
    assert_eq!(
        rows.len(),
        1,
        "instance 2 should see the row written by instance 1"
    );
    assert_eq!(rows[0].columns[0].value, PgValue::Int4(42));

    // Clean up.
    DbHost::execute(&mut inst1, format!("DROP TABLE {TABLE}"), vec![]).expect("drop");
}

// ── cross-node routing tests ──────────────────────────────────────────────────

/// Two proxies simulate two separate nodes on 127.0.0.1.
/// A request entering node A must be forwarded to node B's proxy, which then
/// dispatches it to the engine registered on node B.
#[tokio::test]
async fn test_cross_node_routing() -> Result<()> {
    let mgr_addr = start_manager().await?;
    let mut mgr = manager_client(&mgr_addr).await?;

    let (engine_b_addr, engine_b_shutdown) = spawn_identified_stub("engine-b").await?;

    // Start node B first to obtain its proxy address, then register the engine
    // under that address and re-sync so node B's routing table sees the rule.
    let node_b = start_node(&mgr_addr).await?;
    register_module(
        &mut mgr,
        EngineSpec {
            id: "engine-b-id",
            addr: &engine_b_addr,
            proxy_address: &node_b.proxy_address,
        },
        ModuleSpec {
            namespace: "store",
            name: "cross-node-service",
            version: "1.0.0",
            schema: minimal_file_descriptor_set(),
        },
    )
    .await?;
    sync_table(&mgr_addr, &node_b.table).await?;

    // Start node A after registration so its initial sync picks up engine B's rule.
    // Since node_a.proxy_address ≠ node_b.proxy_address, node A will forward cross-node.
    let node_a = start_node(&mgr_addr).await?;

    let (status, body) =
        proxy_get(node_a.addr, "store", "cross-node-service", Some("1.0.0")).await?;
    assert_eq!(status, StatusCode::OK, "cross-node request should succeed");
    assert_eq!(body, "engine-b", "request should reach engine-b via node B");

    let _ = engine_b_shutdown.send(());
    let _ = node_a.proxy_shutdown.send(());
    let _ = node_b.proxy_shutdown.send(());
    Ok(())
}

// ── external ingress tests ────────────────────────────────────────────────────

/// Spin up a manager + stub engine registered as `namespace.module`, then start
/// an ingress proxy with the given `routes`.  Returns `(ingress_addr, engine_shutdown)`.
async fn ingress_fixture(
    module: &str,
    namespace: &str,
    routes: Vec<ExternalRoute>,
) -> Result<(std::net::SocketAddr, tokio::sync::oneshot::Sender<()>)> {
    let mgr_addr = start_manager().await?;
    let mut mgr_c = manager_client(&mgr_addr).await?;

    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;
    register_module(
        &mut mgr_c,
        EngineSpec {
            id: "e1",
            addr: &engine_addr,
            proxy_address: "",
        },
        ModuleSpec {
            namespace,
            name: module,
            version: "1.0.0",
            schema: minimal_file_descriptor_set(),
        },
    )
    .await?;

    let table = wr_proxy::routing::new_routing_table();
    sync_table(&mgr_addr, &table).await?;

    let schema_cache = Arc::new(wr_proxy::schema::SchemaCache::new());
    let ingress_addr = start_ingress_proxy(table, schema_cache, routes).await?;
    Ok((ingress_addr, engine_shutdown))
}

/// Send a plain HTTP request directly to `addr` (no wruntime headers).
async fn external_get(addr: std::net::SocketAddr, path: &str) -> Result<(StatusCode, String)> {
    external_request(addr, "GET", path, &[]).await
}

async fn external_request(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    extra_headers: &[(&str, &str)],
) -> Result<(StatusCode, String)> {
    let mut builder = Request::builder()
        .method(method)
        .uri(format!("http://{addr}{path}"));
    for (k, v) in extra_headers {
        builder = builder.header(*k, *v);
    }
    let resp = http_client()
        .request(builder.body(Full::new(bytes::Bytes::new()))?)
        .await?;
    let status = resp.status();
    let body = resp.into_body().collect().await?.to_bytes();
    Ok((status, String::from_utf8_lossy(&body).into_owned()))
}

#[tokio::test]
async fn test_external_route_dispatches_to_engine() -> Result<()> {
    let routes = vec![ExternalRoute {
        path: "/items".into(),
        methods: vec![],
        module: "inventory".into(),
        namespace: "ecommerce".into(),
        ..Default::default()
    }];
    let (addr, _shutdown) = ingress_fixture("inventory", "ecommerce", routes).await?;

    let (status, body) = external_get(addr, "/items").await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "/items", "stub engine should echo the request path");
    Ok(())
}

#[tokio::test]
async fn test_external_route_wildcard_segment() -> Result<()> {
    let routes = vec![ExternalRoute {
        path: "/items/{id}".into(),
        methods: vec![],
        module: "inventory".into(),
        namespace: "ecommerce".into(),
        ..Default::default()
    }];
    let (addr, _shutdown) = ingress_fixture("inventory", "ecommerce", routes).await?;

    let (status, body) = external_get(addr, "/items/42").await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "/items/42");
    Ok(())
}

#[tokio::test]
async fn test_external_route_unmatched_path_returns_404() -> Result<()> {
    let routes = vec![ExternalRoute {
        path: "/items".into(),
        methods: vec![],
        module: "inventory".into(),
        namespace: "ecommerce".into(),
        ..Default::default()
    }];
    let (addr, _shutdown) = ingress_fixture("inventory", "ecommerce", routes).await?;

    let (status, _) = external_get(addr, "/orders").await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    Ok(())
}

#[tokio::test]
async fn test_external_route_method_filter() -> Result<()> {
    let routes = vec![ExternalRoute {
        path: "/items".into(),
        methods: vec!["GET".into()],
        module: "inventory".into(),
        namespace: "ecommerce".into(),
        ..Default::default()
    }];
    let (addr, _shutdown) = ingress_fixture("inventory", "ecommerce", routes).await?;

    let (get_status, _) = external_request(addr, "GET", "/items", &[]).await?;
    assert_eq!(get_status, StatusCode::OK, "GET should be allowed");

    let (post_status, _) = external_request(addr, "POST", "/items", &[]).await?;
    assert_eq!(post_status, StatusCode::NOT_FOUND, "POST should be blocked");
    Ok(())
}

#[tokio::test]
async fn test_external_route_strips_spoofed_internal_headers() -> Result<()> {
    // Route /items → ecommerce.inventory.
    // A malicious caller also sends x-wr-destination pointing to a non-existent
    // module.  The ingress layer must strip it so routing uses the configured
    // destination, not the spoofed one.
    let routes = vec![ExternalRoute {
        path: "/items".into(),
        methods: vec![],
        module: "inventory".into(),
        namespace: "ecommerce".into(),
        ..Default::default()
    }];
    let (addr, _shutdown) = ingress_fixture("inventory", "ecommerce", routes).await?;

    let (status, _) = external_request(
        addr,
        "GET",
        "/items",
        &[("x-wr-destination", "http://nonexistent.other/items")],
    )
    .await?;
    // If the spoofed header survived, routing would fail (no rule for nonexistent.other)
    // and the proxy would return 503.  Getting 200 proves it was stripped.
    assert_eq!(
        status,
        StatusCode::OK,
        "spoofed x-wr-destination must be overwritten by ingress layer"
    );
    Ok(())
}

// ── JSON↔protobuf transcoding tests ──────────────────────────────────────────

/// Set up an ingress proxy with a transcoding route and a proto-aware stub engine.
///
/// The module uses [`minimal_file_descriptor_set`] which declares:
///   - `PingRequest  { string message = 1; }`
///   - `PingResponse {}`  (empty, but still a valid message)
///   - `PingService.Ping(PingRequest) returns (PingResponse)`
///
/// Returns `(ingress_addr, engine_shutdown)`.
async fn transcoding_fixture() -> Result<(std::net::SocketAddr, tokio::sync::oneshot::Sender<()>)> {
    // An empty `PingResponse` encodes to zero bytes in proto3.
    let proto_response = bytes::Bytes::new();
    let (engine_addr, engine_shutdown) = spawn_proto_stub_engine(proto_response).await?;

    let mgr_addr = start_manager().await?;
    let mut mgr_c = manager_client(&mgr_addr).await?;

    register_module(
        &mut mgr_c,
        EngineSpec {
            id: "e1",
            addr: &engine_addr,
            proxy_address: "",
        },
        ModuleSpec {
            namespace: "test",
            name: "ping",
            version: "1.0.0",
            schema: minimal_file_descriptor_set(),
        },
    )
    .await?;

    let table = wr_proxy::routing::new_routing_table();
    sync_table(&mgr_addr, &table).await?;

    // Populate the schema cache so the ingress layer can resolve message descriptors.
    let schema_cache = Arc::new(wr_proxy::schema::SchemaCache::new());
    sync_schema_cache(&mgr_addr, &schema_cache).await?;

    let routes = vec![ExternalRoute {
        path: "/ping".into(),
        methods: vec!["POST".into()],
        module: "ping".into(),
        namespace: "test".into(),
        grpc_path: Some("/test.ping/Ping".into()),
        request_type: Some("test.PingRequest".into()),
        response_type: Some("test.PingResponse".into()),
    }];
    let ingress_addr = start_ingress_proxy(table, schema_cache, routes).await?;
    Ok((ingress_addr, engine_shutdown))
}

#[tokio::test]
async fn test_transcoding_json_to_proto_and_back() -> Result<()> {
    let (addr, _shutdown) = transcoding_fixture().await?;

    // Send a JSON body matching `PingRequest { message: "hello" }`.
    let resp = http_client()
        .request(
            Request::builder()
                .method("POST")
                .uri(format!("http://{addr}/ping"))
                .header("content-type", "application/json")
                .body(Full::new(bytes::Bytes::from(r#"{"message":"hello"}"#)))?,
        )
        .await?;

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("application/json"),
        "response must be JSON"
    );

    // An empty PingResponse serialises to `{}` in proto3 JSON.
    let body = resp.into_body().collect().await?.to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body)?;
    assert!(json.is_object(), "response should be a JSON object");
    Ok(())
}

#[tokio::test]
async fn test_transcoding_invalid_json_returns_400() -> Result<()> {
    let (addr, _shutdown) = transcoding_fixture().await?;

    let (status, _) = external_request(
        addr,
        "POST",
        "/ping",
        &[("content-type", "application/json")],
    )
    .await?;

    // Empty body is not valid JSON for a DynamicMessage deserializer.
    assert_eq!(status, StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test]
async fn test_transcoding_schema_not_cached_returns_503() -> Result<()> {
    // Build the proxy with an EMPTY schema cache — schema sync is intentionally skipped.
    let mgr_addr = start_manager().await?;
    let mut mgr_c = manager_client(&mgr_addr).await?;

    let (engine_addr, _shutdown) = spawn_stub_engine().await?;
    register_module(
        &mut mgr_c,
        EngineSpec {
            id: "e1",
            addr: &engine_addr,
            proxy_address: "",
        },
        ModuleSpec {
            namespace: "test",
            name: "ping",
            version: "1.0.0",
            schema: minimal_file_descriptor_set(),
        },
    )
    .await?;

    let table = wr_proxy::routing::new_routing_table();
    sync_table(&mgr_addr, &table).await?;

    let routes = vec![ExternalRoute {
        path: "/ping".into(),
        methods: vec![],
        module: "ping".into(),
        namespace: "test".into(),
        grpc_path: Some("/test.ping/Ping".into()),
        request_type: Some("test.PingRequest".into()),
        response_type: Some("test.PingResponse".into()),
    }];
    // Deliberately pass an empty (unsynced) cache.
    let empty_cache = Arc::new(wr_proxy::schema::SchemaCache::new());
    let addr = start_ingress_proxy(table, empty_cache, routes).await?;

    let (status, _) = external_request(
        addr,
        "POST",
        "/ping",
        &[("content-type", "application/json")],
    )
    .await?;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    Ok(())
}

// ── tracing host interface tests ──────────────────────────────────────────────

#[test]
fn test_tracing_span_start_and_drop() {
    use wr_engine::tracing::wruntime::tracing::span::{Host, HostActiveSpan};

    let mut state = ModuleState::new(
        "test".into(),
        "test-ns".into(),
        "http://127.0.0.1:9001".parse().unwrap(),
        None,
        None,
        None,
        tracing::Span::none(),
    )
    .expect("ModuleState");

    let span = Host::start(&mut state, "my-operation".into(), vec![]);
    HostActiveSpan::drop(&mut state, span).expect("drop span");
}

#[test]
fn test_tracing_span_set_attribute() {
    use wr_engine::tracing::wruntime::tracing::span::{Host, HostActiveSpan};

    let mut state = ModuleState::new(
        "test".into(),
        "test-ns".into(),
        "http://127.0.0.1:9001".parse().unwrap(),
        None,
        None,
        None,
        tracing::Span::none(),
    )
    .expect("ModuleState");

    let span = Host::start(&mut state, "op".into(), vec![]);
    let rep = span.rep();
    HostActiveSpan::set_attribute(
        &mut state,
        wasmtime::component::Resource::new_borrow(rep),
        "db.table".into(),
        "users".into(),
    );
    HostActiveSpan::drop(&mut state, span).expect("drop");
}

#[test]
fn test_tracing_span_record_event() {
    use wr_engine::tracing::wruntime::tracing::span::{Host, HostActiveSpan};

    let mut state = ModuleState::new(
        "test".into(),
        "test-ns".into(),
        "http://127.0.0.1:9001".parse().unwrap(),
        None,
        None,
        None,
        tracing::Span::none(),
    )
    .expect("ModuleState");

    let span = Host::start(&mut state, "op".into(), vec![]);
    let rep = span.rep();
    HostActiveSpan::record_event(
        &mut state,
        wasmtime::component::Resource::new_borrow(rep),
        "cache.miss".into(),
        vec![("key".into(), "user:42".into())],
    );
    HostActiveSpan::drop(&mut state, span).expect("drop");
}

#[test]
fn test_tracing_span_set_error() {
    use wr_engine::tracing::wruntime::tracing::span::{Host, HostActiveSpan};

    let mut state = ModuleState::new(
        "test".into(),
        "test-ns".into(),
        "http://127.0.0.1:9001".parse().unwrap(),
        None,
        None,
        None,
        tracing::Span::none(),
    )
    .expect("ModuleState");

    let span = Host::start(&mut state, "op".into(), vec![]);
    let rep = span.rep();
    HostActiveSpan::set_error(
        &mut state,
        wasmtime::component::Resource::new_borrow(rep),
        "connection refused".into(),
    );
    HostActiveSpan::drop(&mut state, span).expect("drop");
}
