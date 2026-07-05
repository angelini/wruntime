#[allow(dead_code, unused_imports)]
mod helpers;
use helpers::*;

use anyhow::Result;

use wr_common::wruntime::{
    DeregisterEngineRequest, EngineRegistration, GetRoutingTableRequest, GetSchemaRequest,
    HeartbeatRequest, ListEnginesRequest, ModuleDescriptor, RegisterEngineRequest, RoutingRule,
    SecretRequest,
};

#[tokio::test]
async fn test_register_and_list_engines() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    c.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "e1".into(),
            address: "http://127.0.0.1:9100".into(),
            proxy_address: String::new(),
            peer_address: String::new(),
            modules: vec![ModuleDescriptor {
                name: "inventory-service".into(),
                namespace: "store".into(),
                version: "1.0.0".into(),
                proto_schema: minimal_file_descriptor_set(),
            }],
            secrets: vec![],
            db_namespaces: vec![],
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
    let (_pool, _addr, mut c) = manager_trio().await?;

    c.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "e1".into(),
            address: "http://127.0.0.1:9101".into(),
            proxy_address: String::new(),
            peer_address: String::new(),
            modules: vec![],
            secrets: vec![],
            db_namespaces: vec![],
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
    let (_pool, _addr, mut c) = manager_trio().await?;

    c.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "e1".into(),
            address: "http://127.0.0.1:9102".into(),
            proxy_address: String::new(),
            peer_address: String::new(),
            modules: vec![],
            secrets: vec![],
            db_namespaces: vec![],
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
    let (_pool, _addr, mut c) = manager_trio().await?;

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
        peer_address: String::new(),
        healthy: false, // server sets this to true on upsert
    })
    .await?;

    let table = c
        .get_routing_table(GetRoutingTableRequest { known_version: 0 })
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

// ── GetSchema RPC tests ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_get_schema_after_registration() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    let schema_bytes = minimal_file_descriptor_set();

    c.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "schema-e1".into(),
            address: "http://127.0.0.1:9200".into(),
            proxy_address: String::new(),
            peer_address: String::new(),
            modules: vec![ModuleDescriptor {
                name: "orders".into(),
                namespace: "shop".into(),
                version: "1.0.0".into(),
                proto_schema: schema_bytes.clone(),
            }],
            secrets: vec![],
            db_namespaces: vec![],
        }),
    })
    .await?;

    let resp = c
        .get_schema(GetSchemaRequest {
            namespace: "shop".into(),
            module: "orders".into(),
            version: "1.0.0".into(),
        })
        .await?
        .into_inner();

    assert_eq!(
        resp.proto_schema, schema_bytes,
        "schema bytes should round-trip"
    );

    Ok(())
}

#[tokio::test]
async fn test_get_schema_not_found() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    let err = c
        .get_schema(GetSchemaRequest {
            namespace: "nope".into(),
            module: "missing".into(),
            version: "0.0.0".into(),
        })
        .await
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::NotFound);
    assert!(
        err.message().contains("no schema"),
        "expected 'no schema' message, got: {}",
        err.message(),
    );

    Ok(())
}

#[tokio::test]
async fn test_get_schema_empty_namespace_rejected() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    let err = c
        .get_schema(GetSchemaRequest {
            namespace: "".into(),
            module: "svc".into(),
            version: "1.0.0".into(),
        })
        .await
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("namespace"),
        "expected namespace error, got: {}",
        err.message(),
    );

    Ok(())
}

#[tokio::test]
async fn test_get_schema_multiple_versions() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    // Build two distinct schemas so we can tell them apart.
    let schema_v1 = minimal_file_descriptor_set();

    // Create a slightly different schema for v2 by adding a second file.
    use prost::Message;
    use prost_types::{FileDescriptorProto, FileDescriptorSet};
    let mut fds = FileDescriptorSet::decode(schema_v1.as_slice()).unwrap();
    fds.file.push(FileDescriptorProto {
        name: Some("v2_extra.proto".into()),
        package: Some("test".into()),
        syntax: Some("proto3".into()),
        ..Default::default()
    });
    let schema_v2 = fds.encode_to_vec();
    assert_ne!(schema_v1, schema_v2, "test schemas must differ");

    // Register v1.
    c.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "ver-e1".into(),
            address: "http://127.0.0.1:9210".into(),
            proxy_address: String::new(),
            peer_address: String::new(),
            modules: vec![ModuleDescriptor {
                name: "catalog".into(),
                namespace: "retail".into(),
                version: "1.0.0".into(),
                proto_schema: schema_v1.clone(),
            }],
            secrets: vec![],
            db_namespaces: vec![],
        }),
    })
    .await?;

    // Register v2 from a different engine.
    c.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "ver-e2".into(),
            address: "http://127.0.0.1:9211".into(),
            proxy_address: String::new(),
            peer_address: String::new(),
            modules: vec![ModuleDescriptor {
                name: "catalog".into(),
                namespace: "retail".into(),
                version: "2.0.0".into(),
                proto_schema: schema_v2.clone(),
            }],
            secrets: vec![],
            db_namespaces: vec![],
        }),
    })
    .await?;

    // Fetch each version independently.
    let resp_v1 = c
        .get_schema(GetSchemaRequest {
            namespace: "retail".into(),
            module: "catalog".into(),
            version: "1.0.0".into(),
        })
        .await?
        .into_inner();
    assert_eq!(resp_v1.proto_schema, schema_v1);

    let resp_v2 = c
        .get_schema(GetSchemaRequest {
            namespace: "retail".into(),
            module: "catalog".into(),
            version: "2.0.0".into(),
        })
        .await?
        .into_inner();
    assert_eq!(resp_v2.proto_schema, schema_v2);

    Ok(())
}

#[tokio::test]
async fn test_get_schema_cross_namespace_isolation() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    let schema = minimal_file_descriptor_set();

    // Register same module name in two different namespaces.
    c.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "ns-e1".into(),
            address: "http://127.0.0.1:9220".into(),
            proxy_address: String::new(),
            peer_address: String::new(),
            modules: vec![ModuleDescriptor {
                name: "gateway".into(),
                namespace: "alpha".into(),
                version: "1.0.0".into(),
                proto_schema: schema.clone(),
            }],
            secrets: vec![],
            db_namespaces: vec![],
        }),
    })
    .await?;

    // Query with the wrong namespace — should not find it.
    let err = c
        .get_schema(GetSchemaRequest {
            namespace: "beta".into(),
            module: "gateway".into(),
            version: "1.0.0".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);

    // Query with the correct namespace — should succeed.
    let resp = c
        .get_schema(GetSchemaRequest {
            namespace: "alpha".into(),
            module: "gateway".into(),
            version: "1.0.0".into(),
        })
        .await?
        .into_inner();
    assert_eq!(resp.proto_schema, schema);

    Ok(())
}

#[tokio::test]
async fn test_get_schema_updated_on_reregistration() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    let schema_v1 = minimal_file_descriptor_set();

    // Initial registration.
    c.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "reup-e1".into(),
            address: "http://127.0.0.1:9230".into(),
            proxy_address: String::new(),
            peer_address: String::new(),
            modules: vec![ModuleDescriptor {
                name: "payments".into(),
                namespace: "billing".into(),
                version: "1.0.0".into(),
                proto_schema: schema_v1.clone(),
            }],
            secrets: vec![],
            db_namespaces: vec![],
        }),
    })
    .await?;

    // Re-register the same module/version with a different schema (ON CONFLICT UPDATE).
    use prost::Message;
    use prost_types::{FileDescriptorProto, FileDescriptorSet};
    let mut fds = FileDescriptorSet::decode(schema_v1.as_slice()).unwrap();
    fds.file.push(FileDescriptorProto {
        name: Some("updated.proto".into()),
        package: Some("billing".into()),
        syntax: Some("proto3".into()),
        ..Default::default()
    });
    let schema_updated = fds.encode_to_vec();

    c.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "reup-e1".into(),
            address: "http://127.0.0.1:9230".into(),
            proxy_address: String::new(),
            peer_address: String::new(),
            modules: vec![ModuleDescriptor {
                name: "payments".into(),
                namespace: "billing".into(),
                version: "1.0.0".into(),
                proto_schema: schema_updated.clone(),
            }],
            secrets: vec![],
            db_namespaces: vec![],
        }),
    })
    .await?;

    let resp = c
        .get_schema(GetSchemaRequest {
            namespace: "billing".into(),
            module: "payments".into(),
            version: "1.0.0".into(),
        })
        .await?
        .into_inner();

    assert_eq!(
        resp.proto_schema, schema_updated,
        "schema should be updated after re-registration",
    );
    assert_ne!(resp.proto_schema, schema_v1);

    Ok(())
}

#[tokio::test]
async fn test_get_schema_multi_module_engine() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    let schema = minimal_file_descriptor_set();

    // Register one engine with two modules.
    c.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "multi-e1".into(),
            address: "http://127.0.0.1:9240".into(),
            proxy_address: String::new(),
            peer_address: String::new(),
            modules: vec![
                ModuleDescriptor {
                    name: "auth".into(),
                    namespace: "platform".into(),
                    version: "1.0.0".into(),
                    proto_schema: schema.clone(),
                },
                ModuleDescriptor {
                    name: "users".into(),
                    namespace: "platform".into(),
                    version: "1.0.0".into(),
                    proto_schema: schema.clone(),
                },
            ],
            secrets: vec![],
            db_namespaces: vec![],
        }),
    })
    .await?;

    // Both modules should be retrievable.
    let resp_auth = c
        .get_schema(GetSchemaRequest {
            namespace: "platform".into(),
            module: "auth".into(),
            version: "1.0.0".into(),
        })
        .await?
        .into_inner();
    assert_eq!(resp_auth.proto_schema, schema);

    let resp_users = c
        .get_schema(GetSchemaRequest {
            namespace: "platform".into(),
            module: "users".into(),
            version: "1.0.0".into(),
        })
        .await?
        .into_inner();
    assert_eq!(resp_users.proto_schema, schema);

    Ok(())
}

#[tokio::test]
async fn test_register_engine_creates_default_routing_rule() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    c.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "route-e1".into(),
            address: "http://127.0.0.1:9600".into(),
            proxy_address: String::new(),
            peer_address: "https://127.0.0.1:9443".into(),
            modules: vec![ModuleDescriptor {
                name: "inventory".into(),
                namespace: "store".into(),
                version: "1.0.0".into(),
                proto_schema: minimal_file_descriptor_set(),
            }],
            secrets: vec![],
            db_namespaces: vec![],
        }),
    })
    .await?;

    let table = c
        .get_routing_table(GetRoutingTableRequest { known_version: 0 })
        .await?
        .into_inner()
        .table
        .unwrap();

    assert_eq!(
        table.rules.len(),
        1,
        "manager creates exactly one default rule"
    );
    let r = &table.rules[0];
    assert_eq!(r.rule_id, "route-e1/store/inventory/1.0.0");
    assert_eq!(r.destination_namespace, "store");
    assert_eq!(r.destination_module, "inventory");
    assert_eq!(r.destination_version, "1.0.0");
    assert_eq!(r.engine_id, "route-e1");
    assert_eq!(r.engine_address, "http://127.0.0.1:9600");
    assert_eq!(r.proxy_address, "https://127.0.0.1:9443");
    assert_eq!(r.peer_address, "https://127.0.0.1:9443");
    assert_eq!(r.source_namespace, "");
    assert_eq!(r.source_module, "");
    assert!(r.healthy, "default rule starts healthy");
    Ok(())
}

#[tokio::test]
async fn test_register_engine_dedups_duplicate_module_instances() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;
    let schema = minimal_file_descriptor_set();

    c.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "dup-e1".into(),
            address: "http://127.0.0.1:9610".into(),
            proxy_address: String::new(),
            peer_address: String::new(),
            modules: vec![
                ModuleDescriptor {
                    name: "inventory".into(),
                    namespace: "store".into(),
                    version: "1.0.0".into(),
                    proto_schema: schema.clone(),
                },
                ModuleDescriptor {
                    name: "inventory".into(),
                    namespace: "store".into(),
                    version: "1.0.0".into(),
                    proto_schema: schema.clone(),
                },
            ],
            secrets: vec![],
            db_namespaces: vec![],
        }),
    })
    .await?;

    let table = c
        .get_routing_table(GetRoutingTableRequest { known_version: 0 })
        .await?
        .into_inner()
        .table
        .unwrap();
    assert_eq!(table.rules.len(), 1, "duplicate instances produce one rule");
    assert_eq!(table.rules[0].rule_id, "dup-e1/store/inventory/1.0.0");
    Ok(())
}

#[tokio::test]
async fn test_register_engine_missing_schema_rejected_no_writes() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    let err = c
        .register_engine(RegisterEngineRequest {
            registration: Some(EngineRegistration {
                engine_id: "badschema-e1".into(),
                address: "http://127.0.0.1:9620".into(),
                proxy_address: String::new(),
                peer_address: String::new(),
                modules: vec![ModuleDescriptor {
                    name: "inventory".into(),
                    namespace: "store".into(),
                    version: "1.0.0".into(),
                    proto_schema: vec![], // empty first descriptor -> rejected
                }],
                secrets: vec![],
                db_namespaces: vec![],
            }),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    let engines = c
        .list_engines(ListEnginesRequest {})
        .await?
        .into_inner()
        .engines;
    assert!(
        engines.iter().all(|e| e.engine_id != "badschema-e1"),
        "rejected registration must write no engine row",
    );

    let table = c
        .get_routing_table(GetRoutingTableRequest { known_version: 0 })
        .await?
        .into_inner()
        .table
        .unwrap();
    assert!(
        table
            .rules
            .iter()
            .all(|r| !r.rule_id.starts_with("badschema-e1/")),
        "rejected registration must write no routing rules",
    );
    Ok(())
}

#[tokio::test]
async fn test_register_engine_missing_secret_leaves_no_routes() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    let err = c
        .register_engine(RegisterEngineRequest {
            registration: Some(EngineRegistration {
                engine_id: "secret-e1".into(),
                address: "http://127.0.0.1:9630".into(),
                proxy_address: String::new(),
                peer_address: String::new(),
                modules: vec![ModuleDescriptor {
                    name: "inventory".into(),
                    namespace: "store".into(),
                    version: "1.0.0".into(),
                    proto_schema: minimal_file_descriptor_set(),
                }],
                secrets: vec![SecretRequest {
                    namespace: "store".into(),
                    key: "api-key".into(), // never stored -> resolve_secrets fails
                }],
                db_namespaces: vec![],
            }),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);
    assert!(err.message().contains("missing secrets"));

    let engines = c
        .list_engines(ListEnginesRequest {})
        .await?
        .into_inner()
        .engines;
    assert!(
        engines.iter().all(|e| e.engine_id != "secret-e1"),
        "failed secret resolution must leave no engine row",
    );

    let table = c
        .get_routing_table(GetRoutingTableRequest { known_version: 0 })
        .await?
        .into_inner()
        .table
        .unwrap();
    assert!(
        table
            .rules
            .iter()
            .all(|r| !r.rule_id.starts_with("secret-e1/")),
        "failed registration must leave zero routing rules",
    );
    Ok(())
}

#[tokio::test]
async fn test_reregister_removes_dropped_module_route_and_heartbeat() -> Result<()> {
    let (pool, _addr, mut c) = manager_trio().await?;
    let schema = minimal_file_descriptor_set();

    let module = |name: &str| ModuleDescriptor {
        name: name.into(),
        namespace: "store".into(),
        version: "1.0.0".into(),
        proto_schema: schema.clone(),
    };

    c.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "recon-e1".into(),
            address: "http://127.0.0.1:9640".into(),
            proxy_address: String::new(),
            peer_address: "https://127.0.0.1:9443".into(),
            modules: vec![module("alpha"), module("beta")],
            secrets: vec![],
            db_namespaces: vec![],
        }),
    })
    .await?;

    let v_before: i64 = pool
        .get()
        .await?
        .query_one("SELECT version FROM wr_manager_lock WHERE id = 1", &[])
        .await?
        .get(0);

    c.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "recon-e1".into(),
            address: "http://127.0.0.1:9640".into(),
            proxy_address: String::new(),
            peer_address: "https://127.0.0.1:9443".into(),
            modules: vec![module("alpha")],
            secrets: vec![],
            db_namespaces: vec![],
        }),
    })
    .await?;

    let table = c
        .get_routing_table(GetRoutingTableRequest { known_version: 0 })
        .await?
        .into_inner()
        .table
        .unwrap();
    let recon_rules: Vec<&str> = table
        .rules
        .iter()
        .filter(|r| r.rule_id.starts_with("recon-e1/"))
        .map(|r| r.rule_id.as_str())
        .collect();
    assert_eq!(
        recon_rules,
        vec!["recon-e1/store/alpha/1.0.0"],
        "only the retained module's default rule should survive re-registration",
    );

    let hb_modules: Vec<String> = pool
        .get()
        .await?
        .query(
            "SELECT module_name FROM wr_module_heartbeats
             WHERE engine_id = $1 ORDER BY module_name",
            &[&"recon-e1"],
        )
        .await?
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect();
    assert_eq!(
        hb_modules,
        vec!["alpha".to_string()],
        "dropped module's heartbeat row must be removed; retained module's remains",
    );

    let v_after: i64 = pool
        .get()
        .await?
        .query_one("SELECT version FROM wr_manager_lock WHERE id = 1", &[])
        .await?
        .get(0);
    assert!(
        v_after > v_before,
        "removing a default route must bump the version"
    );

    Ok(())
}

#[tokio::test]
async fn test_reregister_with_no_modules_clears_routes_and_bumps_version() -> Result<()> {
    let (pool, _addr, mut c) = manager_trio().await?;

    c.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "recon-e2".into(),
            address: "http://127.0.0.1:9650".into(),
            proxy_address: String::new(),
            peer_address: String::new(),
            modules: vec![ModuleDescriptor {
                name: "inventory".into(),
                namespace: "store".into(),
                version: "1.0.0".into(),
                proto_schema: minimal_file_descriptor_set(),
            }],
            secrets: vec![],
            db_namespaces: vec![],
        }),
    })
    .await?;

    let v_before: i64 = pool
        .get()
        .await?
        .query_one("SELECT version FROM wr_manager_lock WHERE id = 1", &[])
        .await?
        .get(0);

    c.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "recon-e2".into(),
            address: "http://127.0.0.1:9650".into(),
            proxy_address: String::new(),
            peer_address: String::new(),
            modules: vec![],
            secrets: vec![],
            db_namespaces: vec![],
        }),
    })
    .await?;

    let table = c
        .get_routing_table(GetRoutingTableRequest { known_version: 0 })
        .await?
        .into_inner()
        .table
        .unwrap();
    assert!(
        table
            .rules
            .iter()
            .all(|r| !r.rule_id.starts_with("recon-e2/")),
        "re-registration with no modules must remove all of the engine's default rules",
    );

    let hb_count: i64 = pool
        .get()
        .await?
        .query_one(
            "SELECT COUNT(*) FROM wr_module_heartbeats WHERE engine_id = $1",
            &[&"recon-e2"],
        )
        .await?
        .get(0);
    assert_eq!(
        hb_count, 0,
        "all module heartbeats for the engine must be removed"
    );

    let v_after: i64 = pool
        .get()
        .await?
        .query_one("SELECT version FROM wr_manager_lock WHERE id = 1", &[])
        .await?
        .get(0);
    assert!(
        v_after > v_before,
        "delete-only reconciliation must still bump the routing version",
    );

    Ok(())
}
