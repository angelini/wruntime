mod helpers;
use helpers::{manager::manager_trio, proxy::TEST_SELF_PEER, wasm::minimal_file_descriptor_set};

use anyhow::Result;

use wr_common::wruntime::{
    BeginDeploymentRequest, BeginEngineDrainRequest, BeginRollbackRequest,
    CompleteDeploymentRequest, DeploymentMetadata, DeploymentState, DeregisterEngineRequest,
    EngineRegistration, ExpectedEngine, GetClusterStatusRequest, GetRoutingTableRequest,
    GetSchemaRequest, HeartbeatRequest, ListEnginesRequest, ModuleDescriptor, ModuleIdentity,
    RegisterEngineRequest, RoutingRule, SecretRequest, StatusSeverity, VerifyDeploymentRequest,
};

#[tokio::test]
async fn test_register_and_list_engines() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    c.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "e1".into(),
            address: "http://127.0.0.1:9100".into(),
            proxy_address: TEST_SELF_PEER.into(),
            peer_address: TEST_SELF_PEER.into(),
            modules: vec![ModuleDescriptor {
                name: "inventory-service".into(),
                namespace: "store".into(),
                version: "1.0.0".into(),
                proto_schema: minimal_file_descriptor_set(),
            }],
            secrets: vec![],
            db_namespaces: vec![],
            deployment: None,
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
            proxy_address: TEST_SELF_PEER.into(),
            peer_address: TEST_SELF_PEER.into(),
            modules: vec![],
            secrets: vec![],
            db_namespaces: vec![],
            deployment: None,
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
            proxy_address: TEST_SELF_PEER.into(),
            peer_address: TEST_SELF_PEER.into(),
            modules: vec![],
            secrets: vec![],
            db_namespaces: vec![],
            deployment: None,
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
async fn test_readiness_and_drain_are_atomic_versioned_and_fenced() -> Result<()> {
    let (_pool, _addr, mut client) = manager_trio().await?;
    client
        .register_engine(RegisterEngineRequest {
            registration: Some(EngineRegistration {
                engine_id: "lifecycle-engine".into(),
                address: "http://127.0.0.1:9199".into(),
                proxy_address: TEST_SELF_PEER.into(),
                peer_address: TEST_SELF_PEER.into(),
                modules: vec![ModuleDescriptor {
                    name: "lifecycle-service".into(),
                    namespace: "store".into(),
                    version: "1.0.0".into(),
                    proto_schema: minimal_file_descriptor_set(),
                }],
                secrets: vec![],
                db_namespaces: vec![],
                deployment: None,
            }),
        })
        .await?;

    let readiness = client
        .heartbeat(HeartbeatRequest {
            engine_id: "lifecycle-engine".into(),
            healthy_modules: vec![ModuleDescriptor {
                name: "lifecycle-service".into(),
                namespace: "store".into(),
                version: "1.0.0".into(),
                proto_schema: vec![],
            }],
        })
        .await?
        .into_inner();
    assert!(readiness.manager_routing_table_version > 0);
    let ready_table = client
        .get_routing_table(GetRoutingTableRequest { known_version: 0 })
        .await?
        .into_inner()
        .table
        .ok_or_else(|| anyhow::anyhow!("ready routing table missing"))?;
    assert!(ready_table.rules[0].healthy);

    let drained = client
        .begin_engine_drain(BeginEngineDrainRequest {
            engine_id: "lifecycle-engine".into(),
        })
        .await?
        .into_inner();
    assert!(drained.manager_routing_table_version > readiness.manager_routing_table_version);
    let drained_table = client
        .get_routing_table(GetRoutingTableRequest { known_version: 0 })
        .await?
        .into_inner()
        .table
        .ok_or_else(|| anyhow::anyhow!("drained routing table missing"))?;
    assert!(!drained_table.rules[0].healthy);

    let stale = match client
        .heartbeat(HeartbeatRequest {
            engine_id: "lifecycle-engine".into(),
            healthy_modules: vec![],
        })
        .await
    {
        Ok(_) => anyhow::bail!("draining engine heartbeat was not fenced"),
        Err(status) => status,
    };
    assert_eq!(stale.code(), tonic::Code::FailedPrecondition);

    for _ in 0..2 {
        client
            .deregister_engine(DeregisterEngineRequest {
                engine_id: "lifecycle-engine".into(),
            })
            .await?;
    }
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
        peer_address: "https://127.0.0.1:9443".into(),
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

#[tokio::test]
async fn test_routing_rule_rejects_invalid_identity_version_and_peer_scheme() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;
    for (namespace, version, peer) in [
        ("bad_namespace", "1.0.0", "https://127.0.0.1:9443"),
        ("ns", "latest", "https://127.0.0.1:9443"),
        ("ns", "1.0.0", "http://127.0.0.1:9443"),
    ] {
        let error = c
            .upsert_routing_rule(RoutingRule {
                rule_id: "invalid-route".into(),
                source_module: String::new(),
                source_namespace: String::new(),
                destination_module: "svc".into(),
                destination_namespace: namespace.into(),
                destination_version: version.into(),
                engine_id: "e1".into(),
                engine_address: "http://127.0.0.1:9103".into(),
                peer_address: peer.into(),
                healthy: false,
            })
            .await
            .expect_err("invalid route boundary must be rejected");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }
    Ok(())
}

#[tokio::test]
async fn test_routing_rule_rejects_empty_peer_address() -> Result<()> {
    let (_pool, _addr, mut c) = manager_trio().await?;

    let err = c
        .upsert_routing_rule(RoutingRule {
            rule_id: "empty-peer-r1".into(),
            source_module: String::new(),
            source_namespace: String::new(),
            destination_module: "svc".into(),
            destination_namespace: "ns".into(),
            destination_version: "1.0.0".into(),
            engine_id: "e1".into(),
            engine_address: "http://127.0.0.1:9103".into(),
            peer_address: String::new(),
            healthy: false,
        })
        .await
        .expect_err("empty peer_address must be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

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
            proxy_address: TEST_SELF_PEER.into(),
            peer_address: TEST_SELF_PEER.into(),
            modules: vec![ModuleDescriptor {
                name: "orders".into(),
                namespace: "shop".into(),
                version: "1.0.0".into(),
                proto_schema: schema_bytes.clone(),
            }],
            secrets: vec![],
            db_namespaces: vec![],
            deployment: None,
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
            proxy_address: TEST_SELF_PEER.into(),
            peer_address: TEST_SELF_PEER.into(),
            modules: vec![ModuleDescriptor {
                name: "catalog".into(),
                namespace: "retail".into(),
                version: "1.0.0".into(),
                proto_schema: schema_v1.clone(),
            }],
            secrets: vec![],
            db_namespaces: vec![],
            deployment: None,
        }),
    })
    .await?;

    // Register v2 from a different engine.
    c.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "ver-e2".into(),
            address: "http://127.0.0.1:9211".into(),
            proxy_address: TEST_SELF_PEER.into(),
            peer_address: TEST_SELF_PEER.into(),
            modules: vec![ModuleDescriptor {
                name: "catalog".into(),
                namespace: "retail".into(),
                version: "2.0.0".into(),
                proto_schema: schema_v2.clone(),
            }],
            secrets: vec![],
            db_namespaces: vec![],
            deployment: None,
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
            proxy_address: TEST_SELF_PEER.into(),
            peer_address: TEST_SELF_PEER.into(),
            modules: vec![ModuleDescriptor {
                name: "gateway".into(),
                namespace: "alpha".into(),
                version: "1.0.0".into(),
                proto_schema: schema.clone(),
            }],
            secrets: vec![],
            db_namespaces: vec![],
            deployment: None,
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
            proxy_address: TEST_SELF_PEER.into(),
            peer_address: TEST_SELF_PEER.into(),
            modules: vec![ModuleDescriptor {
                name: "payments".into(),
                namespace: "billing".into(),
                version: "1.0.0".into(),
                proto_schema: schema_v1.clone(),
            }],
            secrets: vec![],
            db_namespaces: vec![],
            deployment: None,
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
            proxy_address: TEST_SELF_PEER.into(),
            peer_address: TEST_SELF_PEER.into(),
            modules: vec![ModuleDescriptor {
                name: "payments".into(),
                namespace: "billing".into(),
                version: "1.0.0".into(),
                proto_schema: schema_updated.clone(),
            }],
            secrets: vec![],
            db_namespaces: vec![],
            deployment: None,
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
            proxy_address: TEST_SELF_PEER.into(),
            peer_address: TEST_SELF_PEER.into(),
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
            deployment: None,
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
            proxy_address: TEST_SELF_PEER.into(),
            peer_address: "https://127.0.0.1:9443".into(),
            modules: vec![ModuleDescriptor {
                name: "inventory".into(),
                namespace: "store".into(),
                version: "1.0.0".into(),
                proto_schema: minimal_file_descriptor_set(),
            }],
            secrets: vec![],
            db_namespaces: vec![],
            deployment: None,
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
    assert_eq!(r.peer_address, "https://127.0.0.1:9443");
    assert_eq!(r.source_namespace, "");
    assert_eq!(r.source_module, "");
    assert!(
        !r.healthy,
        "default rule starts unhealthy until module heartbeat readiness"
    );
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
            proxy_address: TEST_SELF_PEER.into(),
            peer_address: TEST_SELF_PEER.into(),
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
            deployment: None,
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
    assert!(
        !table.rules[0].healthy,
        "deduped default rule starts unhealthy"
    );
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
                proxy_address: TEST_SELF_PEER.into(),
                peer_address: TEST_SELF_PEER.into(),
                modules: vec![ModuleDescriptor {
                    name: "inventory".into(),
                    namespace: "store".into(),
                    version: "1.0.0".into(),
                    proto_schema: vec![], // empty first descriptor -> rejected
                }],
                secrets: vec![],
                db_namespaces: vec![],
                deployment: None,
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
                proxy_address: TEST_SELF_PEER.into(),
                peer_address: TEST_SELF_PEER.into(),
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
                deployment: None,
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
            proxy_address: TEST_SELF_PEER.into(),
            peer_address: "https://127.0.0.1:9443".into(),
            modules: vec![module("alpha"), module("beta")],
            secrets: vec![],
            db_namespaces: vec![],
            deployment: None,
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
            proxy_address: TEST_SELF_PEER.into(),
            peer_address: "https://127.0.0.1:9443".into(),
            modules: vec![module("alpha")],
            secrets: vec![],
            db_namespaces: vec![],
            deployment: None,
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
    assert!(
        hb_modules.is_empty(),
        "re-registration clears module heartbeat rows for both retained and dropped tuples",
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
            proxy_address: TEST_SELF_PEER.into(),
            peer_address: TEST_SELF_PEER.into(),
            modules: vec![ModuleDescriptor {
                name: "inventory".into(),
                namespace: "store".into(),
                version: "1.0.0".into(),
                proto_schema: minimal_file_descriptor_set(),
            }],
            secrets: vec![],
            db_namespaces: vec![],
            deployment: None,
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
            proxy_address: TEST_SELF_PEER.into(),
            peer_address: TEST_SELF_PEER.into(),
            modules: vec![],
            secrets: vec![],
            db_namespaces: vec![],
            deployment: None,
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

#[tokio::test]
async fn test_revisioned_deployment_verification_and_rollback_history() -> Result<()> {
    let (pool, _addr, mut client) = manager_trio().await?;
    let expected = vec![ExpectedEngine {
        engine_slot: "primary".into(),
        modules: vec![ModuleIdentity {
            namespace: "store".into(),
            name: "inventory".into(),
            version: "1.0.0".into(),
        }],
    }];
    let digest_one = format!("sha256:{}", "1".repeat(64));
    let digest_two = format!("sha256:{}", "2".repeat(64));

    let first = client
        .begin_deployment(BeginDeploymentRequest {
            node_id: "node-a".into(),
            attempt_token: "attempt-one".into(),
            bundle_digest: digest_one.clone(),
            expected_engines: expected.clone(),
        })
        .await?
        .into_inner()
        .deployment
        .unwrap();
    assert_eq!(first.revision, 1);
    let retry = client
        .begin_deployment(BeginDeploymentRequest {
            node_id: "node-a".into(),
            attempt_token: "attempt-one".into(),
            bundle_digest: digest_one.clone(),
            expected_engines: expected.clone(),
        })
        .await?
        .into_inner()
        .deployment
        .unwrap();
    assert_eq!(retry.revision, first.revision);
    let conflict = client
        .begin_deployment(BeginDeploymentRequest {
            node_id: "node-a".into(),
            attempt_token: "attempt-one".into(),
            bundle_digest: digest_two.clone(),
            expected_engines: expected.clone(),
        })
        .await
        .unwrap_err();
    assert_eq!(conflict.code(), tonic::Code::AlreadyExists);

    let missing = client
        .verify_deployment(VerifyDeploymentRequest {
            node_id: "node-a".into(),
            revision: first.revision,
        })
        .await?
        .into_inner();
    assert!(!missing.ready);
    assert_eq!(missing.conditions[0].code, "MISSING_ENGINE");

    async fn activate(
        client: &mut wr_common::wruntime::manager_service_client::ManagerServiceClient<
            tonic::transport::Channel,
        >,
        pool: &deadpool_postgres::Pool,
        engine_id: &str,
        revision: u64,
        digest: &str,
    ) -> Result<()> {
        let module = ModuleDescriptor {
            name: "inventory".into(),
            namespace: "store".into(),
            version: "1.0.0".into(),
            proto_schema: minimal_file_descriptor_set(),
        };
        client
            .register_engine(RegisterEngineRequest {
                registration: Some(EngineRegistration {
                    engine_id: engine_id.into(),
                    address: "http://127.0.0.1:9700".into(),
                    proxy_address: "http://127.0.0.1:9001".into(),
                    peer_address: TEST_SELF_PEER.into(),
                    modules: vec![module.clone()],
                    secrets: vec![],
                    db_namespaces: vec![],
                    deployment: Some(DeploymentMetadata {
                        node_id: "node-a".into(),
                        revision,
                        bundle_digest: digest.into(),
                        engine_slot: "primary".into(),
                    }),
                }),
            })
            .await?;
        client
            .heartbeat(HeartbeatRequest {
                engine_id: engine_id.into(),
                healthy_modules: vec![module],
            })
            .await?;
        wr_manager::db::update_route_health(pool, 10.0, 10.0).await?;
        Ok(())
    }

    activate(&mut client, &pool, "deploy-e1", 1, &digest_one).await?;
    let ready = client
        .verify_deployment(VerifyDeploymentRequest {
            node_id: "node-a".into(),
            revision: 1,
        })
        .await?
        .into_inner();
    assert!(ready.ready, "conditions: {:?}", ready.conditions);
    assert!(
        ready
            .deployment
            .as_ref()
            .and_then(|record| record.activated_at.as_ref())
            .is_some(),
        "registration should stamp activation time"
    );
    let healthy_status = client
        .get_cluster_status(GetClusterStatusRequest {})
        .await?
        .into_inner();
    assert!(healthy_status.database_observed_at.is_some());
    assert!(healthy_status.gossip_observed_at.is_some());
    assert!(healthy_status.response_at.is_some());
    assert!(healthy_status.routing_table_version > 0);
    let node = healthy_status
        .nodes
        .iter()
        .find(|node| node.node_id == "node-a")
        .expect("status should include desired node");
    assert_eq!(node.severity, StatusSeverity::Healthy as i32);
    assert_eq!(node.deployment_history.len(), 1);
    assert_eq!(node.engines[0].heartbeat_age_seconds, 0);
    assert!(node.engines[0].modules[0].last_healthy.is_some());
    assert_eq!(healthy_status.services[0].healthy_routes, 1);
    assert!(healthy_status
        .conditions
        .iter()
        .all(|condition| condition.code == "SIGNAL_NOT_REPORTED"));
    pool.get()
        .await?
        .execute(
            "UPDATE wr_engines SET last_heartbeat = NOW() - INTERVAL '30 seconds' WHERE engine_id = $1",
            &[&"deploy-e1"],
        )
        .await?;
    let stale = client
        .verify_deployment(VerifyDeploymentRequest {
            node_id: "node-a".into(),
            revision: 1,
        })
        .await?
        .into_inner();
    assert_eq!(stale.conditions[0].code, "STALE_ENGINE_HEARTBEAT");
    let stale_status = client
        .get_cluster_status(GetClusterStatusRequest {})
        .await?
        .into_inner();
    let stale_node = stale_status
        .nodes
        .iter()
        .find(|node| node.node_id == "node-a")
        .expect("status should retain stale desired node");
    assert_eq!(stale_node.severity, StatusSeverity::Unhealthy as i32);
    assert!(stale_node
        .conditions
        .iter()
        .any(|condition| condition.code == "STALE_ENGINE_HEARTBEAT"));
    client
        .heartbeat(HeartbeatRequest {
            engine_id: "deploy-e1".into(),
            healthy_modules: vec![ModuleDescriptor {
                name: "inventory".into(),
                namespace: "store".into(),
                version: "1.0.0".into(),
                proto_schema: vec![],
            }],
        })
        .await?;
    client
        .complete_deployment(CompleteDeploymentRequest {
            node_id: "node-a".into(),
            revision: 1,
            succeeded: true,
            failure_detail: String::new(),
        })
        .await?;

    let failed = client
        .begin_deployment(BeginDeploymentRequest {
            node_id: "node-a".into(),
            attempt_token: "attempt-two".into(),
            bundle_digest: digest_two.clone(),
            expected_engines: expected.clone(),
        })
        .await?
        .into_inner()
        .deployment
        .unwrap();
    assert_eq!(failed.revision, 2);
    client
        .complete_deployment(CompleteDeploymentRequest {
            node_id: "node-a".into(),
            revision: failed.revision,
            succeeded: false,
            failure_detail: "staging failed".into(),
        })
        .await?;
    let preserved = client
        .get_cluster_status(GetClusterStatusRequest {})
        .await?
        .into_inner();
    let preserved_node = preserved
        .nodes
        .iter()
        .find(|node| node.node_id == "node-a")
        .expect("status should retain the prior serving deployment");
    assert_eq!(preserved_node.severity, StatusSeverity::Healthy as i32);
    assert_eq!(preserved.services[0].healthy_routes, 1);
    assert_eq!(
        preserved_node
            .desired_deployment
            .as_ref()
            .expect("desired deployment")
            .revision,
        1
    );
    assert!(preserved_node.deployment_history.iter().any(|deployment| {
        deployment.revision == failed.revision && deployment.state == DeploymentState::Failed as i32
    }));

    let second = client
        .begin_deployment(BeginDeploymentRequest {
            node_id: "node-a".into(),
            attempt_token: "attempt-three".into(),
            bundle_digest: digest_two.clone(),
            expected_engines: expected,
        })
        .await?
        .into_inner()
        .deployment
        .unwrap();
    assert_eq!(second.revision, 3);
    let committed_during_overlap = client
        .verify_deployment(VerifyDeploymentRequest {
            node_id: "node-a".into(),
            revision: 1,
        })
        .await?
        .into_inner();
    assert!(
        committed_during_overlap.ready,
        "the committed source remains authoritative while revision 3 is staged"
    );
    let overlap_status = client
        .get_cluster_status(GetClusterStatusRequest {})
        .await?
        .into_inner();
    let overlap_node = overlap_status
        .nodes
        .iter()
        .find(|node| node.node_id == "node-a")
        .expect("overlap node");
    assert_eq!(
        overlap_node.desired_deployment.as_ref().unwrap().revision,
        1
    );
    assert_eq!(overlap_node.target_deployment.as_ref().unwrap().revision, 3);
    activate(
        &mut client,
        &pool,
        "deploy-e2",
        second.revision,
        &digest_two,
    )
    .await?;
    client
        .complete_deployment(CompleteDeploymentRequest {
            node_id: "node-a".into(),
            revision: second.revision,
            succeeded: true,
            failure_detail: String::new(),
        })
        .await?;

    let rollback = client
        .begin_rollback(BeginRollbackRequest {
            node_id: "node-a".into(),
            to_revision: 0,
            attempt_token: "rollback-one".into(),
        })
        .await?
        .into_inner()
        .deployment
        .unwrap();
    assert_eq!(rollback.revision, 4);
    assert_eq!(rollback.source_revision, 1);
    assert_eq!(rollback.bundle_digest, digest_one);
    let historical_count: i64 = pool
        .get()
        .await?
        .query_one(
            "SELECT COUNT(*) FROM wr_node_deployments WHERE node_id = 'node-a'",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(historical_count, 4);

    Ok(())
}

#[tokio::test]
async fn test_concurrent_deployment_revision_allocation_is_unique() -> Result<()> {
    let (_pool, _addr, client) = manager_trio().await?;
    let request = |token: &str| BeginDeploymentRequest {
        node_id: "concurrent-node".into(),
        attempt_token: token.into(),
        bundle_digest: format!("sha256:{}", "a".repeat(64)),
        expected_engines: vec![ExpectedEngine {
            engine_slot: "primary".into(),
            modules: vec![],
        }],
    };
    let mut left = client.clone();
    let mut right = client;
    let (left, right) = tokio::join!(
        left.begin_deployment(request("concurrent-left")),
        right.begin_deployment(request("concurrent-right")),
    );
    let outcomes = [left, right];
    let successes = outcomes.iter().filter(|result| result.is_ok()).count();
    let conflicts = outcomes
        .iter()
        .filter(|result| {
            result
                .as_ref()
                .is_err_and(|status| status.code() == tonic::Code::FailedPrecondition)
        })
        .count();
    assert_eq!(successes, 1, "only one staged target is allowed");
    assert_eq!(conflicts, 1);
    Ok(())
}
