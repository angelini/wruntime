mod helpers;
use helpers::{
    manager::{manager_trio, start_authorized_manager},
    proxy::TEST_SELF_PEER,
    wasm::minimal_file_descriptor_set,
};

use anyhow::Result;

use wr_common::wruntime::{
    AbandonDeploymentRequest, AttestNodeAgentRequest, BackendProcessState, BeginDeploymentRequest,
    BeginEngineDrainRequest, ClaimOperationRequest, DeploymentInventoryV1, DeploymentMetadata,
    DeploymentState, DeregisterEngineRequest, EngineOwnershipFence, EngineRegistration,
    ExpectedEngine, ExpectedModule, FinalizeDeploymentRequest, GetClusterStatusRequest,
    GetOperatorStatusRequest, GetRoutingTableRequest, GetSchemaRequest, HeartbeatRequest,
    ListEnginesRequest, ModuleDescriptor, ModuleIdentity, NodeOperationAction,
    NodeOperationStepKind, PutNodeAgentPolicyRequest, RegisterEngineRequest,
    ReportNodeObservationRequest, ReportStepResultRequest, ResumeOperationRequest, RolloutPolicy,
    RoutingRule, SecretRequest, StatusSeverity, SubmitOperationRequest, VerifyDeploymentRequest,
    VerifyDeploymentResponse,
};

async fn verify_deployment(
    pool: &deadpool_postgres::Pool,
    node_id: &str,
    revision: u64,
) -> Result<VerifyDeploymentResponse> {
    let deployment = wr_manager::db::get_deployment(pool, node_id, revision)
        .await?
        .record;
    let conditions = wr_manager::db::deployment_conditions(pool, &deployment, 10.0, 10.0)
        .await?
        .into_iter()
        .map(|(code, detail)| wr_common::wruntime::DeploymentCondition {
            code,
            detail,
            ..Default::default()
        })
        .collect::<Vec<_>>();
    Ok(VerifyDeploymentResponse {
        deployment: Some(deployment),
        ready: conditions.is_empty(),
        conditions,
    })
}

#[tokio::test]
async fn test_register_and_list_engines() -> Result<()> {
    let (pool, _addr, mut c) = manager_trio().await?;

    helpers::manager::register_managed_engine(
        &pool,
        &mut c,
        EngineRegistration {
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
            job_queue_id: String::new(),
            job_admin_address: String::new(),
        },
    )
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
async fn job_admin_registration_is_atomic_validated_and_persisted() -> Result<()> {
    let (pool, _addr, mut client) = manager_trio().await?;
    let registration = EngineRegistration {
        engine_id: "job-engine".into(),
        address: "http://127.0.0.1:9190".into(),
        proxy_address: TEST_SELF_PEER.into(),
        peer_address: TEST_SELF_PEER.into(),
        modules: vec![],
        secrets: vec![],
        db_namespaces: vec![],
        deployment: None,
        job_queue_id: "primary-jobs".into(),
        job_admin_address: "https://127.0.0.1:9150/".into(),
    };
    helpers::manager::register_managed_engine(&pool, &mut client, registration.clone()).await?;

    let listed = client
        .list_engines(ListEnginesRequest {})
        .await?
        .into_inner()
        .engines;
    assert_eq!(listed[0].job_queue_id, "primary-jobs");
    assert_eq!(listed[0].job_admin_address, "https://127.0.0.1:9150/");
    let delegates = wr_manager::db::list_job_admin_delegates(&pool, 30).await?;
    assert_eq!(delegates.len(), 1);
    assert!(delegates[0].fresh);
    assert_eq!(delegates[0].engine_id, "job-engine");

    for (queue_id, address) in [
        ("primary-jobs", ""),
        ("", "https://127.0.0.1:9151/"),
        ("Invalid_Queue", "https://127.0.0.1:9151/"),
        ("other-jobs", "http://127.0.0.1:9151"),
        ("other-jobs", "https://0.0.0.0:9151"),
    ] {
        let mut malformed = registration.clone();
        malformed.engine_id = format!("bad-{}", malformed.engine_id);
        malformed.job_queue_id = queue_id.into();
        malformed.job_admin_address = address.into();
        let status = helpers::manager::register_managed_engine(&pool, &mut client, malformed)
            .await
            .expect_err("malformed job delegate must be rejected");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    Ok(())
}

#[tokio::test]
async fn test_deregister_engine() -> Result<()> {
    let (pool, _addr, mut c) = manager_trio().await?;

    let registration = helpers::manager::register_managed_engine(
        &pool,
        &mut c,
        EngineRegistration {
            engine_id: "e1".into(),
            address: "http://127.0.0.1:9101".into(),
            proxy_address: TEST_SELF_PEER.into(),
            peer_address: TEST_SELF_PEER.into(),
            modules: vec![],
            secrets: vec![],
            db_namespaces: vec![],
            deployment: None,
            job_queue_id: String::new(),
            job_admin_address: String::new(),
        },
    )
    .await?;

    c.deregister_engine(DeregisterEngineRequest {
        engine_id: "e1".into(),

        fence: registration.into_inner().fence,
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
    let (pool, _addr, mut c) = manager_trio().await?;

    let registration = helpers::manager::register_managed_engine(
        &pool,
        &mut c,
        EngineRegistration {
            engine_id: "e1".into(),
            address: "http://127.0.0.1:9102".into(),
            proxy_address: TEST_SELF_PEER.into(),
            peer_address: TEST_SELF_PEER.into(),
            modules: vec![],
            secrets: vec![],
            db_namespaces: vec![],
            deployment: None,
            job_queue_id: String::new(),
            job_admin_address: String::new(),
        },
    )
    .await?;

    c.heartbeat(HeartbeatRequest {
        engine_id: "e1".into(),
        healthy_modules: vec![],

        fence: registration.into_inner().fence,
    })
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_readiness_and_drain_are_atomic_versioned_and_fenced() -> Result<()> {
    let (pool, _addr, mut client) = manager_trio().await?;
    let registration = helpers::manager::register_managed_engine(
        &pool,
        &mut client,
        EngineRegistration {
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
            job_queue_id: String::new(),
            job_admin_address: String::new(),
        },
    )
    .await?;
    let fence = registration.into_inner().fence;

    let readiness = client
        .heartbeat(HeartbeatRequest {
            engine_id: "lifecycle-engine".into(),
            healthy_modules: vec![ModuleDescriptor {
                name: "lifecycle-service".into(),
                namespace: "store".into(),
                version: "1.0.0".into(),
                proto_schema: vec![],
            }],

            fence: fence.clone(),
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

            fence: fence.clone(),
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

            fence: fence.clone(),
        })
        .await
    {
        Ok(_) => anyhow::bail!("draining engine heartbeat was not fenced"),
        Err(status) => status,
    };
    assert_eq!(stale.code(), tonic::Code::FailedPrecondition);

    client
        .deregister_engine(DeregisterEngineRequest {
            engine_id: "lifecycle-engine".into(),
            fence,
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
    let (pool, _addr, mut c) = manager_trio().await?;

    let schema_bytes = minimal_file_descriptor_set();

    helpers::manager::register_managed_engine(
        &pool,
        &mut c,
        EngineRegistration {
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
            job_queue_id: String::new(),
            job_admin_address: String::new(),
        },
    )
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
    let (pool, _addr, mut c) = manager_trio().await?;

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
    helpers::manager::register_managed_engine(
        &pool,
        &mut c,
        EngineRegistration {
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
            job_queue_id: String::new(),
            job_admin_address: String::new(),
        },
    )
    .await?;

    // Register v2 from a different engine.
    helpers::manager::register_managed_engine(
        &pool,
        &mut c,
        EngineRegistration {
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
            job_queue_id: String::new(),
            job_admin_address: String::new(),
        },
    )
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
    let (pool, _addr, mut c) = manager_trio().await?;

    let schema = minimal_file_descriptor_set();

    // Register same module name in two different namespaces.
    helpers::manager::register_managed_engine(
        &pool,
        &mut c,
        EngineRegistration {
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
            job_queue_id: String::new(),
            job_admin_address: String::new(),
        },
    )
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
    let (pool, _addr, mut c) = manager_trio().await?;

    let schema_v1 = minimal_file_descriptor_set();

    // Initial registration.
    helpers::manager::register_managed_engine(
        &pool,
        &mut c,
        EngineRegistration {
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
            job_queue_id: String::new(),
            job_admin_address: String::new(),
        },
    )
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

    helpers::manager::register_managed_engine(
        &pool,
        &mut c,
        EngineRegistration {
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
            job_queue_id: String::new(),
            job_admin_address: String::new(),
        },
    )
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
    let (pool, _addr, mut c) = manager_trio().await?;

    let schema = minimal_file_descriptor_set();

    // Register one engine with two modules.
    helpers::manager::register_managed_engine(
        &pool,
        &mut c,
        EngineRegistration {
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
            job_queue_id: String::new(),
            job_admin_address: String::new(),
        },
    )
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
    let (pool, _addr, mut c) = manager_trio().await?;

    helpers::manager::register_managed_engine(
        &pool,
        &mut c,
        EngineRegistration {
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
            job_queue_id: String::new(),
            job_admin_address: String::new(),
        },
    )
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
    let (pool, _addr, mut c) = manager_trio().await?;
    let schema = minimal_file_descriptor_set();

    helpers::manager::register_managed_engine(
        &pool,
        &mut c,
        EngineRegistration {
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
            job_queue_id: String::new(),
            job_admin_address: String::new(),
        },
    )
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
    let (pool, _addr, mut c) = manager_trio().await?;

    let err = helpers::manager::register_managed_engine(
        &pool,
        &mut c,
        EngineRegistration {
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
            job_queue_id: String::new(),
            job_admin_address: String::new(),
        },
    )
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
    let (pool, _addr, mut c) = manager_trio().await?;

    let err = helpers::manager::register_managed_engine(
        &pool,
        &mut c,
        EngineRegistration {
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
            job_queue_id: String::new(),
            job_admin_address: String::new(),
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);
    assert!(err.message().contains("missing secret"));

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

    helpers::manager::register_managed_engine(
        &pool,
        &mut c,
        EngineRegistration {
            engine_id: "recon-e1".into(),
            address: "http://127.0.0.1:9640".into(),
            proxy_address: TEST_SELF_PEER.into(),
            peer_address: "https://127.0.0.1:9443".into(),
            modules: vec![module("alpha"), module("beta")],
            secrets: vec![],
            db_namespaces: vec![],
            deployment: None,
            job_queue_id: String::new(),
            job_admin_address: String::new(),
        },
    )
    .await?;

    let v_before: i64 = pool
        .get()
        .await?
        .query_one("SELECT version FROM wr_manager_lock WHERE id = 1", &[])
        .await?
        .get(0);

    helpers::manager::register_managed_engine(
        &pool,
        &mut c,
        EngineRegistration {
            engine_id: "recon-e1".into(),
            address: "http://127.0.0.1:9640".into(),
            proxy_address: TEST_SELF_PEER.into(),
            peer_address: "https://127.0.0.1:9443".into(),
            modules: vec![module("alpha")],
            secrets: vec![],
            db_namespaces: vec![],
            deployment: None,
            job_queue_id: String::new(),
            job_admin_address: String::new(),
        },
    )
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

    helpers::manager::register_managed_engine(
        &pool,
        &mut c,
        EngineRegistration {
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
            job_queue_id: String::new(),
            job_admin_address: String::new(),
        },
    )
    .await?;

    let v_before: i64 = pool
        .get()
        .await?
        .query_one("SELECT version FROM wr_manager_lock WHERE id = 1", &[])
        .await?
        .get(0);

    helpers::manager::register_managed_engine(
        &pool,
        &mut c,
        EngineRegistration {
            engine_id: "recon-e2".into(),
            address: "http://127.0.0.1:9650".into(),
            proxy_address: TEST_SELF_PEER.into(),
            peer_address: TEST_SELF_PEER.into(),
            modules: vec![],
            secrets: vec![],
            db_namespaces: vec![],
            deployment: None,
            job_queue_id: String::new(),
            job_admin_address: String::new(),
        },
    )
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
        modules: vec![ExpectedModule {
            identity: Some(ModuleIdentity {
                namespace: "store".into(),
                name: "inventory".into(),
                version: "1.0.0".into(),
            }),
            proto_schema_digest: wr_common::deployment_contract::schema_digest(
                &minimal_file_descriptor_set(),
            ),
        }],
        ..Default::default()
    }];
    let digest_one = format!("sha256:{}", "1".repeat(64));
    let digest_two = format!("sha256:{}", "2".repeat(64));
    let actor = "urn:wruntime:cluster-a:human:test-admin";

    let first = wr_manager::db::begin_deployment(
        &pool,
        &BeginDeploymentRequest {
            node_id: "node-a".into(),
            attempt_token: "attempt-one".into(),
            bundle_digest: digest_one.clone(),
            inventory: Some(DeploymentInventoryV1 {
                schema_version: 1,
                engines: expected.clone(),
            }),
        },
        actor,
    )
    .await?
    .record;
    assert_eq!(first.revision, 1);
    let retry = wr_manager::db::begin_deployment(
        &pool,
        &BeginDeploymentRequest {
            node_id: "node-a".into(),
            attempt_token: "attempt-one".into(),
            bundle_digest: digest_one.clone(),
            inventory: Some(DeploymentInventoryV1 {
                schema_version: 1,
                engines: expected.clone(),
            }),
        },
        actor,
    )
    .await?
    .record;
    assert_eq!(retry.revision, first.revision);
    let conflict = wr_manager::db::begin_deployment(
        &pool,
        &BeginDeploymentRequest {
            node_id: "node-a".into(),
            attempt_token: "attempt-one".into(),
            bundle_digest: digest_two.clone(),
            inventory: Some(DeploymentInventoryV1 {
                schema_version: 1,
                engines: expected.clone(),
            }),
        },
        actor,
    )
    .await
    .unwrap_err();
    assert_eq!(conflict.code(), tonic::Code::AlreadyExists);

    let missing = verify_deployment(&pool, "node-a", first.revision).await?;
    assert!(!missing.ready);
    assert_eq!(missing.conditions[0].code, "MISSING_ENGINE");

    async fn activate(
        client: &mut wr_common::manager_client::ManagerClient<tonic::transport::Channel>,
        pool: &deadpool_postgres::Pool,
        engine_id: &str,
        revision: u64,
        digest: &str,
        revision_digest: &str,
        attempt_token: &str,
    ) -> Result<EngineOwnershipFence> {
        let resolved_release_digest = format!("sha256:{}", "b".repeat(64));
        wr_manager::db::finalize_deployment(
            pool,
            &FinalizeDeploymentRequest {
                node_id: "node-a".into(),
                attempt_token: attempt_token.into(),
                revision,
                bundle_digest: digest.into(),
                resolved_release_digest: resolved_release_digest.clone(),
            },
            "urn:wruntime:cluster-a:human:test-admin",
        )
        .await?;
        let operation = client
            .submit_operation(SubmitOperationRequest {
                node_id: "node-a".into(),
                request_token: attempt_token.into(),
                action: if revision == 1 {
                    NodeOperationAction::InitialApply as i32
                } else {
                    NodeOperationAction::RollingUpgrade as i32
                },
                engine_slots: vec!["primary".into()],
                target_revision: revision,
                bundle_digest: digest.into(),
                policy: Some(RolloutPolicy {
                    max_unavailable: 1,
                    canary_slot: "primary".into(),
                    pause_after_canary: false,
                    allow_downtime: true,
                    deadline_seconds: 300,
                }),
                resolved_release_digest,
            })
            .await?
            .into_inner()
            .operation
            .ok_or_else(|| anyhow::anyhow!("activation operation missing"))?;
        let module = ModuleDescriptor {
            name: "inventory".into(),
            namespace: "store".into(),
            version: "1.0.0".into(),
            proto_schema: minimal_file_descriptor_set(),
        };
        let registration = client
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
                        operation_id: operation.operation_id.clone(),
                        revision_digest: revision_digest.into(),
                    }),
                    job_queue_id: String::new(),
                    job_admin_address: String::new(),
                }),
                activation_id: uuid::Uuid::new_v4().to_string(),
            })
            .await?
            .into_inner();
        pool.get()
            .await?
            .execute(
                "UPDATE wr_node_operations SET state='succeeded', phase='complete', updated_at=NOW() WHERE operation_id=$1",
                &[&uuid::Uuid::parse_str(&operation.operation_id)?],
            )
            .await?;
        let revision = i64::try_from(revision)?;
        let mut db = pool.get().await?;
        let transaction = db.transaction().await?;
        transaction
            .execute(
                "UPDATE wr_node_slot_authority SET authoritative = FALSE, updated_at = NOW()
                 WHERE node_id = 'node-a' AND engine_slot = 'primary' AND authoritative",
                &[],
            )
            .await?;
        transaction
            .execute(
                "INSERT INTO wr_node_slot_authority
                   (node_id, engine_slot, revision, authoritative)
                 VALUES ('node-a', 'primary', $1, TRUE)
                 ON CONFLICT (node_id, engine_slot, revision) DO UPDATE SET
                   authoritative = TRUE, updated_at = NOW()",
                &[&revision],
            )
            .await?;
        transaction.commit().await?;
        client
            .heartbeat(HeartbeatRequest {
                engine_id: engine_id.into(),
                healthy_modules: vec![module],
                fence: registration.fence.clone(),
            })
            .await?;
        wr_manager::db::update_route_health(pool, 10.0, 10.0).await?;
        registration
            .fence
            .ok_or_else(|| anyhow::anyhow!("registration fence missing"))
    }

    let first_fence = activate(
        &mut client,
        &pool,
        "deploy-e1",
        1,
        &digest_one,
        &first.revision_digest,
        "attempt-one",
    )
    .await?;
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
    let stale = verify_deployment(&pool, "node-a", 1).await?;
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

            fence: Some(first_fence),
        })
        .await?;
    wr_manager::db::complete_deployment(&pool, "node-a", 1, true, "").await?;

    let failed = wr_manager::db::begin_deployment(
        &pool,
        &BeginDeploymentRequest {
            node_id: "node-a".into(),
            attempt_token: "attempt-two".into(),
            bundle_digest: digest_two.clone(),
            inventory: Some(DeploymentInventoryV1 {
                schema_version: 1,
                engines: expected.clone(),
            }),
        },
        actor,
    )
    .await?
    .record;
    assert_eq!(failed.revision, 2);
    wr_manager::db::complete_deployment(&pool, "node-a", failed.revision, false, "staging failed")
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

    let second = wr_manager::db::begin_deployment(
        &pool,
        &BeginDeploymentRequest {
            node_id: "node-a".into(),
            attempt_token: "attempt-three".into(),
            bundle_digest: digest_two.clone(),
            inventory: Some(DeploymentInventoryV1 {
                schema_version: 1,
                engines: expected,
            }),
        },
        actor,
    )
    .await?
    .record;
    assert_eq!(second.revision, 3);
    let committed_during_overlap = verify_deployment(&pool, "node-a", 1).await?;
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
    let conflicting_rollback =
        wr_manager::db::begin_rollback(&pool, "node-a", 1, "rollback-while-staged", actor)
            .await
            .expect_err("rollback must not overwrite an existing staged target");
    assert_eq!(conflicting_rollback.code(), tonic::Code::FailedPrecondition);
    let _second_fence = activate(
        &mut client,
        &pool,
        "deploy-e2",
        second.revision,
        &digest_two,
        &second.revision_digest,
        "attempt-three",
    )
    .await?;
    wr_manager::db::complete_deployment(&pool, "node-a", second.revision, true, "").await?;

    let rollback = wr_manager::db::begin_rollback(&pool, "node-a", 0, "rollback-one", actor)
        .await?
        .record;
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
    let (pool, _addr, _client) = manager_trio().await?;
    let request = |token: &str| BeginDeploymentRequest {
        node_id: "concurrent-node".into(),
        attempt_token: token.into(),
        bundle_digest: format!("sha256:{}", "a".repeat(64)),
        inventory: Some(DeploymentInventoryV1 {
            schema_version: 1,
            engines: vec![ExpectedEngine {
                engine_slot: "primary".into(),
                modules: vec![],
                ..Default::default()
            }],
        }),
    };
    let left_request = request("concurrent-left");
    let right_request = request("concurrent-right");
    let (left, right) = tokio::join!(
        wr_manager::db::begin_deployment(&pool, &left_request, "operator-a"),
        wr_manager::db::begin_deployment(&pool, &right_request, "operator-a"),
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

#[tokio::test]
async fn role_gated_services_enforce_real_mtls_identity_and_node_binding() -> Result<()> {
    let pool = helpers::db::manager_pool().await;
    let server = start_authorized_manager(pool).await?;

    let mut viewer = server.operator_client(Some(&server.pki.viewer)).await?;
    viewer
        .get_status(GetOperatorStatusRequest {
            node_id: String::new(),
            engine_slot: String::new(),
        })
        .await?;
    let denied = viewer
        .submit_operation(SubmitOperationRequest {
            node_id: "node-a".into(),
            request_token: "viewer-denied".into(),
            action: NodeOperationAction::Restart as i32,
            engine_slots: vec!["blue".into()],
            target_revision: 0,
            bundle_digest: String::new(),
            policy: Some(RolloutPolicy {
                max_unavailable: 1,
                canary_slot: "blue".into(),
                pause_after_canary: false,
                allow_downtime: true,
                deadline_seconds: 300,
            }),
            resolved_release_digest: String::new(),
        })
        .await
        .expect_err("viewer mutation must be denied");
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);
    let denied = viewer
        .begin_deployment(BeginDeploymentRequest {
            node_id: "viewer-node".into(),
            attempt_token: "viewer-allocation".into(),
            bundle_digest: format!("sha256:{}", "a".repeat(64)),
            inventory: Some(DeploymentInventoryV1 {
                schema_version: 1,
                engines: vec![],
            }),
        })
        .await
        .expect_err("viewer allocation must be denied");
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);

    let mut unknown = server.operator_client(Some(&server.pki.unknown)).await?;
    let denied = unknown
        .get_status(GetOperatorStatusRequest {
            node_id: String::new(),
            engine_slot: String::new(),
        })
        .await
        .expect_err("unmapped certificate must be denied");
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);
    let mut missing_certificate = server.operator_client(None).await?;
    let denied = missing_certificate
        .get_status(GetOperatorStatusRequest {
            node_id: String::new(),
            engine_slot: String::new(),
        })
        .await
        .expect_err("a request without a client certificate must be denied");
    assert_eq!(
        denied.code(),
        tonic::Code::Unknown,
        "tonic maps the mTLS handshake rejection to a transport status"
    );

    let mut agent = server.agent_client(Some(&server.pki.agent_a)).await?;
    let denied = agent
        .attest(AttestNodeAgentRequest {
            attestation: Some(helpers::node_agent::attestation(
                &helpers::node_agent::systemd_policy("node-b", 2),
                "activation-a",
            )),
        })
        .await
        .expect_err("node-a certificate must not act for node-b");
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);

    Ok(())
}

#[tokio::test]
async fn rotated_operator_identity_and_attestation_policy_are_applied_over_mtls() -> Result<()> {
    let pool = helpers::db::manager_pool().await;
    let server = start_authorized_manager(pool.clone()).await?;
    let policy = helpers::node_agent::systemd_policy("node-a", 2);
    let mut inconsistent_policy = policy.clone();
    inconsistent_policy.retention_count += 1;
    let rejected = server
        .operator_client(Some(&server.pki.operator))
        .await?
        .put_node_agent_policy(PutNodeAgentPolicyRequest {
            policy: Some(inconsistent_policy),
        })
        .await
        .expect_err("manager must recompute the canonical config digest");
    assert_eq!(rejected.code(), tonic::Code::InvalidArgument);
    for identity in [&server.pki.operator, &server.pki.operator_rotated] {
        server
            .operator_client(Some(identity))
            .await?
            .put_node_agent_policy(PutNodeAgentPolicyRequest {
                policy: Some(policy.clone()),
            })
            .await?;
    }
    let actor: String = pool
        .get()
        .await?
        .query_one(
            "SELECT actor FROM wr_node_agent_policies WHERE node_id = 'node-a'",
            &[],
        )
        .await?
        .get("actor");
    assert_eq!(
        actor, "urn:wruntime:cluster-a:human:operator-a",
        "rotated certificates retain one principal"
    );

    server
        .operator_client(Some(&server.pki.operator))
        .await?
        .begin_deployment(BeginDeploymentRequest {
            node_id: "abandon-node".into(),
            attempt_token: "abandon-token".into(),
            bundle_digest: format!("sha256:{}", "d".repeat(64)),
            inventory: Some(DeploymentInventoryV1 {
                schema_version: 1,
                engines: vec![ExpectedEngine {
                    engine_slot: "blue".into(),
                    modules: vec![],
                    ..Default::default()
                }],
            }),
        })
        .await?;
    let abandoned = server
        .operator_client(Some(&server.pki.operator_rotated))
        .await?
        .abandon_deployment(AbandonDeploymentRequest {
            node_id: "abandon-node".into(),
            attempt_token: "abandon-token".into(),
        })
        .await?
        .into_inner();
    assert!(abandoned.abandoned);
    let abandoned_by: String = pool
        .get()
        .await?
        .query_one(
            "SELECT abandoned_by FROM wr_node_deployments
             WHERE node_id = 'abandon-node' AND attempt_token = 'abandon-token'",
            &[],
        )
        .await?
        .get("abandoned_by");
    assert_eq!(abandoned_by, "urn:wruntime:cluster-a:human:operator-a");

    let mut agent = server.agent_client(Some(&server.pki.agent_a)).await?;
    let mut mismatches = Vec::new();
    let mut protocol = helpers::node_agent::attestation(&policy, "activation-protocol");
    protocol.protocol_version = "wrong".into();
    mismatches.push(("PROTOCOL_MISMATCH", protocol));
    let mut binary = helpers::node_agent::attestation(&policy, "activation-binary");
    binary.binary_digest = format!("sha256:{}", "b".repeat(64));
    mismatches.push(("BINARY_MISMATCH", binary));
    let mut config = helpers::node_agent::attestation(&policy, "activation-config");
    config.config_digest = format!("sha256:{}", "c".repeat(64));
    mismatches.push(("CONFIG_MISMATCH", config));
    let mut backend = helpers::node_agent::attestation(&policy, "activation-backend");
    backend.backend = wr_common::wruntime::BackendKind::Docker as i32;
    mismatches.push(("BACKEND_MISMATCH", backend));
    let mut retention = helpers::node_agent::attestation(&policy, "activation-retention");
    retention.retention_count += 1;
    mismatches.push(("RETENTION_MISMATCH", retention));
    let mut capability = helpers::node_agent::attestation(&policy, "activation-capability");
    capability.capabilities.pop();
    mismatches.push(("CAPABILITY_MISMATCH", capability));
    for (expected_code, attestation) in mismatches {
        let mismatch = agent
            .attest(AttestNodeAgentRequest {
                attestation: Some(attestation),
            })
            .await?
            .into_inner();
        assert!(!mismatch.accepted);
        assert!(mismatch
            .conditions
            .iter()
            .any(|condition| condition.code == expected_code));
    }

    let accepted = agent
        .attest(AttestNodeAgentRequest {
            attestation: Some(helpers::node_agent::attestation(&policy, "activation-a")),
        })
        .await?
        .into_inner();
    assert!(accepted.accepted);

    Ok(())
}

#[tokio::test]
async fn resumed_agent_receives_epoch_bound_inspection_through_node_agent_rpc() -> Result<()> {
    let pool = helpers::db::manager_pool().await;
    let server = start_authorized_manager(pool.clone()).await?;
    let mut operator = server.operator_client(Some(&server.pki.operator)).await?;
    operator
        .put_node_agent_policy(PutNodeAgentPolicyRequest {
            policy: Some(helpers::node_agent::systemd_policy("node-a", 2)),
        })
        .await?;
    let digest = format!("sha256:{}", "9".repeat(64));
    let deployment = wr_manager::db::begin_deployment(
        &pool,
        &BeginDeploymentRequest {
            node_id: "node-a".into(),
            attempt_token: "rpc-resume".into(),
            bundle_digest: digest.clone(),
            inventory: Some(DeploymentInventoryV1 {
                schema_version: 1,
                engines: vec![ExpectedEngine {
                    engine_slot: "blue".into(),
                    modules: vec![],
                    ..Default::default()
                }],
            }),
        },
        "urn:wruntime:cluster-a:human:operator-a",
    )
    .await?
    .record;
    operator
        .finalize_deployment(FinalizeDeploymentRequest {
            node_id: "node-a".into(),
            attempt_token: "rpc-resume".into(),
            revision: deployment.revision,
            bundle_digest: digest.clone(),
            resolved_release_digest: format!("sha256:{}", "8".repeat(64)),
        })
        .await?;
    let operation = operator
        .submit_operation(SubmitOperationRequest {
            node_id: "node-a".into(),
            request_token: "rpc-resume".into(),
            action: NodeOperationAction::InitialApply as i32,
            engine_slots: vec!["blue".into()],
            target_revision: deployment.revision,
            bundle_digest: digest.clone(),
            policy: Some(RolloutPolicy {
                max_unavailable: 1,
                canary_slot: "blue".into(),
                pause_after_canary: false,
                allow_downtime: true,
                deadline_seconds: 300,
            }),
            resolved_release_digest: format!("sha256:{}", "8".repeat(64)),
        })
        .await?
        .into_inner()
        .operation
        .expect("submitted operation");
    let mut agent = server.agent_client(Some(&server.pki.agent_a)).await?;
    for activation in ["activation-a", "activation-b"] {
        let accepted = agent
            .attest(AttestNodeAgentRequest {
                attestation: Some(helpers::node_agent::attestation(
                    &helpers::node_agent::systemd_policy("node-a", 2),
                    activation,
                )),
            })
            .await?
            .into_inner();
        assert!(accepted.accepted);
    }
    for expected in [
        NodeOperationStepKind::SelectRelease,
        NodeOperationStepKind::StartBackend,
        NodeOperationStepKind::VerifyProxy,
    ] {
        let instruction = agent
            .claim_operation(ClaimOperationRequest {
                node_id: "node-a".into(),
                agent_instance_id: "activation-a".into(),
            })
            .await?
            .into_inner()
            .instruction
            .expect("proxy operation instruction");
        assert_eq!(instruction.step, expected as i32);
        let target = instruction.target.as_ref().expect("proxy target");
        assert!(target.engine_slot.is_empty());
        agent
            .report_step_result(ReportStepResultRequest {
                node_id: "node-a".into(),
                operation_id: operation.operation_id.clone(),
                lease_epoch: instruction.lease_epoch,
                step: instruction.step,
                agent_instance_id: "activation-a".into(),
                observed_revision: target.revision,
                observed_digest: target.bundle_digest.clone(),
                observed_resolved_release_digest: target.resolved_release_digest.clone(),
                backend_instance_id: "proxy-backend".into(),
                process_instance_id: "proxy-process".into(),
                ..Default::default()
            })
            .await?;
    }
    let verify_release = agent
        .claim_operation(ClaimOperationRequest {
            node_id: "node-a".into(),
            agent_instance_id: "activation-a".into(),
        })
        .await?
        .into_inner()
        .instruction
        .expect("verify release instruction");
    assert_eq!(
        verify_release.step,
        NodeOperationStepKind::VerifyReleaseMetadata as i32
    );
    let target = verify_release.target.as_ref().expect("engine target");
    agent
        .report_step_result(ReportStepResultRequest {
            node_id: "node-a".into(),
            operation_id: operation.operation_id.clone(),
            engine_slot: "blue".into(),
            lease_epoch: verify_release.lease_epoch,
            step: verify_release.step,
            agent_instance_id: "activation-a".into(),
            observed_revision: target.revision,
            observed_digest: target.bundle_digest.clone(),
            observed_resolved_release_digest: target.resolved_release_digest.clone(),
            ..Default::default()
        })
        .await?;
    let select = agent
        .claim_operation(ClaimOperationRequest {
            node_id: "node-a".into(),
            agent_instance_id: "activation-a".into(),
        })
        .await?
        .into_inner()
        .instruction
        .expect("select instruction");
    assert_eq!(select.step, NodeOperationStepKind::SelectRelease as i32);
    pool.get()
        .await?
        .execute(
            "UPDATE wr_node_operations SET lease_expires_at = NOW() - INTERVAL '1 second'
             WHERE operation_id = $1",
            &[&uuid::Uuid::parse_str(&operation.operation_id)?],
        )
        .await?;
    assert!(agent
        .claim_operation(ClaimOperationRequest {
            node_id: "node-a".into(),
            agent_instance_id: "activation-a".into(),
        })
        .await?
        .into_inner()
        .instruction
        .is_none());
    operator
        .resume_operation(ResumeOperationRequest {
            operation_id: operation.operation_id.clone(),
        })
        .await?;
    let inspection = agent
        .claim_operation(ClaimOperationRequest {
            node_id: "node-a".into(),
            agent_instance_id: "activation-b".into(),
        })
        .await?
        .into_inner()
        .instruction
        .expect("replacement activation receives inspection context");
    assert_eq!(
        inspection.step,
        NodeOperationStepKind::InspectBackend as i32
    );
    assert_eq!(inspection.operation_id, operation.operation_id);
    assert_eq!(inspection.agent_instance_id, "activation-b");
    assert!(inspection.lease_epoch > select.lease_epoch);
    assert_eq!(inspection.target.as_ref().unwrap().engine_slot, "blue");
    agent
        .report_observation(ReportNodeObservationRequest {
            node_id: "node-a".into(),
            engine_slot: "blue".into(),
            backend_state: BackendProcessState::Exited as i32,
            backend_instance_id: "pre-effect".into(),
            observed_revision: 0,
            observed_digest: String::new(),
            operation_id: inspection.operation_id.clone(),
            agent_instance_id: inspection.agent_instance_id.clone(),
            lease_epoch: inspection.lease_epoch,
            ..Default::default()
        })
        .await?;
    let reissued = agent
        .claim_operation(ClaimOperationRequest {
            node_id: "node-a".into(),
            agent_instance_id: "activation-b".into(),
        })
        .await?
        .into_inner()
        .instruction
        .expect("fresh unchanged evidence permits fenced effect retry");
    assert_eq!(reissued.step, NodeOperationStepKind::SelectRelease as i32);
    assert_eq!(reissued.operation_id, operation.operation_id);
    assert_eq!(reissued.lease_epoch, inspection.lease_epoch);

    Ok(())
}

#[tokio::test]
async fn manager_rollout_lease_phases_and_activation_barriers_are_fenced() -> Result<()> {
    use wr_common::wruntime::{
        BeginManagerRolloutRequest, ManagerRolloutPhase, ManagerRolloutTarget,
    };

    let pool = helpers::db::manager_pool().await;
    let target_digest = format!("sha256:{}", "a".repeat(64));
    wr_manager::db::register_manager(&pool, "manager-a", "https://manager-a:9000").await?;
    wr_manager::db::initialize_manager_policy_state(&pool, "manager-a", 2, &target_digest).await?;
    let request = BeginManagerRolloutRequest {
        client_operation_id: "83c54ac7-fe23-4ddf-96e5-0da2e4b18d7d".into(),
        cluster_id: "cluster-a".into(),
        target_generation: 2,
        target_policy_digest: target_digest.clone(),
        expected_targets: vec![ManagerRolloutTarget {
            manager_id: "manager-a".into(),
            endpoint: "https://manager-a:9000".into(),
            host_digest: format!("sha256:{}", "b".repeat(64)),
            config_digest: format!("sha256:{}", "c".repeat(64)),
            ..Default::default()
        }],
        recovery_of: String::new(),
        target_policy_validator_version: 1,
        target_deployment_principal_uri: "urn:wruntime:cluster-a:human:deployer".into(),
        target_deployment_leaf_fingerprint: format!("sha256:{}", "d".repeat(64)),
        ..Default::default()
    };
    let rollout = wr_manager::db::begin_manager_rollout(
        &pool,
        &request.target_deployment_principal_uri,
        &request.target_deployment_leaf_fingerprint,
        &request,
        &format!("sha256:{}", "e".repeat(64)),
        false,
    )
    .await?;
    let owner = "11111111-1111-4111-8111-111111111111";
    let lease = wr_manager::db::lease_manager_rollout(&pool, &rollout.rollout_id, owner, 0).await?;
    assert_eq!(lease.lease_epoch, 1);
    let denied = wr_manager::db::lease_manager_rollout(
        &pool,
        &rollout.rollout_id,
        "22222222-2222-4222-8222-222222222222",
        1,
    )
    .await
    .unwrap_err();
    assert_eq!(denied.code(), tonic::Code::FailedPrecondition);

    let advance = |expected: ManagerRolloutPhase, next: ManagerRolloutPhase| {
        wr_manager::db::advance_manager_rollout(
            &pool,
            &rollout.rollout_id,
            owner,
            1,
            expected as i32,
            next as i32,
            &[],
        )
    };
    advance(ManagerRolloutPhase::Prepared, ManagerRolloutPhase::Staging).await?;
    advance(
        ManagerRolloutPhase::Staging,
        ManagerRolloutPhase::ClosingOld,
    )
    .await?;
    advance(
        ManagerRolloutPhase::ClosingOld,
        ManagerRolloutPhase::OldClosed,
    )
    .await?;
    advance(
        ManagerRolloutPhase::OldClosed,
        ManagerRolloutPhase::StartingTarget,
    )
    .await?;
    pool.get().await?.execute(
        "UPDATE wr_manager_rollout_members SET process_state='READY',admission_state='CLOSED_ROLLOUT',observed_policy_generation=2,observed_policy_digest=$2 WHERE rollout_id=$1 AND member_role='target'",
        &[&uuid::Uuid::parse_str(&rollout.rollout_id)?, &target_digest],
    ).await?;
    advance(
        ManagerRolloutPhase::StartingTarget,
        ManagerRolloutPhase::TargetReadyClosed,
    )
    .await?;
    advance(
        ManagerRolloutPhase::TargetReadyClosed,
        ManagerRolloutPhase::ActivatingTarget,
    )
    .await?;
    pool.get().await?.execute(
        "UPDATE wr_manager_rollout_members SET admission_state='OPEN' WHERE rollout_id=$1 AND member_role='target'",
        &[&uuid::Uuid::parse_str(&rollout.rollout_id)?],
    ).await?;
    advance(
        ManagerRolloutPhase::ActivatingTarget,
        ManagerRolloutPhase::Completed,
    )
    .await?;
    let guard = pool.get().await?.query_one(
        "SELECT accepted_generation,accepted_digest,active_rollout_id FROM wr_manager_rollout_guard WHERE singleton",
        &[],
    ).await?;
    assert_eq!(guard.get::<_, Option<i64>>(0), Some(2));
    assert_eq!(
        guard.get::<_, Option<String>>(1).as_deref(),
        Some(target_digest.as_str())
    );
    assert!(guard.get::<_, Option<uuid::Uuid>>(2).is_none());
    Ok(())
}

#[tokio::test]
async fn manager_rollout_observer_disambiguates_and_updates_in_place_member_roles() -> Result<()> {
    use wr_common::authorization_policy::ValidatedPolicy;
    use wr_common::lifecycle_service::{AdmissionGate, ManagerLifecycleState};
    use wr_common::wruntime::{
        BeginManagerRolloutRequest, ManagerRolloutSource, ManagerRolloutTarget,
    };

    let policy = |generation: u64| {
        ValidatedPolicy::load(
            format!(
                r#"schema_version=1
generation={generation}
cluster_id="cluster-a"
revoked_leaf_fingerprints=[]
principals=[
 {{uri="urn:wruntime:cluster-a:human:deployer",kind="human"}},
 {{uri="urn:wruntime:cluster-a:manager:manager-a",kind="manager"}}
]
assignments=[{{principal="urn:wruntime:cluster-a:human:deployer",role="admin",scope={{}}}}]
manager_enrollments=[{{principal="urn:wruntime:cluster-a:manager:manager-a",manager_id="manager-a",endpoint="https://manager-a:9000"}}]
proxy_enrollments=[]
node_agent_enrollments=[]
"#
            )
            .as_bytes(),
        )
        .unwrap()
    };
    let source_policy = policy(1);
    let target_policy = policy(2);
    let pool = helpers::db::manager_pool().await;
    wr_manager::db::register_manager(&pool, "manager-a", "https://manager-a:9000").await?;
    let client = pool.get().await?;
    client
        .execute(
            "UPDATE wr_manager_rollout_guard SET accepted_generation=1,accepted_digest=$1",
            &[&source_policy.digest],
        )
        .await?;
    client
        .execute(
            "UPDATE wr_managers SET policy_generation=1,policy_digest=$1,admission_state='OPEN',last_heartbeat=NOW() WHERE manager_id='manager-a'",
            &[&source_policy.digest],
        )
        .await?;
    drop(client);
    let request = BeginManagerRolloutRequest {
        client_operation_id: "93c54ac7-fe23-4ddf-96e5-0da2e4b18d7d".into(),
        cluster_id: "cluster-a".into(),
        target_generation: 2,
        target_policy_digest: target_policy.digest.clone(),
        expected_targets: vec![ManagerRolloutTarget {
            manager_id: "manager-a".into(),
            endpoint: "https://manager-a:9000".into(),
            host_digest: format!("sha256:{}", "b".repeat(64)),
            config_digest: format!("sha256:{}", "c".repeat(64)),
            ..Default::default()
        }],
        recovery_of: String::new(),
        target_policy_validator_version: 1,
        target_deployment_principal_uri: "urn:wruntime:cluster-a:human:deployer".into(),
        target_deployment_leaf_fingerprint: format!("sha256:{}", "d".repeat(64)),
        source_managers: vec![ManagerRolloutSource {
            manager_id: "manager-a".into(),
            endpoint: "https://manager-a:9000".into(),
            host_digest: format!("sha256:{}", "e".repeat(64)),
            selector_digest: format!("sha256:{}", "f".repeat(64)),
        }],
        ..Default::default()
    };
    let rollout = wr_manager::db::begin_manager_rollout(
        &pool,
        &request.target_deployment_principal_uri,
        &request.target_deployment_leaf_fingerprint,
        &request,
        &format!("sha256:{}", "e".repeat(64)),
        true,
    )
    .await?;
    let admission = AdmissionGate::closed();
    let lifecycle = ManagerLifecycleState::default();
    wr_manager::db::observe_manager_rollout(
        &pool,
        "manager-a",
        1,
        &source_policy.digest,
        &admission,
        &lifecycle,
        &source_policy,
    )
    .await?;
    let rollout_id = uuid::Uuid::parse_str(&rollout.rollout_id)?;
    let rows = pool
        .get()
        .await?
        .query(
            "SELECT member_role,observed_policy_generation FROM wr_manager_rollout_members WHERE rollout_id=$1 ORDER BY member_role",
            &[&rollout_id],
        )
        .await?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>(0), "source");
    assert_eq!(rows[0].get::<_, Option<i64>>(1), Some(1));
    assert_eq!(rows[1].get::<_, String>(0), "target");
    assert_eq!(rows[1].get::<_, Option<i64>>(1), None);

    wr_manager::db::observe_manager_rollout(
        &pool,
        "manager-a",
        2,
        &target_policy.digest,
        &admission,
        &lifecycle,
        &target_policy,
    )
    .await?;
    let rows = pool
        .get()
        .await?
        .query(
            "SELECT member_role,observed_policy_generation FROM wr_manager_rollout_members WHERE rollout_id=$1 ORDER BY member_role",
            &[&rollout_id],
        )
        .await?;
    assert_eq!(rows[0].get::<_, Option<i64>>(1), Some(1));
    assert_eq!(rows[1].get::<_, Option<i64>>(1), Some(2));
    Ok(())
}

#[tokio::test]
async fn manager_rollout_same_generation_reports_digest_mismatch() -> Result<()> {
    use wr_common::wruntime::{BeginManagerRolloutRequest, ManagerRolloutTarget};

    let pool = helpers::db::manager_pool().await;
    let accepted_digest = format!("sha256:{}", "a".repeat(64));
    pool.get()
        .await?
        .execute(
            "UPDATE wr_manager_rollout_guard SET accepted_generation=2,accepted_digest=$1",
            &[&accepted_digest],
        )
        .await?;
    let request = BeginManagerRolloutRequest {
        client_operation_id: "a3c54ac7-fe23-4ddf-96e5-0da2e4b18d7d".into(),
        cluster_id: "cluster-a".into(),
        target_generation: 2,
        target_policy_digest: format!("sha256:{}", "b".repeat(64)),
        expected_targets: vec![ManagerRolloutTarget {
            manager_id: "manager-a".into(),
            endpoint: "https://manager-a:9000".into(),
            host_digest: format!("sha256:{}", "c".repeat(64)),
            config_digest: format!("sha256:{}", "d".repeat(64)),
            ..Default::default()
        }],
        recovery_of: String::new(),
        target_policy_validator_version: 1,
        target_deployment_principal_uri: "urn:wruntime:cluster-a:human:deployer".into(),
        target_deployment_leaf_fingerprint: format!("sha256:{}", "e".repeat(64)),
        ..Default::default()
    };
    let mut stale_restoration = request.clone();
    stale_restoration.client_operation_id = "b3c54ac7-fe23-4ddf-96e5-0da2e4b18d7d".into();
    stale_restoration.target_generation = 1;
    stale_restoration.target_policy_digest = accepted_digest;
    let stale_error = wr_manager::db::begin_manager_rollout(
        &pool,
        &stale_restoration.target_deployment_principal_uri,
        &stale_restoration.target_deployment_leaf_fingerprint,
        &stale_restoration,
        &format!("sha256:{}", "1".repeat(64)),
        true,
    )
    .await
    .unwrap_err();
    assert_eq!(stale_error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        stale_error.message(),
        "target policy generation must be strictly newer"
    );

    let error = wr_manager::db::begin_manager_rollout(
        &pool,
        &request.target_deployment_principal_uri,
        &request.target_deployment_leaf_fingerprint,
        &request,
        &format!("sha256:{}", "f".repeat(64)),
        true,
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        error.message(),
        "same policy generation has a different digest"
    );
    Ok(())
}

#[tokio::test]
async fn manager_rollout_create_recovers_lost_response_and_rejects_token_reuse() -> Result<()> {
    use wr_common::wruntime::{BeginManagerRolloutRequest, ManagerRolloutTarget};

    let pool = helpers::db::manager_pool().await;
    let target_digest = format!("sha256:{}", "a".repeat(64));
    wr_manager::db::register_manager(&pool, "manager-a", "https://manager-a:9000").await?;
    assert!(
        !wr_manager::db::initialize_manager_policy_state(&pool, "manager-a", 2, &target_digest,)
            .await?
    );
    let request = BeginManagerRolloutRequest {
        client_operation_id: "73c54ac7-fe23-4ddf-96e5-0da2e4b18d7d".into(),
        cluster_id: "cluster-a".into(),
        target_generation: 2,
        target_policy_digest: target_digest,
        expected_targets: vec![ManagerRolloutTarget {
            manager_id: "manager-a".into(),
            endpoint: "https://manager-a:9000".into(),
            host_digest: format!("sha256:{}", "b".repeat(64)),
            config_digest: format!("sha256:{}", "c".repeat(64)),
            ..Default::default()
        }],
        recovery_of: String::new(),
        target_policy_validator_version: 1,
        target_deployment_principal_uri: "urn:wruntime:cluster-a:human:deployer".into(),
        target_deployment_leaf_fingerprint: format!("sha256:{}", "d".repeat(64)),
        ..Default::default()
    };
    let first = wr_manager::db::begin_manager_rollout(
        &pool,
        "urn:wruntime:cluster-a:human:deployer",
        &format!("sha256:{}", "d".repeat(64)),
        &request,
        &format!("sha256:{}", "e".repeat(64)),
        false,
    )
    .await?;
    let replay = wr_manager::db::begin_manager_rollout(
        &pool,
        "urn:wruntime:cluster-a:human:deployer",
        &format!("sha256:{}", "f".repeat(64)),
        &request,
        &format!("sha256:{}", "e".repeat(64)),
        false,
    )
    .await?;
    assert_eq!(replay.rollout_id, first.rollout_id);
    assert_eq!(replay.phase, first.phase);

    let conflict = wr_manager::db::begin_manager_rollout(
        &pool,
        "urn:wruntime:cluster-a:human:deployer",
        &format!("sha256:{}", "d".repeat(64)),
        &request,
        &format!("sha256:{}", "0".repeat(64)),
        false,
    )
    .await
    .unwrap_err();
    assert_eq!(conflict.code(), tonic::Code::AlreadyExists);
    let count: i64 = pool
        .get()
        .await?
        .query_one("SELECT COUNT(*) FROM wr_manager_rollouts", &[])
        .await?
        .get(0);
    assert_eq!(count, 1);
    Ok(())
}
