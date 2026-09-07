mod helpers;
use helpers::{
    db::manager_pool,
    manager::{
        manager_proxy_tls, manager_trio, register_test_module_raw, register_test_module_ready,
        start_manager, start_manager_cluster, sync_table, synced_routing_table,
    },
    proxy::{http_client, proxy_get, start_proxy},
    stubs::spawn_stub_engine,
    wasm::{invalid_protobuf, minimal_file_descriptor_set},
};

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use http::{Request, StatusCode};
use http_body_util::Full;

use wr_common::discovery::ManagerDiscovery;
use wr_common::process_lifecycle::{LifecycleOwner, ServiceKind};
use wr_common::wruntime::proxy_node_control_service_server::ProxyNodeControlService; // brings proxy-local methods into scope
use wr_common::wruntime::{
    BeginDeploymentRequest, BeginEngineDrainRequest, DeploymentInventoryV1, DeploymentMetadata,
    EngineRegistration, ExpectedEngine, ExpectedModule, GetProxyRoutingStatusRequest,
    GetRoutingTableRequest, HeartbeatRequest, ModuleDescriptor, ModuleIdentity,
    NodeOperationAction, RegisterEngineRequest, RolloutPolicy, SubmitOperationRequest,
};
use wr_proxy::node_service::NodeAgent;

#[tokio::test]
async fn test_proxy_routes_to_engine() -> Result<()> {
    let (pool, mgr_addr, mut mgr_c) = manager_trio().await?;

    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;

    register_test_module_ready(
        &pool,
        &mut mgr_c,
        "stub-engine",
        &engine_addr,
        "store",
        "inventory-service",
        "1.0.0",
    )
    .await?;

    let table = synced_routing_table(&mgr_addr).await?;
    let proxy = start_proxy(table).await?;

    let (status, body) = proxy_get(proxy, "store", "inventory-service", Some("1.0.0")).await?;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("/Ping"),
        "expected stub to echo request path, got: {body}"
    );

    let _ = engine_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn test_internal_proxy_trusts_protobuf_body() -> Result<()> {
    let (pool, mgr_addr, mut mgr_c) = manager_trio().await?;
    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;
    register_test_module_ready(
        &pool,
        &mut mgr_c,
        "trusted-engine",
        &engine_addr,
        "store",
        "inventory-service",
        "1.0.0",
    )
    .await?;

    let proxy = start_proxy(synced_routing_table(&mgr_addr).await?).await?;
    let response = http_client()
        .request(
            Request::builder()
                .method("POST")
                .uri(format!("http://{proxy}/test.PingService/Ping"))
                .header(
                    "x-wr-destination",
                    "http://store.inventory-service/test.PingService/Ping",
                )
                .body(Full::new(invalid_protobuf()))?,
        )
        .await?;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "trusted internal traffic must reach the engine without proxy schema validation"
    );

    let _ = engine_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn test_proxy_excludes_raw_registration_until_healthy() -> Result<()> {
    let (pool, mgr_addr, mut mgr_c) = manager_trio().await?;
    let (engine_addr, engine_shutdown) = spawn_stub_engine().await?;

    let fence = register_test_module_raw(
        &pool,
        &mut mgr_c,
        "proxy-ready-e1",
        &engine_addr,
        "store",
        "readiness-service",
        "1.0.0",
    )
    .await?;

    let table = synced_routing_table(&mgr_addr).await?;
    let proxy = start_proxy(table.clone()).await?;

    let (status, _) = proxy_get(proxy, "store", "readiness-service", Some("1.0.0")).await?;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

    mgr_c
        .heartbeat(HeartbeatRequest {
            engine_id: "proxy-ready-e1".into(),
            healthy_modules: vec![ModuleDescriptor {
                name: "readiness-service".into(),
                namespace: "store".into(),
                version: "1.0.0".into(),
                proto_schema: vec![],
            }],

            fence: Some(fence),
        })
        .await?;
    wr_manager::db::update_route_health(&pool, 30.0, 30.0)
        .await
        .map_err(|status| anyhow::anyhow!("update_route_health failed: {status}"))?;
    sync_table(&mgr_addr, &table).await?;

    let (status, body) = proxy_get(proxy, "store", "readiness-service", Some("1.0.0")).await?;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("/Ping"));

    let _ = engine_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn test_proxy_register_engine_forwards_without_creating_rules() -> Result<()> {
    let (pool, mgr_addr, mut mgr_c) = manager_trio().await?;

    // ManagerDiscovery resolves managers from wr_managers and authenticates
    // every selected manager epoch with the enrolled proxy identity.
    wr_manager::db::register_manager(&pool, "proxy-test-mgr", &mgr_addr).await?;
    let discovery = Arc::new(ManagerDiscovery::new(
        pool.clone(),
        manager_proxy_tls(),
        "test-manager-server-root.pem",
        "test-proxy-client.pem",
        wr_common::DEFAULT_MANAGER_LIVENESS_THRESHOLD_SECS,
    )?);
    discovery.refresh().await;

    let routing = wr_proxy::routing::new_routing_table(
        wr_proxy::config::CircuitBreakerConfig::default(),
        "https://127.0.0.1:9443",
    );
    let lifecycle = LifecycleOwner::new(ServiceKind::Proxy, "proxy-activation-1");
    let agent = Arc::new(NodeAgent::new(
        discovery,
        routing.clone(),
        lifecycle.snapshot(),
    ));

    let initial_status = agent
        .get_proxy_routing_status(tonic::Request::new(GetProxyRoutingStatusRequest {}))
        .await?
        .into_inner();
    assert_eq!(initial_status.process_instance_id, "proxy-activation-1");
    assert_eq!(
        initial_status.installed_routing_table_version,
        routing.version().await
    );

    let bundle_digest = format!("sha256:{}", "c".repeat(64));
    let schema = minimal_file_descriptor_set();
    let deployment = mgr_c
        .begin_deployment(BeginDeploymentRequest {
            node_id: "node-a".into(),
            attempt_token: "proxy-register-deployment".into(),
            bundle_digest: bundle_digest.clone(),
            inventory: Some(DeploymentInventoryV1 {
                schema_version: 1,
                engines: vec![ExpectedEngine {
                    engine_slot: "primary".into(),
                    modules: vec![ExpectedModule {
                        identity: Some(ModuleIdentity {
                            namespace: "store".into(),
                            name: "inventory".into(),
                            version: "1.0.0".into(),
                        }),
                        proto_schema_digest: wr_common::deployment_contract::schema_digest(&schema),
                    }],
                    ..Default::default()
                }],
            }),
        })
        .await?
        .into_inner()
        .deployment
        .expect("proxy registration deployment");
    let resolved_release_digest = format!("sha256:{}", "d".repeat(64));
    mgr_c
        .finalize_deployment(wr_common::wruntime::FinalizeDeploymentRequest {
            node_id: "node-a".into(),
            attempt_token: "proxy-register-deployment".into(),
            revision: deployment.revision,
            bundle_digest: bundle_digest.clone(),
            resolved_release_digest: resolved_release_digest.clone(),
        })
        .await?;
    let operation = mgr_c
        .submit_operation(SubmitOperationRequest {
            node_id: "node-a".into(),
            request_token: "proxy-register-deployment".into(),
            action: NodeOperationAction::InitialApply as i32,
            engine_slots: vec!["primary".into()],
            target_revision: deployment.revision,
            bundle_digest: bundle_digest.clone(),
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
        .expect("proxy registration operation");
    let resp = agent
        .register_engine(tonic::Request::new(RegisterEngineRequest {
            registration: Some(EngineRegistration {
                engine_id: "proxy-e1".into(),
                address: "http://127.0.0.1:9700".into(),
                proxy_address: "https://test-node:9443".into(),
                peer_address: "https://127.0.0.1:9443".into(),
                modules: vec![ModuleDescriptor {
                    name: "inventory".into(),
                    namespace: "store".into(),
                    version: "1.0.0".into(),
                    proto_schema: schema,
                }],
                secrets: vec![],
                db_namespaces: vec![],
                deployment: Some(DeploymentMetadata {
                    node_id: "node-a".into(),
                    revision: deployment.revision,
                    bundle_digest,
                    engine_slot: "primary".into(),
                    operation_id: operation.operation_id,
                    revision_digest: deployment.revision_digest,
                }),
                job_queue_id: String::new(),
                job_admin_address: String::new(),
            }),

            activation_id: uuid::Uuid::new_v4().to_string(),
        }))
        .await?
        .into_inner();
    assert!(resp.accepted);
    let fence = resp.fence.clone();

    let table = mgr_c
        .get_routing_table(GetRoutingTableRequest { known_version: 0 })
        .await?
        .into_inner()
        .table
        .unwrap();
    assert_eq!(
        table.rules.len(),
        1,
        "proxy must forward only; the single rule is the manager-created default",
    );
    assert_eq!(table.rules[0].rule_id, "proxy-e1/store/inventory/1.0.0");
    assert!(
        !table.rules[0].healthy,
        "forwarded manager-created default starts unhealthy"
    );

    let readiness = agent
        .heartbeat(tonic::Request::new(HeartbeatRequest {
            engine_id: "proxy-e1".into(),
            healthy_modules: vec![ModuleDescriptor {
                name: "inventory".into(),
                namespace: "store".into(),
                version: "1.0.0".into(),
                proto_schema: vec![],
            }],

            fence: fence.clone(),
        }))
        .await?
        .into_inner();
    assert!(
        readiness.proxy_routing_table_version >= readiness.manager_routing_table_version,
        "first heartbeat must synchronously converge the local proxy"
    );
    let ready_status = agent
        .get_proxy_routing_status(tonic::Request::new(GetProxyRoutingStatusRequest {}))
        .await?
        .into_inner();
    assert_eq!(ready_status.process_instance_id, "proxy-activation-1");
    assert_eq!(
        ready_status.installed_routing_table_version, readiness.proxy_routing_table_version,
        "read-only routing status must reflect the existing convergence snapshot"
    );

    let mut tasks = wr_common::task_group::TaskGroup::new();
    let heartbeat_agent = Arc::clone(&agent);
    tasks.spawn("test-heartbeat-loop", move |cancellation| {
        heartbeat_agent.run_heartbeat_loop(Duration::from_millis(20), cancellation)
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let heartbeat_count: i64 = pool
        .get()
        .await?
        .query_one(
            "SELECT COUNT(*) FROM wr_module_heartbeats WHERE engine_id = $1",
            &[&"proxy-e1"],
        )
        .await?
        .get(0);
    assert_eq!(heartbeat_count, 1, "ready heartbeat must reach the manager");

    let withdrawal = agent
        .begin_engine_drain(tonic::Request::new(BeginEngineDrainRequest {
            engine_id: "proxy-e1".into(),

            fence: fence.clone(),
        }))
        .await?
        .into_inner();
    assert!(
        withdrawal.proxy_routing_table_version >= withdrawal.manager_routing_table_version,
        "drain must synchronously converge route withdrawal"
    );
    let drained_status = agent
        .get_proxy_routing_status(tonic::Request::new(GetProxyRoutingStatusRequest {}))
        .await?
        .into_inner();
    assert_eq!(drained_status.process_instance_id, "proxy-activation-1");
    assert!(
        drained_status.installed_routing_table_version
            >= ready_status.installed_routing_table_version,
        "routing status must never regress as convergence installs newer tables"
    );
    let stale = match agent
        .heartbeat(tonic::Request::new(HeartbeatRequest {
            engine_id: "proxy-e1".into(),
            healthy_modules: vec![],

            fence,
        }))
        .await
    {
        Ok(_) => anyhow::bail!("draining engine heartbeat was not fenced"),
        Err(status) => status,
    };
    assert_eq!(stale.code(), tonic::Code::FailedPrecondition);
    let heartbeat_before: f64 = pool
        .get()
        .await?
        .query_one(
            "SELECT EXTRACT(EPOCH FROM last_healthy)::double precision
             FROM wr_module_heartbeats WHERE engine_id = $1",
            &[&"proxy-e1"],
        )
        .await?
        .get(0);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let heartbeat_after: f64 = pool
        .get()
        .await?
        .query_one(
            "SELECT EXTRACT(EPOCH FROM last_healthy)::double precision
             FROM wr_module_heartbeats WHERE engine_id = $1",
            &[&"proxy-e1"],
        )
        .await?
        .get(0);
    assert_eq!(
        heartbeat_after, heartbeat_before,
        "a stale periodic snapshot must not refresh heartbeat state after drain"
    );

    let report = tasks
        .shutdown(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert!(report.is_clean(), "{report:?}");
    Ok(())
}

#[tokio::test]
async fn test_discovery_refreshes_via_list_managers() -> Result<()> {
    let pool = manager_pool().await;
    // A real manager registered in the shared PostgreSQL lease table.
    let managers = start_manager_cluster(pool.clone(), 1, 30).await?;

    let discovery = Arc::new(ManagerDiscovery::new(
        pool.clone(),
        manager_proxy_tls(),
        "test-manager-server-root.pem",
        "test-proxy-client.pem",
        wr_common::DEFAULT_MANAGER_LIVENESS_THRESHOLD_SECS,
    )?);
    discovery.refresh().await; // cold-start DB seed → ListManagers RPC → cache

    // A client can be obtained, i.e. the cache was populated with a reachable addr.
    let client = discovery
        .pin(wr_common::manager_client::RetryClass::ReadOnly)
        .await;
    assert!(client.is_ok(), "discovery should have a reachable manager");
    let _ = managers;
    Ok(())
}

#[tokio::test]
async fn test_discovery_repin_rotates_order_and_rejects_no_replay() -> Result<()> {
    let pool = manager_pool().await;
    let _managers = start_manager_cluster(pool.clone(), 2, 30).await?;
    let discovery = ManagerDiscovery::new(
        pool,
        manager_proxy_tls(),
        "test-manager-server-root.pem",
        "test-proxy-client.pem",
        wr_common::DEFAULT_MANAGER_LIVENESS_THRESHOLD_SECS,
    )?;
    discovery.refresh().await;

    let first = discovery
        .pin(wr_common::manager_client::RetryClass::ReadOnly)
        .await?;
    let second = discovery.repin(&first).await?;
    assert_ne!(first.endpoint(), second.endpoint());

    let no_replay = discovery
        .pin(wr_common::manager_client::RetryClass::NoReplayMutation)
        .await?;
    let error = discovery.repin(&no_replay).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    Ok(())
}

#[tokio::test]
async fn test_discovery_evicts_cached_affinity_after_lease_becomes_stale() -> Result<()> {
    let pool = manager_pool().await;
    let manager_addr = start_manager(pool.clone()).await?;
    wr_manager::db::register_manager(&pool, "cached-mgr", &manager_addr).await?;
    let discovery = ManagerDiscovery::new(
        pool.clone(),
        manager_proxy_tls(),
        "test-manager-server-root.pem",
        "test-proxy-client.pem",
        wr_common::DEFAULT_MANAGER_LIVENESS_THRESHOLD_SECS,
    )?;
    discovery.refresh().await;
    assert!(discovery
        .pin(wr_common::manager_client::RetryClass::ReadOnly)
        .await
        .is_ok());

    pool.get()
        .await?
        .execute(
            "UPDATE wr_managers SET last_heartbeat = NOW() - INTERVAL '10 seconds' WHERE manager_id = $1",
            &[&"cached-mgr"],
        )
        .await?;
    discovery.refresh().await;

    assert!(
        discovery
            .pin(wr_common::manager_client::RetryClass::ReadOnly)
            .await
            .is_err(),
        "successful empty lease evidence must evict cached managers"
    );
    Ok(())
}

#[tokio::test]
async fn test_discovery_direct_db_fallback_uses_configured_lease_threshold() -> Result<()> {
    let pool = manager_pool().await;
    let manager_addr = start_manager(pool.clone()).await?;
    wr_manager::db::register_manager(&pool, "stale-mgr", &manager_addr).await?;
    pool.get()
        .await?
        .execute(
            "UPDATE wr_managers SET last_heartbeat = NOW() - INTERVAL '2 seconds' WHERE manager_id = $1",
            &[&"stale-mgr"],
        )
        .await?;

    let discovery = ManagerDiscovery::new(
        pool,
        manager_proxy_tls(),
        "test-manager-server-root.pem",
        "test-proxy-client.pem",
        1,
    )?;
    discovery.refresh().await;

    assert!(
        discovery
            .pin(wr_common::manager_client::RetryClass::ReadOnly)
            .await
            .is_err(),
        "a row stale under the configured threshold must not bootstrap discovery"
    );
    Ok(())
}

#[tokio::test]
async fn test_discovery_falls_back_to_db_when_no_manager_reachable() -> Result<()> {
    let pool = manager_pool().await;
    // A fresh wr_managers row whose grpc_address has no server behind it.
    wr_manager::db::register_manager(&pool, "unreachable-mgr", "http://127.0.0.1:1").await?;

    let discovery = Arc::new(ManagerDiscovery::new(
        pool.clone(),
        manager_proxy_tls(),
        "test-manager-server-root.pem",
        "test-proxy-client.pem",
        wr_common::DEFAULT_MANAGER_LIVENESS_THRESHOLD_SECS,
    )?);
    discovery.refresh().await; // cold-start DB seed; ListManagers unreachable → DB fallback keeps the row

    assert!(discovery
        .pin(wr_common::manager_client::RetryClass::ReadOnly)
        .await
        .is_err());
    Ok(())
}
