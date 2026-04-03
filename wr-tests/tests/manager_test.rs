#[allow(dead_code, unused_imports)]
mod helpers;
use helpers::*;

use anyhow::Result;

use wr_common::wruntime::{
    DeregisterEngineRequest, EngineRegistration, GetRoutingTableRequest, HeartbeatRequest,
    ListEnginesRequest, ModuleDescriptor, RegisterEngineRequest, RoutingRule,
};

#[tokio::test]
async fn test_register_and_list_engines() -> Result<()> {
    let pool = manager_pool().await;
    let addr = start_manager(pool).await?;
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
            secrets: vec![],
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
    let pool = manager_pool().await;
    let addr = start_manager(pool).await?;
    let mut c = manager_client(&addr).await?;

    c.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "e1".into(),
            address: "http://127.0.0.1:9101".into(),
            proxy_address: String::new(),
            modules: vec![],
            secrets: vec![],
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
    let pool = manager_pool().await;
    let addr = start_manager(pool).await?;
    let mut c = manager_client(&addr).await?;

    c.register_engine(RegisterEngineRequest {
        registration: Some(EngineRegistration {
            engine_id: "e1".into(),
            address: "http://127.0.0.1:9102".into(),
            proxy_address: String::new(),
            modules: vec![],
            secrets: vec![],
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
    let pool = manager_pool().await;
    let addr = start_manager(pool).await?;
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
