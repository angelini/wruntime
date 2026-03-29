mod engine;
mod registry;
mod server;

use wr_engine::config;

use anyhow::{Context, Result};
use std::time::Duration;
use tracing::{error, info, warn};
use uuid::Uuid;

use wr_common::wruntime::{
    manager_service_client::ManagerServiceClient, DeregisterEngineRequest, EngineRegistration,
    HeartbeatRequest, ModuleDescriptor, RegisterEngineRequest, RoutingRule,
};

#[tokio::main]
async fn main() -> Result<()> {
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "engine.toml".to_string());

    let _telemetry = wr_common::telemetry::init("wr-engine")?;

    let config = config::EngineConfig::load(&config_path)?;
    let engine_id = Uuid::new_v4().to_string();
    // Convert listen_address (may bind on 0.0.0.0) to a routable HTTP URL.
    let advertise_address = {
        let a = config.listen_address.trim_start_matches("http://");
        let a = if a.starts_with("0.0.0.0") {
            a.replacen("0.0.0.0", "127.0.0.1", 1)
        } else {
            a.to_string()
        };
        format!("http://{a}")
    };
    info!(engine_id, "engine starting");

    // ── Load WASM modules ─────────────────────────────────────────────────
    let registry = registry::ModuleRegistry::new();
    let runner = engine::EngineRunner::new(config.clone())?;
    runner.load_modules(&registry).await?;
    info!("all modules loaded");

    // ── Start inbound HTTP server ─────────────────────────────────────────
    {
        let reg = registry.clone();
        let addr = config.listen_address.clone();
        tokio::spawn(async move {
            if let Err(e) = server::serve(&addr, reg).await {
                error!(error = %e, "inbound server error");
            }
        });
    }

    // ── Connect to wr-manager ─────────────────────────────────────────────
    let mut client = ManagerServiceClient::connect(config.manager_address.clone()).await?;

    // Build module descriptors — schema_path is required and validated at config load time.
    let mut module_descriptors: Vec<ModuleDescriptor> = Vec::new();
    for m in &config.modules {
        let proto_schema = std::fs::read(&m.schema_path)
            .with_context(|| format!("failed to read schema for module '{}'", m.name))?;
        module_descriptors.push(ModuleDescriptor {
            name: m.name.clone(),
            namespace: m.namespace.clone(),
            version: m.version.clone(),
            proto_schema,
        });
    }

    // ── Register with manager ─────────────────────────────────────────────
    client
        .register_engine(RegisterEngineRequest {
            registration: Some(EngineRegistration {
                engine_id: engine_id.clone(),
                address: advertise_address.clone(),
                modules: module_descriptors,
            }),
        })
        .await?;
    info!(address = %config.manager_address, engine_id, "registered with manager");

    // ── Upsert routing rules for every hosted module ──────────────────────
    for module in &config.modules {
        client
            .upsert_routing_rule(RoutingRule {
                rule_id: format!(
                    "{}/{}/{}/{}",
                    engine_id, module.namespace, module.name, module.version
                ),
                source_module: String::new(),
                source_namespace: String::new(),
                destination_module: module.name.clone(),
                destination_namespace: module.namespace.clone(),
                destination_version: module.version.clone(),
                engine_id: engine_id.clone(),
                engine_address: advertise_address.clone(),
                healthy: false, // manager overrides to true on upsert
            })
            .await?;
        info!(
            namespace = %module.namespace,
            module    = %module.name,
            version   = %module.version,
            "routing rule registered",
        );
    }

    // ── Heartbeat background task ─────────────────────────────────────────
    {
        let mut hb_client = client.clone();
        let hb_id = engine_id.clone();
        let hb_modules: Vec<ModuleDescriptor> = config
            .modules
            .iter()
            .map(|m| ModuleDescriptor {
                name: m.name.clone(),
                namespace: m.namespace.clone(),
                version: m.version.clone(),
                proto_schema: vec![],
            })
            .collect();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
                if let Err(e) = hb_client
                    .heartbeat(HeartbeatRequest {
                        engine_id: hb_id.clone(),
                        healthy_modules: hb_modules.clone(),
                    })
                    .await
                {
                    warn!(error = %e, "heartbeat failed");
                }
            }
        });
    }

    info!("engine running — press Ctrl+C to stop");

    // ── Wait for shutdown signal (SIGINT or SIGTERM) ──────────────────────
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigint = signal(SignalKind::interrupt())?;
        let mut sigterm = signal(SignalKind::terminate())?;
        tokio::select! {
            _ = sigint.recv()  => {},
            _ = sigterm.recv() => {},
        }
    }
    info!("engine shutting down");

    // ── Deregister ────────────────────────────────────────────────────────
    if let Err(e) = client
        .deregister_engine(DeregisterEngineRequest {
            engine_id: engine_id.clone(),
        })
        .await
    {
        warn!(error = %e, "deregister failed (manager may be down)");
    } else {
        info!(engine_id, "engine deregistered");
    }

    Ok(())
}
