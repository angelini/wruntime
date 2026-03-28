mod config;
mod engine;
mod registry;
mod server;
mod state;

use anyhow::Result;
use std::time::Duration;
use uuid::Uuid;

use wr_common::wruntime::{
    manager_service_client::ManagerServiceClient,
    DeregisterEngineRequest, EngineRegistration, HeartbeatRequest,
    ModuleDescriptor, RegisterEngineRequest,
};

#[tokio::main]
async fn main() -> Result<()> {
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "engine.toml".to_string());

    let config    = config::EngineConfig::load(&config_path)?;
    let engine_id = Uuid::new_v4().to_string();
    println!("[engine] starting  id={engine_id}");

    // ── Load WASM modules ─────────────────────────────────────────────────
    let registry = registry::ModuleRegistry::new();
    let runner   = engine::EngineRunner::new(config.clone())?;
    runner.load_modules(&registry).await?;
    println!("[engine] all modules loaded");

    // ── Start inbound HTTP server ─────────────────────────────────────────
    {
        let reg  = registry.clone();
        let addr = config.listen_address.clone();
        tokio::spawn(async move {
            if let Err(e) = server::serve(&addr, reg).await {
                eprintln!("[engine] inbound server error: {e}");
            }
        });
    }

    // ── Connect to wr-manager ─────────────────────────────────────────────
    let mut client =
        ManagerServiceClient::connect(config.manager_address.clone()).await?;

    // Build module descriptors, loading schema files where available.
    let module_descriptors: Vec<ModuleDescriptor> = config
        .modules
        .iter()
        .map(|m| {
            // schema_path is validated to exist at config load time if present.
            let proto_schema = m
                .schema_path
                .as_deref()
                .map(|p| std::fs::read(p).unwrap_or_default())
                .unwrap_or_default();
            ModuleDescriptor {
                name: m.name.clone(),
                version: m.version.clone(),
                proto_schema,
            }
        })
        .collect();

    // ── Register with manager ─────────────────────────────────────────────
    client
        .register_engine(RegisterEngineRequest {
            registration: Some(EngineRegistration {
                engine_id:      engine_id.clone(),
                address:        config.listen_address.clone(),
                modules:        module_descriptors,
            }),
        })
        .await?;
    println!("[engine] registered with manager at {}", config.manager_address);

    // ── Heartbeat background task ─────────────────────────────────────────
    {
        let mut hb_client = client.clone();
        let hb_id         = engine_id.clone();
        let hb_modules: Vec<ModuleDescriptor> = config.modules.iter().map(|m| ModuleDescriptor {
            name:         m.name.clone(),
            version:      m.version.clone(),
            proto_schema: vec![],
        }).collect();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
                if let Err(e) = hb_client
                    .heartbeat(HeartbeatRequest {
                        engine_id:       hb_id.clone(),
                        healthy_modules: hb_modules.clone(),
                    })
                    .await
                {
                    eprintln!("[engine] heartbeat failed: {e}");
                }
            }
        });
    }

    println!("[engine] running — press Ctrl+C to stop");

    // ── Wait for shutdown signal ──────────────────────────────────────────
    tokio::signal::ctrl_c().await?;
    println!("[engine] shutting down");

    // ── Deregister ────────────────────────────────────────────────────────
    if let Err(e) = client
        .deregister_engine(DeregisterEngineRequest { engine_id: engine_id.clone() })
        .await
    {
        eprintln!("[engine] deregister failed (manager may be down): {e}");
    } else {
        println!("[engine] deregistered");
    }

    Ok(())
}
