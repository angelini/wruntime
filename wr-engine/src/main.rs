mod engine;
mod registry;
mod server;

use wr_engine::config::{self, EnvValue};

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::time::Duration;
use tracing::{error, info, warn};
use uuid::Uuid;

use wr_common::wruntime::{
    manager_service_client::ManagerServiceClient, DeregisterEngineRequest, EngineRegistration,
    HeartbeatRequest, ModuleDescriptor, RegisterEngineRequest, RoutingRule, SecretRequest,
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

    // ── Prepare WASM runtime (schemas + migrations, but don't load modules yet)
    let registry = registry::ModuleRegistry::new();
    let runner = engine::EngineRunner::new(config.clone())?;
    runner.provision_schemas().await?;
    runner.run_migrations().await?;

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

    // Build secret requests from module env configs
    let mut secret_requests: Vec<SecretRequest> = Vec::new();
    for m in &config.modules {
        for (key, val) in &m.env {
            if matches!(val, EnvValue::Secret { secret: true }) {
                secret_requests.push(SecretRequest {
                    namespace: m.namespace.clone(),
                    key: key.clone(),
                });
            }
        }
    }

    // ── Register with manager ─────────────────────────────────────────────
    let reg_response = client
        .register_engine(RegisterEngineRequest {
            registration: Some(EngineRegistration {
                engine_id: engine_id.clone(),
                address: advertise_address.clone(),
                proxy_address: config.node.proxy_address.clone(),
                modules: module_descriptors,
                secrets: secret_requests,
            }),
        })
        .await?
        .into_inner();
    info!(address = %config.manager_address, engine_id, "registered with manager");

    // ── Resolve secrets into env vars per module ──────────────────────────
    // Build a lookup: (namespace, key) → plaintext value
    let mut secrets_map: HashMap<(String, String), String> = HashMap::new();
    for ns_secrets in &reg_response.secrets {
        for (key, value) in &ns_secrets.secrets {
            secrets_map.insert((ns_secrets.namespace.clone(), key.clone()), value.clone());
        }
    }

    // Resolve each module's env block into a flat HashMap<String, String>
    let mut resolved_envs: HashMap<(String, String), HashMap<String, String>> = HashMap::new();
    for module in &config.modules {
        let mut env = HashMap::new();
        for (key, val) in &module.env {
            match val {
                EnvValue::Plain(v) => {
                    env.insert(key.clone(), v.clone());
                }
                EnvValue::Secret { secret: true } => {
                    let plaintext = secrets_map
                        .get(&(module.namespace.clone(), key.clone()))
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "secret '{key}' not found for namespace '{}'",
                                module.namespace
                            )
                        })?;
                    env.insert(key.clone(), plaintext.clone());
                }
                EnvValue::Secret { secret: false } => {}
            }
        }
        resolved_envs.insert((module.namespace.clone(), module.name.clone()), env);
    }

    // ── Load WASM modules (now that secrets are resolved) ─────────────────
    runner.load_modules(&registry, &resolved_envs).await?;
    info!("all modules loaded");

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
                proxy_address: config.node.proxy_address.clone(),
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
        let hb_registry = registry.clone();
        let hb_module_configs = config.modules.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;

                // Health-check each module; only include passing ones in the heartbeat.
                let mut healthy = Vec::new();
                for m in &hb_module_configs {
                    if let Some(tx) = hb_registry
                        .next_sender(&m.namespace, &m.name, &m.version)
                        .await
                    {
                        if engine::check_module_health(&tx).await {
                            healthy.push(ModuleDescriptor {
                                name: m.name.clone(),
                                namespace: m.namespace.clone(),
                                version: m.version.clone(),
                                proto_schema: vec![],
                            });
                        } else {
                            warn!(
                                namespace = %m.namespace,
                                module    = %m.name,
                                version   = %m.version,
                                "module failed health check",
                            );
                        }
                    }
                }

                if let Err(e) = hb_client
                    .heartbeat(HeartbeatRequest {
                        engine_id: hb_id.clone(),
                        healthy_modules: healthy,
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
