mod engine;
mod registry;
mod server;

use wr_engine::config::{self, EnvValue};

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::time::Duration;
use tokio_retry::strategy::FixedInterval;
use tokio_retry::Retry;
use tracing::{error, info, warn};
use uuid::Uuid;

use wr_common::wruntime::{
    node_service_client::NodeServiceClient, DeregisterEngineRequest, EngineRegistration,
    HeartbeatRequest, ModuleDescriptor, RegisterEngineRequest, SecretRequest,
};

fn main() -> Result<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .on_thread_start(|| {
            wasmtime::Engine::tls_eager_initialize();
        })
        .build()?;
    rt.block_on(async_main())
}

async fn async_main() -> Result<()> {
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
    runner.spawn_epoch_ticker();
    runner.provision_schemas().await?;

    // Provision the wr__jobs schema if any module uses worker mode.
    let has_workers = config
        .modules
        .iter()
        .any(|m| m.mode == config::ModuleMode::Worker);
    if has_workers {
        let db = config
            .database
            .as_ref()
            .expect("worker mode requires [database] section");
        let admin_pool = wr_engine::pool::build_pool(&db.url, db.max_connections)?;
        wr_engine::worker::provision_job_schema(&admin_pool).await?;
    }

    runner.run_migrations().await?;

    // ── Start inbound HTTP server ─────────────────────────────────────────
    {
        let reg = registry.clone();
        let addr = config.listen_address.clone();
        let server_db_pool = config
            .database
            .as_ref()
            .map(|db| {
                wr_engine::pool::build_pool(&db.url, db.max_connections).map(std::sync::Arc::new)
            })
            .transpose()?;
        tokio::spawn(async move {
            if let Err(e) = server::serve(&addr, reg, server_db_pool).await {
                error!(error = %e, "inbound server error");
            }
        });
    }

    // ── Connect to proxy NodeService (retry with backoff) ───────────────────
    let mut client = {
        use tokio_retry::strategy::ExponentialBackoff;
        use tokio_retry::Retry;

        let strategy = ExponentialBackoff::from_millis(200)
            .max_delay(Duration::from_secs(5))
            .take(10);
        let addr = config.node.control_address.clone();
        Retry::spawn(strategy, || {
            let addr = addr.clone();
            async move {
                let c = NodeServiceClient::connect(addr).await?;
                Ok::<_, tonic::transport::Error>(c)
            }
        })
        .await
        .with_context(|| {
            format!(
                "failed to connect to proxy at {} after retries",
                config.node.control_address
            )
        })?
    };

    // Build module descriptors — only modules with a schema_path are registered
    // with the manager (runner modules without schemas are skipped).
    let mut module_descriptors: Vec<ModuleDescriptor> = Vec::new();
    for m in &config.modules {
        let Some(ref schema_path) = m.schema_path else {
            continue;
        };
        let proto_schema = std::fs::read(schema_path)
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

    // ── Register with manager (retry with backoff) ─────────────────────────
    let reg_response = {
        use tokio_retry::strategy::ExponentialBackoff;
        use tokio_retry::Retry;

        let strategy = ExponentialBackoff::from_millis(500)
            .max_delay(Duration::from_secs(5))
            .take(10);
        let req = RegisterEngineRequest {
            registration: Some(EngineRegistration {
                engine_id: engine_id.clone(),
                address: advertise_address.clone(),
                proxy_address: config.node.proxy_address.clone(),
                peer_address: config.node.peer_address(),
                modules: module_descriptors,
                secrets: secret_requests,
            }),
        };
        let cl = client.clone();
        Retry::spawn(strategy, || {
            let req = req.clone();
            let mut cl = cl.clone();
            async move { cl.register_engine(req).await }
        })
        .await
        .context("engine registration failed after retries")?
        .into_inner()
    };
    info!(address = %config.node.control_address, engine_id, "registered via proxy");

    // ── Resolve secrets into env vars per module ──────────────────────────
    // Build a lookup: (namespace, key) → plaintext value
    let mut secrets_map: HashMap<(&str, &str), &str> = HashMap::new();
    for ns_secrets in &reg_response.secrets {
        for (key, value) in &ns_secrets.secrets {
            secrets_map.insert((&ns_secrets.namespace, key), value);
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
                        .get(&(module.namespace.as_str(), key.as_str()))
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "secret '{key}' not found for namespace '{}'",
                                module.namespace
                            )
                        })?;
                    env.insert(key.clone(), plaintext.to_string());
                }
                EnvValue::Secret { secret: false } => {}
            }
        }
        resolved_envs.insert((module.namespace.clone(), module.name.clone()), env);
    }

    // ── Load WASM modules (now that secrets are resolved) ─────────────────
    runner
        .load_modules(&registry, &resolved_envs, &engine_id)
        .await?;
    info!("all modules loaded");

    // ── Heartbeat background task ─────────────────────────────────────────
    {
        let mut hb_client = client.clone();
        let hb_id = engine_id.clone();
        let hb_registry = registry.clone();
        let hb_module_configs = config.modules.clone();
        let hb_control_address = config.node.control_address.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(3));
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

                let hb_req = HeartbeatRequest {
                    engine_id: hb_id.clone(),
                    healthy_modules: healthy,
                };
                let strategy = FixedInterval::from_millis(50).take(2);
                let sent = Retry::spawn(strategy, || {
                    let mut c = hb_client.clone();
                    let r = hb_req.clone();
                    async move { c.heartbeat(r).await }
                })
                .await;
                if let Err(e) = &sent {
                    warn!(error = %e, "heartbeat failed after retries");
                }
                if sent.is_err() {
                    // Connection may be stale — reconnect for next cycle.
                    if let Ok(c) = NodeServiceClient::connect(hb_control_address.clone()).await {
                        hb_client = c;
                    }
                }
            }
        });
    }

    info!("engine running — press Ctrl+C to stop");

    // ── Wait for shutdown signal (SIGINT or SIGTERM) ──────────────────────
    wr_common::signal::shutdown_signal().await;
    info!("engine shutting down");

    // ── Deregister ────────────────────────────────────────────────────────
    if let Err(e) = client
        .deregister_engine(DeregisterEngineRequest {
            engine_id: engine_id.clone(),
        })
        .await
    {
        warn!(error = %e, "deregister failed (proxy may be down)");
    } else {
        info!(engine_id, "engine deregistered");
    }

    Ok(())
}
