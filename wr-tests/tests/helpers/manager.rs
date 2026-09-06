use std::sync::Arc;

use anyhow::{bail, Result};
use tokio::net::TcpListener;
use tonic::transport::{
    Certificate, Channel, ClientTlsConfig, Endpoint, Identity, Server, ServerTlsConfig,
};

use wr_common::wruntime::{
    manager_service_client::ManagerServiceClient, manager_service_server::ManagerServiceServer,
    node_agent_service_client::NodeAgentServiceClient,
    node_agent_service_server::NodeAgentServiceServer,
    operator_service_client::OperatorServiceClient, operator_service_server::OperatorServiceServer,
    EngineRegistration, GetRoutingTableRequest, HeartbeatRequest, ModuleDescriptor,
    RegisterEngineRequest,
};
use wr_manager::config::{PrincipalMapping, PrincipalRole};
use wr_manager::service::{Manager, NodeAgentApi, OperatorApi};

use super::pki::{generate_role_test_pki, RoleTestPki, TonicTestIdentity};

use super::db::manager_pool;
use super::proxy::{register_module_raw, EngineSpec, ModuleSpec, TEST_SELF_PEER};
use super::wasm::minimal_file_descriptor_set;

fn test_secret_crypto() -> Arc<wr_manager::crypto::SecretCrypto> {
    let key = wr_manager::crypto::SecretCrypto::generate_random_password();
    Arc::new(
        wr_manager::crypto::SecretCrypto::from_hex(&key).expect("generated test encryption key"),
    )
}

async fn test_cluster_handle() -> Result<std::sync::Arc<wr_manager::cluster::ClusterHandle>> {
    let gossip_port = {
        let tmp = TcpListener::bind("127.0.0.1:0").await?;
        tmp.local_addr()?.port()
    };
    let listen: std::net::SocketAddr = format!("127.0.0.1:{gossip_port}").parse()?;
    Ok(std::sync::Arc::new(
        wr_manager::cluster::ClusterHandle::new(
            &uuid::Uuid::new_v4().to_string(),
            "test-cluster",
            listen,
            vec![],
            std::time::Duration::from_millis(100),
            chitchat::FailureDetectorConfig::default(),
        )
        .await?,
    ))
}

pub struct AuthorizedManager {
    pub endpoint: String,
    pub pki: Arc<RoleTestPki>,
    server_task: tokio::task::JoinHandle<()>,
}

impl Drop for AuthorizedManager {
    fn drop(&mut self) {
        self.server_task.abort();
    }
}

impl AuthorizedManager {
    async fn channel(&self, identity: Option<&TonicTestIdentity>) -> Result<Channel> {
        let mut tls = ClientTlsConfig::new()
            .domain_name("localhost")
            .ca_certificate(Certificate::from_pem(self.pki.ca_pem.clone()));
        if let Some(identity) = identity {
            tls = tls.identity(Identity::from_pem(
                identity.cert_pem.clone(),
                identity.key_pem.clone(),
            ));
        }
        Ok(Endpoint::from_shared(self.endpoint.clone())?
            .tls_config(tls)?
            .connect()
            .await?)
    }

    pub async fn operator_client(
        &self,
        identity: Option<&TonicTestIdentity>,
    ) -> Result<OperatorServiceClient<Channel>> {
        Ok(OperatorServiceClient::new(self.channel(identity).await?))
    }

    pub async fn agent_client(
        &self,
        identity: Option<&TonicTestIdentity>,
    ) -> Result<NodeAgentServiceClient<Channel>> {
        Ok(NodeAgentServiceClient::new(self.channel(identity).await?))
    }
}

/// Start all role-gated manager services on one real mTLS listener.
pub async fn start_authorized_manager(pool: deadpool_postgres::Pool) -> Result<AuthorizedManager> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let pki = Arc::new(generate_role_test_pki());
    let mappings = vec![
        PrincipalMapping {
            fingerprint: pki.viewer.fingerprint.clone(),
            principal: "viewer-a".into(),
            role: PrincipalRole::Viewer,
            node_id: None,
        },
        PrincipalMapping {
            fingerprint: pki.operator.fingerprint.clone(),
            principal: "operator-a".into(),
            role: PrincipalRole::Operator,
            node_id: None,
        },
        PrincipalMapping {
            fingerprint: pki.operator_rotated.fingerprint.clone(),
            principal: "operator-a".into(),
            role: PrincipalRole::Operator,
            node_id: None,
        },
        PrincipalMapping {
            fingerprint: pki.agent_a.fingerprint.clone(),
            principal: "agent-a".into(),
            role: PrincipalRole::NodeAgent,
            node_id: Some("node-a".into()),
        },
        PrincipalMapping {
            fingerprint: pki.agent_b.fingerprint.clone(),
            principal: "agent-b".into(),
            role: PrincipalRole::NodeAgent,
            node_id: Some("node-b".into()),
        },
    ];
    let policy = wr_manager::auth::PrincipalPolicy::new(&mappings);
    let crypto = test_secret_crypto();
    let cluster = test_cluster_handle().await?;
    let tls = ServerTlsConfig::new()
        .identity(Identity::from_pem(
            pki.server.cert_pem.clone(),
            pki.server.key_pem.clone(),
        ))
        .client_ca_root(Certificate::from_pem(pki.ca_pem.clone()));
    let server_task = tokio::spawn(async move {
        if let Err(error) = Server::builder()
            .tls_config(tls)
            .expect("test TLS configuration is valid")
            .add_service(ManagerServiceServer::new(Manager::new(
                pool.clone(),
                crypto,
                cluster.clone(),
            )))
            .add_service(OperatorServiceServer::new(OperatorApi::new(
                pool.clone(),
                cluster,
                policy.clone(),
                30.0,
                30.0,
            )))
            .add_service(NodeAgentServiceServer::new(NodeAgentApi::new(pool, policy)))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
        {
            panic!("authorized manager test server failed: {error}");
        }
    });
    Ok(AuthorizedManager {
        endpoint: format!("https://localhost:{}", addr.port()),
        pki,
        server_task,
    })
}

/// Start an in-process wr-manager on a random port; returns its gRPC address.
pub async fn start_manager(pool: deadpool_postgres::Pool) -> Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let crypto = test_secret_crypto();
    let cluster = test_cluster_handle().await?;
    tokio::spawn(
        Server::builder()
            .add_service(ManagerServiceServer::new(Manager::new(
                pool, crypto, cluster,
            )))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    Ok(format!("http://{addr}"))
}

/// Return a connected manager gRPC client.
pub async fn manager_client(addr: &str) -> Result<ManagerServiceClient<tonic::transport::Channel>> {
    Ok(ManagerServiceClient::connect(addr.to_string()).await?)
}

/// Set up an in-process manager: pool + gRPC server + connected client.
pub async fn manager_trio() -> Result<(
    deadpool_postgres::Pool,
    String,
    ManagerServiceClient<tonic::transport::Channel>,
)> {
    let pool = manager_pool().await;
    let addr = start_manager(pool.clone()).await?;
    let client = manager_client(&addr).await?;
    Ok((pool, addr, client))
}

/// Like [`manager_trio`] but also spawns the heartbeat monitor background task.
pub async fn manager_trio_with_monitor(
    timeout_secs: u64,
) -> Result<(
    deadpool_postgres::Pool,
    String,
    ManagerServiceClient<tonic::transport::Channel>,
)> {
    let pool = manager_pool().await;
    let addr = start_manager_with_monitor(pool.clone(), timeout_secs).await?;
    let client = manager_client(&addr).await?;
    Ok((pool, addr, client))
}

/// Raw register a module with sensible test defaults; does not create an admin route or mark the route healthy.
pub async fn register_test_module_raw(
    c: &mut ManagerServiceClient<tonic::transport::Channel>,
    engine_id: &str,
    engine_addr: &str,
    namespace: &str,
    name: &str,
    version: &str,
) -> Result<()> {
    register_module_raw(
        c,
        EngineSpec {
            id: engine_id,
            addr: engine_addr,
            peer_address: TEST_SELF_PEER,
        },
        ModuleSpec {
            namespace,
            name,
            version,
            schema: minimal_file_descriptor_set(),
        },
    )
    .await
}

pub async fn register_test_module_ready(
    pool: &deadpool_postgres::Pool,
    c: &mut ManagerServiceClient<tonic::transport::Channel>,
    engine_id: &str,
    engine_addr: &str,
    namespace: &str,
    name: &str,
    version: &str,
) -> Result<()> {
    register_test_module_ready_with_peer(
        pool,
        c,
        EngineSpec {
            id: engine_id,
            addr: engine_addr,
            peer_address: TEST_SELF_PEER,
        },
        ModuleSpec {
            namespace,
            name,
            version,
            schema: minimal_file_descriptor_set(),
        },
    )
    .await
}

pub async fn register_test_module_ready_with_peer(
    pool: &deadpool_postgres::Pool,
    c: &mut ManagerServiceClient<tonic::transport::Channel>,
    engine: EngineSpec<'_>,
    module: ModuleSpec<'_>,
) -> Result<()> {
    let engine_id = engine.id.to_string();
    let namespace = module.namespace.to_string();
    let name = module.name.to_string();
    let version = module.version.to_string();

    register_module_raw(c, engine, module).await?;
    c.heartbeat(HeartbeatRequest {
        engine_id: engine_id.clone(),
        healthy_modules: vec![ModuleDescriptor {
            name: name.clone(),
            namespace: namespace.clone(),
            version: version.clone(),
            proto_schema: vec![],
        }],
    })
    .await?;
    wr_manager::db::update_route_health(pool, 30.0, 30.0)
        .await
        .map_err(|status| anyhow::anyhow!("update_route_health failed: {status}"))?;

    let started = std::time::Instant::now();
    loop {
        let (healthy, table_version) =
            get_default_rule_health(c, &engine_id, &namespace, &name, &version).await?;
        if healthy {
            return Ok(());
        }
        if started.elapsed() >= super::wait::DEFAULT_WAIT_TIMEOUT {
            bail!(
                "default route {engine_id}/{namespace}/{name}/{version} remained unhealthy after heartbeat/recompute; last table version={table_version}"
            );
        }
        tokio::time::sleep(super::wait::DEFAULT_POLL_INTERVAL).await;
    }
}

/// Create a routing cache and sync it from the manager in one step.
pub async fn synced_routing_table(mgr_addr: &str) -> Result<wr_proxy::routing::CachedRoutingTable> {
    synced_routing_table_with_config(mgr_addr, Default::default()).await
}

pub async fn synced_routing_table_with_config(
    mgr_addr: &str,
    config: wr_proxy::config::CircuitBreakerConfig,
) -> Result<wr_proxy::routing::CachedRoutingTable> {
    let table = wr_proxy::routing::new_routing_table(config, Arc::<str>::from(TEST_SELF_PEER));
    sync_table(mgr_addr, &table).await?;
    Ok(table)
}

/// Query the routing table via gRPC and find a rule by destination module name.
/// Returns `(healthy, version)`.
pub async fn get_rule_health(
    mgr: &mut ManagerServiceClient<tonic::transport::Channel>,
    destination_module: &str,
) -> Result<(bool, u64)> {
    let table = mgr
        .get_routing_table(GetRoutingTableRequest { known_version: 0 })
        .await?
        .into_inner()
        .table
        .expect("routing table present");
    let rule = table
        .rules
        .iter()
        .find(|r| r.destination_module == destination_module)
        .unwrap_or_else(|| panic!("no rule for destination_module={destination_module}"));
    Ok((rule.healthy, table.version))
}

pub async fn get_default_rule_health(
    mgr: &mut ManagerServiceClient<tonic::transport::Channel>,
    engine_id: &str,
    namespace: &str,
    destination_module: &str,
    version: &str,
) -> Result<(bool, u64)> {
    let table = mgr
        .get_routing_table(GetRoutingTableRequest { known_version: 0 })
        .await?
        .into_inner()
        .table
        .expect("routing table present");
    let rule_id = format!("{engine_id}/{namespace}/{destination_module}/{version}");
    let rule = table
        .rules
        .iter()
        .find(|r| {
            r.rule_id == rule_id
                && r.engine_id == engine_id
                && r.destination_namespace == namespace
                && r.destination_module == destination_module
                && r.destination_version == version
        })
        .unwrap_or_else(|| panic!("no default rule {rule_id}"));
    Ok((rule.healthy, table.version))
}

/// Query the routing table version via gRPC.
pub async fn get_routing_table_version(
    mgr: &mut ManagerServiceClient<tonic::transport::Channel>,
) -> Result<u64> {
    let table = mgr
        .get_routing_table(GetRoutingTableRequest { known_version: 0 })
        .await?
        .into_inner()
        .table
        .expect("routing table present");
    Ok(table.version)
}

pub async fn sync_table(
    mgr_addr: &str,
    table: &wr_proxy::routing::CachedRoutingTable,
) -> Result<()> {
    let mut c = manager_client(mgr_addr).await?;
    if let Some(incoming) = c
        .get_routing_table(GetRoutingTableRequest { known_version: 0 })
        .await?
        .into_inner()
        .table
    {
        table.replace(&incoming).await;
    }
    Ok(())
}

pub async fn start_manager_with_monitor(
    pool: deadpool_postgres::Pool,
    timeout_secs: u64,
) -> Result<String> {
    let crypto = test_secret_crypto();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let cluster = test_cluster_handle().await?;
    tokio::spawn(
        Server::builder()
            .add_service(ManagerServiceServer::new(Manager::new(
                pool.clone(),
                crypto,
                cluster,
            )))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    tokio::spawn(wr_manager::state::monitor_heartbeats(
        pool,
        timeout_secs,
        timeout_secs,
        std::time::Duration::from_millis(200),
    ));
    Ok(format!("http://{addr}"))
}

/// Backdate an engine's heartbeat in the database for testing health timeout.
pub async fn backdate_engine_heartbeat(
    pool: &deadpool_postgres::Pool,
    engine_id: &str,
    secs_ago: i64,
) {
    let client = pool.get().await.unwrap();
    client
        .execute(
            "UPDATE wr_engines SET last_heartbeat = NOW() - make_interval(secs => $1::double precision) WHERE engine_id = $2",
            &[&(secs_ago as f64), &engine_id],
        )
        .await
        .unwrap();
}

// ── Manager cluster ──────────────────────────────────────────────────────────

/// A running manager instance in a cluster.
pub struct ClusteredManager {
    /// gRPC address of this manager.
    pub addr: String,
    /// This manager's chitchat node id (== manager_id).
    pub manager_id: String,
    /// The live cluster handle (test hook to simulate death via `initiate_shutdown`).
    pub cluster: std::sync::Arc<wr_manager::cluster::ClusterHandle>,
    grpc_task: tokio::task::JoinHandle<()>,
}

impl ClusteredManager {
    /// Abruptly stop this manager's request-serving process while preserving the
    /// shared database, as a takeover fixture.
    pub fn abort_service(&self) {
        self.grpc_task.abort();
        let _ = self.cluster.initiate_shutdown();
    }
}

/// Start `count` managers with chitchat gossip, all sharing the same Postgres.
/// Chitchat is the primary manager liveness signal — engine heartbeats are in Postgres.
pub async fn start_manager_cluster(
    pool: deadpool_postgres::Pool,
    count: usize,
    heartbeat_timeout_secs: u64,
) -> Result<Vec<ClusteredManager>> {
    start_manager_cluster_inner(
        pool,
        count,
        heartbeat_timeout_secs,
        chitchat::FailureDetectorConfig::default(),
    )
    .await
}

/// Like `start_manager_cluster` but with a short failure detector so a killed
/// peer is detected dead by chitchat within a couple of seconds (deterministic,
/// bounded — used by tests that must observe a real chitchat death).
pub async fn start_manager_cluster_fast_death(
    pool: deadpool_postgres::Pool,
    count: usize,
    heartbeat_timeout_secs: u64,
) -> Result<Vec<ClusteredManager>> {
    let fd = chitchat::FailureDetectorConfig {
        phi_threshold: 8.0,
        sampling_window_size: 10,
        max_interval: std::time::Duration::from_millis(500),
        initial_interval: std::time::Duration::from_millis(200),
        dead_node_grace_period: std::time::Duration::from_secs(10),
    };
    start_manager_cluster_inner(pool, count, heartbeat_timeout_secs, fd).await
}

async fn start_manager_cluster_inner(
    pool: deadpool_postgres::Pool,
    count: usize,
    heartbeat_timeout_secs: u64,
    failure_detector: chitchat::FailureDetectorConfig,
) -> Result<Vec<ClusteredManager>> {
    let mut managers = Vec::with_capacity(count);
    let mut gossip_addrs: Vec<String> = Vec::new();

    for _ in 0..count {
        let manager_id = uuid::Uuid::new_v4().to_string();

        // Bind gRPC listener
        let grpc_listener = TcpListener::bind("127.0.0.1:0").await?;
        let grpc_addr = grpc_listener.local_addr()?;
        let grpc_url = format!("http://{grpc_addr}");

        // Bind gossip UDP port (pick a free TCP port and use it for UDP)
        let gossip_port = {
            let tmp = TcpListener::bind("127.0.0.1:0").await?;
            tmp.local_addr()?.port()
        };
        let gossip_listen: std::net::SocketAddr = format!("127.0.0.1:{gossip_port}").parse()?;
        let gossip_addr_str = gossip_listen.to_string();

        // Register in wr_managers
        wr_manager::db::register_manager(&pool, &manager_id, &grpc_url, &gossip_addr_str)
            .await
            .map_err(|e| anyhow::anyhow!("register_manager: {e}"))?;

        let cluster = std::sync::Arc::new(
            wr_manager::cluster::ClusterHandle::new(
                &manager_id,
                "test-cluster",
                gossip_listen,
                gossip_addrs.clone(),
                std::time::Duration::from_millis(100),
                failure_detector.clone(),
            )
            .await?,
        );
        cluster.publish_metadata(&grpc_url, &gossip_addr_str).await;

        gossip_addrs.push(gossip_addr_str);

        let crypto = test_secret_crypto();

        let manager = Manager::new(pool.clone(), crypto, cluster.clone());

        // Start gRPC server and retain its owner so takeover tests can prove a
        // real serving process disappeared without changing shared state.
        let grpc_task = tokio::spawn(async move {
            if let Err(error) = Server::builder()
                .add_service(ManagerServiceServer::new(manager))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                    grpc_listener,
                ))
                .await
            {
                panic!("clustered manager test server failed: {error}");
            }
        });

        // Start heartbeat monitor (reads Postgres, no gossip)
        tokio::spawn(wr_manager::state::monitor_heartbeats(
            pool.clone(),
            heartbeat_timeout_secs,
            heartbeat_timeout_secs,
            std::time::Duration::from_millis(200),
        ));

        managers.push(ClusteredManager {
            addr: grpc_url,
            manager_id,
            cluster,
            grpc_task,
        });
    }

    Ok(managers)
}
