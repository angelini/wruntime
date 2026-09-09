use std::sync::{Arc, OnceLock};

use anyhow::{bail, Context, Result};
use prost::Message;
use tokio::net::TcpListener;
use tonic::transport::{
    Certificate, Channel, ClientTlsConfig as TonicClientTlsConfig, Endpoint, Identity, Server,
};

use wr_common::manager_client::ManagerClient as ManagerServiceClient;
use wr_common::wruntime::{
    cluster_service_server::ClusterServiceServer,
    infrastructure_service_server::InfrastructureServiceServer,
    job_service_server::JobServiceServer, lifecycle_service_server::LifecycleServiceServer,
    node_service_server::NodeServiceServer, policy_service_server::PolicyServiceServer,
    BeginDeploymentRequest, DeploymentInventoryV1, DeploymentMetadata, EngineOwnershipFence,
    EngineRegistration, ExpectedEngine, ExpectedModule, FinalizeDeploymentRequest,
    GetRoutingTableRequest, HeartbeatRequest, ModuleDescriptor, NodeOperationAction,
    RegisterEngineRequest, RegisterEngineResponse, RolloutPolicy, SubmitOperationRequest,
};
use wr_manager::service::Manager;

use super::pki::{generate_role_test_pki, RoleTestPki, TonicTestIdentity};

use super::db::manager_pool;
use super::pki::{generate_test_pki_files_for_principal, TestPkiFiles};
use super::proxy::{EngineSpec, ModuleSpec, TEST_SELF_PEER};
use super::wasm::minimal_file_descriptor_set;

fn test_policy() -> Arc<wr_common::authorization_policy::ValidatedPolicy> {
    let policy = br#"schema_version=1
generation=1
cluster_id="cluster-a"
assignments=[
 {principal="urn:wruntime:cluster-a:human:test-admin",role="admin",scope={}},
 {principal="urn:wruntime:cluster-a:human:viewer-a",role="view",scope={}},
 {principal="urn:wruntime:cluster-a:human:operator-a",role="admin",scope={}}
]
proxy_enrollments=[{principal="urn:wruntime:cluster-a:proxy:proxy-a",node_id="node-a"}]
node_agent_enrollments=[
 {principal="urn:wruntime:cluster-a:node-agent:node-a",node_id="node-a"},
 {principal="urn:wruntime:cluster-a:node-agent:node-b",node_id="node-b"}
]
revoked_leaf_fingerprints=[]
principals=[
 {uri="urn:wruntime:cluster-a:human:test-admin",kind="human"},
 {uri="urn:wruntime:cluster-a:human:viewer-a",kind="human"},
 {uri="urn:wruntime:cluster-a:human:operator-a",kind="human"},
 {uri="urn:wruntime:cluster-a:proxy:proxy-a",kind="proxy"},
 {uri="urn:wruntime:cluster-a:node-agent:node-a",kind="node-agent"},
 {uri="urn:wruntime:cluster-a:node-agent:node-b",kind="node-agent"}
]
manager_enrollments=[]
"#;
    Arc::new(wr_common::authorization_policy::ValidatedPolicy::load(policy).unwrap())
}

fn test_principal_policy() -> wr_manager::auth::PrincipalPolicy {
    wr_manager::auth::PrincipalPolicy::new(test_policy())
}

struct ManagerTestPki {
    _human: TestPkiFiles,
    _proxy: TestPkiFiles,
    _bundle_dir: tempfile::TempDir,
    server_tls: wr_common::node::ServerTlsConfig,
    human_tls: wr_common::node::ClientTlsConfig,
    proxy_tls: wr_common::node::ClientTlsConfig,
}

fn manager_test_pki() -> &'static ManagerTestPki {
    static PKI: OnceLock<ManagerTestPki> = OnceLock::new();
    PKI.get_or_init(|| {
        let human = generate_test_pki_files_for_principal(
            "manager-test-human",
            "urn:wruntime:cluster-a:human:test-admin",
        );
        let proxy = generate_test_pki_files_for_principal(
            "manager-test-proxy",
            "urn:wruntime:cluster-a:proxy:proxy-a",
        );
        let bundle_dir = tempfile::tempdir().unwrap();
        let client_roots = bundle_dir.path().join("client-roots.pem");
        let roots = format!(
            "{}\n{}",
            std::fs::read_to_string(&human.server_tls.client_ca_cert_path).unwrap(),
            std::fs::read_to_string(&proxy.server_tls.client_ca_cert_path).unwrap(),
        );
        std::fs::write(&client_roots, roots).unwrap();
        let mut server_tls = human.server_tls.clone();
        server_tls.client_ca_cert_path = client_roots.to_string_lossy().into_owned();
        let human_tls = human.client_tls.clone();
        let mut proxy_tls = proxy.client_tls.clone();
        proxy_tls.server_ca_cert_path = human_tls.server_ca_cert_path.clone();
        ManagerTestPki {
            _human: human,
            _proxy: proxy,
            _bundle_dir: bundle_dir,
            server_tls,
            human_tls,
            proxy_tls,
        }
    })
}

fn test_admission() -> wr_common::lifecycle_service::AdmissionGate {
    let admission = wr_common::lifecycle_service::AdmissionGate::closed();
    admission.open();
    admission
}

fn test_secret_crypto() -> Arc<wr_manager::crypto::SecretCrypto> {
    let key = wr_manager::crypto::SecretCrypto::generate_random_password();
    Arc::new(
        wr_manager::crypto::SecretCrypto::from_hex(&key).expect("generated test encryption key"),
    )
}

struct TestManagerServices {
    cluster: wr_manager::service::AuthorizedClusterService,
    infrastructure: wr_manager::service::AuthorizedInfrastructureService,
    node: wr_manager::service::AuthorizedNodeService,
    job: wr_manager::job_admin::AuthorizedJobService,
    policy: wr_manager::service::AuthorizedPolicyService,
    lifecycle: wr_manager::service::AuthorizedLifecycleService,
}

fn test_manager_services(pool: deadpool_postgres::Pool, manager: Manager) -> TestManagerServices {
    let admission = test_admission();
    let principal_policy = test_principal_policy();
    let authorizer = Arc::new(wr_manager::auth::ManagerAuthorizer::new(
        principal_policy.clone(),
        admission.clone(),
    ));
    let operator = wr_manager::service::OperatorApi::with_admission(
        pool.clone(),
        principal_policy.clone(),
        admission.clone(),
        5.0,
        10.0,
        10.0,
    );
    let infrastructure = wr_manager::service::InfrastructureApi::new(manager.clone(), operator);
    let node = wr_manager::service::ManagerNodeApi::new(
        manager.clone(),
        wr_manager::service::NodeAgentApi::with_admission(
            pool.clone(),
            principal_policy.clone(),
            admission.clone(),
        ),
    );
    let job = wr_manager::job_admin::JobAdminApi::new(
        pool,
        30,
        manager_test_pki().human_tls.clone(),
        admission.clone(),
    );
    let rollout = wr_common::lifecycle_service::ManagerLifecycleState::default();
    rollout.update(
        wr_common::wruntime::PrivilegedAdmissionState::Open,
        "",
        wr_common::wruntime::ManagerRolloutPhase::Unspecified as i32,
        "",
        0,
    );
    let policy =
        wr_manager::service::PolicyApi::new(principal_policy.clone(), admission, rollout.clone());
    let mut lifecycle_owner = wr_common::process_lifecycle::LifecycleOwner::new(
        wr_common::process_lifecycle::ServiceKind::Manager,
        "test-manager".to_string(),
    );
    lifecycle_owner.mark_ready("test manager ready").unwrap();
    let validated_policy = test_policy();
    let lifecycle = wr_manager::service::AuthenticatedLifecycleApi::new(
        wr_common::lifecycle_service::LifecycleServiceAdapter::new_manager(
            lifecycle_owner.snapshot(),
            "test-manager".to_owned(),
            validated_policy.generation,
            validated_policy.digest.clone(),
            rollout,
        ),
        principal_policy,
    );
    TestManagerServices {
        cluster: wr_manager::service::AuthorizedClusterService::new(manager, authorizer.clone()),
        infrastructure: wr_manager::service::AuthorizedInfrastructureService::new(
            infrastructure,
            authorizer.clone(),
        ),
        node: wr_manager::service::AuthorizedNodeService::new(node, authorizer.clone()),
        job: wr_manager::job_admin::AuthorizedJobService::new(job, authorizer.clone()),
        policy: wr_manager::service::AuthorizedPolicyService::new(policy, authorizer.clone()),
        lifecycle: wr_manager::service::AuthorizedLifecycleService::new(lifecycle, authorizer),
    }
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
        let mut tls = TonicClientTlsConfig::new()
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
    ) -> Result<ManagerServiceClient<Channel>> {
        let channel = self.channel(identity).await?;
        Ok(ManagerServiceClient::from_test_channels(
            channel.clone(),
            channel,
        ))
    }

    pub async fn agent_client(
        &self,
        identity: Option<&TonicTestIdentity>,
    ) -> Result<ManagerServiceClient<Channel>> {
        let channel = self.channel(identity).await?;
        Ok(ManagerServiceClient::from_test_channels(
            channel.clone(),
            channel,
        ))
    }
}

/// Start all role-gated manager services on one real mTLS listener.
pub async fn start_authorized_manager(pool: deadpool_postgres::Pool) -> Result<AuthorizedManager> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let pki = Arc::new(generate_role_test_pki());
    let manager = Manager::new(pool.clone(), test_secret_crypto());
    let services = test_manager_services(pool, manager);
    let tls = tonic::transport::ServerTlsConfig::new()
        .identity(Identity::from_pem(
            pki.server.cert_pem.clone(),
            pki.server.key_pem.clone(),
        ))
        .client_ca_root(Certificate::from_pem(pki.ca_pem.clone()));
    let server_task = tokio::spawn(async move {
        if let Err(error) = Server::builder()
            .tls_config(tls)
            .expect("test TLS configuration is valid")
            .add_service(ClusterServiceServer::new(services.cluster))
            .add_service(InfrastructureServiceServer::new(services.infrastructure))
            .add_service(NodeServiceServer::new(services.node))
            .add_service(JobServiceServer::new(services.job))
            .add_service(PolicyServiceServer::new(services.policy))
            .add_service(LifecycleServiceServer::new(services.lifecycle))
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
    let manager = Manager::new(pool.clone(), test_secret_crypto());
    let services = test_manager_services(pool, manager);
    tokio::spawn(
        Server::builder()
            .tls_config(
                wr_common::tls::build_tonic_server_tls(&manager_test_pki().server_tls)
                    .expect("test manager TLS"),
            )
            .expect("apply test manager TLS")
            .add_service(ClusterServiceServer::new(services.cluster))
            .add_service(InfrastructureServiceServer::new(services.infrastructure))
            .add_service(NodeServiceServer::new(services.node))
            .add_service(JobServiceServer::new(services.job))
            .add_service(PolicyServiceServer::new(services.policy))
            .add_service(LifecycleServiceServer::new(services.lifecycle))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    Ok(format!("https://{addr}"))
}

/// Provision manager-owned desired state for an integration-test registration,
/// then submit the registration with its operation-bound metadata.
pub async fn register_managed_engine(
    pool: &deadpool_postgres::Pool,
    client: &mut ManagerServiceClient<tonic::transport::Channel>,
    mut registration: EngineRegistration,
) -> Result<tonic::Response<RegisterEngineResponse>, tonic::Status> {
    let node_id = "node-a".to_string();
    let engine_slot = registration.engine_id.clone();
    let bundle_digest = format!("sha256:{}", "a".repeat(64));
    let mut modules = registration
        .modules
        .iter()
        .map(|module| ExpectedModule {
            identity: Some(wr_common::wruntime::ModuleIdentity {
                namespace: module.namespace.clone(),
                name: module.name.clone(),
                version: module.version.clone(),
            }),
            proto_schema_digest: wr_common::deployment_contract::schema_digest(
                &module.proto_schema,
            ),
        })
        .collect::<Vec<_>>();
    modules.sort_by(|a, b| {
        let ai = a.identity.as_ref().expect("test module identity");
        let bi = b.identity.as_ref().expect("test module identity");
        (&ai.namespace, &ai.name, &ai.version, &a.proto_schema_digest).cmp(&(
            &bi.namespace,
            &bi.name,
            &bi.version,
            &b.proto_schema_digest,
        ))
    });
    modules.dedup();
    let mut secrets = registration.secrets.clone();
    secrets.sort_by(|a, b| (&a.namespace, &a.key).cmp(&(&b.namespace, &b.key)));
    secrets.dedup();
    let mut db_namespaces = registration.db_namespaces.clone();
    db_namespaces.sort();
    db_namespaces.dedup();
    let expected_engine = ExpectedEngine {
        engine_slot: engine_slot.clone(),
        modules,
        secrets,
        db_namespaces,
        job_queue_id: registration.job_queue_id.clone(),
        job_admin_address: registration.job_admin_address.clone(),
    };
    let db = pool.get().await.map_err(|error| {
        tonic::Status::internal(format!("test fixture database checkout failed: {error}"))
    })?;
    let rows = db
        .query(
            "SELECT registration FROM wr_engines WHERE deployment_node_id=$1 AND engine_id<>$2",
            &[&node_id, &registration.engine_id],
        )
        .await
        .map_err(|error| {
            tonic::Status::internal(format!(
                "test fixture existing inventory query failed: {error}"
            ))
        })?;
    let mut engines = Vec::with_capacity(rows.len() + 1);
    let mut existing_registrations = Vec::with_capacity(rows.len());
    for row in rows {
        let existing =
            EngineRegistration::decode(row.get::<_, Vec<u8>>(0).as_slice()).map_err(|error| {
                tonic::Status::internal(format!("test fixture registration decode failed: {error}"))
            })?;
        let existing_slot = existing
            .deployment
            .as_ref()
            .map(|metadata| metadata.engine_slot.clone())
            .ok_or_else(|| {
                tonic::Status::internal("managed test engine omitted deployment metadata")
            })?;
        engines.push(ExpectedEngine {
            engine_slot: existing_slot,
            modules: existing
                .modules
                .iter()
                .map(|module| ExpectedModule {
                    identity: Some(wr_common::wruntime::ModuleIdentity {
                        namespace: module.namespace.clone(),
                        name: module.name.clone(),
                        version: module.version.clone(),
                    }),
                    proto_schema_digest: wr_common::deployment_contract::schema_digest(
                        &module.proto_schema,
                    ),
                })
                .collect(),
            secrets: existing.secrets.clone(),
            db_namespaces: existing.db_namespaces.clone(),
            job_queue_id: existing.job_queue_id.clone(),
            job_admin_address: existing.job_admin_address.clone(),
        });
        existing_registrations.push(existing);
    }
    engines.push(expected_engine);
    engines.sort_by(|a, b| a.engine_slot.cmp(&b.engine_slot));
    let attempt_token = uuid::Uuid::new_v4().simple().to_string();
    let deployment = client
        .begin_deployment(BeginDeploymentRequest {
            node_id: node_id.clone(),
            attempt_token: attempt_token.clone(),
            bundle_digest: bundle_digest.clone(),
            inventory: Some(DeploymentInventoryV1 {
                schema_version: 1,
                engines,
            }),
        })
        .await?
        .into_inner()
        .deployment
        .ok_or_else(|| tonic::Status::internal("test deployment response omitted deployment"))?;
    let resolved_release_digest = format!("sha256:{}", "b".repeat(64));
    client
        .finalize_deployment(FinalizeDeploymentRequest {
            node_id: node_id.clone(),
            attempt_token: attempt_token.clone(),
            revision: deployment.revision,
            bundle_digest: bundle_digest.clone(),
            resolved_release_digest: resolved_release_digest.clone(),
        })
        .await?;
    let operation = client
        .submit_operation(SubmitOperationRequest {
            node_id: node_id.clone(),
            request_token: attempt_token,
            action: NodeOperationAction::Deployment as i32,
            target_revision: deployment.revision,
            bundle_digest: bundle_digest.clone(),
            policy: Some(RolloutPolicy {
                max_unavailable: 1,
                allow_downtime: true,
                deadline_seconds: 300,
            }),
            resolved_release_digest,
            engine_slot: String::new(),
        })
        .await?
        .into_inner()
        .operation
        .ok_or_else(|| tonic::Status::internal("test operation response omitted operation"))?;
    for mut existing in existing_registrations {
        let existing_slot = existing
            .deployment
            .as_ref()
            .expect("validated managed test deployment metadata")
            .engine_slot
            .clone();
        existing.deployment = Some(DeploymentMetadata {
            node_id: node_id.clone(),
            revision: deployment.revision,
            bundle_digest: bundle_digest.clone(),
            engine_slot: existing_slot,
            operation_id: operation.operation_id.clone(),
            revision_digest: deployment.revision_digest.clone(),
        });
        let healthy_modules = existing.modules.clone();
        let existing_engine_id = existing.engine_id.clone();
        let fence = client
            .register_engine(RegisterEngineRequest {
                registration: Some(existing),
                activation_id: uuid::Uuid::new_v4().to_string(),
            })
            .await?
            .into_inner()
            .fence
            .ok_or_else(|| tonic::Status::internal("managed test re-registration omitted fence"))?;
        client
            .heartbeat(HeartbeatRequest {
                engine_id: existing_engine_id,
                healthy_modules,
                fence: Some(fence),
            })
            .await?;
    }
    registration.deployment = Some(DeploymentMetadata {
        node_id: node_id.clone(),
        revision: deployment.revision,
        bundle_digest,
        engine_slot,
        operation_id: operation.operation_id.clone(),
        revision_digest: deployment.revision_digest,
    });
    let response = client
        .register_engine(RegisterEngineRequest {
            registration: Some(registration),
            activation_id: uuid::Uuid::new_v4().to_string(),
        })
        .await?;
    let operation_id = uuid::Uuid::parse_str(&operation.operation_id)
        .map_err(|_| tonic::Status::internal("test operation ID was not a UUID"))?;
    db.execute(
        "UPDATE wr_node_operations SET state='succeeded', phase='complete', updated_at=NOW() WHERE operation_id=$1",
        &[&operation_id],
    )
    .await
    .map_err(|error| {
        tonic::Status::internal(format!("test fixture operation cleanup failed: {error}"))
    })?;
    db.execute(
        "UPDATE wr_nodes SET current_revision=$2, target_revision=NULL, updated_at=NOW() WHERE node_id=$1 AND target_revision=$2",
        &[&node_id, &(deployment.revision as i64)],
    )
    .await
    .map_err(|error| tonic::Status::internal(format!("test fixture deployment cleanup failed: {error}")))?;
    Ok(response)
}

pub async fn current_engine_fence(
    pool: &deadpool_postgres::Pool,
    engine_id: &str,
) -> Result<EngineOwnershipFence> {
    let db = pool.get().await?;
    let row = db
        .query_one(
            "SELECT deployment_node_id, deployment_engine_slot, deployment_revision_digest, activation_id::text, slot_generation FROM wr_engines WHERE engine_id=$1",
            &[&engine_id],
        )
        .await?;
    let generation: Vec<u8> = row.get(4);
    let generation: [u8; 8] = generation
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid slot generation width for engine {engine_id}"))?;
    Ok(EngineOwnershipFence {
        node_id: row.get(0),
        slot: row.get(1),
        revision_digest: row.get(2),
        activation_id: row.get(3),
        slot_generation: u64::from_be_bytes(generation),
    })
}

pub fn manager_proxy_tls() -> tonic::transport::ClientTlsConfig {
    wr_common::tls::build_tonic_client_tls(&manager_test_pki().proxy_tls)
        .expect("build test proxy manager TLS")
}

/// Return a connected manager gRPC client using role-correct human and proxy identities.
pub async fn manager_client(addr: &str) -> Result<ManagerServiceClient<tonic::transport::Channel>> {
    async fn channel(
        addr: &str,
        tls: &wr_common::node::ClientTlsConfig,
    ) -> Result<tonic::transport::Channel> {
        Ok(Endpoint::from_shared(addr.to_string())?
            .tls_config(wr_common::tls::build_tonic_client_tls(tls)?)?
            .connect()
            .await?)
    }
    let pki = manager_test_pki();
    let human = channel(addr, &pki.human_tls).await?;
    let proxy = channel(addr, &pki.proxy_tls).await?;
    Ok(ManagerServiceClient::from_test_channels(human, proxy))
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
    pool: &deadpool_postgres::Pool,
    c: &mut ManagerServiceClient<tonic::transport::Channel>,
    engine_id: &str,
    engine_addr: &str,
    namespace: &str,
    name: &str,
    version: &str,
) -> Result<EngineOwnershipFence> {
    let response = register_managed_engine(
        pool,
        c,
        EngineRegistration {
            engine_id: engine_id.into(),
            address: engine_addr.into(),
            proxy_address: TEST_SELF_PEER.into(),
            peer_address: TEST_SELF_PEER.into(),
            modules: vec![ModuleDescriptor {
                namespace: namespace.into(),
                name: name.into(),
                version: version.into(),
                proto_schema: minimal_file_descriptor_set(),
            }],
            ..Default::default()
        },
    )
    .await?
    .into_inner();
    response
        .fence
        .ok_or_else(|| anyhow::anyhow!("managed test registration omitted fence"))
}

pub async fn register_test_module_ready(
    pool: &deadpool_postgres::Pool,
    c: &mut ManagerServiceClient<tonic::transport::Channel>,
    engine_id: &str,
    engine_addr: &str,
    namespace: &str,
    name: &str,
    version: &str,
) -> Result<EngineOwnershipFence> {
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

/// Register two or more replicas as distinct slots of one desired revision.
/// This is required by operation-bound registration: sequential one-slot
/// deployments intentionally fence the earlier revision and are not replicas.
pub async fn register_test_replica_set_ready(
    pool: &deadpool_postgres::Pool,
    c: &mut ManagerServiceClient<tonic::transport::Channel>,
    engines: &[(&str, &str)],
    namespace: &str,
    name: &str,
    version: &str,
) -> Result<()> {
    let node_id = "node-a".to_string();
    let bundle_digest = format!("sha256:{}", "a".repeat(64));
    let schema = minimal_file_descriptor_set();
    let schema_digest = wr_common::deployment_contract::schema_digest(&schema);
    let inventory = DeploymentInventoryV1 {
        schema_version: 1,
        engines: engines
            .iter()
            .map(|(engine_id, _)| ExpectedEngine {
                engine_slot: (*engine_id).to_string(),
                modules: vec![ExpectedModule {
                    identity: Some(wr_common::wruntime::ModuleIdentity {
                        namespace: namespace.into(),
                        name: name.into(),
                        version: version.into(),
                    }),
                    proto_schema_digest: schema_digest.clone(),
                }],
                ..Default::default()
            })
            .collect(),
    };
    let attempt_token = uuid::Uuid::new_v4().simple().to_string();
    let deployment = c
        .begin_deployment(BeginDeploymentRequest {
            node_id: node_id.clone(),
            attempt_token: attempt_token.clone(),
            bundle_digest: bundle_digest.clone(),
            inventory: Some(inventory),
        })
        .await?
        .into_inner()
        .deployment
        .ok_or_else(|| tonic::Status::internal("replica deployment omitted deployment"))?;
    let resolved_release_digest = format!("sha256:{}", "b".repeat(64));
    c.finalize_deployment(FinalizeDeploymentRequest {
        node_id: node_id.clone(),
        attempt_token: attempt_token.clone(),
        revision: deployment.revision,
        bundle_digest: bundle_digest.clone(),
        resolved_release_digest: resolved_release_digest.clone(),
    })
    .await?;
    let operation = c
        .submit_operation(SubmitOperationRequest {
            node_id: node_id.clone(),
            request_token: attempt_token,
            action: NodeOperationAction::Deployment as i32,
            target_revision: deployment.revision,
            bundle_digest: bundle_digest.clone(),
            policy: Some(RolloutPolicy {
                max_unavailable: 1,
                allow_downtime: true,
                deadline_seconds: 300,
            }),
            resolved_release_digest,
            engine_slot: String::new(),
        })
        .await?
        .into_inner()
        .operation
        .ok_or_else(|| tonic::Status::internal("replica operation omitted operation"))?;
    let mut registrations = Vec::new();
    for (engine_id, address) in engines {
        let response = c
            .register_engine(RegisterEngineRequest {
                registration: Some(EngineRegistration {
                    engine_id: (*engine_id).into(),
                    address: (*address).into(),
                    proxy_address: TEST_SELF_PEER.into(),
                    peer_address: TEST_SELF_PEER.into(),
                    modules: vec![ModuleDescriptor {
                        namespace: namespace.into(),
                        name: name.into(),
                        version: version.into(),
                        proto_schema: schema.clone(),
                    }],
                    deployment: Some(DeploymentMetadata {
                        node_id: node_id.clone(),
                        revision: deployment.revision,
                        bundle_digest: bundle_digest.clone(),
                        engine_slot: (*engine_id).into(),
                        operation_id: operation.operation_id.clone(),
                        revision_digest: deployment.revision_digest.clone(),
                    }),
                    ..Default::default()
                }),
                activation_id: uuid::Uuid::new_v4().to_string(),
            })
            .await?
            .into_inner();
        registrations.push((
            (*engine_id).to_string(),
            response
                .fence
                .context("replica registration omitted fence")?,
        ));
    }
    let db = pool.get().await?;
    let operation_id = uuid::Uuid::parse_str(&operation.operation_id)?;
    db.execute(
        "UPDATE wr_node_operations SET state='succeeded',phase='complete',updated_at=NOW() WHERE operation_id=$1",
        &[&operation_id],
    )
    .await?;
    db.execute("UPDATE wr_nodes SET current_revision=$2,target_revision=NULL,updated_at=NOW() WHERE node_id=$1", &[&node_id, &(deployment.revision as i64)]).await?;
    drop(db);
    for (engine_id, fence) in registrations {
        c.heartbeat(HeartbeatRequest {
            engine_id,
            healthy_modules: vec![ModuleDescriptor {
                namespace: namespace.into(),
                name: name.into(),
                version: version.into(),
                ..Default::default()
            }],
            fence: Some(fence),
        })
        .await?;
    }
    wr_manager::db::update_route_health(pool, 30.0, 30.0)
        .await
        .map_err(|status| anyhow::anyhow!("update_route_health failed: {status}"))?;
    Ok(())
}

pub async fn register_test_module_ready_with_peer(
    pool: &deadpool_postgres::Pool,
    c: &mut ManagerServiceClient<tonic::transport::Channel>,
    engine: EngineSpec<'_>,
    module: ModuleSpec<'_>,
) -> Result<EngineOwnershipFence> {
    let engine_id = engine.id.to_string();
    let namespace = module.namespace.to_string();
    let name = module.name.to_string();
    let version = module.version.to_string();

    let registration = register_managed_engine(
        pool,
        c,
        EngineRegistration {
            engine_id: engine.id.into(),
            address: engine.addr.into(),
            proxy_address: TEST_SELF_PEER.into(),
            peer_address: engine.peer_address.into(),
            modules: vec![ModuleDescriptor {
                namespace: module.namespace.into(),
                name: module.name.into(),
                version: module.version.into(),
                proto_schema: module.schema.to_vec(),
            }],
            ..Default::default()
        },
    )
    .await?
    .into_inner();
    let fence = registration
        .fence
        .ok_or_else(|| anyhow::anyhow!("managed ready registration omitted fence"))?;
    c.heartbeat(HeartbeatRequest {
        engine_id: engine_id.clone(),
        healthy_modules: vec![ModuleDescriptor {
            name: name.clone(),
            namespace: namespace.clone(),
            version: version.clone(),
            proto_schema: vec![],
        }],

        fence: Some(fence.clone()),
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
            return Ok(fence);
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
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let manager = Manager::new(pool.clone(), test_secret_crypto());
    let services = test_manager_services(pool.clone(), manager);
    tokio::spawn(
        Server::builder()
            .tls_config(
                wr_common::tls::build_tonic_server_tls(&manager_test_pki().server_tls)
                    .expect("test manager TLS"),
            )
            .expect("apply test manager TLS")
            .add_service(ClusterServiceServer::new(services.cluster))
            .add_service(InfrastructureServiceServer::new(services.infrastructure))
            .add_service(NodeServiceServer::new(services.node))
            .add_service(JobServiceServer::new(services.job))
            .add_service(PolicyServiceServer::new(services.policy))
            .add_service(LifecycleServiceServer::new(services.lifecycle))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    tokio::spawn(wr_manager::state::monitor_heartbeats(
        pool,
        timeout_secs,
        timeout_secs,
        std::time::Duration::from_millis(200),
    ));
    Ok(format!("https://{addr}"))
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
    /// This manager's PostgreSQL lease identity.
    pub manager_id: String,
    grpc_task: tokio::task::JoinHandle<()>,
}

impl ClusteredManager {
    /// Abruptly stop this manager's request-serving process while preserving the
    /// shared database, as a takeover fixture.
    pub fn abort_service(&self) {
        self.grpc_task.abort();
    }
}

/// Start `count` managers sharing the same PostgreSQL control plane.
pub async fn start_manager_cluster(
    pool: deadpool_postgres::Pool,
    count: usize,
    heartbeat_timeout_secs: u64,
) -> Result<Vec<ClusteredManager>> {
    let mut managers = Vec::with_capacity(count);

    for _ in 0..count {
        let manager_id = uuid::Uuid::new_v4().to_string();
        let grpc_listener = TcpListener::bind("127.0.0.1:0").await?;
        let grpc_addr = grpc_listener.local_addr()?;
        let grpc_url = format!("https://{grpc_addr}");

        wr_manager::db::register_manager(&pool, &manager_id, &grpc_url)
            .await
            .map_err(|e| anyhow::anyhow!("register_manager: {e}"))?;

        let manager = Manager::new(pool.clone(), test_secret_crypto());
        let services = test_manager_services(pool.clone(), manager);

        // Retain the request-serving owner so takeover tests can stop one
        // manager without altering its shared PostgreSQL lease row.
        let grpc_task = tokio::spawn(async move {
            if let Err(error) = Server::builder()
                .tls_config(
                    wr_common::tls::build_tonic_server_tls(&manager_test_pki().server_tls)
                        .expect("test manager TLS"),
                )
                .expect("apply test manager TLS")
                .add_service(ClusterServiceServer::new(services.cluster))
                .add_service(InfrastructureServiceServer::new(services.infrastructure))
                .add_service(NodeServiceServer::new(services.node))
                .add_service(JobServiceServer::new(services.job))
                .add_service(PolicyServiceServer::new(services.policy))
                .add_service(LifecycleServiceServer::new(services.lifecycle))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                    grpc_listener,
                ))
                .await
            {
                panic!("clustered manager test server failed: {error}");
            }
        });

        tokio::spawn(wr_manager::state::monitor_heartbeats(
            pool.clone(),
            heartbeat_timeout_secs,
            heartbeat_timeout_secs,
            std::time::Duration::from_millis(200),
        ));

        managers.push(ClusteredManager {
            addr: grpc_url,
            manager_id,
            grpc_task,
        });
    }

    Ok(managers)
}
