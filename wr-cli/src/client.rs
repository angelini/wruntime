use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{Context, Result};
use tonic::transport::{Channel, Endpoint};
use wr_common::node::TlsConfig;
use wr_common::wruntime::lifecycle_service_client::LifecycleServiceClient;
use wr_common::wruntime::manager_service_client::ManagerServiceClient;
use wr_common::wruntime::node_service_client::NodeServiceClient;
use wr_common::wruntime::{GetClusterStatusRequest, GetClusterStatusResponse, ListManagersRequest};

/// Global TLS config for CLI → manager connections.
/// Set once at startup via [`set_tls_config`].
static TLS_CONFIG: OnceLock<TlsConfig> = OnceLock::new();

/// Store the TLS config for all subsequent `connect()` calls.
pub fn set_tls_config(config: TlsConfig) {
    let _ = TLS_CONFIG.set(config);
}

fn connection_tls_config(explicit: Option<&TlsConfig>) -> Option<&TlsConfig> {
    match explicit {
        Some(config) => Some(config),
        None => TLS_CONFIG.get(),
    }
}

fn endpoint_with_tls(addr: &str, tls: Option<&TlsConfig>) -> Result<Endpoint> {
    let mut endpoint = Endpoint::from_shared(addr.to_string())?
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10));

    if let Some(tls) = tls {
        endpoint = endpoint.tls_config(wr_common::tls::build_tonic_client_tls(tls)?)?;
    }

    Ok(endpoint)
}

fn endpoint(addr: &str, explicit_tls: Option<&TlsConfig>) -> Result<Endpoint> {
    endpoint_with_tls(addr, connection_tls_config(explicit_tls))
}

async fn connect_inner(
    addr: &str,
    explicit_tls: Option<&TlsConfig>,
) -> Result<ManagerServiceClient<Channel>> {
    let channel = endpoint(addr, explicit_tls)?
        .connect()
        .await
        .context("failed to connect to manager")?;
    Ok(ManagerServiceClient::new(channel))
}

/// Connect to a specific manager address with the standard endpoint timeouts.
/// Uses the global TLS config if set via [`set_tls_config`].
pub async fn connect(addr: &str) -> Result<ManagerServiceClient<Channel>> {
    connect_inner(addr, None).await
}

/// Return the process-global CLI TLS credentials, when initialized.
pub fn tls_config() -> Option<&'static TlsConfig> {
    TLS_CONFIG.get()
}

/// Connect to a lifecycle endpoint. Passing `None` intentionally creates a
/// plaintext channel and does not inherit the manager TLS configuration.
pub async fn connect_lifecycle(
    addr: &str,
    tls: Option<&TlsConfig>,
) -> Result<LifecycleServiceClient<Channel>> {
    let channel = endpoint_with_tls(addr, tls)?
        .connect()
        .await
        .with_context(|| format!("failed to connect to lifecycle endpoint {addr}"))?;
    Ok(LifecycleServiceClient::new(channel))
}

/// Connect to the proxy's loopback NodeService without inheriting manager TLS.
pub async fn connect_node(addr: &str) -> Result<NodeServiceClient<Channel>> {
    let channel = endpoint_with_tls(addr, None)?
        .connect()
        .await
        .with_context(|| format!("failed to connect to proxy node endpoint {addr}"))?;
    Ok(NodeServiceClient::new(channel))
}

/// Fetch one coherent cluster status snapshot from a seed manager.
pub async fn get_cluster_status(addr: &str) -> Result<GetClusterStatusResponse> {
    let mut client = connect(addr).await?;
    Ok(client
        .get_cluster_status(GetClusterStatusRequest {})
        .await?
        .into_inner())
}

/// List all active managers in the cluster via a seed manager.
pub async fn list_managers(addr: &str) -> Result<Vec<(String, String)>> {
    let mut client = connect(addr).await?;
    let resp = client
        .list_managers(ListManagersRequest {})
        .await?
        .into_inner();
    Ok(resp
        .managers
        .into_iter()
        .map(|m| (m.manager_id, m.grpc_address))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tls_config(prefix: &str) -> TlsConfig {
        TlsConfig {
            cert_path: format!("{prefix}/client.crt"),
            key_path: format!("{prefix}/client.key"),
            ca_cert_path: format!("{prefix}/ca.crt"),
        }
    }

    #[test]
    fn explicit_tls_overrides_initialized_global_config() {
        set_tls_config(tls_config("global"));
        let explicit = tls_config("deploy");

        let selected = connection_tls_config(Some(&explicit)).unwrap();

        assert_eq!(selected.cert_path, "deploy/client.crt");
        assert_eq!(selected.key_path, "deploy/client.key");
        assert_eq!(selected.ca_cert_path, "deploy/ca.crt");
    }
}
