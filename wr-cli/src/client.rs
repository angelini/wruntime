use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{Context, Result};
use tonic::transport::{Channel, Endpoint};
use wr_common::node::TlsConfig;
use wr_common::wruntime::manager_service_client::ManagerServiceClient;
use wr_common::wruntime::ListManagersRequest;

/// Global TLS config for CLI → manager connections.
/// Set once at startup via [`set_tls_config`].
static TLS_CONFIG: OnceLock<TlsConfig> = OnceLock::new();

/// Store the TLS config for all subsequent `connect()` calls.
pub fn set_tls_config(config: TlsConfig) {
    let _ = TLS_CONFIG.set(config);
}

/// Connect to a specific manager address with a 5-second timeout.
/// Uses the global TLS config if set via [`set_tls_config`].
pub async fn connect(addr: &str) -> Result<ManagerServiceClient<Channel>> {
    let mut endpoint = Endpoint::from_shared(addr.to_string())?
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10));

    if let Some(tls) = TLS_CONFIG.get() {
        let tls_config = wr_common::tls::build_tonic_client_tls(tls)?;
        endpoint = endpoint.tls_config(tls_config)?;
    }

    let channel = endpoint
        .connect()
        .await
        .context("failed to connect to manager")?;
    Ok(ManagerServiceClient::new(channel))
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
