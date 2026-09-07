use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use serde::Deserialize;
use wr_common::authorization_policy::ValidatedPolicy;
use wr_common::identity::ManagerId;
use wr_common::node::{ClientTlsConfig, ServerTlsConfig};
use wr_common::DEFAULT_MANAGER_LIVENESS_THRESHOLD_SECS;

pub const DEFAULT_MANAGER_HEARTBEAT_INTERVAL_SECS: u64 = 1;
pub const DEFAULT_MANAGER_STALE_ROW_REAP_THRESHOLD_SECS: u64 = 300;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HeartbeatTimeoutSecs(NonZeroU64);

impl HeartbeatTimeoutSecs {
    pub fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationConfig {
    pub policy_file: String,
}

#[derive(Clone)]
pub struct ManagerConfig {
    /// Stable manager identity across process activations.
    pub manager_id: ManagerId,
    /// gRPC listen address, e.g. "0.0.0.0:9000"
    pub listen_address: String,
    /// How long (seconds) without a heartbeat before an engine is considered unhealthy
    pub engine_heartbeat_timeout_secs: u64,
    /// How long (seconds) without a per-module heartbeat before that module's
    /// routes are marked unhealthy.
    pub module_heartbeat_timeout_secs: HeartbeatTimeoutSecs,
    /// Loopback proxy address the scheduler POSTs jobs to, e.g.
    /// "http://127.0.0.1:9001". REQUIRED — startup fails if unset or empty.
    pub local_proxy_address: String,
    /// How long (seconds) a claimed schedule lease is held before another manager
    /// may reclaim it. Must exceed worst-case per-tick submission time.
    pub scheduler_lease_secs: u64,
    /// Base backoff (seconds) for a failed submission; doubles per consecutive failure.
    pub scheduler_retry_base_secs: u64,
    /// Maximum backoff (seconds) cap for consecutive failures.
    pub scheduler_retry_cap_secs: u64,
    /// PostgreSQL connection pool configuration.
    pub database: DatabaseConfig,
    /// Cluster configuration for multi-manager HA.
    pub cluster: ClusterConfig,
    /// Server identity and client roots for the sole manager gRPC listener.
    pub tls: ServerTlsConfig,
    /// Stable manager workload identity used for self-observation and engine job administration.
    pub client_tls: ClientTlsConfig,
    /// Resolved authorization-policy path and immutable validated snapshot.
    pub authorization_policy_path: PathBuf,
    pub authorization_policy: Arc<ValidatedPolicy>,
}

#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    /// This manager's gRPC address as reachable by proxies.
    /// Defaults to listen_address if not set.
    #[serde(default)]
    pub advertise_grpc_address: Option<String>,
    /// How often this manager renews its PostgreSQL lease.
    #[serde(default = "default_manager_heartbeat_interval_secs")]
    pub manager_heartbeat_interval_secs: u64,
    /// How long a manager lease remains live for discovery and status.
    #[serde(default = "default_manager_liveness_threshold_secs")]
    pub manager_liveness_threshold_secs: u64,
    /// How old a dead manager row must be before it is reaped.
    #[serde(default = "default_manager_stale_row_reap_threshold_secs")]
    pub manager_stale_row_reap_threshold_secs: u64,
}

fn default_manager_heartbeat_interval_secs() -> u64 {
    DEFAULT_MANAGER_HEARTBEAT_INTERVAL_SECS
}

fn default_manager_liveness_threshold_secs() -> u64 {
    DEFAULT_MANAGER_LIVENESS_THRESHOLD_SECS
}

fn default_manager_stale_row_reap_threshold_secs() -> u64 {
    DEFAULT_MANAGER_STALE_ROW_REAP_THRESHOLD_SECS
}

#[derive(Deserialize, Clone)]
pub struct DatabaseConfig {
    /// `postgres://user:pass@host:port/dbname` connection string.
    pub url: String,
    /// Maximum number of pooled connections. Defaults to 10.
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
}

fn default_heartbeat_timeout() -> u64 {
    10
}

fn default_scheduler_lease_secs() -> u64 {
    30
}
fn default_scheduler_retry_base_secs() -> u64 {
    5
}
fn default_scheduler_retry_cap_secs() -> u64 {
    300
}

fn default_max_connections() -> usize {
    10
}

#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct RawManagerConfig {
    pub manager_id: String,
    pub listen_address: String,
    #[serde(default = "default_heartbeat_timeout")]
    pub engine_heartbeat_timeout_secs: u64,
    #[serde(default)]
    pub module_heartbeat_timeout_secs: Option<u64>,
    pub local_proxy_address: String,
    #[serde(default = "default_scheduler_lease_secs")]
    pub scheduler_lease_secs: u64,
    #[serde(default = "default_scheduler_retry_base_secs")]
    pub scheduler_retry_base_secs: u64,
    #[serde(default = "default_scheduler_retry_cap_secs")]
    pub scheduler_retry_cap_secs: u64,
    pub database: DatabaseConfig,
    pub cluster: ClusterConfig,
    pub tls: ServerTlsConfig,
    pub client_tls: ClientTlsConfig,
    pub authorization: AuthorizationConfig,
}

impl wr_common::config::Validatable for RawManagerConfig {
    fn validate(&self) -> Result<()> {
        self.validate_inner()
    }
}

impl RawManagerConfig {
    fn validate_inner(&self) -> Result<()> {
        use wr_common::config::Validator;
        let mut v = Validator::new();

        v.check(
            ManagerId::parse(&self.manager_id).is_ok(),
            "manager_id must be a valid stable identity",
        );
        v.check(
            !self.listen_address.is_empty(),
            "listen_address is required",
        );
        v.check(
            self.engine_heartbeat_timeout_secs > 0,
            "engine_heartbeat_timeout_secs must be > 0",
        );
        if let Some(t) = self.module_heartbeat_timeout_secs {
            v.check(t > 0, "module_heartbeat_timeout_secs must be > 0");
        }
        v.check(
            !self.local_proxy_address.is_empty(),
            "local_proxy_address is required",
        );
        v.check(
            self.scheduler_lease_secs > 0,
            "scheduler_lease_secs must be > 0",
        );
        v.check(
            self.scheduler_retry_base_secs > 0,
            "scheduler_retry_base_secs must be > 0",
        );
        v.check(
            self.scheduler_retry_cap_secs >= self.scheduler_retry_base_secs,
            "scheduler_retry_cap_secs must be >= scheduler_retry_base_secs",
        );
        v.check(!self.database.url.is_empty(), "database.url is required");
        v.check(
            self.database.max_connections > 0,
            "database.max_connections must be > 0",
        );
        v.check(
            self.cluster.manager_heartbeat_interval_secs > 0,
            "cluster.manager_heartbeat_interval_secs must be > 0",
        );
        v.check(
            self.cluster.manager_liveness_threshold_secs
                > self.cluster.manager_heartbeat_interval_secs,
            "cluster.manager_liveness_threshold_secs must be greater than manager_heartbeat_interval_secs",
        );
        v.check(
            self.cluster.manager_stale_row_reap_threshold_secs
                >= self
                    .cluster
                    .manager_liveness_threshold_secs
                    .saturating_mul(10),
            "cluster.manager_stale_row_reap_threshold_secs must be at least 10 times manager_liveness_threshold_secs",
        );
        v.check(!self.tls.cert_path.is_empty(), "tls.cert_path is required");
        v.check(!self.tls.key_path.is_empty(), "tls.key_path is required");
        v.check(
            !self.tls.client_ca_cert_path.is_empty(),
            "tls.client_ca_cert_path is required",
        );
        v.check(
            !self.client_tls.cert_path.is_empty(),
            "client_tls.cert_path is required",
        );
        v.check(
            !self.client_tls.key_path.is_empty(),
            "client_tls.key_path is required",
        );
        v.check(
            !self.client_tls.server_ca_cert_path.is_empty(),
            "client_tls.server_ca_cert_path is required",
        );
        v.check(
            !self.authorization.policy_file.trim().is_empty(),
            "authorization.policy_file is required",
        );

        v.finish()
    }
}

impl ManagerConfig {
    pub fn load(path: &str) -> Result<Self> {
        let raw: RawManagerConfig = wr_common::config::load(path)?;
        let module_timeout = raw
            .module_heartbeat_timeout_secs
            .unwrap_or(raw.engine_heartbeat_timeout_secs);
        let module_heartbeat_timeout_secs = NonZeroU64::new(module_timeout)
            .map(HeartbeatTimeoutSecs)
            .ok_or_else(|| anyhow::anyhow!("module_heartbeat_timeout_secs must be > 0"))?;
        let manager_id = ManagerId::parse(raw.manager_id)?;
        let config_path = Path::new(path);
        let policy_path = {
            let configured = Path::new(&raw.authorization.policy_file);
            if configured.is_absolute() {
                configured.to_path_buf()
            } else {
                config_path
                    .parent()
                    .unwrap_or_else(|| Path::new("."))
                    .join(configured)
            }
        };
        let metadata = std::fs::metadata(&policy_path).with_context(|| {
            format!(
                "failed to inspect authorization policy {}",
                policy_path.display()
            )
        })?;
        anyhow::ensure!(
            metadata.len() <= wr_common::authorization_policy::MAX_POLICY_BYTES as u64,
            "authorization policy exceeds 4 MiB raw limit"
        );
        let bytes = std::fs::read(&policy_path).with_context(|| {
            format!(
                "failed to read authorization policy {}",
                policy_path.display()
            )
        })?;
        let authorization_policy = Arc::new(ValidatedPolicy::load(&bytes)?);
        let advertised = raw
            .cluster
            .advertise_grpc_address
            .clone()
            .unwrap_or_else(|| {
                format!(
                    "https://{}",
                    raw.listen_address.replace("0.0.0.0", "127.0.0.1")
                )
            });
        let enrollment = authorization_policy
            .manager_enrollments
            .iter()
            .find(|item| item.manager_id == manager_id.as_str())
            .ok_or_else(|| {
                anyhow::anyhow!("running manager is not enrolled by authorization policy")
            })?;
        anyhow::ensure!(
            enrollment.endpoint == advertised,
            "manager advertised endpoint does not match authorization policy enrollment"
        );
        let cluster_id = wr_common::identity::ClusterId::parse(&authorization_policy.cluster_id)?;
        let manager_leaf =
            wr_common::tls::load_client_leaf_evidence(&raw.client_tls.cert_path, Some(&cluster_id))
                .context("failed to validate manager client-profile certificate")?;
        let manager_principal = manager_leaf.principal.as_ref().ok_or_else(|| {
            anyhow::anyhow!("manager client-profile certificate has no principal")
        })?;
        anyhow::ensure!(manager_principal.as_str() == enrollment.principal, "manager client-profile certificate principal does not match authorization policy enrollment");
        anyhow::ensure!(
            !authorization_policy
                .revoked_leaf_fingerprints
                .contains(&manager_leaf.fingerprint),
            "manager client-profile certificate is revoked by authorization policy"
        );

        Ok(Self {
            manager_id,
            listen_address: raw.listen_address,
            engine_heartbeat_timeout_secs: raw.engine_heartbeat_timeout_secs,
            module_heartbeat_timeout_secs,
            local_proxy_address: raw.local_proxy_address,
            scheduler_lease_secs: raw.scheduler_lease_secs,
            scheduler_retry_base_secs: raw.scheduler_retry_base_secs,
            scheduler_retry_cap_secs: raw.scheduler_retry_cap_secs,
            database: raw.database,
            cluster: raw.cluster,
            tls: raw.tls,
            client_tls: raw.client_tls,
            authorization_policy_path: policy_path,
            authorization_policy,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> RawManagerConfig {
        RawManagerConfig {
            manager_id: "manager-a".into(),
            listen_address: "127.0.0.1:9000".into(),
            engine_heartbeat_timeout_secs: 10,
            module_heartbeat_timeout_secs: None,
            local_proxy_address: "http://127.0.0.1:9001".into(),
            scheduler_lease_secs: 30,
            scheduler_retry_base_secs: 5,
            scheduler_retry_cap_secs: 300,
            database: DatabaseConfig {
                url: "postgres://localhost/wruntime".into(),
                max_connections: 10,
            },
            cluster: ClusterConfig {
                advertise_grpc_address: None,
                manager_heartbeat_interval_secs: DEFAULT_MANAGER_HEARTBEAT_INTERVAL_SECS,
                manager_liveness_threshold_secs: DEFAULT_MANAGER_LIVENESS_THRESHOLD_SECS,
                manager_stale_row_reap_threshold_secs:
                    DEFAULT_MANAGER_STALE_ROW_REAP_THRESHOLD_SECS,
            },
            tls: ServerTlsConfig {
                cert_path: "cert".into(),
                key_path: "key".into(),
                client_ca_cert_path: "client-ca".into(),
            },
            client_tls: ClientTlsConfig {
                cert_path: "manager-client-cert".into(),
                key_path: "manager-client-key".into(),
                server_ca_cert_path: "server-ca".into(),
            },
            authorization: AuthorizationConfig {
                policy_file: "policy/authorization.toml".into(),
            },
        }
    }

    #[test]
    fn manager_lease_intervals_are_ordered_and_positive() {
        let mut invalid = config();
        invalid.cluster.manager_heartbeat_interval_secs = 0;
        invalid.cluster.manager_liveness_threshold_secs = 1;
        invalid.cluster.manager_stale_row_reap_threshold_secs = 9;
        let error = invalid.validate_inner().unwrap_err().to_string();
        assert!(error.contains("manager_heartbeat_interval_secs must be > 0"));
        assert!(error.contains("must be at least 10 times"));
    }

    #[test]
    fn policy_path_and_stable_manager_id_are_required() {
        let mut invalid = config();
        invalid.manager_id = "not_valid".into();
        invalid.authorization.policy_file.clear();
        let error = invalid.validate_inner().unwrap_err().to_string();
        assert!(error.contains("manager_id"));
        assert!(error.contains("authorization.policy_file"));
    }
}
