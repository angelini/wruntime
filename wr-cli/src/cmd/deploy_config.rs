use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use super::helpers::DeployPort;

/// Optional deploy configuration file (`wr-deploy.toml`).
///
/// All fields are optional — values are merged with CLI flags and env vars
/// using the precedence: CLI flag > config file > env var > default.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct DeployConfig {
    /// Platform PostgreSQL URL used by manager/proxy/job-queue code only.
    pub db_url: Option<String>,
    /// Node-local endpoint for certificate-authenticated tenant databases.
    pub tenant_database: Option<TenantDeployConfigSource>,
    /// Secret encryption key (manager deploy only)
    pub secret_key: Option<String>,
    /// SSH private key path
    pub ssh_key: Option<String>,
    /// SSH port
    pub ssh_port: Option<u16>,
    /// Cross-compilation target triple
    pub target: Option<String>,
    /// Base directory for installed files on the remote host
    pub workdir: Option<String>,
    /// Source proxy config file for node bundle generation
    pub proxy_config: Option<String>,
    /// Disable OpenTelemetry export in generated service units
    pub no_otel: Option<bool>,
    /// Path to schedules TOML file for post-deploy apply
    pub schedules_path: Option<String>,
    /// Local directory containing CA + node certificates (from `wr-cli cert`)
    pub cert_dir: Option<String>,
    /// mTLS peer listener port (default: 9443)
    pub peer_port: Option<u16>,
    /// Local node-agent client certificate used only during explicit install/update.
    pub agent_cert: Option<String>,
    /// Local node-agent private key used only during explicit install/update.
    pub agent_key: Option<String>,
    /// Local CA certificate installed for the node agent.
    pub agent_ca_cert: Option<String>,
    /// Absolute host systemctl binary path attested by the agent.
    pub agent_systemctl_path: Option<String>,
    /// Absolute host Docker CLI path attested by the agent.
    pub agent_docker_path: Option<String>,
    /// Stable Docker Compose project identity used by the host agent.
    pub agent_compose_project: Option<String>,
    /// Agent manager polling interval in seconds.
    pub agent_poll_seconds: Option<u64>,
    /// Agent lease-renewal interval in seconds.
    pub agent_renew_seconds: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TenantDeployConfigSource {
    pub server_name: String,
    pub host_addr: Option<String>,
    pub port: Option<u16>,
    pub connect_timeout_secs: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TenantDeployConfig {
    pub server_name: String,
    pub host_addr: Option<String>,
    pub port: u16,
    pub connect_timeout_secs: u64,
}

#[derive(Clone, Debug, Default)]
pub struct TenantDeployOverrides {
    pub server_name: Option<String>,
    pub host_addr: Option<String>,
    pub port: Option<u16>,
    pub connect_timeout_secs: Option<u64>,
}

impl TenantDeployConfig {
    pub fn validate(&self) -> Result<()> {
        let name = self.server_name.as_str();
        anyhow::ensure!(
            !name.is_empty()
                && name.len() <= 253
                && !name.contains("://")
                && !name.chars().any(|ch| matches!(ch, '/' | '@' | '*' | ':'))
                && !name.starts_with('.')
                && !name.ends_with('.')
                && name.split('.').all(|label| {
                    !label.is_empty()
                        && label.len() <= 63
                        && !label.starts_with('-')
                        && !label.ends_with('-')
                        && label.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                }),
            "tenant database server_name must be a canonical DNS name without URL syntax or wildcards"
        );
        anyhow::ensure!(self.port != 0, "tenant database port must be nonzero");
        anyhow::ensure!(
            self.connect_timeout_secs != 0,
            "tenant database connect timeout must be nonzero"
        );
        if let Some(host) = &self.host_addr {
            let address: std::net::IpAddr = host
                .parse()
                .with_context(|| "tenant database host_addr must be an IP literal")?;
            let private = match address {
                std::net::IpAddr::V4(ip) => {
                    ip.is_private() || ip.is_loopback() || ip.is_link_local()
                }
                std::net::IpAddr::V6(ip) => {
                    ip.is_unique_local() || ip.is_loopback() || ip.is_unicast_link_local()
                }
            };
            anyhow::ensure!(
                private,
                "tenant database host_addr must be on a private node/service network"
            );
        }
        Ok(())
    }
}

fn env_optional(key: &str) -> Result<Option<String>> {
    match std::env::var(key) {
        Ok(value) if !value.is_empty() => Ok(Some(value)),
        Ok(_) | Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => anyhow::bail!("{key} must be UTF-8"),
    }
}

pub fn resolve_tenant_database(
    cli: TenantDeployOverrides,
    config: Option<TenantDeployConfigSource>,
) -> Result<Option<TenantDeployConfig>> {
    let configured = config.is_some();
    let config = config.unwrap_or(TenantDeployConfigSource {
        server_name: String::new(),
        host_addr: None,
        port: None,
        connect_timeout_secs: None,
    });
    let env_server = env_optional("WR_TENANT_DB_SERVER_NAME")?;
    let env_host = env_optional("WR_TENANT_DB_HOST_ADDR")?;
    let env_port = env_optional("WR_TENANT_DB_PORT")?;
    let env_timeout = env_optional("WR_TENANT_DB_CONNECT_TIMEOUT_SECS")?;
    let present = cli.server_name.is_some()
        || cli.host_addr.is_some()
        || cli.port.is_some()
        || cli.connect_timeout_secs.is_some()
        || configured
        || env_server.is_some()
        || env_host.is_some()
        || env_port.is_some()
        || env_timeout.is_some();
    if !present {
        return Ok(None);
    }
    let parse = |value: Option<String>, field: &str| -> Result<Option<u64>> {
        value
            .map(|value| {
                value
                    .parse::<u64>()
                    .with_context(|| format!("{field} must be an integer"))
            })
            .transpose()
    };
    let port = cli
        .port
        .map(u64::from)
        .or(config.port.map(u64::from))
        .or(parse(env_port, "WR_TENANT_DB_PORT")?)
        .unwrap_or(5432);
    let port = u16::try_from(port).context("tenant database port is out of range")?;
    let value = TenantDeployConfig {
        server_name: cli
            .server_name
            .or_else(|| (!config.server_name.is_empty()).then_some(config.server_name))
            .or(env_server)
            .context("tenant database server_name is required")?,
        host_addr: cli.host_addr.or(config.host_addr).or(env_host),
        port,
        connect_timeout_secs: cli
            .connect_timeout_secs
            .or(config.connect_timeout_secs)
            .or(parse(env_timeout, "WR_TENANT_DB_CONNECT_TIMEOUT_SECS")?)
            .unwrap_or(10),
    };
    value.validate()?;
    Ok(Some(value))
}

impl DeployConfig {
    /// Load from a TOML file.
    pub fn load(path: &str) -> Result<Self> {
        let content =
            std::fs::read_to_string(path).with_context(|| format!("failed to read {path}"))?;
        toml::from_str(&content).with_context(|| format!("failed to parse {path}"))
    }

    /// Load from an explicit path (error on failure) or auto-discover
    /// `wr-deploy.toml` in the current directory (silently return defaults if absent).
    pub fn load_or_discover(explicit: Option<&str>) -> Result<Self> {
        if let Some(path) = explicit {
            return Self::load(path);
        }
        if std::path::Path::new("wr-deploy.toml").exists() {
            Self::load("wr-deploy.toml")
        } else {
            Ok(Self::default())
        }
    }
}

// --- Resolution helpers ---
// Precedence: CLI flag > config file > env var

/// Resolve an optional string value from CLI > config > env.
pub fn resolve_string(
    cli: Option<String>,
    config: Option<String>,
    env_key: &str,
) -> Option<String> {
    resolve_string_from(
        cli,
        config,
        std::env::var(env_key)
            .ok()
            .filter(|value| !value.is_empty()),
    )
}

pub(crate) fn resolve_string_from(
    cli: Option<String>,
    config: Option<String>,
    environment: Option<String>,
) -> Option<String> {
    cli.or(config).or(environment)
}

/// Resolve a required string value. Bails with a message listing all sources.
pub fn resolve_required(
    cli: Option<String>,
    config: Option<String>,
    env_key: &str,
    field_name: &str,
) -> Result<String> {
    resolve_string(cli, config, env_key).ok_or_else(|| {
        let flag = field_name.replace('_', "-");
        anyhow::anyhow!(
            "{field_name} is required: pass --{flag}, set {env_key}, or add {field_name} to wr-deploy.toml"
        )
    })
}

/// Resolve a string with a hardcoded default: CLI > config > env > default.
pub fn resolve_with_default(
    cli: &str,
    clap_default: &str,
    config: Option<String>,
    env_key: &str,
) -> String {
    // If the CLI value differs from clap's default, the user explicitly passed it.
    if cli != clap_default {
        return cli.to_string();
    }
    config
        .or_else(|| std::env::var(env_key).ok().filter(|s| !s.is_empty()))
        .unwrap_or_else(|| cli.to_string())
}

fn optional_deploy_port(port: Option<u16>, source: &str) -> Result<Option<DeployPort>> {
    port.map(|value| DeployPort::new(value).with_context(|| format!("invalid {source}")))
        .transpose()
}

fn parse_deploy_port(value: &str, source: &str) -> Result<DeployPort> {
    let port = value
        .parse::<u16>()
        .with_context(|| format!("{source} must be a non-zero TCP port, got '{value}'"))?;
    DeployPort::new(port)
        .with_context(|| format!("{source} must be a non-zero TCP port, got '{value}'"))
}

fn env_deploy_port(key: &str) -> Result<Option<DeployPort>> {
    match std::env::var(key) {
        Ok(value) => parse_deploy_port(&value, key).map(Some),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => {
            anyhow::bail!("{key} must contain a valid UTF-8 TCP port")
        }
    }
}

#[derive(Debug, Default)]
pub(crate) enum SshPortEnvironment {
    #[default]
    NotPresent,
    Value(String),
    NotUnicode,
}

impl SshPortEnvironment {
    pub(crate) fn from_var(value: std::result::Result<String, std::env::VarError>) -> Self {
        match value {
            Ok(value) => Self::Value(value),
            Err(std::env::VarError::NotPresent) => Self::NotPresent,
            Err(std::env::VarError::NotUnicode(_)) => Self::NotUnicode,
        }
    }
}

/// Resolve SSH port from CLI > config > env. Returns None to use SSH default.
pub fn resolve_ssh_port(cli: Option<u16>, config: Option<u16>) -> Result<Option<DeployPort>> {
    resolve_ssh_port_from(
        cli,
        config,
        SshPortEnvironment::from_var(std::env::var("WR_SSH_PORT")),
    )
}

pub(crate) fn resolve_ssh_port_from(
    cli: Option<u16>,
    config: Option<u16>,
    environment: SshPortEnvironment,
) -> Result<Option<DeployPort>> {
    if let Some(port) = optional_deploy_port(cli, "--ssh-port")? {
        return Ok(Some(port));
    }
    if let Some(port) = optional_deploy_port(config, "ssh_port in wr-deploy.toml")? {
        return Ok(Some(port));
    }
    match environment {
        SshPortEnvironment::NotPresent => Ok(None),
        SshPortEnvironment::Value(value) => parse_deploy_port(&value, "WR_SSH_PORT").map(Some),
        SshPortEnvironment::NotUnicode => {
            anyhow::bail!("WR_SSH_PORT must contain a valid UTF-8 TCP port")
        }
    }
}

/// Resolve peer port from CLI > config > env > default (9443).
pub fn resolve_peer_port(cli: Option<u16>, config: Option<u16>) -> Result<DeployPort> {
    if let Some(port) = optional_deploy_port(cli, "--peer-port")? {
        return Ok(port);
    }
    if let Some(port) = optional_deploy_port(config, "peer_port in wr-deploy.toml")? {
        return Ok(port);
    }
    Ok(env_deploy_port("WR_PEER_PORT")?.unwrap_or(DeployPort::new(9443)?))
}

/// Resolve cert_dir from CLI > config > env > default ("./certs").
pub fn resolve_cert_dir(cli: &str, config: Option<String>) -> String {
    // If the CLI value differs from clap's default, the user explicitly passed it.
    if cli != "./certs" {
        return cli.to_string();
    }
    config
        .or_else(|| std::env::var("WR_CERT_DIR").ok().filter(|s| !s.is_empty()))
        .unwrap_or_else(|| cli.to_string())
}

/// Resolve no_otel flag from CLI > config > env > default (false).
pub fn resolve_no_otel(cli: bool, config: Option<bool>) -> bool {
    if cli {
        return true;
    }
    if let Some(v) = config {
        return v;
    }
    std::env::var("WR_NO_OTEL")
        .ok()
        .map(|s| s == "1" || s.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn removed_node_backend_config_keys_are_rejected() {
        assert!(toml::from_str::<DeployConfig>("format = \"docker\"").is_err());
        assert!(toml::from_str::<DeployConfig>("image_prefix = \"wr\"").is_err());
    }

    #[test]
    fn deployment_ports_reject_malformed_and_zero_values() {
        assert!(parse_deploy_port("not-a-port", "WR_PEER_PORT").is_err());
        assert!(parse_deploy_port("0", "WR_SSH_PORT").is_err());
        assert!(optional_deploy_port(Some(0), "--peer-port").is_err());
    }

    #[test]
    fn tenant_database_is_strict_and_keeps_platform_url_out_of_the_contract() {
        let parsed: DeployConfig = toml::from_str(
            r#"
db_url = "postgres://platform.example/jobs"
[tenant_database]
server_name = "postgres.internal"
host_addr = "10.0.0.15"
port = 5432
connect_timeout_secs = 10
"#,
        )
        .unwrap();
        let tenant =
            resolve_tenant_database(TenantDeployOverrides::default(), parsed.tenant_database)
                .unwrap()
                .unwrap();
        assert_eq!(tenant.server_name, "postgres.internal");
        assert_eq!(tenant.host_addr.as_deref(), Some("10.0.0.15"));
        assert_eq!(
            parsed.db_url.as_deref(),
            Some("postgres://platform.example/jobs")
        );
        for bad in ["postgres://internal", "user@internal", "*.internal", ""] {
            let value = TenantDeployConfig {
                server_name: bad.into(),
                host_addr: Some("10.0.0.1".into()),
                port: 5432,
                connect_timeout_secs: 10,
            };
            assert!(value.validate().is_err(), "accepted {bad:?}");
        }
        let public = TenantDeployConfig {
            server_name: "postgres.internal".into(),
            host_addr: Some("203.0.113.5".into()),
            port: 5432,
            connect_timeout_secs: 10,
        };
        assert!(public.validate().is_err());
    }

    #[test]
    fn tenant_database_cli_precedence_is_field_by_field() {
        let resolved = resolve_tenant_database(
            TenantDeployOverrides {
                server_name: Some("override.internal".into()),
                port: Some(6432),
                ..TenantDeployOverrides::default()
            },
            Some(TenantDeployConfigSource {
                server_name: "configured.internal".into(),
                host_addr: Some("10.2.0.8".into()),
                port: Some(5432),
                connect_timeout_secs: Some(12),
            }),
        )
        .unwrap()
        .unwrap();
        assert_eq!(resolved.server_name, "override.internal");
        assert_eq!(resolved.host_addr.as_deref(), Some("10.2.0.8"));
        assert_eq!(resolved.port, 6432);
        assert_eq!(resolved.connect_timeout_secs, 12);
    }

    #[test]
    fn deployment_ports_accept_nonzero_values() {
        assert_eq!(
            parse_deploy_port("9443", "WR_PEER_PORT").unwrap().get(),
            9443
        );
        assert_eq!(
            optional_deploy_port(Some(22), "--ssh-port")
                .unwrap()
                .unwrap()
                .get(),
            22
        );
    }

    #[cfg(unix)]
    #[test]
    fn non_utf8_ssh_port_environment_is_rejected_instead_of_defaulted() {
        use std::os::unix::ffi::OsStringExt;

        let environment = SshPortEnvironment::from_var(Err(std::env::VarError::NotUnicode(
            std::ffi::OsString::from_vec(vec![0xff]),
        )));
        let error = resolve_ssh_port_from(None, None, environment).unwrap_err();
        assert!(error
            .to_string()
            .contains("WR_SSH_PORT must contain a valid UTF-8 TCP port"));
    }
}
