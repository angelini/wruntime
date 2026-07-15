use anyhow::{Context, Result};
use clap::ValueEnum;
use serde::Deserialize;

use super::helpers::DeployPort;

/// Shared deployment format used by both manager and node deploy commands.
#[derive(Clone, ValueEnum, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeployFormat {
    Systemd,
    Docker,
}

/// Optional deploy configuration file (`wr-deploy.toml`).
///
/// All fields are optional — values are merged with CLI flags and env vars
/// using the precedence: CLI flag > config file > env var > default.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct DeployConfig {
    /// Deployment format: "systemd" or "docker"
    pub format: Option<DeployFormat>,
    /// Postgres database URL
    pub db_url: Option<String>,
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
    /// Docker image name prefix
    pub image_prefix: Option<String>,
    /// Source proxy config file for node bundle generation
    pub proxy_config: Option<String>,
    /// Gossip seed node addresses (manager deploy only)
    #[allow(dead_code)]
    pub seed_nodes: Option<Vec<String>>,
    /// Disable OpenTelemetry export in generated service units
    pub no_otel: Option<bool>,
    /// Path to schedules TOML file for post-deploy apply
    pub schedules_path: Option<String>,
    /// Local directory containing CA + node certificates (from `wr cert`)
    pub cert_dir: Option<String>,
    /// mTLS peer listener port (default: 9443)
    pub peer_port: Option<u16>,
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
    cli.or(config)
        .or_else(|| std::env::var(env_key).ok().filter(|s| !s.is_empty()))
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

/// Resolve deploy format from CLI > config > env > default (Systemd).
pub fn resolve_format(cli: Option<DeployFormat>, config: Option<DeployFormat>) -> DeployFormat {
    cli.or(config)
        .or_else(|| {
            std::env::var("WR_FORMAT")
                .ok()
                .and_then(|s| match s.to_lowercase().as_str() {
                    "systemd" => Some(DeployFormat::Systemd),
                    "docker" => Some(DeployFormat::Docker),
                    _ => None,
                })
        })
        .unwrap_or(DeployFormat::Systemd)
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

/// Resolve SSH port from CLI > config > env. Returns None to use SSH default.
pub fn resolve_ssh_port(cli: Option<u16>, config: Option<u16>) -> Result<Option<DeployPort>> {
    if let Some(port) = optional_deploy_port(cli, "--ssh-port")? {
        return Ok(Some(port));
    }
    if let Some(port) = optional_deploy_port(config, "ssh_port in wr-deploy.toml")? {
        return Ok(Some(port));
    }
    env_deploy_port("WR_SSH_PORT")
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
    fn deployment_ports_reject_malformed_and_zero_values() {
        assert!(parse_deploy_port("not-a-port", "WR_PEER_PORT").is_err());
        assert!(parse_deploy_port("0", "WR_SSH_PORT").is_err());
        assert!(optional_deploy_port(Some(0), "--peer-port").is_err());
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
}
