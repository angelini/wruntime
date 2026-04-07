use anyhow::{Context, Result};
use clap::ValueEnum;
use serde::Deserialize;

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
    /// Guest database URL (node deploy only)
    pub guest_db_url: Option<String>,
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
    /// Gossip seed node addresses (manager deploy only)
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

/// Resolve SSH port from CLI > config > env. Returns None to use SSH default.
pub fn resolve_ssh_port(cli: Option<u16>, config: Option<u16>) -> Option<u16> {
    cli.or(config).or_else(|| {
        std::env::var("WR_SSH_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
    })
}

/// Resolve peer port from CLI > config > env > default (9443).
pub fn resolve_peer_port(cli: Option<u16>, config: Option<u16>) -> u16 {
    cli.or(config)
        .or_else(|| {
            std::env::var("WR_PEER_PORT")
                .ok()
                .and_then(|s| s.parse().ok())
        })
        .unwrap_or(9443)
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
