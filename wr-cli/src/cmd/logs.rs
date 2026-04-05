use anyhow::Result;
use clap::{Args, Subcommand};

use super::deploy_config::DeployFormat;
use super::helpers;

#[derive(Args)]
pub struct LogsArgs {
    #[command(subcommand)]
    pub command: LogsCommand,
}

#[derive(Subcommand)]
pub enum LogsCommand {
    /// View logs from services on a remote node
    Node {
        /// Remote host in user@host format
        remote: String,
        /// Deployment format (systemd or docker)
        #[arg(long)]
        format: DeployFormat,
        /// SSH private key path
        #[arg(long)]
        ssh_key: Option<String>,
        /// SSH port (omit to use SSH config default)
        #[arg(long)]
        ssh_port: Option<u16>,
        /// Base directory for installed files
        #[arg(long, default_value = "/opt/wruntime")]
        workdir: String,
        /// Filter to a specific service (e.g. wr-proxy, wr-engine-inventory)
        #[arg(long)]
        service: Option<String>,
        /// Number of recent log lines to show
        #[arg(long, default_value = "100")]
        tail: u32,
        /// Lookback window, e.g. "5m", "1h" (systemd only)
        #[arg(long, default_value = "5m")]
        since: String,
        /// Follow log output (tail -f)
        #[arg(long)]
        follow: bool,
    },
}

pub async fn run(args: LogsArgs) -> Result<()> {
    match args.command {
        LogsCommand::Node {
            remote,
            format,
            ssh_key,
            ssh_port,
            workdir,
            service,
            tail,
            since,
            follow,
        } => {
            let ssh_base = helpers::build_ssh_args(&remote, ssh_key.as_deref(), ssh_port);
            let cmd = match format {
                DeployFormat::Systemd => {
                    build_journalctl_command(service.as_deref(), tail, &since, follow)
                }
                DeployFormat::Docker => {
                    build_docker_logs_command(&workdir, service.as_deref(), tail, follow)
                }
            };
            helpers::run_ssh_streaming(&ssh_base, &cmd)
        }
    }
}

/// Convert shorthand durations like "5m", "1h", "30s" to journalctl-compatible
/// relative timestamps like "5 minutes ago", "1 hours ago", "30 seconds ago".
/// Passes through values that don't match the shorthand pattern (e.g. absolute timestamps).
fn normalize_since(since: &str) -> String {
    let since = since.trim();
    if let Some(num) = since.strip_suffix('s') {
        if let Ok(n) = num.parse::<u64>() {
            return format!("{n} seconds ago");
        }
    }
    if let Some(num) = since.strip_suffix('m') {
        if let Ok(n) = num.parse::<u64>() {
            return format!("{n} minutes ago");
        }
    }
    if let Some(num) = since.strip_suffix('h') {
        if let Ok(n) = num.parse::<u64>() {
            return format!("{n} hours ago");
        }
    }
    if let Some(num) = since.strip_suffix('d') {
        if let Ok(n) = num.parse::<u64>() {
            return format!("{n} days ago");
        }
    }
    since.to_string()
}

pub fn build_journalctl_command(
    service: Option<&str>,
    tail: u32,
    since: &str,
    follow: bool,
) -> String {
    let since_val = normalize_since(since);
    let mut cmd = match service {
        Some(s) => {
            format!("sudo journalctl -q -u {s} --since '{since_val}' -n {tail} --no-pager")
        }
        None => {
            // Discover wr-* units dynamically to avoid journalctl glob issues
            format!(
                r#"units=$(systemctl list-units --plain --no-legend 'wr-*' | awk '{{printf "-u %s ", $1}}'); sudo journalctl -q $units --since '{since_val}' -n {tail} --no-pager"#
            )
        }
    };
    if follow {
        cmd.push_str(" -f");
    }
    cmd
}

pub fn build_docker_logs_command(
    workdir: &str,
    service: Option<&str>,
    tail: u32,
    follow: bool,
) -> String {
    let compose = format!("{workdir}/wr-node/docker/docker-compose.yml");
    let mut cmd = format!("docker compose -f {compose} logs --tail {tail}");
    if follow {
        cmd.push_str(" -f");
    }
    if let Some(s) = service {
        cmd.push(' ');
        cmd.push_str(s);
    }
    cmd
}
