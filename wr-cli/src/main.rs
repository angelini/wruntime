use anyhow::{bail, Result};
use clap::{Parser, Subcommand};
use wr_cli::{client, cmd};
use wr_common::node::TlsConfig;

#[derive(Parser)]
#[command(name = "wr-cli", about = "wruntime deployment management CLI")]
struct Cli {
    /// Manager gRPC address (not needed for local bundle/inspect-bundle commands)
    #[arg(long, env = "WR_MANAGER", global = true)]
    manager: Option<String>,

    /// Dedicated manager job-administration gRPC address
    #[arg(long, env = "WR_JOB_ADMIN_MANAGER", global = true)]
    job_admin_manager: Option<String>,

    /// Operator-admin CA certificate for the dedicated manager listener
    #[arg(long, env = "WR_JOB_ADMIN_CA_CERT", global = true)]
    job_admin_ca_cert: Option<String>,

    /// Operator-admin client certificate for the dedicated manager listener
    #[arg(long, env = "WR_JOB_ADMIN_CLIENT_CERT", global = true)]
    job_admin_client_cert: Option<String>,

    /// Operator-admin private key for the dedicated manager listener
    #[arg(long, env = "WR_JOB_ADMIN_CLIENT_KEY", global = true)]
    job_admin_client_key: Option<String>,

    /// CA certificate for verifying the manager's TLS cert
    #[arg(
        long,
        env = "WR_CA_CERT",
        global = true,
        default_value = "certs/ca.crt"
    )]
    ca_cert: String,

    /// Client certificate for mTLS authentication to the manager
    #[arg(
        long,
        env = "WR_CLIENT_CERT",
        global = true,
        default_value = "certs/127.0.0.1.crt"
    )]
    client_cert: String,

    /// Client private key for mTLS authentication to the manager
    #[arg(
        long,
        env = "WR_CLIENT_KEY",
        global = true,
        default_value = "certs/127.0.0.1.key"
    )]
    client_key: String,

    /// Enable verbose debug output (connection attempts, SSH commands, retries)
    #[arg(long, short, global = true)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

fn build_tls_config(cli: &Cli) -> TlsConfig {
    TlsConfig {
        cert_path: cli.client_cert.clone(),
        key_path: cli.client_key.clone(),
        ca_cert_path: cli.ca_cert.clone(),
    }
}

#[derive(Subcommand)]
enum Commands {
    /// Database management (reset schemas, migrations)
    Db(cmd::db::DbArgs),
    /// View coherent cluster-wide status
    Cluster(cmd::cluster::ClusterArgs),
    /// Local development workflow (start infra, build, deploy)
    Dev(cmd::dev::DevArgs),
    /// Manage wruntime engines
    Engines(cmd::engines::EnginesArgs),
    /// Manage cluster managers
    Managers(cmd::managers::ManagersArgs),
    /// View logical services derived from the routing table
    Services(cmd::services::ServicesArgs),
    /// View aggregated request metrics
    Metrics(cmd::metrics::MetricsArgs),
    /// Send an HTTP request through the proxy to a module
    Invoke(cmd::invoke::InvokeArgs),
    /// Inspect and safely administer worker job queues
    Jobs(cmd::jobs::JobsArgs),
    /// Manage scheduled jobs
    Schedules(cmd::schedules::SchedulesArgs),
    /// Manage namespace-scoped secrets
    Secrets(cmd::secrets::SecretsArgs),
    /// Remote node deployment and lifecycle (bundle, deploy, upgrade, scale, rollback, agent)
    Node(cmd::node::NodeArgs),
    /// Inspect, resume, or cancel durable node operations
    Operations(cmd::operations::OperationsArgs),
    /// Query a trusted process lifecycle endpoint
    Lifecycle(cmd::lifecycle::LifecycleArgs),
    /// View logs from remote services
    Logs(cmd::logs::LogsArgs),
    /// Generate TLS certificates for mTLS
    Cert(cmd::cert::CertArgs),
}

fn require_manager(manager: &Option<String>) -> Result<&str> {
    match manager {
        Some(m) => Ok(m.as_str()),
        None => bail!("--manager (or WR_MANAGER env var) is required for this command"),
    }
}

fn require_job_admin_connection(cli: &Cli) -> Result<(String, TlsConfig)> {
    let manager = cli.job_admin_manager.clone().ok_or_else(|| {
        anyhow::anyhow!(
            "--job-admin-manager (or WR_JOB_ADMIN_MANAGER) is required for jobs commands"
        )
    })?;
    let ca_cert_path = cli.job_admin_ca_cert.clone().ok_or_else(|| {
        anyhow::anyhow!(
            "--job-admin-ca-cert (or WR_JOB_ADMIN_CA_CERT) is required for jobs commands"
        )
    })?;
    let cert_path = cli.job_admin_client_cert.clone().ok_or_else(|| {
        anyhow::anyhow!(
            "--job-admin-client-cert (or WR_JOB_ADMIN_CLIENT_CERT) is required for jobs commands"
        )
    })?;
    let key_path = cli.job_admin_client_key.clone().ok_or_else(|| {
        anyhow::anyhow!(
            "--job-admin-client-key (or WR_JOB_ADMIN_CLIENT_KEY) is required for jobs commands"
        )
    })?;
    Ok((
        manager,
        TlsConfig {
            cert_path,
            key_path,
            ca_cert_path,
        },
    ))
}

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    let cli = Cli::parse();

    cmd::helpers::set_verbose(cli.verbose);

    client::set_tls_config(build_tls_config(&cli));
    let job_admin_connection = matches!(&cli.command, Commands::Jobs(_))
        .then(|| require_job_admin_connection(&cli))
        .transpose()?;

    match cli.command {
        Commands::Db(args) => cmd::db::run(args).await,
        Commands::Cluster(args) => cmd::cluster::run(args, require_manager(&cli.manager)?).await,
        Commands::Dev(args) => cmd::dev::run(args, cli.manager.as_deref()).await,
        Commands::Engines(args) => cmd::engines::run(args, require_manager(&cli.manager)?).await,
        Commands::Managers(args) => cmd::managers::run(args, cli.manager.as_deref()).await,
        Commands::Services(args) => cmd::services::run(args, require_manager(&cli.manager)?).await,
        Commands::Metrics(args) => cmd::metrics::run(args).await,
        Commands::Invoke(args) => cmd::invoke::run(args, require_manager(&cli.manager)?).await,
        Commands::Jobs(args) => {
            let (manager, tls) = job_admin_connection
                .as_ref()
                .expect("jobs connection was validated before command dispatch");
            cmd::jobs::run(args, manager, tls).await
        }
        Commands::Schedules(args) => {
            cmd::schedules::run(args, require_manager(&cli.manager)?).await
        }
        Commands::Secrets(args) => cmd::secrets::run(args, require_manager(&cli.manager)?).await,
        Commands::Node(args) => cmd::node::run(args, cli.manager.as_deref()).await,
        Commands::Operations(args) => {
            cmd::operations::run(args, require_manager(&cli.manager)?).await
        }
        Commands::Lifecycle(args) => cmd::lifecycle::run(args).await,
        Commands::Logs(args) => cmd::logs::run(args).await,
        Commands::Cert(args) => cmd::cert::run(args),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jobs_never_falls_back_to_runtime_manager_credentials() {
        let cli = Cli::try_parse_from([
            "wr-cli",
            "--manager",
            "https://runtime-manager:9000",
            "--ca-cert",
            "runtime-ca.crt",
            "--client-cert",
            "runtime-client.crt",
            "--client-key",
            "runtime-client.key",
            "jobs",
            "queues",
        ])
        .unwrap();

        let error = require_job_admin_connection(&cli).unwrap_err();
        assert!(error.to_string().contains("--job-admin-manager"));
    }

    #[test]
    fn jobs_clap_enforces_page_bounds_and_accepts_explicit_exports() {
        assert!(Cli::try_parse_from([
            "wr-cli",
            "jobs",
            "list",
            "--queue",
            "primary-jobs",
            "--page-size",
            "201",
        ])
        .is_err());
        assert!(Cli::try_parse_from([
            "wr-cli",
            "jobs",
            "inspect",
            "--queue",
            "primary-jobs",
            "job-1",
            "--payload-out",
            "payload.bin",
            "--result-out",
            "result.bin",
            "--force",
            "--format",
            "json",
        ])
        .is_ok());
    }

    #[test]
    fn jobs_requires_and_uses_complete_dedicated_credentials() {
        let cli = Cli::try_parse_from([
            "wr-cli",
            "--job-admin-manager",
            "https://jobs-manager:9020",
            "--job-admin-ca-cert",
            "operator-ca.crt",
            "--job-admin-client-cert",
            "operator.crt",
            "--job-admin-client-key",
            "operator.key",
            "jobs",
            "queues",
        ])
        .unwrap();

        let (manager, tls) = require_job_admin_connection(&cli).unwrap();
        assert_eq!(manager, "https://jobs-manager:9020");
        assert_eq!(tls.ca_cert_path, "operator-ca.crt");
        assert_eq!(tls.cert_path, "operator.crt");
        assert_eq!(tls.key_path, "operator.key");
    }
}
