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

    /// CA certificate for verifying the manager's TLS cert
    #[arg(
        long,
        env = "WR_CA_CERT",
        global = true,
        default_value = "certs/runtime-server-root/ca.crt"
    )]
    ca_cert: String,

    /// Client certificate for mTLS authentication to the manager
    #[arg(
        long,
        env = "WR_CLIENT_CERT",
        global = true,
        default_value = "certs/runtime-human-client/leaf.pem"
    )]
    client_cert: String,

    /// Client private key for mTLS authentication to the manager
    #[arg(
        long,
        env = "WR_CLIENT_KEY",
        global = true,
        default_value = "certs/runtime-human-client/key.pem"
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
    /// Remote node deployment and lifecycle (bundle, reconcile deployment, rollback, agent)
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

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    let cli = Cli::parse();

    cmd::helpers::set_verbose(cli.verbose);

    client::set_tls_config(build_tls_config(&cli));
    let job_connection = matches!(&cli.command, Commands::Jobs(_))
        .then(|| {
            Ok::<_, anyhow::Error>((
                require_manager(&cli.manager)?.to_string(),
                build_tls_config(&cli),
            ))
        })
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
            let (manager, tls) = job_connection
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
    fn lifecycle_cli_retains_deploy_rollback_and_restart_only() {
        assert!(Cli::try_parse_from([
            "wr-cli",
            "node",
            "deploy",
            "bundle.tar.gz",
            "operator@example",
            "--node-id",
            "node-a",
        ])
        .is_ok());
        assert!(Cli::try_parse_from([
            "wr-cli",
            "node",
            "rollback",
            "operator@example",
            "--node-id",
            "node-a",
        ])
        .is_ok());
        assert!(Cli::try_parse_from([
            "wr-cli",
            "engines",
            "restart",
            "--node-id",
            "node-a",
            "--slot",
            "blue",
            "--allow-downtime",
        ])
        .is_ok());

        for removed in ["upgrade", "scale"] {
            assert!(Cli::try_parse_from([
                "wr-cli",
                "node",
                removed,
                "bundle.tar.gz",
                "operator@example",
                "--node-id",
                "node-a",
            ])
            .is_err());
        }
        assert!(Cli::try_parse_from([
            "wr-cli",
            "engines",
            "drain",
            "--node-id",
            "node-a",
            "--slot",
            "blue",
        ])
        .is_err());
        for removed_flag in ["--canary", "--pause-after-canary"] {
            let mut args = vec![
                "wr-cli",
                "node",
                "deploy",
                "bundle.tar.gz",
                "operator@example",
                "--node-id",
                "node-a",
                removed_flag,
            ];
            if removed_flag == "--canary" {
                args.push("blue");
            }
            assert!(Cli::try_parse_from(args).is_err());
        }
    }

    #[test]
    fn obsolete_job_admin_listener_flags_are_rejected() {
        assert!(Cli::try_parse_from([
            "wr-cli",
            "--job-admin-manager",
            "https://jobs-manager:9020",
            "jobs",
            "queues",
        ])
        .is_err());
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
    fn jobs_uses_the_named_manager_identity() {
        let cli = Cli::try_parse_from([
            "wr-cli",
            "--manager",
            "https://manager:9000",
            "--ca-cert",
            "server-root.crt",
            "--client-cert",
            "operator.crt",
            "--client-key",
            "operator.key",
            "jobs",
            "queues",
        ])
        .unwrap();
        assert_eq!(
            require_manager(&cli.manager).unwrap(),
            "https://manager:9000"
        );
        let tls = build_tls_config(&cli);
        assert_eq!(tls.ca_cert_path, "server-root.crt");
        assert_eq!(tls.cert_path, "operator.crt");
    }
}
