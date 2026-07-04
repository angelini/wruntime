use anyhow::{bail, Result};
use clap::{Parser, Subcommand};
use wr_common::node::TlsConfig;

mod client;
mod cmd;
mod display;

#[derive(Parser)]
#[command(name = "wr-cli", about = "wruntime deployment management CLI")]
struct Cli {
    /// Manager gRPC address (required for most commands; not needed for node init/bundle/status)
    #[arg(long, env = "WR_MANAGER", global = true)]
    manager: Option<String>,

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
    /// Manage scheduled jobs
    Schedules(cmd::schedules::SchedulesArgs),
    /// Manage namespace-scoped secrets
    Secrets(cmd::secrets::SecretsArgs),
    /// Remote node deployment (init, bundle, deploy, status)
    Node(cmd::node::NodeArgs),
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

    match cli.command {
        Commands::Db(args) => cmd::db::run(args).await,
        Commands::Dev(args) => cmd::dev::run(args, cli.manager.as_deref()).await,
        Commands::Engines(args) => cmd::engines::run(args, require_manager(&cli.manager)?).await,
        Commands::Managers(args) => cmd::managers::run(args, cli.manager.as_deref()).await,
        Commands::Services(args) => cmd::services::run(args, require_manager(&cli.manager)?).await,
        Commands::Metrics(args) => cmd::metrics::run(args).await,
        Commands::Invoke(args) => cmd::invoke::run(args, require_manager(&cli.manager)?).await,
        Commands::Schedules(args) => {
            cmd::schedules::run(args, require_manager(&cli.manager)?).await
        }
        Commands::Secrets(args) => cmd::secrets::run(args, require_manager(&cli.manager)?).await,
        Commands::Node(args) => cmd::node::run(args, cli.manager.as_deref()).await,
        Commands::Logs(args) => cmd::logs::run(args).await,
        Commands::Cert(args) => cmd::cert::run(args),
    }
}
