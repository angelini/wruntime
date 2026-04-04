use anyhow::{bail, Result};
use clap::{Parser, Subcommand};

mod client;
mod cmd;
mod display;

#[derive(Parser)]
#[command(name = "wr-cli", about = "wruntime deployment management CLI")]
struct Cli {
    /// Manager gRPC address (required for most commands; not needed for node init/bundle/status)
    #[arg(long, env = "WR_MANAGER", global = true)]
    manager: Option<String>,

    #[command(subcommand)]
    command: Commands,
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
    /// Manage namespace-scoped secrets
    Secrets(cmd::secrets::SecretsArgs),
    /// Remote node deployment (init, bundle, deploy, status)
    Node(cmd::node::NodeArgs),
}

fn require_manager(manager: &Option<String>) -> Result<&str> {
    match manager {
        Some(m) => Ok(m.as_str()),
        None => bail!("--manager (or WR_MANAGER env var) is required for this command"),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Db(args) => cmd::db::run(args).await,
        Commands::Dev(args) => cmd::dev::run(args, cli.manager.as_deref()).await,
        Commands::Engines(args) => cmd::engines::run(args, require_manager(&cli.manager)?).await,
        Commands::Managers(args) => cmd::managers::run(args, cli.manager.as_deref()).await,
        Commands::Services(args) => cmd::services::run(args, require_manager(&cli.manager)?).await,
        Commands::Metrics(args) => cmd::metrics::run(args).await,
        Commands::Invoke(args) => cmd::invoke::run(args, require_manager(&cli.manager)?).await,
        Commands::Secrets(args) => cmd::secrets::run(args, require_manager(&cli.manager)?).await,
        Commands::Node(args) => cmd::node::run(args, cli.manager.as_deref()).await,
    }
}
