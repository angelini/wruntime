use anyhow::Result;
use clap::{Parser, Subcommand};

mod client;
mod cmd;
mod display;

#[derive(Parser)]
#[command(name = "wr-cli", about = "wruntime deployment management CLI")]
struct Cli {
    /// Manager gRPC address (direct override — bypasses discovery)
    #[arg(long, env = "WR_MANAGER", global = true)]
    manager: Option<String>,

    /// Database URL for manager discovery via wr_managers table
    #[arg(long, env = "WRT_DATABASE_URL", global = true)]
    database_url: Option<String>,

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
    /// View logical services derived from the routing table
    Services(cmd::services::ServicesArgs),
    /// View aggregated request metrics
    Metrics(cmd::metrics::MetricsArgs),
    /// Send an HTTP request through the proxy to a module
    Invoke(cmd::invoke::InvokeArgs),
    /// Manage namespace-scoped secrets
    Secrets(cmd::secrets::SecretsArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Resolve manager address: --manager overrides, else discover via DB
    let manager = resolve_manager(&cli).await?;

    match cli.command {
        Commands::Db(args) => cmd::db::run(args).await,
        Commands::Dev(args) => cmd::dev::run(args, &manager).await,
        Commands::Engines(args) => cmd::engines::run(args, &manager).await,
        Commands::Services(args) => cmd::services::run(args, &manager).await,
        Commands::Metrics(args) => cmd::metrics::run(args).await,
        Commands::Invoke(args) => cmd::invoke::run(args, &manager).await,
        Commands::Secrets(args) => cmd::secrets::run(args, &manager).await,
    }
}

async fn resolve_manager(cli: &Cli) -> Result<String> {
    // Direct override
    if let Some(addr) = &cli.manager {
        return Ok(addr.clone());
    }

    // Discovery via Postgres
    if let Some(url) = &cli.database_url {
        let discovery = client::discover_manager(url).await?;
        return Ok(discovery);
    }

    // Fallback: try default manager address
    Ok("http://127.0.0.1:9000".to_string())
}
