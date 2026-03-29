use anyhow::Result;
use clap::{Parser, Subcommand};

mod client;
mod cmd;
mod display;

#[derive(Parser)]
#[command(name = "wr-cli", about = "wruntime deployment management CLI")]
struct Cli {
    /// Manager gRPC address
    #[arg(
        long,
        env = "WR_MANAGER",
        default_value = "http://127.0.0.1:9000",
        global = true
    )]
    manager: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Manage wruntime engines
    Engines(cmd::engines::EnginesArgs),
    /// View logical services derived from the routing table
    Services(cmd::services::ServicesArgs),
    /// View aggregated request metrics
    Metrics(cmd::metrics::MetricsArgs),
    /// Send an HTTP request through the proxy to a module
    Invoke(cmd::invoke::InvokeArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Engines(args) => cmd::engines::run(args, &cli.manager).await,
        Commands::Services(args) => cmd::services::run(args, &cli.manager).await,
        Commands::Metrics(args) => cmd::metrics::run(args, &cli.manager).await,
        Commands::Invoke(args) => cmd::invoke::run(args).await,
    }
}
