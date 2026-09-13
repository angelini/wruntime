use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, Subcommand, ValueEnum};

#[derive(Args)]
pub struct PostgresArgs {
    #[command(subcommand)]
    pub command: PostgresCommand,
}

#[derive(Subcommand)]
pub enum PostgresCommand {
    /// Converge private namespace databases and PostgreSQL client-certificate mappings.
    Provision(ProvisionArgs),
    /// Execute an authenticated immutable migration bundle offline.
    Migrate(MigrateArgs),
    /// Approve one exact retry of a blocked migration attempt.
    ApproveRetry(ApproveRetryArgs),
    /// Serially provision the declared databases and then run their migration bundle.
    ProvisionMigrate(ProvisionMigrateArgs),
}

#[derive(Args)]
pub struct ProvisionArgs {
    #[arg(long)]
    pub manifest: PathBuf,
    #[arg(long)]
    pub admin_url_file: PathBuf,
    #[arg(long)]
    pub pg_ident_target: PathBuf,
    #[arg(long)]
    pub dry_run: bool,
    #[arg(long, value_enum, default_value_t = ProvisionOutput::Human)]
    pub output: ProvisionOutput,
}

#[derive(Args)]
pub struct MigrateArgs {
    /// PostgreSQL provisioning manifest defining the namespace topology.
    #[arg(long)]
    pub manifest: PathBuf,
    /// Immutable migration manifest whose hashes are authenticated by its deployment digest.
    #[arg(long)]
    pub bundle_manifest: PathBuf,
    /// Root containing `<namespace>/<module>/<canonical filename>` bundle entries.
    #[arg(long)]
    pub bundle_root: PathBuf,
    #[arg(long)]
    pub admin_url_file: PathBuf,
    /// Verified node bundle used for deployment-bound receipt emission.
    #[arg(long, requires_all = ["deployment_reservation", "receipt_out"])]
    pub node_bundle: Option<PathBuf>,
    /// Reservation emitted by `node reserve-deployment`.
    #[arg(long, requires_all = ["node_bundle", "receipt_out"])]
    pub deployment_reservation: Option<PathBuf>,
    /// Atomic non-secret tenant state receipt output.
    #[arg(long, requires_all = ["node_bundle", "deployment_reservation"])]
    pub receipt_out: Option<PathBuf>,
}

#[derive(Args)]
pub struct ApproveRetryArgs {
    #[arg(long)]
    pub manifest: PathBuf,
    #[arg(long)]
    pub admin_url_file: PathBuf,
    #[arg(long)]
    pub namespace: String,
    #[arg(long)]
    pub module: String,
    #[arg(long)]
    pub version: u64,
    #[arg(long)]
    pub attempt: i64,
    #[arg(long)]
    pub state: String,
    #[arg(long)]
    pub content_hash: String,
    #[arg(long)]
    pub operator: String,
    #[arg(long)]
    pub reason: String,
}

#[derive(Args)]
pub struct ProvisionMigrateArgs {
    #[arg(long)]
    pub manifest: PathBuf,
    #[arg(long)]
    pub bundle_manifest: PathBuf,
    #[arg(long)]
    pub bundle_root: PathBuf,
    #[arg(long)]
    pub admin_url_file: PathBuf,
    #[arg(long)]
    pub pg_ident_target: PathBuf,
    #[arg(long)]
    pub node_bundle: Option<PathBuf>,
    #[arg(long)]
    pub deployment_reservation: Option<PathBuf>,
    #[arg(long)]
    pub receipt_out: Option<PathBuf>,
    #[arg(long, value_enum, default_value_t = ProvisionOutput::Human)]
    pub output: ProvisionOutput,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum ProvisionOutput {
    Human,
    Json,
}

pub async fn run(args: PostgresArgs) -> Result<()> {
    match args.command {
        PostgresCommand::Provision(args) => crate::postgres::provision(args).await,
        PostgresCommand::Migrate(args) => crate::postgres::migrate(args).await,
        PostgresCommand::ApproveRetry(args) => crate::postgres::approve_retry(args).await,
        PostgresCommand::ProvisionMigrate(args) => crate::postgres::provision_migrate(args).await,
    }
}
