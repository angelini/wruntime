use anyhow::Result;
use clap::{Args, Subcommand};
use tabled::builder::Builder;

use crate::{client, display};

#[derive(Args)]
pub struct ManagersArgs {
    #[command(subcommand)]
    pub command: ManagersCommand,
}

#[derive(Subcommand)]
pub enum ManagersCommand {
    /// List all active managers in the cluster
    List,
}

pub async fn run(args: ManagersArgs, manager: &str) -> Result<()> {
    match args.command {
        ManagersCommand::List => list(manager).await,
    }
}

async fn list(manager: &str) -> Result<()> {
    let managers = client::list_managers(manager).await?;

    if managers.is_empty() {
        println!("No managers found.");
        return Ok(());
    }

    let mut builder = Builder::new();
    builder.push_record(["ID", "gRPC Address"]);
    for (id, addr) in &managers {
        builder.push_record([id.as_str(), addr.as_str()]);
    }
    display::print_table(builder);
    Ok(())
}
