use anyhow::{bail, Result};
use clap::{Args, Subcommand};
use tabled::builder::Builder;
use wr_common::wruntime::{DeregisterEngineRequest, ListEnginesRequest};

use crate::{client, display};

#[derive(Args)]
pub struct EnginesArgs {
    #[command(subcommand)]
    pub command: EnginesCommand,
}

#[derive(Subcommand)]
pub enum EnginesCommand {
    /// List all registered engines
    List,
    /// Show modules registered on a specific engine
    Get {
        /// Engine ID
        id: String,
    },
    /// Deregister an engine from the manager
    Remove {
        /// Engine ID
        id: String,
    },
}

pub async fn run(args: EnginesArgs, manager: &str) -> Result<()> {
    match args.command {
        EnginesCommand::List => list(manager).await,
        EnginesCommand::Get { id } => get(manager, &id).await,
        EnginesCommand::Remove { id } => remove(manager, &id).await,
    }
}

async fn list(manager: &str) -> Result<()> {
    let mut client = client::connect(manager).await?;
    let resp = client.list_engines(ListEnginesRequest {}).await?.into_inner();

    if resp.engines.is_empty() {
        println!("No engines registered.");
        return Ok(());
    }

    let mut builder = Builder::new();
    builder.push_record(["ID", "Address", "Modules"]);
    for engine in &resp.engines {
        builder.push_record([
            engine.engine_id.as_str(),
            engine.address.as_str(),
            &engine.modules.len().to_string(),
        ]);
    }
    display::print_table(builder);
    Ok(())
}

async fn get(manager: &str, id: &str) -> Result<()> {
    let mut client = client::connect(manager).await?;
    let resp = client.list_engines(ListEnginesRequest {}).await?.into_inner();

    let engine = resp.engines.iter().find(|e| e.engine_id == id);
    let Some(engine) = engine else {
        bail!("Engine '{}' not found", id);
    };

    println!("Engine: {}  Address: {}", engine.engine_id, engine.address);
    println!();

    if engine.modules.is_empty() {
        println!("No modules registered on this engine.");
        return Ok(());
    }

    let mut builder = Builder::new();
    builder.push_record(["Namespace", "Module", "Version"]);
    for module in &engine.modules {
        builder.push_record([
            module.namespace.as_str(),
            module.name.as_str(),
            module.version.as_str(),
        ]);
    }
    display::print_table(builder);
    Ok(())
}

async fn remove(manager: &str, id: &str) -> Result<()> {
    let mut client = client::connect(manager).await?;
    client
        .deregister_engine(DeregisterEngineRequest {
            engine_id: id.to_string(),
        })
        .await?;
    println!("Engine '{}' deregistered.", id);
    Ok(())
}
