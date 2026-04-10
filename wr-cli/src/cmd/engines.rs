use anyhow::{bail, Result};
use clap::{Args, Subcommand};
use tabled::builder::Builder;
use tabled::grid::config::HorizontalLine;
use tabled::settings::{Style, Theme};
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
    let resp = client
        .list_engines(ListEnginesRequest {})
        .await?
        .into_inner();

    if resp.engines.is_empty() {
        println!("No engines registered.");
        return Ok(());
    }

    let mut builder = Builder::new();
    builder.push_record(["Engine", "Address", "Module"]);
    let mut separator_rows = Vec::new();
    let mut row_idx = 1; // row 0 is header
    for (i, engine) in resp.engines.iter().enumerate() {
        if i > 0 {
            separator_rows.push(row_idx);
        }
        if engine.modules.is_empty() {
            builder.push_record([engine.engine_id.as_str(), engine.address.as_str(), ""]);
            row_idx += 1;
        } else {
            let mut modules: Vec<_> = engine.modules.iter().collect();
            modules.sort_by(|a, b| {
                (&a.namespace, &a.name, &a.version).cmp(&(&b.namespace, &b.name, &b.version))
            });
            for (j, module) in modules.iter().enumerate() {
                let module_str =
                    format!("{}.{} v{}", module.namespace, module.name, module.version);
                if j == 0 {
                    builder.push_record([
                        engine.engine_id.as_str(),
                        engine.address.as_str(),
                        &module_str,
                    ]);
                } else {
                    builder.push_record(["", "", &module_str]);
                }
                row_idx += 1;
            }
        }
    }

    let mut table = builder.build();
    let mut theme = Theme::from_style(Style::rounded());
    for row in separator_rows {
        theme.insert_horizontal_line(
            row,
            HorizontalLine::new(Some('─'), Some('┼'), Some('├'), Some('┤')),
        );
    }
    table.with(theme);
    println!("{table}");
    Ok(())
}

async fn get(manager: &str, id: &str) -> Result<()> {
    let mut client = client::connect(manager).await?;
    let resp = client
        .list_engines(ListEnginesRequest {})
        .await?
        .into_inner();

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

    let mut modules: Vec<_> = engine.modules.iter().collect();
    modules.sort_by(|a, b| {
        (&a.namespace, &a.name, &a.version).cmp(&(&b.namespace, &b.name, &b.version))
    });
    let mut builder = Builder::new();
    builder.push_record(["Namespace", "Module", "Version"]);
    for module in &modules {
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
