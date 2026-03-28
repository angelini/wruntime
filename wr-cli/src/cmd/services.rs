use anyhow::{bail, Result};
use clap::{Args, Subcommand};
use std::collections::HashMap;
use tabled::builder::Builder;
use wr_common::wruntime::GetRoutingTableRequest;

use crate::{client, display};

#[derive(Args)]
pub struct ServicesArgs {
    #[command(subcommand)]
    pub command: ServicesCommand,
}

#[derive(Subcommand)]
pub enum ServicesCommand {
    /// List all logical services derived from the routing table
    List,
    /// Show routing rules for a specific service (format: namespace.module)
    Get {
        /// Service in the form namespace.module (e.g. payments.order-service)
        service: String,
    },
}

pub async fn run(args: ServicesArgs, manager: &str) -> Result<()> {
    match args.command {
        ServicesCommand::List => list(manager).await,
        ServicesCommand::Get { service } => get(manager, &service).await,
    }
}

async fn list(manager: &str) -> Result<()> {
    let mut client = client::connect(manager).await?;
    let resp = client
        .get_routing_table(GetRoutingTableRequest {})
        .await?
        .into_inner();

    let rules = resp.table.map(|t| t.rules).unwrap_or_default();

    if rules.is_empty() {
        println!("No routing rules found.");
        return Ok(());
    }

    // Group by (namespace, module)
    let mut groups: HashMap<(String, String), (u32, u32)> = HashMap::new();
    for rule in &rules {
        let entry = groups
            .entry((
                rule.destination_namespace.clone(),
                rule.destination_module.clone(),
            ))
            .or_insert((0, 0));
        if rule.healthy {
            entry.0 += 1;
        } else {
            entry.1 += 1;
        }
    }

    let mut rows: Vec<_> = groups.into_iter().collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));

    let mut builder = Builder::new();
    builder.push_record(["Service", "Total", "Healthy", "Unhealthy"]);
    for ((ns, module), (healthy, unhealthy)) in &rows {
        let service = format!("{ns}.{module}");
        let total = healthy + unhealthy;
        builder.push_record([
            service.as_str(),
            &total.to_string(),
            &healthy.to_string(),
            &unhealthy.to_string(),
        ]);
    }
    display::print_table(builder);
    Ok(())
}

async fn get(manager: &str, service: &str) -> Result<()> {
    let (ns, module) = parse_service(service)?;

    let mut client = client::connect(manager).await?;
    let resp = client
        .get_routing_table(GetRoutingTableRequest {})
        .await?
        .into_inner();

    let rules = resp.table.map(|t| t.rules).unwrap_or_default();
    let matching: Vec<_> = rules
        .iter()
        .filter(|r| r.destination_namespace == ns && r.destination_module == module)
        .collect();

    if matching.is_empty() {
        bail!("No routing rules found for service '{}'", service);
    }

    let mut builder = Builder::new();
    builder.push_record(["Rule ID", "Engine ID", "Engine Address", "Version", "Healthy"]);
    for rule in &matching {
        builder.push_record([
            rule.rule_id.as_str(),
            rule.engine_id.as_str(),
            rule.engine_address.as_str(),
            rule.destination_version.as_str(),
            if rule.healthy { "yes" } else { "no" },
        ]);
    }
    display::print_table(builder);
    Ok(())
}

fn parse_service(service: &str) -> Result<(&str, &str)> {
    match service.split_once('.') {
        Some((ns, module)) => Ok((ns, module)),
        None => bail!(
            "Invalid service format '{}'. Expected namespace.module (e.g. payments.order-service)",
            service
        ),
    }
}
