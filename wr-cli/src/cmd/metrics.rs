use anyhow::Result;
use clap::{Args, Subcommand};
use std::collections::HashMap;
use tabled::builder::Builder;
use wr_common::wruntime::GetMetricsSummaryRequest;

use crate::{client, display};

#[derive(Args)]
pub struct MetricsArgs {
    #[command(subcommand)]
    pub command: MetricsCommand,
}

#[derive(Subcommand)]
pub enum MetricsCommand {
    /// Show aggregated request metrics across all source→destination pairs
    Summary,
}

pub async fn run(args: MetricsArgs, manager: &str) -> Result<()> {
    match args.command {
        MetricsCommand::Summary => summary(manager).await,
    }
}

async fn summary(manager: &str) -> Result<()> {
    let mut client = client::connect(manager).await?;
    let resp = client
        .get_metrics_summary(GetMetricsSummaryRequest {})
        .await?
        .into_inner();

    if resp.metrics.is_empty() {
        println!("No metrics recorded yet.");
        return Ok(());
    }

    // Group by (source, destination)
    let mut groups: HashMap<(String, String), Vec<(u64, u32, bool)>> = HashMap::new();
    for m in &resp.metrics {
        groups
            .entry((m.source.clone(), m.destination.clone()))
            .or_default()
            .push((m.duration_ms, m.status, !m.error.is_empty()));
    }

    let mut rows: Vec<_> = groups.into_iter().collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));

    let mut builder = Builder::new();
    builder.push_record(["Source", "Destination", "Requests", "Avg ms", "P99 ms", "Errors"]);

    for ((source, destination), entries) in &rows {
        let count = entries.len();
        let errors: usize = entries.iter().filter(|(_, _, e)| *e).count();
        let avg_ms = entries.iter().map(|(d, _, _)| *d).sum::<u64>() / count as u64;

        let mut durations: Vec<u64> = entries.iter().map(|(d, _, _)| *d).collect();
        durations.sort_unstable();
        let p99_idx = ((count as f64 * 0.99) as usize).min(count - 1);
        let p99_ms = durations[p99_idx];

        builder.push_record([
            source.as_str(),
            destination.as_str(),
            &count.to_string(),
            &avg_ms.to_string(),
            &p99_ms.to_string(),
            &errors.to_string(),
        ]);
    }

    display::print_table(builder);
    Ok(())
}
