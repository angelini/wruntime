use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use std::collections::HashMap;

use tabled::builder::Builder;

use crate::display;

#[derive(Args)]
pub struct MetricsArgs {
    #[command(subcommand)]
    pub command: MetricsCommand,
}

#[derive(Subcommand)]
pub enum MetricsCommand {
    /// Show aggregated request metrics from OpenTelemetry traces (Tempo)
    Summary {
        /// Tempo HTTP endpoint
        #[arg(long, default_value = "http://localhost:3200")]
        tempo: String,
        /// Lookback window, e.g. "1h", "30m", "6h"
        #[arg(long, default_value = "1h")]
        since: String,
    },
}

pub async fn run(args: MetricsArgs) -> Result<()> {
    match args.command {
        MetricsCommand::Summary { tempo, since } => summary(&tempo, &since).await,
    }
}

/// Parse a human-friendly duration string like "1h", "30m", "6h" into seconds.
fn parse_duration_secs(s: &str) -> Result<u64> {
    let s = s.trim();
    if let Some(h) = s.strip_suffix('h') {
        return Ok(h.parse::<u64>().context("invalid hours")? * 3600);
    }
    if let Some(m) = s.strip_suffix('m') {
        return Ok(m.parse::<u64>().context("invalid minutes")? * 60);
    }
    if let Some(secs) = s.strip_suffix('s') {
        return secs.parse::<u64>().context("invalid seconds");
    }
    anyhow::bail!("unsupported duration format: {s} (use e.g. 1h, 30m, 120s)");
}

/// Represents a single span result from Tempo's search API.
struct SpanRecord {
    source: String,
    destination: String,
    duration_ms: u64,
    is_error: bool,
}

/// Query Tempo's search API for proxy.request spans and aggregate metrics.
async fn summary(tempo: &str, since: &str) -> Result<()> {
    let lookback_secs = parse_duration_secs(since)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let start = now - lookback_secs;

    let client = reqwest::Client::new();
    let url = format!("{tempo}/api/search");

    let resp = client
        .get(&url)
        .query(&[
            ("q", r#"{name = "proxy.request"}"#),
            ("start", &start.to_string()),
            ("end", &now.to_string()),
        ])
        .send()
        .await
        .context("failed to query Tempo")?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("Tempo returned {status}: {body}");
    }

    let body: serde_json::Value = resp
        .json()
        .await
        .context("failed to parse Tempo response")?;

    let spans = parse_tempo_search_response(&body)?;

    if spans.is_empty() {
        println!("No proxy.request traces found in the last {since}.");
        return Ok(());
    }

    // Group by (source, destination)
    let mut groups: HashMap<(String, String), Vec<&SpanRecord>> = HashMap::new();
    for span in &spans {
        groups
            .entry((span.source.clone(), span.destination.clone()))
            .or_default()
            .push(span);
    }

    let mut rows: Vec<_> = groups.into_iter().collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));

    let mut builder = Builder::new();
    builder.push_record([
        "Source",
        "Destination",
        "Requests",
        "Avg ms",
        "P99 ms",
        "Errors",
    ]);

    for ((source, destination), entries) in &rows {
        let count = entries.len();
        let errors: usize = entries.iter().filter(|s| s.is_error).count();
        let avg_ms = entries.iter().map(|s| s.duration_ms).sum::<u64>() / count as u64;

        let mut durations: Vec<u64> = entries.iter().map(|s| s.duration_ms).collect();
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

/// Parse the Tempo `/api/search` JSON response into span records.
///
/// Tempo's search response shape:
/// ```json
/// {
///   "traces": [
///     {
///       "traceID": "...",
///       "spanSets": [
///         {
///           "spans": [
///             {
///               "durationNanos": "123456789",
///               "attributes": [
///                 { "key": "wr.source", "value": { "stringValue": "..." } },
///                 { "key": "wr.destination", "value": { "stringValue": "..." } },
///                 { "key": "http.response.status_code", "value": { "intValue": "200" } }
///               ]
///             }
///           ]
///         }
///       ]
///     }
///   ]
/// }
/// ```
fn parse_tempo_search_response(body: &serde_json::Value) -> Result<Vec<SpanRecord>> {
    let mut records = Vec::new();

    let traces = body["traces"].as_array().unwrap_or(&Vec::new()).clone();

    for trace in &traces {
        // Tempo may return rootSpan-level duration or spanSets depending on the query.
        // When using TraceQL, results come in spanSets.
        if let Some(span_sets) = trace["spanSets"].as_array() {
            for span_set in span_sets {
                if let Some(spans) = span_set["spans"].as_array() {
                    for span in spans {
                        if let Some(record) = parse_span(span) {
                            records.push(record);
                        }
                    }
                }
            }
        }

        // Also handle the top-level trace summary (rootServiceName / rootTraceName)
        // for simpler search responses without spanSets.
        if trace["spanSets"].is_null() {
            if let Some(record) = parse_trace_summary(trace) {
                records.push(record);
            }
        }
    }

    Ok(records)
}

/// Parse a span from Tempo's spanSets response.
fn parse_span(span: &serde_json::Value) -> Option<SpanRecord> {
    let duration_nanos = span["durationNanos"]
        .as_str()
        .and_then(|s| s.parse::<u64>().ok())
        .or_else(|| span["durationNanos"].as_u64())?;
    let duration_ms = duration_nanos / 1_000_000;

    let attrs = span["attributes"].as_array()?;
    let mut source = String::from("unknown");
    let mut destination = String::from("unknown");
    let mut status_code: u16 = 0;

    for attr in attrs {
        let key = attr["key"].as_str().unwrap_or_default();
        match key {
            "wr.source" => {
                source = attr_string_value(attr).unwrap_or_else(|| "unknown".into());
            }
            "wr.destination" => {
                destination = attr_string_value(attr).unwrap_or_else(|| "unknown".into());
            }
            "http.response.status_code" => {
                status_code = attr_int_value(attr).unwrap_or(0) as u16;
            }
            _ => {}
        }
    }

    Some(SpanRecord {
        source,
        destination,
        duration_ms,
        is_error: status_code >= 400,
    })
}

/// Parse a trace-level summary (for search responses without spanSets).
fn parse_trace_summary(trace: &serde_json::Value) -> Option<SpanRecord> {
    let duration_ms = trace["durationMs"]
        .as_u64()
        .or_else(|| trace["durationMs"].as_str().and_then(|s| s.parse().ok()))?;

    let root_name = trace["rootTraceName"].as_str().unwrap_or("unknown");
    let service = trace["rootServiceName"].as_str().unwrap_or("unknown");

    // The root trace name for proxy.request spans is formatted as "{method} {destination}"
    let destination = root_name
        .split_once(' ')
        .map(|(_, d)| d.to_string())
        .unwrap_or_else(|| root_name.to_string());

    Some(SpanRecord {
        source: service.to_string(),
        destination,
        duration_ms,
        is_error: false,
    })
}

fn attr_string_value(attr: &serde_json::Value) -> Option<String> {
    attr["value"]["stringValue"].as_str().map(|s| s.to_string())
}

fn attr_int_value(attr: &serde_json::Value) -> Option<i64> {
    attr["value"]["intValue"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .or_else(|| attr["value"]["intValue"].as_i64())
}
