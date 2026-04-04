use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use serde::Deserialize;
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

// ---------------------------------------------------------------------------
// Typed Tempo search API response structures
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct TempoSearchResponse {
    #[serde(default)]
    traces: Vec<TempoTrace>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TempoTrace {
    #[serde(default)]
    span_sets: Option<Vec<TempoSpanSet>>,
    #[serde(default)]
    root_trace_name: Option<String>,
    #[serde(default)]
    root_service_name: Option<String>,
    #[serde(default)]
    duration_ms: Option<TempoNumber>,
}

#[derive(Deserialize)]
struct TempoSpanSet {
    #[serde(default)]
    spans: Vec<TempoSpan>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TempoSpan {
    #[serde(default)]
    duration_nanos: Option<TempoNumber>,
    #[serde(default)]
    attributes: Vec<TempoAttribute>,
}

#[derive(Deserialize)]
struct TempoAttribute {
    key: String,
    value: TempoAttrValue,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TempoAttrValue {
    #[serde(default)]
    string_value: Option<String>,
    #[serde(default)]
    int_value: Option<TempoNumber>,
}

/// Tempo encodes numeric values as either JSON numbers or strings.
/// This type handles both transparently.
#[derive(Deserialize)]
#[serde(untagged)]
enum TempoNumber {
    Int(u64),
    Str(String),
}

impl TempoNumber {
    fn as_u64(&self) -> Option<u64> {
        match self {
            TempoNumber::Int(n) => Some(*n),
            TempoNumber::Str(s) => s.parse().ok(),
        }
    }
}

// ---------------------------------------------------------------------------
// Internal span record (parsed from Tempo response)
// ---------------------------------------------------------------------------

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

    let body: TempoSearchResponse = resp
        .json()
        .await
        .context("failed to parse Tempo response")?;

    let spans = parse_tempo_search_response(&body);

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

/// Parse the typed Tempo search response into span records.
fn parse_tempo_search_response(body: &TempoSearchResponse) -> Vec<SpanRecord> {
    let mut records = Vec::new();

    for trace in &body.traces {
        // When using TraceQL, results come in spanSets.
        if let Some(span_sets) = &trace.span_sets {
            for span_set in span_sets {
                for span in &span_set.spans {
                    if let Some(record) = parse_span(span) {
                        records.push(record);
                    }
                }
            }
        } else {
            // Handle top-level trace summary (rootServiceName / rootTraceName)
            // for simpler search responses without spanSets.
            if let Some(record) = parse_trace_summary(trace) {
                records.push(record);
            }
        }
    }

    records
}

/// Parse a span from Tempo's spanSets response.
fn parse_span(span: &TempoSpan) -> Option<SpanRecord> {
    let duration_nanos = span.duration_nanos.as_ref()?.as_u64()?;
    let duration_ms = duration_nanos / 1_000_000;

    let mut source = String::from("unknown");
    let mut destination = String::from("unknown");
    let mut status_code: u16 = 0;

    for attr in &span.attributes {
        match attr.key.as_str() {
            "wr.source" => {
                if let Some(ref s) = attr.value.string_value {
                    source = s.clone();
                }
            }
            "wr.destination" => {
                if let Some(ref s) = attr.value.string_value {
                    destination = s.clone();
                }
            }
            "http.response.status_code" => {
                if let Some(ref n) = attr.value.int_value {
                    status_code = n.as_u64().unwrap_or(0) as u16;
                }
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
fn parse_trace_summary(trace: &TempoTrace) -> Option<SpanRecord> {
    let duration_ms = trace.duration_ms.as_ref()?.as_u64()?;

    let root_name = trace.root_trace_name.as_deref().unwrap_or("unknown");
    let service = trace.root_service_name.as_deref().unwrap_or("unknown");

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
