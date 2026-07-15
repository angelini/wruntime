use anyhow::Result;
use clap::{Args, Subcommand};
use tabled::builder::Builder;
use wr_common::wruntime::{DeleteScheduleRequest, ListSchedulesRequest, UpsertScheduleRequest};

use crate::{client, display};

#[derive(Args)]
pub struct SchedulesArgs {
    #[command(subcommand)]
    pub command: SchedulesCommand,
}

#[derive(Subcommand)]
pub enum SchedulesCommand {
    /// Apply schedules from a TOML file (upserts each entry)
    Apply {
        /// Path to schedules TOML file
        #[arg(long)]
        file: String,
    },
    /// List schedules
    List {
        /// Filter by worker namespace
        #[arg(long)]
        namespace: Option<String>,
    },
    /// Delete a schedule
    Delete {
        /// Worker namespace
        #[arg(long)]
        namespace: String,
        /// Worker name
        #[arg(long)]
        name: String,
        /// Worker version
        #[arg(long)]
        version: String,
        /// Job type
        #[arg(long)]
        job_type: String,
    },
}

pub async fn run(args: SchedulesArgs, manager: &str) -> Result<()> {
    match args.command {
        SchedulesCommand::Apply { file } => apply(manager, &file).await,
        SchedulesCommand::List { namespace } => list(manager, namespace.as_deref()).await,
        SchedulesCommand::Delete {
            namespace,
            name,
            version,
            job_type,
        } => delete(manager, &namespace, &name, &version, &job_type).await,
    }
}

// ── Apply ────────────────────────────────────────────────────────────────────

#[derive(serde::Deserialize)]
pub struct SchedulesFile {
    pub schedule: Vec<ScheduleEntry>,
}

#[derive(serde::Deserialize)]
pub struct ScheduleEntry {
    pub worker_namespace: String,
    pub worker_name: String,
    pub worker_version: String,
    pub job_type: String,
    pub interval_secs: u32,
    #[serde(default)]
    pub immediate: bool,
    #[serde(default)]
    pub payload: String,
    #[serde(default = "default_timeout")]
    pub timeout_secs: u32,
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
}

fn default_timeout() -> u32 {
    300
}
fn default_max_attempts() -> u32 {
    3
}

impl ScheduleEntry {
    fn validate(&self) -> Result<()> {
        wr_common::lifecycle::ScheduleIntervalSecs::new(self.interval_secs)?;
        wr_common::lifecycle::JobTimeoutSecs::new(self.timeout_secs)?;
        wr_common::lifecycle::MaxAttempts::new(self.max_attempts)?;
        Ok(())
    }
}

async fn apply(manager: &str, file_path: &str) -> Result<()> {
    let content = std::fs::read_to_string(file_path)?;
    let schedules_file: SchedulesFile = toml::from_str(&content)?;

    let mut client = client::connect(manager).await?;

    for entry in &schedules_file.schedule {
        entry.validate()?;
        let resp = client
            .upsert_schedule(UpsertScheduleRequest {
                worker_namespace: entry.worker_namespace.clone(),
                worker_name: entry.worker_name.clone(),
                worker_version: entry.worker_version.clone(),
                job_type: entry.job_type.clone(),
                interval_secs: entry.interval_secs,
                immediate: entry.immediate,
                payload: entry.payload.as_bytes().to_vec(),
                timeout_secs: entry.timeout_secs,
                max_attempts: entry.max_attempts,
            })
            .await?
            .into_inner();
        println!(
            "Schedule '{}/{}/{} {}' upserted (id: {})",
            entry.worker_namespace,
            entry.worker_name,
            entry.worker_version,
            entry.job_type,
            resp.schedule_id,
        );
    }

    println!("{} schedule(s) applied.", schedules_file.schedule.len());
    Ok(())
}

/// Apply schedules from parsed entries (used by node deploy integration).
pub async fn apply_entries(manager: &str, entries: &[ScheduleEntry]) -> Result<()> {
    let mut client = client::connect(manager).await?;

    for entry in entries {
        entry.validate()?;
        let resp = client
            .upsert_schedule(UpsertScheduleRequest {
                worker_namespace: entry.worker_namespace.clone(),
                worker_name: entry.worker_name.clone(),
                worker_version: entry.worker_version.clone(),
                job_type: entry.job_type.clone(),
                interval_secs: entry.interval_secs,
                immediate: entry.immediate,
                payload: entry.payload.as_bytes().to_vec(),
                timeout_secs: entry.timeout_secs,
                max_attempts: entry.max_attempts,
            })
            .await?
            .into_inner();
        println!(
            "  Schedule '{}/{}/{} {}' upserted (id: {})",
            entry.worker_namespace,
            entry.worker_name,
            entry.worker_version,
            entry.job_type,
            resp.schedule_id,
        );
    }
    Ok(())
}

// ── List ─────────────────────────────────────────────────────────────────────

fn format_timestamp(timestamp: &prost_types::Timestamp) -> String {
    let nanos = i128::from(timestamp.seconds) * 1_000_000_000 + i128::from(timestamp.nanos);
    match time::OffsetDateTime::from_unix_timestamp_nanos(nanos) {
        Ok(value) => match value.format(&time::format_description::well_known::Rfc3339) {
            Ok(formatted) => formatted,
            Err(_) => "invalid timestamp".to_string(),
        },
        Err(_) => "invalid timestamp".to_string(),
    }
}

async fn list(manager: &str, namespace: Option<&str>) -> Result<()> {
    let mut client = client::connect(manager).await?;
    let resp = client
        .list_schedules(ListSchedulesRequest {
            worker_namespace: namespace.unwrap_or_default().to_string(),
        })
        .await?
        .into_inner();

    if resp.schedules.is_empty() {
        println!("No schedules found.");
        return Ok(());
    }

    let mut builder = Builder::new();
    builder.push_record([
        "Namespace",
        "Name",
        "Version",
        "Job Type",
        "Interval",
        "Immediate",
        "Enabled",
        "Last Fired",
    ]);
    for s in &resp.schedules {
        let last_fired = s
            .last_fired_at
            .as_ref()
            .map(format_timestamp)
            .unwrap_or_else(|| "never".to_string());
        builder.push_record([
            s.worker_namespace.as_str(),
            s.worker_name.as_str(),
            s.worker_version.as_str(),
            s.job_type.as_str(),
            &format!("{}s", s.interval_secs),
            &s.immediate.to_string(),
            &s.enabled.to_string(),
            &last_fired,
        ]);
    }
    display::print_table(builder);
    Ok(())
}

// ── Delete ───────────────────────────────────────────────────────────────────

async fn delete(
    manager: &str,
    namespace: &str,
    name: &str,
    version: &str,
    job_type: &str,
) -> Result<()> {
    let mut client = client::connect(manager).await?;
    client
        .delete_schedule(DeleteScheduleRequest {
            worker_namespace: namespace.to_string(),
            worker_name: name.to_string(),
            worker_version: version.to_string(),
            job_type: job_type.to_string(),
        })
        .await?;
    println!(
        "Schedule '{}/{}/{} {}' deleted.",
        namespace, name, version, job_type
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedule_timestamp_is_rendered_as_rfc3339() {
        let timestamp = prost_types::Timestamp {
            seconds: 1_700_000_000,
            nanos: 123_456_789,
        };
        assert_eq!(
            format_timestamp(&timestamp),
            "2023-11-14T22:13:20.123456789Z"
        );
    }

    #[test]
    fn schedule_file_rejects_negative_and_zero_lifecycle_values() {
        let negative = r#"[[schedule]]
worker_namespace = "ns"
worker_name = "worker"
worker_version = "1.0.0"
job_type = "/Run"
interval_secs = -1
"#;
        assert!(toml::from_str::<SchedulesFile>(negative).is_err());

        let entry = ScheduleEntry {
            worker_namespace: "ns".into(),
            worker_name: "worker".into(),
            worker_version: "1.0.0".into(),
            job_type: "/Run".into(),
            interval_secs: 0,
            immediate: false,
            payload: String::new(),
            timeout_secs: 300,
            max_attempts: 3,
        };
        assert!(entry.validate().is_err());
    }
}
