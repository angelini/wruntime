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
    pub interval_secs: i32,
    #[serde(default)]
    pub immediate: bool,
    #[serde(default)]
    pub payload: String,
    #[serde(default = "default_timeout")]
    pub timeout_secs: i32,
    #[serde(default = "default_max_attempts")]
    pub max_attempts: i32,
}

fn default_timeout() -> i32 {
    300
}
fn default_max_attempts() -> i32 {
    3
}

async fn apply(manager: &str, file_path: &str) -> Result<()> {
    let content = std::fs::read_to_string(file_path)?;
    let schedules_file: SchedulesFile = toml::from_str(&content)?;

    let mut client = client::connect(manager).await?;

    for entry in &schedules_file.schedule {
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
        builder.push_record([
            s.worker_namespace.as_str(),
            s.worker_name.as_str(),
            s.worker_version.as_str(),
            s.job_type.as_str(),
            &format!("{}s", s.interval_secs),
            &s.immediate.to_string(),
            &s.enabled.to_string(),
            if s.last_fired_at.is_empty() {
                "never"
            } else {
                &s.last_fired_at
            },
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
