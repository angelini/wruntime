use std::io::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use clap::{Args, Subcommand, ValueEnum};
use prost_types::Timestamp;
use serde_json::json;
use tabled::builder::Builder;

use wr_common::identity::JobQueueId;
use wr_common::node::TlsConfig;
use wr_common::wruntime::{
    GetJobQueueSummaryRequest, GetJobRequest, JobDetail, JobFilter, JobQueueSummary, JobState,
    JobSummary, ListJobQueuesRequest, ListJobsRequest, RetryJobRequest,
};

use crate::{client, display};

#[derive(Args)]
pub struct JobsArgs {
    #[command(subcommand)]
    pub command: JobsCommand,
}

#[derive(Clone, Copy)]
struct JobAdminConnection<'a> {
    manager: &'a str,
    tls: &'a TlsConfig,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum OutputFormat {
    Table,
    Json,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum JobStateArg {
    Pending,
    Running,
    Complete,
    Dead,
}

impl JobStateArg {
    fn wire(self) -> i32 {
        match self {
            Self::Pending => JobState::Pending as i32,
            Self::Running => JobState::Running as i32,
            Self::Complete => JobState::Complete as i32,
            Self::Dead => JobState::Dead as i32,
        }
    }
}

#[derive(Args, Clone, Default)]
pub struct FilterArgs {
    #[arg(long)]
    worker_namespace: Option<String>,
    #[arg(long)]
    worker_name: Option<String>,
    #[arg(long)]
    worker_version: Option<String>,
    #[arg(long)]
    job_type: Option<String>,
    #[arg(long)]
    source_namespace: Option<String>,
    #[arg(long)]
    source_module: Option<String>,
    /// Inclusive RFC3339 creation-time lower bound.
    #[arg(long)]
    created_from: Option<String>,
    /// Exclusive RFC3339 creation-time upper bound.
    #[arg(long)]
    created_before: Option<String>,
}

#[derive(Subcommand)]
pub enum JobsCommand {
    /// Discover registered job queues.
    Queues {
        #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
        format: OutputFormat,
    },
    /// List one bounded page of job metadata.
    List {
        #[arg(long)]
        queue: String,
        #[command(flatten)]
        filter: FilterArgs,
        #[arg(long, value_enum)]
        status: Option<JobStateArg>,
        #[arg(long, default_value_t = 50, value_parser = clap::value_parser!(u32).range(1..=200))]
        page_size: u32,
        #[arg(long)]
        cursor: Option<String>,
        #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
        format: OutputFormat,
    },
    /// Summarize queue depth and lifecycle counts.
    Summary {
        #[arg(long)]
        queue: String,
        #[command(flatten)]
        filter: FilterArgs,
        #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
        format: OutputFormat,
    },
    /// Inspect one job. Payload/result bytes are never printed.
    Inspect {
        #[arg(long)]
        queue: String,
        job_id: String,
        #[arg(long)]
        payload_out: Option<PathBuf>,
        #[arg(long)]
        result_out: Option<PathBuf>,
        #[arg(long)]
        force: bool,
        #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
        format: OutputFormat,
    },
    /// Retry one dead job with a fresh attempt budget.
    Retry {
        #[arg(long)]
        queue: String,
        job_id: String,
        /// Explicitly confirm the state-changing retry.
        #[arg(long)]
        yes: bool,
        #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
        format: OutputFormat,
    },
}

pub async fn run(args: JobsArgs, manager: &str, tls: &TlsConfig) -> Result<()> {
    match args.command {
        JobsCommand::Queues { format } => queues(manager, tls, format).await,
        JobsCommand::List {
            queue,
            filter,
            status,
            page_size,
            cursor,
            format,
        } => {
            list(
                JobAdminConnection { manager, tls },
                &validate_queue(&queue)?,
                filter.try_wire()?,
                status,
                page_size,
                validate_cursor(cursor)?,
                format,
            )
            .await
        }
        JobsCommand::Summary {
            queue,
            filter,
            format,
        } => {
            summary(
                manager,
                tls,
                &validate_queue(&queue)?,
                filter.try_wire()?,
                format,
            )
            .await
        }
        JobsCommand::Inspect {
            queue,
            job_id,
            payload_out,
            result_out,
            force,
            format,
        } => {
            validate_job_id(&job_id)?;
            inspect(
                JobAdminConnection { manager, tls },
                &validate_queue(&queue)?,
                &job_id,
                payload_out.as_deref(),
                result_out.as_deref(),
                force,
                format,
            )
            .await
        }
        JobsCommand::Retry {
            queue,
            job_id,
            yes,
            format,
        } => {
            if !yes {
                bail!("retry requires --yes");
            }
            validate_job_id(&job_id)?;
            retry(manager, tls, &validate_queue(&queue)?, &job_id, format).await
        }
    }
}

fn validate_queue(value: &str) -> Result<String> {
    Ok(JobQueueId::parse(value)?.to_string())
}

fn validate_job_id(value: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("job ID is required");
    }
    Ok(())
}

fn validate_cursor(value: Option<String>) -> Result<String> {
    match value {
        Some(value) if value.is_empty() => bail!("--cursor must not be empty"),
        Some(value) => Ok(value),
        None => Ok(String::new()),
    }
}

fn parse_timestamp(value: &str, field: &str) -> Result<(DateTime<Utc>, Timestamp)> {
    let value = DateTime::parse_from_rfc3339(value)
        .with_context(|| format!("{field} must be an RFC3339 timestamp"))?
        .with_timezone(&Utc);
    Ok((
        value,
        Timestamp {
            seconds: value.timestamp(),
            nanos: value.timestamp_subsec_nanos() as i32,
        },
    ))
}

impl FilterArgs {
    fn try_wire(self) -> Result<Option<JobFilter>> {
        if self.worker_name.is_some() && self.worker_namespace.is_none() {
            bail!("--worker-namespace is required with --worker-name");
        }
        if self.worker_version.is_some() && self.worker_name.is_none() {
            bail!("--worker-name is required with --worker-version");
        }
        if self.source_module.is_some() && self.source_namespace.is_none() {
            bail!("--source-namespace is required with --source-module");
        }
        let (from_dt, created_at_from) = self
            .created_from
            .as_deref()
            .map(|value| parse_timestamp(value, "--created-from"))
            .transpose()?
            .unzip();
        let (before_dt, created_at_before) = self
            .created_before
            .as_deref()
            .map(|value| parse_timestamp(value, "--created-before"))
            .transpose()?
            .unzip();
        if from_dt
            .zip(before_dt)
            .is_some_and(|(from, before)| from >= before)
        {
            bail!("--created-from must be earlier than --created-before");
        }
        let filter = JobFilter {
            worker_namespace: self.worker_namespace.unwrap_or_default(),
            worker_name: self.worker_name.unwrap_or_default(),
            worker_version: self.worker_version.unwrap_or_default(),
            job_type: self.job_type.unwrap_or_default(),
            source_namespace: self.source_namespace.unwrap_or_default(),
            source_module: self.source_module.unwrap_or_default(),
            created_at_from,
            created_at_before,
        };
        let empty = filter.worker_namespace.is_empty()
            && filter.worker_name.is_empty()
            && filter.worker_version.is_empty()
            && filter.job_type.is_empty()
            && filter.source_namespace.is_empty()
            && filter.source_module.is_empty()
            && filter.created_at_from.is_none()
            && filter.created_at_before.is_none();
        Ok((!empty).then_some(filter))
    }
}

fn wire_timestamp(timestamp: &Timestamp, field: &str) -> Result<DateTime<Utc>> {
    let nanos = u32::try_from(timestamp.nanos)
        .ok()
        .filter(|value| *value < 1_000_000_000)
        .with_context(|| format!("manager returned malformed {field}"))?;
    DateTime::from_timestamp(timestamp.seconds, nanos)
        .with_context(|| format!("manager returned out-of-range {field}"))
}

fn format_timestamp(timestamp: Option<&Timestamp>) -> String {
    timestamp
        .and_then(|timestamp| wire_timestamp(timestamp, "timestamp").ok())
        .map(|value| value.to_rfc3339())
        .unwrap_or_else(|| "-".into())
}

fn validate_status(value: i32) -> Result<()> {
    match JobState::try_from(value) {
        Ok(JobState::Pending | JobState::Running | JobState::Complete | JobState::Dead) => Ok(()),
        _ => bail!("manager returned an invalid job status"),
    }
}

fn validate_summary_wire(job: &JobSummary) -> Result<()> {
    validate_status(job.status)?;
    let created = wire_timestamp(
        job.created_at
            .as_ref()
            .context("manager returned no job created_at")?,
        "job created_at",
    )?;
    let updated = wire_timestamp(
        job.updated_at
            .as_ref()
            .context("manager returned no job updated_at")?,
        "job updated_at",
    )?;
    if updated < created || job.max_attempts == 0 || job.attempt > job.max_attempts {
        bail!("manager returned inconsistent job lifecycle counters or timestamps");
    }
    if job.status != JobState::Pending as i32 && job.attempt == 0 {
        bail!("manager returned a non-pending job with zero attempts");
    }
    if job.status == JobState::Dead as i32 && job.attempt != job.max_attempts {
        bail!("manager returned a dead job with remaining attempts");
    }
    Ok(())
}

fn validate_detail_wire(job: &JobDetail) -> Result<()> {
    validate_status(job.status)?;
    let created = wire_timestamp(
        job.created_at
            .as_ref()
            .context("manager returned no job created_at")?,
        "job created_at",
    )?;
    let updated = wire_timestamp(
        job.updated_at
            .as_ref()
            .context("manager returned no job updated_at")?,
        "job updated_at",
    )?;
    if updated < created || job.max_attempts == 0 || job.attempt > job.max_attempts {
        bail!("manager returned inconsistent job lifecycle counters or timestamps");
    }
    let claimed = job
        .claimed_at
        .as_ref()
        .map(|value| wire_timestamp(value, "job claimed_at"))
        .transpose()?;
    let lease = job
        .lease_expires_at
        .as_ref()
        .map(|value| wire_timestamp(value, "job lease_expires_at"))
        .transpose()?;
    let completed = job
        .completed_at
        .as_ref()
        .map(|value| wire_timestamp(value, "job completed_at"))
        .transpose()?;
    let has_claim = claimed.is_some() || lease.is_some() || !job.claimed_by.is_empty();
    match JobState::try_from(job.status) {
        Ok(JobState::Pending) => {
            if has_claim || completed.is_some() || !job.result.is_empty() {
                bail!("manager returned inconsistent pending-job metadata");
            }
        }
        Ok(JobState::Running) => {
            let (Some(claimed), Some(lease)) = (claimed, lease) else {
                bail!("manager returned incomplete running-job claim metadata");
            };
            if job.attempt == 0
                || job.claimed_by.is_empty()
                || completed.is_some()
                || !job.result.is_empty()
                || claimed < created
                || claimed > updated
                || lease <= claimed
            {
                bail!("manager returned inconsistent running-job metadata");
            }
        }
        Ok(JobState::Complete) => {
            let Some(completed) = completed else {
                bail!("manager returned a complete job without completed_at");
            };
            if job.attempt == 0 || has_claim || completed < created || completed > updated {
                bail!("manager returned inconsistent complete-job metadata");
            }
        }
        Ok(JobState::Dead) => {
            if job.attempt != job.max_attempts
                || job.last_error.is_empty()
                || has_claim
                || completed.is_some()
                || !job.result.is_empty()
            {
                bail!("manager returned inconsistent dead-job metadata");
            }
        }
        Ok(JobState::Unspecified) | Err(_) => {
            bail!("manager returned an invalid job status")
        }
    }
    Ok(())
}

fn validate_queue_summary_wire(summary: &JobQueueSummary) -> Result<()> {
    let observed = wire_timestamp(
        summary
            .observed_at
            .as_ref()
            .context("manager returned no summary observed_at")?,
        "summary observed_at",
    )?;
    if let Some(oldest) = summary.oldest_pending_at.as_ref() {
        if wire_timestamp(oldest, "summary oldest_pending_at")? > observed {
            bail!("manager returned a future oldest-pending timestamp");
        }
    }
    let total = summary
        .pending
        .checked_add(summary.running)
        .and_then(|value| value.checked_add(summary.complete))
        .and_then(|value| value.checked_add(summary.dead))
        .context("manager returned overflowing summary counts")?;
    if total != summary.total || summary.depth != summary.pending {
        bail!("manager returned inconsistent queue summary counts");
    }
    Ok(())
}

fn status_name(value: i32) -> &'static str {
    match JobState::try_from(value) {
        Ok(JobState::Pending) => "pending",
        Ok(JobState::Running) => "running",
        Ok(JobState::Complete) => "complete",
        Ok(JobState::Dead) => "dead",
        _ => "invalid",
    }
}

async fn queues(manager: &str, tls: &TlsConfig, format: OutputFormat) -> Result<()> {
    let mut client = client::connect_job_admin(manager, tls).await?;
    let response = client
        .list_job_queues(ListJobQueuesRequest {})
        .await?
        .into_inner();
    for queue in &response.queues {
        match queue.availability() {
            wr_common::wruntime::JobQueueAvailability::Available
                if queue.fresh_delegates > 0 && queue.fresh_delegates <= queue.total_delegates => {}
            wr_common::wruntime::JobQueueAvailability::Unavailable
                if queue.fresh_delegates == 0 => {}
            _ => bail!("manager returned inconsistent job queue availability"),
        }
    }
    match format {
        OutputFormat::Json => {
            let values = response
                .queues
                .iter()
                .map(|queue| {
                    json!({
                        "job_queue_id": queue.job_queue_id,
                        "availability": match queue.availability() {
                            wr_common::wruntime::JobQueueAvailability::Available => "available",
                            wr_common::wruntime::JobQueueAvailability::Unavailable => "unavailable",
                            _ => "unspecified",
                        },
                        "fresh_delegates": queue.fresh_delegates,
                        "total_delegates": queue.total_delegates,
                    })
                })
                .collect::<Vec<_>>();
            println!("{}", serde_json::to_string_pretty(&values)?);
        }
        OutputFormat::Table => {
            let mut builder = Builder::new();
            builder.push_record(["Queue", "Availability", "Fresh", "Total"]);
            for queue in response.queues {
                let availability = match queue.availability() {
                    wr_common::wruntime::JobQueueAvailability::Available => "available".into(),
                    wr_common::wruntime::JobQueueAvailability::Unavailable => "unavailable".into(),
                    _ => "unspecified".into(),
                };
                builder.push_record([
                    queue.job_queue_id,
                    availability,
                    queue.fresh_delegates.to_string(),
                    queue.total_delegates.to_string(),
                ]);
            }
            display::print_table(builder);
        }
    }
    Ok(())
}

async fn list(
    connection: JobAdminConnection<'_>,
    queue: &str,
    filter: Option<JobFilter>,
    status: Option<JobStateArg>,
    page_size: u32,
    cursor: String,
    format: OutputFormat,
) -> Result<()> {
    let mut client = client::connect_job_admin(connection.manager, connection.tls).await?;
    let response = client
        .list_jobs(ListJobsRequest {
            job_queue_id: queue.into(),
            filter,
            status: status.map(JobStateArg::wire),
            page_size,
            cursor,
        })
        .await?
        .into_inner();
    for job in &response.jobs {
        validate_summary_wire(job)?;
    }
    match format {
        OutputFormat::Json => {
            let jobs = response
                .jobs
                .iter()
                .map(|job| {
                    json!({
                        "job_id": job.job_id,
                        "worker_namespace": job.worker_namespace,
                        "worker_name": job.worker_name,
                        "worker_version": job.worker_version,
                        "job_type": job.job_type,
                        "status": status_name(job.status),
                        "attempt": job.attempt,
                        "max_attempts": job.max_attempts,
                        "source_namespace": job.source_namespace,
                        "source_module": job.source_module,
                        "created_at": format_timestamp(job.created_at.as_ref()),
                        "updated_at": format_timestamp(job.updated_at.as_ref()),
                    })
                })
                .collect::<Vec<_>>();
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "jobs": jobs,
                    "next_cursor": response.next_cursor,
                }))?
            );
        }
        OutputFormat::Table => {
            let mut builder = Builder::new();
            builder.push_record([
                "Job ID", "Worker", "Version", "Type", "Status", "Attempt", "Created",
            ]);
            for job in &response.jobs {
                builder.push_record([
                    job.job_id.clone(),
                    format!("{}.{}", job.worker_namespace, job.worker_name),
                    job.worker_version.clone(),
                    job.job_type.clone(),
                    status_name(job.status).into(),
                    format!("{}/{}", job.attempt, job.max_attempts),
                    format_timestamp(job.created_at.as_ref()),
                ]);
            }
            display::print_table(builder);
            if !response.next_cursor.is_empty() {
                eprintln!("Next cursor: {}", response.next_cursor);
            }
        }
    }
    Ok(())
}

async fn summary(
    manager: &str,
    tls: &TlsConfig,
    queue: &str,
    filter: Option<JobFilter>,
    format: OutputFormat,
) -> Result<()> {
    let mut client = client::connect_job_admin(manager, tls).await?;
    let summary = client
        .get_job_queue_summary(GetJobQueueSummaryRequest {
            job_queue_id: queue.into(),
            filter,
        })
        .await?
        .into_inner()
        .summary
        .context("manager returned no queue summary")?;
    validate_queue_summary_wire(&summary)?;
    let value = json!({
        "observed_at": format_timestamp(summary.observed_at.as_ref()),
        "total": summary.total,
        "pending": summary.pending,
        "running": summary.running,
        "complete": summary.complete,
        "dead": summary.dead,
        "depth": summary.depth,
        "oldest_pending_at": summary.oldest_pending_at.as_ref().map(|value| format_timestamp(Some(value))),
    });
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&value)?),
        OutputFormat::Table => {
            let mut builder = Builder::new();
            builder.push_record([
                "Observed",
                "Total",
                "Pending",
                "Running",
                "Complete",
                "Dead",
                "Depth",
                "Oldest Pending",
            ]);
            builder.push_record([
                format_timestamp(summary.observed_at.as_ref()),
                summary.total.to_string(),
                summary.pending.to_string(),
                summary.running.to_string(),
                summary.complete.to_string(),
                summary.dead.to_string(),
                summary.depth.to_string(),
                format_timestamp(summary.oldest_pending_at.as_ref()),
            ]);
            display::print_table(builder);
        }
    }
    Ok(())
}

async fn inspect(
    connection: JobAdminConnection<'_>,
    queue: &str,
    job_id: &str,
    payload_out: Option<&Path>,
    result_out: Option<&Path>,
    force: bool,
    format: OutputFormat,
) -> Result<()> {
    if payload_out.is_some() && payload_out == result_out {
        bail!("--payload-out and --result-out must be different paths");
    }
    if let Some(path) = payload_out {
        validate_binary_output(path, force)?;
    }
    if let Some(path) = result_out {
        validate_binary_output(path, force)?;
    }
    let mut client = client::connect_job_admin(connection.manager, connection.tls).await?;
    let job = client
        .get_job(GetJobRequest {
            job_queue_id: queue.into(),
            job_id: job_id.into(),
        })
        .await?
        .into_inner()
        .job
        .context("manager returned no job detail")?;
    validate_detail_wire(&job)?;
    if let Some(path) = payload_out {
        write_binary(path, &job.payload, force)?;
    }
    if let Some(path) = result_out {
        write_binary(path, &job.result, force)?;
    }
    render_detail(&job, format)
}

fn detail_json(job: &JobDetail) -> Result<serde_json::Value> {
    validate_detail_wire(job)?;
    Ok(json!({
        "job_id": job.job_id,
        "worker_namespace": job.worker_namespace,
        "worker_name": job.worker_name,
        "worker_version": job.worker_version,
        "job_type": job.job_type,
        "status": status_name(job.status),
        "attempt": job.attempt,
        "max_attempts": job.max_attempts,
        "timeout_secs": job.timeout_secs,
        "source_namespace": job.source_namespace,
        "source_module": job.source_module,
        "created_at": format_timestamp(job.created_at.as_ref()),
        "updated_at": format_timestamp(job.updated_at.as_ref()),
        "claimed_at": job.claimed_at.as_ref().map(|value| format_timestamp(Some(value))),
        "lease_expires_at": job.lease_expires_at.as_ref().map(|value| format_timestamp(Some(value))),
        "completed_at": job.completed_at.as_ref().map(|value| format_timestamp(Some(value))),
        "claimed_by": job.claimed_by,
        "last_error": job.last_error,
        "payload_length": job.payload.len(),
        "result_length": job.result.len(),
    }))
}

fn render_detail(job: &JobDetail, format: OutputFormat) -> Result<()> {
    let value = detail_json(job)?;
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&value)?),
        OutputFormat::Table => {
            let mut builder = Builder::new();
            builder.push_record(["Field", "Value"]);
            for (field, value) in [
                ("job_id", job.job_id.clone()),
                (
                    "worker",
                    format!(
                        "{}.{}@{}",
                        job.worker_namespace, job.worker_name, job.worker_version
                    ),
                ),
                ("job_type", job.job_type.clone()),
                ("status", status_name(job.status).into()),
                ("attempt", format!("{}/{}", job.attempt, job.max_attempts)),
                ("timeout_secs", job.timeout_secs.to_string()),
                (
                    "source",
                    format!("{}.{}", job.source_namespace, job.source_module),
                ),
                ("created_at", format_timestamp(job.created_at.as_ref())),
                ("updated_at", format_timestamp(job.updated_at.as_ref())),
                ("claimed_at", format_timestamp(job.claimed_at.as_ref())),
                (
                    "lease_expires_at",
                    format_timestamp(job.lease_expires_at.as_ref()),
                ),
                ("completed_at", format_timestamp(job.completed_at.as_ref())),
                ("claimed_by", job.claimed_by.clone()),
                ("last_error", job.last_error.clone()),
                ("payload_length", job.payload.len().to_string()),
                ("result_length", job.result.len().to_string()),
            ] {
                builder.push_record([field.to_string(), value]);
            }
            display::print_table(builder);
        }
    }
    Ok(())
}

async fn retry(
    manager: &str,
    tls: &TlsConfig,
    queue: &str,
    job_id: &str,
    format: OutputFormat,
) -> Result<()> {
    let mut client = client::connect_job_admin(manager, tls).await?;
    let job = client
        .retry_job(RetryJobRequest {
            job_queue_id: queue.into(),
            job_id: job_id.into(),
        })
        .await?
        .into_inner()
        .job
        .context("manager returned no retried job detail")?;
    validate_detail_wire(&job)?;
    match format {
        OutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "job_id": job.job_id,
                "status": status_name(job.status),
                "attempt": job.attempt,
            }))?
        ),
        OutputFormat::Table => println!(
            "Job {} retried: status={}, attempt={}",
            job.job_id,
            status_name(job.status),
            job.attempt
        ),
    }
    Ok(())
}

fn validate_binary_output(path: &Path, force: bool) -> Result<()> {
    path.file_name()
        .context("binary output path must name a file")?;
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => {
            bail!("binary output path {} is a directory", path.display())
        }
        Ok(_) if !force => bail!(
            "refusing to overwrite {}; pass --force to replace it",
            path.display()
        ),
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| format!("failed to inspect {}", path.display()))
        }
    }
    Ok(())
}

fn write_binary(path: &Path, bytes: &[u8], force: bool) -> Result<()> {
    let parent = path
        .parent()
        .filter(|value| !value.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .context("binary output path must name a file")?
        .to_string_lossy();
    let temporary = parent.join(format!(".{file_name}.{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| -> Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .with_context(|| {
                format!("failed to create temporary output {}", temporary.display())
            })?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        if force {
            std::fs::rename(&temporary, path)
                .with_context(|| format!("failed to replace {}", path.display()))?;
        } else {
            std::fs::hard_link(&temporary, path).with_context(|| {
                format!(
                    "refusing to overwrite {}; pass --force to replace it",
                    path.display()
                )
            })?;
            std::fs::remove_file(&temporary)?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filters_require_hierarchy_and_ordered_time_range() {
        assert!(FilterArgs {
            worker_name: Some("worker".into()),
            ..Default::default()
        }
        .try_wire()
        .is_err());
        assert!(FilterArgs {
            source_module: Some("source".into()),
            ..Default::default()
        }
        .try_wire()
        .is_err());
        assert!(FilterArgs {
            created_from: Some("2025-01-02T00:00:00Z".into()),
            created_before: Some("2025-01-01T00:00:00Z".into()),
            ..Default::default()
        }
        .try_wire()
        .is_err());
    }

    #[test]
    fn binary_export_is_create_new_unless_forced() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("payload.bin");
        write_binary(&path, b"first", false).unwrap();
        assert!(validate_binary_output(&path, false).is_err());
        assert!(write_binary(&path, b"second", false).is_err());
        write_binary(&path, b"second", true).unwrap();
        assert_eq!(std::fs::read(path).unwrap(), b"second");
    }

    #[test]
    fn detail_json_never_contains_payload_bytes() {
        let timestamp = Timestamp {
            seconds: 1_700_000_000,
            nanos: 0,
        };
        let job = JobDetail {
            job_id: "job-1".into(),
            status: JobState::Complete as i32,
            attempt: 1,
            max_attempts: 1,
            created_at: Some(timestamp),
            updated_at: Some(timestamp),
            completed_at: Some(timestamp),
            payload: b"secret-payload".to_vec(),
            result: b"secret-result".to_vec(),
            ..Default::default()
        };
        let rendered = detail_json(&job).unwrap().to_string();
        assert!(!rendered.contains("secret-payload"));
        assert!(!rendered.contains("secret-result"));
        assert!(rendered.contains("payload_length"));
        assert!(rendered.contains("result_length"));
    }

    #[test]
    fn malformed_server_lifecycle_values_fail_closed() {
        let timestamp = Timestamp {
            seconds: 1_700_000_000,
            nanos: 0,
        };
        let mut job = JobDetail {
            status: 99,
            max_attempts: 1,
            created_at: Some(timestamp),
            updated_at: Some(timestamp),
            ..Default::default()
        };
        assert!(detail_json(&job).is_err());
        job.status = JobState::Pending as i32;
        job.updated_at = None;
        assert!(detail_json(&job).is_err());
        job.updated_at = Some(Timestamp {
            seconds: 1_700_000_000,
            nanos: -1,
        });
        assert!(detail_json(&job).is_err());

        job.updated_at = Some(timestamp);
        job.status = JobState::Dead as i32;
        assert!(detail_json(&job).is_err());
        job.attempt = 1;
        assert!(detail_json(&job).is_err());
        job.last_error = "failed".into();
        assert!(detail_json(&job).is_ok());
    }
}
