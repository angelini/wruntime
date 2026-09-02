use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_postgres::types::ToSql;
use tokio_postgres::Row;

use wr_common::lifecycle::{
    AttemptCount, JobState, JobTimeoutSecs, MaxAttempts, MAX_JOB_BLOB_BYTES, MAX_JOB_ERROR_BYTES,
    MAX_JOB_METADATA_BYTES,
};

pub const DEFAULT_PAGE_SIZE: u32 = 50;
pub const MAX_PAGE_SIZE: u32 = 200;

#[derive(Clone, Debug, Default)]
pub struct JobFilter {
    pub worker_namespace: Option<String>,
    pub worker_name: Option<String>,
    pub worker_version: Option<String>,
    pub job_type: Option<String>,
    pub source_namespace: Option<String>,
    pub source_module: Option<String>,
    pub created_at_from: Option<DateTime<Utc>>,
    pub created_at_before: Option<DateTime<Utc>>,
}

impl JobFilter {
    pub fn validate(&self) -> Result<(), JobAdminError> {
        if self.worker_name.is_some() && self.worker_namespace.is_none() {
            return Err(JobAdminError::Invalid(
                "worker_namespace is required with worker_name".into(),
            ));
        }
        if self.worker_version.is_some() && self.worker_name.is_none() {
            return Err(JobAdminError::Invalid(
                "worker_name is required with worker_version".into(),
            ));
        }
        if self.source_module.is_some() && self.source_namespace.is_none() {
            return Err(JobAdminError::Invalid(
                "source_namespace is required with source_module".into(),
            ));
        }
        if self
            .created_at_from
            .zip(self.created_at_before)
            .is_some_and(|(from, before)| from >= before)
        {
            return Err(JobAdminError::Invalid(
                "created_at_from must be earlier than created_at_before".into(),
            ));
        }
        Ok(())
    }

    fn fingerprint(&self, status: Option<JobState>) -> String {
        let normalized = format!(
            "{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}",
            self.worker_namespace.as_deref().unwrap_or(""),
            self.worker_name.as_deref().unwrap_or(""),
            self.worker_version.as_deref().unwrap_or(""),
            self.job_type.as_deref().unwrap_or(""),
            self.source_namespace.as_deref().unwrap_or(""),
            self.source_module.as_deref().unwrap_or(""),
            self.created_at_from
                .map(|value| value.to_rfc3339())
                .unwrap_or_default(),
            self.created_at_before
                .map(|value| value.to_rfc3339())
                .unwrap_or_default(),
            status.map(JobState::as_str).unwrap_or("")
        );
        format!("{:x}", Sha256::digest(normalized.as_bytes()))
    }
}

#[derive(Clone, Debug)]
pub struct JobListItem {
    pub job_id: String,
    pub worker_namespace: String,
    pub worker_name: String,
    pub worker_version: String,
    pub job_type: String,
    pub status: JobState,
    pub attempt: AttemptCount,
    pub max_attempts: MaxAttempts,
    pub source_namespace: String,
    pub source_module: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct JobDetail {
    pub summary: JobListItem,
    pub payload: Vec<u8>,
    pub result: Vec<u8>,
    pub last_error: String,
    pub timeout: JobTimeoutSecs,
    pub claimed_at: Option<DateTime<Utc>>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub claimed_by: String,
}

#[derive(Clone, Debug)]
pub struct JobPage {
    pub jobs: Vec<JobListItem>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug)]
pub struct JobQueueSummary {
    pub observed_at: DateTime<Utc>,
    pub pending: u64,
    pub running: u64,
    pub complete: u64,
    pub dead: u64,
    pub total: u64,
    pub oldest_pending_at: Option<DateTime<Utc>>,
}

#[derive(Debug)]
pub enum JobAdminError {
    Invalid(String),
    NotFound,
    FailedPrecondition(String),
    Database(anyhow::Error),
}

impl std::fmt::Display for JobAdminError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(message) | Self::FailedPrecondition(message) => {
                formatter.write_str(message)
            }
            Self::NotFound => formatter.write_str("job not found"),
            Self::Database(_) => formatter.write_str("job queue database operation failed"),
        }
    }
}

impl std::error::Error for JobAdminError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Database(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}

impl From<anyhow::Error> for JobAdminError {
    fn from(error: anyhow::Error) -> Self {
        Self::Database(error)
    }
}

impl From<tokio_postgres::Error> for JobAdminError {
    fn from(error: tokio_postgres::Error) -> Self {
        Self::Database(error.into())
    }
}

impl From<deadpool_postgres::PoolError> for JobAdminError {
    fn from(error: deadpool_postgres::PoolError) -> Self {
        Self::Database(error.into())
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct CursorV1 {
    v: u8,
    created_at: String,
    job_id: String,
    filter: String,
}

fn decode_cursor(
    value: &str,
    filter: &JobFilter,
    status: Option<JobState>,
) -> Result<Option<(DateTime<Utc>, String)>, JobAdminError> {
    if value.is_empty() {
        return Ok(None);
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| JobAdminError::Invalid("cursor is malformed".into()))?;
    let cursor: CursorV1 = serde_json::from_slice(&bytes)
        .map_err(|_| JobAdminError::Invalid("cursor is malformed".into()))?;
    if cursor.v != 1 || cursor.filter != filter.fingerprint(status) {
        return Err(JobAdminError::Invalid(
            "cursor does not match the requested filters".into(),
        ));
    }
    let created_at = DateTime::parse_from_rfc3339(&cursor.created_at)
        .map_err(|_| JobAdminError::Invalid("cursor is malformed".into()))?
        .with_timezone(&Utc);
    if cursor.job_id.is_empty() {
        return Err(JobAdminError::Invalid("cursor is malformed".into()));
    }
    Ok(Some((created_at, cursor.job_id)))
}

fn encode_cursor(item: &JobListItem, filter: &JobFilter, status: Option<JobState>) -> String {
    let cursor = CursorV1 {
        v: 1,
        created_at: item.created_at.to_rfc3339(),
        job_id: item.job_id.clone(),
        filter: filter.fingerprint(status),
    };
    URL_SAFE_NO_PAD.encode(serde_json::to_vec(&cursor).expect("cursor serialization is infallible"))
}

fn push_param<T: ToSql + Sync + Send + 'static>(
    params: &mut Vec<Box<dyn ToSql + Sync + Send>>,
    value: T,
) -> usize {
    params.push(Box::new(value));
    params.len()
}

fn filter_predicates(
    filter: &JobFilter,
    params: &mut Vec<Box<dyn ToSql + Sync + Send>>,
) -> Vec<String> {
    let mut predicates = Vec::new();
    macro_rules! text_filter {
        ($field:ident, $column:literal) => {
            if let Some(value) = &filter.$field {
                let index = push_param(params, value.clone());
                predicates.push(format!(concat!($column, " = ${}"), index));
            }
        };
    }
    text_filter!(worker_namespace, "worker_namespace");
    text_filter!(worker_name, "worker_name");
    text_filter!(worker_version, "worker_version");
    text_filter!(job_type, "job_type");
    text_filter!(source_namespace, "source_namespace");
    text_filter!(source_module, "source_module");
    if let Some(value) = filter.created_at_from {
        let index = push_param(params, value);
        predicates.push(format!("created_at >= ${index}"));
    }
    if let Some(value) = filter.created_at_before {
        let index = push_param(params, value);
        predicates.push(format!("created_at < ${index}"));
    }
    predicates
}

fn sql_where(predicates: &[String]) -> String {
    if predicates.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", predicates.join(" AND "))
    }
}

fn checked_counts(row: &Row) -> Result<(AttemptCount, MaxAttempts), JobAdminError> {
    let attempt = u32::try_from(row.get::<_, i32>(6))
        .map_err(|_| JobAdminError::Database(anyhow::anyhow!("negative attempt")))?;
    let max_attempts = u32::try_from(row.get::<_, i32>(7))
        .ok()
        .and_then(|value| MaxAttempts::new(value).ok())
        .ok_or_else(|| JobAdminError::Database(anyhow::anyhow!("invalid max attempts")))?;
    let attempt = AttemptCount::new(attempt)
        .validate(max_attempts)
        .map_err(JobAdminError::Database)?;
    Ok((attempt, max_attempts))
}

fn list_item(row: &Row) -> Result<JobListItem, JobAdminError> {
    let status = JobState::try_from(row.get::<_, &str>(5)).map_err(JobAdminError::Database)?;
    let (attempt, max_attempts) = checked_counts(row)?;
    if status != JobState::Pending && attempt.get() == 0 {
        return Err(JobAdminError::Database(anyhow::anyhow!(
            "non-pending job has zero attempts"
        )));
    }
    if status == JobState::Dead && attempt.get() != max_attempts.get() {
        return Err(JobAdminError::Database(anyhow::anyhow!(
            "dead job has remaining attempts"
        )));
    }
    let created_at = row.get(10);
    let updated_at = row.get(11);
    if updated_at < created_at {
        return Err(JobAdminError::Database(anyhow::anyhow!(
            "job updated_at precedes created_at"
        )));
    }
    let item = JobListItem {
        job_id: row.get(0),
        worker_namespace: row.get(1),
        worker_name: row.get(2),
        worker_version: row.get(3),
        job_type: row.get(4),
        status,
        attempt,
        max_attempts,
        source_namespace: row.get(8),
        source_module: row.get(9),
        created_at,
        updated_at,
    };
    let metadata_bytes = [
        item.job_id.as_str(),
        item.worker_namespace.as_str(),
        item.worker_name.as_str(),
        item.worker_version.as_str(),
        item.job_type.as_str(),
        item.source_namespace.as_str(),
        item.source_module.as_str(),
    ]
    .into_iter()
    .try_fold(0_usize, |total, value| total.checked_add(value.len()))
    .ok_or_else(|| JobAdminError::Database(anyhow::anyhow!("job metadata length overflow")))?;
    if metadata_bytes > MAX_JOB_METADATA_BYTES {
        return Err(JobAdminError::Database(anyhow::anyhow!(
            "job metadata exceeds configured size bounds"
        )));
    }
    Ok(item)
}

const LIST_COLUMNS: &str = "job_id, worker_namespace, worker_name, worker_version, job_type, status, attempt, max_attempts, source_namespace, source_module, created_at, updated_at";
const DETAIL_COLUMNS: &str = "job_id, worker_namespace, worker_name, worker_version, job_type, status, attempt, max_attempts, source_namespace, source_module, created_at, updated_at, payload, result, error_message, timeout_secs, claimed_at, lease_expires_at, completed_at, claimed_by, claim_id";

fn validate_dead_lifecycle(
    attempt: u32,
    max_attempts: u32,
    last_error: Option<&str>,
) -> Result<(), JobAdminError> {
    if attempt != max_attempts || !last_error.is_some_and(|value| !value.is_empty()) {
        return Err(JobAdminError::Database(anyhow::anyhow!(
            "dead job has inconsistent attempt or failure metadata"
        )));
    }
    Ok(())
}

fn detail(row: &Row) -> Result<JobDetail, JobAdminError> {
    let summary = list_item(row)?;
    let timeout = u32::try_from(row.get::<_, i32>(15))
        .ok()
        .and_then(|value| JobTimeoutSecs::new(value).ok())
        .ok_or_else(|| JobAdminError::Database(anyhow::anyhow!("invalid job timeout")))?;
    let payload: Vec<u8> = row.get(12);
    let result: Option<Vec<u8>> = row.get(13);
    let last_error: Option<String> = row.get(14);
    let claimed_at: Option<DateTime<Utc>> = row.get(16);
    let lease_expires_at: Option<DateTime<Utc>> = row.get(17);
    let completed_at: Option<DateTime<Utc>> = row.get(18);
    let claimed_by: Option<String> = row.get(19);
    let claim_id: Option<uuid::Uuid> = row.get(20);

    if payload.len() > MAX_JOB_BLOB_BYTES
        || result
            .as_ref()
            .is_some_and(|value| value.len() > MAX_JOB_BLOB_BYTES)
        || last_error
            .as_ref()
            .is_some_and(|value| value.len() > MAX_JOB_ERROR_BYTES)
    {
        return Err(JobAdminError::Database(anyhow::anyhow!(
            "job detail exceeds configured size bounds"
        )));
    }
    let metadata_bytes = [
        summary.worker_namespace.as_str(),
        summary.worker_name.as_str(),
        summary.worker_version.as_str(),
        summary.job_type.as_str(),
        summary.source_namespace.as_str(),
        summary.source_module.as_str(),
    ]
    .into_iter()
    .try_fold(0_usize, |total, value| total.checked_add(value.len()))
    .ok_or_else(|| JobAdminError::Database(anyhow::anyhow!("job metadata length overflow")))?;
    if metadata_bytes > MAX_JOB_METADATA_BYTES {
        return Err(JobAdminError::Database(anyhow::anyhow!(
            "job metadata exceeds configured size bounds"
        )));
    }

    let no_claim = claimed_at.is_none()
        && lease_expires_at.is_none()
        && claimed_by.is_none()
        && claim_id.is_none();
    match summary.status {
        JobState::Pending => {
            if !no_claim || completed_at.is_some() || result.is_some() {
                return Err(JobAdminError::Database(anyhow::anyhow!(
                    "pending job has terminal or claim metadata"
                )));
            }
        }
        JobState::Dead => {
            validate_dead_lifecycle(
                summary.attempt.get(),
                summary.max_attempts.get(),
                last_error.as_deref(),
            )?;
            if !no_claim || completed_at.is_some() || result.is_some() {
                return Err(JobAdminError::Database(anyhow::anyhow!(
                    "dead job has terminal or claim metadata"
                )));
            }
        }
        JobState::Running => {
            let (Some(claimed_at), Some(lease_expires_at), Some(claimed_by), Some(_)) = (
                claimed_at,
                lease_expires_at,
                claimed_by.as_deref(),
                claim_id,
            ) else {
                return Err(JobAdminError::Database(anyhow::anyhow!(
                    "running job is missing claim metadata"
                )));
            };
            if claimed_by.is_empty()
                || completed_at.is_some()
                || result.is_some()
                || claimed_at < summary.created_at
                || claimed_at > summary.updated_at
                || lease_expires_at <= claimed_at
            {
                return Err(JobAdminError::Database(anyhow::anyhow!(
                    "running job has inconsistent lifecycle metadata"
                )));
            }
        }
        JobState::Complete => {
            let Some(completed_at) = completed_at else {
                return Err(JobAdminError::Database(anyhow::anyhow!(
                    "complete job is missing completed_at"
                )));
            };
            if !no_claim
                || result.is_none()
                || completed_at < summary.created_at
                || completed_at > summary.updated_at
            {
                return Err(JobAdminError::Database(anyhow::anyhow!(
                    "complete job has inconsistent lifecycle metadata"
                )));
            }
        }
    }

    Ok(JobDetail {
        summary,
        payload,
        result: result.unwrap_or_default(),
        last_error: last_error.unwrap_or_default(),
        timeout,
        claimed_at,
        lease_expires_at,
        completed_at,
        claimed_by: claimed_by.unwrap_or_default(),
    })
}

pub async fn list_jobs(
    pool: &Pool,
    filter: &JobFilter,
    status: Option<JobState>,
    page_size: u32,
    cursor: &str,
) -> Result<JobPage, JobAdminError> {
    filter.validate()?;
    let page_size = if page_size == 0 {
        DEFAULT_PAGE_SIZE
    } else {
        page_size
    };
    if !(1..=MAX_PAGE_SIZE).contains(&page_size) {
        return Err(JobAdminError::Invalid(format!(
            "page_size must be in 1..={MAX_PAGE_SIZE}"
        )));
    }
    let cursor = decode_cursor(cursor, filter, status)?;
    let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
    let mut predicates = filter_predicates(filter, &mut params);
    if let Some(status) = status {
        let index = push_param(&mut params, status.as_str().to_string());
        predicates.push(format!("status = ${index}"));
    }
    if let Some((created_at, job_id)) = cursor {
        let created_index = push_param(&mut params, created_at);
        let id_index = push_param(&mut params, job_id);
        predicates.push(format!(
            "(created_at, job_id) < (${created_index}, ${id_index})"
        ));
    }
    let limit_index = push_param(&mut params, i64::from(page_size) + 1);
    let sql = format!(
        "SELECT {LIST_COLUMNS} FROM wr__jobs.jobs{} ORDER BY created_at DESC, job_id DESC LIMIT ${limit_index}",
        sql_where(&predicates)
    );
    let refs: Vec<&(dyn ToSql + Sync)> = params
        .iter()
        .map(|value| value.as_ref() as &(dyn ToSql + Sync))
        .collect();
    let rows = pool.get().await?.query(&sql, &refs).await?;
    let has_more = rows.len() > page_size as usize;
    let mut jobs = rows
        .iter()
        .take(page_size as usize)
        .map(list_item)
        .collect::<Result<Vec<_>, _>>()?;
    let next_cursor = if has_more {
        jobs.last().map(|item| encode_cursor(item, filter, status))
    } else {
        None
    };
    jobs.shrink_to_fit();
    Ok(JobPage { jobs, next_cursor })
}

pub async fn summarize_jobs(
    pool: &Pool,
    filter: &JobFilter,
) -> Result<JobQueueSummary, JobAdminError> {
    filter.validate()?;
    let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
    let predicates = filter_predicates(filter, &mut params);
    let sql = format!(
        "SELECT statement_timestamp(), \
         count(*) FILTER (WHERE status = 'pending'), \
         count(*) FILTER (WHERE status = 'running'), \
         count(*) FILTER (WHERE status = 'complete'), \
         count(*) FILTER (WHERE status = 'dead'), count(*), \
         min(created_at) FILTER (WHERE status = 'pending') \
         FROM wr__jobs.jobs{}",
        sql_where(&predicates)
    );
    let refs: Vec<&(dyn ToSql + Sync)> = params
        .iter()
        .map(|value| value.as_ref() as &(dyn ToSql + Sync))
        .collect();
    let row = pool.get().await?.query_one(&sql, &refs).await?;
    let count = |index| -> Result<u64, JobAdminError> {
        u64::try_from(row.get::<_, i64>(index))
            .map_err(|_| JobAdminError::Database(anyhow::anyhow!("invalid aggregate count")))
    };
    Ok(JobQueueSummary {
        observed_at: row.get(0),
        pending: count(1)?,
        running: count(2)?,
        complete: count(3)?,
        dead: count(4)?,
        total: count(5)?,
        oldest_pending_at: row.get(6),
    })
}

pub async fn get_job(pool: &Pool, job_id: &str) -> Result<JobDetail, JobAdminError> {
    if job_id.trim().is_empty() {
        return Err(JobAdminError::Invalid("job_id is required".into()));
    }
    let sql = format!("SELECT {DETAIL_COLUMNS} FROM wr__jobs.jobs WHERE job_id = $1");
    let row = pool
        .get()
        .await?
        .query_opt(&sql, &[&job_id])
        .await?
        .ok_or(JobAdminError::NotFound)?;
    detail(&row)
}

pub async fn retry_dead_job(pool: &Pool, job_id: &str) -> Result<JobDetail, JobAdminError> {
    if job_id.trim().is_empty() {
        return Err(JobAdminError::Invalid("job_id is required".into()));
    }
    let mut client = pool.get().await?;
    let transaction = client.transaction().await?;
    let row = transaction
        .query_opt(
            "SELECT status, worker_namespace, worker_name, worker_version \
             FROM wr__jobs.jobs WHERE job_id = $1 FOR UPDATE",
            &[&job_id],
        )
        .await?
        .ok_or(JobAdminError::NotFound)?;
    let state = JobState::try_from(row.get::<_, &str>(0)).map_err(JobAdminError::Database)?;
    if state != JobState::Dead {
        return Err(JobAdminError::FailedPrecondition(format!(
            "job is {}, only dead jobs can be retried",
            state.as_str()
        )));
    }
    let channel = crate::worker::worker_channel(
        row.get::<_, &str>(1),
        row.get::<_, &str>(2),
        row.get::<_, &str>(3),
    );
    let sql = format!(
        "UPDATE wr__jobs.jobs SET status = 'pending', attempt = 0, result = NULL, \
         completed_at = NULL, claimed_at = NULL, claimed_by = NULL, claim_id = NULL, \
         lease_expires_at = NULL, updated_at = now() WHERE job_id = $1 RETURNING {DETAIL_COLUMNS}"
    );
    let updated = transaction.query_one(&sql, &[&job_id]).await?;
    transaction
        .execute("SELECT pg_notify($1, $2)", &[&channel, &job_id])
        .await?;
    let updated = detail(&updated)?;
    transaction.commit().await?;
    Ok(updated)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dead_lifecycle_requires_exhausted_attempts_and_failure_evidence() {
        assert!(validate_dead_lifecycle(3, 3, Some("failed")).is_ok());
        assert!(validate_dead_lifecycle(2, 3, Some("failed")).is_err());
        assert!(validate_dead_lifecycle(3, 3, None).is_err());
        assert!(validate_dead_lifecycle(3, 3, Some("")).is_err());
    }
}
