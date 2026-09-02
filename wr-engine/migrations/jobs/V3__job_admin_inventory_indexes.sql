CREATE INDEX IF NOT EXISTS idx_jobs_admin_created
    ON jobs (created_at DESC, job_id DESC);

CREATE INDEX IF NOT EXISTS idx_jobs_admin_status_created
    ON jobs (status, created_at DESC, job_id DESC);

CREATE INDEX IF NOT EXISTS idx_jobs_admin_worker_created
    ON jobs (
        worker_namespace,
        worker_name,
        worker_version,
        created_at DESC,
        job_id DESC
    );
