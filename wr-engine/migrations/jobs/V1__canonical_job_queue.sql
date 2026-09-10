-- Clean-slate job queue baseline. Existing job persistence and migration history
-- must be destroyed before using this migration.
CREATE TABLE jobs (
    job_id            TEXT        PRIMARY KEY,
    worker_namespace  TEXT        NOT NULL,
    worker_name       TEXT        NOT NULL,
    worker_version    TEXT        NOT NULL,
    job_type          TEXT        NOT NULL DEFAULT '/',
    payload           BYTEA       NOT NULL DEFAULT '',
    status            TEXT        NOT NULL DEFAULT 'pending',
    result            BYTEA,
    error_message     TEXT,
    attempt           INT         NOT NULL DEFAULT 0,
    max_attempts      INT         NOT NULL DEFAULT 3,
    timeout_secs      INT         NOT NULL DEFAULT 300,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at        TIMESTAMPTZ,
    completed_at      TIMESTAMPTZ,
    claimed_by        TEXT,
    source_namespace  TEXT        NOT NULL DEFAULT '',
    source_module     TEXT        NOT NULL DEFAULT '',
    claim_id          UUID,
    lease_expires_at  TIMESTAMPTZ,
    CONSTRAINT jobs_status_valid CHECK (status IN ('pending', 'running', 'complete', 'dead')),
    CONSTRAINT jobs_timeout_positive CHECK (timeout_secs > 0),
    CONSTRAINT jobs_max_attempts_positive CHECK (max_attempts > 0),
    CONSTRAINT jobs_attempt_valid CHECK (attempt >= 0 AND attempt <= max_attempts),
    CONSTRAINT jobs_claim_metadata_valid CHECK (
        (status = 'running'
            AND claimed_at IS NOT NULL
            AND claimed_by IS NOT NULL
            AND claim_id IS NOT NULL
            AND lease_expires_at IS NOT NULL)
        OR
        (status <> 'running'
            AND claimed_at IS NULL
            AND claimed_by IS NULL
            AND claim_id IS NULL
            AND lease_expires_at IS NULL)
    ),
    CONSTRAINT jobs_dead_lifecycle_valid CHECK (
        status <> 'dead' OR (
            attempt = max_attempts
            AND error_message IS NOT NULL
            AND error_message <> ''
            AND result IS NULL
            AND completed_at IS NULL
            AND claimed_at IS NULL
            AND claimed_by IS NULL
            AND claim_id IS NULL
            AND lease_expires_at IS NULL
        )
    )
);

CREATE INDEX idx_jobs_pending
    ON jobs (worker_namespace, worker_name, worker_version, created_at)
    WHERE status = 'pending';
CREATE INDEX idx_jobs_running_lease
    ON jobs (lease_expires_at)
    WHERE status = 'running';
CREATE INDEX idx_jobs_admin_created
    ON jobs (created_at DESC, job_id DESC);
CREATE INDEX idx_jobs_admin_status_created
    ON jobs (status, created_at DESC, job_id DESC);
CREATE INDEX idx_jobs_admin_worker_created
    ON jobs (worker_namespace, worker_name, worker_version, created_at DESC, job_id DESC);

CREATE FUNCTION notify_new_job() RETURNS trigger AS $$
DECLARE
    channel_name TEXT := CASE WHEN NEW.worker_version = ''
        THEN 'wr_jobs_' || NEW.worker_namespace || '_' || NEW.worker_name || '_unversioned'
        ELSE 'wr_jobs_' || NEW.worker_namespace || '_' || NEW.worker_name || '_' || NEW.worker_version
    END;
BEGIN
    IF octet_length(channel_name) > 63 THEN
        channel_name := 'wr_jobs_long_identity';
    END IF;
    PERFORM pg_notify(channel_name, NEW.job_id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_notify_new_job
    AFTER INSERT ON jobs
    FOR EACH ROW EXECUTE FUNCTION notify_new_job();
