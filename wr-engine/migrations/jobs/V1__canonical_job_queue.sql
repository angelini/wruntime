CREATE TABLE IF NOT EXISTS jobs (
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
    claim_id          UUID
);

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS claim_id UUID;

DROP INDEX IF EXISTS idx_jobs_pending;
CREATE INDEX idx_jobs_pending
    ON jobs (worker_namespace, worker_name, worker_version, created_at)
    WHERE status = 'pending';

UPDATE jobs SET status = 'dead'
WHERE status NOT IN ('pending', 'running', 'complete', 'dead');
UPDATE jobs SET timeout_secs = 300 WHERE timeout_secs <= 0;
UPDATE jobs SET max_attempts = 3 WHERE max_attempts <= 0;
UPDATE jobs SET attempt = 0 WHERE attempt < 0;
UPDATE jobs SET attempt = max_attempts WHERE attempt > max_attempts;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'jobs_status_valid' AND conrelid = 'jobs'::regclass) THEN
        ALTER TABLE jobs ADD CONSTRAINT jobs_status_valid CHECK (status IN ('pending', 'running', 'complete', 'dead')) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'jobs_timeout_positive' AND conrelid = 'jobs'::regclass) THEN
        ALTER TABLE jobs ADD CONSTRAINT jobs_timeout_positive CHECK (timeout_secs > 0) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'jobs_max_attempts_positive' AND conrelid = 'jobs'::regclass) THEN
        ALTER TABLE jobs ADD CONSTRAINT jobs_max_attempts_positive CHECK (max_attempts > 0) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'jobs_attempt_valid' AND conrelid = 'jobs'::regclass) THEN
        ALTER TABLE jobs ADD CONSTRAINT jobs_attempt_valid CHECK (attempt >= 0 AND attempt <= max_attempts) NOT VALID;
    END IF;
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
ALTER TABLE jobs VALIDATE CONSTRAINT jobs_status_valid;
ALTER TABLE jobs VALIDATE CONSTRAINT jobs_timeout_positive;
ALTER TABLE jobs VALIDATE CONSTRAINT jobs_max_attempts_positive;
ALTER TABLE jobs VALIDATE CONSTRAINT jobs_attempt_valid;

CREATE OR REPLACE FUNCTION notify_new_job() RETURNS trigger AS $$
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

DROP TRIGGER IF EXISTS trg_notify_new_job ON jobs;
CREATE TRIGGER trg_notify_new_job
    AFTER INSERT ON jobs
    FOR EACH ROW EXECUTE FUNCTION notify_new_job();
