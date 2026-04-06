CREATE TABLE IF NOT EXISTS wr_schedules (
    schedule_id       TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    worker_namespace  TEXT NOT NULL,
    worker_name       TEXT NOT NULL,
    worker_version    TEXT NOT NULL,
    job_type          TEXT NOT NULL,
    interval_secs     INT NOT NULL CHECK (interval_secs > 0),
    immediate         BOOL NOT NULL DEFAULT FALSE,
    payload           BYTEA NOT NULL DEFAULT ''::bytea,
    timeout_secs      INT NOT NULL DEFAULT 300,
    max_attempts      INT NOT NULL DEFAULT 3,
    enabled           BOOL NOT NULL DEFAULT TRUE,
    last_fired_at     TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (worker_namespace, worker_name, worker_version, job_type)
);

CREATE INDEX IF NOT EXISTS idx_schedules_due
    ON wr_schedules (enabled, last_fired_at, interval_secs)
    WHERE enabled = TRUE;
