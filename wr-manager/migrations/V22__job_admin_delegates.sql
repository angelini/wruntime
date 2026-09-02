ALTER TABLE wr_engines
    ADD COLUMN IF NOT EXISTS job_queue_id TEXT,
    ADD COLUMN IF NOT EXISTS job_admin_address TEXT;

-- Registrations created before job administration remain non-delegates.
UPDATE wr_engines
SET job_queue_id = NULL,
    job_admin_address = NULL
WHERE COALESCE(job_queue_id, '') = ''
   OR COALESCE(job_admin_address, '') = '';

CREATE INDEX IF NOT EXISTS idx_wr_engines_job_admin_delegates
    ON wr_engines (job_queue_id, last_heartbeat DESC, engine_id)
    WHERE job_queue_id IS NOT NULL AND job_admin_address IS NOT NULL;
