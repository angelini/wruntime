ALTER TABLE jobs ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;

-- Pre-deployment queue state is disposable. Requeue any in-flight row so every
-- future running state is created atomically with complete lease metadata.
UPDATE jobs
SET status = CASE WHEN attempt < max_attempts THEN 'pending' ELSE 'dead' END,
    claimed_at = NULL,
    claimed_by = NULL,
    claim_id = NULL,
    lease_expires_at = NULL,
    updated_at = now()
WHERE status = 'running';

UPDATE jobs
SET claimed_at = NULL,
    claimed_by = NULL,
    claim_id = NULL,
    lease_expires_at = NULL
WHERE status <> 'running';

DROP INDEX IF EXISTS idx_jobs_stale;
CREATE INDEX idx_jobs_running_lease
    ON jobs (lease_expires_at)
    WHERE status = 'running';

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'jobs_claim_metadata_valid' AND conrelid = 'jobs'::regclass) THEN
        ALTER TABLE jobs ADD CONSTRAINT jobs_claim_metadata_valid CHECK (
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
        ) NOT VALID;
    END IF;
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
ALTER TABLE jobs VALIDATE CONSTRAINT jobs_claim_metadata_valid;
