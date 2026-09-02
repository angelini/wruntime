UPDATE jobs
SET attempt = max_attempts,
    result = NULL,
    completed_at = NULL,
    claimed_at = NULL,
    claimed_by = NULL,
    claim_id = NULL,
    lease_expires_at = NULL,
    error_message = COALESCE(NULLIF(error_message, ''), 'legacy dead job: failure unavailable')
WHERE status = 'dead';

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'jobs_dead_lifecycle_valid'
          AND conrelid = 'jobs'::regclass
    ) THEN
        ALTER TABLE jobs ADD CONSTRAINT jobs_dead_lifecycle_valid CHECK (
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
        ) NOT VALID;
    END IF;
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

ALTER TABLE jobs VALIDATE CONSTRAINT jobs_dead_lifecycle_valid;
