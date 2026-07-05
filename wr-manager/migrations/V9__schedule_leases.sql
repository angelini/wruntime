ALTER TABLE wr_schedules ADD COLUMN IF NOT EXISTS next_fire_at         TIMESTAMPTZ;
ALTER TABLE wr_schedules ADD COLUMN IF NOT EXISTS claimed_by           TEXT;
ALTER TABLE wr_schedules ADD COLUMN IF NOT EXISTS claimed_until        TIMESTAMPTZ;
ALTER TABLE wr_schedules ADD COLUMN IF NOT EXISTS claim_id             UUID;
ALTER TABLE wr_schedules ADD COLUMN IF NOT EXISTS last_attempt_at      TIMESTAMPTZ;
ALTER TABLE wr_schedules ADD COLUMN IF NOT EXISTS last_error           TEXT;
ALTER TABLE wr_schedules ADD COLUMN IF NOT EXISTS consecutive_failures INT NOT NULL DEFAULT 0;

-- Backfill next_fire_at to match the old due logic (V5 claim predicate):
-- immediate never-fired -> due now; non-immediate never-fired -> created_at + interval;
-- already-fired -> last_fired_at + interval.
UPDATE wr_schedules
SET next_fire_at = CASE
    WHEN last_fired_at IS NULL AND immediate THEN NOW()
    WHEN last_fired_at IS NULL             THEN created_at   + make_interval(secs => interval_secs::double precision)
    ELSE                                        last_fired_at + make_interval(secs => interval_secs::double precision)
  END
WHERE next_fire_at IS NULL;

DROP INDEX IF EXISTS idx_schedules_due;
CREATE INDEX IF NOT EXISTS idx_schedules_due
    ON wr_schedules (enabled, next_fire_at)
    WHERE enabled = TRUE;
