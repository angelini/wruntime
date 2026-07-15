-- Normalize legacy invalid values before enforcing lifecycle invariants.
UPDATE wr_schedules SET timeout_secs = 300 WHERE timeout_secs <= 0;
UPDATE wr_schedules SET max_attempts = 3 WHERE max_attempts <= 0;
UPDATE wr_schedules SET consecutive_failures = 0 WHERE consecutive_failures < 0;

ALTER TABLE wr_schedules
    DROP CONSTRAINT IF EXISTS wr_schedules_timeout_secs_positive,
    DROP CONSTRAINT IF EXISTS wr_schedules_max_attempts_positive,
    DROP CONSTRAINT IF EXISTS wr_schedules_consecutive_failures_nonnegative;
ALTER TABLE wr_schedules
    ADD CONSTRAINT wr_schedules_timeout_secs_positive CHECK (timeout_secs > 0) NOT VALID,
    ADD CONSTRAINT wr_schedules_max_attempts_positive CHECK (max_attempts > 0) NOT VALID,
    ADD CONSTRAINT wr_schedules_consecutive_failures_nonnegative CHECK (consecutive_failures >= 0) NOT VALID;
ALTER TABLE wr_schedules VALIDATE CONSTRAINT wr_schedules_timeout_secs_positive;
ALTER TABLE wr_schedules VALIDATE CONSTRAINT wr_schedules_max_attempts_positive;
ALTER TABLE wr_schedules VALIDATE CONSTRAINT wr_schedules_consecutive_failures_nonnegative;
