-- Digest-qualified manager activation evidence is persisted with each target
-- member so takeover can reconcile database intent with protected host records.
ALTER TABLE wr_manager_rollout_members
    ADD COLUMN expected_backend TEXT,
    ADD COLUMN expected_executable_digest TEXT,
    ADD COLUMN expected_backend_spec_digest TEXT,
    ADD COLUMN expected_credential_digest TEXT,
    ADD COLUMN expected_old_selector_digest TEXT,
    ADD COLUMN expected_new_selector_digest TEXT;
