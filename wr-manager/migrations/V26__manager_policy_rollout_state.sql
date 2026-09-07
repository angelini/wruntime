-- Durable manager-set policy rollout coordination. Policy contents remain TOML-only.
CREATE TABLE wr_manager_rollout_guard (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    accepted_generation BIGINT CHECK (accepted_generation IS NULL OR accepted_generation > 0),
    accepted_digest TEXT,
    active_rollout_id UUID,
    CHECK ((accepted_generation IS NULL) = (accepted_digest IS NULL))
);
INSERT INTO wr_manager_rollout_guard (singleton) VALUES (TRUE) ON CONFLICT DO NOTHING;

ALTER TABLE wr_manager_rollouts
    ADD COLUMN target_policy_validator_version INTEGER NOT NULL,
    ADD COLUMN target_deployment_principal_uri TEXT NOT NULL,
    ADD COLUMN target_deployment_leaf_fingerprint TEXT NOT NULL,
    ADD COLUMN expected_target_set_hash TEXT NOT NULL,
    ADD COLUMN cluster_id TEXT NOT NULL,
    ADD COLUMN target_generation BIGINT NOT NULL CHECK (target_generation > 0),
    ADD COLUMN target_policy_digest TEXT NOT NULL,
    ADD COLUMN recovery_of UUID REFERENCES wr_manager_rollouts(rollout_id),
    ADD COLUMN executor_id UUID,
    ADD COLUMN lease_epoch BIGINT NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
    ADD COLUMN lease_expires_at TIMESTAMPTZ,
    ADD COLUMN failure TEXT,
    ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

ALTER TABLE wr_manager_rollout_guard
    ADD CONSTRAINT wr_manager_rollout_guard_active_fk
    FOREIGN KEY (active_rollout_id) REFERENCES wr_manager_rollouts(rollout_id);
CREATE UNIQUE INDEX wr_manager_rollouts_one_recovery
    ON wr_manager_rollouts(recovery_of) WHERE recovery_of IS NOT NULL;

CREATE TABLE wr_manager_rollout_members (
    rollout_id UUID NOT NULL REFERENCES wr_manager_rollouts(rollout_id) ON DELETE CASCADE,
    member_role TEXT NOT NULL CHECK (member_role IN ('source', 'target')),
    manager_id TEXT NOT NULL,
    expected_host_digest TEXT,
    expected_config_digest TEXT,
    observed_policy_generation BIGINT,
    observed_policy_digest TEXT,
    process_state TEXT,
    admission_state TEXT,
    last_acknowledged_at TIMESTAMPTZ,
    host_action_outcome TEXT,
    error TEXT,
    PRIMARY KEY (rollout_id, member_role, manager_id)
);

CREATE TABLE wr_manager_rollout_events (
    rollout_id UUID NOT NULL REFERENCES wr_manager_rollouts(rollout_id) ON DELETE CASCADE,
    sequence BIGSERIAL,
    event_type TEXT NOT NULL,
    phase INTEGER NOT NULL CHECK (phase BETWEEN 1 AND 10),
    detail TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (rollout_id, sequence)
);

ALTER TABLE wr_managers
    ADD COLUMN policy_generation BIGINT,
    ADD COLUMN policy_digest TEXT,
    ADD COLUMN admission_state TEXT NOT NULL DEFAULT 'CLOSED_STARTUP',
    ADD COLUMN rollout_id UUID,
    ADD COLUMN rollout_phase INTEGER,
    ADD COLUMN rollout_lease_epoch BIGINT NOT NULL DEFAULT 0;
