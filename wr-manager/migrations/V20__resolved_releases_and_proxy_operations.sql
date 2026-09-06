-- Bind post-template release bytes independently from the immutable source bundle
-- and persist one operation-level proxy rollout state machine.
ALTER TABLE wr_node_deployments
    ADD COLUMN resolved_release_digest TEXT NOT NULL DEFAULT '',
    ADD COLUMN finalized_at TIMESTAMPTZ,
    ADD COLUMN finalized_by TEXT,
    ADD CONSTRAINT wr_node_deployments_finalization_check CHECK (
        (resolved_release_digest = '' AND finalized_at IS NULL AND finalized_by IS NULL)
        OR (resolved_release_digest <> '' AND finalized_at IS NOT NULL AND finalized_by IS NOT NULL)
    );

ALTER TABLE wr_node_operations
    ADD COLUMN resolved_release_digest TEXT NOT NULL DEFAULT '',
    ADD COLUMN proxy_next_step TEXT NOT NULL DEFAULT 'complete',
    ADD COLUMN proxy_source_revision BIGINT NOT NULL DEFAULT 0 CHECK (proxy_source_revision >= 0),
    ADD COLUMN proxy_source_digest TEXT NOT NULL DEFAULT '',
    ADD COLUMN proxy_source_resolved_digest TEXT NOT NULL DEFAULT '',
    ADD COLUMN proxy_target_revision BIGINT NOT NULL DEFAULT 0 CHECK (proxy_target_revision >= 0),
    ADD COLUMN proxy_target_digest TEXT NOT NULL DEFAULT '',
    ADD COLUMN proxy_target_resolved_digest TEXT NOT NULL DEFAULT '',
    ADD COLUMN proxy_backend_instance_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN proxy_changed BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN proxy_effect_ambiguous BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN proxy_effect_delivered_at TIMESTAMPTZ,
    ADD COLUMN proxy_effect_reported BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN proxy_effect_observed_revision BIGINT NOT NULL DEFAULT 0 CHECK (proxy_effect_observed_revision >= 0),
    ADD COLUMN proxy_effect_observed_digest TEXT NOT NULL DEFAULT '',
    ADD COLUMN proxy_effect_observed_resolved_digest TEXT NOT NULL DEFAULT '',
    ADD COLUMN proxy_effect_backend_instance_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN proxy_effect_process_instance_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN proxy_effect_condition_code TEXT NOT NULL DEFAULT '',
    ADD COLUMN proxy_effect_detail TEXT NOT NULL DEFAULT '',
    ADD CONSTRAINT wr_node_operations_proxy_step_check CHECK (proxy_next_step IN (
        'inspect_backend', 'stop_backend', 'select_release', 'start_backend',
        'verify_proxy', 'restore_source', 'complete'
    ));

ALTER TABLE wr_node_operation_slots
    ADD COLUMN source_resolved_digest TEXT NOT NULL DEFAULT '',
    ADD COLUMN target_resolved_digest TEXT NOT NULL DEFAULT '',
    ADD COLUMN effect_observed_resolved_digest TEXT NOT NULL DEFAULT '';

ALTER TABLE wr_node_slot_observations
    ADD COLUMN observed_resolved_digest TEXT NOT NULL DEFAULT '';

ALTER TABLE wr_node_slot_authority
    ADD COLUMN resolved_release_digest TEXT NOT NULL DEFAULT '';

ALTER TABLE wr_node_release_deletions
    ADD COLUMN resolved_release_digest TEXT NOT NULL DEFAULT '';
