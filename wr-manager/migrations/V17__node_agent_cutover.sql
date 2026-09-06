-- Complete the manager-owned lifecycle contract without rewriting V16 history.
-- Forward work has one immutable absolute deadline. Source restoration is
-- deliberately deadline-free, but every effect remains activation/epoch fenced.
ALTER TABLE wr_node_operations
    ADD COLUMN forward_deadline TIMESTAMPTZ,
    ADD COLUMN phase TEXT NOT NULL DEFAULT 'forward',
    ADD COLUMN forward_fenced BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN restoration_requested BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN restoration_terminal_state TEXT,
    ADD COLUMN agent_instance_id TEXT,
    ADD COLUMN committed_at TIMESTAMPTZ,
    ADD COLUMN cleanup_superseded_by UUID REFERENCES wr_node_operations(operation_id),
    ADD COLUMN cleanup_evidence BYTEA,
    ADD COLUMN cleanup_delivered_at TIMESTAMPTZ,
    ADD COLUMN cleanup_reported_at TIMESTAMPTZ,
    ADD COLUMN cleanup_backend_query_error TEXT NOT NULL DEFAULT '',
    ADD COLUMN proxy_process_instance_id TEXT NOT NULL DEFAULT '';

-- Existing V16 operations predate the cutover and cannot resume under the new
-- wire. Preserve their history while placing terminal rows in a legal phase.
UPDATE wr_node_operations
SET forward_deadline = created_at + INTERVAL '30 minutes',
    phase = CASE
        WHEN state IN ('succeeded', 'failed', 'cancelled') THEN 'complete'
        ELSE 'forward'
    END,
    committed_at = CASE WHEN committed THEN updated_at ELSE NULL END;
ALTER TABLE wr_node_operations
    ALTER COLUMN forward_deadline SET NOT NULL,
    ADD CONSTRAINT wr_node_operations_phase_check CHECK (
        phase IN ('forward', 'restoring_source', 'committing', 'committed_cleanup', 'complete', 'superseded')
    ),
    ADD CONSTRAINT wr_node_operations_restoration_terminal_check CHECK (
        restoration_terminal_state IS NULL OR restoration_terminal_state IN ('failed', 'cancelled')
    ),
    ADD CONSTRAINT wr_node_operations_forward_deadline_check CHECK (
        forward_deadline >= created_at
    ),
    ADD CONSTRAINT wr_node_operations_commit_phase_check CHECK (
        (NOT committed AND committed_at IS NULL AND phase NOT IN ('committed_cleanup', 'superseded'))
        OR (committed AND committed_at IS NOT NULL AND phase IN ('committed_cleanup', 'complete', 'superseded'))
    ),
    ADD CONSTRAINT wr_node_operations_phase_state_check CHECK (
        (phase = 'complete' AND state IN ('succeeded', 'failed', 'cancelled'))
        OR (phase = 'superseded' AND state = 'succeeded')
        OR (phase IN ('forward', 'restoring_source', 'committing', 'committed_cleanup')
            AND state IN ('queued', 'running', 'paused'))
    ),
    ADD CONSTRAINT wr_node_operations_restoration_check CHECK (
        (phase = 'restoring_source' AND restoration_requested AND forward_fenced
            AND restoration_terminal_state IS NOT NULL)
        OR phase <> 'restoring_source'
    ),
    ADD CONSTRAINT wr_node_operations_lease_owner_check CHECK (
        (lease_expires_at IS NULL AND claimed_by IS NULL AND agent_instance_id IS NULL)
        OR (lease_expires_at IS NOT NULL AND claimed_by IS NOT NULL
            AND agent_instance_id IS NOT NULL AND agent_instance_id <> '')
    );

ALTER TABLE wr_node_operation_slots
    DROP CONSTRAINT IF EXISTS wr_node_operation_slots_next_step_check;
ALTER TABLE wr_node_operation_slots
    ADD COLUMN source_revision BIGINT NOT NULL DEFAULT 0 CHECK (source_revision >= 0),
    ADD COLUMN source_digest TEXT NOT NULL DEFAULT '',
    ADD COLUMN target_revision BIGINT NOT NULL DEFAULT 0 CHECK (target_revision >= 0),
    ADD COLUMN target_digest TEXT NOT NULL DEFAULT '',
    ADD COLUMN pinned_backend_instance_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN pinned_process_instance_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN authority_switched BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN serving_converged BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN changed BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN effect_ambiguous BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN effect_delivered_at TIMESTAMPTZ,
    ADD COLUMN effect_reported BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN effect_condition_code TEXT NOT NULL DEFAULT '',
    ADD COLUMN effect_detail TEXT NOT NULL DEFAULT '',
    ADD COLUMN effect_observed_revision BIGINT NOT NULL DEFAULT 0 CHECK (effect_observed_revision >= 0),
    ADD COLUMN effect_observed_digest TEXT NOT NULL DEFAULT '',
    ADD COLUMN effect_backend_instance_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN effect_process_instance_id TEXT NOT NULL DEFAULT '';

-- Backfill the immutable source/target snapshot carried by every slot before
-- constraining it to the canonical step vocabulary.
UPDATE wr_node_operation_slots s
SET source_revision = o.source_revision,
    target_revision = o.target_revision,
    source_digest = COALESCE(source.bundle_digest, ''),
    target_digest = o.bundle_digest,
    next_step = CASE s.next_step
        WHEN 'verify_release' THEN 'verify_release_metadata'
        WHEN 'stop_slot' THEN 'stop_backend'
        WHEN 'start_slot' THEN 'start_backend'
        WHEN 'verify_ready' THEN 'verify_target'
        ELSE s.next_step
    END
FROM wr_node_operations o
LEFT JOIN wr_node_deployments source
  ON source.node_id = o.node_id AND source.revision = o.source_revision
WHERE s.operation_id = o.operation_id;

ALTER TABLE wr_node_operation_slots
    ADD CONSTRAINT wr_node_operation_slots_next_step_check CHECK (next_step IN (
        'verify_release_metadata', 'verify_proxy', 'stop_backend', 'select_release',
        'start_backend', 'verify_target', 'switch_authority', 'verify_serving',
        'restore_source', 'cleanup_release', 'complete'
    ));

ALTER TABLE wr_node_slot_observations
    DROP CONSTRAINT wr_node_slot_observations_backend_state_check,
    ADD CONSTRAINT wr_node_slot_observations_backend_state_check CHECK (
        backend_state IN ('unknown', 'running', 'exited', 'query_error')
    ),
    ADD COLUMN observed_digest TEXT NOT NULL DEFAULT '',
    ADD COLUMN backend_query_error TEXT NOT NULL DEFAULT '',
    ADD COLUMN operation_id UUID REFERENCES wr_node_operations(operation_id),
    ADD COLUMN agent_instance_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN lease_epoch BIGINT NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
    ADD CONSTRAINT wr_node_slot_observations_query_error_check CHECK (
        (backend_state = 'query_error' AND backend_query_error <> '')
        OR (backend_state <> 'query_error' AND backend_query_error = '')
    );

CREATE TABLE wr_node_agent_policies (
    node_id TEXT PRIMARY KEY REFERENCES wr_nodes(node_id),
    protocol_version TEXT NOT NULL CHECK (protocol_version <> ''),
    config_digest TEXT NOT NULL CHECK (config_digest <> ''),
    backend TEXT NOT NULL CHECK (backend IN ('systemd', 'docker')),
    retention_count INTEGER NOT NULL CHECK (retention_count >= 1),
    actor TEXT NOT NULL CHECK (actor <> ''),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE wr_node_agent_attestations (
    node_id TEXT NOT NULL REFERENCES wr_nodes(node_id),
    agent_instance_id TEXT NOT NULL CHECK (agent_instance_id <> ''),
    authenticated_principal TEXT NOT NULL CHECK (authenticated_principal <> ''),
    protocol_version TEXT NOT NULL CHECK (protocol_version <> ''),
    binary_digest TEXT NOT NULL CHECK (binary_digest <> ''),
    config_digest TEXT NOT NULL CHECK (config_digest <> ''),
    backend TEXT NOT NULL CHECK (backend IN ('systemd', 'docker')),
    capabilities TEXT[] NOT NULL DEFAULT '{}',
    observed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (node_id, agent_instance_id)
);
CREATE INDEX idx_wr_node_agent_attestations_fresh
    ON wr_node_agent_attestations (node_id, observed_at DESC);

-- The allocation token is the same actor/token identity used at submission.
-- Actor is populated when the operation binds the staged allocation; until then
-- the allocation may be safely abandoned only by an authenticated operator.
ALTER TABLE wr_node_deployments
    ADD COLUMN allocation_actor TEXT,
    ADD COLUMN abandoned_at TIMESTAMPTZ,
    ADD COLUMN abandoned_by TEXT,
    ADD COLUMN operation_id UUID REFERENCES wr_node_operations(operation_id),
    ADD CONSTRAINT wr_node_deployments_abandonment_check CHECK (
        (abandoned_at IS NULL AND abandoned_by IS NULL)
        OR (abandoned_at IS NOT NULL AND abandoned_by IS NOT NULL AND operation_id IS NULL)
    );
CREATE UNIQUE INDEX idx_wr_node_deployments_operation
    ON wr_node_deployments(operation_id) WHERE operation_id IS NOT NULL;

-- Only one effect-capable committed cleanup or forward/restoration operation may
-- exist for a node. Superseded cleanup is retained but is no longer active.
DROP INDEX IF EXISTS idx_wr_node_operations_one_active;
CREATE UNIQUE INDEX idx_wr_node_operations_one_active
    ON wr_node_operations (node_id)
    WHERE state IN ('queued', 'running', 'paused') AND phase <> 'superseded';
