-- Release retention is durable per-node maintenance, independent of rollout success.
-- Existing coupled cleanup operations are terminalized; periodic reconciliation
-- rediscovers any remaining work after upgrade.

UPDATE wr_node_operations
SET phase = 'complete', state = 'succeeded', lease_expires_at = NULL,
    claimed_by = NULL, agent_instance_id = NULL, updated_at = NOW()
WHERE phase IN ('committed_cleanup', 'superseded');

ALTER TABLE wr_node_operations
    DROP CONSTRAINT wr_node_operations_phase_check,
    DROP CONSTRAINT wr_node_operations_commit_phase_check,
    DROP CONSTRAINT wr_node_operations_phase_state_check;
ALTER TABLE wr_node_operations
    ADD CONSTRAINT wr_node_operations_phase_check CHECK (
        phase IN ('forward', 'restoring_source', 'committing', 'complete')
    ),
    ADD CONSTRAINT wr_node_operations_commit_phase_check CHECK (
        (NOT committed AND committed_at IS NULL)
        OR (committed AND committed_at IS NOT NULL AND phase = 'complete')
    ),
    ADD CONSTRAINT wr_node_operations_phase_state_check CHECK (
        (phase = 'complete' AND state IN ('succeeded', 'failed', 'cancelled'))
        OR (phase IN ('forward', 'restoring_source', 'committing')
            AND state IN ('queued', 'running', 'paused'))
    );

UPDATE wr_node_operation_slots
SET next_step = 'complete', complete = TRUE, updated_at = NOW()
WHERE next_step = 'cleanup_release';

ALTER TABLE wr_node_operation_slots
    DROP CONSTRAINT wr_node_operation_slots_next_step_check,
    ADD CONSTRAINT wr_node_operation_slots_next_step_check CHECK (next_step IN (
        'verify_release_metadata', 'verify_proxy', 'stop_backend', 'select_release',
        'start_backend', 'verify_target', 'switch_authority', 'verify_serving',
        'restore_source', 'complete'
    ));

CREATE TABLE wr_node_release_cleanup (
    node_id TEXT PRIMARY KEY REFERENCES wr_nodes(node_id),
    generation BIGINT NOT NULL DEFAULT 1 CHECK (generation > 0),
    state TEXT NOT NULL DEFAULT 'needs_reconcile' CHECK (
        state IN ('clean', 'needs_reconcile', 'pending', 'claimed', 'paused')
    ),
    protection_fingerprint TEXT NOT NULL DEFAULT '',
    authority_payload BYTEA,
    payload_digest TEXT NOT NULL DEFAULT '',
    known_inventory BYTEA,
    candidate_count INTEGER NOT NULL DEFAULT 0 CHECK (candidate_count >= 0),
    agent_instance_id TEXT,
    claimed_by TEXT,
    claim_instance UUID,
    lease_epoch BIGINT NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
    lease_expires_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    last_reconciled_at TIMESTAMPTZ,
    next_reconcile_at TIMESTAMPTZ,
    last_attempt_at TIMESTAMPTZ,
    diagnostic_code TEXT NOT NULL DEFAULT '',
    diagnostic_detail TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (node_id, generation),
    CHECK ((state = 'claimed') = (agent_instance_id IS NOT NULL)),
    CHECK ((state = 'claimed') = (claimed_by IS NOT NULL)),
    CHECK ((state = 'claimed') = (claim_instance IS NOT NULL)),
    CHECK ((state = 'claimed') = (lease_expires_at IS NOT NULL)),
    CHECK ((authority_payload IS NULL) = (payload_digest = '')),
    CHECK (state NOT IN ('pending', 'claimed') OR authority_payload IS NOT NULL)
);
CREATE INDEX idx_wr_node_release_cleanup_due
    ON wr_node_release_cleanup (last_reconciled_at ASC NULLS FIRST, node_id);

CREATE TABLE wr_node_release_cleanup_generations (
    node_id TEXT NOT NULL REFERENCES wr_nodes(node_id),
    generation BIGINT NOT NULL CHECK (generation > 0),
    protection_fingerprint TEXT NOT NULL,
    authority_payload BYTEA NOT NULL,
    payload_digest TEXT NOT NULL CHECK (payload_digest <> ''),
    known_inventory BYTEA,
    outcome TEXT NOT NULL DEFAULT 'materialized' CHECK (
        outcome IN ('materialized', 'superseded', 'succeeded', 'paused')
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (node_id, generation),
    UNIQUE (node_id, generation, payload_digest)
);

CREATE TABLE wr_node_release_cleanup_events (
    node_id TEXT NOT NULL REFERENCES wr_nodes(node_id),
    generation BIGINT NOT NULL CHECK (generation > 0),
    sequence BIGSERIAL,
    event_code TEXT NOT NULL CHECK (event_code <> ''),
    detail TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (node_id, sequence)
);

CREATE TABLE wr_node_release_cleanup_result_receipts (
    node_id TEXT NOT NULL,
    generation BIGINT NOT NULL,
    agent_instance_id TEXT NOT NULL CHECK (agent_instance_id <> ''),
    lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
    claim_instance UUID NOT NULL,
    authenticated_principal TEXT NOT NULL CHECK (authenticated_principal <> ''),
    payload_digest TEXT NOT NULL CHECK (payload_digest <> ''),
    result_payload BYTEA NOT NULL,
    accepted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (node_id, generation, agent_instance_id, lease_epoch, claim_instance),
    FOREIGN KEY (node_id, generation) REFERENCES wr_node_release_cleanup_generations(node_id, generation)
);

ALTER TABLE wr_node_release_deletions
    ALTER COLUMN operation_id DROP NOT NULL,
    ADD COLUMN cleanup_generation BIGINT,
    ADD CONSTRAINT wr_node_release_deletions_cleanup_generation_fk
        FOREIGN KEY (node_id, cleanup_generation)
        REFERENCES wr_node_release_cleanup_generations(node_id, generation),
    ADD CONSTRAINT wr_node_release_deletions_exactly_one_provenance CHECK (
        (operation_id IS NOT NULL) <> (cleanup_generation IS NOT NULL)
    );

-- Legacy operation receipts and deletion provenance remain queryable, but the
-- old operation-owned execution payload is no longer runnable.
ALTER TABLE wr_node_operations
    DROP COLUMN cleanup_superseded_by,
    DROP COLUMN cleanup_evidence,
    DROP COLUMN cleanup_delivered_at,
    DROP COLUMN cleanup_reported_at,
    DROP COLUMN cleanup_backend_query_error,
    DROP COLUMN cleanup_delete_allowlist;
