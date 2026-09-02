-- Durable, manager-owned operator intent and fenced node-agent progress.
-- current_revision remains the committed serving revision; target_revision is
-- staged until every authoritative slot has converged and commit is recorded.
ALTER TABLE wr_nodes
    ADD COLUMN IF NOT EXISTS target_revision BIGINT CHECK (target_revision > 0);

-- Materialize deployment identity beside the protobuf registration so routing
-- authority can be enforced transactionally without decoding protobuf in SQL.
ALTER TABLE wr_engines
    ADD COLUMN IF NOT EXISTS deployment_node_id TEXT,
    ADD COLUMN IF NOT EXISTS deployment_revision BIGINT CHECK (deployment_revision > 0),
    ADD COLUMN IF NOT EXISTS deployment_bundle_digest TEXT,
    ADD COLUMN IF NOT EXISTS deployment_engine_slot TEXT;
CREATE INDEX IF NOT EXISTS idx_wr_engines_deployment_slot
    ON wr_engines (deployment_node_id, deployment_engine_slot, deployment_revision);

CREATE TABLE IF NOT EXISTS wr_node_operations (
    operation_id       UUID PRIMARY KEY,
    node_id            TEXT NOT NULL REFERENCES wr_nodes(node_id),
    request_token      TEXT NOT NULL,
    actor              TEXT NOT NULL,
    action             TEXT NOT NULL CHECK (action IN (
        'initial_apply', 'drain', 'restart', 'rolling_upgrade', 'scale', 'rollback'
    )),
    state              TEXT NOT NULL CHECK (state IN (
        'queued', 'running', 'paused', 'succeeded', 'failed', 'cancelled'
    )),
    request_payload    BYTEA NOT NULL,
    policy             BYTEA NOT NULL,
    source_revision    BIGINT NOT NULL DEFAULT 0 CHECK (source_revision >= 0),
    target_revision    BIGINT NOT NULL DEFAULT 0 CHECK (target_revision >= 0),
    bundle_digest      TEXT NOT NULL DEFAULT '',
    committed          BOOLEAN NOT NULL DEFAULT FALSE,
    lease_epoch        BIGINT NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
    lease_expires_at   TIMESTAMPTZ,
    claimed_by         TEXT,
    failure_code       TEXT NOT NULL DEFAULT '',
    failure_detail     TEXT NOT NULL DEFAULT '',
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (actor, request_token)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_wr_node_operations_one_active
    ON wr_node_operations (node_id)
    WHERE state IN ('queued', 'running', 'paused');
CREATE INDEX IF NOT EXISTS idx_wr_node_operations_history
    ON wr_node_operations (node_id, created_at DESC, operation_id);

CREATE TABLE IF NOT EXISTS wr_node_operation_slots (
    operation_id            UUID NOT NULL REFERENCES wr_node_operations(operation_id),
    node_id                 TEXT NOT NULL REFERENCES wr_nodes(node_id),
    engine_slot             TEXT NOT NULL,
    rollout_order           INTEGER NOT NULL CHECK (rollout_order >= 0),
    authoritative_revision  BIGINT NOT NULL DEFAULT 0 CHECK (authoritative_revision >= 0),
    next_step               TEXT NOT NULL CHECK (next_step IN (
        'verify_release', 'stop_slot', 'select_release', 'start_slot', 'verify_ready', 'complete'
    )),
    completed_steps         INTEGER NOT NULL DEFAULT 0 CHECK (completed_steps >= 0),
    complete                BOOLEAN NOT NULL DEFAULT FALSE,
    condition_code          TEXT NOT NULL DEFAULT '',
    condition_detail        TEXT NOT NULL DEFAULT '',
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (operation_id, engine_slot),
    UNIQUE (operation_id, node_id, engine_slot),
    UNIQUE (operation_id, rollout_order)
);

CREATE TABLE IF NOT EXISTS wr_node_slot_authority (
    node_id          TEXT NOT NULL REFERENCES wr_nodes(node_id),
    engine_slot      TEXT NOT NULL,
    revision         BIGINT NOT NULL CHECK (revision > 0),
    authoritative    BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (node_id, engine_slot, revision),
    FOREIGN KEY (node_id, revision) REFERENCES wr_node_deployments(node_id, revision)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_wr_node_slot_one_authority
    ON wr_node_slot_authority (node_id, engine_slot)
    WHERE authoritative;

CREATE TABLE IF NOT EXISTS wr_node_operation_events (
    sequence        BIGSERIAL PRIMARY KEY,
    operation_id    UUID NOT NULL REFERENCES wr_node_operations(operation_id),
    actor           TEXT NOT NULL,
    event_code      TEXT NOT NULL,
    detail          TEXT NOT NULL DEFAULT '',
    lease_epoch     BIGINT NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_wr_node_operation_events_operation
    ON wr_node_operation_events (operation_id, sequence);

CREATE TABLE IF NOT EXISTS wr_node_slot_observations (
    node_id             TEXT NOT NULL REFERENCES wr_nodes(node_id),
    engine_slot         TEXT NOT NULL,
    lifecycle_status    BYTEA,
    backend_state       TEXT NOT NULL CHECK (backend_state IN ('unknown', 'running', 'exited')),
    backend_instance_id TEXT NOT NULL DEFAULT '',
    observed_revision   BIGINT NOT NULL DEFAULT 0 CHECK (observed_revision >= 0),
    observed_at         TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (node_id, engine_slot)
);
