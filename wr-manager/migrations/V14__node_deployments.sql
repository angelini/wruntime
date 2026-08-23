-- Desired node deployment state is separate from ephemeral engine registrations.
-- Revisions are allocated under a row lock on wr_nodes and deployment snapshots
-- are append-only so a rollback always creates a new revision.
CREATE TABLE IF NOT EXISTS wr_nodes (
    node_id          TEXT PRIMARY KEY,
    current_revision BIGINT NOT NULL DEFAULT 0 CHECK (current_revision >= 0),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS wr_node_deployments (
    node_id            TEXT NOT NULL REFERENCES wr_nodes(node_id),
    revision           BIGINT NOT NULL CHECK (revision > 0),
    attempt_token      TEXT NOT NULL,
    bundle_digest      TEXT NOT NULL,
    expected_inventory BYTEA NOT NULL,
    state              TEXT NOT NULL CHECK (state IN ('pending', 'active', 'succeeded', 'failed')),
    failure_detail     TEXT NOT NULL DEFAULT '',
    source_revision    BIGINT NOT NULL DEFAULT 0 CHECK (source_revision >= 0),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    activated_at       TIMESTAMPTZ,
    completed_at       TIMESTAMPTZ,
    PRIMARY KEY (node_id, revision),
    UNIQUE (node_id, attempt_token)
);
CREATE INDEX IF NOT EXISTS idx_wr_node_deployments_history
    ON wr_node_deployments (node_id, revision DESC);
CREATE INDEX IF NOT EXISTS idx_wr_node_deployments_success
    ON wr_node_deployments (node_id, revision DESC) WHERE state = 'succeeded';
