-- Production node-agent policy is a complete canonical non-secret record.
-- V17's initial attestation columns remain intact; extend them append-only.
ALTER TABLE wr_node_agent_policies
    ADD COLUMN policy_version INTEGER NOT NULL DEFAULT 1 CHECK (policy_version = 1),
    ADD COLUMN binary_digest TEXT NOT NULL DEFAULT '',
    ADD COLUMN manager_endpoint TEXT NOT NULL DEFAULT '',
    ADD COLUMN client_cert_path TEXT NOT NULL DEFAULT '',
    ADD COLUMN client_key_path TEXT NOT NULL DEFAULT '',
    ADD COLUMN ca_cert_path TEXT NOT NULL DEFAULT '',
    ADD COLUMN deployment_root TEXT NOT NULL DEFAULT '',
    ADD COLUMN runtime_dir TEXT NOT NULL DEFAULT '',
    ADD COLUMN compose_project TEXT NOT NULL DEFAULT '',
    ADD COLUMN systemctl_path TEXT NOT NULL DEFAULT '',
    ADD COLUMN docker_path TEXT NOT NULL DEFAULT '',
    ADD COLUMN poll_interval_seconds BIGINT NOT NULL DEFAULT 5 CHECK (poll_interval_seconds > 0),
    ADD COLUMN renew_interval_seconds BIGINT NOT NULL DEFAULT 5 CHECK (renew_interval_seconds > 0),
    ADD COLUMN capabilities TEXT[] NOT NULL DEFAULT '{}';

ALTER TABLE wr_node_agent_attestations
    ADD COLUMN retention_count INTEGER NOT NULL DEFAULT 1 CHECK (retention_count > 0);

-- Persist the exact allow-list that was delivered. Cleanup retries and result
-- reconciliation must prove the same manager-derived revision/digest set.
ALTER TABLE wr_node_operations
    ADD COLUMN cleanup_delete_allowlist BYTEA;

CREATE TABLE wr_node_release_deletions (
    node_id TEXT NOT NULL REFERENCES wr_nodes(node_id),
    revision BIGINT NOT NULL CHECK (revision > 0),
    bundle_digest TEXT NOT NULL CHECK (bundle_digest <> ''),
    operation_id UUID NOT NULL REFERENCES wr_node_operations(operation_id),
    deleted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (node_id, revision)
);
