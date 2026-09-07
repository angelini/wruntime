-- Lost-response-safe manager rollout creation identity. The broader rollout
-- state machine is added by the policy/rollout phase; this table establishes
-- the immutable caller identity and canonical request binding first.
CREATE TABLE wr_manager_rollouts (
    rollout_id UUID PRIMARY KEY,
    deployment_principal_uri TEXT NOT NULL,
    deployment_leaf_fingerprint TEXT NOT NULL,
    client_operation_id UUID NOT NULL,
    canonical_request_digest TEXT NOT NULL,
    canonical_request BYTEA NOT NULL,
    phase INTEGER NOT NULL DEFAULT 1 CHECK (phase BETWEEN 1 AND 10),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (deployment_principal_uri, client_operation_id)
);
