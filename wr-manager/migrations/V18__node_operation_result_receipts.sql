-- Result delivery may commit even when the RPC response is lost. Retain the
-- exact accepted protobuf payload so the same activation can retry and receive
-- an idempotent acknowledgement without claiming another effect. Engine slot
-- is part of the instruction target identity because one operation may execute
-- the same typed step for multiple slots under one lease epoch.
CREATE TABLE wr_node_operation_result_receipts (
    operation_id UUID NOT NULL REFERENCES wr_node_operations(operation_id),
    node_id TEXT NOT NULL CHECK (node_id <> ''),
    agent_instance_id TEXT NOT NULL CHECK (agent_instance_id <> ''),
    lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
    step INTEGER NOT NULL,
    engine_slot TEXT NOT NULL,
    authenticated_principal TEXT NOT NULL CHECK (authenticated_principal <> ''),
    result_payload BYTEA NOT NULL,
    accepted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (
        operation_id,
        node_id,
        agent_instance_id,
        lease_epoch,
        step,
        engine_slot
    )
);
