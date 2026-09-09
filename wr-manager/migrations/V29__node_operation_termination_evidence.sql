-- Typed backend stop evidence is encoded using the canonical protobuf message.
-- NULL preserves the fail-closed unknown result for historical operations.
ALTER TABLE wr_node_operation_slots
    ADD COLUMN effect_termination_evidence BYTEA;

ALTER TABLE wr_node_operations
    ADD COLUMN proxy_effect_termination_evidence BYTEA;
