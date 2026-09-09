-- Cut over the durable node lifecycle vocabulary after the unified target migration.
-- Legacy action history is intentionally not translated because drain and the former
-- deployment variants do not have an unambiguous new intent.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM wr_node_operations
        WHERE action NOT IN ('deployment', 'rollback', 'restart')
    ) THEN
        RAISE EXCEPTION 'V33 cannot cut over legacy node operation actions; reset legacy operation history before retrying';
    END IF;
END $$;

ALTER TABLE wr_node_operations
    DROP CONSTRAINT IF EXISTS wr_node_operations_action_check;
ALTER TABLE wr_node_operations
    ADD CONSTRAINT wr_node_operations_action_check
    CHECK (action IN ('deployment', 'rollback', 'restart'));

ALTER TABLE wr_node_operation_engine_target_details
    ADD COLUMN transition_kind TEXT;

UPDATE wr_node_operation_engine_target_details d
SET transition_kind = CASE
    WHEN o.action = 'restart' THEN 'restart'
    WHEN t.source_revision = 0 AND t.target_revision > 0 THEN 'addition'
    WHEN t.source_revision > 0 AND t.target_revision = 0 THEN 'removal'
    WHEN t.source_revision = t.target_revision
         AND t.source_digest = t.target_digest
         AND t.source_resolved_digest = t.target_resolved_digest THEN 'unchanged'
    ELSE 'replacement'
END
FROM wr_node_operation_targets t
JOIN wr_node_operations o ON o.operation_id = t.operation_id
WHERE (d.operation_id, d.target_kind, d.target_key) =
      (t.operation_id, t.target_kind, t.target_key);

-- V31's relation-integrity triggers are deferred; flush the update before
-- altering the same table again in this migration transaction.
SET CONSTRAINTS ALL IMMEDIATE;

ALTER TABLE wr_node_operation_engine_target_details
    ALTER COLUMN transition_kind SET NOT NULL,
    ADD CONSTRAINT wr_node_operation_engine_transition_kind_check
    CHECK (transition_kind IN ('addition', 'replacement', 'unchanged', 'removal', 'restart'));
