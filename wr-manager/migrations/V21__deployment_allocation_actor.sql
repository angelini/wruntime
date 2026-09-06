-- Bind every staged deployment allocation to the authenticated operator actor.
-- Existing rows predate the role-gated allocation endpoint; preserve their
-- operation actor where available and mark only fixture/history rows explicitly.
ALTER TABLE wr_node_deployments
    ADD COLUMN allocated_by TEXT;

UPDATE wr_node_deployments AS deployment
SET allocated_by = COALESCE(
    (SELECT operation.actor
     FROM wr_node_operations AS operation
     WHERE operation.operation_id = deployment.operation_id),
    finalized_by,
    abandoned_by,
    'pre-v21-manager-api'
);

ALTER TABLE wr_node_deployments
    ALTER COLUMN allocated_by SET NOT NULL;

ALTER TABLE wr_node_deployments
    ADD CONSTRAINT wr_node_deployments_allocated_by_not_empty
    CHECK (allocated_by <> '');
