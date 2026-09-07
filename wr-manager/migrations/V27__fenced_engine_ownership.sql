-- Phase-4 cutover: manager-derived deployment identities and full-u64 slot fences.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM wr_node_deployments) OR EXISTS (SELECT 1 FROM wr_node_operations) THEN
        RAISE EXCEPTION 'V27 certificate authorization cutover requires reset: legacy deployment/operation rows exist';
    END IF;
END $$;

ALTER TABLE wr_node_deployments
    ADD COLUMN inventory_schema_version INTEGER NOT NULL CHECK (inventory_schema_version = 1),
    ADD COLUMN revision_digest TEXT NOT NULL;

ALTER TABLE wr_node_operations
    ADD COLUMN target_revision_digest TEXT;
ALTER TABLE wr_node_operations ADD CONSTRAINT wr_node_operations_target_digest
    CHECK (target_revision = 0 OR target_revision_digest IS NOT NULL);

ALTER TABLE wr_engines
    ADD COLUMN operation_id UUID,
    ADD COLUMN deployment_revision_digest TEXT,
    ADD COLUMN activation_id UUID,
    ADD COLUMN slot_generation BYTEA CHECK (slot_generation IS NULL OR octet_length(slot_generation) = 8);

CREATE TABLE wr_node_slot_owners (
    node_id TEXT NOT NULL REFERENCES wr_nodes(node_id),
    engine_slot TEXT NOT NULL,
    operation_id UUID,
    revision BIGINT CHECK (revision IS NULL OR revision > 0),
    revision_digest TEXT,
    activation_id UUID,
    engine_id TEXT,
    slot_generation BYTEA NOT NULL CHECK (octet_length(slot_generation) = 8),
    route_authority BOOLEAN NOT NULL DEFAULT FALSE,
    lifecycle_authority BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (node_id, engine_slot),
    CHECK ((operation_id IS NULL) = (activation_id IS NULL)),
    CHECK ((revision IS NULL) = (revision_digest IS NULL))
);
