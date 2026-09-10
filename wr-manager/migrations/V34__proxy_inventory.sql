-- Authenticated proxy inventory is observational. Distinct process tuples may
-- coexist; a clean deregistration retains the exact tuple as a late-report fence.
CREATE TABLE wr_proxy_inventory (
    proxy_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    process_instance_id TEXT NOT NULL,
    deployment_revision BIGINT,
    bundle_digest TEXT,
    operation_id UUID,
    revision_digest TEXT,
    registration BYTEA NOT NULL,
    report BYTEA,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    report_received_at TIMESTAMPTZ,
    receiving_manager_id TEXT,
    deregistered_at TIMESTAMPTZ,
    PRIMARY KEY (proxy_id, node_id, process_instance_id),
    CONSTRAINT wr_proxy_inventory_process_instance_id_length CHECK (
        octet_length(process_instance_id) BETWEEN 1 AND 255
    ),
    CONSTRAINT wr_proxy_inventory_managed_identity_complete CHECK (
        (deployment_revision IS NULL AND bundle_digest IS NULL AND operation_id IS NULL AND revision_digest IS NULL)
        OR
        (deployment_revision > 0 AND bundle_digest IS NOT NULL AND operation_id IS NOT NULL AND revision_digest IS NOT NULL)
    ),
    CONSTRAINT wr_proxy_inventory_report_receipt_complete CHECK (
        (report IS NULL AND report_received_at IS NULL AND receiving_manager_id IS NULL)
        OR
        (report IS NOT NULL AND report_received_at IS NOT NULL AND receiving_manager_id IS NOT NULL)
    )
);

CREATE INDEX wr_proxy_inventory_active_order
    ON wr_proxy_inventory (node_id, proxy_id, process_instance_id)
    WHERE deregistered_at IS NULL;
CREATE INDEX wr_proxy_inventory_tombstone_retention
    ON wr_proxy_inventory (deregistered_at)
    WHERE deregistered_at IS NOT NULL;
