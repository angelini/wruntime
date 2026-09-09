-- Replace whole-host-config authorization with a narrow compatibility record.
-- Operator cleanup retention remains on the expected policy only.
UPDATE wr_node_agent_policies p
SET capabilities = COALESCE((
    SELECT array_agg(DISTINCT capability ORDER BY capability)
    FROM unnest(p.capabilities) AS capability
), '{}');
UPDATE wr_node_agent_attestations a
SET capabilities = COALESCE((
    SELECT array_agg(DISTINCT capability ORDER BY capability)
    FROM unnest(a.capabilities) AS capability
), '{}');

ALTER TABLE wr_node_agent_policies
    DROP COLUMN config_digest,
    DROP COLUMN policy_version,
    DROP COLUMN manager_endpoint,
    DROP COLUMN client_cert_path,
    DROP COLUMN client_key_path,
    DROP COLUMN ca_cert_path,
    DROP COLUMN deployment_root,
    DROP COLUMN runtime_dir,
    DROP COLUMN compose_project,
    DROP COLUMN systemctl_path,
    DROP COLUMN docker_path,
    DROP COLUMN poll_interval_seconds,
    DROP COLUMN renew_interval_seconds;

ALTER TABLE wr_node_agent_attestations
    DROP COLUMN config_digest,
    DROP COLUMN retention_count;
