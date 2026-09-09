-- Unify proxy and engine operation progress after the V30 cleanup cutover.
-- This is a breaking, quiesced conversion: in-flight effects may not cross it.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM wr_node_operations
        WHERE state IN ('queued', 'running', 'paused')
    ) THEN
        RAISE EXCEPTION 'V31 requires all node operations to be terminal';
    END IF;
END $$;

CREATE TABLE wr_node_operation_targets (
    operation_id UUID NOT NULL REFERENCES wr_node_operations(operation_id) ON DELETE CASCADE,
    node_id TEXT NOT NULL REFERENCES wr_nodes(node_id),
    target_kind TEXT NOT NULL CHECK (target_kind IN ('proxy', 'engine_slot')),
    target_key TEXT NOT NULL CHECK (
        (target_kind = 'proxy' AND target_key = 'proxy') OR
        (target_kind = 'engine_slot' AND target_key <> '' AND target_key <> 'proxy')
    ),
    next_step TEXT NOT NULL,
    completed_steps INTEGER NOT NULL DEFAULT 0 CHECK (completed_steps >= 0),
    complete BOOLEAN NOT NULL DEFAULT FALSE,
    condition_code TEXT NOT NULL DEFAULT '',
    condition_detail TEXT NOT NULL DEFAULT '',
    source_revision BIGINT NOT NULL DEFAULT 0 CHECK (source_revision >= 0),
    source_digest TEXT NOT NULL DEFAULT '',
    source_resolved_digest TEXT NOT NULL DEFAULT '',
    target_revision BIGINT NOT NULL DEFAULT 0 CHECK (target_revision >= 0),
    target_digest TEXT NOT NULL DEFAULT '',
    target_resolved_digest TEXT NOT NULL DEFAULT '',
    pinned_backend_instance_id TEXT NOT NULL DEFAULT '',
    pinned_process_instance_id TEXT NOT NULL DEFAULT '',
    changed BOOLEAN NOT NULL DEFAULT FALSE,
    effect_ambiguous BOOLEAN NOT NULL DEFAULT FALSE,
    effect_delivered_at TIMESTAMPTZ,
    effect_reported BOOLEAN NOT NULL DEFAULT FALSE,
    effect_observed_revision BIGINT NOT NULL DEFAULT 0 CHECK (effect_observed_revision >= 0),
    effect_observed_digest TEXT NOT NULL DEFAULT '',
    effect_observed_resolved_digest TEXT NOT NULL DEFAULT '',
    effect_backend_instance_id TEXT NOT NULL DEFAULT '',
    effect_process_instance_id TEXT NOT NULL DEFAULT '',
    effect_condition_code TEXT NOT NULL DEFAULT '',
    effect_detail TEXT NOT NULL DEFAULT '',
    effect_termination_evidence BYTEA,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (operation_id, target_kind, target_key),
    UNIQUE (operation_id, node_id, target_kind, target_key),
    CHECK (
        (target_kind = 'proxy' AND next_step IN (
            'inspect_backend', 'verify_target', 'verify_proxy', 'stop_backend',
            'select_release', 'start_backend', 'restore_source', 'complete'
        )) OR
        (target_kind = 'engine_slot' AND next_step IN (
            'verify_release_metadata', 'stop_backend', 'select_release', 'start_backend',
            'verify_target', 'switch_authority', 'verify_serving',
            'restore_source', 'complete'
        ))
    ),
    CHECK ((next_step = 'complete') = complete)
);
CREATE UNIQUE INDEX wr_node_operation_one_proxy_target
    ON wr_node_operation_targets(operation_id) WHERE target_kind = 'proxy';

CREATE TABLE wr_node_operation_engine_target_details (
    operation_id UUID NOT NULL,
    target_kind TEXT NOT NULL DEFAULT 'engine_slot' CHECK (target_kind = 'engine_slot'),
    target_key TEXT NOT NULL,
    rollout_order INTEGER NOT NULL CHECK (rollout_order >= 0),
    authoritative_revision BIGINT NOT NULL DEFAULT 0 CHECK (authoritative_revision >= 0),
    authority_switched BOOLEAN NOT NULL DEFAULT FALSE,
    serving_converged BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (operation_id, target_kind, target_key),
    UNIQUE (operation_id, rollout_order),
    FOREIGN KEY (operation_id, target_kind, target_key)
        REFERENCES wr_node_operation_targets(operation_id, target_kind, target_key)
        ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
);

-- A deferred constraint trigger makes the engine-detail relation mandatory.
CREATE FUNCTION wr_check_node_operation_engine_target_detail() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM wr_node_operation_targets t
        WHERE t.target_kind = 'engine_slot'
          AND NOT EXISTS (
              SELECT 1 FROM wr_node_operation_engine_target_details d
              WHERE (d.operation_id, d.target_kind, d.target_key) =
                    (t.operation_id, t.target_kind, t.target_key)
          )
    ) THEN
        RAISE EXCEPTION 'every engine operation target requires exactly one engine detail row';
    END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER wr_node_operation_target_detail_required
AFTER INSERT OR UPDATE OR DELETE ON wr_node_operation_targets
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
EXECUTE FUNCTION wr_check_node_operation_engine_target_detail();
CREATE CONSTRAINT TRIGGER wr_node_operation_engine_detail_required
AFTER INSERT OR UPDATE OR DELETE ON wr_node_operation_engine_target_details
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
EXECUTE FUNCTION wr_check_node_operation_engine_target_detail();

INSERT INTO wr_node_operation_targets (
    operation_id, node_id, target_kind, target_key, next_step, completed_steps, complete,
    source_revision, source_digest, source_resolved_digest,
    target_revision, target_digest, target_resolved_digest,
    pinned_backend_instance_id, pinned_process_instance_id, changed,
    effect_ambiguous, effect_delivered_at, effect_reported,
    effect_observed_revision, effect_observed_digest, effect_observed_resolved_digest,
    effect_backend_instance_id, effect_process_instance_id,
    effect_condition_code, effect_detail, effect_termination_evidence, updated_at
)
SELECT operation_id, node_id, 'proxy', 'proxy', proxy_next_step, 0,
       proxy_next_step = 'complete',
       proxy_source_revision, proxy_source_digest, proxy_source_resolved_digest,
       proxy_target_revision, proxy_target_digest, proxy_target_resolved_digest,
       proxy_backend_instance_id, proxy_process_instance_id, proxy_changed,
       proxy_effect_ambiguous, proxy_effect_delivered_at, proxy_effect_reported,
       proxy_effect_observed_revision, proxy_effect_observed_digest,
       proxy_effect_observed_resolved_digest, proxy_effect_backend_instance_id,
       proxy_effect_process_instance_id, proxy_effect_condition_code,
       proxy_effect_detail, proxy_effect_termination_evidence, updated_at
FROM wr_node_operations;

INSERT INTO wr_node_operation_targets (
    operation_id, node_id, target_kind, target_key, next_step, completed_steps, complete,
    condition_code, condition_detail, source_revision, source_digest, source_resolved_digest,
    target_revision, target_digest, target_resolved_digest,
    pinned_backend_instance_id, pinned_process_instance_id, changed,
    effect_ambiguous, effect_delivered_at, effect_reported,
    effect_observed_revision, effect_observed_digest, effect_observed_resolved_digest,
    effect_backend_instance_id, effect_process_instance_id,
    effect_condition_code, effect_detail, effect_termination_evidence, updated_at
)
SELECT operation_id, node_id, 'engine_slot', engine_slot, next_step, completed_steps, complete,
       condition_code, condition_detail, source_revision, source_digest, source_resolved_digest,
       target_revision, target_digest, target_resolved_digest,
       pinned_backend_instance_id, pinned_process_instance_id, changed,
       effect_ambiguous, effect_delivered_at, effect_reported,
       effect_observed_revision, effect_observed_digest, effect_observed_resolved_digest,
       effect_backend_instance_id, effect_process_instance_id,
       effect_condition_code, effect_detail, effect_termination_evidence, updated_at
FROM wr_node_operation_slots;

INSERT INTO wr_node_operation_engine_target_details (
    operation_id, target_key, rollout_order, authoritative_revision,
    authority_switched, serving_converged
)
SELECT operation_id, engine_slot, rollout_order, authoritative_revision,
       authority_switched, serving_converged
FROM wr_node_operation_slots;

ALTER TABLE wr_node_operation_result_receipts
    ADD COLUMN target_kind TEXT,
    ADD COLUMN target_key TEXT;
UPDATE wr_node_operation_result_receipts
SET target_kind = CASE WHEN engine_slot = '' THEN 'proxy' ELSE 'engine_slot' END,
    target_key = CASE WHEN engine_slot = '' THEN 'proxy' ELSE engine_slot END;
ALTER TABLE wr_node_operation_result_receipts
    ALTER COLUMN target_kind SET NOT NULL,
    ALTER COLUMN target_key SET NOT NULL,
    ADD CONSTRAINT wr_node_operation_result_receipts_target_kind_check
        CHECK (target_kind IN ('proxy', 'engine_slot')),
    ADD CONSTRAINT wr_node_operation_result_receipts_target_fk
        FOREIGN KEY (operation_id, target_kind, target_key)
        REFERENCES wr_node_operation_targets(operation_id, target_kind, target_key),
    DROP CONSTRAINT wr_node_operation_result_receipts_pkey,
    ADD PRIMARY KEY (
        operation_id, node_id, agent_instance_id, lease_epoch, step, target_kind, target_key
    );

DO $$
BEGIN
    IF (SELECT count(*) FROM wr_node_operation_targets WHERE target_kind = 'proxy') <>
       (SELECT count(*) FROM wr_node_operations) THEN
        RAISE EXCEPTION 'proxy target conversion count mismatch';
    END IF;
    IF (SELECT count(*) FROM wr_node_operation_targets WHERE target_kind = 'engine_slot') <>
       (SELECT count(*) FROM wr_node_operation_slots) OR
       (SELECT count(*) FROM wr_node_operation_engine_target_details) <>
       (SELECT count(*) FROM wr_node_operation_slots) THEN
        RAISE EXCEPTION 'engine target conversion count mismatch';
    END IF;
    IF EXISTS (
        SELECT 1 FROM wr_node_operation_slots s
        JOIN wr_node_operation_targets t
          ON t.operation_id = s.operation_id AND t.target_kind = 'engine_slot'
         AND t.target_key = s.engine_slot
        JOIN wr_node_operation_engine_target_details d
          ON (d.operation_id, d.target_kind, d.target_key) =
             (t.operation_id, t.target_kind, t.target_key)
        WHERE ROW(s.next_step, s.completed_steps, s.complete, s.condition_code,
                  s.condition_detail, s.source_revision, s.source_digest,
                  s.source_resolved_digest, s.target_revision, s.target_digest,
                  s.target_resolved_digest, s.pinned_backend_instance_id,
                  s.pinned_process_instance_id, s.changed, s.effect_ambiguous,
                  s.effect_delivered_at, s.effect_reported, s.effect_observed_revision,
                  s.effect_observed_digest, s.effect_observed_resolved_digest,
                  s.effect_backend_instance_id, s.effect_process_instance_id,
                  s.effect_condition_code, s.effect_detail, s.effect_termination_evidence,
                  s.rollout_order,
                  s.authoritative_revision, s.authority_switched, s.serving_converged)
           IS DISTINCT FROM
              ROW(t.next_step, t.completed_steps, t.complete, t.condition_code,
                  t.condition_detail, t.source_revision, t.source_digest,
                  t.source_resolved_digest, t.target_revision, t.target_digest,
                  t.target_resolved_digest, t.pinned_backend_instance_id,
                  t.pinned_process_instance_id, t.changed, t.effect_ambiguous,
                  t.effect_delivered_at, t.effect_reported, t.effect_observed_revision,
                  t.effect_observed_digest, t.effect_observed_resolved_digest,
                  t.effect_backend_instance_id, t.effect_process_instance_id,
                  t.effect_condition_code, t.effect_detail, t.effect_termination_evidence,
                  d.rollout_order,
                  d.authoritative_revision, d.authority_switched, d.serving_converged)
    ) THEN
        RAISE EXCEPTION 'engine target conversion field mismatch';
    END IF;
    IF EXISTS (
        SELECT 1 FROM wr_node_operations o
        JOIN wr_node_operation_targets t
          ON t.operation_id = o.operation_id AND t.target_kind = 'proxy'
        WHERE ROW(o.proxy_next_step, o.proxy_source_revision, o.proxy_source_digest,
                  o.proxy_source_resolved_digest, o.proxy_target_revision,
                  o.proxy_target_digest, o.proxy_target_resolved_digest,
                  o.proxy_backend_instance_id, o.proxy_process_instance_id,
                  o.proxy_changed, o.proxy_effect_ambiguous, o.proxy_effect_delivered_at,
                  o.proxy_effect_reported, o.proxy_effect_observed_revision,
                  o.proxy_effect_observed_digest, o.proxy_effect_observed_resolved_digest,
                  o.proxy_effect_backend_instance_id, o.proxy_effect_process_instance_id,
                  o.proxy_effect_condition_code, o.proxy_effect_detail,
                  o.proxy_effect_termination_evidence)
           IS DISTINCT FROM
              ROW(t.next_step, t.source_revision, t.source_digest,
                  t.source_resolved_digest, t.target_revision, t.target_digest,
                  t.target_resolved_digest, t.pinned_backend_instance_id,
                  t.pinned_process_instance_id, t.changed, t.effect_ambiguous,
                  t.effect_delivered_at, t.effect_reported, t.effect_observed_revision,
                  t.effect_observed_digest, t.effect_observed_resolved_digest,
                  t.effect_backend_instance_id, t.effect_process_instance_id,
                  t.effect_condition_code, t.effect_detail,
                  t.effect_termination_evidence)
    ) THEN
        RAISE EXCEPTION 'proxy target conversion field mismatch';
    END IF;
END $$;

ALTER TABLE wr_node_operation_result_receipts DROP COLUMN engine_slot;
DROP TABLE wr_node_operation_slots;
ALTER TABLE wr_node_operations
    DROP COLUMN proxy_next_step,
    DROP COLUMN proxy_source_revision,
    DROP COLUMN proxy_source_digest,
    DROP COLUMN proxy_source_resolved_digest,
    DROP COLUMN proxy_target_revision,
    DROP COLUMN proxy_target_digest,
    DROP COLUMN proxy_target_resolved_digest,
    DROP COLUMN proxy_backend_instance_id,
    DROP COLUMN proxy_process_instance_id,
    DROP COLUMN proxy_changed,
    DROP COLUMN proxy_effect_ambiguous,
    DROP COLUMN proxy_effect_delivered_at,
    DROP COLUMN proxy_effect_reported,
    DROP COLUMN proxy_effect_observed_revision,
    DROP COLUMN proxy_effect_observed_digest,
    DROP COLUMN proxy_effect_observed_resolved_digest,
    DROP COLUMN proxy_effect_backend_instance_id,
    DROP COLUMN proxy_effect_process_instance_id,
    DROP COLUMN proxy_effect_condition_code,
    DROP COLUMN proxy_effect_detail,
    DROP COLUMN proxy_effect_termination_evidence;
