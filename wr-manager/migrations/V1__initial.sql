-- Clean-slate manager schema baseline. Existing manager persistence and
-- migration history must be destroyed before using this migration.

CREATE FUNCTION wr_check_node_operation_engine_target_detail() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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

SET default_tablespace = '';

SET default_table_access_method = heap;

CREATE TABLE wr_engines (
    engine_id text NOT NULL,
    address text NOT NULL,
    proxy_address text NOT NULL,
    registration bytea NOT NULL,
    registered_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    last_heartbeat timestamp with time zone DEFAULT now() NOT NULL,
    peer_address text DEFAULT ''::text NOT NULL,
    draining boolean DEFAULT false NOT NULL,
    deployment_node_id text,
    deployment_revision bigint,
    deployment_bundle_digest text,
    deployment_engine_slot text,
    job_queue_id text,
    job_admin_address text,
    operation_id uuid,
    deployment_revision_digest text,
    activation_id uuid,
    slot_generation bytea,
    CONSTRAINT wr_engines_deployment_revision_check CHECK ((deployment_revision > 0)),
    CONSTRAINT wr_engines_slot_generation_check CHECK (((slot_generation IS NULL) OR (octet_length(slot_generation) = 8)))
);

CREATE TABLE wr_manager_lock (
    id integer DEFAULT 1 NOT NULL,
    version bigint DEFAULT 0 NOT NULL,
    CONSTRAINT wr_manager_lock_id_check CHECK ((id = 1))
);

CREATE TABLE wr_manager_rollout_events (
    rollout_id uuid NOT NULL,
    sequence bigint NOT NULL,
    event_type text NOT NULL,
    phase integer NOT NULL,
    detail text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT wr_manager_rollout_events_phase_check CHECK (((phase >= 1) AND (phase <= 10)))
);

CREATE SEQUENCE wr_manager_rollout_events_sequence_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE wr_manager_rollout_events_sequence_seq OWNED BY wr_manager_rollout_events.sequence;

CREATE TABLE wr_manager_rollout_guard (
    singleton boolean DEFAULT true NOT NULL,
    accepted_generation bigint,
    accepted_digest text,
    active_rollout_id uuid,
    recovery_permit_principal_uri text,
    CONSTRAINT wr_manager_rollout_guard_accepted_generation_check CHECK (((accepted_generation IS NULL) OR (accepted_generation > 0))),
    CONSTRAINT wr_manager_rollout_guard_check CHECK (((accepted_generation IS NULL) = (accepted_digest IS NULL))),
    CONSTRAINT wr_manager_rollout_guard_permit_check CHECK (active_rollout_id IS NULL OR recovery_permit_principal_uri IS NULL),
    CONSTRAINT wr_manager_rollout_guard_singleton_check CHECK (singleton)
);

CREATE TABLE wr_manager_rollout_members (
    rollout_id uuid NOT NULL,
    member_role text NOT NULL,
    manager_id text NOT NULL,
    observed_policy_generation bigint,
    observed_policy_digest text,
    process_state text,
    admission_state text,
    last_acknowledged_at timestamp with time zone,
    host_action_outcome text,
    error text,
    CONSTRAINT wr_manager_rollout_members_member_role_check CHECK ((member_role = ANY (ARRAY['source'::text, 'target'::text])))
);

CREATE TABLE wr_manager_rollouts (
    rollout_id uuid NOT NULL,
    deployment_principal_uri text NOT NULL,
    client_operation_id uuid NOT NULL,
    canonical_request_digest text NOT NULL,
    canonical_request bytea NOT NULL,
    phase integer DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    target_policy_validator_version integer NOT NULL,
    target_deployment_principal_uri text NOT NULL,
    target_deployment_leaf_fingerprint text NOT NULL,
    expected_target_set_hash text NOT NULL,
    cluster_id text NOT NULL,
    target_generation bigint NOT NULL,
    target_policy_digest text NOT NULL,
    failure text,
    reset_request_digest text,
    reset_evidence_digest text,
    reset_policy_generation bigint,
    reset_policy_digest text,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT wr_manager_rollouts_reset_receipt_check CHECK ((reset_request_digest IS NULL) = (reset_evidence_digest IS NULL) AND (reset_request_digest IS NULL) = (reset_policy_generation IS NULL) AND (reset_request_digest IS NULL) = (reset_policy_digest IS NULL)),
    CONSTRAINT wr_manager_rollouts_reset_generation_check CHECK (reset_policy_generation IS NULL OR reset_policy_generation > 0),
    CONSTRAINT wr_manager_rollouts_phase_check CHECK (((phase >= 1) AND (phase <= 10))),
    CONSTRAINT wr_manager_rollouts_target_generation_check CHECK ((target_generation > 0))
);

CREATE TABLE wr_managers (
    manager_id text NOT NULL,
    grpc_address text NOT NULL,
    registered_at timestamp with time zone DEFAULT now() NOT NULL,
    last_heartbeat timestamp with time zone DEFAULT now() NOT NULL,
    policy_generation bigint,
    policy_digest text,
    admission_state text DEFAULT 'CLOSED_STARTUP'::text NOT NULL,
    rollout_id uuid,
    rollout_phase integer
);

CREATE TABLE wr_module_heartbeats (
    engine_id text NOT NULL,
    namespace text NOT NULL,
    module_name text NOT NULL,
    version text NOT NULL,
    last_healthy timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE wr_node_agent_attestations (
    node_id text NOT NULL,
    agent_instance_id text NOT NULL,
    authenticated_principal text NOT NULL,
    protocol_version text NOT NULL,
    binary_digest text NOT NULL,
    backend text NOT NULL,
    capabilities text[] DEFAULT '{}'::text[] NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    CONSTRAINT wr_node_agent_attestations_agent_instance_id_check CHECK ((agent_instance_id <> ''::text)),
    CONSTRAINT wr_node_agent_attestations_authenticated_principal_check CHECK ((authenticated_principal <> ''::text)),
    CONSTRAINT wr_node_agent_attestations_backend_check CHECK ((backend = ANY (ARRAY['systemd'::text, 'docker'::text]))),
    CONSTRAINT wr_node_agent_attestations_binary_digest_check CHECK ((binary_digest <> ''::text)),
    CONSTRAINT wr_node_agent_attestations_protocol_version_check CHECK ((protocol_version <> ''::text))
);

CREATE TABLE wr_node_agent_policies (
    node_id text NOT NULL,
    protocol_version text NOT NULL,
    backend text NOT NULL,
    retention_count integer NOT NULL,
    actor text NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    binary_digest text DEFAULT ''::text NOT NULL,
    capabilities text[] DEFAULT '{}'::text[] NOT NULL,
    CONSTRAINT wr_node_agent_policies_actor_check CHECK ((actor <> ''::text)),
    CONSTRAINT wr_node_agent_policies_backend_check CHECK ((backend = ANY (ARRAY['systemd'::text, 'docker'::text]))),
    CONSTRAINT wr_node_agent_policies_protocol_version_check CHECK ((protocol_version <> ''::text)),
    CONSTRAINT wr_node_agent_policies_retention_count_check CHECK ((retention_count >= 1))
);

CREATE TABLE wr_node_deployments (
    node_id text NOT NULL,
    revision bigint NOT NULL,
    attempt_token text NOT NULL,
    bundle_digest text NOT NULL,
    expected_inventory bytea NOT NULL,
    state text NOT NULL,
    failure_detail text DEFAULT ''::text NOT NULL,
    source_revision bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    activated_at timestamp with time zone,
    completed_at timestamp with time zone,
    allocation_actor text,
    abandoned_at timestamp with time zone,
    abandoned_by text,
    operation_id uuid,
    resolved_release_digest text DEFAULT ''::text NOT NULL,
    finalized_at timestamp with time zone,
    finalized_by text,
    allocated_by text NOT NULL,
    inventory_schema_version integer NOT NULL,
    revision_digest text NOT NULL,
    CONSTRAINT wr_node_deployments_abandonment_check CHECK ((((abandoned_at IS NULL) AND (abandoned_by IS NULL)) OR ((abandoned_at IS NOT NULL) AND (abandoned_by IS NOT NULL) AND (operation_id IS NULL)))),
    CONSTRAINT wr_node_deployments_allocated_by_not_empty CHECK ((allocated_by <> ''::text)),
    CONSTRAINT wr_node_deployments_finalization_check CHECK ((((resolved_release_digest = ''::text) AND (finalized_at IS NULL) AND (finalized_by IS NULL)) OR ((resolved_release_digest <> ''::text) AND (finalized_at IS NOT NULL) AND (finalized_by IS NOT NULL)))),
    CONSTRAINT wr_node_deployments_inventory_schema_version_check CHECK ((inventory_schema_version = 1)),
    CONSTRAINT wr_node_deployments_revision_check CHECK ((revision > 0)),
    CONSTRAINT wr_node_deployments_source_revision_check CHECK ((source_revision >= 0)),
    CONSTRAINT wr_node_deployments_state_check CHECK ((state = ANY (ARRAY['pending'::text, 'active'::text, 'succeeded'::text, 'failed'::text])))
);

CREATE TABLE wr_node_operation_engine_target_details (
    operation_id uuid NOT NULL,
    target_kind text DEFAULT 'engine_slot'::text NOT NULL,
    target_key text NOT NULL,
    rollout_order integer NOT NULL,
    authoritative_revision bigint DEFAULT 0 CONSTRAINT wr_node_operation_engine_target_authoritative_revision_not_null NOT NULL,
    authority_switched boolean DEFAULT false CONSTRAINT wr_node_operation_engine_target_det_authority_switched_not_null NOT NULL,
    serving_converged boolean DEFAULT false CONSTRAINT wr_node_operation_engine_target_deta_serving_converged_not_null NOT NULL,
    transition_kind text CONSTRAINT wr_node_operation_engine_target_detail_transition_kind_not_null NOT NULL,
    CONSTRAINT wr_node_operation_engine_target_de_authoritative_revision_check CHECK ((authoritative_revision >= 0)),
    CONSTRAINT wr_node_operation_engine_target_details_rollout_order_check CHECK ((rollout_order >= 0)),
    CONSTRAINT wr_node_operation_engine_target_details_target_kind_check CHECK ((target_kind = 'engine_slot'::text)),
    CONSTRAINT wr_node_operation_engine_transition_kind_check CHECK ((transition_kind = ANY (ARRAY['addition'::text, 'replacement'::text, 'unchanged'::text, 'removal'::text, 'restart'::text])))
);

CREATE TABLE wr_node_operation_events (
    sequence bigint NOT NULL,
    operation_id uuid NOT NULL,
    actor text NOT NULL,
    event_code text NOT NULL,
    detail text DEFAULT ''::text NOT NULL,
    lease_epoch bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT wr_node_operation_events_lease_epoch_check CHECK ((lease_epoch >= 0))
);

CREATE SEQUENCE wr_node_operation_events_sequence_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE wr_node_operation_events_sequence_seq OWNED BY wr_node_operation_events.sequence;

CREATE TABLE wr_node_operation_result_receipts (
    operation_id uuid NOT NULL,
    node_id text NOT NULL,
    agent_instance_id text NOT NULL,
    lease_epoch bigint NOT NULL,
    step integer NOT NULL,
    authenticated_principal text CONSTRAINT wr_node_operation_result_recei_authenticated_principal_not_null NOT NULL,
    result_payload bytea NOT NULL,
    accepted_at timestamp with time zone DEFAULT now() NOT NULL,
    target_kind text NOT NULL,
    target_key text NOT NULL,
    CONSTRAINT wr_node_operation_result_receipts_agent_instance_id_check CHECK ((agent_instance_id <> ''::text)),
    CONSTRAINT wr_node_operation_result_receipts_authenticated_principal_check CHECK ((authenticated_principal <> ''::text)),
    CONSTRAINT wr_node_operation_result_receipts_lease_epoch_check CHECK ((lease_epoch > 0)),
    CONSTRAINT wr_node_operation_result_receipts_node_id_check CHECK ((node_id <> ''::text)),
    CONSTRAINT wr_node_operation_result_receipts_target_kind_check CHECK ((target_kind = ANY (ARRAY['proxy'::text, 'engine_slot'::text])))
);

CREATE TABLE wr_node_operation_targets (
    operation_id uuid NOT NULL,
    node_id text NOT NULL,
    target_kind text NOT NULL,
    target_key text NOT NULL,
    next_step text NOT NULL,
    completed_steps integer DEFAULT 0 NOT NULL,
    complete boolean DEFAULT false NOT NULL,
    condition_code text DEFAULT ''::text NOT NULL,
    condition_detail text DEFAULT ''::text NOT NULL,
    source_revision bigint DEFAULT 0 NOT NULL,
    source_digest text DEFAULT ''::text NOT NULL,
    source_resolved_digest text DEFAULT ''::text NOT NULL,
    target_revision bigint DEFAULT 0 NOT NULL,
    target_digest text DEFAULT ''::text NOT NULL,
    target_resolved_digest text DEFAULT ''::text NOT NULL,
    pinned_backend_instance_id text DEFAULT ''::text NOT NULL,
    pinned_process_instance_id text DEFAULT ''::text NOT NULL,
    changed boolean DEFAULT false NOT NULL,
    effect_ambiguous boolean DEFAULT false NOT NULL,
    effect_delivered_at timestamp with time zone,
    effect_reported boolean DEFAULT false NOT NULL,
    effect_observed_revision bigint DEFAULT 0 NOT NULL,
    effect_observed_digest text DEFAULT ''::text NOT NULL,
    effect_observed_resolved_digest text DEFAULT ''::text CONSTRAINT wr_node_operation_targets_effect_observed_resolved_dig_not_null NOT NULL,
    effect_backend_instance_id text DEFAULT ''::text NOT NULL,
    effect_process_instance_id text DEFAULT ''::text NOT NULL,
    effect_condition_code text DEFAULT ''::text NOT NULL,
    effect_detail text DEFAULT ''::text NOT NULL,
    effect_termination_evidence bytea,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT wr_node_operation_targets_check CHECK ((((target_kind = 'proxy'::text) AND (target_key = 'proxy'::text)) OR ((target_kind = 'engine_slot'::text) AND (target_key <> ''::text) AND (target_key <> 'proxy'::text)))),
    CONSTRAINT wr_node_operation_targets_check1 CHECK ((((target_kind = 'proxy'::text) AND (next_step = ANY (ARRAY['inspect_backend'::text, 'verify_target'::text, 'verify_proxy'::text, 'stop_backend'::text, 'select_release'::text, 'start_backend'::text, 'restore_source'::text, 'complete'::text]))) OR ((target_kind = 'engine_slot'::text) AND (next_step = ANY (ARRAY['verify_release_metadata'::text, 'stop_backend'::text, 'select_release'::text, 'start_backend'::text, 'verify_target'::text, 'switch_authority'::text, 'verify_serving'::text, 'restore_source'::text, 'complete'::text]))))),
    CONSTRAINT wr_node_operation_targets_check2 CHECK (((next_step = 'complete'::text) = complete)),
    CONSTRAINT wr_node_operation_targets_completed_steps_check CHECK ((completed_steps >= 0)),
    CONSTRAINT wr_node_operation_targets_effect_observed_revision_check CHECK ((effect_observed_revision >= 0)),
    CONSTRAINT wr_node_operation_targets_source_revision_check CHECK ((source_revision >= 0)),
    CONSTRAINT wr_node_operation_targets_target_kind_check CHECK ((target_kind = ANY (ARRAY['proxy'::text, 'engine_slot'::text]))),
    CONSTRAINT wr_node_operation_targets_target_revision_check CHECK ((target_revision >= 0))
);

CREATE TABLE wr_node_operations (
    operation_id uuid NOT NULL,
    node_id text NOT NULL,
    request_token text NOT NULL,
    actor text NOT NULL,
    action text NOT NULL,
    state text NOT NULL,
    request_payload bytea NOT NULL,
    policy bytea NOT NULL,
    source_revision bigint DEFAULT 0 NOT NULL,
    target_revision bigint DEFAULT 0 NOT NULL,
    bundle_digest text DEFAULT ''::text NOT NULL,
    committed boolean DEFAULT false NOT NULL,
    lease_epoch bigint DEFAULT 0 NOT NULL,
    lease_expires_at timestamp with time zone,
    claimed_by text,
    failure_code text DEFAULT ''::text NOT NULL,
    failure_detail text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    forward_deadline timestamp with time zone NOT NULL,
    phase text DEFAULT 'forward'::text NOT NULL,
    forward_fenced boolean DEFAULT false NOT NULL,
    restoration_requested boolean DEFAULT false NOT NULL,
    restoration_terminal_state text,
    agent_instance_id text,
    committed_at timestamp with time zone,
    resolved_release_digest text DEFAULT ''::text NOT NULL,
    target_revision_digest text,
    CONSTRAINT wr_node_operations_action_check CHECK ((action = ANY (ARRAY['deployment'::text, 'rollback'::text, 'restart'::text]))),
    CONSTRAINT wr_node_operations_commit_phase_check CHECK ((((NOT committed) AND (committed_at IS NULL)) OR (committed AND (committed_at IS NOT NULL) AND (phase = 'complete'::text)))),
    CONSTRAINT wr_node_operations_forward_deadline_check CHECK ((forward_deadline >= created_at)),
    CONSTRAINT wr_node_operations_lease_epoch_check CHECK ((lease_epoch >= 0)),
    CONSTRAINT wr_node_operations_lease_owner_check CHECK ((((lease_expires_at IS NULL) AND (claimed_by IS NULL) AND (agent_instance_id IS NULL)) OR ((lease_expires_at IS NOT NULL) AND (claimed_by IS NOT NULL) AND (agent_instance_id IS NOT NULL) AND (agent_instance_id <> ''::text)))),
    CONSTRAINT wr_node_operations_phase_check CHECK ((phase = ANY (ARRAY['forward'::text, 'restoring_source'::text, 'committing'::text, 'complete'::text]))),
    CONSTRAINT wr_node_operations_phase_state_check CHECK ((((phase = 'complete'::text) AND (state = ANY (ARRAY['succeeded'::text, 'failed'::text, 'cancelled'::text]))) OR ((phase = ANY (ARRAY['forward'::text, 'restoring_source'::text, 'committing'::text])) AND (state = ANY (ARRAY['queued'::text, 'running'::text, 'paused'::text]))))),
    CONSTRAINT wr_node_operations_restoration_check CHECK ((((phase = 'restoring_source'::text) AND restoration_requested AND forward_fenced AND (restoration_terminal_state IS NOT NULL)) OR (phase <> 'restoring_source'::text))),
    CONSTRAINT wr_node_operations_restoration_terminal_check CHECK (((restoration_terminal_state IS NULL) OR (restoration_terminal_state = ANY (ARRAY['failed'::text, 'cancelled'::text])))),
    CONSTRAINT wr_node_operations_source_revision_check CHECK ((source_revision >= 0)),
    CONSTRAINT wr_node_operations_state_check CHECK ((state = ANY (ARRAY['queued'::text, 'running'::text, 'paused'::text, 'succeeded'::text, 'failed'::text, 'cancelled'::text]))),
    CONSTRAINT wr_node_operations_target_digest CHECK (((target_revision = 0) OR (target_revision_digest IS NOT NULL))),
    CONSTRAINT wr_node_operations_target_revision_check CHECK ((target_revision >= 0))
);

CREATE TABLE wr_node_release_cleanup (
    node_id text NOT NULL,
    generation bigint DEFAULT 1 NOT NULL,
    state text DEFAULT 'needs_reconcile'::text NOT NULL,
    protection_fingerprint text DEFAULT ''::text NOT NULL,
    authority_payload bytea,
    payload_digest text DEFAULT ''::text NOT NULL,
    known_inventory bytea,
    candidate_count integer DEFAULT 0 NOT NULL,
    agent_instance_id text,
    claimed_by text,
    claim_instance uuid,
    lease_epoch bigint DEFAULT 0 NOT NULL,
    lease_expires_at timestamp with time zone,
    delivered_at timestamp with time zone,
    last_reconciled_at timestamp with time zone,
    next_reconcile_at timestamp with time zone,
    last_attempt_at timestamp with time zone,
    diagnostic_code text DEFAULT ''::text NOT NULL,
    diagnostic_detail text DEFAULT ''::text NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT wr_node_release_cleanup_candidate_count_check CHECK ((candidate_count >= 0)),
    CONSTRAINT wr_node_release_cleanup_check CHECK (((state = 'claimed'::text) = (agent_instance_id IS NOT NULL))),
    CONSTRAINT wr_node_release_cleanup_check1 CHECK (((state = 'claimed'::text) = (claimed_by IS NOT NULL))),
    CONSTRAINT wr_node_release_cleanup_check2 CHECK (((state = 'claimed'::text) = (claim_instance IS NOT NULL))),
    CONSTRAINT wr_node_release_cleanup_check3 CHECK (((state = 'claimed'::text) = (lease_expires_at IS NOT NULL))),
    CONSTRAINT wr_node_release_cleanup_check4 CHECK (((authority_payload IS NULL) = (payload_digest = ''::text))),
    CONSTRAINT wr_node_release_cleanup_check5 CHECK (((state <> ALL (ARRAY['pending'::text, 'claimed'::text])) OR (authority_payload IS NOT NULL))),
    CONSTRAINT wr_node_release_cleanup_generation_check CHECK ((generation > 0)),
    CONSTRAINT wr_node_release_cleanup_lease_epoch_check CHECK ((lease_epoch >= 0)),
    CONSTRAINT wr_node_release_cleanup_state_check CHECK ((state = ANY (ARRAY['clean'::text, 'needs_reconcile'::text, 'pending'::text, 'claimed'::text, 'paused'::text])))
);

CREATE TABLE wr_node_release_cleanup_events (
    node_id text NOT NULL,
    generation bigint NOT NULL,
    sequence bigint NOT NULL,
    event_code text NOT NULL,
    detail text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT wr_node_release_cleanup_events_event_code_check CHECK ((event_code <> ''::text)),
    CONSTRAINT wr_node_release_cleanup_events_generation_check CHECK ((generation > 0))
);

CREATE SEQUENCE wr_node_release_cleanup_events_sequence_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE wr_node_release_cleanup_events_sequence_seq OWNED BY wr_node_release_cleanup_events.sequence;

CREATE TABLE wr_node_release_cleanup_generations (
    node_id text NOT NULL,
    generation bigint NOT NULL,
    protection_fingerprint text CONSTRAINT wr_node_release_cleanup_generat_protection_fingerprint_not_null NOT NULL,
    authority_payload bytea NOT NULL,
    payload_digest text NOT NULL,
    known_inventory bytea,
    outcome text DEFAULT 'materialized'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    CONSTRAINT wr_node_release_cleanup_generations_generation_check CHECK ((generation > 0)),
    CONSTRAINT wr_node_release_cleanup_generations_outcome_check CHECK ((outcome = ANY (ARRAY['materialized'::text, 'superseded'::text, 'succeeded'::text, 'paused'::text]))),
    CONSTRAINT wr_node_release_cleanup_generations_payload_digest_check CHECK ((payload_digest <> ''::text))
);

CREATE TABLE wr_node_release_cleanup_result_receipts (
    node_id text NOT NULL,
    generation bigint NOT NULL,
    agent_instance_id text CONSTRAINT wr_node_release_cleanup_result_recei_agent_instance_id_not_null NOT NULL,
    lease_epoch bigint NOT NULL,
    claim_instance uuid NOT NULL,
    authenticated_principal text CONSTRAINT wr_node_release_cleanup_result_authenticated_principal_not_null NOT NULL,
    payload_digest text NOT NULL,
    result_payload bytea NOT NULL,
    accepted_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT wr_node_release_cleanup_result_re_authenticated_principal_check CHECK ((authenticated_principal <> ''::text)),
    CONSTRAINT wr_node_release_cleanup_result_receipts_agent_instance_id_check CHECK ((agent_instance_id <> ''::text)),
    CONSTRAINT wr_node_release_cleanup_result_receipts_lease_epoch_check CHECK ((lease_epoch > 0)),
    CONSTRAINT wr_node_release_cleanup_result_receipts_payload_digest_check CHECK ((payload_digest <> ''::text))
);

CREATE TABLE wr_node_release_deletions (
    node_id text NOT NULL,
    revision bigint NOT NULL,
    bundle_digest text NOT NULL,
    operation_id uuid,
    deleted_at timestamp with time zone DEFAULT now() NOT NULL,
    resolved_release_digest text DEFAULT ''::text NOT NULL,
    cleanup_generation bigint,
    CONSTRAINT wr_node_release_deletions_bundle_digest_check CHECK ((bundle_digest <> ''::text)),
    CONSTRAINT wr_node_release_deletions_exactly_one_provenance CHECK (((operation_id IS NOT NULL) <> (cleanup_generation IS NOT NULL))),
    CONSTRAINT wr_node_release_deletions_revision_check CHECK ((revision > 0))
);

CREATE TABLE wr_node_slot_authority (
    node_id text NOT NULL,
    engine_slot text NOT NULL,
    revision bigint NOT NULL,
    authoritative boolean DEFAULT false NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    resolved_release_digest text DEFAULT ''::text NOT NULL,
    CONSTRAINT wr_node_slot_authority_revision_check CHECK ((revision > 0))
);

CREATE TABLE wr_node_slot_observations (
    node_id text NOT NULL,
    engine_slot text NOT NULL,
    lifecycle_status bytea,
    backend_state text NOT NULL,
    backend_instance_id text DEFAULT ''::text NOT NULL,
    observed_revision bigint DEFAULT 0 NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    observed_digest text DEFAULT ''::text NOT NULL,
    backend_query_error text DEFAULT ''::text NOT NULL,
    operation_id uuid,
    agent_instance_id text DEFAULT ''::text NOT NULL,
    lease_epoch bigint DEFAULT 0 NOT NULL,
    observed_resolved_digest text DEFAULT ''::text NOT NULL,
    CONSTRAINT wr_node_slot_observations_backend_state_check CHECK ((backend_state = ANY (ARRAY['unknown'::text, 'running'::text, 'exited'::text, 'query_error'::text]))),
    CONSTRAINT wr_node_slot_observations_lease_epoch_check CHECK ((lease_epoch >= 0)),
    CONSTRAINT wr_node_slot_observations_observed_revision_check CHECK ((observed_revision >= 0)),
    CONSTRAINT wr_node_slot_observations_query_error_check CHECK ((((backend_state = 'query_error'::text) AND (backend_query_error <> ''::text)) OR ((backend_state <> 'query_error'::text) AND (backend_query_error = ''::text))))
);

CREATE TABLE wr_node_slot_owners (
    node_id text NOT NULL,
    engine_slot text NOT NULL,
    operation_id uuid,
    revision bigint,
    revision_digest text,
    activation_id uuid,
    engine_id text,
    slot_generation bytea NOT NULL,
    route_authority boolean DEFAULT false NOT NULL,
    lifecycle_authority boolean DEFAULT false NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT wr_node_slot_owners_check CHECK (((operation_id IS NULL) = (activation_id IS NULL))),
    CONSTRAINT wr_node_slot_owners_check1 CHECK (((revision IS NULL) = (revision_digest IS NULL))),
    CONSTRAINT wr_node_slot_owners_revision_check CHECK (((revision IS NULL) OR (revision > 0))),
    CONSTRAINT wr_node_slot_owners_slot_generation_check CHECK ((octet_length(slot_generation) = 8))
);

CREATE TABLE wr_nodes (
    node_id text NOT NULL,
    current_revision bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    target_revision bigint,
    CONSTRAINT wr_nodes_current_revision_check CHECK ((current_revision >= 0)),
    CONSTRAINT wr_nodes_target_revision_check CHECK ((target_revision > 0))
);

CREATE TABLE wr_proxy_inventory (
    proxy_id text NOT NULL,
    node_id text NOT NULL,
    process_instance_id text NOT NULL,
    deployment_revision bigint,
    bundle_digest text,
    operation_id uuid,
    revision_digest text,
    registration bytea NOT NULL,
    report bytea,
    registered_at timestamp with time zone DEFAULT statement_timestamp() NOT NULL,
    report_received_at timestamp with time zone,
    receiving_manager_id text,
    deregistered_at timestamp with time zone,
    CONSTRAINT wr_proxy_inventory_managed_identity_complete CHECK ((((deployment_revision IS NULL) AND (bundle_digest IS NULL) AND (operation_id IS NULL) AND (revision_digest IS NULL)) OR ((deployment_revision > 0) AND (bundle_digest IS NOT NULL) AND (operation_id IS NOT NULL) AND (revision_digest IS NOT NULL)))),
    CONSTRAINT wr_proxy_inventory_process_instance_id_length CHECK (((octet_length(process_instance_id) >= 1) AND (octet_length(process_instance_id) <= 255))),
    CONSTRAINT wr_proxy_inventory_report_receipt_complete CHECK ((((report IS NULL) AND (report_received_at IS NULL) AND (receiving_manager_id IS NULL)) OR ((report IS NOT NULL) AND (report_received_at IS NOT NULL) AND (receiving_manager_id IS NOT NULL))))
);

CREATE TABLE wr_routing_rules (
    rule_id text NOT NULL,
    source_namespace text DEFAULT ''::text NOT NULL,
    source_module text DEFAULT ''::text NOT NULL,
    destination_namespace text DEFAULT ''::text NOT NULL,
    destination_module text DEFAULT ''::text NOT NULL,
    destination_version text DEFAULT ''::text NOT NULL,
    engine_id text NOT NULL,
    engine_address text DEFAULT ''::text NOT NULL,
    healthy boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    peer_address text DEFAULT ''::text NOT NULL,
    CONSTRAINT wr_routing_rules_peer_address_not_empty CHECK ((peer_address <> ''::text))
);

CREATE TABLE wr_schedules (
    schedule_id text DEFAULT (gen_random_uuid())::text NOT NULL,
    worker_namespace text NOT NULL,
    worker_name text NOT NULL,
    worker_version text NOT NULL,
    job_type text NOT NULL,
    interval_secs integer NOT NULL,
    immediate boolean DEFAULT false NOT NULL,
    payload bytea DEFAULT '\x'::bytea NOT NULL,
    timeout_secs integer DEFAULT 300 NOT NULL,
    max_attempts integer DEFAULT 3 NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    last_fired_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    next_fire_at timestamp with time zone,
    claimed_by text,
    claimed_until timestamp with time zone,
    claim_id uuid,
    last_attempt_at timestamp with time zone,
    last_error text,
    consecutive_failures integer DEFAULT 0 NOT NULL,
    CONSTRAINT wr_schedules_consecutive_failures_nonnegative CHECK ((consecutive_failures >= 0)),
    CONSTRAINT wr_schedules_interval_secs_check CHECK ((interval_secs > 0)),
    CONSTRAINT wr_schedules_max_attempts_positive CHECK ((max_attempts > 0)),
    CONSTRAINT wr_schedules_timeout_secs_positive CHECK ((timeout_secs > 0))
);

CREATE TABLE wr_schemas (
    namespace text NOT NULL,
    module_name text NOT NULL,
    version text NOT NULL,
    proto_schema bytea NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE wr_secrets (
    namespace text NOT NULL,
    key text NOT NULL,
    ciphertext bytea NOT NULL,
    nonce bytea NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

ALTER TABLE ONLY wr_manager_rollout_events ALTER COLUMN sequence SET DEFAULT nextval('wr_manager_rollout_events_sequence_seq'::regclass);

ALTER TABLE ONLY wr_node_operation_events ALTER COLUMN sequence SET DEFAULT nextval('wr_node_operation_events_sequence_seq'::regclass);

ALTER TABLE ONLY wr_node_release_cleanup_events ALTER COLUMN sequence SET DEFAULT nextval('wr_node_release_cleanup_events_sequence_seq'::regclass);

ALTER TABLE ONLY wr_engines
    ADD CONSTRAINT wr_engines_pkey PRIMARY KEY (engine_id);

ALTER TABLE ONLY wr_manager_lock
    ADD CONSTRAINT wr_manager_lock_pkey PRIMARY KEY (id);

ALTER TABLE ONLY wr_manager_rollout_events
    ADD CONSTRAINT wr_manager_rollout_events_pkey PRIMARY KEY (rollout_id, sequence);

ALTER TABLE ONLY wr_manager_rollout_guard
    ADD CONSTRAINT wr_manager_rollout_guard_pkey PRIMARY KEY (singleton);

ALTER TABLE ONLY wr_manager_rollout_members
    ADD CONSTRAINT wr_manager_rollout_members_pkey PRIMARY KEY (rollout_id, member_role, manager_id);

ALTER TABLE ONLY wr_manager_rollouts
    ADD CONSTRAINT wr_manager_rollouts_deployment_principal_uri_client_operati_key UNIQUE (deployment_principal_uri, client_operation_id);

ALTER TABLE ONLY wr_manager_rollouts
    ADD CONSTRAINT wr_manager_rollouts_pkey PRIMARY KEY (rollout_id);

ALTER TABLE ONLY wr_managers
    ADD CONSTRAINT wr_managers_pkey PRIMARY KEY (manager_id);

ALTER TABLE ONLY wr_module_heartbeats
    ADD CONSTRAINT wr_module_heartbeats_pkey PRIMARY KEY (engine_id, namespace, module_name, version);

ALTER TABLE ONLY wr_node_agent_attestations
    ADD CONSTRAINT wr_node_agent_attestations_pkey PRIMARY KEY (node_id, agent_instance_id);

ALTER TABLE ONLY wr_node_agent_policies
    ADD CONSTRAINT wr_node_agent_policies_pkey PRIMARY KEY (node_id);

ALTER TABLE ONLY wr_node_deployments
    ADD CONSTRAINT wr_node_deployments_node_id_attempt_token_key UNIQUE (node_id, attempt_token);

ALTER TABLE ONLY wr_node_deployments
    ADD CONSTRAINT wr_node_deployments_pkey PRIMARY KEY (node_id, revision);

ALTER TABLE ONLY wr_node_operation_engine_target_details
    ADD CONSTRAINT wr_node_operation_engine_target__operation_id_rollout_order_key UNIQUE (operation_id, rollout_order);

ALTER TABLE ONLY wr_node_operation_engine_target_details
    ADD CONSTRAINT wr_node_operation_engine_target_details_pkey PRIMARY KEY (operation_id, target_kind, target_key);

ALTER TABLE ONLY wr_node_operation_events
    ADD CONSTRAINT wr_node_operation_events_pkey PRIMARY KEY (sequence);

ALTER TABLE ONLY wr_node_operation_result_receipts
    ADD CONSTRAINT wr_node_operation_result_receipts_pkey PRIMARY KEY (operation_id, node_id, agent_instance_id, lease_epoch, step, target_kind, target_key);

ALTER TABLE ONLY wr_node_operation_targets
    ADD CONSTRAINT wr_node_operation_targets_operation_id_node_id_target_kind__key UNIQUE (operation_id, node_id, target_kind, target_key);

ALTER TABLE ONLY wr_node_operation_targets
    ADD CONSTRAINT wr_node_operation_targets_pkey PRIMARY KEY (operation_id, target_kind, target_key);

ALTER TABLE ONLY wr_node_operations
    ADD CONSTRAINT wr_node_operations_actor_request_token_key UNIQUE (actor, request_token);

ALTER TABLE ONLY wr_node_operations
    ADD CONSTRAINT wr_node_operations_pkey PRIMARY KEY (operation_id);

ALTER TABLE ONLY wr_node_release_cleanup_events
    ADD CONSTRAINT wr_node_release_cleanup_events_pkey PRIMARY KEY (node_id, sequence);

ALTER TABLE ONLY wr_node_release_cleanup_generations
    ADD CONSTRAINT wr_node_release_cleanup_gener_node_id_generation_payload_di_key UNIQUE (node_id, generation, payload_digest);

ALTER TABLE ONLY wr_node_release_cleanup_generations
    ADD CONSTRAINT wr_node_release_cleanup_generations_pkey PRIMARY KEY (node_id, generation);

ALTER TABLE ONLY wr_node_release_cleanup
    ADD CONSTRAINT wr_node_release_cleanup_node_id_generation_key UNIQUE (node_id, generation);

ALTER TABLE ONLY wr_node_release_cleanup
    ADD CONSTRAINT wr_node_release_cleanup_pkey PRIMARY KEY (node_id);

ALTER TABLE ONLY wr_node_release_cleanup_result_receipts
    ADD CONSTRAINT wr_node_release_cleanup_result_receipts_pkey PRIMARY KEY (node_id, generation, agent_instance_id, lease_epoch, claim_instance);

ALTER TABLE ONLY wr_node_release_deletions
    ADD CONSTRAINT wr_node_release_deletions_pkey PRIMARY KEY (node_id, revision);

ALTER TABLE ONLY wr_node_slot_authority
    ADD CONSTRAINT wr_node_slot_authority_pkey PRIMARY KEY (node_id, engine_slot, revision);

ALTER TABLE ONLY wr_node_slot_observations
    ADD CONSTRAINT wr_node_slot_observations_pkey PRIMARY KEY (node_id, engine_slot);

ALTER TABLE ONLY wr_node_slot_owners
    ADD CONSTRAINT wr_node_slot_owners_pkey PRIMARY KEY (node_id, engine_slot);

ALTER TABLE ONLY wr_nodes
    ADD CONSTRAINT wr_nodes_pkey PRIMARY KEY (node_id);

ALTER TABLE ONLY wr_proxy_inventory
    ADD CONSTRAINT wr_proxy_inventory_pkey PRIMARY KEY (proxy_id, node_id, process_instance_id);

ALTER TABLE ONLY wr_routing_rules
    ADD CONSTRAINT wr_routing_rules_pkey PRIMARY KEY (rule_id);

ALTER TABLE ONLY wr_schedules
    ADD CONSTRAINT wr_schedules_pkey PRIMARY KEY (schedule_id);

ALTER TABLE ONLY wr_schedules
    ADD CONSTRAINT wr_schedules_worker_namespace_worker_name_worker_version_jo_key UNIQUE (worker_namespace, worker_name, worker_version, job_type);

ALTER TABLE ONLY wr_schemas
    ADD CONSTRAINT wr_schemas_pkey PRIMARY KEY (namespace, module_name, version);

ALTER TABLE ONLY wr_secrets
    ADD CONSTRAINT wr_secrets_pkey PRIMARY KEY (namespace, key);

CREATE INDEX idx_module_heartbeats_engine ON wr_module_heartbeats USING btree (engine_id);

CREATE INDEX idx_routing_rules_engine ON wr_routing_rules USING btree (engine_id);

CREATE INDEX idx_schedules_due ON wr_schedules USING btree (enabled, next_fire_at) WHERE (enabled = true);

CREATE INDEX idx_wr_engines_deployment_slot ON wr_engines USING btree (deployment_node_id, deployment_engine_slot, deployment_revision);

CREATE INDEX idx_wr_engines_job_admin_delegates ON wr_engines USING btree (job_queue_id, last_heartbeat DESC, engine_id) WHERE ((job_queue_id IS NOT NULL) AND (job_admin_address IS NOT NULL));

CREATE INDEX idx_wr_node_agent_attestations_fresh ON wr_node_agent_attestations USING btree (node_id, observed_at DESC);

CREATE INDEX idx_wr_node_deployments_history ON wr_node_deployments USING btree (node_id, revision DESC);

CREATE UNIQUE INDEX idx_wr_node_deployments_operation ON wr_node_deployments USING btree (operation_id) WHERE (operation_id IS NOT NULL);

CREATE INDEX idx_wr_node_deployments_success ON wr_node_deployments USING btree (node_id, revision DESC) WHERE (state = 'succeeded'::text);

CREATE INDEX idx_wr_node_operation_events_operation ON wr_node_operation_events USING btree (operation_id, sequence);

CREATE INDEX idx_wr_node_operations_history ON wr_node_operations USING btree (node_id, created_at DESC, operation_id);

CREATE UNIQUE INDEX idx_wr_node_operations_one_active ON wr_node_operations USING btree (node_id) WHERE ((state = ANY (ARRAY['queued'::text, 'running'::text, 'paused'::text])) AND (phase <> 'superseded'::text));

CREATE INDEX idx_wr_node_release_cleanup_due ON wr_node_release_cleanup USING btree (last_reconciled_at NULLS FIRST, node_id);

CREATE UNIQUE INDEX idx_wr_node_slot_one_authority ON wr_node_slot_authority USING btree (node_id, engine_slot) WHERE authoritative;

CREATE UNIQUE INDEX wr_node_operation_one_proxy_target ON wr_node_operation_targets USING btree (operation_id) WHERE (target_kind = 'proxy'::text);

CREATE INDEX wr_proxy_inventory_active_order ON wr_proxy_inventory USING btree (node_id, proxy_id, process_instance_id) WHERE (deregistered_at IS NULL);

CREATE INDEX wr_proxy_inventory_tombstone_retention ON wr_proxy_inventory USING btree (deregistered_at) WHERE (deregistered_at IS NOT NULL);

CREATE CONSTRAINT TRIGGER wr_node_operation_engine_detail_required AFTER INSERT OR DELETE OR UPDATE ON wr_node_operation_engine_target_details DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION wr_check_node_operation_engine_target_detail();

CREATE CONSTRAINT TRIGGER wr_node_operation_target_detail_required AFTER INSERT OR DELETE OR UPDATE ON wr_node_operation_targets DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION wr_check_node_operation_engine_target_detail();

ALTER TABLE ONLY wr_manager_rollout_events
    ADD CONSTRAINT wr_manager_rollout_events_rollout_id_fkey FOREIGN KEY (rollout_id) REFERENCES wr_manager_rollouts(rollout_id) ON DELETE CASCADE;

ALTER TABLE ONLY wr_manager_rollout_guard
    ADD CONSTRAINT wr_manager_rollout_guard_active_fk FOREIGN KEY (active_rollout_id) REFERENCES wr_manager_rollouts(rollout_id);

ALTER TABLE ONLY wr_manager_rollout_members
    ADD CONSTRAINT wr_manager_rollout_members_rollout_id_fkey FOREIGN KEY (rollout_id) REFERENCES wr_manager_rollouts(rollout_id) ON DELETE CASCADE;

ALTER TABLE ONLY wr_node_agent_attestations
    ADD CONSTRAINT wr_node_agent_attestations_node_id_fkey FOREIGN KEY (node_id) REFERENCES wr_nodes(node_id);

ALTER TABLE ONLY wr_node_agent_policies
    ADD CONSTRAINT wr_node_agent_policies_node_id_fkey FOREIGN KEY (node_id) REFERENCES wr_nodes(node_id);

ALTER TABLE ONLY wr_node_deployments
    ADD CONSTRAINT wr_node_deployments_node_id_fkey FOREIGN KEY (node_id) REFERENCES wr_nodes(node_id);

ALTER TABLE ONLY wr_node_deployments
    ADD CONSTRAINT wr_node_deployments_operation_id_fkey FOREIGN KEY (operation_id) REFERENCES wr_node_operations(operation_id);

ALTER TABLE ONLY wr_node_operation_engine_target_details
    ADD CONSTRAINT wr_node_operation_engine_targ_operation_id_target_kind_tar_fkey FOREIGN KEY (operation_id, target_kind, target_key) REFERENCES wr_node_operation_targets(operation_id, target_kind, target_key) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY wr_node_operation_events
    ADD CONSTRAINT wr_node_operation_events_operation_id_fkey FOREIGN KEY (operation_id) REFERENCES wr_node_operations(operation_id);

ALTER TABLE ONLY wr_node_operation_result_receipts
    ADD CONSTRAINT wr_node_operation_result_receipts_operation_id_fkey FOREIGN KEY (operation_id) REFERENCES wr_node_operations(operation_id);

ALTER TABLE ONLY wr_node_operation_result_receipts
    ADD CONSTRAINT wr_node_operation_result_receipts_target_fk FOREIGN KEY (operation_id, target_kind, target_key) REFERENCES wr_node_operation_targets(operation_id, target_kind, target_key);

ALTER TABLE ONLY wr_node_operation_targets
    ADD CONSTRAINT wr_node_operation_targets_node_id_fkey FOREIGN KEY (node_id) REFERENCES wr_nodes(node_id);

ALTER TABLE ONLY wr_node_operation_targets
    ADD CONSTRAINT wr_node_operation_targets_operation_id_fkey FOREIGN KEY (operation_id) REFERENCES wr_node_operations(operation_id) ON DELETE CASCADE;

ALTER TABLE ONLY wr_node_operations
    ADD CONSTRAINT wr_node_operations_node_id_fkey FOREIGN KEY (node_id) REFERENCES wr_nodes(node_id);

ALTER TABLE ONLY wr_node_release_cleanup_events
    ADD CONSTRAINT wr_node_release_cleanup_events_node_id_fkey FOREIGN KEY (node_id) REFERENCES wr_nodes(node_id);

ALTER TABLE ONLY wr_node_release_cleanup_generations
    ADD CONSTRAINT wr_node_release_cleanup_generations_node_id_fkey FOREIGN KEY (node_id) REFERENCES wr_nodes(node_id);

ALTER TABLE ONLY wr_node_release_cleanup
    ADD CONSTRAINT wr_node_release_cleanup_node_id_fkey FOREIGN KEY (node_id) REFERENCES wr_nodes(node_id);

ALTER TABLE ONLY wr_node_release_cleanup_result_receipts
    ADD CONSTRAINT wr_node_release_cleanup_result_receipts_node_id_generation_fkey FOREIGN KEY (node_id, generation) REFERENCES wr_node_release_cleanup_generations(node_id, generation);

ALTER TABLE ONLY wr_node_release_deletions
    ADD CONSTRAINT wr_node_release_deletions_cleanup_generation_fk FOREIGN KEY (node_id, cleanup_generation) REFERENCES wr_node_release_cleanup_generations(node_id, generation);

ALTER TABLE ONLY wr_node_release_deletions
    ADD CONSTRAINT wr_node_release_deletions_node_id_fkey FOREIGN KEY (node_id) REFERENCES wr_nodes(node_id);

ALTER TABLE ONLY wr_node_release_deletions
    ADD CONSTRAINT wr_node_release_deletions_operation_id_fkey FOREIGN KEY (operation_id) REFERENCES wr_node_operations(operation_id);

ALTER TABLE ONLY wr_node_slot_authority
    ADD CONSTRAINT wr_node_slot_authority_node_id_fkey FOREIGN KEY (node_id) REFERENCES wr_nodes(node_id);

ALTER TABLE ONLY wr_node_slot_authority
    ADD CONSTRAINT wr_node_slot_authority_node_id_revision_fkey FOREIGN KEY (node_id, revision) REFERENCES wr_node_deployments(node_id, revision);

ALTER TABLE ONLY wr_node_slot_observations
    ADD CONSTRAINT wr_node_slot_observations_node_id_fkey FOREIGN KEY (node_id) REFERENCES wr_nodes(node_id);

ALTER TABLE ONLY wr_node_slot_observations
    ADD CONSTRAINT wr_node_slot_observations_operation_id_fkey FOREIGN KEY (operation_id) REFERENCES wr_node_operations(operation_id);

ALTER TABLE ONLY wr_node_slot_owners
    ADD CONSTRAINT wr_node_slot_owners_node_id_fkey FOREIGN KEY (node_id) REFERENCES wr_nodes(node_id);

-- Operational singleton rows required on every fresh manager database.
INSERT INTO wr_manager_lock (id, version) VALUES (1, 0) ON CONFLICT DO NOTHING;
INSERT INTO wr_manager_rollout_guard (singleton) VALUES (TRUE) ON CONFLICT DO NOTHING;
