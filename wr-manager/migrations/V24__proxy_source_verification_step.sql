-- Keep observation-only ambiguity inspection distinct from the explicit,
-- result-bearing source proxy verification step.
ALTER TABLE wr_node_operations
    DROP CONSTRAINT wr_node_operations_proxy_step_check,
    ADD CONSTRAINT wr_node_operations_proxy_step_check CHECK (proxy_next_step IN (
        'inspect_backend', 'stop_backend', 'select_release', 'start_backend',
        'verify_target', 'verify_proxy', 'restore_source', 'complete'
    ));
