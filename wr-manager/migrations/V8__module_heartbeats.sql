CREATE TABLE IF NOT EXISTS wr_module_heartbeats (
    engine_id     TEXT NOT NULL,
    namespace     TEXT NOT NULL,
    module_name   TEXT NOT NULL,
    version       TEXT NOT NULL,
    last_healthy  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (engine_id, namespace, module_name, version)
);
CREATE INDEX IF NOT EXISTS idx_module_heartbeats_engine
    ON wr_module_heartbeats (engine_id);
