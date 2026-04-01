-- Lock sentinel: single row holds the authoritative routing table version.
CREATE TABLE IF NOT EXISTS wr_manager_lock (
    id      INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    version BIGINT NOT NULL DEFAULT 0
);
INSERT INTO wr_manager_lock VALUES (1, 0) ON CONFLICT DO NOTHING;

-- Engines: full EngineRegistration serialised as protobuf BYTEA.
CREATE TABLE IF NOT EXISTS wr_engines (
    engine_id     TEXT PRIMARY KEY,
    address       TEXT NOT NULL,
    proxy_address TEXT NOT NULL,
    registration  BYTEA NOT NULL,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Routing rules: columns (not blob) so UPDATE SET healthy = false WHERE engine_id works.
CREATE TABLE IF NOT EXISTS wr_routing_rules (
    rule_id               TEXT PRIMARY KEY,
    source_namespace      TEXT NOT NULL DEFAULT '',
    source_module         TEXT NOT NULL DEFAULT '',
    destination_namespace TEXT NOT NULL DEFAULT '',
    destination_module    TEXT NOT NULL DEFAULT '',
    destination_version   TEXT NOT NULL DEFAULT '',
    engine_id             TEXT NOT NULL,
    engine_address        TEXT NOT NULL DEFAULT '',
    proxy_address         TEXT NOT NULL DEFAULT '',
    healthy               BOOL NOT NULL DEFAULT TRUE,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_routing_rules_engine ON wr_routing_rules (engine_id);

-- Schemas: keyed by (namespace, module, version).
CREATE TABLE IF NOT EXISTS wr_schemas (
    namespace    TEXT NOT NULL,
    module_name  TEXT NOT NULL,
    version      TEXT NOT NULL,
    proto_schema BYTEA NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (namespace, module_name, version)
);
