CREATE TABLE IF NOT EXISTS wr_managers (
    manager_id      TEXT PRIMARY KEY,
    grpc_address    TEXT NOT NULL,
    gossip_address  TEXT NOT NULL,
    registered_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_heartbeat  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
