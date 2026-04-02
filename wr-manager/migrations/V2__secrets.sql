CREATE TABLE IF NOT EXISTS wr_secrets (
    namespace   TEXT NOT NULL,
    key         TEXT NOT NULL,
    ciphertext  BYTEA NOT NULL,
    nonce       BYTEA NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (namespace, key)
);
