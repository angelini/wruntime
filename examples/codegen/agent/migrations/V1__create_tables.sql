CREATE TABLE sessions (
    session_id  TEXT PRIMARY KEY,
    status      TEXT NOT NULL DEFAULT 'active',
    latest_diff TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE conversation_turns (
    id              BIGSERIAL PRIMARY KEY,
    session_id      TEXT NOT NULL REFERENCES sessions(session_id),
    turn_number     INTEGER NOT NULL,
    user_prompt     TEXT NOT NULL,
    assistant_resp  TEXT NOT NULL,
    input_tokens    INTEGER NOT NULL DEFAULT 0,
    output_tokens   INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_turns_session ON conversation_turns (session_id, turn_number);

CREATE TABLE session_doc_prefixes (
    session_id TEXT NOT NULL REFERENCES sessions(session_id),
    doc_prefix TEXT NOT NULL,
    label      TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (session_id, doc_prefix)
);
