CREATE TABLE tasks (
    task_id             TEXT PRIMARY KEY,
    repo_url            TEXT NOT NULL,
    "ref"               TEXT NOT NULL,
    doc_sources         JSONB NOT NULL DEFAULT '[]',
    task_description    TEXT NOT NULL,
    max_agent_turns     INTEGER NOT NULL DEFAULT 3,
    status              TEXT NOT NULL DEFAULT 'pending',
    session_id          TEXT,
    unified_diff        TEXT NOT NULL DEFAULT '',
    message             TEXT NOT NULL DEFAULT '',
    agent_turns         INTEGER NOT NULL DEFAULT 0,
    total_input_tokens  INTEGER NOT NULL DEFAULT 0,
    total_output_tokens INTEGER NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_tasks_status ON tasks (status);
CREATE INDEX idx_tasks_created ON tasks (created_at DESC);
