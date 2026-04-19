CREATE TABLE IF NOT EXISTS dag_runs (
    id TEXT PRIMARY KEY,
    schedule_id TEXT,
    trigger_source TEXT NOT NULL DEFAULT 'manual',
    started_at TEXT NOT NULL,
    ended_at TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    retry_of TEXT
);
