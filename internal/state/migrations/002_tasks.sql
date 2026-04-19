CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    dag_run_id TEXT NOT NULL REFERENCES dag_runs(id),
    pipeline TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempt INTEGER NOT NULL DEFAULT 1,
    exit_code INTEGER,
    started_at TEXT,
    ended_at TEXT
);
