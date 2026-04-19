CREATE INDEX IF NOT EXISTS idx_tasks_run_status ON tasks(dag_run_id, status);
CREATE INDEX IF NOT EXISTS idx_tasks_run_pipeline ON tasks(dag_run_id, pipeline);
