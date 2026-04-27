package state

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct {
	db *sql.DB

	stmtInsertRun  *sql.Stmt
	stmtInsertTask *sql.Stmt
	stmtUpdateTask *sql.Stmt
	stmtUpdateRun  *sql.Stmt
	stmtTasksByRun *sql.Stmt
	stmtRunByID    *sql.Stmt
}

type DAGRun struct {
	ID            string
	ScheduleID    string
	TriggerSource string
	StartedAt     string
	EndedAt       string
	Status        string
	RetryOf       string
}

type Task struct {
	ID       string
	DAGRunID string
	Pipeline string
	Status   string
	Attempt  int
	ExitCode *int
	StartedAt string
	EndedAt   string
}

func Open(dbPath string) (*Store, error) {
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating state directory: %w", err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	// Single connection serializes all SQLite operations, avoiding SQLITE_BUSY.
	// WAL mode ensures reads don't block the TUI polling in the future.
	db.SetMaxOpenConns(1)

	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("enabling WAL: %w", err)
	}

	if _, err := db.Exec("PRAGMA busy_timeout = 5000"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("setting busy timeout: %w", err)
	}

	if err := runMigrations(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("running migrations: %w", err)
	}

	s := &Store{db: db}
	if err := s.prepareStatements(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("preparing statements: %w", err)
	}

	if n, recoverErr := s.recoverOrphanedRuns(); recoverErr != nil {
		_ = db.Close()
		return nil, fmt.Errorf("crash recovery: %w", recoverErr)
	} else if n > 0 {
		fmt.Fprintf(os.Stderr, "warning: recovered %d orphaned task(s) from previous crash\n", n)
	}

	return s, nil
}

// recoverOrphanedRuns finds tasks stuck in "running" from a previous crash
// and marks them as "failed". Their parent dag_run is set to "partial_failure"
// if it was still marked "running".
func (s *Store) recoverOrphanedRuns() (int, error) {
	rows, err := s.db.Query(`SELECT id, dag_run_id FROM tasks WHERE status = 'running'`)
	if err != nil {
		return 0, err
	}
	defer func() { _ = rows.Close() }()

	type orphan struct{ taskID, dagRunID string }
	var orphans []orphan
	for rows.Next() {
		var o orphan
		if err := rows.Scan(&o.taskID, &o.dagRunID); err != nil {
			return 0, err
		}
		orphans = append(orphans, o)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	if len(orphans) == 0 {
		return 0, nil
	}

	now := time.Now().UTC().Format(time.RFC3339)
	ec := -1
	dagRuns := make(map[string]bool)

	for _, o := range orphans {
		if err := s.UpdateTask(o.taskID, "failed", &ec, "", now); err != nil {
			return 0, fmt.Errorf("recovering task %s: %w", o.taskID, err)
		}
		dagRuns[o.dagRunID] = true
	}

	for runID := range dagRuns {
		_ = s.UpdateDAGRun(runID, "partial_failure", now)
	}

	return len(orphans), nil
}

func (s *Store) Close() error {
	for _, stmt := range []*sql.Stmt{
		s.stmtInsertRun, s.stmtInsertTask, s.stmtUpdateTask,
		s.stmtUpdateRun, s.stmtTasksByRun, s.stmtRunByID,
	} {
		if stmt != nil {
			_ = stmt.Close()
		}
	}
	return s.db.Close()
}

func runMigrations(db *sql.DB) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (
		version INTEGER PRIMARY KEY,
		applied_at TEXT NOT NULL
	)`)
	if err != nil {
		return fmt.Errorf("creating migrations table: %w", err)
	}

	for i, filename := range migrationFiles {
		version := i + 1

		var count int
		if err := db.QueryRow("SELECT COUNT(*) FROM schema_migrations WHERE version = ?", version).Scan(&count); err != nil {
			return err
		}
		if count > 0 {
			continue
		}

		sqlBytes, err := migrationsFS.ReadFile("migrations/" + filename)
		if err != nil {
			return fmt.Errorf("reading migration %s: %w", filename, err)
		}

		if _, err := db.Exec(string(sqlBytes)); err != nil {
			return fmt.Errorf("applying migration %s: %w", filename, err)
		}

		if _, err := db.Exec("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
			version, time.Now().UTC().Format(time.RFC3339)); err != nil {
			return fmt.Errorf("recording migration %s: %w", filename, err)
		}
	}
	return nil
}

func (s *Store) prepareStatements() error {
	var err error

	s.stmtInsertRun, err = s.db.Prepare(`INSERT INTO dag_runs (id, schedule_id, trigger_source, started_at, status, retry_of)
		VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}

	s.stmtInsertTask, err = s.db.Prepare(`INSERT INTO tasks (id, dag_run_id, pipeline, status, attempt)
		VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}

	s.stmtUpdateTask, err = s.db.Prepare(`UPDATE tasks SET status = ?, exit_code = ?, started_at = ?, ended_at = ? WHERE id = ?`)
	if err != nil {
		return err
	}

	s.stmtUpdateRun, err = s.db.Prepare(`UPDATE dag_runs SET status = ?, ended_at = ? WHERE id = ?`)
	if err != nil {
		return err
	}

	s.stmtTasksByRun, err = s.db.Prepare(`SELECT id, dag_run_id, pipeline, status, attempt, exit_code, started_at, ended_at
		FROM tasks WHERE dag_run_id = ?`)
	if err != nil {
		return err
	}

	s.stmtRunByID, err = s.db.Prepare(`SELECT id, schedule_id, trigger_source, started_at, ended_at, status, retry_of
		FROM dag_runs WHERE id = ?`)
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) InsertDAGRun(run *DAGRun) error {
	_, err := s.stmtInsertRun.Exec(run.ID, run.ScheduleID, run.TriggerSource, run.StartedAt, run.Status, run.RetryOf)
	return err
}

func (s *Store) InsertTask(task *Task) error {
	_, err := s.stmtInsertTask.Exec(task.ID, task.DAGRunID, task.Pipeline, task.Status, task.Attempt)
	return err
}

func (s *Store) UpdateTask(id, status string, exitCode *int, startedAt, endedAt string) error {
	_, err := s.stmtUpdateTask.Exec(status, exitCode, startedAt, endedAt, id)
	return err
}

func (s *Store) GetDAGRun(id string) (*DAGRun, error) {
	var run DAGRun
	var endedAt, retryOf sql.NullString
	err := s.stmtRunByID.QueryRow(id).Scan(
		&run.ID, &run.ScheduleID, &run.TriggerSource, &run.StartedAt, &endedAt, &run.Status, &retryOf,
	)
	if err != nil {
		return nil, err
	}
	run.EndedAt = endedAt.String
	run.RetryOf = retryOf.String
	return &run, nil
}

func (s *Store) TasksByRun(dagRunID string) ([]Task, error) {
	rows, err := s.stmtTasksByRun.Query(dagRunID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var tasks []Task
	for rows.Next() {
		var t Task
		var exitCode sql.NullInt64
		var startedAt, endedAt sql.NullString
		if err := rows.Scan(&t.ID, &t.DAGRunID, &t.Pipeline, &t.Status, &t.Attempt, &exitCode, &startedAt, &endedAt); err != nil {
			return nil, err
		}
		if exitCode.Valid {
			v := int(exitCode.Int64)
			t.ExitCode = &v
		}
		t.StartedAt = startedAt.String
		t.EndedAt = endedAt.String
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}

// ReadyTasks returns pending tasks whose upstream dependencies are all completed.
// deps maps pipeline name -> list of pipeline names it depends on (matching dag.Graph).
func (s *Store) ReadyTasks(dagRunID string, deps map[string][]string) ([]Task, error) {
	allTasks, err := s.TasksByRun(dagRunID)
	if err != nil {
		return nil, err
	}

	statusMap := make(map[string]string, len(allTasks))
	for _, t := range allTasks {
		statusMap[t.Pipeline] = t.Status
	}

	var ready []Task
	for _, t := range allTasks {
		if t.Status != "pending" {
			continue
		}
		upstreams := deps[t.Pipeline]
		if len(upstreams) == 0 {
			ready = append(ready, t)
			continue
		}
		blocked := false
		for _, dep := range upstreams {
			if statusMap[dep] != "completed" {
				blocked = true
				break
			}
		}
		if !blocked {
			ready = append(ready, t)
		}
	}
	return ready, nil
}

func (s *Store) UpdateDAGRun(id, status, endedAt string) error {
	_, err := s.stmtUpdateRun.Exec(status, endedAt, id)
	return err
}

// RecentRuns returns the last N DAG runs, newest first.
func (s *Store) RecentRuns(limit int) ([]DAGRun, error) {
	rows, err := s.db.Query(`SELECT id, schedule_id, trigger_source, started_at, ended_at, status, retry_of
		FROM dag_runs ORDER BY started_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var runs []DAGRun
	for rows.Next() {
		var r DAGRun
		var scheduleID, endedAt, retryOf sql.NullString
		if err := rows.Scan(&r.ID, &scheduleID, &r.TriggerSource, &r.StartedAt, &endedAt, &r.Status, &retryOf); err != nil {
			return nil, err
		}
		r.ScheduleID = scheduleID.String
		r.EndedAt = endedAt.String
		r.RetryOf = retryOf.String
		runs = append(runs, r)
	}
	return runs, rows.Err()
}

// FindDAGRunByPrefix returns a DAG run whose ID starts with the given prefix.
// Returns sql.ErrNoRows if no match is found, or an error if multiple runs match.
func (s *Store) FindDAGRunByPrefix(prefix string) (*DAGRun, error) {
	rows, err := s.db.Query(`SELECT id, schedule_id, trigger_source, started_at, ended_at, status, retry_of
		FROM dag_runs WHERE id LIKE ? ORDER BY started_at DESC LIMIT 2`, prefix+"%")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var runs []DAGRun
	for rows.Next() {
		var r DAGRun
		var scheduleID, endedAt, retryOf sql.NullString
		if err := rows.Scan(&r.ID, &scheduleID, &r.TriggerSource, &r.StartedAt, &endedAt, &r.Status, &retryOf); err != nil {
			return nil, err
		}
		r.ScheduleID = scheduleID.String
		r.EndedAt = endedAt.String
		r.RetryOf = retryOf.String
		runs = append(runs, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(runs) == 0 {
		return nil, sql.ErrNoRows
	}
	if len(runs) > 1 {
		return nil, fmt.Errorf("ambiguous prefix %q: matches %s and %s", prefix, runs[0].ID[:8], runs[1].ID[:8])
	}
	return &runs[0], nil
}

// OldestQueuedRun returns the oldest DAG run with status "queued", or nil if none.
func (s *Store) OldestQueuedRun() (*DAGRun, error) {
	var r DAGRun
	var scheduleID, endedAt, retryOf sql.NullString
	err := s.db.QueryRow(`SELECT id, schedule_id, trigger_source, started_at, ended_at, status, retry_of
		FROM dag_runs WHERE status = 'queued' ORDER BY started_at ASC LIMIT 1`).Scan(
		&r.ID, &scheduleID, &r.TriggerSource, &r.StartedAt, &endedAt, &r.Status, &retryOf)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	r.ScheduleID = scheduleID.String
	r.EndedAt = endedAt.String
	r.RetryOf = retryOf.String
	return &r, nil
}

// HasRunningRun returns true if any DAG run has status "running".
func (s *Store) HasRunningRun() (bool, error) {
	var count int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM dag_runs WHERE status = 'running'`).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// VacuumOldRuns deletes dag_runs (and their tasks) that ended before the cutoff.
// Returns the number of runs removed.
func (s *Store) VacuumOldRuns(cutoff time.Time) (int, error) {
	cutoffStr := cutoff.UTC().Format(time.RFC3339)
	_, err := s.db.Exec(`DELETE FROM tasks WHERE dag_run_id IN (
		SELECT id FROM dag_runs WHERE ended_at != '' AND ended_at < ? AND status != 'running')`, cutoffStr)
	if err != nil {
		return 0, err
	}
	result, err := s.db.Exec(`DELETE FROM dag_runs WHERE ended_at != '' AND ended_at < ? AND status != 'running'`, cutoffStr)
	if err != nil {
		return 0, err
	}
	n, _ := result.RowsAffected()
	return int(n), nil
}

// PipelineSummary holds aggregated info about a pipeline's most recent task.
type PipelineSummary struct {
	Pipeline     string
	LastStatus   string
	LastRunAt    string
	LastDuration string
	RunCount     int
}

// PipelinesSummary returns per-pipeline summary data (latest status, last run time, run count).
// Designed for the TUI pipeline list — one query instead of N+1.
func (s *Store) PipelinesSummary() ([]PipelineSummary, error) {
	rows, err := s.db.Query(`
		WITH ranked AS (
			SELECT t.pipeline,
				t.status,
				r.started_at AS run_started,
				t.started_at AS task_started,
				t.ended_at   AS task_ended,
				ROW_NUMBER() OVER (PARTITION BY t.pipeline ORDER BY r.started_at DESC, t.ended_at DESC) AS rn
			FROM tasks t
			JOIN dag_runs r ON r.id = t.dag_run_id
		)
		SELECT pipeline,
			status,
			COALESCE(run_started, ''),
			COALESCE(task_started, ''),
			CASE WHEN task_ended != '' AND task_started != '' THEN task_ended ELSE '' END,
			(SELECT COUNT(DISTINCT t2.dag_run_id) FROM tasks t2 WHERE t2.pipeline = ranked.pipeline)
		FROM ranked
		WHERE rn = 1
		ORDER BY pipeline`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var results []PipelineSummary
	for rows.Next() {
		var ps PipelineSummary
		var taskStarted, taskEnded string
		if err := rows.Scan(&ps.Pipeline, &ps.LastStatus, &ps.LastRunAt, &taskStarted, &taskEnded, &ps.RunCount); err != nil {
			return nil, err
		}
		if taskStarted != "" && taskEnded != "" {
			if s, e := parseTime(taskStarted), parseTime(taskEnded); !s.IsZero() && !e.IsZero() {
				ps.LastDuration = e.Sub(s).Truncate(time.Millisecond).String()
			}
		}
		results = append(results, ps)
	}
	return results, rows.Err()
}

func parseTime(s string) time.Time {
	t, _ := time.Parse(time.RFC3339, s)
	return t
}

// RunsForPipeline returns the last N DAG runs that contain a task for the given pipeline.
func (s *Store) RunsForPipeline(pipeline string, limit int) ([]DAGRun, error) {
	rows, err := s.db.Query(`
		SELECT DISTINCT r.id, r.schedule_id, r.trigger_source, r.started_at, r.ended_at, r.status, r.retry_of
		FROM dag_runs r
		JOIN tasks t ON t.dag_run_id = r.id
		WHERE t.pipeline = ?
		ORDER BY r.started_at DESC
		LIMIT ?`, pipeline, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var runs []DAGRun
	for rows.Next() {
		var r DAGRun
		var scheduleID, endedAt, retryOf sql.NullString
		if err := rows.Scan(&r.ID, &scheduleID, &r.TriggerSource, &r.StartedAt, &endedAt, &r.Status, &retryOf); err != nil {
			return nil, err
		}
		r.ScheduleID = scheduleID.String
		r.EndedAt = endedAt.String
		r.RetryOf = retryOf.String
		runs = append(runs, r)
	}
	return runs, rows.Err()
}

// RecentFailedTasks returns tasks that transitioned to "failed" within the last N seconds.
func (s *Store) RecentFailedTasks(withinSeconds int) ([]Task, error) {
	rows, err := s.db.Query(`
		SELECT t.id, t.dag_run_id, t.pipeline, t.status, t.attempt, t.exit_code, t.started_at, t.ended_at
		FROM tasks t
		WHERE t.status = 'failed'
		  AND t.ended_at != ''
		  AND t.ended_at >= datetime('now', '-' || ? || ' seconds')
		ORDER BY t.ended_at DESC`, withinSeconds)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var tasks []Task
	for rows.Next() {
		var t Task
		var exitCode sql.NullInt64
		var startedAt, endedAt sql.NullString
		if err := rows.Scan(&t.ID, &t.DAGRunID, &t.Pipeline, &t.Status, &t.Attempt, &exitCode, &startedAt, &endedAt); err != nil {
			return nil, err
		}
		if exitCode.Valid {
			v := int(exitCode.Int64)
			t.ExitCode = &v
		}
		t.StartedAt = startedAt.String
		t.EndedAt = endedAt.String
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}

// IsWALEnabled checks if WAL journal mode is active.
func (s *Store) IsWALEnabled() (bool, error) {
	var mode string
	err := s.db.QueryRow("PRAGMA journal_mode").Scan(&mode)
	if err != nil {
		return false, err
	}
	return mode == "wal", nil
}
