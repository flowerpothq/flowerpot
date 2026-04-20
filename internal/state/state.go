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

	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("enabling WAL: %w", err)
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

	return s, nil
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

// IsWALEnabled checks if WAL journal mode is active.
func (s *Store) IsWALEnabled() (bool, error) {
	var mode string
	err := s.db.QueryRow("PRAGMA journal_mode").Scan(&mode)
	if err != nil {
		return false, err
	}
	return mode == "wal", nil
}
