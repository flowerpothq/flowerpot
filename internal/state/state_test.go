package state

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func tempStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, ".flowerpot", "state.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func mustInsertRun(t *testing.T, s *Store, run *DAGRun) {
	t.Helper()
	if err := s.InsertDAGRun(run); err != nil {
		t.Fatalf("InsertDAGRun: %v", err)
	}
}

func mustInsertTask(t *testing.T, s *Store, task *Task) {
	t.Helper()
	if err := s.InsertTask(task); err != nil {
		t.Fatalf("InsertTask: %v", err)
	}
}

func mustUpdateTask(t *testing.T, s *Store, id, status string) {
	t.Helper()
	if err := s.UpdateTask(id, status, nil, "2026-01-01T00:00:01Z", "2026-01-01T00:00:02Z"); err != nil {
		t.Fatalf("UpdateTask: %v", err)
	}
}

func TestOpen_CreatesDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, ".flowerpot", "state.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatal("expected state.db to exist")
	}

	isWAL, err := s.IsWALEnabled()
	if err != nil {
		t.Fatalf("IsWALEnabled: %v", err)
	}
	if !isWAL {
		t.Fatal("expected WAL mode")
	}
}

func TestOpen_Idempotent(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, ".flowerpot", "state.db")

	s1, err := Open(dbPath)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	_ = s1.Close()

	s2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	defer func() { _ = s2.Close() }()

	// Verify tables exist by querying them
	var count int
	err = s2.db.QueryRow("SELECT COUNT(*) FROM dag_runs").Scan(&count)
	if err != nil {
		t.Fatalf("querying dag_runs: %v", err)
	}
	err = s2.db.QueryRow("SELECT COUNT(*) FROM tasks").Scan(&count)
	if err != nil {
		t.Fatalf("querying tasks: %v", err)
	}
}

func TestInsertDAGRun(t *testing.T) {
	s := tempStore(t)

	run := &DAGRun{
		ID:            "run-001",
		ScheduleID:    "sched-1",
		TriggerSource: "manual",
		StartedAt:     "2026-01-01T00:00:00Z",
		Status:        "running",
	}
	if err := s.InsertDAGRun(run); err != nil {
		t.Fatalf("InsertDAGRun: %v", err)
	}

	got, err := s.GetDAGRun("run-001")
	if err != nil {
		t.Fatalf("GetDAGRun: %v", err)
	}
	if got.ID != "run-001" {
		t.Fatalf("expected id run-001, got %s", got.ID)
	}
	if got.TriggerSource != "manual" {
		t.Fatalf("expected trigger_source manual, got %s", got.TriggerSource)
	}
	if got.Status != "running" {
		t.Fatalf("expected status running, got %s", got.Status)
	}
}

func TestInsertTasks(t *testing.T) {
	s := tempStore(t)

	run := &DAGRun{ID: "run-001", TriggerSource: "manual", StartedAt: "2026-01-01T00:00:00Z", Status: "running"}
	if err := s.InsertDAGRun(run); err != nil {
		t.Fatalf("InsertDAGRun: %v", err)
	}

	pipelines := []string{"extract", "load", "transform"}
	for i, p := range pipelines {
		task := &Task{
			ID:       fmt.Sprintf("task-%03d", i+1),
			DAGRunID: "run-001",
			Pipeline: p,
			Status:   "pending",
			Attempt:  1,
		}
		if err := s.InsertTask(task); err != nil {
			t.Fatalf("InsertTask %s: %v", p, err)
		}
	}

	tasks, err := s.TasksByRun("run-001")
	if err != nil {
		t.Fatalf("TasksByRun: %v", err)
	}
	if len(tasks) != 3 {
		t.Fatalf("expected 3 tasks, got %d", len(tasks))
	}
}

func TestReadyTasks_Roots(t *testing.T) {
	s := tempStore(t)

	run := &DAGRun{ID: "run-001", TriggerSource: "manual", StartedAt: "2026-01-01T00:00:00Z", Status: "running"}
	mustInsertRun(t, s, run)

	mustInsertTask(t, s, &Task{ID: "t-a", DAGRunID: "run-001", Pipeline: "A", Status: "pending", Attempt: 1})
	mustInsertTask(t, s, &Task{ID: "t-b", DAGRunID: "run-001", Pipeline: "B", Status: "pending", Attempt: 1})
	mustInsertTask(t, s, &Task{ID: "t-c", DAGRunID: "run-001", Pipeline: "C", Status: "pending", Attempt: 1})

	deps := map[string][]string{
		"A": {},
		"B": {},
		"C": {"A", "B"},
	}

	ready, err := s.ReadyTasks("run-001", deps)
	if err != nil {
		t.Fatalf("ReadyTasks: %v", err)
	}

	readyNames := make(map[string]bool)
	for _, r := range ready {
		readyNames[r.Pipeline] = true
	}
	if !readyNames["A"] || !readyNames["B"] {
		t.Fatalf("expected A and B ready, got %v", readyNames)
	}
	if readyNames["C"] {
		t.Fatal("C should not be ready yet")
	}
}

func TestReadyTasks_AfterCompletion(t *testing.T) {
	s := tempStore(t)

	run := &DAGRun{ID: "run-001", TriggerSource: "manual", StartedAt: "2026-01-01T00:00:00Z", Status: "running"}
	mustInsertRun(t, s, run)

	mustInsertTask(t, s, &Task{ID: "t-a", DAGRunID: "run-001", Pipeline: "A", Status: "pending", Attempt: 1})
	mustInsertTask(t, s, &Task{ID: "t-b", DAGRunID: "run-001", Pipeline: "B", Status: "pending", Attempt: 1})

	mustUpdateTask(t, s, "t-a", "completed")

	deps := map[string][]string{
		"A": {},
		"B": {"A"},
	}

	ready, err := s.ReadyTasks("run-001", deps)
	if err != nil {
		t.Fatalf("ReadyTasks: %v", err)
	}

	if len(ready) != 1 || ready[0].Pipeline != "B" {
		t.Fatalf("expected only B ready, got %v", ready)
	}
}

func TestReadyTasks_Blocked(t *testing.T) {
	s := tempStore(t)

	run := &DAGRun{ID: "run-001", TriggerSource: "manual", StartedAt: "2026-01-01T00:00:00Z", Status: "running"}
	mustInsertRun(t, s, run)

	mustInsertTask(t, s, &Task{ID: "t-a", DAGRunID: "run-001", Pipeline: "A", Status: "pending", Attempt: 1})
	mustInsertTask(t, s, &Task{ID: "t-b", DAGRunID: "run-001", Pipeline: "B", Status: "pending", Attempt: 1})

	deps := map[string][]string{
		"A": {},
		"B": {"A"},
	}

	ready, err := s.ReadyTasks("run-001", deps)
	if err != nil {
		t.Fatalf("ReadyTasks: %v", err)
	}

	if len(ready) != 1 || ready[0].Pipeline != "A" {
		t.Fatalf("expected only A ready, got %v", ready)
	}
}

func TestReadyTasks_CrossRunIsolation(t *testing.T) {
	s := tempStore(t)

	// Run 1
	mustInsertRun(t, s, &DAGRun{ID: "run-001", TriggerSource: "manual", StartedAt: "2026-01-01T00:00:00Z", Status: "running"})
	mustInsertTask(t, s, &Task{ID: "t1-a", DAGRunID: "run-001", Pipeline: "A", Status: "pending", Attempt: 1})
	mustInsertTask(t, s, &Task{ID: "t1-b", DAGRunID: "run-001", Pipeline: "B", Status: "pending", Attempt: 1})

	// Run 2
	mustInsertRun(t, s, &DAGRun{ID: "run-002", TriggerSource: "manual", StartedAt: "2026-01-01T01:00:00Z", Status: "running"})
	mustInsertTask(t, s, &Task{ID: "t2-a", DAGRunID: "run-002", Pipeline: "A", Status: "pending", Attempt: 1})
	mustInsertTask(t, s, &Task{ID: "t2-b", DAGRunID: "run-002", Pipeline: "B", Status: "pending", Attempt: 1})

	// Complete A in run 1 only
	mustUpdateTask(t, s, "t1-a", "completed")

	deps := map[string][]string{
		"A": {},
		"B": {"A"},
	}

	// Run 1: B should be ready (A completed)
	ready1, _ := s.ReadyTasks("run-001", deps)
	readyNames1 := make(map[string]bool)
	for _, r := range ready1 {
		readyNames1[r.Pipeline] = true
	}
	if !readyNames1["B"] {
		t.Fatalf("run-001: expected B ready after A completed, got %v", readyNames1)
	}

	// Run 2: B should NOT be ready (A still pending)
	ready2, _ := s.ReadyTasks("run-002", deps)
	for _, r := range ready2 {
		if r.Pipeline == "B" {
			t.Fatal("run-002: B should not be ready (A is still pending in this run)")
		}
	}
}
