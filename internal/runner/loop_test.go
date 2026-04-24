package runner

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/flowerpothq/flowerpot/internal/config"
	"github.com/flowerpothq/flowerpot/internal/state"
	"github.com/google/uuid"
)

func setupTestRunner(t *testing.T, cfg *config.Config, deps map[string][]string) (*DAGRunner, *state.Store) {
	t.Helper()

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "state.db")
	store, err := state.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	dagRunID := uuid.New().String()
	now := time.Now().UTC().Format(time.RFC3339)
	if err := store.InsertDAGRun(&state.DAGRun{
		ID:            dagRunID,
		TriggerSource: "test",
		StartedAt:     now,
		Status:        "running",
	}); err != nil {
		t.Fatal(err)
	}

	for name := range cfg.Pipelines {
		if err := store.InsertTask(&state.Task{
			ID:       uuid.New().String(),
			DAGRunID: dagRunID,
			Pipeline: name,
			Status:   "pending",
			Attempt:  1,
		}); err != nil {
			t.Fatal(err)
		}
	}

	logDir := filepath.Join(tmpDir, "logs")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatal(err)
	}

	maxWorkers := cfg.MaxConcurrent
	if maxWorkers <= 0 {
		maxWorkers = 4
	}

	runner := &DAGRunner{
		Store:      store,
		Config:     cfg,
		ProjectDir: tmpDir,
		DagRunID:   dagRunID,
		LogDir:     logDir,
		MaxWorkers: maxWorkers,
		Deps:       deps,
	}

	return runner, store
}

func TestRunLoop_LinearDAG(t *testing.T) {
	cfg := &config.Config{
		MaxConcurrent: 4,
		Pipelines: map[string]*config.Pipeline{
			"A": {Run: "echo A"},
			"B": {Run: "echo B"},
			"C": {Run: "echo C"},
		},
	}
	deps := map[string][]string{
		"A": {},
		"B": {"A"},
		"C": {"B"},
	}

	r, store := setupTestRunner(t, cfg, deps)
	status, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != "completed" {
		t.Fatalf("expected completed, got %s", status)
	}

	tasks, _ := store.TasksByRun(r.DagRunID)
	for _, task := range tasks {
		if task.Status != "completed" {
			t.Errorf("task %s should be completed, got %s", task.Pipeline, task.Status)
		}
	}
}

func TestRunLoop_ParallelBranches(t *testing.T) {
	cfg := &config.Config{
		MaxConcurrent: 4,
		Pipelines: map[string]*config.Pipeline{
			"A": {Run: "echo A"},
			"B": {Run: "sleep 0.05 && echo B"},
			"C": {Run: "sleep 0.05 && echo C"},
		},
	}
	deps := map[string][]string{
		"A": {},
		"B": {"A"},
		"C": {"A"},
	}

	r, store := setupTestRunner(t, cfg, deps)
	status, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != "completed" {
		t.Fatalf("expected completed, got %s", status)
	}

	tasks, _ := store.TasksByRun(r.DagRunID)
	for _, task := range tasks {
		if task.Status != "completed" {
			t.Errorf("task %s should be completed, got %s", task.Pipeline, task.Status)
		}
	}
}

func TestRunLoop_DiamondDAG(t *testing.T) {
	cfg := &config.Config{
		MaxConcurrent: 4,
		Pipelines: map[string]*config.Pipeline{
			"A": {Run: "echo A"},
			"B": {Run: "echo B"},
			"C": {Run: "echo C"},
			"D": {Run: "echo D"},
		},
	}
	deps := map[string][]string{
		"A": {},
		"B": {"A"},
		"C": {"A"},
		"D": {"B", "C"},
	}

	r, store := setupTestRunner(t, cfg, deps)
	status, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != "completed" {
		t.Fatalf("expected completed, got %s", status)
	}

	tasks, _ := store.TasksByRun(r.DagRunID)
	taskMap := make(map[string]state.Task)
	for _, task := range tasks {
		taskMap[task.Pipeline] = task
	}

	if taskMap["D"].Status != "completed" {
		t.Fatal("D should be completed")
	}
	dStart := parseTimeTest(t, taskMap["D"].StartedAt)
	bEnd := parseTimeTest(t, taskMap["B"].EndedAt)
	cEnd := parseTimeTest(t, taskMap["C"].EndedAt)
	if dStart.Before(bEnd) || dStart.Before(cEnd) {
		t.Error("D should have started after both B and C completed")
	}
}

func TestRunLoop_FailurePropagation(t *testing.T) {
	cfg := &config.Config{
		MaxConcurrent: 4,
		Pipelines: map[string]*config.Pipeline{
			"A": {Run: "exit 1"},
			"B": {Run: "echo B"},
			"C": {Run: "echo C"},
		},
	}
	deps := map[string][]string{
		"A": {},
		"B": {"A"},
		"C": {"B"},
	}

	r, store := setupTestRunner(t, cfg, deps)
	status, _ := r.Run(context.Background())
	if status != "failed" {
		t.Fatalf("expected failed, got %s", status)
	}

	tasks, _ := store.TasksByRun(r.DagRunID)
	taskMap := make(map[string]state.Task)
	for _, task := range tasks {
		taskMap[task.Pipeline] = task
	}

	if taskMap["A"].Status != "failed" {
		t.Errorf("A should be failed, got %s", taskMap["A"].Status)
	}
	if taskMap["B"].Status != "skipped" {
		t.Errorf("B should be skipped, got %s", taskMap["B"].Status)
	}
	if taskMap["C"].Status != "skipped" {
		t.Errorf("C should be skipped, got %s", taskMap["C"].Status)
	}
}

func TestRunLoop_IndependentBranchContinues(t *testing.T) {
	cfg := &config.Config{
		MaxConcurrent: 4,
		Pipelines: map[string]*config.Pipeline{
			"A": {Run: "exit 1"},
			"B": {Run: "echo B"},
			"C": {Run: "echo C"},
			"D": {Run: "echo D"},
		},
	}
	deps := map[string][]string{
		"A": {},
		"B": {"A"},
		"C": {},
		"D": {"C"},
	}

	r, store := setupTestRunner(t, cfg, deps)
	status, _ := r.Run(context.Background())
	if status != "partial_failure" {
		t.Fatalf("expected partial_failure, got %s", status)
	}

	tasks, _ := store.TasksByRun(r.DagRunID)
	taskMap := make(map[string]state.Task)
	for _, task := range tasks {
		taskMap[task.Pipeline] = task
	}

	if taskMap["A"].Status != "failed" {
		t.Errorf("A should be failed, got %s", taskMap["A"].Status)
	}
	if taskMap["B"].Status != "skipped" {
		t.Errorf("B should be skipped, got %s", taskMap["B"].Status)
	}
	if taskMap["C"].Status != "completed" {
		t.Errorf("C should be completed, got %s", taskMap["C"].Status)
	}
	if taskMap["D"].Status != "completed" {
		t.Errorf("D should be completed, got %s", taskMap["D"].Status)
	}
}

func TestRunLoop_BoundedConcurrency(t *testing.T) {
	pipelines := make(map[string]*config.Pipeline)
	deps := make(map[string][]string)
	for i := 0; i < 6; i++ {
		name := string(rune('A' + i))
		pipelines[name] = &config.Pipeline{Run: "sleep 0.05"}
		deps[name] = nil
	}

	cfg := &config.Config{
		MaxConcurrent: 2,
		Pipelines:     pipelines,
	}

	r, _ := setupTestRunner(t, cfg, deps)

	start := time.Now()
	status, err := r.Run(context.Background())
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != "completed" {
		t.Fatalf("expected completed, got %s", status)
	}

	// 6 tasks at 50ms each, max 2 concurrent → at least ~150ms total
	if elapsed < 100*time.Millisecond {
		t.Errorf("expected at least 100ms (bounded concurrency), got %v", elapsed)
	}
}

func TestRunLoop_CrashRecovery(t *testing.T) {
	cfg := &config.Config{
		MaxConcurrent: 4,
		Pipelines: map[string]*config.Pipeline{
			"A": {Run: "echo A"},
			"B": {Run: "echo B"},
		},
	}
	deps := map[string][]string{
		"A": {},
		"B": {"A"},
	}

	r, store := setupTestRunner(t, cfg, deps)

	tasks, _ := store.TasksByRun(r.DagRunID)
	for _, task := range tasks {
		if task.Pipeline == "A" {
			_ = store.UpdateTask(task.ID, "running", nil, time.Now().UTC().Format(time.RFC3339), "")
		}
	}

	status, _ := r.Run(context.Background())

	finalTasks, _ := store.TasksByRun(r.DagRunID)
	taskMap := make(map[string]state.Task)
	for _, task := range finalTasks {
		taskMap[task.Pipeline] = task
	}

	if taskMap["A"].Status != "failed" {
		t.Errorf("A should be failed (crash recovery), got %s", taskMap["A"].Status)
	}
	if taskMap["B"].Status != "skipped" {
		t.Errorf("B should be skipped (after crash recovery), got %s", taskMap["B"].Status)
	}
	if status != "failed" {
		t.Errorf("DAG should be failed, got %s", status)
	}
}

func TestRunLoop_ContextCancellation(t *testing.T) {
	cfg := &config.Config{
		MaxConcurrent: 4,
		Pipelines: map[string]*config.Pipeline{
			"A": {Run: "sleep 10"},
		},
	}
	deps := map[string][]string{"A": {}}

	r, _ := setupTestRunner(t, cfg, deps)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, _ = r.Run(ctx)
	elapsed := time.Since(start)

	if elapsed > 5*time.Second {
		t.Errorf("run should have been cancelled quickly, took %v", elapsed)
	}
}

func TestRunLoop_SingleRoot(t *testing.T) {
	cfg := &config.Config{
		MaxConcurrent: 4,
		Pipelines: map[string]*config.Pipeline{
			"solo": {Run: "echo hello"},
		},
	}
	deps := map[string][]string{"solo": {}}

	r, store := setupTestRunner(t, cfg, deps)
	status, err := r.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status != "completed" {
		t.Fatalf("expected completed, got %s", status)
	}

	tasks, _ := store.TasksByRun(r.DagRunID)
	if len(tasks) != 1 || tasks[0].Status != "completed" {
		t.Error("single task should be completed")
	}
}

func parseTimeTest(t *testing.T, s string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("invalid time %q: %v", s, err)
	}
	return ts
}
