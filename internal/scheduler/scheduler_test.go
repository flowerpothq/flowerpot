package scheduler

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/flowerpothq/flowerpot/internal/config"
	"github.com/flowerpothq/flowerpot/internal/state"
)

func testStore(t *testing.T) (*state.Store, string) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, ".flowerpot", "state.db")
	store, err := state.Open(dbPath)
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, dir
}

func TestCronScheduler_FiresOnSchedule(t *testing.T) {
	store, projectDir := testStore(t)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cfg := &config.Config{
		Schedule:      "* * * * *",
		Timezone:      "UTC",
		MaxConcurrent: 2,
		Pipelines: map[string]*config.Pipeline{
			"hello": {Run: "echo hello"},
		},
	}

	s, err := New(cfg, store, projectDir, logger)
	if err != nil {
		t.Fatalf("creating scheduler: %v", err)
	}

	// Directly call executeDagRun instead of waiting for cron tick
	if err := s.executeDagRun(); err != nil {
		t.Fatalf("executeDagRun: %v", err)
	}

	runs, err := store.RecentRuns(10)
	if err != nil {
		t.Fatalf("querying runs: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("expected 1 run, got %d", len(runs))
	}
	if runs[0].Status != "completed" {
		t.Fatalf("expected completed, got %s", runs[0].Status)
	}
	if runs[0].TriggerSource != "schedule" {
		t.Fatalf("expected trigger_source=schedule, got %s", runs[0].TriggerSource)
	}
}

func TestOverlap_Skip(t *testing.T) {
	store, projectDir := testStore(t)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cfg := &config.Config{
		Schedule:      "* * * * *",
		Timezone:      "UTC",
		MaxConcurrent: 2,
		Pipelines: map[string]*config.Pipeline{
			"slow": {Run: "sleep 10"},
		},
	}

	s, err := New(cfg, store, projectDir, logger)
	if err != nil {
		t.Fatalf("creating scheduler: %v", err)
	}

	// Simulate a running DAG run
	now := time.Now().UTC().Format(time.RFC3339)
	if err := store.InsertDAGRun(&state.DAGRun{
		ID:            "existing-run",
		TriggerSource: "schedule",
		StartedAt:     now,
		Status:        "running",
	}); err != nil {
		t.Fatalf("inserting running DAG: %v", err)
	}

	// tick should skip because there's already a running DAG
	s.tick()

	runs, err := store.RecentRuns(10)
	if err != nil {
		t.Fatalf("querying runs: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("expected 1 run (the existing one), got %d", len(runs))
	}
}

func TestScheduler_StartStop(t *testing.T) {
	store, projectDir := testStore(t)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cfg := &config.Config{
		Schedule:      "0 0 1 1 *",
		Timezone:      "UTC",
		MaxConcurrent: 2,
		Pipelines: map[string]*config.Pipeline{
			"hello": {Run: "echo hello"},
		},
	}

	s, err := New(cfg, store, projectDir, logger)
	if err != nil {
		t.Fatalf("creating scheduler: %v", err)
	}

	ctx := context.Background()
	s.Start(ctx)

	next := s.NextRun()
	if next.IsZero() {
		t.Fatal("expected non-zero next run time")
	}

	stopCtx := s.Stop()
	<-stopCtx.Done()
}
