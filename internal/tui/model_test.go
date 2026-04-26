package tui

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/flowerpothq/flowerpot/internal/state"
)

func tempStore(t *testing.T) (*state.Store, string) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, ".flowerpot", "state.db")
	s, err := state.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, dir
}

func insertRun(t *testing.T, store *state.Store, id, trigger, status string, pipelines []string) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339)
	run := &state.DAGRun{
		ID:            id,
		TriggerSource: trigger,
		StartedAt:     now,
		Status:        status,
	}
	if err := store.InsertDAGRun(run); err != nil {
		t.Fatal(err)
	}
	for i, p := range pipelines {
		task := &state.Task{
			ID:       fmt.Sprintf("%s-task-%d", id, i),
			DAGRunID: id,
			Pipeline: p,
			Status:   "pending",
			Attempt:  1,
		}
		if err := store.InsertTask(task); err != nil {
			t.Fatal(err)
		}
	}
	if status == "completed" || status == "failed" {
		ended := time.Now().UTC().Format(time.RFC3339)
		for _, p := range pipelines {
			taskID := ""
			for i, pp := range pipelines {
				if pp == p {
					taskID = fmt.Sprintf("%s-task-%d", id, i)
					break
				}
			}
			if err := store.UpdateTask(taskID, status, nil, now, ended); err != nil {
				t.Fatal(err)
			}
		}
		if err := store.UpdateDAGRun(id, status, ended); err != nil {
			t.Fatal(err)
		}
	}
}

func TestModel_Init(t *testing.T) {
	store, dir := tempStore(t)
	m := NewModel(store, dir)
	cmd := m.Init()
	if cmd == nil {
		t.Fatal("Init should return a tick command")
	}
}

func TestModel_EmptyState(t *testing.T) {
	store, dir := tempStore(t)
	m := NewModel(store, dir)

	updated, _ := m.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
	m = updated.(Model)
	view := m.View()
	if view == "" {
		t.Fatal("View should not be empty")
	}
	if len(view) < 10 {
		t.Fatal("View too short for an empty state display")
	}
}

func TestModel_Navigation(t *testing.T) {
	store, dir := tempStore(t)
	insertRun(t, store, "run-001", "manual", "completed", []string{"extract"})

	logDir := filepath.Join(dir, ".flowerpot", "logs", "run-001")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(logDir, "extract.stdout"), []byte("hello world\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	m := NewModel(store, dir)
	var model tea.Model = m

	model, _ = model.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
	model, _ = model.Update(tickMsg{})

	v := model.View()
	if v == "" || v == "Loading..." {
		t.Fatal("expected pipeline list view after tick")
	}

	// Drill into runs
	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("l")})
	model, _ = model.Update(tickMsg{})

	cast := model.(Model)
	if cast.currentView != viewRunHistory {
		t.Fatalf("expected viewRunHistory, got %d", cast.currentView)
	}

	// Drill into logs
	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model, _ = model.Update(tickMsg{})

	cast = model.(Model)
	if cast.currentView != viewLogTail {
		t.Fatalf("expected viewLogTail, got %d", cast.currentView)
	}

	// Back to runs
	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyEsc})
	cast = model.(Model)
	if cast.currentView != viewRunHistory {
		t.Fatalf("expected back to viewRunHistory, got %d", cast.currentView)
	}

	// Open help
	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("?")})
	cast = model.(Model)
	if cast.currentView != viewHelp {
		t.Fatalf("expected viewHelp, got %d", cast.currentView)
	}

	// Close help
	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyEsc})
	cast = model.(Model)
	if cast.currentView != viewRunHistory {
		t.Fatalf("expected back to viewRunHistory from help, got %d", cast.currentView)
	}

	// Back to pipelines
	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("h")})
	cast = model.(Model)
	if cast.currentView != viewPipelineList {
		t.Fatalf("expected back to viewPipelineList, got %d", cast.currentView)
	}
}

func TestPipelinesSummary_WithData(t *testing.T) {
	store, _ := tempStore(t)
	insertRun(t, store, "run-1", "cron", "completed", []string{"extract", "transform", "load"})

	ps, err := store.PipelinesSummary()
	if err != nil {
		t.Fatal(err)
	}
	if len(ps) != 3 {
		t.Fatalf("expected 3 pipeline summaries, got %d", len(ps))
	}
}

func TestRunsForPipeline(t *testing.T) {
	store, _ := tempStore(t)
	for i := range 5 {
		id := fmt.Sprintf("run-%d", i)
		insertRun(t, store, id, "cron", "completed", []string{"extract"})
	}

	runs, err := store.RunsForPipeline("extract", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 3 {
		t.Fatalf("expected 3 runs, got %d", len(runs))
	}
}
