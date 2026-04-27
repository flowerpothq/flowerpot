package tui

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/flowerpothq/flowerpot/internal/config"
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

func testConfig() *config.Config {
	return &config.Config{
		Schedule: "0 0 1 1 *",
		Timezone: "UTC",
		Overlap:  "skip",
		Pipelines: map[string]*config.Pipeline{
			"extract":   {Run: "echo extract"},
			"transform": {Run: "echo transform", After: []string{"extract"}},
			"load":      {Run: "echo load", After: []string{"transform"}},
		},
	}
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
	m := NewModel(store, testConfig(), nil, dir)
	cmd := m.Init()
	if cmd == nil {
		t.Fatal("Init should return a tick command")
	}
}

func TestModel_EmptyState(t *testing.T) {
	store, dir := tempStore(t)
	m := NewModel(store, testConfig(), nil, dir)

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

	m := NewModel(store, testConfig(), nil, dir)
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

	// Drill into tasks
	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model, _ = model.Update(tickMsg{})

	cast = model.(Model)
	if cast.currentView != viewTaskDetail {
		t.Fatalf("expected viewTaskDetail, got %d", cast.currentView)
	}

	// Drill into logs
	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model, _ = model.Update(tickMsg{})

	cast = model.(Model)
	if cast.currentView != viewLogTail {
		t.Fatalf("expected viewLogTail, got %d", cast.currentView)
	}

	// Back to tasks
	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyEsc})
	cast = model.(Model)
	if cast.currentView != viewTaskDetail {
		t.Fatalf("expected back to viewTaskDetail, got %d", cast.currentView)
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

func TestModel_DescribeView(t *testing.T) {
	store, dir := tempStore(t)
	insertRun(t, store, "run-001", "manual", "completed", []string{"extract"})

	m := NewModel(store, testConfig(), nil, dir)
	var model tea.Model = m

	model, _ = model.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
	model, _ = model.Update(tickMsg{})

	// Press d to describe
	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("d")})
	cast := model.(Model)
	if cast.currentView != viewDescribe {
		t.Fatalf("expected viewDescribe, got %d", cast.currentView)
	}
	v := cast.View()
	if !strings.Contains(v, "Pipeline") {
		t.Fatal("describe view should contain Pipeline label")
	}

	// Back
	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyEsc})
	cast = model.(Model)
	if cast.currentView != viewPipelineList {
		t.Fatalf("expected back to viewPipelineList, got %d", cast.currentView)
	}
}

func TestModel_TriggerNoScheduler(t *testing.T) {
	store, dir := tempStore(t)
	insertRun(t, store, "run-001", "manual", "completed", []string{"extract"})

	m := NewModel(store, testConfig(), nil, dir)
	var model tea.Model = m

	model, _ = model.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
	model, _ = model.Update(tickMsg{})

	// Trigger pipeline without daemon -> flash error
	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("t")})
	cast := model.(Model)
	if cast.flash == nil {
		t.Fatal("expected flash message after trigger without daemon")
	}
	if !strings.Contains(cast.flash.text, "no scheduler") {
		t.Fatalf("expected 'no scheduler' flash, got %q", cast.flash.text)
	}
}

func TestModel_TriggerAllNoScheduler(t *testing.T) {
	store, dir := tempStore(t)
	insertRun(t, store, "run-001", "manual", "completed", []string{"extract"})

	m := NewModel(store, testConfig(), nil, dir)
	var model tea.Model = m

	model, _ = model.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
	model, _ = model.Update(tickMsg{})

	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("T")})
	cast := model.(Model)
	if cast.flash == nil {
		t.Fatal("expected flash message after trigger-all without daemon")
	}
}

func TestModel_RetryNoScheduler(t *testing.T) {
	store, dir := tempStore(t)
	insertRun(t, store, "run-001", "manual", "failed", []string{"extract"})

	m := NewModel(store, testConfig(), nil, dir)
	var model tea.Model = m

	model, _ = model.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
	model, _ = model.Update(tickMsg{})

	// Drill into runs
	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("l")})
	model, _ = model.Update(tickMsg{})

	// Press r to retry
	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("r")})
	cast := model.(Model)
	if cast.flash == nil {
		t.Fatal("expected flash message after retry without daemon")
	}
	if !strings.Contains(cast.flash.text, "no scheduler") {
		t.Fatalf("expected 'no scheduler' flash, got %q", cast.flash.text)
	}
}

func TestModel_Filter(t *testing.T) {
	store, dir := tempStore(t)
	insertRun(t, store, "run-001", "manual", "completed", []string{"extract", "transform", "load"})

	m := NewModel(store, testConfig(), nil, dir)
	var model tea.Model = m

	model, _ = model.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
	model, _ = model.Update(tickMsg{})

	// Enter filter mode
	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("/")})
	cast := model.(Model)
	if !cast.filtering {
		t.Fatal("expected filtering mode to be active")
	}

	// Type filter text -- "load" is unique: only the load pipeline matches
	for _, r := range "load" {
		model, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}

	// Confirm filter
	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	cast = model.(Model)
	if cast.filtering {
		t.Fatal("expected filtering mode to be deactivated after enter")
	}
	if cast.filterText != "load" {
		t.Fatalf("expected filterText='load', got %q", cast.filterText)
	}

	// Check table is filtered
	rows := cast.pipelines.table.Rows()
	if len(rows) != 1 {
		t.Fatalf("expected 1 filtered row, got %d", len(rows))
	}
	if rows[0][0] != "load" {
		t.Fatalf("expected load row, got %s", rows[0][0])
	}
}

func TestModel_FilterClear(t *testing.T) {
	store, dir := tempStore(t)
	insertRun(t, store, "run-001", "manual", "completed", []string{"extract", "transform"})

	m := NewModel(store, testConfig(), nil, dir)
	var model tea.Model = m

	model, _ = model.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
	model, _ = model.Update(tickMsg{})

	// Enter filter, type, then esc to clear
	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("/")})
	for _, r := range "xyz" {
		model, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}
	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyEsc})
	cast := model.(Model)
	if cast.filterText != "" {
		t.Fatalf("expected empty filterText after esc, got %q", cast.filterText)
	}
}

func TestModel_TaskDetailView(t *testing.T) {
	store, dir := tempStore(t)
	insertRun(t, store, "run-001", "manual", "completed", []string{"extract", "transform"})

	m := NewModel(store, testConfig(), nil, dir)
	var model tea.Model = m

	model, _ = model.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
	model, _ = model.Update(tickMsg{})

	// Navigate: pipelines -> runs -> tasks
	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model, _ = model.Update(tickMsg{})
	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model, _ = model.Update(tickMsg{})

	cast := model.(Model)
	if cast.currentView != viewTaskDetail {
		t.Fatalf("expected viewTaskDetail, got %d", cast.currentView)
	}

	v := cast.View()
	if v == "" {
		t.Fatal("task detail view should not be empty")
	}
}

func TestModel_FlashExpiry(t *testing.T) {
	store, dir := tempStore(t)
	m := NewModel(store, testConfig(), nil, dir)
	m.setFlash("test message", true)

	if m.flash == nil {
		t.Fatal("expected flash to be set")
	}

	m.flashExp = time.Now().Add(-1 * time.Second)

	var model tea.Model = m
	model, _ = model.Update(tickMsg{})
	cast := model.(Model)
	if cast.flash != nil {
		t.Fatal("expected flash to be cleared after expiry")
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
