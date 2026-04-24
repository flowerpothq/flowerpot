package runner

import (
	"testing"

	"github.com/flowerpothq/flowerpot/internal/state"
)

func TestForwardGraph(t *testing.T) {
	deps := map[string][]string{
		"A": {},
		"B": {"A"},
		"C": {"A"},
		"D": {"B", "C"},
	}
	fwd := ForwardGraph(deps)

	if got := len(fwd["A"]); got != 2 {
		t.Fatalf("A should have 2 downstream, got %d", got)
	}
	if got := len(fwd["B"]); got != 1 {
		t.Fatalf("B should have 1 downstream, got %d", got)
	}
	if got := len(fwd["D"]); got != 0 {
		t.Fatalf("D should have 0 downstream, got %d", got)
	}
}

func TestDownstreamOf_Linear(t *testing.T) {
	deps := map[string][]string{
		"A": {},
		"B": {"A"},
		"C": {"B"},
		"D": {"C"},
	}
	fwd := ForwardGraph(deps)
	got := DownstreamOf("A", fwd)
	if len(got) != 3 {
		t.Fatalf("expected 3 downstream of A, got %d: %v", len(got), got)
	}
}

func TestDownstreamOf_Diamond(t *testing.T) {
	deps := map[string][]string{
		"A": {},
		"B": {"A"},
		"C": {"A"},
		"D": {"B", "C"},
	}
	fwd := ForwardGraph(deps)
	got := DownstreamOf("A", fwd)
	if len(got) != 3 {
		t.Fatalf("expected 3 downstream of A, got %d: %v", len(got), got)
	}
}

func TestDownstreamOf_Leaf(t *testing.T) {
	deps := map[string][]string{
		"A": {},
		"B": {"A"},
	}
	fwd := ForwardGraph(deps)
	got := DownstreamOf("B", fwd)
	if len(got) != 0 {
		t.Fatalf("expected 0 downstream of B, got %d", len(got))
	}
}

func TestUpstreamOf_Linear(t *testing.T) {
	deps := map[string][]string{
		"A": {},
		"B": {"A"},
		"C": {"B"},
	}
	got := UpstreamOf("C", deps)
	if len(got) != 2 {
		t.Fatalf("expected 2 upstream of C, got %d: %v", len(got), got)
	}
}

func TestUpstreamOf_Diamond(t *testing.T) {
	deps := map[string][]string{
		"A": {},
		"B": {"A"},
		"C": {"A"},
		"D": {"B", "C"},
	}
	got := UpstreamOf("D", deps)
	if len(got) != 3 {
		t.Fatalf("expected 3 upstream of D, got %d: %v", len(got), got)
	}
}

func TestUpstreamOf_Root(t *testing.T) {
	deps := map[string][]string{
		"A": {},
		"B": {"A"},
	}
	got := UpstreamOf("A", deps)
	if len(got) != 0 {
		t.Fatalf("expected 0 upstream of A, got %d", len(got))
	}
}

type mockTaskStore struct {
	tasks   []state.Task
	updates map[string]string
}

func (m *mockTaskStore) TasksByRun(_ string) ([]state.Task, error) {
	return m.tasks, nil
}

func (m *mockTaskStore) UpdateTask(id, status string, _ *int, _, _ string) error {
	m.updates[id] = status
	for i := range m.tasks {
		if m.tasks[i].ID == id {
			m.tasks[i].Status = status
		}
	}
	return nil
}

func TestSkipDownstream_FailedSkipsTransitive(t *testing.T) {
	// A → B → C → D, A fails: B, C, D should be skipped
	deps := map[string][]string{
		"A": {},
		"B": {"A"},
		"C": {"B"},
		"D": {"C"},
	}
	store := &mockTaskStore{
		tasks: []state.Task{
			{ID: "t1", Pipeline: "A", Status: "failed"},
			{ID: "t2", Pipeline: "B", Status: "pending"},
			{ID: "t3", Pipeline: "C", Status: "pending"},
			{ID: "t4", Pipeline: "D", Status: "pending"},
		},
		updates: make(map[string]string),
	}

	if err := SkipDownstream("A", "run1", deps, store); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"t2", "t3", "t4"} {
		if store.updates[id] != "skipped" {
			t.Errorf("task %s should be skipped, got %q", id, store.updates[id])
		}
	}
}

func TestSkipDownstream_IndependentBranchUnaffected(t *testing.T) {
	// A→B fails; C→D independent — C and D should NOT be skipped
	deps := map[string][]string{
		"A": {},
		"B": {"A"},
		"C": {},
		"D": {"C"},
	}
	store := &mockTaskStore{
		tasks: []state.Task{
			{ID: "t1", Pipeline: "A", Status: "failed"},
			{ID: "t2", Pipeline: "B", Status: "pending"},
			{ID: "t3", Pipeline: "C", Status: "completed"},
			{ID: "t4", Pipeline: "D", Status: "pending"},
		},
		updates: make(map[string]string),
	}

	if err := SkipDownstream("A", "run1", deps, store); err != nil {
		t.Fatal(err)
	}
	if store.updates["t2"] != "skipped" {
		t.Error("B should be skipped")
	}
	if _, touched := store.updates["t3"]; touched {
		t.Error("C should not be touched")
	}
	if _, touched := store.updates["t4"]; touched {
		t.Error("D should not be touched")
	}
}

func TestSkipDownstream_PartialFailure(t *testing.T) {
	// A→C, B→C: A fails, B succeeds — C should be skipped (downstream of A)
	deps := map[string][]string{
		"A": {},
		"B": {},
		"C": {"A", "B"},
	}
	store := &mockTaskStore{
		tasks: []state.Task{
			{ID: "t1", Pipeline: "A", Status: "failed"},
			{ID: "t2", Pipeline: "B", Status: "completed"},
			{ID: "t3", Pipeline: "C", Status: "pending"},
		},
		updates: make(map[string]string),
	}

	if err := SkipDownstream("A", "run1", deps, store); err != nil {
		t.Fatal(err)
	}
	if store.updates["t3"] != "skipped" {
		t.Error("C should be skipped (downstream of failed A)")
	}
}

func TestComputeDAGRunStatus_AllCompleted(t *testing.T) {
	tasks := []state.Task{
		{Status: "completed"},
		{Status: "completed"},
	}
	if got := ComputeDAGRunStatus(tasks); got != "completed" {
		t.Errorf("expected completed, got %s", got)
	}
}

func TestComputeDAGRunStatus_PartialFailure(t *testing.T) {
	tasks := []state.Task{
		{Status: "completed"},
		{Status: "failed"},
		{Status: "skipped"},
	}
	if got := ComputeDAGRunStatus(tasks); got != "partial_failure" {
		t.Errorf("expected partial_failure, got %s", got)
	}
}

func TestComputeDAGRunStatus_AllFailed(t *testing.T) {
	tasks := []state.Task{
		{Status: "failed"},
		{Status: "skipped"},
	}
	if got := ComputeDAGRunStatus(tasks); got != "failed" {
		t.Errorf("expected failed, got %s", got)
	}
}

func TestComputeDAGRunStatus_Empty(t *testing.T) {
	if got := ComputeDAGRunStatus(nil); got != "completed" {
		t.Errorf("expected completed for empty tasks, got %s", got)
	}
}
