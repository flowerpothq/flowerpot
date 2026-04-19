package dag

import (
	"strings"
	"testing"
)

func TestTopoSort_Linear(t *testing.T) {
	g := Graph{
		"A": {},
		"B": {"A"},
		"C": {"B"},
	}
	order, err := TopoSort(g)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(order) != 3 || order[0] != "A" || order[1] != "B" || order[2] != "C" {
		t.Fatalf("expected [A B C], got %v", order)
	}
}

func TestTopoSort_Diamond(t *testing.T) {
	g := Graph{
		"A": {},
		"B": {"A"},
		"C": {"A"},
		"D": {"B", "C"},
	}
	order, err := TopoSort(g)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(order) != 4 {
		t.Fatalf("expected 4 nodes, got %d", len(order))
	}
	if order[0] != "A" {
		t.Fatalf("expected A first, got %s", order[0])
	}
	if order[3] != "D" {
		t.Fatalf("expected D last, got %s", order[3])
	}
}

func TestTopoSort_ParallelRoots(t *testing.T) {
	g := Graph{
		"A": {},
		"B": {},
		"C": {"A", "B"},
	}
	order, err := TopoSort(g)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(order) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(order))
	}
	// A and B must come before C
	cIdx := indexOf(order, "C")
	if cIdx != 2 {
		t.Fatalf("expected C last, got index %d in %v", cIdx, order)
	}
}

func TestTopoSort_SqlDuckdbExample(t *testing.T) {
	g := Graph{
		"create-tables": {},
		"load-seed":     {"create-tables"},
		"transform":     {"load-seed"},
	}
	order, err := TopoSort(g)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []string{"create-tables", "load-seed", "transform"}
	if len(order) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(order))
	}
	for i, name := range expected {
		if order[i] != name {
			t.Fatalf("position %d: expected %s, got %s (full: %v)", i, name, order[i], order)
		}
	}
}

func TestCycleDetection_Simple(t *testing.T) {
	g := Graph{
		"A": {"B"},
		"B": {"A"},
	}
	_, err := TopoSort(g)
	if err == nil {
		t.Fatal("expected cycle error")
	}
	if !strings.Contains(err.Error(), "A") || !strings.Contains(err.Error(), "B") {
		t.Fatalf("error should name both pipelines, got: %v", err)
	}
}

func TestCycleDetection_SelfRef(t *testing.T) {
	g := Graph{
		"A": {"A"},
	}
	_, err := TopoSort(g)
	if err == nil {
		t.Fatal("expected cycle error")
	}
	if !strings.Contains(err.Error(), "A") {
		t.Fatalf("error should name pipeline A, got: %v", err)
	}
}

func TestCycleDetection_Long(t *testing.T) {
	g := Graph{
		"A": {"D"},
		"B": {"A"},
		"C": {"B"},
		"D": {"C"},
	}
	_, err := TopoSort(g)
	if err == nil {
		t.Fatal("expected cycle error")
	}
	for _, name := range []string{"A", "B", "C", "D"} {
		if !strings.Contains(err.Error(), name) {
			t.Fatalf("error should name %s, got: %v", name, err)
		}
	}
}

func TestOrphanNode_Valid(t *testing.T) {
	g := Graph{
		"A": {},
		"B": {"A"},
		"C": {},
	}
	order, err := TopoSort(g)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(order) != 3 {
		t.Fatalf("expected 3 nodes, got %d: %v", len(order), order)
	}
	found := make(map[string]bool)
	for _, n := range order {
		found[n] = true
	}
	for _, name := range []string{"A", "B", "C"} {
		if !found[name] {
			t.Fatalf("missing node %s in output %v", name, order)
		}
	}
}

func indexOf(slice []string, s string) int {
	for i, v := range slice {
		if v == s {
			return i
		}
	}
	return -1
}
