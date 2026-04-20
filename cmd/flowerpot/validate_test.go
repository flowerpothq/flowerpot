package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateCommand_RealExample(t *testing.T) {
	if os.Getenv("FLOWERPOT_INTEGRATION") == "" && testing.Short() {
		t.Skip("set FLOWERPOT_INTEGRATION=1 or remove -short to run integration tests")
	}

	examplePath := filepath.Join("..", "..", "testdata", "examples", "sql-duckdb-basic", "flowerpot.yaml")

	out, stderr, code := execBinary(t, "validate", examplePath)
	combined := out + stderr
	if code != 0 {
		t.Fatalf("expected exit 0, got %d\noutput: %s", code, combined)
	}
	if !strings.Contains(combined, "Schema valid") {
		t.Fatalf("expected 'Schema valid' in output, got: %s", combined)
	}
	if !strings.Contains(combined, "DAG valid") {
		t.Fatalf("expected 'DAG valid' in output, got: %s", combined)
	}
	if !strings.Contains(combined, "SQL files found") {
		t.Fatalf("expected 'SQL files found' in output, got: %s", combined)
	}
}

func TestValidateCommand_CycleError(t *testing.T) {
	if os.Getenv("FLOWERPOT_INTEGRATION") == "" && testing.Short() {
		t.Skip("set FLOWERPOT_INTEGRATION=1 or remove -short to run integration tests")
	}

	_, stderr, code := execBinary(t, "validate", filepath.Join("..", "..", "testdata", "error_cycle.yaml"))
	if code != 1 {
		t.Fatalf("expected exit 1, got %d\noutput: %s", code, stderr)
	}
	if !strings.Contains(stderr, "cycle") {
		t.Fatalf("expected 'cycle' in output, got: %s", stderr)
	}
}

func TestValidateCommand_SchemaError(t *testing.T) {
	if os.Getenv("FLOWERPOT_INTEGRATION") == "" && testing.Short() {
		t.Skip("set FLOWERPOT_INTEGRATION=1 or remove -short to run integration tests")
	}

	_, stderr, code := execBinary(t, "validate", filepath.Join("..", "..", "testdata", "error_mutual_exclusive.yaml"))
	if code != 1 {
		t.Fatalf("expected exit 1, got %d\noutput: %s", code, stderr)
	}
	if !strings.Contains(stderr, "line") {
		t.Fatalf("expected line number in output, got: %s", stderr)
	}
}

func TestValidateCommand_MissingFile(t *testing.T) {
	if os.Getenv("FLOWERPOT_INTEGRATION") == "" && testing.Short() {
		t.Skip("set FLOWERPOT_INTEGRATION=1 or remove -short to run integration tests")
	}

	_, stderr, code := execBinary(t, "validate", filepath.Join("..", "..", "testdata", "error_sql_file_missing.yaml"))
	if code != 1 {
		t.Fatalf("expected exit 1, got %d\noutput: %s", code, stderr)
	}
	if !strings.Contains(stderr, "not found") {
		t.Fatalf("expected 'not found' in output, got: %s", stderr)
	}
}
