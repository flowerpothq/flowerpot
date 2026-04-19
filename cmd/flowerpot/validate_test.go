package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func binaryPath(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "flowerpot")
	cmd := exec.Command("go", "build", "-buildvcs=false", "-o", bin, "./cmd/flowerpot")
	cmd.Dir = filepath.Join("..", "..")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build failed: %v\n%s", err, out)
	}
	return bin
}

func runBinary(t *testing.T, bin string, args ...string) (string, int) {
	t.Helper()
	cmd := exec.Command(bin, args...)
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	err := cmd.Run()
	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			t.Fatalf("run failed: %v", err)
		}
	}
	return buf.String(), exitCode
}

func TestValidateCommand_RealExample(t *testing.T) {
	if os.Getenv("FLOWERPOT_INTEGRATION") == "" && testing.Short() {
		t.Skip("set FLOWERPOT_INTEGRATION=1 or remove -short to run integration tests")
	}

	bin := binaryPath(t)
	examplePath := filepath.Join("..", "..", "testdata", "examples", "sql-duckdb-basic", "flowerpot.yaml")

	out, code := runBinary(t, bin, "validate", examplePath)
	if code != 0 {
		t.Fatalf("expected exit 0, got %d\noutput: %s", code, out)
	}
	if !strings.Contains(out, "Schema valid") {
		t.Fatalf("expected 'Schema valid' in output, got: %s", out)
	}
	if !strings.Contains(out, "DAG valid") {
		t.Fatalf("expected 'DAG valid' in output, got: %s", out)
	}
	if !strings.Contains(out, "SQL files found") {
		t.Fatalf("expected 'SQL files found' in output, got: %s", out)
	}
}

func TestValidateCommand_CycleError(t *testing.T) {
	if os.Getenv("FLOWERPOT_INTEGRATION") == "" && testing.Short() {
		t.Skip("set FLOWERPOT_INTEGRATION=1 or remove -short to run integration tests")
	}

	bin := binaryPath(t)
	out, code := runBinary(t, bin, "validate", filepath.Join("..", "..", "testdata", "error_cycle.yaml"))
	if code != 1 {
		t.Fatalf("expected exit 1, got %d\noutput: %s", code, out)
	}
	if !strings.Contains(out, "cycle") {
		t.Fatalf("expected 'cycle' in output, got: %s", out)
	}
}

func TestValidateCommand_SchemaError(t *testing.T) {
	if os.Getenv("FLOWERPOT_INTEGRATION") == "" && testing.Short() {
		t.Skip("set FLOWERPOT_INTEGRATION=1 or remove -short to run integration tests")
	}

	bin := binaryPath(t)
	out, code := runBinary(t, bin, "validate", filepath.Join("..", "..", "testdata", "error_mutual_exclusive.yaml"))
	if code != 1 {
		t.Fatalf("expected exit 1, got %d\noutput: %s", code, out)
	}
	if !strings.Contains(out, "line") {
		t.Fatalf("expected line number in output, got: %s", out)
	}
}

func TestValidateCommand_MissingFile(t *testing.T) {
	if os.Getenv("FLOWERPOT_INTEGRATION") == "" && testing.Short() {
		t.Skip("set FLOWERPOT_INTEGRATION=1 or remove -short to run integration tests")
	}

	bin := binaryPath(t)
	out, code := runBinary(t, bin, "validate", filepath.Join("..", "..", "testdata", "error_sql_file_missing.yaml"))
	if code != 1 {
		t.Fatalf("expected exit 1, got %d\noutput: %s", code, out)
	}
	if !strings.Contains(out, "not found") {
		t.Fatalf("expected 'not found' in output, got: %s", out)
	}
}
