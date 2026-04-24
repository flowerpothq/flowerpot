package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func writeRunTestYAML(t *testing.T, dir string, content string) string {
	t.Helper()
	path := filepath.Join(dir, "flowerpot.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("writing test YAML: %v", err)
	}
	return path
}

func TestRunCommand_Success(t *testing.T) {
	dir := t.TempDir()
	configPath := writeRunTestYAML(t, dir, `
pipelines:
  hello:
    run: "echo hello from flowerpot"
`)

	stdout, stderr, exitCode := execBinary(t, "run", "hello", "-c", configPath)
	if exitCode != 0 {
		t.Fatalf("expected exit 0, got %d\nstderr: %s\nstdout: %s", exitCode, stderr, stdout)
	}
	if len(stdout) == 0 {
		t.Fatal("expected output, got empty stdout")
	}
}

func TestRunCommand_Failure(t *testing.T) {
	dir := t.TempDir()
	configPath := writeRunTestYAML(t, dir, `
pipelines:
  fail:
    run: "exit 1"
`)

	_, _, exitCode := execBinary(t, "run", "fail", "-c", configPath)
	if exitCode == 0 {
		t.Fatal("expected non-zero exit code for failing pipeline")
	}
}

func TestRunCommand_JsonOutput(t *testing.T) {
	dir := t.TempDir()
	configPath := writeRunTestYAML(t, dir, `
pipelines:
  hello:
    run: "echo json-test"
`)

	stdout, stderr, exitCode := execBinary(t, "run", "hello", "-c", configPath, "--json")
	if exitCode != 0 {
		t.Fatalf("expected exit 0, got %d\nstderr: %s", exitCode, stderr)
	}

	var result runResult
	if err := json.Unmarshal([]byte(stdout), &result); err != nil {
		t.Fatalf("invalid JSON: %v\nstdout: %s", err, stdout)
	}
	if result.Pipeline != "hello" {
		t.Fatalf("expected pipeline 'hello', got %q", result.Pipeline)
	}
	if result.Status != "completed" {
		t.Fatalf("expected status 'completed', got %q", result.Status)
	}
	if result.ExitCode != 0 {
		t.Fatalf("expected exit_code 0, got %d", result.ExitCode)
	}
}

func TestRunCommand_MissingPipeline(t *testing.T) {
	dir := t.TempDir()
	configPath := writeRunTestYAML(t, dir, `
pipelines:
  hello:
    run: "echo ok"
`)

	_, stderr, exitCode := execBinary(t, "run", "nonexistent", "-c", configPath)
	if exitCode == 0 {
		t.Fatal("expected non-zero exit code for missing pipeline")
	}
	if len(stderr) == 0 {
		t.Fatal("expected error message on stderr")
	}
}

func TestRunFullDAG_Diamond(t *testing.T) {
	dir := t.TempDir()
	configPath := writeRunTestYAML(t, dir, `
pipelines:
  A:
    run: "echo A"
  B:
    run: "echo B"
    after: [A]
  C:
    run: "echo C"
    after: [A]
  D:
    run: "echo D"
    after: [B, C]
`)

	stdout, stderr, exitCode := execBinary(t, "run", "-c", configPath)
	if exitCode != 0 {
		t.Fatalf("expected exit 0, got %d\nstdout: %s\nstderr: %s", exitCode, stdout, stderr)
	}
	if len(stdout) == 0 {
		t.Fatal("expected output")
	}
}

func TestRunFullDAG_FailurePropagation(t *testing.T) {
	dir := t.TempDir()
	configPath := writeRunTestYAML(t, dir, `
pipelines:
  A:
    run: "exit 1"
  B:
    run: "echo B"
    after: [A]
`)

	stdout, stderr, exitCode := execBinary(t, "run", "-c", configPath)
	if exitCode == 0 {
		t.Fatal("expected non-zero exit code for failed DAG")
	}
	combined := stdout + stderr
	if len(combined) == 0 {
		t.Fatal("expected output showing failure")
	}
}

func TestRunFullDAG_JsonOutput(t *testing.T) {
	dir := t.TempDir()
	configPath := writeRunTestYAML(t, dir, `
pipelines:
  A:
    run: "echo A"
  B:
    run: "echo B"
    after: [A]
`)

	stdout, stderr, exitCode := execBinary(t, "run", "-c", configPath, "--json")
	if exitCode != 0 {
		t.Fatalf("expected exit 0, got %d\nstderr: %s", exitCode, stderr)
	}

	var result dagRunResult
	if err := json.Unmarshal([]byte(stdout), &result); err != nil {
		t.Fatalf("invalid JSON: %v\nstdout: %s", err, stdout)
	}
	if result.Status != "completed" {
		t.Fatalf("expected completed, got %s", result.Status)
	}
	if len(result.Tasks) != 2 {
		t.Fatalf("expected 2 tasks, got %d", len(result.Tasks))
	}
}

func TestRunWithUpstream(t *testing.T) {
	dir := t.TempDir()
	configPath := writeRunTestYAML(t, dir, `
pipelines:
  extract:
    run: "echo extract"
  transform:
    run: "echo transform"
    after: [extract]
  load:
    run: "echo load"
    after: [transform]
`)

	stdout, stderr, exitCode := execBinary(t, "run", "load", "--with-upstream", "-c", configPath, "--json")
	if exitCode != 0 {
		t.Fatalf("expected exit 0, got %d\nstderr: %s", exitCode, stderr)
	}

	var result dagRunResult
	if err := json.Unmarshal([]byte(stdout), &result); err != nil {
		t.Fatalf("invalid JSON: %v\nstdout: %s", err, stdout)
	}
	if result.Status != "completed" {
		t.Fatalf("expected completed, got %s", result.Status)
	}
	if len(result.Tasks) != 3 {
		t.Fatalf("expected 3 tasks (extract+transform+load), got %d", len(result.Tasks))
	}
}

func TestStatusCommand_ShowsLastRun(t *testing.T) {
	dir := t.TempDir()
	configPath := writeRunTestYAML(t, dir, `
pipelines:
  hello:
    run: "echo hello"
`)

	// First, do a run to create some state
	_, _, exitCode := execBinary(t, "run", "hello", "-c", configPath)
	if exitCode != 0 {
		t.Fatal("run should succeed first")
	}

	stdout, stderr, exitCode := execBinary(t, "status", "-c", configPath)
	if exitCode != 0 {
		t.Fatalf("status should succeed, stderr: %s", stderr)
	}
	if len(stdout) == 0 {
		t.Fatal("expected status output")
	}
}

func TestStatusCommand_JsonOutput(t *testing.T) {
	dir := t.TempDir()
	configPath := writeRunTestYAML(t, dir, `
pipelines:
  hello:
    run: "echo hello"
`)

	// Do a run first
	_, _, _ = execBinary(t, "run", "hello", "-c", configPath)

	stdout, stderr, exitCode := execBinary(t, "status", "-c", configPath, "--json")
	if exitCode != 0 {
		t.Fatalf("status --json should succeed, stderr: %s", stderr)
	}

	var runs []statusRunJSON
	if err := json.Unmarshal([]byte(stdout), &runs); err != nil {
		t.Fatalf("invalid JSON: %v\nstdout: %s", err, stdout)
	}
	if len(runs) == 0 {
		t.Fatal("expected at least 1 run")
	}
	if runs[0].Status != "completed" {
		t.Fatalf("expected completed, got %s", runs[0].Status)
	}
}
