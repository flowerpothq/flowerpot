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
