package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLogsCommand_ListFiles(t *testing.T) {
	dir := t.TempDir()
	configPath := writeRunTestYAML(t, dir, `
pipelines:
  hello:
    run: "echo hello"
`)

	// Run a pipeline first to create log files
	_, _, exitCode := execBinary(t, "run", "hello", "-c", configPath)
	if exitCode != 0 {
		t.Fatal("run should succeed")
	}

	// List runs
	stdout, stderr, exitCode := execBinary(t, "logs", "-c", configPath)
	if exitCode != 0 {
		t.Fatalf("logs should succeed\nstderr: %s", stderr)
	}
	if len(stdout) == 0 {
		t.Fatal("expected output listing runs")
	}

	// Find the run directory
	logsDir := filepath.Join(dir, ".flowerpot", "logs")
	entries, err := os.ReadDir(logsDir)
	if err != nil {
		t.Fatalf("reading logs dir: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("expected at least one run directory")
	}

	runID := entries[0].Name()
	prefix := runID[:8]

	// List files for the run
	stdout, stderr, exitCode = execBinary(t, "logs", prefix, "-c", configPath)
	if exitCode != 0 {
		t.Fatalf("logs <run> should succeed\nstderr: %s", stderr)
	}
	if !strings.Contains(stdout, "hello.stdout") {
		t.Fatalf("expected hello.stdout in output, got: %s", stdout)
	}
}

func TestLogsCommand_CatPipeline(t *testing.T) {
	dir := t.TempDir()
	configPath := writeRunTestYAML(t, dir, `
pipelines:
  hello:
    run: "echo log-output-test"
`)

	_, _, exitCode := execBinary(t, "run", "hello", "-c", configPath)
	if exitCode != 0 {
		t.Fatal("run should succeed")
	}

	logsDir := filepath.Join(dir, ".flowerpot", "logs")
	entries, _ := os.ReadDir(logsDir)
	if len(entries) == 0 {
		t.Fatal("expected run directory")
	}
	prefix := entries[0].Name()[:8]

	stdout, stderr, exitCode := execBinary(t, "logs", prefix, "hello", "-c", configPath)
	if exitCode != 0 {
		t.Fatalf("logs <run> <pipeline> should succeed\nstderr: %s", stderr)
	}
	if !strings.Contains(stdout, "log-output-test") {
		t.Fatalf("expected pipeline output, got: %s", stdout)
	}
}
