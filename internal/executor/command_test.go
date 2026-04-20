package executor

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/flowerpothq/flowerpot/internal/config"
)

func tempLogDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return dir
}

func TestCommandExecutor_Success(t *testing.T) {
	logDir := tempLogDir(t)
	e := &CommandExecutor{ProjectDir: t.TempDir()}
	p := &config.Pipeline{Run: "echo hello"}

	result, err := e.Execute(context.Background(), "test", p, logDir, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", result.ExitCode)
	}

	stdout, _ := os.ReadFile(result.StdoutPath)
	if strings.TrimSpace(string(stdout)) != "hello" {
		t.Fatalf("expected stdout 'hello', got %q", string(stdout))
	}
}

func TestCommandExecutor_Failure(t *testing.T) {
	logDir := tempLogDir(t)
	e := &CommandExecutor{ProjectDir: t.TempDir()}
	p := &config.Pipeline{Run: "exit 42"}

	result, err := e.Execute(context.Background(), "test", p, logDir, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ExitCode != 42 {
		t.Fatalf("expected exit code 42, got %d", result.ExitCode)
	}
}

func TestCommandExecutor_Timeout(t *testing.T) {
	logDir := tempLogDir(t)
	e := &CommandExecutor{ProjectDir: t.TempDir()}
	p := &config.Pipeline{Run: "sleep 60"}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	result, err := e.Execute(ctx, "test", p, logDir, 1)
	if err != context.DeadlineExceeded {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
	if result.ExitCode != -1 {
		t.Fatalf("expected exit code -1 for killed process, got %d", result.ExitCode)
	}
}

func TestCommandExecutor_EnvVars(t *testing.T) {
	logDir := tempLogDir(t)
	e := &CommandExecutor{ProjectDir: t.TempDir()}
	p := &config.Pipeline{
		Run: "echo $CUSTOM_VAR",
		Env: map[string]string{"CUSTOM_VAR": "flowerpot_test_value"},
	}

	result, err := e.Execute(context.Background(), "test", p, logDir, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	stdout, _ := os.ReadFile(result.StdoutPath)
	if strings.TrimSpace(string(stdout)) != "flowerpot_test_value" {
		t.Fatalf("expected 'flowerpot_test_value', got %q", string(stdout))
	}
}

func TestCommandExecutor_FlowerpotEnvVars(t *testing.T) {
	logDir := tempLogDir(t)
	e := &CommandExecutor{
		ProjectDir:   t.TempDir(),
		FlowerpotEnv: map[string]string{"FLOWERPOT_DAG_RUN_ID": "run-123"},
	}
	p := &config.Pipeline{Run: "echo $FLOWERPOT_DAG_RUN_ID-$FLOWERPOT_PIPELINE-$FLOWERPOT_ATTEMPT"}

	result, err := e.Execute(context.Background(), "my-pipeline", p, logDir, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	stdout, _ := os.ReadFile(result.StdoutPath)
	got := strings.TrimSpace(string(stdout))
	want := "run-123-my-pipeline-3"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestCommandExecutor_Cwd(t *testing.T) {
	logDir := tempLogDir(t)
	workDir := t.TempDir()
	e := &CommandExecutor{ProjectDir: t.TempDir()}
	p := &config.Pipeline{Run: "pwd", Cwd: workDir}

	result, err := e.Execute(context.Background(), "test", p, logDir, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	stdout, _ := os.ReadFile(result.StdoutPath)
	got := strings.TrimSpace(string(stdout))
	// Resolve symlinks for macOS /private/var/... vs /var/...
	wantResolved, _ := filepath.EvalSymlinks(workDir)
	gotResolved, _ := filepath.EvalSymlinks(got)
	if gotResolved != wantResolved {
		t.Fatalf("expected cwd %q, got %q", wantResolved, gotResolved)
	}
}

func TestCommandExecutor_LogCapture(t *testing.T) {
	logDir := tempLogDir(t)
	e := &CommandExecutor{ProjectDir: t.TempDir()}
	p := &config.Pipeline{Run: "echo out && echo err >&2"}

	result, err := e.Execute(context.Background(), "test", p, logDir, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	stdout, _ := os.ReadFile(result.StdoutPath)
	if strings.TrimSpace(string(stdout)) != "out" {
		t.Fatalf("expected stdout 'out', got %q", string(stdout))
	}

	stderr, _ := os.ReadFile(result.StderrPath)
	if strings.TrimSpace(string(stderr)) != "err" {
		t.Fatalf("expected stderr 'err', got %q", string(stderr))
	}
}

func TestCommandExecutor_StdinClosed(t *testing.T) {
	logDir := tempLogDir(t)
	e := &CommandExecutor{ProjectDir: t.TempDir()}
	// read with a 1-second timeout — should fail/return immediately since stdin is closed
	p := &config.Pipeline{Run: "read -t 1 line; echo done"}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := e.Execute(ctx, "test", p, logDir, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	stdout, _ := os.ReadFile(result.StdoutPath)
	if !strings.Contains(string(stdout), "done") {
		t.Fatalf("expected 'done' in stdout, got %q", string(stdout))
	}
	if result.Duration > 4*time.Second {
		t.Fatalf("command took too long (%v), stdin may not be closed", result.Duration)
	}
}
