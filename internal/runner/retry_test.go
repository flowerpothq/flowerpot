package runner

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/flowerpothq/flowerpot/internal/config"
	"github.com/flowerpothq/flowerpot/internal/executor"
)

// mockExecutor allows controlling execution results per attempt.
type mockExecutor struct {
	attempts   int
	results    []int // exit codes per attempt; if shorter than attempts, last repeats
	timestamps []time.Time
}

func (m *mockExecutor) Execute(ctx context.Context, name string, p *config.Pipeline, logDir string, attempt int) (*executor.Result, error) {
	m.attempts++
	m.timestamps = append(m.timestamps, time.Now())

	exitCode := 1
	idx := attempt - 1
	if idx < len(m.results) {
		exitCode = m.results[idx]
	} else if len(m.results) > 0 {
		exitCode = m.results[len(m.results)-1]
	}

	stdoutPath := filepath.Join(logDir, fmt.Sprintf("%s.stdout.attempt-%d", name, attempt))
	stderrPath := filepath.Join(logDir, fmt.Sprintf("%s.stderr.attempt-%d", name, attempt))
	_ = os.WriteFile(stdoutPath, []byte(fmt.Sprintf("attempt %d\n", attempt)), 0o644)
	_ = os.WriteFile(stderrPath, []byte{}, 0o644)

	return &executor.Result{
		ExitCode:   exitCode,
		Duration:   time.Millisecond,
		StdoutPath: stdoutPath,
		StderrPath: stderrPath,
	}, nil
}

func TestRetry_SuccessOnFirst(t *testing.T) {
	logDir := t.TempDir()
	m := &mockExecutor{results: []int{0}}
	p := &config.Pipeline{Run: "echo ok"}
	rc := RetryConfig{MaxAttempts: 3, BaseDelay: time.Millisecond, Strategy: BackoffFixed}

	result, err := ExecuteWithRetry(context.Background(), "test", p, m, rc, logDir, "", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", result.ExitCode)
	}
	if result.Attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", result.Attempts)
	}
	if m.attempts != 1 {
		t.Fatalf("expected mock called 1 time, got %d", m.attempts)
	}
}

func TestRetry_SuccessOnThird(t *testing.T) {
	logDir := t.TempDir()
	m := &mockExecutor{results: []int{1, 1, 0}}
	p := &config.Pipeline{Run: "sometimes-fails"}
	rc := RetryConfig{MaxAttempts: 3, BaseDelay: time.Millisecond, Strategy: BackoffFixed}

	result, err := ExecuteWithRetry(context.Background(), "test", p, m, rc, logDir, "", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", result.ExitCode)
	}
	if result.Attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", result.Attempts)
	}
}

func TestRetry_ExhaustedRetries(t *testing.T) {
	logDir := t.TempDir()
	m := &mockExecutor{results: []int{1, 1}}
	p := &config.Pipeline{Run: "always-fails"}
	rc := RetryConfig{MaxAttempts: 2, BaseDelay: time.Millisecond, Strategy: BackoffFixed}

	result, err := ExecuteWithRetry(context.Background(), "test", p, m, rc, logDir, "", nil)
	if err == nil {
		t.Fatal("expected error for exhausted retries, got nil")
	}
	if !strings.Contains(err.Error(), "failed after 2 attempts") {
		t.Fatalf("unexpected error message: %v", err)
	}
	if result.ExitCode != 1 {
		t.Fatalf("expected exit code 1, got %d", result.ExitCode)
	}
	if result.Attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", result.Attempts)
	}
}

func TestRetry_ExponentialBackoff(t *testing.T) {
	logDir := t.TempDir()
	m := &mockExecutor{results: []int{1, 1, 0}}
	p := &config.Pipeline{Run: "backoff-test"}
	rc := RetryConfig{MaxAttempts: 3, BaseDelay: 50 * time.Millisecond, Strategy: BackoffExponential}

	_, err := ExecuteWithRetry(context.Background(), "test", p, m, rc, logDir, "", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(m.timestamps) != 3 {
		t.Fatalf("expected 3 timestamps, got %d", len(m.timestamps))
	}

	delay1 := m.timestamps[1].Sub(m.timestamps[0])
	delay2 := m.timestamps[2].Sub(m.timestamps[1])

	// First delay should be ~50ms, second ~100ms (with tolerance)
	if delay1 < 30*time.Millisecond {
		t.Fatalf("first delay too short: %v", delay1)
	}
	if delay2 < 60*time.Millisecond {
		t.Fatalf("second delay too short: %v (should be ~2x first)", delay2)
	}
	if delay2 < delay1 {
		t.Fatalf("expected exponential growth: delay2 (%v) < delay1 (%v)", delay2, delay1)
	}
}

func TestRetry_RespectsContextCancel(t *testing.T) {
	logDir := t.TempDir()
	m := &mockExecutor{results: []int{1, 1, 1}}
	p := &config.Pipeline{Run: "cancel-test"}
	rc := RetryConfig{MaxAttempts: 3, BaseDelay: 5 * time.Second, Strategy: BackoffFixed}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err := ExecuteWithRetry(ctx, "test", p, m, rc, logDir, "", nil)
	elapsed := time.Since(start)

	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("should have cancelled quickly, took %v", elapsed)
	}
}

func TestRetry_PerAttemptLogs(t *testing.T) {
	logDir := t.TempDir()
	m := &mockExecutor{results: []int{1, 0}}
	p := &config.Pipeline{Run: "log-test"}
	rc := RetryConfig{MaxAttempts: 2, BaseDelay: time.Millisecond, Strategy: BackoffFixed}

	_, err := ExecuteWithRetry(context.Background(), "log-test", p, m, rc, logDir, "", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check attempt 1 log
	log1, err := os.ReadFile(filepath.Join(logDir, "log-test.stdout.attempt-1"))
	if err != nil {
		t.Fatalf("attempt 1 log not found: %v", err)
	}
	if !strings.Contains(string(log1), "attempt 1") {
		t.Fatalf("expected 'attempt 1' in log, got %q", string(log1))
	}

	// Check attempt 2 log
	log2, err := os.ReadFile(filepath.Join(logDir, "log-test.stdout.attempt-2"))
	if err != nil {
		t.Fatalf("attempt 2 log not found: %v", err)
	}
	if !strings.Contains(string(log2), "attempt 2") {
		t.Fatalf("expected 'attempt 2' in log, got %q", string(log2))
	}
}
