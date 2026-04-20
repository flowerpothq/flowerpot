package runner

import (
	"context"
	"fmt"
	"time"

	"github.com/flowerpothq/flowerpot/internal/config"
	"github.com/flowerpothq/flowerpot/internal/executor"
)

// BackoffStrategy determines the delay between retry attempts.
type BackoffStrategy int

const (
	BackoffFixed       BackoffStrategy = iota
	BackoffExponential
)

// RetryConfig controls retry behavior for ExecuteWithRetry.
type RetryConfig struct {
	MaxAttempts int
	BaseDelay   time.Duration
	Strategy    BackoffStrategy
}

// DefaultRetryConfig returns retry settings for a pipeline, merging pipeline-level
// and global defaults.
func DefaultRetryConfig(p *config.Pipeline, globalRetry *config.RetryConfig) RetryConfig {
	rc := RetryConfig{
		MaxAttempts: 1,
		BaseDelay:   time.Minute,
		Strategy:    BackoffFixed,
	}

	src := globalRetry
	if p.Retry != nil {
		src = p.Retry
	}
	if src == nil {
		return rc
	}

	if src.Attempts > 0 {
		rc.MaxAttempts = src.Attempts
	}
	if src.Delay != "" {
		if d, err := time.ParseDuration(src.Delay); err == nil {
			rc.BaseDelay = d
		}
	}
	if src.Strategy == "exponential" {
		rc.Strategy = BackoffExponential
	}

	return rc
}

func computeDelay(rc RetryConfig, attempt int) time.Duration {
	switch rc.Strategy {
	case BackoffExponential:
		d := rc.BaseDelay
		for i := 1; i < attempt; i++ {
			d *= 2
		}
		return d
	default:
		return rc.BaseDelay
	}
}

// TaskUpdater is the subset of state.Store needed by the retry engine.
type TaskUpdater interface {
	UpdateTask(id, status string, exitCode *int, startedAt, endedAt string) error
}

// RetryResult wraps the executor result with retry metadata.
type RetryResult struct {
	*executor.Result
	Attempts int
}

// ExecuteWithRetry runs the executor up to MaxAttempts times, respecting backoff
// and context cancellation. Each attempt gets its own log files.
func ExecuteWithRetry(
	ctx context.Context,
	name string,
	p *config.Pipeline,
	exec executor.Executor,
	rc RetryConfig,
	logDir string,
	taskID string,
	store TaskUpdater,
) (*RetryResult, error) {
	var lastResult *executor.Result

	for attempt := 1; attempt <= rc.MaxAttempts; attempt++ {
		startedAt := time.Now().UTC()

		result, execErr := exec.Execute(ctx, name, p, logDir, attempt)
		lastResult = result

		endedAt := time.Now().UTC()

		if store != nil && taskID != "" {
			status := "completed"
			if (result != nil && result.ExitCode != 0) || execErr != nil {
				status = "failed"
			}
			var exitCode *int
			if result != nil {
				ec := result.ExitCode
				exitCode = &ec
			}
			_ = store.UpdateTask(taskID, status, exitCode,
				startedAt.Format(time.RFC3339),
				endedAt.Format(time.RFC3339))
		}

		if execErr != nil {
			if ctx.Err() != nil {
				return &RetryResult{lastResult, attempt}, ctx.Err()
			}
			return &RetryResult{lastResult, attempt}, fmt.Errorf("attempt %d: %w", attempt, execErr)
		}

		if result.ExitCode == 0 {
			return &RetryResult{result, attempt}, nil
		}

		if attempt < rc.MaxAttempts {
			delay := computeDelay(rc, attempt)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return &RetryResult{lastResult, attempt}, ctx.Err()
			}

			if store != nil && taskID != "" {
				_ = store.UpdateTask(taskID, "pending", nil, "", "")
			}
		}
	}

	return &RetryResult{lastResult, rc.MaxAttempts}, fmt.Errorf("pipeline %q failed after %d attempts (exit code %d)", name, rc.MaxAttempts, lastResult.ExitCode)
}
