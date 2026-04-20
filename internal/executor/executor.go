package executor

import (
	"context"
	"time"

	"github.com/flowerpothq/flowerpot/internal/config"
)

// Result holds the outcome of a pipeline execution.
type Result struct {
	ExitCode   int
	Duration   time.Duration
	StdoutPath string
	StderrPath string
}

// Executor runs a single pipeline and returns its result.
type Executor interface {
	Execute(ctx context.Context, name string, p *config.Pipeline, logDir string, attempt int) (*Result, error)
}
