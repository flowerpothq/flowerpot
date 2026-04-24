package executor

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/flowerpothq/flowerpot/internal/config"
)

const killGracePeriod = 10 * time.Second

// CommandExecutor runs pipelines that have a run: field via sh -c.
type CommandExecutor struct {
	// ProjectDir is the root directory of the flowerpot project (where flowerpot.yaml lives).
	ProjectDir string

	// FlowerpotEnv holds extra env vars injected by the runtime
	// (FLOWERPOT_DAG_RUN_ID, FLOWERPOT_PIPELINE, etc.).
	FlowerpotEnv map[string]string

	// Wrapper transforms the command before execution (e.g. uv, docker).
	// If nil, Passthrough is used.
	Wrapper RuntimeWrapper
}

func (e *CommandExecutor) Execute(ctx context.Context, name string, p *config.Pipeline, logDir string, attempt int) (*Result, error) {
	start := time.Now()

	w := e.Wrapper
	if w == nil {
		w = Passthrough{}
	}
	cmdArgs := w.Wrap([]string{"sh", "-c", p.Run})

	cmd := exec.CommandContext(ctx, cmdArgs[0], cmdArgs[1:]...)

	cmd.Dir = e.workDir(p)
	cmd.Env = e.buildEnv(name, p, attempt)
	cmd.Stdin = nil

	stdoutPath, stderrPath := logPaths(logDir, name, attempt)

	stdoutFile, err := os.Create(stdoutPath)
	if err != nil {
		return nil, fmt.Errorf("creating stdout log: %w", err)
	}
	defer func() { _ = stdoutFile.Close() }()

	stderrFile, err := os.Create(stderrPath)
	if err != nil {
		return nil, fmt.Errorf("creating stderr log: %w", err)
	}
	defer func() { _ = stderrFile.Close() }()

	cmd.Stdout = stdoutFile
	cmd.Stderr = stderrFile

	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		return &Result{ExitCode: -1, Duration: time.Since(start), StdoutPath: stdoutPath, StderrPath: stderrPath}, fmt.Errorf("starting command: %w", err)
	}

	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()

	select {
	case err := <-waitDone:
		return e.buildResult(cmd, err, start, stdoutPath, stderrPath), nil
	case <-ctx.Done():
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)

		select {
		case <-waitDone:
		case <-time.After(killGracePeriod):
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			<-waitDone
		}

		return &Result{
			ExitCode:   -1,
			Duration:   time.Since(start),
			StdoutPath: stdoutPath,
			StderrPath: stderrPath,
		}, ctx.Err()
	}
}

func (e *CommandExecutor) workDir(p *config.Pipeline) string {
	if p.Cwd != "" {
		if filepath.IsAbs(p.Cwd) {
			return p.Cwd
		}
		return filepath.Join(e.ProjectDir, p.Cwd)
	}
	return e.ProjectDir
}

func (e *CommandExecutor) buildEnv(name string, p *config.Pipeline, attempt int) []string {
	env := os.Environ()

	for k, v := range e.FlowerpotEnv {
		env = append(env, k+"="+v)
	}

	env = append(env, "FLOWERPOT_PIPELINE="+name)
	env = append(env, fmt.Sprintf("FLOWERPOT_ATTEMPT=%d", attempt))

	if _, exists := e.FlowerpotEnv["FLOWERPOT_LOGICAL_DATE"]; !exists {
		env = append(env, "FLOWERPOT_LOGICAL_DATE="+time.Now().UTC().Format(time.RFC3339))
	}

	for k, v := range p.Env {
		env = append(env, k+"="+v)
	}

	return env
}

func (e *CommandExecutor) buildResult(cmd *exec.Cmd, err error, start time.Time, stdoutPath, stderrPath string) *Result {
	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			exitCode = -1
		}
	}
	return &Result{
		ExitCode:   exitCode,
		Duration:   time.Since(start),
		StdoutPath: stdoutPath,
		StderrPath: stderrPath,
	}
}

func logPaths(logDir, pipeline string, attempt int) (stdout, stderr string) {
	safe := strings.ReplaceAll(pipeline, "/", "_")
	if attempt > 1 {
		return filepath.Join(logDir, fmt.Sprintf("%s.stdout.attempt-%d", safe, attempt)),
			filepath.Join(logDir, fmt.Sprintf("%s.stderr.attempt-%d", safe, attempt))
	}
	return filepath.Join(logDir, safe+".stdout"),
		filepath.Join(logDir, safe+".stderr")
}
