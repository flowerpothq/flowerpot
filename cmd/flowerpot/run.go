package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/flowerpothq/flowerpot/internal/config"
	"github.com/flowerpothq/flowerpot/internal/executor"
	"github.com/flowerpothq/flowerpot/internal/logs"
	"github.com/flowerpothq/flowerpot/internal/runner"
	"github.com/flowerpothq/flowerpot/internal/state"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

var errRunFailed = errors.New("run failed")

type runResult struct {
	Pipeline string `json:"pipeline"`
	Status   string `json:"status"`
	ExitCode int    `json:"exit_code"`
	Attempts int    `json:"attempts"`
	Duration string `json:"duration"`
	DagRunID string `json:"dag_run_id"`
	LogDir   string `json:"log_dir"`
}

func runCmd() *cobra.Command {
	var (
		jsonOutput bool
		configPath string
		timeout    time.Duration
	)

	cmd := &cobra.Command{
		Use:   "run <pipeline>",
		Short: "Execute a single pipeline",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				result, loadErr := config.Load(configPath)
				if loadErr == nil && !result.HasErrors() && len(result.Config.Pipelines) > 0 {
					msg := "missing pipeline name. available pipelines:"
					for name := range result.Config.Pipelines {
						msg += "\n  - " + name
					}
					return fmt.Errorf("%s", msg)
				}
				return fmt.Errorf("missing pipeline name\n\nUsage: flowerpot run <pipeline>")
			}
			if len(args) > 1 {
				return fmt.Errorf("expected 1 pipeline name, got %d", len(args))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPipeline(cmd, args[0], configPath, jsonOutput, timeout)
		},
		SilenceUsage:  true,
		SilenceErrors: true,
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			result, err := config.Load(configPath)
			if err != nil || result.HasErrors() {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			names := make([]string, 0, len(result.Config.Pipelines))
			for name := range result.Config.Pipelines {
				names = append(names, name)
			}
			return names, cobra.ShellCompDirectiveNoFileComp
		},
	}

	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output result as JSON")
	cmd.Flags().StringVarP(&configPath, "config", "c", "flowerpot.yaml", "Path to flowerpot.yaml")
	cmd.Flags().DurationVar(&timeout, "timeout", 0, "Override pipeline timeout")

	return cmd
}

func runPipeline(cmd *cobra.Command, pipelineName, configPath string, jsonOutput bool, timeoutOverride time.Duration) error {
	result, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render("✗ "+err.Error()))
		return errRunFailed
	}
	if result.HasErrors() {
		for _, e := range result.Errors {
			fmt.Fprintln(os.Stderr, styleFail.Render("✗ "+e.Error()))
		}
		return errRunFailed
	}

	p, ok := result.Config.Pipelines[pipelineName]
	if !ok {
		fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("✗ pipeline %q not found", pipelineName)))
		if len(result.Config.Pipelines) > 0 {
			fmt.Fprintln(os.Stderr, "  available pipelines:")
			for name := range result.Config.Pipelines {
				fmt.Fprintf(os.Stderr, "    - %s\n", name)
			}
		}
		return errRunFailed
	}

	if p.SQL != "" {
		fmt.Fprintln(os.Stderr, styleFail.Render("✗ SQL pipelines are not yet supported in 'run' (use run: commands)"))
		return errRunFailed
	}

	projectDir := filepath.Dir(configPath)
	if !filepath.IsAbs(projectDir) {
		abs, err := filepath.Abs(projectDir)
		if err == nil {
			projectDir = abs
		}
	}

	dbPath := filepath.Join(projectDir, ".flowerpot", "state.db")
	store, err := state.Open(dbPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render("✗ opening state: "+err.Error()))
		return errRunFailed
	}
	defer func() { _ = store.Close() }()

	dagRunID := uuid.New().String()
	taskID := uuid.New().String()
	now := time.Now().UTC().Format(time.RFC3339)

	if err := store.InsertDAGRun(&state.DAGRun{
		ID:            dagRunID,
		TriggerSource: "cli",
		StartedAt:     now,
		Status:        "running",
	}); err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render("✗ creating DAG run: "+err.Error()))
		return errRunFailed
	}

	if err := store.InsertTask(&state.Task{
		ID:       taskID,
		DAGRunID: dagRunID,
		Pipeline: pipelineName,
		Status:   "pending",
		Attempt:  1,
	}); err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render("✗ creating task: "+err.Error()))
		return errRunFailed
	}

	logDir, err := logs.CreateLogDir(projectDir, dagRunID)
	if err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render("✗ creating log dir: "+err.Error()))
		return errRunFailed
	}

	ctx := context.Background()
	if timeoutOverride > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeoutOverride)
		defer cancel()
	} else if p.Timeout != "" {
		if d, parseErr := time.ParseDuration(p.Timeout); parseErr == nil && d > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, d)
			defer cancel()
		}
	}

	wrapper, wrapErr := executor.NewWrapper(p)
	if wrapErr != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render("✗ runtime wrapper: "+wrapErr.Error()))
		return errRunFailed
	}

	exec := &executor.CommandExecutor{
		ProjectDir: projectDir,
		FlowerpotEnv: map[string]string{
			"FLOWERPOT_DAG_RUN_ID": dagRunID,
		},
		Wrapper: wrapper,
	}

	rc := runner.DefaultRetryConfig(p, result.Config.DefaultRetry)

	retryResult, execErr := runner.ExecuteWithRetry(ctx, pipelineName, p, exec, rc, logDir, taskID, store)

	endedAt := time.Now().UTC().Format(time.RFC3339)
	runStatus := "completed"
	exitCode := 0
	if execErr != nil || (retryResult.Result != nil && retryResult.ExitCode != 0) {
		runStatus = "failed"
		if retryResult.Result != nil {
			exitCode = retryResult.ExitCode
		} else {
			exitCode = -1
		}
	}

	_ = store.UpdateDAGRun(dagRunID, runStatus, endedAt)

	duration := time.Duration(0)
	if retryResult.Result != nil {
		duration = retryResult.Duration
	}

	rr := runResult{
		Pipeline: pipelineName,
		Status:   runStatus,
		ExitCode: exitCode,
		Attempts: retryResult.Attempts,
		Duration: duration.Truncate(time.Millisecond).String(),
		DagRunID: dagRunID,
		LogDir:   logDir,
	}

	if jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(rr)
	} else {
		if runStatus == "completed" {
			fmt.Println(stylePass.Render(fmt.Sprintf("✓ %s completed (exit 0, %s)", pipelineName, rr.Duration)))
		} else {
			fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("✗ %s failed (exit %d, %s)", pipelineName, exitCode, rr.Duration)))
		}
	}

	if runStatus != "completed" {
		return errRunFailed
	}
	return nil
}
