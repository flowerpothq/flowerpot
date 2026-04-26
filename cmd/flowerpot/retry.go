package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/flowerpothq/flowerpot/internal/logs"
	"github.com/flowerpothq/flowerpot/internal/runner"
	"github.com/flowerpothq/flowerpot/internal/state"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

func retryCmd() *cobra.Command {
	var (
		configPath string
		jsonOutput bool
	)

	cmd := &cobra.Command{
		Use:   "retry <dag-run-id>",
		Short: "Retry failed/skipped tasks from a previous run",
		Long: `Creates a new DAG run linked to the original via retry_of.
Completed tasks are marked skipped_on_retry (not re-run).
Failed and skipped tasks are set to pending and re-executed.
Uses the current flowerpot.yaml, not the original config.`,
		Example: `flowerpot retry abc123
flowerpot retry abc123 --json`,
		Args:          cobra.ExactArgs(1),
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runRetry(args[0], configPath, jsonOutput)
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "flowerpot.yaml", "Path to flowerpot.yaml")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output result as JSON")

	return cmd
}

func runRetry(runPrefix, configPath string, jsonOutput bool) error {
	if !jsonOutput {
		fmt.Print(cmdHeader("retry"))
	}

	result, projectDir, err := loadAndValidate(configPath)
	if err != nil {
		return err
	}

	store, err := openStore(projectDir)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	originalRun, err := store.FindDAGRunByPrefix(runPrefix)
	if err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s run %q not found: %s", iconFail, runPrefix, err)))
		return errRunFailed
	}

	if originalRun.Status == "running" {
		fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s run %s is still running", iconFail, originalRun.ID[:8])))
		return errRunFailed
	}

	originalTasks, err := store.TasksByRun(originalRun.ID)
	if err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s reading tasks: %s", iconFail, err)))
		return errRunFailed
	}

	deps := result.Config.DAGGraph()

	retryRunID := uuid.New().String()
	now := time.Now().UTC().Format(time.RFC3339)

	if err := store.InsertDAGRun(&state.DAGRun{
		ID:            retryRunID,
		TriggerSource: "retry",
		StartedAt:     now,
		Status:        "running",
		RetryOf:       originalRun.ID,
	}); err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s creating retry run: %s", iconFail, err)))
		return errRunFailed
	}

	originalStatusMap := make(map[string]string, len(originalTasks))
	for _, t := range originalTasks {
		originalStatusMap[t.Pipeline] = t.Status
	}

	pendingSet := make(map[string]bool)
	for _, t := range originalTasks {
		if t.Status == "failed" || t.Status == "skipped" {
			pendingSet[t.Pipeline] = true
		}
	}

	// Any task downstream of a pending task must also become pending,
	// even if it was completed in the original run.
	fwd := runner.ForwardGraph(deps)
	changed := true
	for changed {
		changed = false
		for p := range pendingSet {
			for _, child := range fwd[p] {
				if !pendingSet[child] {
					pendingSet[child] = true
					changed = true
				}
			}
		}
	}

	for name := range result.Config.Pipelines {
		taskID := uuid.New().String()
		status := "skipped_on_retry"
		if pendingSet[name] {
			status = "pending"
		}
		if err := store.InsertTask(&state.Task{
			ID:       taskID,
			DAGRunID: retryRunID,
			Pipeline: name,
			Status:   status,
			Attempt:  1,
		}); err != nil {
			fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s creating task: %s", iconFail, err)))
			return errRunFailed
		}
	}

	logDir, err := logs.CreateLogDir(projectDir, retryRunID)
	if err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s creating log dir: %s", iconFail, err)))
		return errRunFailed
	}

	maxWorkers := result.Config.MaxConcurrent
	if maxWorkers <= 0 {
		maxWorkers = 4
	}

	dagRunner := &runner.DAGRunner{
		Store:      store,
		Config:     result.Config,
		ProjectDir: projectDir,
		DagRunID:   retryRunID,
		LogDir:     logDir,
		MaxWorkers: maxWorkers,
		Deps:       deps,
	}

	if !jsonOutput {
		fmt.Println(styleDim.Render(fmt.Sprintf("  retrying %s → %s", originalRun.ID[:8], retryRunID[:8])))
		fmt.Println()
	}

	start := time.Now()
	dagStatus, runErr := dagRunner.Run(context.Background())
	duration := time.Since(start)

	return printDAGResult(store, retryRunID, dagStatus, duration, jsonOutput, runErr)
}
