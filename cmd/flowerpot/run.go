package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
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

type dagRunResult struct {
	DagRunID string       `json:"dag_run_id"`
	Status   string       `json:"status"`
	Duration string       `json:"duration"`
	Tasks    []taskResult `json:"tasks"`
}

type taskResult struct {
	Pipeline string `json:"pipeline"`
	Status   string `json:"status"`
	ExitCode *int   `json:"exit_code"`
	Duration string `json:"duration,omitempty"`
}

// dagRunContext holds everything needed to execute a DAG run.
type dagRunContext struct {
	Config     *config.Config
	ProjectDir string
	Store      *state.Store
	DagRunID   string
	LogDir     string
	Deps       map[string][]string
	MaxWorkers int
}

func runCmd() *cobra.Command {
	var (
		jsonOutput   bool
		configPath   string
		timeout      time.Duration
		withUpstream bool
	)

	cmd := &cobra.Command{
		Use:   "run [pipeline]",
		Short: "Execute a single pipeline or the full DAG",
		Long:  "Run a single pipeline by name, or execute the entire DAG\nwhen no pipeline is specified.",
		Example: `flowerpot run                              run full DAG
flowerpot run extract                      run single pipeline
flowerpot run transform --with-upstream    run with dependencies
flowerpot run load --json                  JSON output
flowerpot run extract --timeout 30s        override timeout`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return runDAG(configPath, jsonOutput, "", false)
			}
			if withUpstream {
				return runDAG(configPath, jsonOutput, args[0], true)
			}
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
	cmd.Flags().DurationVar(&timeout, "timeout", 0, "Override pipeline timeout (single pipeline only)")
	cmd.Flags().BoolVar(&withUpstream, "with-upstream", false, "Also run transitive upstream dependencies")

	return cmd
}

func loadAndValidate(configPath string) (*config.ParseResult, string, error) {
	result, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render("  "+iconFail+" "+err.Error()))
		return nil, "", errRunFailed
	}
	if result.HasErrors() {
		for _, e := range result.Errors {
			fmt.Fprintln(os.Stderr, styleFail.Render("  "+iconFail+" "+e.Error()))
		}
		return nil, "", errRunFailed
	}

	projectDir := filepath.Dir(configPath)
	if !filepath.IsAbs(projectDir) {
		abs, absErr := filepath.Abs(projectDir)
		if absErr == nil {
			projectDir = abs
		}
	}
	return result, projectDir, nil
}

func openStore(projectDir string) (*state.Store, error) {
	dbPath := filepath.Join(projectDir, ".flowerpot", "state.db")
	store, err := state.Open(dbPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render("  "+iconFail+" opening state: "+err.Error()))
		return nil, errRunFailed
	}
	return store, nil
}

// prepareDagRun loads config, opens store, inserts a dag_run + tasks, and
// creates the log directory. pipelineNames selects which pipelines to include;
// nil means all pipelines.
func prepareDagRun(configPath, triggerSource string, pipelineNames map[string]bool, deps map[string][]string) (*dagRunContext, error) {
	result, projectDir, err := loadAndValidate(configPath)
	if err != nil {
		return nil, err
	}

	store, err := openStore(projectDir)
	if err != nil {
		return nil, err
	}

	dagRunID := uuid.New().String()
	now := time.Now().UTC().Format(time.RFC3339)

	if err := store.InsertDAGRun(&state.DAGRun{
		ID:            dagRunID,
		TriggerSource: triggerSource,
		StartedAt:     now,
		Status:        "running",
	}); err != nil {
		_ = store.Close()
		fmt.Fprintln(os.Stderr, styleFail.Render("  "+iconFail+" creating DAG run: "+err.Error()))
		return nil, errRunFailed
	}

	if pipelineNames == nil {
		pipelineNames = make(map[string]bool, len(result.Config.Pipelines))
		for name := range result.Config.Pipelines {
			pipelineNames[name] = true
		}
	}

	if deps == nil {
		deps = result.Config.DAGGraph()
	}

	for name := range pipelineNames {
		taskID := uuid.New().String()
		if err := store.InsertTask(&state.Task{
			ID:       taskID,
			DAGRunID: dagRunID,
			Pipeline: name,
			Status:   "pending",
			Attempt:  1,
		}); err != nil {
			_ = store.Close()
			fmt.Fprintln(os.Stderr, styleFail.Render("  "+iconFail+" creating task: "+err.Error()))
			return nil, errRunFailed
		}
	}

	logDir, err := logs.CreateLogDir(projectDir, dagRunID)
	if err != nil {
		_ = store.Close()
		fmt.Fprintln(os.Stderr, styleFail.Render("  "+iconFail+" creating log dir: "+err.Error()))
		return nil, errRunFailed
	}

	maxWorkers := result.Config.MaxConcurrent
	if maxWorkers <= 0 {
		maxWorkers = 4
	}

	return &dagRunContext{
		Config:     result.Config,
		ProjectDir: projectDir,
		Store:      store,
		DagRunID:   dagRunID,
		LogDir:     logDir,
		Deps:       deps,
		MaxWorkers: maxWorkers,
	}, nil
}

// runDAG executes a full DAG or a pipeline with its upstream dependencies.
// If targetPipeline is empty, all pipelines are included.
func runDAG(configPath string, jsonOutput bool, targetPipeline string, withUpstream bool) error {
	headerLabel := "run"
	if targetPipeline != "" {
		headerLabel = "run --with-upstream"
	}
	if !jsonOutput {
		fmt.Print(cmdHeader(headerLabel))
	}

	var pipelineSet map[string]bool
	var subDeps map[string][]string

	if targetPipeline != "" {
		result, _, err := loadAndValidate(configPath)
		if err != nil {
			return err
		}
		if _, ok := result.Config.Pipelines[targetPipeline]; !ok {
			fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s pipeline %q not found", iconFail, targetPipeline)))
			return errRunFailed
		}

		deps := result.Config.DAGGraph()
		upstreams := runner.UpstreamOf(targetPipeline, deps)

		pipelineSet = make(map[string]bool, len(upstreams)+1)
		pipelineSet[targetPipeline] = true
		for _, u := range upstreams {
			pipelineSet[u] = true
		}

		subDeps = make(map[string][]string, len(pipelineSet))
		for p := range pipelineSet {
			var relevant []string
			for _, dep := range deps[p] {
				if pipelineSet[dep] {
					relevant = append(relevant, dep)
				}
			}
			subDeps[p] = relevant
		}
	}

	drc, err := prepareDagRun(configPath, "cli", pipelineSet, subDeps)
	if err != nil {
		return err
	}
	defer func() { _ = drc.Store.Close() }()

	dagRunner := &runner.DAGRunner{
		Store:      drc.Store,
		Config:     drc.Config,
		ProjectDir: drc.ProjectDir,
		DagRunID:   drc.DagRunID,
		LogDir:     drc.LogDir,
		MaxWorkers: drc.MaxWorkers,
		Deps:       drc.Deps,
	}

	start := time.Now()
	dagStatus, runErr := dagRunner.Run(context.Background())
	duration := time.Since(start)

	return printDAGResult(drc.Store, drc.DagRunID, dagStatus, duration, jsonOutput, runErr)
}

func printDAGResult(store *state.Store, dagRunID, dagStatus string, duration time.Duration, jsonOutput bool, runErr error) error {
	tasks, _ := store.TasksByRun(dagRunID)

	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].StartedAt < tasks[j].StartedAt
	})

	if jsonOutput {
		dr := dagRunResult{
			DagRunID: dagRunID,
			Status:   dagStatus,
			Duration: duration.Truncate(time.Millisecond).String(),
		}
		for _, t := range tasks {
			tr := taskResult{
				Pipeline: t.Pipeline,
				Status:   t.Status,
				ExitCode: t.ExitCode,
			}
			if t.StartedAt != "" && t.EndedAt != "" {
				if s, e := parseTime(t.StartedAt), parseTime(t.EndedAt); !s.IsZero() && !e.IsZero() {
					tr.Duration = e.Sub(s).Truncate(time.Millisecond).String()
				}
			}
			dr.Tasks = append(dr.Tasks, tr)
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(dr)
	} else {
		maxName := 0
		for _, t := range tasks {
			if len(t.Pipeline) > maxName {
				maxName = len(t.Pipeline)
			}
		}

		fmt.Println(styleDim.Render(fmt.Sprintf("  %s %s", iconSection, dagRunID[:8])))
		fmt.Println()

		for _, t := range tasks {
			dur := ""
			if t.StartedAt != "" && t.EndedAt != "" {
				if s, e := parseTime(t.StartedAt), parseTime(t.EndedAt); !s.IsZero() && !e.IsZero() {
					dur = e.Sub(s).Truncate(time.Millisecond).String()
				}
			}

			icon := statusIcon(t.Status)
			st := statusStyle(t.Status)
			name := padRight(t.Pipeline, maxName)

			detail := t.Status
			if dur != "" {
				detail += "  " + dur
			}
			if t.Status == "failed" && t.ExitCode != nil {
				detail += fmt.Sprintf("  exit %d", *t.ExitCode)
			}

			fmt.Println(st.Render(fmt.Sprintf("    %s %s  %s", icon, name, detail)))
		}

		var completed, failed, skipped int
		for _, t := range tasks {
			switch t.Status {
			case "completed":
				completed++
			case "failed":
				failed++
			case "skipped":
				skipped++
			}
		}

		fmt.Println()
		summary := fmt.Sprintf("%d ok, %d failed, %d skipped", completed, failed, skipped)
		durStr := duration.Truncate(time.Millisecond).String()
		st := statusStyle(dagStatus)
		icon := statusIcon(dagStatus)
		label := dagStatus
		if dagStatus == "partial_failure" {
			label = "partial failure"
		}
		line := fmt.Sprintf("  %s %s — %s (%s)", icon, label, summary, durStr)

		if dagStatus == "completed" {
			fmt.Println(st.Render(line))
		} else {
			fmt.Fprintln(os.Stderr, st.Render(line))
		}
		fmt.Println()
	}

	if dagStatus != "completed" {
		return errRunFailed
	}
	if runErr != nil {
		return errRunFailed
	}
	return nil
}

func parseTime(s string) time.Time {
	t, _ := time.Parse(time.RFC3339, s)
	return t
}

func runPipeline(cmd *cobra.Command, pipelineName, configPath string, jsonOutput bool, timeoutOverride time.Duration) error {
	if !jsonOutput {
		fmt.Print(cmdHeader("run " + pipelineName))
	}

	result, projectDir, err := loadAndValidate(configPath)
	if err != nil {
		return err
	}

	p, ok := result.Config.Pipelines[pipelineName]
	if !ok {
		fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s pipeline %q not found", iconFail, pipelineName)))
		if len(result.Config.Pipelines) > 0 {
			fmt.Fprintln(os.Stderr, styleDim.Render("  available:"))
			for name := range result.Config.Pipelines {
				fmt.Fprintf(os.Stderr, "    %s\n", styleDim.Render(name))
			}
		}
		fmt.Println()
		return errRunFailed
	}

	if len(p.After) > 0 {
		fmt.Fprintln(os.Stderr, styleWarn.Render(
			fmt.Sprintf("  %s %s has upstream deps %v — use --with-upstream to include", iconWarn, pipelineName, p.After)))
		fmt.Println()
	}

	store, storeErr := openStore(projectDir)
	if storeErr != nil {
		return storeErr
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
		fmt.Fprintln(os.Stderr, styleFail.Render("  "+iconFail+" creating DAG run: "+err.Error()))
		return errRunFailed
	}

	if err := store.InsertTask(&state.Task{
		ID:       taskID,
		DAGRunID: dagRunID,
		Pipeline: pipelineName,
		Status:   "pending",
		Attempt:  1,
	}); err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render("  "+iconFail+" creating task: "+err.Error()))
		return errRunFailed
	}

	logDir, logErr := logs.CreateLogDir(projectDir, dagRunID)
	if logErr != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render("  "+iconFail+" creating log dir: "+logErr.Error()))
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

	var exec executor.Executor
	if p.SQL != "" {
		exec = &executor.SqlExecutor{
			ProjectDir: projectDir,
			Warehouses: result.Config.Warehouses,
		}
	} else {
		wrapper, wrapErr := executor.NewWrapper(p)
		if wrapErr != nil {
			fmt.Fprintln(os.Stderr, styleFail.Render("  "+iconFail+" runtime wrapper: "+wrapErr.Error()))
			return errRunFailed
		}
		exec = &executor.CommandExecutor{
			ProjectDir: projectDir,
			FlowerpotEnv: map[string]string{
				"FLOWERPOT_DAG_RUN_ID":   dagRunID,
				"FLOWERPOT_LOGICAL_DATE": time.Now().UTC().Format(time.RFC3339),
			},
			Wrapper: wrapper,
		}
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
			fmt.Println(stylePass.Render(fmt.Sprintf("  %s %s  completed  %s", iconPass, pipelineName, rr.Duration)))
		} else {
			fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s %s  failed  exit %d  %s", iconFail, pipelineName, exitCode, rr.Duration)))
		}
		fmt.Println()
	}

	if runStatus != "completed" {
		return errRunFailed
	}
	return nil
}
