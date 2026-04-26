package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/flowerpothq/flowerpot/internal/config"
	"github.com/flowerpothq/flowerpot/internal/logs"
	runner "github.com/flowerpothq/flowerpot/internal/runner"
	"github.com/flowerpothq/flowerpot/internal/state"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

func triggerCmd() *cobra.Command {
	var configPath string

	cmd := &cobra.Command{
		Use:   "trigger [pipeline]",
		Short: "Trigger a DAG run via HTTP",
		Long: `Sends an HTTP POST to the running flowerpot serve process
to trigger an immediate DAG run.

Without arguments, triggers the full DAG.
With a pipeline name, triggers only that pipeline (and its upstream deps by default).`,
		Example: `flowerpot trigger                        trigger full DAG
flowerpot trigger extract                trigger single pipeline + upstream
flowerpot trigger extract ?scope=pipeline  trigger single pipeline only`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			pipeline := ""
			if len(args) == 1 {
				pipeline = args[0]
			}
			return runTrigger(configPath, pipeline)
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "flowerpot.yaml", "Path to flowerpot.yaml")
	return cmd
}

func runTrigger(configPath, pipeline string) error {
	fmt.Print(cmdHeader("trigger"))

	_, projectDir, err := loadAndValidate(configPath)
	if err != nil {
		return err
	}

	pidFile := filepath.Join(projectDir, ".flowerpot", "flowerpot.pid")
	_, port, err := readPIDFile(pidFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s no running daemon (PID file not found)", iconFail)))
		fmt.Fprintln(os.Stderr, styleDim.Render("  run "+styleBrand.Render("flowerpot serve")+styleDim.Render(" first")))
		fmt.Println()
		return errRunFailed
	}

	url := fmt.Sprintf("http://127.0.0.1:%d/trigger", port)
	if pipeline != "" {
		url = fmt.Sprintf("http://127.0.0.1:%d/trigger/%s", port, pipeline)
	}
	resp, err := http.Post(url, "application/json", nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s cannot reach daemon: %s", iconFail, err)))
		fmt.Println()
		return errRunFailed
	}
	defer func() { _ = resp.Body.Close() }()

	var result struct {
		DagRunID string `json:"dag_run_id"`
		Status   string `json:"status"`
		Pipeline string `json:"pipeline,omitempty"`
		Scope    string `json:"scope,omitempty"`
		Error    string `json:"error,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s invalid response: %s", iconFail, err)))
		fmt.Println()
		return errRunFailed
	}

	if resp.StatusCode == http.StatusConflict {
		fmt.Println(styleWarn.Render(fmt.Sprintf("  %s %s", iconWarn, result.Error)))
	} else if resp.StatusCode >= 400 {
		fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s %s", iconFail, result.Error)))
		fmt.Println()
		return errRunFailed
	} else {
		label := "triggered DAG run"
		if result.Pipeline != "" {
			label = fmt.Sprintf("triggered %s", result.Pipeline)
			if result.Scope == "pipeline" {
				label += " (single pipeline)"
			} else {
				label += " (with upstream)"
			}
		}
		fmt.Println(stylePass.Render(fmt.Sprintf("  %s %s %s", iconPass, label, result.DagRunID[:8])))
	}

	fmt.Println()
	return nil
}

func healthHandler(store *state.Store, startedAt time.Time) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hasRunning, _ := store.HasRunningRun()
		w.Header().Set("Content-Type", "application/json")
		status := "idle"
		if hasRunning {
			status = "running"
		}
		uptime := time.Since(startedAt).Truncate(time.Second).String()
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status": status,
			"uptime": uptime,
		})
	}
}

func pipelineTriggerHandler(cfg *config.Config, store *state.Store, projectDir string, logger *slog.Logger, runCtx context.Context, wg *sync.WaitGroup) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pipelineName := r.URL.Path[len("/trigger/"):]
		if pipelineName == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "pipeline name required"})
			return
		}

		if _, ok := cfg.Pipelines[pipelineName]; !ok {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("pipeline %q not found", pipelineName)})
			return
		}

		hasRunning, err := store.HasRunningRun()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		if hasRunning {
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"status": "skipped",
				"error":  "a DAG run is already in progress",
			})
			return
		}

		scope := r.URL.Query().Get("scope")
		if scope == "" {
			scope = "dag"
		}

		dagRunID := uuid.New().String()
		now := time.Now().UTC().Format(time.RFC3339)

		if err := store.InsertDAGRun(&state.DAGRun{
			ID:            dagRunID,
			TriggerSource: "http",
			StartedAt:     now,
			Status:        "running",
		}); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		deps := cfg.DAGGraph()
		var pipelineSet map[string]bool
		var subDeps map[string][]string

		if scope == "pipeline" {
			pipelineSet = map[string]bool{pipelineName: true}
			subDeps = map[string][]string{pipelineName: nil}
		} else {
			upstreams := runner.UpstreamOf(pipelineName, deps)
			pipelineSet = make(map[string]bool, len(upstreams)+1)
			pipelineSet[pipelineName] = true
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

		for name := range pipelineSet {
			taskID := uuid.New().String()
			_ = store.InsertTask(&state.Task{
				ID:       taskID,
				DAGRunID: dagRunID,
				Pipeline: name,
				Status:   "pending",
				Attempt:  1,
			})
		}

		logDir, err := logs.CreateLogDir(projectDir, dagRunID)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		maxWorkers := cfg.MaxConcurrent
		if maxWorkers <= 0 {
			maxWorkers = 4
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			dagRunner := &runner.DAGRunner{
				Store:      store,
				Config:     cfg,
				ProjectDir: projectDir,
				DagRunID:   dagRunID,
				LogDir:     logDir,
				MaxWorkers: maxWorkers,
				Deps:       subDeps,
			}
			status, runErr := dagRunner.Run(runCtx)
			if runErr != nil {
				logger.Error("triggered pipeline run failed", "pipeline", pipelineName, "dag_run_id", dagRunID[:8], "error", runErr)
			} else {
				logger.Info("triggered pipeline run finished", "pipeline", pipelineName, "dag_run_id", dagRunID[:8], "status", status)
			}
		}()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"dag_run_id": dagRunID,
			"pipeline":   pipelineName,
			"scope":      scope,
			"status":     "accepted",
		})
	}
}

func triggerHandler(cfg *config.Config, store *state.Store, projectDir string, logger *slog.Logger, runCtx context.Context, wg *sync.WaitGroup) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hasRunning, err := store.HasRunningRun()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		if hasRunning {
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"status": "skipped",
				"error":  "a DAG run is already in progress",
			})
			return
		}

		dagRunID := uuid.New().String()
		now := time.Now().UTC().Format(time.RFC3339)

		if err := store.InsertDAGRun(&state.DAGRun{
			ID:            dagRunID,
			TriggerSource: "http",
			StartedAt:     now,
			Status:        "running",
		}); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		deps := cfg.DAGGraph()
		for name := range cfg.Pipelines {
			taskID := uuid.New().String()
			_ = store.InsertTask(&state.Task{
				ID:       taskID,
				DAGRunID: dagRunID,
				Pipeline: name,
				Status:   "pending",
				Attempt:  1,
			})
		}

		logDir, err := logs.CreateLogDir(projectDir, dagRunID)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		maxWorkers := cfg.MaxConcurrent
		if maxWorkers <= 0 {
			maxWorkers = 4
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			dagRunner := &runner.DAGRunner{
				Store:      store,
				Config:     cfg,
				ProjectDir: projectDir,
				DagRunID:   dagRunID,
				LogDir:     logDir,
				MaxWorkers: maxWorkers,
				Deps:       deps,
			}
			status, runErr := dagRunner.Run(runCtx)
			if runErr != nil {
				logger.Error("triggered DAG run failed", "dag_run_id", dagRunID[:8], "error", runErr)
			} else {
				logger.Info("triggered DAG run finished", "dag_run_id", dagRunID[:8], "status", status)
			}
		}()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"dag_run_id": dagRunID,
			"status":     "accepted",
		})
	}
}

// retryHTTPHandler handles POST /retry/<dag-run-id> to retry a failed run via the daemon.
func retryHTTPHandler(cfg *config.Config, store *state.Store, projectDir string, logger *slog.Logger, runCtx context.Context, wg *sync.WaitGroup) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		runPrefix := r.URL.Path[len("/retry/"):]
		if runPrefix == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "dag-run-id required"})
			return
		}

		originalRun, err := store.FindDAGRunByPrefix(runPrefix)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("run %q not found: %s", runPrefix, err)})
			return
		}
		if originalRun.Status == "running" {
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "run is still in progress"})
			return
		}

		hasRunning, _ := store.HasRunningRun()
		if hasRunning {
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "a DAG run is already in progress"})
			return
		}

		originalTasks, err := store.TasksByRun(originalRun.ID)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		deps := cfg.DAGGraph()
		retryRunID := uuid.New().String()
		now := time.Now().UTC().Format(time.RFC3339)

		if err := store.InsertDAGRun(&state.DAGRun{
			ID:            retryRunID,
			TriggerSource: "retry",
			StartedAt:     now,
			Status:        "running",
			RetryOf:       originalRun.ID,
		}); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		pendingSet := make(map[string]bool)
		for _, t := range originalTasks {
			if t.Status == "failed" || t.Status == "skipped" {
				pendingSet[t.Pipeline] = true
			}
		}
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

		for name := range cfg.Pipelines {
			taskID := uuid.New().String()
			status := "skipped_on_retry"
			if pendingSet[name] {
				status = "pending"
			}
			_ = store.InsertTask(&state.Task{
				ID:       taskID,
				DAGRunID: retryRunID,
				Pipeline: name,
				Status:   status,
				Attempt:  1,
			})
		}

		logDir, err := logs.CreateLogDir(projectDir, retryRunID)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		maxWorkers := cfg.MaxConcurrent
		if maxWorkers <= 0 {
			maxWorkers = 4
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			dagRunner := &runner.DAGRunner{
				Store:      store,
				Config:     cfg,
				ProjectDir: projectDir,
				DagRunID:   retryRunID,
				LogDir:     logDir,
				MaxWorkers: maxWorkers,
				Deps:       deps,
			}
			status, runErr := dagRunner.Run(runCtx)
			if runErr != nil {
				logger.Error("retry run failed", "dag_run_id", retryRunID[:8], "retry_of", originalRun.ID[:8], "error", runErr)
			} else {
				logger.Info("retry run finished", "dag_run_id", retryRunID[:8], "retry_of", originalRun.ID[:8], "status", status)
			}
		}()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"dag_run_id": retryRunID,
			"retry_of":   originalRun.ID,
			"status":     "accepted",
		})
	}
}
