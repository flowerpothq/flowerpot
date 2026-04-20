package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"

	"github.com/flowerpothq/flowerpot/internal/config"
	"github.com/flowerpothq/flowerpot/internal/logs"
	"github.com/flowerpothq/flowerpot/internal/runner"
	"github.com/flowerpothq/flowerpot/internal/state"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"time"
)

func triggerCmd() *cobra.Command {
	var configPath string

	cmd := &cobra.Command{
		Use:   "trigger",
		Short: "Trigger a DAG run via HTTP",
		Long:  "Sends an HTTP POST to the running flowerpot serve process\nto trigger an immediate DAG run.",
		Example: `flowerpot trigger
flowerpot trigger -c path/to/flowerpot.yaml`,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runTrigger(configPath)
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "flowerpot.yaml", "Path to flowerpot.yaml")
	return cmd
}

func runTrigger(configPath string) error {
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
		fmt.Println(stylePass.Render(fmt.Sprintf("  %s triggered DAG run %s", iconPass, result.DagRunID[:8])))
	}

	fmt.Println()
	return nil
}

// healthHandler returns a simple JSON health check.
func healthHandler(store *state.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hasRunning, _ := store.HasRunningRun()
		w.Header().Set("Content-Type", "application/json")
		status := "idle"
		if hasRunning {
			status = "running"
		}
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status": status,
		})
	}
}

// triggerHandler creates and runs a DAG run in a background goroutine.
func triggerHandler(cfg *config.Config, store *state.Store, projectDir string, logger *slog.Logger) http.HandlerFunc {
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

		go func() {
			dagRunner := &runner.DAGRunner{
				Store:      store,
				Config:     cfg,
				ProjectDir: projectDir,
				DagRunID:   dagRunID,
				LogDir:     logDir,
				MaxWorkers: maxWorkers,
				Deps:       deps,
			}
			status, runErr := dagRunner.Run(context.Background())
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
