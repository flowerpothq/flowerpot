package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

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
