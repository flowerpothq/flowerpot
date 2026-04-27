package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/flowerpothq/flowerpot/internal/config"
	"github.com/flowerpothq/flowerpot/internal/daemon"
	"github.com/flowerpothq/flowerpot/internal/state"
	"github.com/flowerpothq/flowerpot/internal/tui"
	"github.com/spf13/cobra"
)

func uiCmd() *cobra.Command {
	var configPath string
	cmd := &cobra.Command{
		Use:   "ui",
		Short: "Open the terminal UI dashboard",
		Long: `Launch an interactive terminal UI to browse pipelines, runs, and logs.

If no daemon is running, an embedded scheduler and HTTP server are started
automatically. They stop when the TUI exits.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runUI(configPath)
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", "flowerpot.yaml", "Path to config file")
	return cmd
}

func runUI(configPath string) error {
	abs, err := filepath.Abs(configPath)
	if err != nil {
		return err
	}
	projectDir := filepath.Dir(abs)

	result, err := config.Load(abs)
	if err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s %s", iconFail, err)))
		return errRunFailed
	}

	store, err := state.Open(filepath.Join(projectDir, ".flowerpot", "state.db"))
	if err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render("  "+iconFail+" opening state: "+err.Error()))
		return errRunFailed
	}
	defer func() { _ = store.Close() }()

	var d *daemon.Daemon
	if !daemon.IsDaemonAlive(projectDir) {
		d, err = daemon.New(result.Config, store, projectDir)
		if err != nil {
			fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s %s", iconFail, err)))
			return errRunFailed
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := d.Start(ctx); err != nil {
			fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s %s", iconFail, err)))
			return errRunFailed
		}
		defer func() { _ = d.Stop() }()
	}

	m := tui.NewModel(store, result.Config, d, projectDir)
	p := tea.NewProgram(m, tea.WithAltScreen())
	_, err = p.Run()
	return err
}
