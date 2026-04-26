package main

import (
	"fmt"
	"os"
	"path/filepath"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/flowerpothq/flowerpot/internal/state"
	"github.com/flowerpothq/flowerpot/internal/tui"
	"github.com/spf13/cobra"
)

func uiCmd() *cobra.Command {
	var configPath string
	cmd := &cobra.Command{
		Use:   "ui",
		Short: "Open the terminal UI dashboard",
		Long:  "Launch an interactive terminal UI to browse pipelines, runs, and logs.",
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

	dbPath := filepath.Join(projectDir, ".flowerpot", "state.db")
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf(
			"  %s No state.db found at %s\n  Run 'flowerpot run' first to create pipeline data.",
			iconFail, dbPath)))
		return errRunFailed
	}

	store, err := state.Open(dbPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render("  "+iconFail+" opening state: "+err.Error()))
		return errRunFailed
	}
	defer func() { _ = store.Close() }()

	m := tui.NewModel(store, projectDir)
	p := tea.NewProgram(m, tea.WithAltScreen())
	_, err = p.Run()
	return err
}
