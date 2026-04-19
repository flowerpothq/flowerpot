package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/flowerpothq/flowerpot/internal/config"
	"github.com/flowerpothq/flowerpot/internal/dag"
	"github.com/spf13/cobra"
)

var (
	stylePass = lipgloss.NewStyle().Foreground(lipgloss.Color("2"))
	styleFail = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
)

var errValidationFailed = errors.New("validation failed")

func validateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "validate [path]",
		Short: "Validate a flowerpot.yaml configuration",
		Args:  cobra.MaximumNArgs(1),
		RunE:  runValidate,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	return cmd
}

func runValidate(cmd *cobra.Command, args []string) error {
	path := "flowerpot.yaml"
	if len(args) > 0 {
		path = args[0]
	}

	result, err := config.Load(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render("✗ "+err.Error()))
		return errValidationFailed
	}

	hasErrors := false

	if result.HasErrors() {
		hasErrors = true
		for _, e := range result.Errors {
			fmt.Fprintln(os.Stderr, styleFail.Render("✗ "+e.Error()))
		}
	} else {
		fmt.Println(stylePass.Render("✓ Schema valid"))
	}

	g := dag.Graph(result.Config.DAGGraph())
	order, dagErr := dag.TopoSort(g)
	if dagErr != nil {
		hasErrors = true
		fmt.Fprintln(os.Stderr, styleFail.Render("✗ "+dagErr.Error()))
	} else {
		msg := fmt.Sprintf("✓ DAG valid (%d pipelines, topo order: %s)",
			len(order), strings.Join(order, " → "))
		fmt.Println(stylePass.Render(msg))
	}

	if result.Config.HasSQLPipelines() {
		baseDir := filepath.Dir(path)
		total, found := result.Config.SQLFileCount(baseDir)
		if found == total {
			fmt.Println(stylePass.Render(fmt.Sprintf("✓ SQL files found (%d/%d)", found, total)))
		} else {
			hasErrors = true
			fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("✗ SQL files missing (%d/%d found)", found, total)))
		}
	}

	if hasErrors {
		return errValidationFailed
	}
	return nil
}
