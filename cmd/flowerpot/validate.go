package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/flowerpothq/flowerpot/internal/config"
	"github.com/flowerpothq/flowerpot/internal/dag"
	"github.com/spf13/cobra"
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

	fmt.Print(cmdHeader("validate"))

	result, err := config.Load(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render("  "+iconFail+" "+err.Error()))
		fmt.Println()
		return errValidationFailed
	}

	hasErrors := false

	if result.HasErrors() {
		hasErrors = true
		for _, e := range result.Errors {
			fmt.Fprintln(os.Stderr, styleFail.Render("  "+iconFail+" "+e.Error()))
		}
	} else {
		schemaMsg := "  " + iconPass + " Schema valid"
		if ng := len(result.Config.Groups); ng > 0 {
			schemaMsg += fmt.Sprintf(" (%d pipeline group(s) expanded)", ng)
		}
		fmt.Println(stylePass.Render(schemaMsg))
	}

	g := dag.Graph(result.Config.DAGGraph())
	order, dagErr := dag.TopoSort(g)
	if dagErr != nil {
		hasErrors = true
		fmt.Fprintln(os.Stderr, styleFail.Render("  "+iconFail+" "+dagErr.Error()))
	} else {
		msg := fmt.Sprintf("  %s DAG valid (%d pipelines: %s)",
			iconPass, len(order), strings.Join(order, " → "))
		fmt.Println(stylePass.Render(msg))
	}

	if result.Config.HasSQLPipelines() {
		baseDir := filepath.Dir(path)
		total, found := result.Config.SQLFileCount(baseDir)
		if found == total {
			fmt.Println(stylePass.Render(fmt.Sprintf("  %s SQL files (%d/%d)", iconPass, found, total)))
		} else {
			hasErrors = true
			fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s SQL files missing (%d/%d found)", iconFail, found, total)))
		}
	}

	fmt.Println()
	if hasErrors {
		return errValidationFailed
	}
	return nil
}
