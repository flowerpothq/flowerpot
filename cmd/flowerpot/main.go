package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	root := &cobra.Command{
		Use:   "flowerpot",
		Short: "A lightweight data pipeline scheduler",
	}

	root.AddCommand(validateCmd())
	root.AddCommand(versionCmd())
	root.AddCommand(stubCmd("run", "Execute pipelines"))
	root.AddCommand(stubCmd("serve", "Start the scheduler daemon"))
	root.AddCommand(stubCmd("trigger", "Trigger a DAG run via HTTP"))
	root.AddCommand(stubCmd("status", "Show pipeline and run status"))
	root.AddCommand(stubCmd("retry", "Retry a failed DAG run"))
	root.AddCommand(stubCmd("logs", "View pipeline logs"))
	root.AddCommand(stubCmd("ui", "Launch the terminal UI"))
	root.AddCommand(stubCmd("init", "Initialize a new flowerpot project"))

	if err := root.Execute(); err != nil {
		if err != errValidationFailed {
			fmt.Fprintln(os.Stderr, err)
		}
		os.Exit(1)
	}
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("flowerpot %s (commit: %s, built: %s)\n", version, commit, date)
		},
	}
}

func stubCmd(name, short string) *cobra.Command {
	return &cobra.Command{
		Use:   name,
		Short: short,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("flowerpot %s: coming soon\n", name)
		},
	}
}
