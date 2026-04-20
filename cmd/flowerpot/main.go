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
		Short: "Lighweight data pipeline scheduler",
		CompletionOptions: cobra.CompletionOptions{
			HiddenDefaultCmd: true,
		},
	}

	root.SetHelpFunc(brandedHelp)

	root.AddCommand(validateCmd())
	root.AddCommand(versionCmd())
	root.AddCommand(runCmd())
	root.AddCommand(statusCmd())
	root.AddCommand(stubCmd("serve", "Start the scheduler daemon"))
	root.AddCommand(stubCmd("trigger", "Trigger a DAG run on the running daemon"))
	root.AddCommand(stubCmd("logs", "View DAG run logs"))
	root.AddCommand(stubCmd("init", "Scaffold a new project"))

	for _, c := range root.Commands() {
		c.SetHelpFunc(subcommandHelp)
	}

	if err := root.Execute(); err != nil {
		if err != errValidationFailed && err != errRunFailed {
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
			fmt.Printf("\n  %s %s %s\n\n",
				styleBrand.Render("flowerpot"),
				styleBold.Render(version),
				styleDim.Render(fmt.Sprintf("(%s, %s)", commit, date)))
		},
	}
}

func stubCmd(name, short string) *cobra.Command {
	return &cobra.Command{
		Use:   name,
		Short: short,
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Fprintf(os.Stderr, "%q is not yet implemented\n", name)
			return nil
		},
	}
}
