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
	root.AddCommand(serveCmd())
	root.AddCommand(triggerCmd())
	root.AddCommand(statusCmd())
	root.AddCommand(logsCmd())
	root.AddCommand(initCmd())

	for _, c := range root.Commands() {
		c.SetHelpFunc(subcommandHelp)
	}

	if err := root.Execute(); err != nil {
		if err != errValidationFailed && err != errRunFailed && err != errInitFailed && err != errServeFailed {
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

