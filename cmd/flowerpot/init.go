package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

func initCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init [directory]",
		Short: "Initialize a new flowerpot project",
		Long:  "Scaffolds a flowerpot.yaml with example pipelines,\na sample script, and a .gitignore for the .flowerpot directory.",
		Example: `flowerpot init
flowerpot init my-project`,
		Args:          cobra.MaximumNArgs(1),
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := "."
			if len(args) > 0 {
				dir = args[0]
			}
			return runInit(dir)
		},
	}
	return cmd
}

var errInitFailed = fmt.Errorf("init failed")

func runInit(dir string) error {
	fmt.Print(cmdHeader("init"))

	if dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s %s", iconFail, err)))
			return errInitFailed
		}
	}

	yamlPath := filepath.Join(dir, "flowerpot.yaml")
	if _, err := os.Stat(yamlPath); err == nil {
		fmt.Fprintln(os.Stderr, styleWarn.Render(fmt.Sprintf("  %s flowerpot.yaml already exists, skipping", iconWarn)))
		return nil
	}

	if err := os.WriteFile(yamlPath, []byte(sampleYAML), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s writing flowerpot.yaml: %s", iconFail, err)))
		return errInitFailed
	}
	fmt.Println(stylePass.Render(fmt.Sprintf("  %s flowerpot.yaml", iconPass)))

	scriptsDir := filepath.Join(dir, "scripts")
	if err := os.MkdirAll(scriptsDir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s creating scripts/: %s", iconFail, err)))
		return errInitFailed
	}

	scriptPath := filepath.Join(scriptsDir, "transform.sh")
	if err := os.WriteFile(scriptPath, []byte(sampleScript), 0o755); err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s writing scripts/transform.sh: %s", iconFail, err)))
		return errInitFailed
	}
	fmt.Println(stylePass.Render(fmt.Sprintf("  %s scripts/transform.sh", iconPass)))

	gitignorePath := filepath.Join(dir, ".gitignore")
	gitignoreContent := ".flowerpot/\n"
	if existing, err := os.ReadFile(gitignorePath); err == nil {
		if len(existing) > 0 {
			gitignoreContent = string(existing)
			if gitignoreContent[len(gitignoreContent)-1] != '\n' {
				gitignoreContent += "\n"
			}
			gitignoreContent += ".flowerpot/\n"
		}
	}
	if err := os.WriteFile(gitignorePath, []byte(gitignoreContent), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s writing .gitignore: %s", iconFail, err)))
		return errInitFailed
	}
	fmt.Println(stylePass.Render(fmt.Sprintf("  %s .gitignore", iconPass)))

	fmt.Println()
	fmt.Println(styleDim.Render("  Next steps:"))
	fmt.Println(styleDim.Render("    1. Edit " + styleBrand.Render("flowerpot.yaml") + styleDim.Render(" to define your pipelines")))
	fmt.Println(styleDim.Render("    2. Run " + styleBrand.Render("flowerpot validate") + styleDim.Render(" to check your config")))
	fmt.Println(styleDim.Render("    3. Run " + styleBrand.Render("flowerpot run") + styleDim.Render(" to execute the DAG")))
	fmt.Println(styleDim.Render("    4. Run " + styleBrand.Render("flowerpot serve") + styleDim.Render(" to start the scheduler")))
	fmt.Println()

	return nil
}

const sampleYAML = `# flowerpot.yaml — pipeline configuration
# Docs: https://github.com/flowerpothq/flowerpot

schedule: "*/5 * * * *"   # every 5 minutes
timezone: "UTC"

pipelines:
  extract:
    run: "echo 'extracting data — replace with your script'"
    timeout: "60s"

  transform:
    run: "bash scripts/transform.sh"
    after: [extract]
    timeout: "120s"
    retry:
      max_retries: 2
      delay: "5s"

  load:
    run: "echo 'loading data — replace with your script'"
    after: [transform]
    timeout: "60s"
`

const sampleScript = `#!/usr/bin/env bash
set -euo pipefail

echo "transform: logical_date=${FLOWERPOT_LOGICAL_DATE:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"
echo "transform: dag_run_id=${FLOWERPOT_DAG_RUN_ID:-unknown}"
echo "transform: processing data..."
sleep 1
echo "transform: done"
`
