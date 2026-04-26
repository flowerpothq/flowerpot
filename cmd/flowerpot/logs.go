package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

func logsCmd() *cobra.Command {
	var (
		configPath string
		follow     bool
		attempt    int
		stderr     bool
	)

	cmd := &cobra.Command{
		Use:   "logs [run-id] [pipeline]",
		Short: "View pipeline logs",
		Long:  "List log files for a DAG run, or display stdout+stderr\nfor a specific pipeline within a run.",
		Example: `flowerpot logs                          list recent run IDs
flowerpot logs abc12345                 list log files for run
flowerpot logs abc12345 extract         show extract pipeline logs
flowerpot logs abc12345 extract -f      follow (tail) extract logs
flowerpot logs abc12345 extract --attempt 2  show retry attempt 2`,
		Args:          cobra.MaximumNArgs(2),
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			switch len(args) {
			case 0:
				return listRuns(configPath)
			case 1:
				return listLogFiles(configPath, args[0])
			default:
				return catPipelineLogs(configPath, args[0], args[1], follow, attempt, stderr)
			}
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "flowerpot.yaml", "Path to flowerpot.yaml")
	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "Follow log output (tail -f)")
	cmd.Flags().IntVar(&attempt, "attempt", 0, "Show logs for a specific retry attempt (0 = all)")
	cmd.Flags().BoolVar(&stderr, "stderr", false, "Show only stderr")
	return cmd
}

func logsBaseDir(configPath string) (string, error) {
	_, projectDir, err := loadAndValidate(configPath)
	if err != nil {
		return "", err
	}
	return filepath.Join(projectDir, ".flowerpot", "logs"), nil
}

func listRuns(configPath string) error {
	fmt.Print(cmdHeader("logs"))

	base, err := logsBaseDir(configPath)
	if err != nil {
		return err
	}

	entries, err := os.ReadDir(base)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println(styleDim.Render("  no runs found"))
			fmt.Println()
			return nil
		}
		fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s %s", iconFail, err)))
		return errRunFailed
	}

	var dirs []string
	for _, e := range entries {
		if e.IsDir() {
			dirs = append(dirs, e.Name())
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(dirs)))

	if len(dirs) == 0 {
		fmt.Println(styleDim.Render("  no runs found"))
		fmt.Println()
		return nil
	}

	limit := 10
	if len(dirs) < limit {
		limit = len(dirs)
	}

	for _, d := range dirs[:limit] {
		short := d
		if len(short) > 8 {
			short = short[:8]
		}
		fmt.Println(styleDim.Render(fmt.Sprintf("  %s %s", iconSection, short)))
	}
	fmt.Println()
	fmt.Println(styleDim.Render(fmt.Sprintf("  Run %s to see log files for a run.",
		styleBrand.Render("flowerpot logs <run-id>"))))
	fmt.Println()
	return nil
}

func listLogFiles(configPath, runIDPrefix string) error {
	fmt.Print(cmdHeader("logs"))

	base, err := logsBaseDir(configPath)
	if err != nil {
		return err
	}

	runDir, err := resolveRunDir(base, runIDPrefix)
	if err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s %s", iconFail, err)))
		fmt.Println()
		return errRunFailed
	}

	entries, err := os.ReadDir(runDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s %s", iconFail, err)))
		return errRunFailed
	}

	if len(entries) == 0 {
		fmt.Println(styleDim.Render("  no log files"))
		fmt.Println()
		return nil
	}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, _ := e.Info()
		size := ""
		if info != nil {
			size = formatBytes(info.Size())
		}
		fmt.Println(styleDim.Render(fmt.Sprintf("  %s %-40s %s", iconSection, e.Name(), size)))
	}
	fmt.Println()
	return nil
}

func catPipelineLogs(configPath, runIDPrefix, pipeline string, follow bool, attempt int, stderrOnly bool) error {
	base, err := logsBaseDir(configPath)
	if err != nil {
		return err
	}

	runDir, err := resolveRunDir(base, runIDPrefix)
	if err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s %s", iconFail, err)))
		fmt.Println()
		return errRunFailed
	}

	safe := strings.ReplaceAll(pipeline, "/", "_")

	if attempt > 0 {
		return catAttemptLogs(runDir, safe, attempt, follow, stderrOnly)
	}

	stdoutPath := filepath.Join(runDir, safe+".stdout")
	stderrPath := filepath.Join(runDir, safe+".stderr")

	printed := false

	if !stderrOnly {
		if n, err := dumpFile(stdoutPath, os.Stdout); err == nil && n > 0 {
			printed = true
		}
	}
	if n, err := dumpFile(stderrPath, os.Stderr); err == nil && n > 0 {
		printed = true
	}

	for a := 2; a <= 10; a++ {
		sp := filepath.Join(runDir, fmt.Sprintf("%s.stdout.attempt-%d", safe, a))
		ep := filepath.Join(runDir, fmt.Sprintf("%s.stderr.attempt-%d", safe, a))
		sData, sErr := os.ReadFile(sp)
		eData, eErr := os.ReadFile(ep)
		if sErr != nil && eErr != nil {
			break
		}
		if !stderrOnly && len(sData) > 0 {
			fmt.Printf("--- attempt %d stdout ---\n", a)
			fmt.Print(string(sData))
			printed = true
		}
		if len(eData) > 0 {
			fmt.Fprintf(os.Stderr, "--- attempt %d stderr ---\n", a)
			fmt.Fprint(os.Stderr, string(eData))
			printed = true
		}
	}

	if !printed && !follow {
		fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s no logs found for %q in run %s", iconFail, pipeline, runIDPrefix)))
		fmt.Println()
		return errRunFailed
	}

	if follow {
		return followLog(stdoutPath, stderrPath, stderrOnly)
	}

	return nil
}

func catAttemptLogs(runDir, safe string, attempt int, follow, stderrOnly bool) error {
	var stdoutPath, stderrPath string
	if attempt == 1 {
		stdoutPath = filepath.Join(runDir, safe+".stdout")
		stderrPath = filepath.Join(runDir, safe+".stderr")
	} else {
		stdoutPath = filepath.Join(runDir, fmt.Sprintf("%s.stdout.attempt-%d", safe, attempt))
		stderrPath = filepath.Join(runDir, fmt.Sprintf("%s.stderr.attempt-%d", safe, attempt))
	}

	printed := false
	if !stderrOnly {
		if n, err := dumpFile(stdoutPath, os.Stdout); err == nil && n > 0 {
			printed = true
		}
	}
	if n, err := dumpFile(stderrPath, os.Stderr); err == nil && n > 0 {
		printed = true
	}

	if !printed && !follow {
		fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s no logs for attempt %d", iconFail, attempt)))
		fmt.Println()
		return errRunFailed
	}

	if follow {
		return followLog(stdoutPath, stderrPath, stderrOnly)
	}
	return nil
}

func dumpFile(path string, w *os.File) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	if len(data) == 0 {
		return 0, nil
	}
	_, err = w.Write(data)
	return len(data), err
}

// followLog tails stdout and stderr files, polling every 500ms until interrupted.
func followLog(stdoutPath, stderrPath string, stderrOnly bool) error {
	var stdoutOffset, stderrOffset int64

	if !stderrOnly {
		if info, err := os.Stat(stdoutPath); err == nil {
			stdoutOffset = info.Size()
		}
	}
	if info, err := os.Stat(stderrPath); err == nil {
		stderrOffset = info.Size()
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		if !stderrOnly {
			stdoutOffset = tailFrom(stdoutPath, stdoutOffset, os.Stdout)
		}
		stderrOffset = tailFrom(stderrPath, stderrOffset, os.Stderr)
	}
	return nil
}

func tailFrom(path string, offset int64, w io.Writer) int64 {
	f, err := os.Open(path)
	if err != nil {
		return offset
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil || info.Size() <= offset {
		return offset
	}

	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return offset
	}

	n, _ := io.Copy(w, f)
	return offset + n
}

func resolveRunDir(base, prefix string) (string, error) {
	exact := filepath.Join(base, prefix)
	if info, err := os.Stat(exact); err == nil && info.IsDir() {
		return exact, nil
	}

	entries, err := os.ReadDir(base)
	if err != nil {
		return "", fmt.Errorf("reading logs directory: %w", err)
	}

	var matches []string
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), prefix) {
			matches = append(matches, e.Name())
		}
	}

	switch len(matches) {
	case 0:
		return "", fmt.Errorf("no run matching %q", prefix)
	case 1:
		return filepath.Join(base, matches[0]), nil
	default:
		return "", fmt.Errorf("ambiguous prefix %q matches %d runs", prefix, len(matches))
	}
}

func formatBytes(b int64) string {
	switch {
	case b >= 1024*1024:
		return fmt.Sprintf("%.1f MB", float64(b)/1024/1024)
	case b >= 1024:
		return fmt.Sprintf("%.1f KB", float64(b)/1024)
	default:
		return fmt.Sprintf("%d B", b)
	}
}
