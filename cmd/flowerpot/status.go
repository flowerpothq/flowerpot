package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/flowerpothq/flowerpot/internal/state"
	"github.com/spf13/cobra"
)

type statusRunJSON struct {
	ID        string           `json:"id"`
	Trigger   string           `json:"trigger"`
	Status    string           `json:"status"`
	StartedAt string           `json:"started_at"`
	EndedAt   string           `json:"ended_at,omitempty"`
	Duration  string           `json:"duration,omitempty"`
	Tasks     []statusTaskJSON `json:"tasks"`
}

type statusTaskJSON struct {
	Pipeline string `json:"pipeline"`
	Status   string `json:"status"`
	ExitCode *int   `json:"exit_code"`
}

func statusCmd() *cobra.Command {
	var (
		jsonOutput bool
		configPath string
		limit      int
	)

	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show recent DAG runs and their status",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runStatus(configPath, jsonOutput, limit)
		},
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output as JSON")
	cmd.Flags().StringVarP(&configPath, "config", "c", "flowerpot.yaml", "Path to flowerpot.yaml")
	cmd.Flags().IntVarP(&limit, "limit", "n", 5, "Number of recent runs to show")

	return cmd
}

func runStatus(configPath string, jsonOutput bool, limit int) error {
	if !jsonOutput {
		fmt.Print(cmdHeader("status"))
	}

	projectDir := filepath.Dir(configPath)
	if !filepath.IsAbs(projectDir) {
		abs, err := filepath.Abs(projectDir)
		if err == nil {
			projectDir = abs
		}
	}

	dbPath := filepath.Join(projectDir, ".flowerpot", "state.db")
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		if jsonOutput {
			fmt.Println("[]")
		} else {
			fmt.Println(styleDim.Render("  No runs yet. Run " + styleBrand.Render("flowerpot run") + styleDim.Render(" to start.")))
			fmt.Println()
		}
		return nil
	}

	store, err := state.Open(dbPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render("  "+iconFail+" opening state: "+err.Error()))
		return errRunFailed
	}
	defer func() { _ = store.Close() }()

	runs, err := store.RecentRuns(limit)
	if err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render("  "+iconFail+" querying runs: "+err.Error()))
		return errRunFailed
	}

	if len(runs) == 0 {
		if jsonOutput {
			fmt.Println("[]")
		} else {
			fmt.Println(styleDim.Render("  No runs yet."))
			fmt.Println()
		}
		return nil
	}

	if jsonOutput {
		return printStatusJSON(store, runs)
	}
	return printStatusText(store, runs)
}

func printStatusJSON(store *state.Store, runs []state.DAGRun) error {
	var result []statusRunJSON
	for _, run := range runs {
		tasks, _ := store.TasksByRun(run.ID)

		sr := statusRunJSON{
			ID:        run.ID,
			Trigger:   run.TriggerSource,
			Status:    run.Status,
			StartedAt: run.StartedAt,
			EndedAt:   run.EndedAt,
		}
		if run.StartedAt != "" && run.EndedAt != "" {
			if s, e := parseTime(run.StartedAt), parseTime(run.EndedAt); !s.IsZero() && !e.IsZero() {
				sr.Duration = e.Sub(s).Truncate(time.Millisecond).String()
			}
		}
		for _, t := range tasks {
			sr.Tasks = append(sr.Tasks, statusTaskJSON{
				Pipeline: t.Pipeline,
				Status:   t.Status,
				ExitCode: t.ExitCode,
			})
		}
		result = append(result, sr)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(result)
}

func printStatusText(store *state.Store, runs []state.DAGRun) error {
	for i, run := range runs {
		if i > 0 {
			fmt.Println()
		}

		dur := ""
		if run.StartedAt != "" && run.EndedAt != "" {
			if s, e := parseTime(run.StartedAt), parseTime(run.EndedAt); !s.IsZero() && !e.IsZero() {
				dur = e.Sub(s).Truncate(time.Millisecond).String()
			}
		}

		ago := ""
		if run.StartedAt != "" {
			if s := parseTime(run.StartedAt); !s.IsZero() {
				ago = timeAgo(s)
			}
		}

		st := statusStyle(run.Status)
		icon := statusIcon(run.Status)
		label := run.Status
		if label == "partial_failure" {
			label = "partial failure"
		}

		header := fmt.Sprintf("  %s %s  %s", icon, run.ID[:8], padRight(label, 17))
		header += padRight(run.TriggerSource, 6)
		if dur != "" {
			header += "  " + padRight(dur, 8)
		}
		if ago != "" {
			header += "  " + ago
		}
		fmt.Println(st.Render(header))

		tasks, _ := store.TasksByRun(run.ID)
		sort.Slice(tasks, func(a, b int) bool {
			return tasks[a].StartedAt < tasks[b].StartedAt
		})

		var taskIcons []string
		for _, t := range tasks {
			ti := statusIcon(t.Status)
			ts := statusStyle(t.Status)
			taskIcons = append(taskIcons, ts.Render(ti+" "+t.Pipeline))
		}
		if len(taskIcons) > 0 {
			line := "    "
			for j, ti := range taskIcons {
				if j > 0 {
					line += "  "
				}
				line += ti
			}
			fmt.Println(line)
		}
	}
	fmt.Println()
	return nil
}

func timeAgo(t time.Time) string {
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		m := int(d.Minutes())
		if m == 1 {
			return "1 min ago"
		}
		return fmt.Sprintf("%d min ago", m)
	case d < 24*time.Hour:
		h := int(d.Hours())
		if h == 1 {
			return "1 hour ago"
		}
		return fmt.Sprintf("%d hours ago", h)
	default:
		days := int(d.Hours() / 24)
		if days == 1 {
			return "1 day ago"
		}
		return fmt.Sprintf("%d days ago", days)
	}
}
