package tui

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/flowerpothq/flowerpot/internal/state"
)

type statusBarModel struct {
	projectDir   string
	daemonAlive  bool
	pipelineCount int
	runCount     int
}

func newStatusBarModel(projectDir string) statusBarModel {
	return statusBarModel{projectDir: projectDir}
}

func (m *statusBarModel) update(store *state.Store) {
	m.daemonAlive = isDaemonAlive(m.projectDir)
	if ps, err := store.PipelinesSummary(); err == nil {
		m.pipelineCount = len(ps)
	}
	if runs, err := store.RecentRuns(1000); err == nil {
		m.runCount = len(runs)
	}
}

func (m *statusBarModel) view(width int, current viewID, pipeline, runID string) string {
	var parts []string

	parts = append(parts, styleBrand.Render("flowerpot"))

	switch current {
	case viewPipelineList:
		parts = append(parts, "Pipelines")
	case viewRunHistory:
		parts = append(parts, "Pipelines", "›", pipeline)
	case viewLogTail:
		short := runID
		if len(short) > 8 {
			short = short[:8]
		}
		parts = append(parts, "Pipelines", "›", pipeline, "›", short)
	case viewHelp:
		parts = append(parts, "Help")
	}

	breadcrumb := strings.Join(parts, " ")

	daemon := styleFail.Render("daemon: stopped")
	if m.daemonAlive {
		daemon = stylePass.Render("daemon: running")
	}

	stats := styleDim.Render(fmt.Sprintf("%d pipelines  %d runs", m.pipelineCount, m.runCount))

	gap := width - lipglossWidth(breadcrumb) - lipglossWidth(daemon) - lipglossWidth(stats) - 6
	if gap < 1 {
		gap = 1
	}

	return styleStatusBar.Width(width).Render(
		breadcrumb + strings.Repeat(" ", gap) + stats + "  " + daemon,
	)
}

func isDaemonAlive(projectDir string) bool {
	pidFile := filepath.Join(projectDir, ".flowerpot", "flowerpot.pid")
	data, err := os.ReadFile(pidFile)
	if err != nil {
		return false
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) == 0 {
		return false
	}
	pid, err := strconv.Atoi(lines[0])
	if err != nil || pid <= 0 {
		return false
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return proc.Signal(syscall.Signal(0)) == nil
}

func lipglossWidth(s string) int {
	n := 0
	for _, c := range s {
		if c == '\x1b' {
			continue
		}
		n++
	}
	return len([]rune(stripAnsi(s)))
}

func stripAnsi(s string) string {
	var b strings.Builder
	inEscape := false
	for _, r := range s {
		if r == '\x1b' {
			inEscape = true
			continue
		}
		if inEscape {
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
				inEscape = false
			}
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}
