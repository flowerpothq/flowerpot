package tui

import (
	"strings"
)

var helpBindings = []struct {
	key  string
	desc string
}{
	{"↑/k", "Move up"},
	{"↓/j", "Move down"},
	{"enter/l", "Drill in (pipelines → runs → tasks → logs)"},
	{"esc/h", "Go back"},
	{"t", "Trigger selected pipeline"},
	{"T", "Trigger full DAG"},
	{"r", "Retry failed run"},
	{"d", "Describe pipeline config"},
	{"g", "Jump to top (in log/describe view)"},
	{"G", "Jump to bottom (in log/describe view)"},
	{"PgUp/PgDn", "Scroll"},
	{"?", "Toggle this help"},
	{"q / ctrl+c", "Quit"},
}

func viewHelp_(width, height int) string {
	var b strings.Builder
	b.WriteString(styleBrand.Render("  Keybindings"))
	b.WriteString("\n\n")

	for _, h := range helpBindings {
		key := styleBrand.Render(padRight(h.key, 16))
		b.WriteString("    ")
		b.WriteString(key)
		b.WriteString("  ")
		b.WriteString(h.desc)
		b.WriteString("\n")
	}

	b.WriteString("\n")
	b.WriteString(styleDim.Render("  Press ? or esc to dismiss"))
	return b.String()
}
