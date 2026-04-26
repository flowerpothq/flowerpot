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
	{"enter/l", "Drill in (pipelines → runs → logs)"},
	{"esc/h", "Go back"},
	{"g", "Jump to top (in log view)"},
	{"G", "Jump to bottom (in log view)"},
	{"PgUp/PgDn", "Scroll logs"},
	{"?", "Toggle this help"},
	{"q / ctrl+c", "Quit"},
}

func viewHelp_(width, height int) string {
	var b strings.Builder
	b.WriteString(styleHeader.Render("  Keybindings"))
	b.WriteString("\n\n")

	for _, h := range helpBindings {
		key := styleBrand.Render(padRight(h.key, 14))
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
