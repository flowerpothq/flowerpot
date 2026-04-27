package tui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/flowerpothq/flowerpot/internal/daemon"
)

func renderHeader(width int, breadcrumb []string, d *daemon.Daemon, projectDir string) string {
	left := styleBrand.Render("flowerpot")
	if len(breadcrumb) > 0 {
		left += styleDim.Render("  ")
		for i, part := range breadcrumb {
			if i > 0 {
				left += styleDim.Render(" › ")
			}
			if i == len(breadcrumb)-1 {
				left += styleBold.Render(part)
			} else {
				left += styleDim.Render(part)
			}
		}
	}

	var right string
	if d != nil {
		right = stylePass.Render(iconRunning + " scheduler: active")
	} else if daemon.IsDaemonAlive(projectDir) {
		right = stylePass.Render(iconRunning + " daemon: running")
	} else {
		right = styleFail.Render(iconPending + " daemon: stopped")
	}

	gap := width - stripAnsiLen(left) - stripAnsiLen(right) - 2
	if gap < 1 {
		gap = 1
	}

	line := styleHeaderBar.Width(width).Render(
		left + strings.Repeat(" ", gap) + right,
	)
	separator := styleDim.Render(strings.Repeat("─", width))
	return line + "\n" + separator
}

func stripAnsiLen(s string) int {
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

// renderKeyBar renders context-sensitive key hints at the bottom.
func renderKeyBar(width int, hints []keyHint, flash *flashMsg) string {
	var parts []string
	for _, h := range hints {
		parts = append(parts, fmt.Sprintf("%s %s",
			styleKeyHint.Render("<"+h.key+">"),
			styleKeyLabel.Render(h.label)))
	}
	line := strings.Join(parts, "  ")

	var flashLine string
	if flash != nil {
		flashLine = " " + flash.style.Render(flash.text)
	}

	separator := styleDim.Render(strings.Repeat("─", width))
	bar := styleKeyBar.Width(width).Render(line)
	if flashLine != "" {
		return flashLine + "\n" + separator + "\n" + bar
	}
	return separator + "\n" + bar
}

type keyHint struct {
	key   string
	label string
}

type flashMsg struct {
	text  string
	style lipgloss.Style
}
