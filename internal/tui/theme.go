package tui

import (
	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/lipgloss"
)

var (
	colorBrand = lipgloss.Color("#F48FB1")
	colorPass  = lipgloss.Color("#81C784")
	colorFail  = lipgloss.Color("#E57373")
	colorWarn  = lipgloss.Color("#FFB74D")
	colorDim   = lipgloss.Color("#9E9E9E")

	stylePass  = lipgloss.NewStyle().Foreground(colorPass)
	styleFail  = lipgloss.NewStyle().Foreground(colorFail)
	styleWarn  = lipgloss.NewStyle().Foreground(colorWarn)
	styleDim   = lipgloss.NewStyle().Foreground(colorDim)
	styleBrand = lipgloss.NewStyle().Foreground(colorBrand).Bold(true)
	styleBold  = lipgloss.NewStyle().Bold(true)

	styleFlashOK  = lipgloss.NewStyle().Foreground(colorPass)
	styleFlashErr = lipgloss.NewStyle().Foreground(colorFail)

	styleHeaderBar = lipgloss.NewStyle().
			Bold(true).
			Padding(0, 1)

	styleKeyBar = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#CCCCCC")).
			Background(lipgloss.Color("#333333")).
			Padding(0, 1)

	styleKeyHint = lipgloss.NewStyle().
			Foreground(colorBrand).
			Bold(true)

	styleKeyLabel = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#CCCCCC"))
)

const (
	iconPass    = "✓"
	iconFail    = "✗"
	iconWarn    = "⚠"
	iconSkip    = "⊘"
	iconRunning = "●"
	iconPending = "○"
)

func statusIcon(status string) string {
	switch status {
	case "completed":
		return iconPass
	case "failed":
		return iconFail
	case "running":
		return iconRunning
	case "skipped", "skipped_on_retry":
		return iconSkip
	case "partial_failure":
		return iconWarn
	default:
		return iconPending
	}
}

func statusStyle(status string) lipgloss.Style {
	switch status {
	case "completed":
		return stylePass
	case "failed":
		return styleFail
	case "running":
		return styleWarn
	case "partial_failure":
		return styleWarn
	case "skipped", "skipped_on_retry":
		return styleDim
	default:
		return styleDim
	}
}

func tableStyles() table.Styles {
	s := table.DefaultStyles()
	s.Header = s.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("#555555")).
		BorderBottom(true).
		Bold(true).
		Foreground(colorBrand)
	s.Selected = s.Selected.
		Foreground(lipgloss.Color("#FFFFFF")).
		Bold(true)
	s.Cell = s.Cell.
		Padding(0, 1)
	return s
}
