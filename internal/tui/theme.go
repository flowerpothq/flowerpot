package tui

import "github.com/charmbracelet/lipgloss"

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

	styleStatusBar = lipgloss.NewStyle().
			Background(lipgloss.Color("#333333")).
			Foreground(lipgloss.Color("#CCCCCC")).
			Padding(0, 1)

	styleHeader = lipgloss.NewStyle().
			Foreground(colorBrand).
			Bold(true).
			Padding(0, 0, 1, 0)

	styleCursor = lipgloss.NewStyle().
			Foreground(colorBrand).
			Bold(true)

	styleSelected = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FFFFFF")).
			Bold(true)
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
