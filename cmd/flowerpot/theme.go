package main

import (
	"fmt"

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
)

const (
	iconPass    = "✓"
	iconFail    = "✗"
	iconWarn    = "⚠"
	iconSkip    = "⊘"
	iconRunning = "●"
	iconPending = "○"
	iconSection = "▸"
)

func statusIcon(status string) string {
	switch status {
	case "completed":
		return iconPass
	case "failed":
		return iconFail
	case "running":
		return iconRunning
	case "skipped":
		return iconSkip
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
	case "skipped":
		return styleDim
	default:
		return styleDim
	}
}

func cmdHeader(command string) string {
	return fmt.Sprintf("\n  %s %s\n", styleBrand.Render("flowerpot"), styleDim.Render(command))
}

func padRight(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return s + fmt.Sprintf("%*s", width-len(s), "")
}
