package tui

import (
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/flowerpothq/flowerpot/internal/state"
)

type pipelineListModel struct {
	items     []state.PipelineSummary
	cursor    int
	drillDown bool
}

func newPipelineListModel() pipelineListModel {
	return pipelineListModel{}
}

func (m *pipelineListModel) refresh(store *state.Store) {
	if ps, err := store.PipelinesSummary(); err == nil {
		m.items = ps
	}
}

func (m *pipelineListModel) selectedPipeline() string {
	if m.cursor < len(m.items) {
		return m.items[m.cursor].Pipeline
	}
	return ""
}

func (m *pipelineListModel) update(msg tea.Msg) tea.Cmd {
	if msg, ok := msg.(tea.KeyMsg); ok {
		switch msg.String() {
		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
			}
		case "down", "j":
			if m.cursor < len(m.items)-1 {
				m.cursor++
			}
		case "enter", "l":
			if len(m.items) > 0 {
				m.drillDown = true
			}
		}
	}
	return nil
}

func (m *pipelineListModel) view(width, height int) string {
	var b strings.Builder
	title := styleHeader.Render("  Pipelines")
	b.WriteString(title)
	b.WriteString("\n")

	if len(m.items) == 0 {
		b.WriteString(styleDim.Render("  No pipeline data. Run flowerpot run first."))
		b.WriteString("\n")
		return b.String()
	}

	maxName := 0
	for _, p := range m.items {
		if len(p.Pipeline) > maxName {
			maxName = len(p.Pipeline)
		}
	}

	visible := height - 3
	if visible < 1 {
		visible = 1
	}

	start := 0
	if m.cursor >= visible {
		start = m.cursor - visible + 1
	}
	end := start + visible
	if end > len(m.items) {
		end = len(m.items)
	}

	for i := start; i < end; i++ {
		p := m.items[i]
		icon := statusIcon(p.LastStatus)
		st := statusStyle(p.LastStatus)
		name := padRight(p.Pipeline, maxName)

		detail := p.LastStatus
		if p.LastDuration != "" {
			detail += "  " + p.LastDuration
		}
		if p.LastRunAt != "" {
			if t, err := time.Parse(time.RFC3339, p.LastRunAt); err == nil {
				detail += "  " + timeAgo(t)
			}
		}
		detail += fmt.Sprintf("  (%d runs)", p.RunCount)

		line := fmt.Sprintf("  %s %s  %s", icon, name, detail)
		if i == m.cursor {
			b.WriteString(styleCursor.Render("▸ "))
			b.WriteString(styleSelected.Render(fmt.Sprintf("%s %s  %s", icon, name, detail)))
		} else {
			b.WriteString(st.Render(line))
		}
		b.WriteString("\n")
	}

	return b.String()
}

func padRight(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
}

func timeAgo(t time.Time) string {
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	}
}
