package tui

import (
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/flowerpothq/flowerpot/internal/state"
)

type runHistoryModel struct {
	pipeline string
	items    []state.DAGRun
	cursor   int
	back     bool
	viewLogs bool
}

func newRunHistoryModel() runHistoryModel {
	return runHistoryModel{}
}

func (m *runHistoryModel) selectPipeline(name string) {
	m.pipeline = name
	m.cursor = 0
	m.items = nil
}

func (m *runHistoryModel) refresh(store *state.Store) {
	if m.pipeline == "" {
		return
	}
	if runs, err := store.RunsForPipeline(m.pipeline, 50); err == nil {
		m.items = runs
	}
}

func (m *runHistoryModel) selectedRunID() string {
	if m.cursor < len(m.items) {
		return m.items[m.cursor].ID
	}
	return ""
}

func (m *runHistoryModel) update(msg tea.Msg) tea.Cmd {
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
				m.viewLogs = true
			}
		case "esc", "h":
			m.back = true
		}
	}
	return nil
}

func (m *runHistoryModel) view(width, height int) string {
	var b strings.Builder
	title := styleHeader.Render(fmt.Sprintf("  Runs: %s", m.pipeline))
	b.WriteString(title)
	b.WriteString("\n")

	if len(m.items) == 0 {
		b.WriteString(styleDim.Render("  No runs found."))
		b.WriteString("\n")
		return b.String()
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
		r := m.items[i]
		icon := statusIcon(r.Status)
		st := statusStyle(r.Status)
		shortID := r.ID
		if len(shortID) > 8 {
			shortID = shortID[:8]
		}

		dur := ""
		if r.EndedAt != "" && r.StartedAt != "" {
			if s, err := time.Parse(time.RFC3339, r.StartedAt); err == nil {
				if e, err := time.Parse(time.RFC3339, r.EndedAt); err == nil {
					dur = e.Sub(s).Truncate(time.Millisecond).String()
				}
			}
		}
		if dur == "" {
			dur = "-"
		}

		started := ""
		if t, err := time.Parse(time.RFC3339, r.StartedAt); err == nil {
			started = timeAgo(t)
		}

		trigger := r.TriggerSource
		if trigger == "" {
			trigger = "manual"
		}
		if r.RetryOf != "" {
			trigger = "retry"
		}

		detail := fmt.Sprintf("%s  %-10s %-7s %-10s %s", shortID, r.Status, trigger, dur, started)

		if i == m.cursor {
			b.WriteString(styleCursor.Render("▸ "))
			b.WriteString(styleSelected.Render(fmt.Sprintf("%s %s", icon, detail)))
		} else {
			b.WriteString(st.Render(fmt.Sprintf("  %s %s", icon, detail)))
		}
		b.WriteString("\n")
	}

	b.WriteString("\n")
	b.WriteString(styleDim.Render("  enter/l: logs  esc/h: back  ?: help"))

	return b.String()
}
