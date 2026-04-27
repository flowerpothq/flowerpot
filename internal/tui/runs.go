package tui

import (
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/flowerpothq/flowerpot/internal/state"
)

type runHistoryModel struct {
	table     table.Model
	pipeline  string
	items     []state.DAGRun
	allRows   []table.Row
	filter    string
	back      bool
	viewTasks bool
	retry     bool
}

func newRunHistoryModel() runHistoryModel {
	cols := []table.Column{
		{Title: "ID", Width: 10},
		{Title: "STATUS", Width: 18},
		{Title: "TRIGGER", Width: 9},
		{Title: "STARTED", Width: 12},
		{Title: "DURATION", Width: 10},
	}
	t := table.New(table.WithColumns(cols), table.WithHeight(10), table.WithFocused(true))
	t.SetStyles(tableStyles())
	return runHistoryModel{table: t}
}

func (m *runHistoryModel) selectPipeline(name string) {
	m.pipeline = name
	m.items = nil
}

func (m *runHistoryModel) refresh(store *state.Store) {
	if m.pipeline == "" {
		return
	}
	runs, err := store.RunsForPipeline(m.pipeline, 50)
	if err != nil {
		return
	}
	m.items = runs

	rows := make([]table.Row, len(runs))
	for i, r := range runs {
		shortID := r.ID
		if len(shortID) > 8 {
			shortID = shortID[:8]
		}

		icon := statusIcon(r.Status)
		st := statusStyle(r.Status)

		trigger := r.TriggerSource
		if trigger == "" {
			trigger = "manual"
		}
		if r.RetryOf != "" {
			trigger = "retry"
		}

		dur := "-"
		if r.EndedAt != "" && r.StartedAt != "" {
			if s, err := time.Parse(time.RFC3339, r.StartedAt); err == nil {
				if e, err := time.Parse(time.RFC3339, r.EndedAt); err == nil {
					dur = e.Sub(s).Truncate(time.Millisecond).String()
				}
			}
		}

		ago := ""
		if t, err := time.Parse(time.RFC3339, r.StartedAt); err == nil {
			ago = timeAgo(t)
		}

		rows[i] = table.Row{
			shortID,
			st.Render(icon + " " + r.Status),
			trigger,
			ago,
			dur,
		}
	}
	m.allRows = rows
	m.applyFilter()
}

func (m *runHistoryModel) applyFilter() {
	if m.filter == "" {
		m.table.SetRows(m.allRows)
		return
	}
	f := strings.ToLower(m.filter)
	var filtered []table.Row
	for _, r := range m.allRows {
		for _, col := range r {
			if strings.Contains(strings.ToLower(col), f) {
				filtered = append(filtered, r)
				break
			}
		}
	}
	m.table.SetRows(filtered)
}

func (m *runHistoryModel) selectedRunID() string {
	idx := m.table.Cursor()
	if idx < len(m.items) {
		return m.items[idx].ID
	}
	return ""
}

func (m *runHistoryModel) update(msg tea.Msg) tea.Cmd {
	if kmsg, ok := msg.(tea.KeyMsg); ok {
		switch kmsg.String() {
		case "enter", "l":
			if len(m.items) > 0 {
				m.viewTasks = true
			}
			return nil
		case "esc", "h":
			m.back = true
			return nil
		case "r":
			if len(m.items) > 0 {
				m.retry = true
			}
			return nil
		}
	}
	var cmd tea.Cmd
	m.table, cmd = m.table.Update(msg)
	return cmd
}

func (m *runHistoryModel) setHeight(h int) {
	m.table.SetHeight(h)
}

func (m *runHistoryModel) hints() []keyHint {
	return []keyHint{
		{"enter", "Tasks"},
		{"r", "Retry"},
		{"/", "Filter"},
		{"esc", "Back"},
		{"?", "Help"},
		{"q", "Quit"},
	}
}

func (m *runHistoryModel) view() string {
	return m.table.View()
}
