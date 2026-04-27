package tui

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/flowerpothq/flowerpot/internal/state"
)

type taskDetailModel struct {
	table    table.Model
	runID    string
	items    []state.Task
	allRows  []table.Row
	filter   string
	back     bool
	viewLogs bool
	retry    bool
}

func newTaskDetailModel() taskDetailModel {
	cols := []table.Column{
		{Title: "PIPELINE", Width: 20},
		{Title: "STATUS", Width: 18},
		{Title: "ATTEMPT", Width: 8},
		{Title: "EXIT", Width: 6},
		{Title: "DURATION", Width: 10},
	}
	t := table.New(table.WithColumns(cols), table.WithHeight(10), table.WithFocused(true))
	t.SetStyles(tableStyles())
	return taskDetailModel{table: t}
}

func (m *taskDetailModel) selectRun(runID string) {
	m.runID = runID
	m.items = nil
}

func (m *taskDetailModel) refresh(store *state.Store) {
	if m.runID == "" {
		return
	}
	tasks, err := store.TasksByRun(m.runID)
	if err != nil {
		return
	}
	m.items = tasks

	rows := make([]table.Row, len(tasks))
	for i, t := range tasks {
		icon := statusIcon(t.Status)
		st := statusStyle(t.Status)

		exit := "-"
		if t.ExitCode != nil {
			exit = fmt.Sprintf("%d", *t.ExitCode)
		}

		dur := "-"
		if t.EndedAt != "" && t.StartedAt != "" {
			if s, err := time.Parse(time.RFC3339, t.StartedAt); err == nil {
				if e, err := time.Parse(time.RFC3339, t.EndedAt); err == nil {
					dur = e.Sub(s).Truncate(time.Millisecond).String()
				}
			}
		}

		rows[i] = table.Row{
			t.Pipeline,
			st.Render(icon + " " + t.Status),
			fmt.Sprintf("%d", t.Attempt),
			exit,
			dur,
		}
	}
	m.allRows = rows
	m.applyFilter()
}

func (m *taskDetailModel) applyFilter() {
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

func (m *taskDetailModel) selectedPipeline() string {
	idx := m.table.Cursor()
	if idx >= 0 && idx < len(m.items) {
		return m.items[idx].Pipeline
	}
	return ""
}

func (m *taskDetailModel) update(msg tea.Msg) tea.Cmd {
	if kmsg, ok := msg.(tea.KeyMsg); ok {
		switch kmsg.String() {
		case "enter", "l":
			if len(m.items) > 0 {
				m.viewLogs = true
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

func (m *taskDetailModel) setHeight(h int) {
	m.table.SetHeight(h)
}

func (m *taskDetailModel) hints() []keyHint {
	return []keyHint{
		{"enter", "Logs"},
		{"r", "Retry Run"},
		{"/", "Filter"},
		{"esc", "Back"},
		{"?", "Help"},
		{"q", "Quit"},
	}
}

func (m *taskDetailModel) view() string {
	return m.table.View()
}
