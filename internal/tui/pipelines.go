package tui

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/flowerpothq/flowerpot/internal/config"
	"github.com/flowerpothq/flowerpot/internal/state"
)

type pipelineListModel struct {
	table      table.Model
	cfg        *config.Config
	names      []string // ordered pipeline names (stable)
	allRows    []table.Row
	filter     string
	drillDown  bool
	describe   bool
	triggerOne bool
	triggerAll bool
}

func newPipelineListModel(cfg *config.Config) pipelineListModel {
	cols := []table.Column{
		{Title: "NAME", Width: 18},
		{Title: "STATUS", Width: 18},
		{Title: "LAST RUN", Width: 11},
		{Title: "DURATION", Width: 10},
		{Title: "RUNS", Width: 5},
		{Title: "AFTER", Width: 20},
	}
	t := table.New(table.WithColumns(cols), table.WithHeight(10), table.WithFocused(true))
	t.SetStyles(tableStyles())

	names := make([]string, 0, len(cfg.Pipelines))
	for n := range cfg.Pipelines {
		names = append(names, n)
	}
	sort.Strings(names)

	return pipelineListModel{table: t, cfg: cfg, names: names}
}

func (m *pipelineListModel) refresh(store *state.Store) {
	summaryMap := make(map[string]state.PipelineSummary)
	if ps, err := store.PipelinesSummary(); err == nil {
		for _, p := range ps {
			summaryMap[p.Pipeline] = p
		}
	}

	rows := make([]table.Row, 0, len(m.names))
	for _, name := range m.names {
		p := m.cfg.Pipelines[name]
		summary, hasData := summaryMap[name]

		var statusText, ago, dur, runs string
		if hasData {
			icon := statusIcon(summary.LastStatus)
			st := statusStyle(summary.LastStatus)
			statusText = st.Render(icon + " " + summary.LastStatus)
			if summary.LastRunAt != "" {
				if t, err := time.Parse(time.RFC3339, summary.LastRunAt); err == nil {
					ago = timeAgo(t)
				}
			}
			dur = summary.LastDuration
			if dur == "" {
				dur = "-"
			}
			runs = fmt.Sprintf("%d", summary.RunCount)
		} else {
			statusText = styleDim.Render(iconPending + " never run")
			ago = "-"
			dur = "-"
			runs = "0"
		}

		after := "-"
		if p != nil && len(p.After) > 0 {
			after = strings.Join(p.After, ", ")
		}

		rows = append(rows, table.Row{name, statusText, ago, dur, runs, after})
	}
	m.allRows = rows
	m.applyFilter()
}

func (m *pipelineListModel) applyFilter() {
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

func (m *pipelineListModel) selectedPipeline() string {
	row := m.table.SelectedRow()
	if row == nil {
		return ""
	}
	return row[0]
}

func (m *pipelineListModel) hasRows() bool {
	return len(m.table.Rows()) > 0
}

func (m *pipelineListModel) update(msg tea.Msg) tea.Cmd {
	if kmsg, ok := msg.(tea.KeyMsg); ok {
		switch kmsg.String() {
		case "enter", "l":
			if m.hasRows() {
				m.drillDown = true
			}
			return nil
		case "d":
			if m.hasRows() {
				m.describe = true
			}
			return nil
		case "t":
			if m.hasRows() {
				m.triggerOne = true
			}
			return nil
		case "T":
			m.triggerAll = true
			return nil
		}
	}
	var cmd tea.Cmd
	m.table, cmd = m.table.Update(msg)
	return cmd
}

func (m *pipelineListModel) setHeight(h int) {
	m.table.SetHeight(h)
}

func (m *pipelineListModel) hints() []keyHint {
	return []keyHint{
		{"enter", "Runs"},
		{"t", "Trigger"},
		{"T", "Trigger All"},
		{"d", "Describe"},
		{"/", "Filter"},
		{"?", "Help"},
		{"q", "Quit"},
	}
}

func (m *pipelineListModel) view() string {
	return m.table.View()
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
