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
	"github.com/robfig/cron/v3"
)

type pipelineListModel struct {
	table      table.Model
	cfg        *config.Config
	names      []string // ordered pipeline names (stable)
	allRows    []table.Row
	filter     string
	hasGroups  bool
	nextRun    string // precomputed from cron schedule
	drillDown  bool
	describe   bool
	triggerOne bool
	triggerAll bool
}

func newPipelineListModel(cfg *config.Config) pipelineListModel {
	hasGroups := len(cfg.Groups) > 0
	var cols []table.Column
	if hasGroups {
		cols = []table.Column{
			{Title: "GROUP", Width: 14},
			{Title: "NAME", Width: 18},
			{Title: "STATUS", Width: 18},
			{Title: "LAST RUN", Width: 11},
			{Title: "DURATION", Width: 10},
			{Title: "NEXT RUN", Width: 14},
			{Title: "AFTER", Width: 20},
		}
	} else {
		cols = []table.Column{
			{Title: "NAME", Width: 18},
			{Title: "STATUS", Width: 18},
			{Title: "LAST RUN", Width: 11},
			{Title: "DURATION", Width: 10},
			{Title: "NEXT RUN", Width: 14},
			{Title: "AFTER", Width: 20},
		}
	}
	t := table.New(table.WithColumns(cols), table.WithHeight(10), table.WithFocused(true))
	t.SetStyles(tableStyles())

	names := make([]string, 0, len(cfg.Pipelines))
	for n := range cfg.Pipelines {
		names = append(names, n)
	}
	sort.Strings(names)

	nextRun := computeNextRun(cfg.Schedule, cfg.Timezone)

	return pipelineListModel{table: t, cfg: cfg, names: names, hasGroups: hasGroups, nextRun: nextRun}
}

func computeNextRun(schedule, tz string) string {
	if schedule == "" {
		return "-"
	}
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	sched, err := parser.Parse(schedule)
	if err != nil {
		return "-"
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		loc = time.UTC
	}
	next := sched.Next(time.Now().In(loc))
	return timeUntil(next)
}

func timeUntil(t time.Time) string {
	d := time.Until(t)
	if d < 0 {
		return "overdue"
	}
	switch {
	case d < time.Minute:
		return "< 1m"
	case d < time.Hour:
		return fmt.Sprintf("in %dm", int(d.Minutes()))
	case d < 24*time.Hour:
		h := int(d.Hours())
		m := int(d.Minutes()) % 60
		if m > 0 {
			return fmt.Sprintf("in %dh%dm", h, m)
		}
		return fmt.Sprintf("in %dh", h)
	default:
		return fmt.Sprintf("in %dd", int(d.Hours()/24))
	}
}

func (m *pipelineListModel) refresh(store *state.Store) {
	groupForPipeline := make(map[string]string)
	for _, g := range m.cfg.Groups {
		prefix := g.Key + "."
		for name := range m.cfg.Pipelines {
			if strings.HasPrefix(name, prefix) {
				groupForPipeline[name] = g.Key
			}
		}
	}

	summaryMap := make(map[string]state.PipelineSummary)
	if ps, err := store.PipelinesSummary(); err == nil {
		for _, p := range ps {
			summaryMap[p.Pipeline] = p
		}
	}

	m.nextRun = computeNextRun(m.cfg.Schedule, m.cfg.Timezone)

	rows := make([]table.Row, 0, len(m.names))
	for _, name := range m.names {
		p := m.cfg.Pipelines[name]
		summary, hasData := summaryMap[name]

		var statusText, ago, dur string
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
		} else {
			statusText = styleDim.Render(iconPending + " never run")
			ago = "-"
			dur = "-"
		}

		after := "-"
		if p != nil && len(p.After) > 0 {
			after = strings.Join(p.After, ", ")
		}

		if m.hasGroups {
			group := groupForPipeline[name]
			rows = append(rows, table.Row{group, name, statusText, ago, dur, m.nextRun, after})
		} else {
			rows = append(rows, table.Row{name, statusText, ago, dur, m.nextRun, after})
		}
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
	if m.hasGroups {
		return row[1]
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
