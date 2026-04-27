package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/flowerpothq/flowerpot/internal/config"
	"github.com/flowerpothq/flowerpot/internal/daemon"
	"github.com/flowerpothq/flowerpot/internal/state"
)

type viewID int

const (
	viewPipelineList viewID = iota
	viewRunHistory
	viewTaskDetail
	viewLogTail
	viewDescribe
	viewHelp
)

type tickMsg time.Time

func tickCmd() tea.Cmd {
	return tea.Tick(500*time.Millisecond, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

// Model is the root bubbletea model for the flowerpot TUI.
type Model struct {
	store      *state.Store
	cfg        *config.Config
	daemon     *daemon.Daemon // nil when connecting to external daemon
	projectDir string

	currentView viewID
	prevView    viewID
	width       int
	height      int

	flash    *flashMsg
	flashExp time.Time

	filtering bool
	filterInput textinput.Model
	filterText  string

	pipelines pipelineListModel
	runs      runHistoryModel
	tasks     taskDetailModel
	logTail   logTailModel
	describe  describeModel
}

// NewModel creates the root TUI model.
func NewModel(store *state.Store, cfg *config.Config, d *daemon.Daemon, projectDir string) Model {
	ti := textinput.New()
	ti.Prompt = "/ "
	ti.PromptStyle = styleBrand
	ti.CharLimit = 64
	return Model{
		store:       store,
		cfg:         cfg,
		daemon:      d,
		projectDir:  projectDir,
		currentView: viewPipelineList,
		filterInput: ti,
		pipelines:   newPipelineListModel(cfg),
		runs:        newRunHistoryModel(),
		tasks:       newTaskDetailModel(),
		logTail:     newLogTailModel(),
		describe:    newDescribeModel(),
	}
}

func (m Model) Init() tea.Cmd {
	return tickCmd()
}

func (m *Model) setFlash(text string, ok bool) {
	style := styleFlashOK
	if !ok {
		style = styleFlashErr
	}
	m.flash = &flashMsg{text: text, style: style}
	m.flashExp = time.Now().Add(3 * time.Second)
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	if m.filtering {
		return m.updateFilter(msg)
	}

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		case "?":
			if m.currentView == viewHelp {
				m.currentView = m.prevView
			} else {
				m.prevView = m.currentView
				m.currentView = viewHelp
			}
			return m, nil
		case "/":
			if m.currentView == viewPipelineList || m.currentView == viewRunHistory || m.currentView == viewTaskDetail {
				m.filtering = true
				m.filterInput.SetValue(m.filterText)
				m.filterInput.Focus()
				return m, m.filterInput.Cursor.BlinkCmd()
			}
		}

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		tableH := m.height - 6
		if tableH < 3 {
			tableH = 3
		}
		m.pipelines.setHeight(tableH)
		m.runs.setHeight(tableH)
		m.tasks.setHeight(tableH)
		m.logTail.setSize(m.width, tableH)
		m.describe.setSize(m.width, tableH)
		return m, nil

	case tickMsg:
		if m.flash != nil && time.Now().After(m.flashExp) {
			m.flash = nil
		}

		var cmds []tea.Cmd
		cmds = append(cmds, tickCmd())

		switch m.currentView {
		case viewPipelineList:
			m.pipelines.refresh(m.store)
		case viewRunHistory:
			m.runs.refresh(m.store)
		case viewTaskDetail:
			m.tasks.refresh(m.store)
		case viewLogTail:
			m.logTail.refresh()
		}
		return m, tea.Batch(cmds...)
	}

	switch m.currentView {
	case viewPipelineList:
		cmd := m.pipelines.update(msg)
		if m.pipelines.drillDown {
			m.pipelines.drillDown = false
			m.filterText = ""
			m.runs.filter = ""
			m.runs.selectPipeline(m.pipelines.selectedPipeline())
			m.runs.refresh(m.store)
			m.currentView = viewRunHistory
		} else if m.pipelines.describe {
			m.pipelines.describe = false
			m.describe.load(m.pipelines.selectedPipeline(), m.cfg)
			m.describe.setSize(m.width, m.height-6)
			m.currentView = viewDescribe
		} else if m.pipelines.triggerOne {
			m.pipelines.triggerOne = false
			m.doTriggerPipeline(m.pipelines.selectedPipeline())
		} else if m.pipelines.triggerAll {
			m.pipelines.triggerAll = false
			m.doTriggerAll()
		}
		return m, cmd

	case viewRunHistory:
		cmd := m.runs.update(msg)
		if m.runs.back {
			m.runs.back = false
			m.filterText = ""
			m.currentView = viewPipelineList
		} else if m.runs.viewTasks {
			m.runs.viewTasks = false
			m.filterText = ""
			m.tasks.filter = ""
			m.tasks.selectRun(m.runs.selectedRunID())
			m.tasks.refresh(m.store)
			m.currentView = viewTaskDetail
		} else if m.runs.retry {
			m.runs.retry = false
			m.doRetry(m.runs.selectedRunID())
		}
		return m, cmd

	case viewTaskDetail:
		cmd := m.tasks.update(msg)
		if m.tasks.back {
			m.tasks.back = false
			m.filterText = ""
			m.currentView = viewRunHistory
		} else if m.tasks.viewLogs {
			m.tasks.viewLogs = false
			m.logTail.load(m.projectDir, m.tasks.runID, m.tasks.selectedPipeline())
			m.logTail.setSize(m.width, m.height-6)
			m.currentView = viewLogTail
		} else if m.tasks.retry {
			m.tasks.retry = false
			m.doRetry(m.tasks.runID)
		}
		return m, cmd

	case viewLogTail:
		cmd := m.logTail.update(msg)
		if m.logTail.back {
			m.logTail.back = false
			m.currentView = viewTaskDetail
		}
		return m, cmd

	case viewDescribe:
		cmd := m.describe.update(msg)
		if m.describe.back {
			m.describe.back = false
			m.currentView = viewPipelineList
		}
		return m, cmd

	case viewHelp:
		if msg, ok := msg.(tea.KeyMsg); ok {
			if msg.String() == "esc" || msg.String() == "?" {
				m.currentView = m.prevView
			}
		}
		return m, nil
	}

	return m, nil
}

func (m Model) updateFilter(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "enter":
			m.filterText = m.filterInput.Value()
			m.filtering = false
			m.filterInput.Blur()
			m.applyFilter()
			return m, nil
		case "esc":
			m.filterText = ""
			m.filtering = false
			m.filterInput.Blur()
			m.applyFilter()
			return m, nil
		}
	case tickMsg:
		return m, tickCmd()
	}
	var cmd tea.Cmd
	m.filterInput, cmd = m.filterInput.Update(msg)
	return m, cmd
}

func (m *Model) applyFilter() {
	switch m.currentView {
	case viewPipelineList:
		m.pipelines.filter = m.filterText
		m.pipelines.applyFilter()
	case viewRunHistory:
		m.runs.filter = m.filterText
		m.runs.applyFilter()
	case viewTaskDetail:
		m.tasks.filter = m.filterText
		m.tasks.applyFilter()
	}
}

func (m *Model) doTriggerPipeline(name string) {
	if name == "" {
		return
	}
	if m.daemon != nil {
		id, err := m.daemon.TriggerPipeline(context.Background(), name)
		if err != nil {
			m.setFlash(fmt.Sprintf("%s trigger %s: %s", iconFail, name, err), false)
		} else {
			m.setFlash(fmt.Sprintf("%s triggered %s → %s", iconPass, name, id[:8]), true)
		}
	} else {
		m.setFlash(iconFail+" no scheduler available", false)
	}
}

func (m *Model) doTriggerAll() {
	if m.daemon != nil {
		id, err := m.daemon.TriggerAll(context.Background())
		if err != nil {
			m.setFlash(fmt.Sprintf("%s trigger all: %s", iconFail, err), false)
		} else {
			m.setFlash(fmt.Sprintf("%s triggered full DAG → %s", iconPass, id[:8]), true)
		}
	} else {
		m.setFlash(iconFail+" no scheduler available", false)
	}
}

func (m *Model) doRetry(runID string) {
	if runID == "" {
		return
	}
	if m.daemon != nil {
		id, _, err := m.daemon.RetryRun(context.Background(), runID)
		if err != nil {
			short := runID
			if len(short) > 8 {
				short = short[:8]
			}
			m.setFlash(fmt.Sprintf("%s retry %s: %s", iconFail, short, err), false)
		} else {
			m.setFlash(fmt.Sprintf("%s retry → %s", iconPass, id[:8]), true)
		}
	} else {
		m.setFlash(iconFail+" no scheduler available", false)
	}
}

func (m Model) View() string {
	if m.width == 0 {
		return "Loading..."
	}

	var breadcrumb []string
	var hints []keyHint

	switch m.currentView {
	case viewPipelineList:
		breadcrumb = []string{"Pipelines"}
		hints = m.pipelines.hints()
	case viewRunHistory:
		breadcrumb = []string{"Pipelines", m.runs.pipeline}
		hints = m.runs.hints()
	case viewTaskDetail:
		short := m.tasks.runID
		if len(short) > 8 {
			short = short[:8]
		}
		breadcrumb = []string{"Pipelines", m.runs.pipeline, short}
		hints = m.tasks.hints()
	case viewLogTail:
		breadcrumb = []string{"Pipelines", m.runs.pipeline, "Logs"}
		hints = m.logTail.hints()
	case viewDescribe:
		breadcrumb = []string{"Pipelines", m.describe.pipeline, "Describe"}
		hints = m.describe.hints()
	case viewHelp:
		breadcrumb = []string{"Help"}
		hints = []keyHint{{"esc", "Back"}, {"?", "Toggle"}, {"q", "Quit"}}
	}

	header := renderHeader(m.width, breadcrumb, m.daemon, m.projectDir)

	var body string
	switch m.currentView {
	case viewPipelineList:
		body = m.pipelines.view()
	case viewRunHistory:
		body = m.runs.view()
	case viewTaskDetail:
		body = m.tasks.view()
	case viewLogTail:
		body = m.logTail.view(m.width, m.height-6)
	case viewDescribe:
		body = m.describe.view()
	case viewHelp:
		body = viewHelp_(m.width, m.height-6)
	}

	if m.filtering {
		filterLine := m.filterInput.View()
		return header + "\n" + body + "\n" + styleDim.Render(strings.Repeat("─", m.width)) + "\n" + filterLine
	}

	var filterIndicator string
	if m.filterText != "" {
		filterIndicator = styleDim.Render(" [filter: "+m.filterText+"]")
	}

	keybar := renderKeyBar(m.width, hints, m.flash)
	return header + "\n" + body + filterIndicator + "\n" + keybar
}
