package tui

import (
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/flowerpothq/flowerpot/internal/state"
)

type viewID int

const (
	viewPipelineList viewID = iota
	viewRunHistory
	viewLogTail
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
	projectDir string

	currentView viewID
	prevView    viewID
	width       int
	height      int

	pipelines pipelineListModel
	runs      runHistoryModel
	logTail   logTailModel
	statusBar statusBarModel
}

// NewModel creates the root TUI model.
func NewModel(store *state.Store, projectDir string) Model {
	return Model{
		store:       store,
		projectDir:  projectDir,
		currentView: viewPipelineList,
		pipelines:   newPipelineListModel(),
		runs:        newRunHistoryModel(),
		logTail:     newLogTailModel(),
		statusBar:   newStatusBarModel(projectDir),
	}
}

func (m Model) Init() tea.Cmd {
	return tickCmd()
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
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
		}

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.logTail.setSize(m.width, m.height-4)
		return m, nil

	case tickMsg:
		var cmds []tea.Cmd
		cmds = append(cmds, tickCmd())

		m.statusBar.update(m.store)

		switch m.currentView {
		case viewPipelineList:
			m.pipelines.refresh(m.store)
		case viewRunHistory:
			m.runs.refresh(m.store)
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
			m.runs.selectPipeline(m.pipelines.selectedPipeline())
			m.runs.refresh(m.store)
			m.currentView = viewRunHistory
		}
		return m, cmd

	case viewRunHistory:
		cmd := m.runs.update(msg)
		if m.runs.back {
			m.runs.back = false
			m.currentView = viewPipelineList
		} else if m.runs.viewLogs {
			m.runs.viewLogs = false
			runID := m.runs.selectedRunID()
			pipeline := m.runs.pipeline
			m.logTail.load(m.projectDir, runID, pipeline)
			m.logTail.setSize(m.width, m.height-4)
			m.currentView = viewLogTail
		}
		return m, cmd

	case viewLogTail:
		cmd := m.logTail.update(msg)
		if m.logTail.back {
			m.logTail.back = false
			m.currentView = viewRunHistory
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

func (m Model) View() string {
	if m.width == 0 {
		return "Loading..."
	}

	var body string
	switch m.currentView {
	case viewPipelineList:
		body = m.pipelines.view(m.width, m.height-3)
	case viewRunHistory:
		body = m.runs.view(m.width, m.height-3)
	case viewLogTail:
		body = m.logTail.view(m.width, m.height-3)
	case viewHelp:
		body = viewHelp_(m.width, m.height-3)
	}

	bar := m.statusBar.view(m.width, m.currentView, m.runs.pipeline, m.runs.selectedRunID())
	return body + "\n" + bar
}
