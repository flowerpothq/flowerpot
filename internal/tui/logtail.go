package tui

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
)

type logTailModel struct {
	stdoutPath string
	stderrPath string
	content    string
	vp         viewport.Model
	ready      bool
	back       bool
	width      int
	height     int
}

func newLogTailModel() logTailModel {
	return logTailModel{}
}

func (m *logTailModel) setSize(w, h int) {
	m.width = w
	m.height = h
	if m.ready {
		m.vp.Width = w
		m.vp.Height = h
	}
}

func (m *logTailModel) load(projectDir, runID, pipeline string) {
	safe := sanitize(pipeline)
	logDir := filepath.Join(projectDir, ".flowerpot", "logs", runID)
	m.stdoutPath = filepath.Join(logDir, safe+".stdout")
	m.stderrPath = filepath.Join(logDir, safe+".stderr")
	m.content = ""
	m.ready = false
	m.back = false
	m.refresh()
}

func (m *logTailModel) refresh() {
	var b strings.Builder
	if data, err := os.ReadFile(m.stdoutPath); err == nil && len(data) > 0 {
		b.WriteString(string(data))
	}
	if data, err := os.ReadFile(m.stderrPath); err == nil && len(data) > 0 {
		if b.Len() > 0 {
			b.WriteString("\n")
		}
		b.WriteString(styleFail.Render("--- stderr ---"))
		b.WriteString("\n")
		b.WriteString(string(data))
	}
	if b.Len() == 0 {
		b.WriteString(styleDim.Render("(no log output)"))
	}
	m.content = b.String()

	if !m.ready && m.width > 0 {
		m.vp = viewport.New(m.width, m.height)
		m.ready = true
	}
	if m.ready {
		m.vp.SetContent(m.content)
	}
}

func (m *logTailModel) update(msg tea.Msg) tea.Cmd {
	if kmsg, ok := msg.(tea.KeyMsg); ok {
		switch kmsg.String() {
		case "esc", "h":
			m.back = true
			return nil
		case "G":
			if m.ready {
				m.vp.GotoBottom()
			}
			return nil
		case "g":
			if m.ready {
				m.vp.GotoTop()
			}
			return nil
		}
	}
	if m.ready {
		var cmd tea.Cmd
		m.vp, cmd = m.vp.Update(msg)
		return cmd
	}
	return nil
}

func (m *logTailModel) hints() []keyHint {
	return []keyHint{
		{"↑↓", "Scroll"},
		{"g/G", "Top/Bottom"},
		{"esc", "Back"},
		{"?", "Help"},
		{"q", "Quit"},
	}
}

func (m *logTailModel) view(width, height int) string {
	if m.ready {
		return m.vp.View()
	}
	return m.content
}

func sanitize(name string) string {
	r := strings.NewReplacer("/", "_", " ", "_")
	return r.Replace(name)
}
