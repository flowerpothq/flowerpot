package tui

import (
	"fmt"
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
		m.vp.Height = h - 4
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
		m.vp = viewport.New(m.width, m.height-4)
		m.ready = true
	}
	if m.ready {
		m.vp.SetContent(m.content)
	}
}

func (m *logTailModel) update(msg tea.Msg) tea.Cmd {
	if msg, ok := msg.(tea.KeyMsg); ok {
		switch msg.String() {
		case "esc", "h":
			m.back = true
			return nil
		case "G":
			m.vp.GotoBottom()
			return nil
		case "g":
			m.vp.GotoTop()
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

func (m *logTailModel) view(width, height int) string {
	var b strings.Builder
	b.WriteString(styleHeader.Render("  Logs"))
	b.WriteString("\n")
	if m.ready {
		b.WriteString(m.vp.View())
	} else {
		b.WriteString(m.content)
	}
	b.WriteString("\n")
	pct := ""
	if m.ready {
		pct = fmt.Sprintf(" %3.0f%%", m.vp.ScrollPercent()*100)
	}
	b.WriteString(styleDim.Render(fmt.Sprintf("  esc/h: back  ↑↓/PgUp/PgDn: scroll  g/G: top/bottom%s", pct)))
	return b.String()
}

func sanitize(name string) string {
	r := strings.NewReplacer("/", "_", " ", "_")
	return r.Replace(name)
}
