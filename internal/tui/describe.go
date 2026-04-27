package tui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/flowerpothq/flowerpot/internal/config"
)

type describeModel struct {
	vp       viewport.Model
	pipeline string
	content  string
	ready    bool
	back     bool
	width    int
	height   int
}

func newDescribeModel() describeModel {
	return describeModel{}
}

func (m *describeModel) load(name string, cfg *config.Config) {
	m.pipeline = name
	m.back = false

	p, ok := cfg.Pipelines[name]
	if !ok {
		m.content = styleFail.Render(fmt.Sprintf("pipeline %q not found in config", name))
		return
	}

	var b strings.Builder
	b.WriteString(styleBrand.Render("Pipeline: ") + styleBold.Render(name) + "\n\n")

	for _, g := range cfg.Groups {
		if strings.HasPrefix(name, g.Key+".") {
			b.WriteString(field("Group", g.Key))
			if g.Metadata != nil {
				if g.Metadata.Name != "" {
					b.WriteString(field("Group Name", g.Metadata.Name))
				}
				if len(g.Metadata.Tags) > 0 {
					b.WriteString(field("Tags", strings.Join(g.Metadata.Tags, ", ")))
				}
			}
			b.WriteString(field("Source", g.Source))
			b.WriteString("\n")
			break
		}
	}

	if cfg.Schedule != "" {
		b.WriteString(field("Schedule", cfg.Schedule))
		b.WriteString(field("Timezone", cfg.Timezone))
	}

	if p.Run != "" {
		b.WriteString(field("Command", p.Run))
	}
	if p.SQL != "" {
		b.WriteString(field("SQL", p.SQL))
	}
	if p.Warehouse != "" {
		b.WriteString(field("Warehouse", p.Warehouse))
	}
	if len(p.After) > 0 {
		b.WriteString(field("After", strings.Join(p.After, ", ")))
	} else {
		b.WriteString(field("After", "(none)"))
	}
	if p.Timeout != "" {
		b.WriteString(field("Timeout", p.Timeout))
	}
	if p.Cwd != "" {
		b.WriteString(field("Cwd", p.Cwd))
	}
	if p.Image != "" {
		b.WriteString(field("Image", p.Image))
	}

	if p.Retry != nil {
		b.WriteString(field("Retry",
			fmt.Sprintf("%d attempts, %s, %s", p.Retry.Attempts, p.Retry.Delay, p.Retry.Strategy)))
	}

	if p.Python != nil {
		if len(p.Python.Deps) > 0 {
			b.WriteString(field("Python deps", strings.Join(p.Python.Deps, ", ")))
		}
		if p.Python.Requirements != "" {
			b.WriteString(field("Requirements", p.Python.Requirements))
		}
	}

	if len(p.Env) > 0 {
		b.WriteString("\n" + styleBrand.Render("Environment:") + "\n")
		for k, v := range p.Env {
			fmt.Fprintf(&b, "  %s = %s\n", styleBold.Render(k), v)
		}
	}

	if p.Transaction != nil {
		b.WriteString(field("Transaction", fmt.Sprintf("%v", *p.Transaction)))
	}

	m.content = b.String()
	m.ready = false
}

func field(label, value string) string {
	return fmt.Sprintf("  %s  %s\n", styleDim.Render(padRight(label+":", 16)), value)
}

func padRight(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
}

func (m *describeModel) setSize(w, h int) {
	m.width = w
	m.height = h
	if m.ready {
		m.vp.Width = w
		m.vp.Height = h
	}
}

func (m *describeModel) ensureReady() {
	if !m.ready && m.width > 0 {
		m.vp = viewport.New(m.width, m.height)
		m.vp.SetContent(m.content)
		m.ready = true
	}
}

func (m *describeModel) update(msg tea.Msg) tea.Cmd {
	if kmsg, ok := msg.(tea.KeyMsg); ok {
		if kmsg.String() == "esc" || kmsg.String() == "h" {
			m.back = true
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

func (m *describeModel) hints() []keyHint {
	return []keyHint{
		{"esc", "Back"},
		{"?", "Help"},
		{"q", "Quit"},
	}
}

func (m *describeModel) view() string {
	m.ensureReady()
	if m.ready {
		return m.vp.View()
	}
	return m.content
}
