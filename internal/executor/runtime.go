package executor

import (
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"

	"github.com/flowerpothq/flowerpot/internal/config"
)

// ErrDockerNotAvailable is returned when a pipeline uses image: but docker is not on PATH.
var ErrDockerNotAvailable = errors.New("docker is required for image: but not found on PATH")

// RuntimeWrapper transforms a command before execution.
type RuntimeWrapper interface {
	Wrap(cmd []string) []string
}

// Passthrough returns the command unchanged.
type Passthrough struct{}

func (Passthrough) Wrap(cmd []string) []string { return cmd }

// UvWrapper rewrites commands to run through uv with inline and/or file-based dependencies.
type UvWrapper struct {
	Deps         []string
	Requirements string // absolute path to requirements file
}

func (u *UvWrapper) Wrap(cmd []string) []string {
	if len(u.Deps) == 0 && u.Requirements == "" {
		return cmd
	}
	args := []string{"uv", "run"}
	if u.Requirements != "" {
		args = append(args, "--with-requirements", u.Requirements)
	}
	for _, dep := range u.Deps {
		args = append(args, "--with", dep)
	}
	return append(args, cmd...)
}

// DockerWrapper rewrites commands to run inside a Docker container.
type DockerWrapper struct {
	Image      string
	ProjectDir string
}

func (d *DockerWrapper) Wrap(cmd []string) []string {
	args := []string{
		"docker", "run", "--rm",
		"-v", d.ProjectDir + ":/workspace",
		"-w", "/workspace",
		d.Image,
	}
	return append(args, cmd...)
}

// NewWrapper selects the appropriate runtime wrapper based on pipeline config.
func NewWrapper(p *config.Pipeline, projectDir string) (RuntimeWrapper, error) {
	if p.Image != "" {
		if _, err := exec.LookPath("docker"); err != nil {
			return nil, fmt.Errorf("%w: install from https://docs.docker.com/get-docker/", ErrDockerNotAvailable)
		}
		return &DockerWrapper{Image: p.Image, ProjectDir: projectDir}, nil
	}

	if p.Python != nil && (len(p.Python.Deps) > 0 || p.Python.Requirements != "") {
		if _, err := exec.LookPath("uv"); err != nil {
			return nil, fmt.Errorf("uv is required for python.deps/python.requirements but not found on PATH: install from https://docs.astral.sh/uv/")
		}
		w := &UvWrapper{Deps: p.Python.Deps}
		if p.Python.Requirements != "" {
			req := p.Python.Requirements
			if !filepath.IsAbs(req) {
				req = filepath.Join(projectDir, req)
			}
			w.Requirements = req
		}
		return w, nil
	}

	return Passthrough{}, nil
}
