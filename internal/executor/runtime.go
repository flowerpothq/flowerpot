package executor

import (
	"errors"
	"fmt"
	"os/exec"

	"github.com/flowerpothq/flowerpot/internal/config"
)

// ErrDockerNotImplemented is returned when a pipeline uses image: (Docker wrapper is Week 3).
var ErrDockerNotImplemented = errors.New("docker runtime wrapper is not yet implemented")

// RuntimeWrapper transforms a command before execution.
type RuntimeWrapper interface {
	Wrap(cmd []string) []string
}

// Passthrough returns the command unchanged.
type Passthrough struct{}

func (Passthrough) Wrap(cmd []string) []string { return cmd }

// UvWrapper rewrites commands to run through uv with inline dependencies.
type UvWrapper struct {
	Deps []string
}

func (u *UvWrapper) Wrap(cmd []string) []string {
	if len(u.Deps) == 0 {
		return cmd
	}
	args := []string{"uv", "run"}
	for _, dep := range u.Deps {
		args = append(args, "--with", dep)
	}
	return append(args, cmd...)
}

// NewWrapper selects the appropriate runtime wrapper based on pipeline config.
// Returns an error if docker image: is set (not yet implemented) or if uv is
// required but not found on PATH.
func NewWrapper(p *config.Pipeline) (RuntimeWrapper, error) {
	if p.Image != "" {
		return nil, fmt.Errorf("%w: pipeline uses image: %q", ErrDockerNotImplemented, p.Image)
	}

	if p.Python != nil && len(p.Python.Deps) > 0 {
		if _, err := exec.LookPath("uv"); err != nil {
			return nil, fmt.Errorf("uv is required for python.deps but not found on PATH: install from https://docs.astral.sh/uv/")
		}
		return &UvWrapper{Deps: p.Python.Deps}, nil
	}

	return Passthrough{}, nil
}
