package executor

import (
	"reflect"
	"testing"

	"github.com/flowerpothq/flowerpot/internal/config"
)

func TestPassthrough_NoChange(t *testing.T) {
	w := Passthrough{}
	input := []string{"python", "./script.py"}
	got := w.Wrap(input)
	if !reflect.DeepEqual(got, input) {
		t.Fatalf("expected %v, got %v", input, got)
	}
}

func TestUvWrapper_RewritesCommand(t *testing.T) {
	w := &UvWrapper{Deps: []string{"pandas", "dlt>=0.5"}}
	input := []string{"python", "./script.py"}
	got := w.Wrap(input)
	want := []string{"uv", "run", "--with", "pandas", "--with", "dlt>=0.5", "python", "./script.py"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestUvWrapper_EmptyDeps(t *testing.T) {
	w := &UvWrapper{Deps: []string{}}
	input := []string{"python", "./script.py"}
	got := w.Wrap(input)
	if !reflect.DeepEqual(got, input) {
		t.Fatalf("expected passthrough for empty deps, got %v", got)
	}
}

func TestUvWrapper_Requirements(t *testing.T) {
	w := &UvWrapper{Requirements: "/proj/requirements.txt"}
	input := []string{"python", "./script.py"}
	got := w.Wrap(input)
	want := []string{"uv", "run", "--with-requirements", "/proj/requirements.txt", "python", "./script.py"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestUvWrapper_DepsAndRequirements(t *testing.T) {
	w := &UvWrapper{
		Deps:         []string{"extra-lib"},
		Requirements: "/proj/requirements.txt",
	}
	input := []string{"python", "./script.py"}
	got := w.Wrap(input)
	want := []string{
		"uv", "run",
		"--with-requirements", "/proj/requirements.txt",
		"--with", "extra-lib",
		"python", "./script.py",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestDockerWrapper_RewritesCommand(t *testing.T) {
	w := &DockerWrapper{Image: "python:3.12", ProjectDir: "/home/user/project"}
	input := []string{"sh", "-c", "python script.py"}
	got := w.Wrap(input)
	want := []string{
		"docker", "run", "--rm",
		"-v", "/home/user/project:/workspace",
		"-w", "/workspace",
		"python:3.12",
		"sh", "-c", "python script.py",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestRuntimeFactory_PicksCorrectWrapper(t *testing.T) {
	tests := []struct {
		name     string
		pipeline *config.Pipeline
		wantType string
		wantErr  bool
	}{
		{
			name:     "passthrough for plain run",
			pipeline: &config.Pipeline{Run: "echo hello"},
			wantType: "executor.Passthrough",
		},
		{
			name: "uv wrapper for python.deps",
			pipeline: &config.Pipeline{
				Run:    "python script.py",
				Python: &config.PythonConfig{Deps: []string{"pandas"}},
			},
			wantType: "*executor.UvWrapper",
		},
		{
			name: "uv wrapper for python.requirements",
			pipeline: &config.Pipeline{
				Run:    "python script.py",
				Python: &config.PythonConfig{Requirements: "requirements.txt"},
			},
			wantType: "*executor.UvWrapper",
		},
		{
			name: "docker wrapper for image",
			pipeline: &config.Pipeline{
				Run:   "python script.py",
				Image: "python:3.12",
			},
			wantType: "*executor.DockerWrapper",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, err := NewWrapper(tt.pipeline, "/tmp/test-project")
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			// uv/docker might not be on PATH in CI — skip if tool missing
			if err != nil {
				t.Skipf("skipping: required tool not on PATH: %v", err)
			}
			got := reflect.TypeOf(w).String()
			if got != tt.wantType {
				t.Fatalf("expected type %s, got %s", tt.wantType, got)
			}
		})
	}
}
