package executor

import (
	"errors"
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

func TestRuntimeFactory_PicksCorrectWrapper(t *testing.T) {
	tests := []struct {
		name     string
		pipeline *config.Pipeline
		wantType string
		wantErr  error
	}{
		{
			name:     "passthrough for plain run",
			pipeline: &config.Pipeline{Run: "echo hello"},
			wantType: "executor.Passthrough",
		},
		{
			name: "docker returns not implemented",
			pipeline: &config.Pipeline{
				Run:   "echo hello",
				Image: "python:3.12",
			},
			wantErr: ErrDockerNotImplemented,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, err := NewWrapper(tt.pipeline)
			if tt.wantErr != nil {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("expected error %v, got %v", tt.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			got := reflect.TypeOf(w).String()
			if got != tt.wantType {
				t.Fatalf("expected type %s, got %s", tt.wantType, got)
			}
		})
	}
}
