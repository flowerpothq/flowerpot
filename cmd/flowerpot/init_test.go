package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInitCommand_CreatesFiles(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "my-project")

	_, stderr, exitCode := execBinary(t, "init", target)
	if exitCode != 0 {
		t.Fatalf("expected exit 0, got %d\nstderr: %s", exitCode, stderr)
	}

	for _, f := range []string{"flowerpot.yaml", ".gitignore"} {
		path := filepath.Join(target, f)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Errorf("expected %s to exist", f)
		}
	}
}

func TestInitCommand_ValidOutput(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "proj")

	_, _, exitCode := execBinary(t, "init", target)
	if exitCode != 0 {
		t.Fatal("init should succeed")
	}

	stdout, stderr, exitCode := execBinary(t, "validate", filepath.Join(target, "flowerpot.yaml"))
	if exitCode != 0 {
		t.Fatalf("scaffolded yaml should validate\nstdout: %s\nstderr: %s", stdout, stderr)
	}
}

func TestInitCommand_NoOverwrite(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "flowerpot.yaml"), []byte("existing"), 0o644); err != nil {
		t.Fatal(err)
	}

	stdout, stderr, exitCode := execBinary(t, "init", dir)
	if exitCode != 0 {
		t.Fatal("init should succeed even if yaml exists")
	}

	content, _ := os.ReadFile(filepath.Join(dir, "flowerpot.yaml"))
	if string(content) != "existing" {
		t.Fatal("existing flowerpot.yaml should not be overwritten")
	}
	combined := stdout + stderr
	if !strings.Contains(combined, "already exists") {
		t.Fatalf("should warn about existing yaml, got stdout: %s\nstderr: %s", stdout, stderr)
	}
}
