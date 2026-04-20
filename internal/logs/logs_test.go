package logs

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCreateLogDir_CreatesDirectory(t *testing.T) {
	projectDir := t.TempDir()
	dir, err := CreateLogDir(projectDir, "run-abc-123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := filepath.Join(projectDir, ".flowerpot/logs/run-abc-123")
	if dir != want {
		t.Fatalf("expected %q, got %q", want, dir)
	}

	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("directory not created: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("expected directory, got file")
	}
}

func TestCreateLogDir_Idempotent(t *testing.T) {
	projectDir := t.TempDir()
	dir1, err := CreateLogDir(projectDir, "run-same")
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	dir2, err := CreateLogDir(projectDir, "run-same")
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if dir1 != dir2 {
		t.Fatalf("expected same path, got %q and %q", dir1, dir2)
	}
}
