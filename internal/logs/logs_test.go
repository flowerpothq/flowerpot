package logs

import (
	"os"
	"path/filepath"
	"testing"
	"time"
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

func TestVacuumLogDirs(t *testing.T) {
	projectDir := t.TempDir()

	// Create two log dirs
	oldDir, _ := CreateLogDir(projectDir, "old-run")
	newDir, _ := CreateLogDir(projectDir, "new-run")

	// Backdate the old dir's modtime
	old := time.Now().Add(-48 * time.Hour)
	_ = os.Chtimes(oldDir, old, old)
	// Ensure new dir is recent
	_ = os.Chtimes(newDir, time.Now(), time.Now())

	n, err := VacuumLogDirs(projectDir, 24*time.Hour)
	if err != nil {
		t.Fatalf("VacuumLogDirs: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 dir removed, got %d", n)
	}

	// Old dir should be gone
	if _, err := os.Stat(oldDir); !os.IsNotExist(err) {
		t.Fatal("expected old dir to be removed")
	}
	// New dir should remain
	if _, err := os.Stat(newDir); os.IsNotExist(err) {
		t.Fatal("expected new dir to remain")
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
