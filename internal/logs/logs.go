package logs

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const defaultBaseDir = ".flowerpot/logs"

// CreateLogDir creates the log directory for a specific DAG run.
// Returns the absolute path to the created directory.
func CreateLogDir(projectDir string, dagRunID string) (string, error) {
	dir := filepath.Join(projectDir, defaultBaseDir, dagRunID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("creating log directory: %w", err)
	}
	return dir, nil
}

// VacuumLogDirs removes log directories older than the given retention.
// Returns the number of directories removed and any error.
func VacuumLogDirs(projectDir string, retention time.Duration) (int, error) {
	base := filepath.Join(projectDir, defaultBaseDir)
	entries, err := os.ReadDir(base)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	cutoff := time.Now().Add(-retention)
	removed := 0

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			dir := filepath.Join(base, e.Name())
			if err := os.RemoveAll(dir); err == nil {
				removed++
			}
		}
	}
	return removed, nil
}
