package logs

import (
	"fmt"
	"os"
	"path/filepath"
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
