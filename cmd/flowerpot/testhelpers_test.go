package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
)

var (
	testBinaryOnce sync.Once
	testBinaryPath string
	testBinaryDir  string
	testBinaryErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if testBinaryDir != "" {
		_ = os.RemoveAll(testBinaryDir)
	}
	os.Exit(code)
}

func getTestBinary(t *testing.T) string {
	t.Helper()
	testBinaryOnce.Do(func() {
		dir, err := os.MkdirTemp("", "flowerpot-test-*")
		if err != nil {
			testBinaryErr = err
			return
		}
		testBinaryDir = dir
		testBinaryPath = filepath.Join(dir, "flowerpot-test")
		cmd := exec.Command("go", "build", "-buildvcs=false", "-o", testBinaryPath, "./cmd/flowerpot")
		cmd.Dir = filepath.Join("..", "..")
		out, err := cmd.CombinedOutput()
		if err != nil {
			testBinaryErr = err
			t.Logf("build output: %s", out)
		}
	})
	if testBinaryErr != nil {
		t.Fatalf("building test binary: %v", testBinaryErr)
	}
	return testBinaryPath
}

// execBinary runs the test binary and returns stdout, stderr, and exit code separately.
func execBinary(t *testing.T, args ...string) (stdout, stderr string, exitCode int) {
	t.Helper()
	bin := getTestBinary(t)
	cmd := exec.Command(bin, args...)
	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf
	err := cmd.Run()
	exitCode = 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			t.Fatalf("running binary: %v", err)
		}
	}
	return outBuf.String(), errBuf.String(), exitCode
}
