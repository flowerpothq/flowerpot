package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeServeTestYAML(t *testing.T, dir string) string {
	t.Helper()
	content := `
schedule: "0 0 1 1 *"
timezone: "UTC"
pipelines:
  hello:
    run: "echo hello"
`
	path := filepath.Join(dir, "flowerpot.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestGracefulShutdown_DrainsCleanly(t *testing.T) {
	dir := t.TempDir()
	configPath := writeServeTestYAML(t, dir)

	port := 19800 + os.Getpid()%1000

	done := make(chan error, 1)
	go func() {
		done <- runServe(configPath, port)
	}()

	// Wait for server to start
	deadline := time.Now().Add(5 * time.Second)
	var started bool
	for time.Now().Before(deadline) {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/health", port))
		if err == nil {
			_ = resp.Body.Close()
			started = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !started {
		t.Fatal("server did not start in time")
	}

	// Send SIGINT via process signal
	p, _ := os.FindProcess(os.Getpid())
	_ = p.Signal(os.Interrupt)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not shut down in time")
	}

	pidFile := filepath.Join(dir, ".flowerpot", "flowerpot.pid")
	if _, err := os.Stat(pidFile); !os.IsNotExist(err) {
		t.Error("PID file should be removed after shutdown")
	}
}

func TestTriggerHTTP_CreatesDagRun(t *testing.T) {
	dir := t.TempDir()
	configPath := writeServeTestYAML(t, dir)

	port := 19800 + (os.Getpid()+1)%1000

	done := make(chan error, 1)
	go func() {
		done <- runServe(configPath, port)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/health", port))
		if err == nil {
			_ = resp.Body.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	resp, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d/trigger", port), "application/json", nil)
	if err != nil {
		t.Fatalf("trigger failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", resp.StatusCode)
	}

	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if result["dag_run_id"] == "" {
		t.Fatal("expected dag_run_id in response")
	}

	// Give the DAG run a moment to complete, then check
	time.Sleep(500 * time.Millisecond)

	p, _ := os.FindProcess(os.Getpid())
	_ = p.Signal(os.Interrupt)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not shut down")
	}
}

func TestTriggerHTTP_Health(t *testing.T) {
	dir := t.TempDir()
	configPath := writeServeTestYAML(t, dir)

	port := 19800 + (os.Getpid()+2)%1000

	done := make(chan error, 1)
	go func() {
		done <- runServe(configPath, port)
	}()

	deadline := time.Now().Add(5 * time.Second)
	var resp *http.Response
	for time.Now().Before(deadline) {
		var err error
		resp, err = http.Get(fmt.Sprintf("http://127.0.0.1:%d/health", port))
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if resp == nil {
		t.Fatal("server did not respond to health check")
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decoding: %v", err)
	}
	if result["status"] != "idle" {
		t.Fatalf("expected idle, got %s", result["status"])
	}

	p, _ := os.FindProcess(os.Getpid())
	_ = p.Signal(os.Interrupt)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not shut down")
	}
}
