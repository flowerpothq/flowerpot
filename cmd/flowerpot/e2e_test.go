package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestEndToEnd_InitValidateRunServe(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "e2e-project")

	// 1. flowerpot init
	_, stderr, exitCode := execBinary(t, "init", target)
	if exitCode != 0 {
		t.Fatalf("init failed: exit %d\nstderr: %s", exitCode, stderr)
	}
	configPath := filepath.Join(target, "flowerpot.yaml")
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Fatal("flowerpot.yaml not created")
	}

	// 2. flowerpot validate
	_, stderr, exitCode = execBinary(t, "validate", configPath)
	if exitCode != 0 {
		t.Fatalf("validate failed: exit %d\nstderr: %s", exitCode, stderr)
	}

	// 3. flowerpot run (full DAG)
	stdout, stderr, exitCode := execBinary(t, "run", "-c", configPath)
	if exitCode != 0 {
		t.Fatalf("run failed: exit %d\nstdout: %s\nstderr: %s", exitCode, stdout, stderr)
	}

	// 4. flowerpot status --json
	stdout, stderr, exitCode = execBinary(t, "status", "-c", configPath, "--json")
	if exitCode != 0 {
		t.Fatalf("status failed: exit %d\nstderr: %s", exitCode, stderr)
	}
	var runs []statusRunJSON
	if err := json.Unmarshal([]byte(stdout), &runs); err != nil {
		t.Fatalf("status JSON invalid: %v\nstdout: %s", err, stdout)
	}
	if len(runs) == 0 {
		t.Fatal("expected at least 1 completed run")
	}
	if runs[0].Status != "completed" {
		t.Fatalf("expected completed run, got %s", runs[0].Status)
	}

	// 5. flowerpot run --json (verify JSON output)
	stdout, stderr, exitCode = execBinary(t, "run", "-c", configPath, "--json")
	if exitCode != 0 {
		t.Fatalf("run --json failed: exit %d\nstderr: %s", exitCode, stderr)
	}
	var dagResult dagRunResult
	if err := json.Unmarshal([]byte(stdout), &dagResult); err != nil {
		t.Fatalf("run JSON invalid: %v\nstdout: %s", err, stdout)
	}
	if dagResult.Status != "completed" {
		t.Fatalf("expected completed, got %s", dagResult.Status)
	}
	if len(dagResult.Tasks) == 0 {
		t.Fatal("expected at least 1 task in JSON output")
	}

	// 6. flowerpot logs (list runs)
	stdout, stderr, exitCode = execBinary(t, "logs", "-c", configPath)
	if exitCode != 0 {
		t.Fatalf("logs failed: exit %d\nstderr: %s", exitCode, stderr)
	}
	if len(stdout) == 0 {
		t.Fatal("expected logs output listing runs")
	}
}

func TestEndToEnd_ServeAndTrigger(t *testing.T) {
	dir := t.TempDir()
	content := `
schedule: "0 0 1 1 *"
timezone: "UTC"
pipelines:
  hello:
    run: "echo hello-e2e"
  world:
    run: "echo world-e2e"
    after: [hello]
`
	configPath := filepath.Join(dir, "flowerpot.yaml")
	if err := os.WriteFile(configPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	port := 19800 + (os.Getpid()+10)%1000

	done := make(chan error, 1)
	go func() {
		done <- runServe(configPath, port, "text")
	}()

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

	// Health check should include uptime
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/health", port))
	if err != nil {
		t.Fatalf("health check failed: %v", err)
	}
	var healthResult map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&healthResult)
	_ = resp.Body.Close()
	if healthResult["uptime"] == "" {
		t.Fatal("expected uptime field in health response")
	}
	if healthResult["status"] != "idle" {
		t.Fatalf("expected idle status, got %s", healthResult["status"])
	}

	// Trigger full DAG
	resp, err = http.Post(fmt.Sprintf("http://127.0.0.1:%d/trigger", port), "application/json", nil)
	if err != nil {
		t.Fatalf("trigger failed: %v", err)
	}
	var trigResult map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&trigResult)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", resp.StatusCode)
	}
	if trigResult["dag_run_id"] == "" {
		t.Fatal("expected dag_run_id")
	}

	// Wait for the DAG to complete
	time.Sleep(1 * time.Second)

	// Trigger a single pipeline
	resp, err = http.Post(fmt.Sprintf("http://127.0.0.1:%d/trigger/hello?scope=pipeline", port), "application/json", nil)
	if err != nil {
		t.Fatalf("pipeline trigger failed: %v", err)
	}
	var pipResult map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&pipResult)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %v", resp.StatusCode, pipResult)
	}
	if pipResult["pipeline"] != "hello" {
		t.Fatalf("expected pipeline=hello, got %s", pipResult["pipeline"])
	}

	time.Sleep(500 * time.Millisecond)

	// Clean shutdown
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

	// Verify PID file cleaned up
	pidFile := filepath.Join(dir, ".flowerpot", "flowerpot.pid")
	if _, err := os.Stat(pidFile); !os.IsNotExist(err) {
		t.Error("PID file should be removed after shutdown")
	}
}

func TestTriggerCLI_PipelineArg(t *testing.T) {
	dir := t.TempDir()
	content := `
schedule: "0 0 1 1 *"
timezone: "UTC"
pipelines:
  extract:
    run: "echo extract"
  transform:
    run: "echo transform"
    after: [extract]
`
	configPath := filepath.Join(dir, "flowerpot.yaml")
	if err := os.WriteFile(configPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	port := 19800 + (os.Getpid()+11)%1000

	done := make(chan error, 1)
	go func() {
		done <- runServe(configPath, port, "text")
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

	// Trigger a specific pipeline via HTTP
	resp, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d/trigger/extract", port), "application/json", nil)
	if err != nil {
		t.Fatalf("trigger failed: %v", err)
	}
	var result map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&result)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %v", resp.StatusCode, result)
	}
	if result["pipeline"] != "extract" {
		t.Fatalf("expected extract, got %s", result["pipeline"])
	}
	if result["scope"] != "dag" {
		t.Fatalf("expected default scope=dag, got %s", result["scope"])
	}

	time.Sleep(500 * time.Millisecond)

	p, _ := os.FindProcess(os.Getpid())
	_ = p.Signal(os.Interrupt)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not shut down")
	}
}

func TestOverlapValidation(t *testing.T) {
	dir := t.TempDir()
	configPath := writeRunTestYAML(t, dir, `
overlap: "invalid_policy"
pipelines:
  hello:
    run: "echo hello"
`)

	_, stderr, exitCode := execBinary(t, "validate", configPath)
	if exitCode == 0 {
		t.Fatal("expected validation error for invalid overlap policy")
	}
	if !strings.Contains(stderr, "unknown policy") {
		t.Fatalf("expected 'unknown policy' in stderr, got: %s", stderr)
	}
}

func TestRetryHTTP_ViaDaemon(t *testing.T) {
	dir := t.TempDir()
	content := `
schedule: "0 0 1 1 *"
timezone: "UTC"
pipelines:
  hello:
    run: "echo hello"
  fail-me:
    run: "exit 1"
    after: [hello]
`
	configPath := filepath.Join(dir, "flowerpot.yaml")
	if err := os.WriteFile(configPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	port := 19800 + (os.Getpid()+12)%1000

	done := make(chan error, 1)
	go func() {
		done <- runServe(configPath, port, "text")
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

	// Trigger full DAG (will have a failure)
	resp, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d/trigger", port), "application/json", nil)
	if err != nil {
		t.Fatalf("trigger failed: %v", err)
	}
	var trigResult map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&trigResult)
	_ = resp.Body.Close()
	dagRunID := trigResult["dag_run_id"]

	// Wait for DAG to complete (fail-me will fail)
	time.Sleep(2 * time.Second)

	// Retry via HTTP
	retryPrefix := dagRunID[:8]
	resp, err = http.Post(fmt.Sprintf("http://127.0.0.1:%d/retry/%s", port, retryPrefix), "application/json", nil)
	if err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	var retryResult map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&retryResult)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %v", resp.StatusCode, retryResult)
	}
	if retryResult["retry_of"] != dagRunID {
		t.Fatalf("expected retry_of=%s, got %s", dagRunID, retryResult["retry_of"])
	}

	time.Sleep(1 * time.Second)

	p, _ := os.FindProcess(os.Getpid())
	_ = p.Signal(os.Interrupt)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not shut down")
	}
}
