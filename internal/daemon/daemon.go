package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/flowerpothq/flowerpot/internal/config"
	"github.com/flowerpothq/flowerpot/internal/logs"
	"github.com/flowerpothq/flowerpot/internal/runner"
	"github.com/flowerpothq/flowerpot/internal/scheduler"
	"github.com/flowerpothq/flowerpot/internal/state"
	"github.com/google/uuid"
)

// Daemon encapsulates the scheduler, HTTP server, and DAG execution engine.
// It is used by both `flowerpot serve` and `flowerpot ui` (embedded mode).
type Daemon struct {
	Store      *state.Store
	Config     *config.Config
	ProjectDir string
	Logger     *slog.Logger

	port      int
	scheduler *scheduler.Scheduler
	listener  net.Listener
	srv       *http.Server
	pidFile   string

	runCtx    context.Context
	runCancel context.CancelFunc
	httpWg    sync.WaitGroup
	startedAt time.Time
}

type Option func(*Daemon)

func WithPort(port int) Option {
	return func(d *Daemon) { d.port = port }
}

func WithLogger(logger *slog.Logger) Option {
	return func(d *Daemon) { d.Logger = logger }
}

// New creates a Daemon. Call Start() to begin scheduling and serving HTTP.
func New(cfg *config.Config, store *state.Store, projectDir string, opts ...Option) (*Daemon, error) {
	d := &Daemon{
		Store:      store,
		Config:     cfg,
		ProjectDir: projectDir,
		Logger:     slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	}
	for _, o := range opts {
		o(d)
	}

	sched, err := scheduler.New(cfg, store, projectDir, d.Logger)
	if err != nil {
		return nil, fmt.Errorf("scheduler: %w", err)
	}
	d.scheduler = sched
	return d, nil
}

// Start begins the scheduler and HTTP server. The context controls the
// scheduler's cron loop. Call Stop() to shut everything down.
func (d *Daemon) Start(ctx context.Context) error {
	fpDir := filepath.Join(d.ProjectDir, ".flowerpot")
	if err := os.MkdirAll(fpDir, 0o755); err != nil {
		return err
	}
	d.pidFile = filepath.Join(fpDir, "flowerpot.pid")
	cleanStalePIDFile(d.pidFile)

	d.scheduler.Start(ctx)

	d.runCtx, d.runCancel = context.WithCancel(ctx)
	d.startedAt = time.Now()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", d.healthHandler())
	mux.HandleFunc("POST /trigger", d.triggerAllHandler())
	mux.HandleFunc("POST /trigger/", d.triggerPipelineHandler())
	mux.HandleFunc("POST /retry/", d.retryHandler())

	addr := fmt.Sprintf(":%d", d.port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	d.listener = listener
	d.port = listener.Addr().(*net.TCPAddr).Port

	if err := d.writePIDFile(); err != nil {
		_ = listener.Close()
		return fmt.Errorf("PID file: %w", err)
	}

	d.srv = &http.Server{Handler: mux}
	go func() {
		if err := d.srv.Serve(listener); err != nil && err != http.ErrServerClosed {
			d.Logger.Error("http server", "error", err)
		}
	}()

	if ret := d.Config.LogRetention; ret != "" {
		if dur, parseErr := time.ParseDuration(ret); parseErr == nil && dur > 0 {
			go d.runVacuum(ctx, dur)
		}
	}

	return nil
}

// Port returns the actual port the HTTP server is listening on.
func (d *Daemon) Port() int { return d.port }

// Stop gracefully shuts down the scheduler, HTTP server, and waits for running DAGs.
func (d *Daemon) Stop() error {
	d.Logger.Info("shutting down...")

	stopCtx := d.scheduler.Stop()
	<-stopCtx.Done()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if d.srv != nil {
		_ = d.srv.Shutdown(shutdownCtx)
	}

	d.runCancel()

	grace := 30 * time.Second
	if d.Config.ShutdownGrace != "" {
		if dur, parseErr := time.ParseDuration(d.Config.ShutdownGrace); parseErr == nil && dur > 0 {
			grace = dur
		}
	}

	d.waitForRuns(grace)

	if d.pidFile != "" {
		_ = os.Remove(d.pidFile)
	}
	d.Logger.Info("shutdown complete")
	return nil
}

// TriggerAll triggers a full DAG run. Returns the new run ID.
func (d *Daemon) TriggerAll(ctx context.Context) (string, error) {
	hasRunning, err := d.Store.HasRunningRun()
	if err != nil {
		return "", err
	}
	if hasRunning {
		return "", fmt.Errorf("a DAG run is already in progress")
	}

	dagRunID := uuid.New().String()
	now := time.Now().UTC().Format(time.RFC3339)

	if err := d.Store.InsertDAGRun(&state.DAGRun{
		ID: dagRunID, TriggerSource: "ui", StartedAt: now, Status: "running",
	}); err != nil {
		return "", err
	}

	deps := d.Config.DAGGraph()
	for name := range d.Config.Pipelines {
		_ = d.Store.InsertTask(&state.Task{
			ID: uuid.New().String(), DAGRunID: dagRunID, Pipeline: name,
			Status: "pending", Attempt: 1,
		})
	}

	logDir, err := logs.CreateLogDir(d.ProjectDir, dagRunID)
	if err != nil {
		return dagRunID, err
	}

	d.httpWg.Add(1)
	go func() {
		defer d.httpWg.Done()
		dr := &runner.DAGRunner{
			Store: d.Store, Config: d.Config, ProjectDir: d.ProjectDir,
			DagRunID: dagRunID, LogDir: logDir, MaxWorkers: d.maxWorkers(), Deps: deps,
		}
		status, runErr := dr.Run(ctx)
		if runErr != nil {
			d.Logger.Error("triggered DAG run failed", "dag_run_id", dagRunID[:8], "error", runErr)
		} else {
			d.Logger.Info("triggered DAG run finished", "dag_run_id", dagRunID[:8], "status", status)
		}
	}()
	return dagRunID, nil
}

// TriggerPipeline triggers a single pipeline (with upstream deps).
func (d *Daemon) TriggerPipeline(ctx context.Context, name string) (string, error) {
	if _, ok := d.Config.Pipelines[name]; !ok {
		return "", fmt.Errorf("pipeline %q not found", name)
	}

	hasRunning, err := d.Store.HasRunningRun()
	if err != nil {
		return "", err
	}
	if hasRunning {
		return "", fmt.Errorf("a DAG run is already in progress")
	}

	deps := d.Config.DAGGraph()
	upstreams := runner.UpstreamOf(name, deps)
	pipelineSet := make(map[string]bool, len(upstreams)+1)
	pipelineSet[name] = true
	for _, u := range upstreams {
		pipelineSet[u] = true
	}
	subDeps := make(map[string][]string, len(pipelineSet))
	for p := range pipelineSet {
		var relevant []string
		for _, dep := range deps[p] {
			if pipelineSet[dep] {
				relevant = append(relevant, dep)
			}
		}
		subDeps[p] = relevant
	}

	dagRunID := uuid.New().String()
	now := time.Now().UTC().Format(time.RFC3339)

	if err := d.Store.InsertDAGRun(&state.DAGRun{
		ID: dagRunID, TriggerSource: "ui", StartedAt: now, Status: "running",
	}); err != nil {
		return "", err
	}

	for n := range pipelineSet {
		_ = d.Store.InsertTask(&state.Task{
			ID: uuid.New().String(), DAGRunID: dagRunID, Pipeline: n,
			Status: "pending", Attempt: 1,
		})
	}

	logDir, err := logs.CreateLogDir(d.ProjectDir, dagRunID)
	if err != nil {
		return dagRunID, err
	}

	d.httpWg.Add(1)
	go func() {
		defer d.httpWg.Done()
		dr := &runner.DAGRunner{
			Store: d.Store, Config: d.Config, ProjectDir: d.ProjectDir,
			DagRunID: dagRunID, LogDir: logDir, MaxWorkers: d.maxWorkers(), Deps: subDeps,
		}
		status, runErr := dr.Run(ctx)
		if runErr != nil {
			d.Logger.Error("triggered pipeline run failed", "pipeline", name, "dag_run_id", dagRunID[:8], "error", runErr)
		} else {
			d.Logger.Info("triggered pipeline run finished", "pipeline", name, "dag_run_id", dagRunID[:8], "status", status)
		}
	}()
	return dagRunID, nil
}

// triggerPipelineOnly triggers a single pipeline (no upstream deps).
func (d *Daemon) triggerPipelineOnly(ctx context.Context, name string) (string, error) {
	if _, ok := d.Config.Pipelines[name]; !ok {
		return "", fmt.Errorf("pipeline %q not found", name)
	}

	hasRunning, err := d.Store.HasRunningRun()
	if err != nil {
		return "", err
	}
	if hasRunning {
		return "", fmt.Errorf("a DAG run is already in progress")
	}

	dagRunID := uuid.New().String()
	now := time.Now().UTC().Format(time.RFC3339)

	if err := d.Store.InsertDAGRun(&state.DAGRun{
		ID: dagRunID, TriggerSource: "ui", StartedAt: now, Status: "running",
	}); err != nil {
		return "", err
	}

	_ = d.Store.InsertTask(&state.Task{
		ID: uuid.New().String(), DAGRunID: dagRunID, Pipeline: name,
		Status: "pending", Attempt: 1,
	})

	logDir, err := logs.CreateLogDir(d.ProjectDir, dagRunID)
	if err != nil {
		return dagRunID, err
	}

	d.httpWg.Add(1)
	go func() {
		defer d.httpWg.Done()
		dr := &runner.DAGRunner{
			Store: d.Store, Config: d.Config, ProjectDir: d.ProjectDir,
			DagRunID: dagRunID, LogDir: logDir, MaxWorkers: d.maxWorkers(),
			Deps: map[string][]string{name: nil},
		}
		status, runErr := dr.Run(ctx)
		if runErr != nil {
			d.Logger.Error("triggered pipeline run failed", "pipeline", name, "dag_run_id", dagRunID[:8], "error", runErr)
		} else {
			d.Logger.Info("triggered pipeline run finished", "pipeline", name, "dag_run_id", dagRunID[:8], "status", status)
		}
	}()
	return dagRunID, nil
}

// RetryRun retries failed/skipped tasks from a previous run.
// Returns (retryRunID, originalRunID, error).
func (d *Daemon) RetryRun(ctx context.Context, runPrefix string) (string, string, error) {
	originalRun, err := d.Store.FindDAGRunByPrefix(runPrefix)
	if err != nil {
		return "", "", fmt.Errorf("run %q not found: %w", runPrefix, err)
	}
	if originalRun.Status == "running" {
		return "", "", fmt.Errorf("run is still in progress")
	}

	hasRunning, _ := d.Store.HasRunningRun()
	if hasRunning {
		return "", "", fmt.Errorf("a DAG run is already in progress")
	}

	originalTasks, err := d.Store.TasksByRun(originalRun.ID)
	if err != nil {
		return "", "", err
	}

	deps := d.Config.DAGGraph()
	retryRunID := uuid.New().String()
	now := time.Now().UTC().Format(time.RFC3339)

	if err := d.Store.InsertDAGRun(&state.DAGRun{
		ID: retryRunID, TriggerSource: "retry", StartedAt: now,
		Status: "running", RetryOf: originalRun.ID,
	}); err != nil {
		return "", "", err
	}

	pendingSet := make(map[string]bool)
	for _, t := range originalTasks {
		if t.Status == "failed" || t.Status == "skipped" {
			pendingSet[t.Pipeline] = true
		}
	}
	fwd := runner.ForwardGraph(deps)
	changed := true
	for changed {
		changed = false
		for p := range pendingSet {
			for _, child := range fwd[p] {
				if !pendingSet[child] {
					pendingSet[child] = true
					changed = true
				}
			}
		}
	}

	for name := range d.Config.Pipelines {
		status := "skipped_on_retry"
		if pendingSet[name] {
			status = "pending"
		}
		_ = d.Store.InsertTask(&state.Task{
			ID: uuid.New().String(), DAGRunID: retryRunID, Pipeline: name,
			Status: status, Attempt: 1,
		})
	}

	logDir, err := logs.CreateLogDir(d.ProjectDir, retryRunID)
	if err != nil {
		return retryRunID, originalRun.ID, err
	}

	d.httpWg.Add(1)
	go func() {
		defer d.httpWg.Done()
		dr := &runner.DAGRunner{
			Store: d.Store, Config: d.Config, ProjectDir: d.ProjectDir,
			DagRunID: retryRunID, LogDir: logDir, MaxWorkers: d.maxWorkers(), Deps: deps,
		}
		status, runErr := dr.Run(ctx)
		if runErr != nil {
			d.Logger.Error("retry run failed", "dag_run_id", retryRunID[:8], "error", runErr)
		} else {
			d.Logger.Info("retry run finished", "dag_run_id", retryRunID[:8], "status", status)
		}
	}()
	return retryRunID, originalRun.ID, nil
}

// IsSchedulerRunning returns true if the embedded scheduler has a run in progress.
func (d *Daemon) IsSchedulerRunning() bool {
	return d.scheduler.IsRunInProgress()
}

// NextRun returns the next scheduled cron time.
func (d *Daemon) NextRun() time.Time {
	return d.scheduler.NextRun()
}

// Uptime returns how long the daemon has been running.
func (d *Daemon) Uptime() time.Duration {
	return time.Since(d.startedAt).Truncate(time.Second)
}

func (d *Daemon) maxWorkers() int {
	if d.Config.MaxConcurrent <= 0 {
		return 4
	}
	return d.Config.MaxConcurrent
}

func (d *Daemon) writePIDFile() error {
	content := fmt.Sprintf("%d\n%d\n", os.Getpid(), d.port)
	return os.WriteFile(d.pidFile, []byte(content), 0o644)
}

func (d *Daemon) waitForRuns(grace time.Duration) {
	hasScheduled := d.scheduler.IsRunInProgress()
	httpDone := make(chan struct{})
	go func() { d.httpWg.Wait(); close(httpDone) }()

	select {
	case <-httpDone:
		if !hasScheduled {
			return
		}
	default:
	}

	d.Logger.Info("waiting for running DAGs to finish", "grace", grace.String())
	deadline := time.After(grace)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			d.Logger.Warn("grace period expired, cancelling running DAGs")
			d.scheduler.CancelRunningDAG()
			time.Sleep(2 * time.Second)
			return
		case <-httpDone:
			if !d.scheduler.IsRunInProgress() {
				return
			}
		case <-ticker.C:
			select {
			case <-httpDone:
			default:
				continue
			}
			if !d.scheduler.IsRunInProgress() {
				return
			}
		}
	}
}

func (d *Daemon) runVacuum(ctx context.Context, retention time.Duration) {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	vacuum := func() {
		cutoff := time.Now().Add(-retention)
		if n, err := d.Store.VacuumOldRuns(cutoff); err != nil {
			d.Logger.Error("vacuum: deleting old runs", "error", err)
		} else if n > 0 {
			d.Logger.Info("vacuum: removed old runs", "count", n)
		}
		if n, err := logs.VacuumLogDirs(d.ProjectDir, retention); err != nil {
			d.Logger.Error("vacuum: cleaning log dirs", "error", err)
		} else if n > 0 {
			d.Logger.Info("vacuum: removed old log dirs", "count", n)
		}
	}

	vacuum()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			vacuum()
		}
	}
}

// HTTP handlers

func (d *Daemon) healthHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hasRunning, _ := d.Store.HasRunningRun()
		status := "idle"
		if hasRunning {
			status = "running"
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status": status,
			"uptime": d.Uptime().String(),
		})
	}
}

func (d *Daemon) triggerAllHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		dagRunID, err := d.TriggerAll(d.runCtx)
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			code := http.StatusInternalServerError
			if strings.Contains(err.Error(), "already in progress") {
				code = http.StatusConflict
			}
			w.WriteHeader(code)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"dag_run_id": dagRunID, "status": "accepted",
		})
	}
}

func (d *Daemon) triggerPipelineHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pipelineName := r.URL.Path[len("/trigger/"):]
		if pipelineName == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "pipeline name required"})
			return
		}
		scope := r.URL.Query().Get("scope")
		if scope == "" {
			scope = "dag"
		}
		var dagRunID string
		var err error
		if scope == "pipeline" {
			dagRunID, err = d.triggerPipelineOnly(d.runCtx, pipelineName)
		} else {
			dagRunID, err = d.TriggerPipeline(d.runCtx, pipelineName)
		}
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			code := http.StatusInternalServerError
			if strings.Contains(err.Error(), "not found") {
				code = http.StatusNotFound
			} else if strings.Contains(err.Error(), "already in progress") {
				code = http.StatusConflict
			}
			w.WriteHeader(code)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"dag_run_id": dagRunID, "pipeline": pipelineName,
			"scope": scope, "status": "accepted",
		})
	}
}

func (d *Daemon) retryHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		runPrefix := r.URL.Path[len("/retry/"):]
		if runPrefix == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "dag-run-id required"})
			return
		}
		retryRunID, originalID, err := d.RetryRun(d.runCtx, runPrefix)
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			code := http.StatusInternalServerError
			if strings.Contains(err.Error(), "not found") {
				code = http.StatusNotFound
			} else if strings.Contains(err.Error(), "already in progress") || strings.Contains(err.Error(), "still in progress") {
				code = http.StatusConflict
			}
			w.WriteHeader(code)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"dag_run_id": retryRunID, "retry_of": originalID, "status": "accepted",
		})
	}
}

// IsDaemonAlive checks if an external daemon process is running by inspecting the PID file.
func IsDaemonAlive(projectDir string) bool {
	pidFile := filepath.Join(projectDir, ".flowerpot", "flowerpot.pid")
	data, err := os.ReadFile(pidFile)
	if err != nil {
		return false
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) == 0 {
		return false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(lines[0]))
	if err != nil || pid <= 0 {
		return false
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return proc.Signal(syscall.Signal(0)) == nil
}

func cleanStalePIDFile(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) == 0 {
		return
	}
	pid, err := strconv.Atoi(strings.TrimSpace(lines[0]))
	if err != nil {
		_ = os.Remove(path)
		return
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		_ = os.Remove(path)
		return
	}
	if err := proc.Signal(syscall.Signal(0)); err != nil {
		_ = os.Remove(path)
	}
}
