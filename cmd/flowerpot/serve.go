package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/flowerpothq/flowerpot/internal/config"
	"github.com/flowerpothq/flowerpot/internal/logs"
	"github.com/flowerpothq/flowerpot/internal/scheduler"
	"github.com/flowerpothq/flowerpot/internal/state"
	"github.com/spf13/cobra"
)

var errServeFailed = fmt.Errorf("serve failed")

func serveCmd() *cobra.Command {
	var (
		configPath string
		port       int
		logFormat  string
	)

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the scheduler daemon",
		Long:  "Runs DAGs on their configured cron schedule.\nExposes a trigger endpoint for manual runs.\nStops gracefully on SIGTERM/SIGINT.",
		Example: `flowerpot serve
flowerpot serve -c path/to/flowerpot.yaml
flowerpot serve --port 9090
flowerpot serve --log-format json`,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runServe(configPath, port, logFormat)
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "flowerpot.yaml", "Path to flowerpot.yaml")
	cmd.Flags().IntVar(&port, "port", 9800, "HTTP port for trigger endpoint")
	cmd.Flags().StringVar(&logFormat, "log-format", "text", "Log format: text or json")

	return cmd
}

func runServe(configPath string, port int, logFormat string) error {
	var handler slog.Handler
	if logFormat == "json" {
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	} else {
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	}
	logger := slog.New(handler)

	result, projectDir, err := loadAndValidate(configPath)
	if err != nil {
		return err
	}

	store, err := openStore(projectDir)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	sched, err := scheduler.New(result.Config, store, projectDir, logger)
	if err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s scheduler: %s", iconFail, err)))
		return errServeFailed
	}

	fpDir := filepath.Join(projectDir, ".flowerpot")
	if err := os.MkdirAll(fpDir, 0o755); err != nil {
		return errServeFailed
	}
	pidFile := filepath.Join(fpDir, "flowerpot.pid")
	cleanStalePIDFile(pidFile)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	sched.Start(ctx)

	// runCtx is cancelled on shutdown; httpWg tracks HTTP-triggered goroutines.
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	var httpWg sync.WaitGroup

	startedAt := time.Now()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler(store, startedAt))
	mux.HandleFunc("POST /trigger", triggerHandler(result.Config, store, projectDir, logger, runCtx, &httpWg))
	mux.HandleFunc("POST /trigger/", pipelineTriggerHandler(result.Config, store, projectDir, logger, runCtx, &httpWg))
	mux.HandleFunc("POST /retry/", retryHTTPHandler(result.Config, store, projectDir, logger, runCtx, &httpWg))

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s listen :%d: %s", iconFail, port, err)))
		return errServeFailed
	}

	// Write PID file after listener is bound so the port is confirmed available.
	if err := writePIDFile(pidFile, port); err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s PID file: %s", iconFail, err)))
		_ = listener.Close()
		return errServeFailed
	}
	defer func() { _ = os.Remove(pidFile) }()

	srv := &http.Server{Handler: mux}

	printStartupBanner(result.Config, sched, port)

	go func() {
		if err := srv.Serve(listener); err != nil && err != http.ErrServerClosed {
			logger.Error("http server", "error", err)
		}
	}()

	if ret := result.Config.LogRetention; ret != "" {
		if d, parseErr := time.ParseDuration(ret); parseErr == nil && d > 0 {
			go runVacuum(ctx, store, projectDir, d, logger)
		}
	}

	<-ctx.Done()
	logger.Info("shutting down...")

	stopCtx := sched.Stop()
	<-stopCtx.Done()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = srv.Shutdown(shutdownCtx)

	// Cancel HTTP-triggered runs and wait for goroutines to finish.
	runCancel()

	grace := 30 * time.Second
	if result.Config.ShutdownGrace != "" {
		if d, parseErr := time.ParseDuration(result.Config.ShutdownGrace); parseErr == nil && d > 0 {
			grace = d
		}
	}

	waitForRuns(logger, sched, &httpWg, grace)

	_ = os.Remove(pidFile)
	logger.Info("shutdown complete")
	return nil
}

func waitForRuns(logger *slog.Logger, sched *scheduler.Scheduler, httpWg *sync.WaitGroup, grace time.Duration) {
	hasScheduled := sched.IsRunInProgress()
	// Quick non-blocking check if httpWg has outstanding goroutines by trying
	// to wait with zero timeout — if it completes instantly there are none.
	httpDone := make(chan struct{})
	go func() { httpWg.Wait(); close(httpDone) }()

	select {
	case <-httpDone:
		if !hasScheduled {
			return
		}
	default:
	}

	logger.Info("waiting for running DAGs to finish", "grace", grace.String())
	deadline := time.After(grace)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			logger.Warn("grace period expired, cancelling running DAGs")
			sched.CancelRunningDAG()
			time.Sleep(2 * time.Second)
			return
		case <-httpDone:
			if !sched.IsRunInProgress() {
				return
			}
		case <-ticker.C:
			select {
			case <-httpDone:
			default:
				continue
			}
			if !sched.IsRunInProgress() {
				return
			}
		}
	}
}

// runVacuum periodically removes old runs and log directories.
func runVacuum(ctx context.Context, store *state.Store, projectDir string, retention time.Duration, logger *slog.Logger) {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	vacuum := func() {
		cutoff := time.Now().Add(-retention)
		if n, err := store.VacuumOldRuns(cutoff); err != nil {
			logger.Error("vacuum: deleting old runs", "error", err)
		} else if n > 0 {
			logger.Info("vacuum: removed old runs", "count", n)
		}
		if n, err := logs.VacuumLogDirs(projectDir, retention); err != nil {
			logger.Error("vacuum: cleaning log dirs", "error", err)
		} else if n > 0 {
			logger.Info("vacuum: removed old log dirs", "count", n)
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

// cleanStalePIDFile removes a PID file if the process it references is no longer running.
func cleanStalePIDFile(path string) {
	pid, _, err := readPIDFile(path)
	if err != nil {
		return
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		_ = os.Remove(path)
		return
	}
	// Signal 0 checks if process exists without affecting it.
	if err := proc.Signal(syscall.Signal(0)); err != nil {
		_ = os.Remove(path)
	}
}

func printStartupBanner(cfg *config.Config, sched *scheduler.Scheduler, port int) {
	fmt.Println()
	fmt.Println(styleBrand.Render("  flowerpot serve"))
	fmt.Println()

	pipelineCount := len(cfg.Pipelines)
	fmt.Println(styleDim.Render(fmt.Sprintf("  schedule   %s", cfg.Schedule)))
	fmt.Println(styleDim.Render(fmt.Sprintf("  timezone   %s", cfg.Timezone)))
	fmt.Println(styleDim.Render(fmt.Sprintf("  pipelines  %d", pipelineCount)))
	fmt.Println(styleDim.Render(fmt.Sprintf("  overlap    %s", cfg.Overlap)))
	fmt.Println(styleDim.Render(fmt.Sprintf("  http       :%d", port)))

	next := sched.NextRun()
	if !next.IsZero() {
		fmt.Println(styleDim.Render(fmt.Sprintf("  next run   %s", next.Format("15:04:05"))))
	}

	fmt.Println()
	fmt.Println(styleDim.Render("  Press Ctrl+C to stop"))
	fmt.Println()
}

func writePIDFile(path string, port int) error {
	content := fmt.Sprintf("%d\n%d\n", os.Getpid(), port)
	return os.WriteFile(path, []byte(content), 0o644)
}

// readPIDFile returns (pid, port) from a PID file.
func readPIDFile(path string) (int, int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, err
	}
	var pid, port int
	n, err := fmt.Sscanf(string(data), "%d\n%d", &pid, &port)
	if err != nil || n < 2 {
		lines := splitLines(string(data))
		if len(lines) >= 1 {
			pid, _ = strconv.Atoi(lines[0])
		}
		if len(lines) >= 2 {
			port, _ = strconv.Atoi(lines[1])
		}
	}
	return pid, port, nil
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}
