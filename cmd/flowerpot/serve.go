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
	"syscall"
	"time"

	"github.com/flowerpothq/flowerpot/internal/config"
	"github.com/flowerpothq/flowerpot/internal/scheduler"
	"github.com/spf13/cobra"
)

var errServeFailed = fmt.Errorf("serve failed")

func serveCmd() *cobra.Command {
	var (
		configPath string
		port       int
	)

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the scheduler daemon",
		Long:  "Runs DAGs on their configured cron schedule.\nExposes a trigger endpoint for manual runs.\nStops gracefully on SIGTERM/SIGINT.",
		Example: `flowerpot serve
flowerpot serve -c path/to/flowerpot.yaml
flowerpot serve --port 9090`,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runServe(configPath, port)
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "flowerpot.yaml", "Path to flowerpot.yaml")
	cmd.Flags().IntVar(&port, "port", 9800, "HTTP port for trigger endpoint")

	return cmd
}

func runServe(configPath string, port int) error {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

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
	if err := writePIDFile(pidFile, port); err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s PID file: %s", iconFail, err)))
		return errServeFailed
	}
	defer func() { _ = os.Remove(pidFile) }()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	sched.Start(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler(store))
	mux.HandleFunc("POST /trigger", triggerHandler(result.Config, store, projectDir, logger))

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s listen :%d: %s", iconFail, port, err)))
		return errServeFailed
	}

	srv := &http.Server{Handler: mux}

	printStartupBanner(result.Config, sched, port)

	go func() {
		if err := srv.Serve(listener); err != nil && err != http.ErrServerClosed {
			logger.Error("http server", "error", err)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down...")

	stopCtx := sched.Stop()
	<-stopCtx.Done()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = srv.Shutdown(shutdownCtx)

	_ = os.Remove(pidFile)
	logger.Info("shutdown complete")
	return nil
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
