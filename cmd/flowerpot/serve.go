package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/flowerpothq/flowerpot/internal/config"
	"github.com/flowerpothq/flowerpot/internal/daemon"
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

	d, err := daemon.New(result.Config, store, projectDir,
		daemon.WithPort(port), daemon.WithLogger(logger))
	if err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s %s", iconFail, err)))
		return errServeFailed
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	if err := d.Start(ctx); err != nil {
		fmt.Fprintln(os.Stderr, styleFail.Render(fmt.Sprintf("  %s %s", iconFail, err)))
		return errServeFailed
	}

	printStartupBanner(result.Config, d)

	<-ctx.Done()
	_ = d.Stop()
	return nil
}

func printStartupBanner(cfg *config.Config, d *daemon.Daemon) {
	fmt.Println()
	fmt.Println(styleBrand.Render("  flowerpot serve"))
	fmt.Println()

	fmt.Println(styleDim.Render(fmt.Sprintf("  schedule   %s", cfg.Schedule)))
	fmt.Println(styleDim.Render(fmt.Sprintf("  timezone   %s", cfg.Timezone)))
	fmt.Println(styleDim.Render(fmt.Sprintf("  pipelines  %d", len(cfg.Pipelines))))
	fmt.Println(styleDim.Render(fmt.Sprintf("  overlap    %s", cfg.Overlap)))
	fmt.Println(styleDim.Render(fmt.Sprintf("  http       :%d", d.Port())))

	next := d.NextRun()
	if !next.IsZero() {
		fmt.Println(styleDim.Render(fmt.Sprintf("  next run   %s", next.Format("15:04:05"))))
	}

	fmt.Println()
	fmt.Println(styleDim.Render("  Press Ctrl+C to stop"))
	fmt.Println()
}
