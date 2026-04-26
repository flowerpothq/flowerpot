package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/flowerpothq/flowerpot/internal/config"
	"github.com/flowerpothq/flowerpot/internal/logs"
	"github.com/flowerpothq/flowerpot/internal/runner"
	"github.com/flowerpothq/flowerpot/internal/state"
	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

// Scheduler wraps a cron engine that fires DAG runs on schedule.
type Scheduler struct {
	cron       *cron.Cron
	store      *state.Store
	cfg        *config.Config
	projectDir string
	logger     *slog.Logger

	mu            sync.Mutex
	cancel        context.CancelFunc
	running       bool
	runCancel     context.CancelFunc // cancel func for the active DAG run
	runInProgress bool
}

// New creates a scheduler from a loaded config. The schedule and timezone
// come from the top-level flowerpot.yaml fields.
func New(cfg *config.Config, store *state.Store, projectDir string, logger *slog.Logger) (*Scheduler, error) {
	if cfg.Schedule == "" {
		return nil, fmt.Errorf("no schedule defined in config")
	}

	loc, err := time.LoadLocation(cfg.Timezone)
	if err != nil {
		return nil, fmt.Errorf("invalid timezone %q: %w", cfg.Timezone, err)
	}

	if cfg.Catchup {
		logger.Warn("catchup: true is not yet implemented; missed cron ticks will not be replayed")
	}

	s := &Scheduler{
		store:      store,
		cfg:        cfg,
		projectDir: projectDir,
		logger:     logger,
	}

	s.cron = cron.New(cron.WithLocation(loc), cron.WithSeconds())

	spec := cfg.Schedule
	if _, err := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor).Parse(spec); err != nil {
		spec = "0 " + cfg.Schedule
	}

	if _, err := s.cron.AddFunc(spec, s.tick); err != nil {
		return nil, fmt.Errorf("invalid schedule %q: %w", cfg.Schedule, err)
	}

	return s, nil
}

// Start begins the cron scheduler.
func (s *Scheduler) Start(ctx context.Context) {
	s.mu.Lock()
	s.running = true
	_, s.cancel = context.WithCancel(ctx)
	s.mu.Unlock()
	s.cron.Start()
}

// Stop halts the cron scheduler and waits for the current tick to finish.
func (s *Scheduler) Stop() context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.running = false
	if s.cancel != nil {
		s.cancel()
	}
	return s.cron.Stop()
}

// CancelRunningDAG cancels the currently running DAG run's context, if any.
func (s *Scheduler) CancelRunningDAG() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.runCancel != nil {
		s.runCancel()
	}
}

// IsRunInProgress returns true if a DAG run is currently executing.
func (s *Scheduler) IsRunInProgress() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.runInProgress
}

// NextRun returns the time of the next scheduled tick.
func (s *Scheduler) NextRun() time.Time {
	entries := s.cron.Entries()
	if len(entries) == 0 {
		return time.Time{}
	}
	return entries[0].Next
}

func (s *Scheduler) tick() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	hasRunning, err := s.store.HasRunningRun()
	if err != nil {
		s.logger.Error("checking running state", "error", err)
		return
	}

	if hasRunning {
		overlap := s.cfg.Overlap
		if overlap == "" {
			overlap = "skip"
		}

		switch overlap {
		case "skip":
			s.logger.Info("skipping tick: previous run still in progress")
			return

		case "queue":
			s.logger.Info("queuing DAG run: previous run still in progress")
			if err := s.queueDagRun(); err != nil {
				s.logger.Error("queueing DAG run failed", "error", err)
			}
			return

		case "kill_previous":
			s.logger.Info("killing previous run to start new one")
			s.mu.Lock()
			if s.runCancel != nil {
				s.runCancel()
			}
			s.mu.Unlock()
			// Wait briefly for the cancelled run to finish marking its state
			time.Sleep(500 * time.Millisecond)

		default:
			s.logger.Warn("unknown overlap policy, defaulting to skip", "overlap", overlap)
			return
		}
	}

	s.logger.Info("cron tick: starting DAG run")
	if err := s.executeDagRun(); err != nil {
		s.logger.Error("DAG run failed", "error", err)
	}

	s.drainQueuedRuns()
}

func (s *Scheduler) queueDagRun() error {
	dagRunID := uuid.New().String()
	now := time.Now().UTC().Format(time.RFC3339)
	return s.store.InsertDAGRun(&state.DAGRun{
		ID:            dagRunID,
		TriggerSource: "schedule",
		StartedAt:     now,
		Status:        "queued",
	})
}

func (s *Scheduler) drainQueuedRuns() {
	for {
		run, err := s.store.OldestQueuedRun()
		if err != nil || run == nil {
			return
		}

		s.logger.Info("executing queued run", "dag_run_id", run.ID[:8])
		if err := s.executeExistingRun(run.ID); err != nil {
			s.logger.Error("queued DAG run failed", "dag_run_id", run.ID[:8], "error", err)
		}
	}
}

func (s *Scheduler) executeDagRun() error {
	dagRunID := uuid.New().String()
	now := time.Now().UTC().Format(time.RFC3339)

	if err := s.store.InsertDAGRun(&state.DAGRun{
		ID:            dagRunID,
		TriggerSource: "schedule",
		StartedAt:     now,
		Status:        "running",
	}); err != nil {
		return fmt.Errorf("inserting dag_run: %w", err)
	}

	deps := s.cfg.DAGGraph()
	for name := range s.cfg.Pipelines {
		taskID := uuid.New().String()
		if err := s.store.InsertTask(&state.Task{
			ID:       taskID,
			DAGRunID: dagRunID,
			Pipeline: name,
			Status:   "pending",
			Attempt:  1,
		}); err != nil {
			return fmt.Errorf("inserting task: %w", err)
		}
	}

	return s.runDAG(dagRunID, deps)
}

func (s *Scheduler) executeExistingRun(dagRunID string) error {
	if err := s.store.UpdateDAGRun(dagRunID, "running", ""); err != nil {
		return fmt.Errorf("updating dag_run to running: %w", err)
	}

	deps := s.cfg.DAGGraph()
	for name := range s.cfg.Pipelines {
		taskID := uuid.New().String()
		if err := s.store.InsertTask(&state.Task{
			ID:       taskID,
			DAGRunID: dagRunID,
			Pipeline: name,
			Status:   "pending",
			Attempt:  1,
		}); err != nil {
			return fmt.Errorf("inserting task: %w", err)
		}
	}

	return s.runDAG(dagRunID, deps)
}

func (s *Scheduler) runDAG(dagRunID string, deps map[string][]string) error {
	logDir, err := logs.CreateLogDir(s.projectDir, dagRunID)
	if err != nil {
		return fmt.Errorf("creating log dir: %w", err)
	}

	maxWorkers := s.cfg.MaxConcurrent
	if maxWorkers <= 0 {
		maxWorkers = 4
	}

	ctx, cancel := context.WithCancel(context.Background())

	s.mu.Lock()
	s.runCancel = cancel
	s.runInProgress = true
	s.mu.Unlock()

	dagRunner := &runner.DAGRunner{
		Store:      s.store,
		Config:     s.cfg,
		ProjectDir: s.projectDir,
		DagRunID:   dagRunID,
		LogDir:     logDir,
		MaxWorkers: maxWorkers,
		Deps:       deps,
	}

	status, runErr := dagRunner.Run(ctx)

	s.mu.Lock()
	s.runCancel = nil
	s.runInProgress = false
	s.mu.Unlock()

	cancel()
	s.logger.Info("DAG run finished", "dag_run_id", dagRunID[:8], "status", status)
	return runErr
}
