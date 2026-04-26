package runner

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/flowerpothq/flowerpot/internal/config"
	"github.com/flowerpothq/flowerpot/internal/executor"
	"github.com/flowerpothq/flowerpot/internal/state"
)

// DAGRunner executes a full DAG using SQLite-as-queue.
// Ready-task resolution goes through a single DB query; idle goroutines
// block on a channel (zero CPU when nothing is pending).
type DAGRunner struct {
	Store      *state.Store
	Config     *config.Config
	ProjectDir string
	DagRunID   string
	LogDir     string
	MaxWorkers int
	Deps       map[string][]string // pipeline → upstream deps (from config.DAGGraph)
}

// Run executes the DAG, blocking until all tasks are completed, failed, or skipped.
// Returns the final DAG status ("completed", "partial_failure", "failed").
func (r *DAGRunner) Run(ctx context.Context) (string, error) {
	if err := r.recoverOrphans(); err != nil {
		return "failed", fmt.Errorf("crash recovery: %w", err)
	}

	sem := make(chan struct{}, r.MaxWorkers)
	// Buffered(1) channel: goroutines signal task completion. Only one signal
	// can be buffered; extras are dropped. The main loop re-queries SQLite after
	// each wakeup, so dropped signals don't cause missed state.
	taskDone := make(chan struct{}, 1)
	var wg sync.WaitGroup

	for ctx.Err() == nil {
		tasks, err := r.Store.TasksByRun(r.DagRunID)
		if err != nil {
			wg.Wait()
			return "failed", fmt.Errorf("querying tasks: %w", err)
		}

		if allFinished(tasks) {
			break
		}

		ready := filterReady(tasks, r.Deps)
		if len(ready) == 0 {
			select {
			case <-taskDone:
			case <-ctx.Done():
			case <-time.After(200 * time.Millisecond):
				// Safety: re-query if a signal was missed.
			}
			continue
		}

		now := time.Now().UTC().Format(time.RFC3339)
		for i := range ready {
			_ = r.Store.UpdateTask(ready[i].ID, "running", nil, now, "")
		}

		for _, task := range ready {
			wg.Add(1)
			sem <- struct{}{} // acquire semaphore slot; blocks at max_concurrent
			go func(t state.Task, startedAt string) {
				defer func() {
					<-sem // release slot
					wg.Done()
					// Non-blocking send: wake the main loop. Dropped if
					// the channel already has a pending signal.
					select {
					case taskDone <- struct{}{}:
					default:
					}
				}()
				r.executeTask(ctx, t, startedAt)
			}(task, now)
		}
	}

	wg.Wait()

	finalTasks, err := r.Store.TasksByRun(r.DagRunID)
	if err != nil {
		return "failed", err
	}

	status := ComputeDAGRunStatus(finalTasks)
	endedAt := time.Now().UTC().Format(time.RFC3339)
	_ = r.Store.UpdateDAGRun(r.DagRunID, status, endedAt)

	return status, nil
}

func (r *DAGRunner) executeTask(ctx context.Context, task state.Task, startedAt string) {
	p := r.Config.Pipelines[task.Pipeline]
	if p == nil {
		now := time.Now().UTC().Format(time.RFC3339)
		_ = r.Store.UpdateTask(task.ID, "failed", nil, startedAt, now)
		_ = SkipDownstream(task.Pipeline, r.DagRunID, r.Deps, r.Store)
		return
	}

	exec, err := r.buildExecutor(p)
	if err != nil {
		now := time.Now().UTC().Format(time.RFC3339)
		_ = r.Store.UpdateTask(task.ID, "failed", nil, startedAt, now)
		_ = SkipDownstream(task.Pipeline, r.DagRunID, r.Deps, r.Store)
		return
	}

	rc := DefaultRetryConfig(p, r.Config.DefaultRetry)

	taskCtx := ctx
	if p.Timeout != "" {
		if d, parseErr := time.ParseDuration(p.Timeout); parseErr == nil && d > 0 {
			var cancel context.CancelFunc
			taskCtx, cancel = context.WithTimeout(ctx, d)
			defer cancel()
		}
	}

	// Pass nil store so the retry engine doesn't write intermediate
	// per-attempt statuses — the DAG runner manages task state.
	result, _ := ExecuteWithRetry(taskCtx, task.Pipeline, p, exec, rc, r.LogDir, "", nil)

	now := time.Now().UTC().Format(time.RFC3339)
	status := "completed"
	var exitCode *int
	if result == nil || result.Result == nil {
		status = "failed"
	} else {
		ec := result.ExitCode
		exitCode = &ec
		if ec != 0 {
			status = "failed"
		}
	}

	_ = r.Store.UpdateTask(task.ID, status, exitCode, startedAt, now)

	if status == "failed" {
		_ = SkipDownstream(task.Pipeline, r.DagRunID, r.Deps, r.Store)
	}
}

func (r *DAGRunner) buildExecutor(p *config.Pipeline) (executor.Executor, error) {
	if p.SQL != "" {
		return &executor.SqlExecutor{
			ProjectDir: r.ProjectDir,
			Warehouses: r.Config.Warehouses,
		}, nil
	}

	wrapper, err := executor.NewWrapper(p, r.ProjectDir)
	if err != nil {
		return nil, err
	}
	return &executor.CommandExecutor{
		ProjectDir: r.ProjectDir,
		FlowerpotEnv: map[string]string{
			"FLOWERPOT_DAG_RUN_ID":   r.DagRunID,
			"FLOWERPOT_LOGICAL_DATE": time.Now().UTC().Format(time.RFC3339),
		},
		Wrapper: wrapper,
	}, nil
}

func (r *DAGRunner) recoverOrphans() error {
	tasks, err := r.Store.TasksByRun(r.DagRunID)
	if err != nil {
		return err
	}
	now := time.Now().UTC().Format(time.RFC3339)
	var recovered []string
	for _, t := range tasks {
		if t.Status == "running" {
			ec := -1
			if err := r.Store.UpdateTask(t.ID, "failed", &ec, t.StartedAt, now); err != nil {
				return err
			}
			recovered = append(recovered, t.Pipeline)
		}
	}
	for _, pipeline := range recovered {
		_ = SkipDownstream(pipeline, r.DagRunID, r.Deps, r.Store)
	}
	return nil
}

func allFinished(tasks []state.Task) bool {
	for _, t := range tasks {
		switch t.Status {
		case "pending", "running":
			return false
		}
	}
	return true
}

func filterReady(tasks []state.Task, deps map[string][]string) []state.Task {
	statusMap := make(map[string]string, len(tasks))
	for _, t := range tasks {
		statusMap[t.Pipeline] = t.Status
	}

	var ready []state.Task
	for _, t := range tasks {
		if t.Status != "pending" {
			continue
		}
		upstreams := deps[t.Pipeline]
		blocked := false
		for _, dep := range upstreams {
			s := statusMap[dep]
			if s != "completed" && s != "skipped_on_retry" {
				blocked = true
				break
			}
		}
		if !blocked {
			ready = append(ready, t)
		}
	}
	return ready
}
