package runner

import "github.com/flowerpothq/flowerpot/internal/state"

// TaskStore is the subset of state.Store needed by propagation and the run loop.
type TaskStore interface {
	TasksByRun(dagRunID string) ([]state.Task, error)
	UpdateTask(id, status string, exitCode *int, startedAt, endedAt string) error
}

// ForwardGraph inverts a dependency map (pipeline → upstreams) into
// a forward adjacency map (pipeline → downstream dependents).
func ForwardGraph(deps map[string][]string) map[string][]string {
	fwd := make(map[string][]string, len(deps))
	for pipeline, upstreams := range deps {
		for _, up := range upstreams {
			fwd[up] = append(fwd[up], pipeline)
		}
	}
	return fwd
}

// DownstreamOf returns all transitive downstream pipelines of the given pipeline.
func DownstreamOf(pipeline string, fwd map[string][]string) []string {
	visited := make(map[string]bool)
	var result []string
	var walk func(string)
	walk = func(p string) {
		for _, child := range fwd[p] {
			if !visited[child] {
				visited[child] = true
				result = append(result, child)
				walk(child)
			}
		}
	}
	walk(pipeline)
	return result
}

// UpstreamOf returns all transitive upstream dependencies of the given pipeline.
func UpstreamOf(pipeline string, deps map[string][]string) []string {
	visited := make(map[string]bool)
	var result []string
	var walk func(string)
	walk = func(p string) {
		for _, parent := range deps[p] {
			if !visited[parent] {
				visited[parent] = true
				result = append(result, parent)
				walk(parent)
			}
		}
	}
	walk(pipeline)
	return result
}

// SkipDownstream marks all transitive downstream tasks of failedPipeline as "skipped"
// if they are still pending. Already-running or finished tasks are left as is.
func SkipDownstream(failedPipeline, dagRunID string, deps map[string][]string, store TaskStore) error {
	fwd := ForwardGraph(deps)
	toSkip := DownstreamOf(failedPipeline, fwd)
	if len(toSkip) == 0 {
		return nil
	}

	tasks, err := store.TasksByRun(dagRunID)
	if err != nil {
		return err
	}

	skipSet := make(map[string]bool, len(toSkip))
	for _, name := range toSkip {
		skipSet[name] = true
	}

	for _, t := range tasks {
		if skipSet[t.Pipeline] && t.Status == "pending" {
			if err := store.UpdateTask(t.ID, "skipped", nil, "", ""); err != nil {
				return err
			}
		}
	}
	return nil
}

// ComputeDAGRunStatus determines the final status of a DAG run based on task statuses.
//   - "completed" if all tasks succeeded
//   - "partial_failure" if some completed and some failed/skipped
//   - "failed" if no tasks completed
func ComputeDAGRunStatus(tasks []state.Task) string {
	if len(tasks) == 0 {
		return "completed"
	}

	var completed int
	for _, t := range tasks {
		if t.Status == "completed" {
			completed++
		}
	}

	if completed == len(tasks) {
		return "completed"
	}
	if completed > 0 {
		return "partial_failure"
	}
	return "failed"
}
