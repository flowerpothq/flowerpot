# Flowerpot

**The data pipeline scheduler. One binary, no services.**

Flowerpot replaces heavyweight orchestrators like Airflow with a single Go binary. Define your DAG in YAML, run `flowerpot serve`, and your pipelines execute on schedule with retries, parallelism, and failure propagation.

## Install

```bash
# Homebrew (coming soon)
brew install flowerpothq/tap/flowerpot

# Go
go install github.com/flowerpothq/flowerpot/cmd/flowerpot@latest

# Binary (Linux/macOS, amd64/arm64)
# Download from https://github.com/flowerpothq/flowerpot/releases
```

## Quick Start

```bash
flowerpot init my-project
cd my-project
flowerpot validate
flowerpot serve
```

That's it. Your pipelines run on the schedule defined in `flowerpot.yaml`. Check results with `flowerpot status`.

### What `flowerpot init` creates

```
my-project/
  flowerpot.yaml         # pipeline definitions + schedule
  scripts/transform.sh   # example script
  .gitignore             # ignores .flowerpot/
```

### Example config

```yaml
schedule: "*/5 * * * *"
timezone: "UTC"

pipelines:
  extract:
    run: "python scripts/extract.py"
    timeout: "60s"

  transform:
    run: "bash scripts/transform.sh"
    after: [extract]
    retry:
      attempts: 2
      delay: "5s"

  load:
    run: "python scripts/load.py"
    after: [transform]
```

## Commands

| Command | Description |
|---------|-------------|
| `flowerpot init [dir]` | Scaffold a new project |
| `flowerpot validate [path]` | Validate config: schema, DAG cycles, SQL files |
| `flowerpot run` | Execute the full DAG |
| `flowerpot run <pipeline>` | Run a single pipeline |
| `flowerpot run <pipeline> --with-upstream` | Run with transitive dependencies |
| `flowerpot serve` | Start the daemon (cron + HTTP trigger) |
| `flowerpot trigger` | Trigger a DAG run via the running daemon |
| `flowerpot status` | Show recent DAG runs |
| `flowerpot logs [run-id] [pipeline]` | View pipeline logs |
| `flowerpot version` | Print version info |

All commands support `--json` for structured output where applicable.

## How it Works

### DAG execution

Pipelines run as a directed acyclic graph. Independent branches execute in parallel, bounded by `max_concurrent` (default 4). Dependencies are resolved via `after:`.

### Failure propagation

If a pipeline fails after exhausting retries, all transitive downstream pipelines are automatically skipped. Independent branches continue unaffected.

### Crash recovery

If the process is killed mid-run, orphaned tasks are marked failed on next startup and their downstream dependents are skipped.

### Scheduling

`flowerpot serve` runs your DAG on a cron schedule. If a run is still in progress when the next tick fires, it's skipped (overlap `skip` policy). Manual runs are triggered with `flowerpot trigger` or `POST /trigger`.

### State

All run history and task status lives in SQLite (`.flowerpot/state.db`, WAL mode). Logs are stored in `.flowerpot/logs/<run-id>/`.

## Configuration Reference

### Pipeline options

```yaml
pipelines:
  my-pipeline:
    run: "bash scripts/run.sh"       # shell command (mutually exclusive with sql)
    sql: "./sql/query.sql"           # SQL file (mutually exclusive with run)
    warehouse: analytics             # required with sql
    after: [dependency-1]            # DAG edges
    timeout: "5m"                    # kill after duration
    cwd: "./scripts"                 # working directory
    env:                             # extra env vars (supports ${VAR} interpolation)
      DB_HOST: "${DB_HOST}"
    retry:
      attempts: 3
      delay: "1m"
      strategy: "fixed"              # "fixed" (default) or "exponential"
    python:
      deps: ["pandas", "requests"]   # auto-wraps with uv run --with
```

### Global options

```yaml
schedule: "0 */2 * * *"             # cron schedule (used by flowerpot serve)
timezone: "UTC"                      # timezone for cron
max_concurrent: 4                    # max parallel pipelines
default_timeout: "1h"               # default per-pipeline timeout
default_retry:
  attempts: 1
  strategy: "fixed"
```

### Environment variables

Flowerpot injects these into every pipeline execution:

| Variable | Description |
|----------|-------------|
| `FLOWERPOT_PIPELINE` | Pipeline name |
| `FLOWERPOT_ATTEMPT` | Current retry attempt (1-based) |
| `FLOWERPOT_DAG_RUN_ID` | UUID of the current DAG run |
| `FLOWERPOT_LOGICAL_DATE` | UTC timestamp of the run |

## Why not Airflow?

Airflow is powerful but heavy: Python environment, metadata database, scheduler process, webserver, worker processes. For a small data team with 5-20 pipelines, that's a lot of infrastructure.

Flowerpot is a single binary. Install it, write a YAML, run `flowerpot serve`. No Docker, no Kubernetes, no database to manage.

## Development

```bash
go build -o flowerpot ./cmd/flowerpot   # build
go test ./...                            # 106 tests, 8 packages
go vet ./...                             # static analysis
```

6,600+ lines of Go across 10 packages.

## License

Apache 2.0
