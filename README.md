# Flowerpot

A lightweight data pipeline scheduler. Single binary, zero infrastructure.

## Install

```bash
go build -o flowerpot ./cmd/flowerpot

# Or with Homebrew (coming soon)
brew install flowerpothq/tap/flowerpot
```

## Quick Start

Create a `flowerpot.yaml`:

```yaml
pipelines:
  extract:
    run: "python scripts/extract.py"
    env:
      API_KEY: "${API_KEY}"

  transform:
    run: "python scripts/transform.py"
    after: [extract]

  load:
    run: "python scripts/load.py"
    after: [transform]
    retry:
      attempts: 3
      delay: "30s"
```

Validate the config:

```bash
$ flowerpot validate
```

Run a pipeline:

```bash
$ flowerpot run extract
$ flowerpot run load --json
```

## Commands

| Command | Status | Description |
|---------|--------|-------------|
| `flowerpot validate [path]` | Working | Validate config: schema, DAG cycles, SQL file checks |
| `flowerpot run <pipeline>` | Working | Execute a single `run:` pipeline with retries and timeout |
| `flowerpot run <pipeline> --json` | Working | JSON output for scripting |
| `flowerpot run <pipeline> --timeout 30s` | Working | Override pipeline timeout |
| `flowerpot version` | Working | Print version info |
| `flowerpot run <sql-pipeline>` | Not yet | SQL executor exists but not wired to CLI |
| `flowerpot serve` | Not yet | Scheduler daemon |
| `flowerpot status` | Not yet | Pipeline and run status |

## Configuration

See [`flowerpot-examples/`](https://github.com/flowerpothq/flowerpot-examples) for full examples.

### Pipeline options

```yaml
pipelines:
  my-pipeline:
    run: "bash scripts/run.sh"     # shell command (mutually exclusive with sql)
    # sql: "./sql/query.sql"       # SQL file (not yet wired to CLI)
    # warehouse: analytics         # required with sql
    after: [dependency-1]          # DAG edges
    timeout: "5m"                  # kill after duration
    cwd: "./scripts"               # working directory
    env:                           # extra env vars (supports ${VAR} interpolation)
      DB_HOST: "${DB_HOST}"
    retry:
      attempts: 3
      delay: "1m"
      strategy: "fixed"            # "fixed" (default) or "exponential"
    python:
      deps: ["pandas", "requests"] # auto-wraps with uv run --with
```

### Runtime features

- **Retries**: fixed or exponential backoff, per-attempt log files
- **Timeouts**: SIGTERM with 10s grace period, then SIGKILL
- **Env vars**: `FLOWERPOT_PIPELINE`, `FLOWERPOT_ATTEMPT`, `FLOWERPOT_DAG_RUN_ID` injected automatically
- **Python deps**: `python.deps` wraps commands with `uv run --with` (requires [uv](https://docs.astral.sh/uv/))
- **Logs**: stdout/stderr captured to `.flowerpot/logs/<run-id>/`
- **State**: SQLite tracking of runs, tasks, attempts in `.flowerpot/state.db`

## Development

```bash
go build -o bin/flowerpot ./cmd/flowerpot
make check
```

## License

Apache 2.0
