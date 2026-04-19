package state

import "embed"

//go:embed migrations/*.sql
var migrationsFS embed.FS

var migrationFiles = []string{
	"001_dag_runs.sql",
	"002_tasks.sql",
	"003_indexes.sql",
}
