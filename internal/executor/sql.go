package executor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/flowerpothq/flowerpot/internal/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DBConn abstracts the database operations needed by SqlExecutor.
type DBConn interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Begin(ctx context.Context) (DBTx, error)
	Close(ctx context.Context) error
}

// DBTx abstracts a database transaction.
type DBTx interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// ConnectFunc opens a database connection given a DSN.
type ConnectFunc func(ctx context.Context, dsn string) (DBConn, error)

// pgxConn wraps *pgx.Conn to satisfy DBConn.
type pgxConn struct{ c *pgx.Conn }

func (p *pgxConn) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return p.c.Exec(ctx, sql, args...)
}
func (p *pgxConn) Begin(ctx context.Context) (DBTx, error) {
	tx, err := p.c.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return tx, nil
}
func (p *pgxConn) Close(ctx context.Context) error         { return p.c.Close(ctx) }

// PgxConnect is the default ConnectFunc using pgx.
func PgxConnect(ctx context.Context, dsn string) (DBConn, error) {
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &pgxConn{c: c}, nil
}

// SqlExecutor runs pipelines that have a sql: field against a warehouse.
type SqlExecutor struct {
	ProjectDir string
	Warehouses map[string]*config.Warehouse
	Connect    ConnectFunc
}

func (e *SqlExecutor) connectFunc() ConnectFunc {
	if e.Connect != nil {
		return e.Connect
	}
	return PgxConnect
}

func (e *SqlExecutor) Execute(ctx context.Context, name string, p *config.Pipeline, logDir string, attempt int) (*Result, error) {
	start := time.Now()

	stdoutPath, stderrPath := logPaths(logDir, name, attempt)

	wh, ok := e.Warehouses[p.Warehouse]
	if !ok {
		return nil, fmt.Errorf("warehouse %q not found", p.Warehouse)
	}

	sqlPath := p.SQL
	if !filepath.IsAbs(sqlPath) {
		sqlPath = filepath.Join(e.ProjectDir, sqlPath)
	}

	sqlBytes, err := os.ReadFile(sqlPath)
	if err != nil {
		return nil, fmt.Errorf("reading SQL file: %w", err)
	}

	sqlContent := config.InterpolateEnv(string(sqlBytes))
	statements := splitStatements(sqlContent)

	var logBuf strings.Builder

	conn, err := e.connectFunc()(ctx, wh.DSN)
	if err != nil {
		return nil, fmt.Errorf("connecting to %s: %w", wh.Driver, err)
	}
	defer func() { _ = conn.Close(ctx) }()

	if p.UseTransaction() {
		if err := execInTransaction(ctx, conn, statements, &logBuf); err != nil {
			writeLogFiles(stdoutPath, stderrPath, logBuf.String(), err.Error())
			return &Result{ExitCode: 1, Duration: time.Since(start), StdoutPath: stdoutPath, StderrPath: stderrPath}, nil
		}
	} else {
		if err := execStatements(ctx, conn, statements, &logBuf); err != nil {
			writeLogFiles(stdoutPath, stderrPath, logBuf.String(), err.Error())
			return &Result{ExitCode: 1, Duration: time.Since(start), StdoutPath: stdoutPath, StderrPath: stderrPath}, nil
		}
	}

	writeLogFiles(stdoutPath, stderrPath, logBuf.String(), "")

	return &Result{
		ExitCode:   0,
		Duration:   time.Since(start),
		StdoutPath: stdoutPath,
		StderrPath: stderrPath,
	}, nil
}

func execInTransaction(ctx context.Context, conn DBConn, stmts []string, logBuf *strings.Builder) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}

	for i, stmt := range stmts {
		fmt.Fprintf(logBuf, "-- statement %d\n%s\n", i+1, stmt)
		tag, err := tx.Exec(ctx, stmt)
		if err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("statement %d: %w", i+1, err)
		}
		fmt.Fprintf(logBuf, "-- rows affected: %d\n\n", tag.RowsAffected())
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	fmt.Fprintln(logBuf, "-- transaction committed")
	return nil
}

func execStatements(ctx context.Context, conn DBConn, stmts []string, logBuf *strings.Builder) error {
	for i, stmt := range stmts {
		fmt.Fprintf(logBuf, "-- statement %d\n%s\n", i+1, stmt)
		tag, err := conn.Exec(ctx, stmt)
		if err != nil {
			return fmt.Errorf("statement %d: %w", i+1, err)
		}
		fmt.Fprintf(logBuf, "-- rows affected: %d\n\n", tag.RowsAffected())
	}
	return nil
}

func splitStatements(sql string) []string {
	parts := strings.Split(sql, ";")
	var stmts []string
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			stmts = append(stmts, trimmed)
		}
	}
	return stmts
}

func writeLogFiles(stdoutPath, stderrPath, stdout, stderr string) {
	_ = os.WriteFile(stdoutPath, []byte(stdout), 0o644)
	_ = os.WriteFile(stderrPath, []byte(stderr), 0o644)
}
