package executor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/flowerpothq/flowerpot/internal/config"
	"github.com/jackc/pgx/v5/pgconn"
)

func boolPtr(b bool) *bool { return &b }

// mockTx records executed statements and can simulate errors.
type mockTx struct {
	stmts      []string
	failOnStmt int // 1-indexed; 0 means no failure
	committed  bool
	rolledBack bool
}

func (m *mockTx) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	m.stmts = append(m.stmts, sql)
	if m.failOnStmt > 0 && len(m.stmts) == m.failOnStmt {
		return pgconn.NewCommandTag(""), fmt.Errorf("simulated error on statement %d", m.failOnStmt)
	}
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (m *mockTx) Commit(_ context.Context) error {
	m.committed = true
	return nil
}

func (m *mockTx) Rollback(_ context.Context) error {
	m.rolledBack = true
	return nil
}

// mockConn records direct-exec statements and produces mockTx on Begin.
type mockConn struct {
	stmts      []string
	failOnStmt int // for direct exec mode
	tx         *mockTx
}

func (m *mockConn) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	m.stmts = append(m.stmts, sql)
	if m.failOnStmt > 0 && len(m.stmts) == m.failOnStmt {
		return pgconn.NewCommandTag(""), fmt.Errorf("simulated error on statement %d", m.failOnStmt)
	}
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (m *mockConn) Begin(_ context.Context) (DBTx, error) {
	if m.tx == nil {
		m.tx = &mockTx{}
	}
	return m.tx, nil
}

func (m *mockConn) Close(_ context.Context) error { return nil }

func newMockConnect(mc *mockConn) ConnectFunc {
	return func(_ context.Context, _ string) (DBConn, error) {
		return mc, nil
	}
}

func writeSQLFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatalf("writing SQL file: %v", err)
	}
}

func TestSqlExecutor_Success(t *testing.T) {
	dir := t.TempDir()
	logDir := t.TempDir()
	writeSQLFile(t, dir, "test.sql", "CREATE TABLE t (id int);\nINSERT INTO t VALUES (1);")

	mc := &mockConn{tx: &mockTx{}}
	e := &SqlExecutor{
		ProjectDir: dir,
		Warehouses: map[string]*config.Warehouse{"pg": {Driver: "postgres", DSN: "mock"}},
		Connect:    newMockConnect(mc),
	}
	p := &config.Pipeline{SQL: "test.sql", Warehouse: "pg"}

	result, err := e.Execute(context.Background(), "test", p, logDir, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ExitCode != 0 {
		stderr, _ := os.ReadFile(result.StderrPath)
		t.Fatalf("expected exit 0, got %d\nstderr: %s", result.ExitCode, stderr)
	}
	if len(mc.tx.stmts) != 2 {
		t.Fatalf("expected 2 statements executed in tx, got %d", len(mc.tx.stmts))
	}
	if !mc.tx.committed {
		t.Fatal("expected transaction to be committed")
	}
}

func TestSqlExecutor_TransactionRollback(t *testing.T) {
	dir := t.TempDir()
	logDir := t.TempDir()
	writeSQLFile(t, dir, "test.sql", "INSERT INTO t VALUES (1);\nINSERT INTO bad_table VALUES (1);")

	mc := &mockConn{tx: &mockTx{failOnStmt: 2}}
	e := &SqlExecutor{
		ProjectDir: dir,
		Warehouses: map[string]*config.Warehouse{"pg": {Driver: "postgres", DSN: "mock"}},
		Connect:    newMockConnect(mc),
	}
	p := &config.Pipeline{SQL: "test.sql", Warehouse: "pg"}

	result, err := e.Execute(context.Background(), "test", p, logDir, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ExitCode != 1 {
		t.Fatalf("expected exit 1, got %d", result.ExitCode)
	}
	if !mc.tx.rolledBack {
		t.Fatal("expected transaction to be rolled back")
	}
	if mc.tx.committed {
		t.Fatal("transaction should not be committed after failure")
	}

	stderr, _ := os.ReadFile(result.StderrPath)
	if !strings.Contains(string(stderr), "statement 2") {
		t.Fatalf("expected stderr to mention statement 2, got: %s", stderr)
	}
}

func TestSqlExecutor_NoTransaction(t *testing.T) {
	dir := t.TempDir()
	logDir := t.TempDir()
	writeSQLFile(t, dir, "test.sql", "INSERT INTO t VALUES (1);\nINSERT INTO bad_table VALUES (1);")

	mc := &mockConn{failOnStmt: 2}
	e := &SqlExecutor{
		ProjectDir: dir,
		Warehouses: map[string]*config.Warehouse{"pg": {Driver: "postgres", DSN: "mock"}},
		Connect:    newMockConnect(mc),
	}
	p := &config.Pipeline{SQL: "test.sql", Warehouse: "pg", Transaction: boolPtr(false)}

	result, err := e.Execute(context.Background(), "test", p, logDir, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ExitCode != 1 {
		t.Fatalf("expected exit 1, got %d", result.ExitCode)
	}
	// First statement executed directly on conn, not via tx
	if len(mc.stmts) != 2 {
		t.Fatalf("expected 2 statements attempted on conn, got %d", len(mc.stmts))
	}
	// No transaction should have been started
	if mc.tx != nil {
		t.Fatal("expected no transaction to be started")
	}
}

func TestSqlExecutor_VarInterpolation(t *testing.T) {
	dir := t.TempDir()
	logDir := t.TempDir()
	writeSQLFile(t, dir, "test.sql", "INSERT INTO t VALUES ('${FP_TEST_VAL}');")

	t.Setenv("FP_TEST_VAL", "hello_world")

	mc := &mockConn{tx: &mockTx{}}
	e := &SqlExecutor{
		ProjectDir: dir,
		Warehouses: map[string]*config.Warehouse{"pg": {Driver: "postgres", DSN: "mock"}},
		Connect:    newMockConnect(mc),
	}
	p := &config.Pipeline{SQL: "test.sql", Warehouse: "pg"}

	result, err := e.Execute(context.Background(), "test", p, logDir, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("expected exit 0, got %d", result.ExitCode)
	}
	if len(mc.tx.stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(mc.tx.stmts))
	}
	if !strings.Contains(mc.tx.stmts[0], "hello_world") {
		t.Fatalf("expected interpolated value in SQL, got: %s", mc.tx.stmts[0])
	}
}

func TestSqlExecutor_WarehouseNotFound(t *testing.T) {
	dir := t.TempDir()
	logDir := t.TempDir()
	writeSQLFile(t, dir, "test.sql", "SELECT 1;")

	e := &SqlExecutor{
		ProjectDir: dir,
		Warehouses: map[string]*config.Warehouse{},
	}
	p := &config.Pipeline{SQL: "test.sql", Warehouse: "missing"}

	_, err := e.Execute(context.Background(), "test", p, logDir, 1)
	if err == nil {
		t.Fatal("expected error for missing warehouse")
	}
	if !strings.Contains(err.Error(), "missing") {
		t.Fatalf("expected error to mention warehouse name, got: %v", err)
	}
}

func TestSqlExecutor_MissingSQLFile(t *testing.T) {
	dir := t.TempDir()
	logDir := t.TempDir()

	mc := &mockConn{}
	e := &SqlExecutor{
		ProjectDir: dir,
		Warehouses: map[string]*config.Warehouse{"pg": {Driver: "postgres", DSN: "mock"}},
		Connect:    newMockConnect(mc),
	}
	p := &config.Pipeline{SQL: "nonexistent.sql", Warehouse: "pg"}

	_, err := e.Execute(context.Background(), "test", p, logDir, 1)
	if err == nil {
		t.Fatal("expected error for missing SQL file")
	}
	if !strings.Contains(err.Error(), "reading SQL file") {
		t.Fatalf("expected file read error, got: %v", err)
	}
}

func TestSqlExecutor_DefaultsToTransaction(t *testing.T) {
	dir := t.TempDir()
	logDir := t.TempDir()
	writeSQLFile(t, dir, "test.sql", "SELECT 1;")

	mc := &mockConn{tx: &mockTx{}}
	e := &SqlExecutor{
		ProjectDir: dir,
		Warehouses: map[string]*config.Warehouse{"pg": {Driver: "postgres", DSN: "mock"}},
		Connect:    newMockConnect(mc),
	}
	// Transaction field not set (nil) — should default to using a transaction
	p := &config.Pipeline{SQL: "test.sql", Warehouse: "pg"}

	_, err := e.Execute(context.Background(), "test", p, logDir, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mc.tx == nil || !mc.tx.committed {
		t.Fatal("expected transaction to be used and committed when Transaction is nil")
	}
	if len(mc.stmts) > 0 {
		t.Fatal("expected statements to go through tx, not direct conn")
	}
}

func TestSplitStatements(t *testing.T) {
	tests := []struct {
		input string
		want  []string
	}{
		{"SELECT 1; SELECT 2;", []string{"SELECT 1", "SELECT 2"}},
		{"  SELECT 1 ;  ", []string{"SELECT 1"}},
		{";;;", nil},
		{"CREATE TABLE t (id int);\nINSERT INTO t VALUES (1);", []string{"CREATE TABLE t (id int)", "INSERT INTO t VALUES (1)"}},
	}
	for _, tt := range tests {
		got := splitStatements(tt.input)
		if len(got) != len(tt.want) {
			t.Errorf("splitStatements(%q): got %d statements, want %d", tt.input, len(got), len(tt.want))
			continue
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("splitStatements(%q)[%d]: got %q, want %q", tt.input, i, got[i], tt.want[i])
			}
		}
	}
}
