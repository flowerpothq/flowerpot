package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testdataPath(name string) string {
	return filepath.Join("..", "..", "testdata", name)
}

func TestParseRealExample_SqlDuckdbBasic(t *testing.T) {
	path := testdataPath(filepath.Join("examples", "sql-duckdb-basic", "flowerpot.yaml"))
	result, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.HasErrors() {
		t.Fatalf("expected no validation errors, got: %v", result.Errors)
	}
	if len(result.Config.Pipelines) != 3 {
		t.Fatalf("expected 3 pipelines, got %d", len(result.Config.Pipelines))
	}
	w, ok := result.Config.Warehouses["analytics"]
	if !ok {
		t.Fatal("expected warehouse 'analytics'")
	}
	if w.Driver != "duckdb" {
		t.Fatalf("expected driver duckdb, got %s", w.Driver)
	}
}

func TestParseValid_FullSchema(t *testing.T) {
	result, err := Load(testdataPath("valid_full.yaml"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.HasErrors() {
		t.Fatalf("expected no validation errors, got: %v", result.Errors)
	}
	cfg := result.Config
	if cfg.Timezone != "America/New_York" {
		t.Fatalf("expected timezone America/New_York, got %s", cfg.Timezone)
	}
	if cfg.Overlap != "queue" {
		t.Fatalf("expected overlap queue, got %s", cfg.Overlap)
	}
	if !cfg.Catchup {
		t.Fatal("expected catchup true")
	}
	if cfg.MaxConcurrent != 8 {
		t.Fatalf("expected max_concurrent 8, got %d", cfg.MaxConcurrent)
	}
	if len(cfg.Warehouses) != 2 {
		t.Fatalf("expected 2 warehouses, got %d", len(cfg.Warehouses))
	}
	if len(cfg.Pipelines) != 4 {
		t.Fatalf("expected 4 pipelines, got %d", len(cfg.Pipelines))
	}
}

func TestParseValid_Minimal(t *testing.T) {
	result, err := Load(testdataPath("valid_minimal.yaml"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.HasErrors() {
		t.Fatalf("expected no validation errors, got: %v", result.Errors)
	}
	cfg := result.Config
	if cfg.Timezone != "UTC" {
		t.Fatalf("expected default timezone UTC, got %s", cfg.Timezone)
	}
	if cfg.Overlap != "skip" {
		t.Fatalf("expected default overlap skip, got %s", cfg.Overlap)
	}
	if cfg.Catchup {
		t.Fatal("expected default catchup false")
	}
	if cfg.MaxConcurrent != 4 {
		t.Fatalf("expected default max_concurrent 4, got %d", cfg.MaxConcurrent)
	}
}

func TestParseError_MutualExclusion(t *testing.T) {
	result, err := Load(testdataPath("error_mutual_exclusive.yaml"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.HasErrors() {
		t.Fatal("expected validation errors")
	}
	found := false
	for _, e := range result.Errors {
		if strings.Contains(e.Message, "mutually exclusive") {
			found = true
			if e.Line == 0 {
				t.Error("expected line number in error")
			}
		}
	}
	if !found {
		t.Fatalf("expected 'mutually exclusive' error, got: %v", result.Errors)
	}
}

func TestParseError_SqlWithoutWarehouse(t *testing.T) {
	result, err := Load(testdataPath("error_sql_no_warehouse.yaml"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.HasErrors() {
		t.Fatal("expected validation errors")
	}
	found := false
	for _, e := range result.Errors {
		if strings.Contains(e.Message, "requires warehouse") || strings.Contains(e.Message, "sql requires warehouse") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected 'warehouse required' error, got: %v", result.Errors)
	}
}

func TestParseError_WarehouseRefNotFound(t *testing.T) {
	result, err := Load(testdataPath("error_warehouse_ref.yaml"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.HasErrors() {
		t.Fatal("expected validation errors")
	}
	found := false
	for _, e := range result.Errors {
		if strings.Contains(e.Message, "nonexistent") && strings.Contains(e.Message, "not defined") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected warehouse not defined error, got: %v", result.Errors)
	}
}

func TestParseError_OrphanAfter(t *testing.T) {
	result, err := Load(testdataPath("error_orphan_after.yaml"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.HasErrors() {
		t.Fatal("expected validation errors")
	}
	found := false
	for _, e := range result.Errors {
		if strings.Contains(e.Message, "nonexistent-pipeline") && strings.Contains(e.Message, "not found") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected orphan after error, got: %v", result.Errors)
	}
}

func TestParseError_PerPipelineSchedule(t *testing.T) {
	result, err := Load(testdataPath("error_per_pipeline_schedule.yaml"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.HasErrors() {
		t.Fatal("expected validation errors")
	}
	found := false
	for _, e := range result.Errors {
		if strings.Contains(e.Message, "v0.2") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected v0.2 feature error, got: %v", result.Errors)
	}
}

func TestParseError_UnknownDriver(t *testing.T) {
	result, err := Load(testdataPath("error_unknown_driver.yaml"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.HasErrors() {
		t.Fatal("expected validation errors")
	}
	found := false
	for _, e := range result.Errors {
		if strings.Contains(e.Message, "unsupported driver") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected unsupported driver error, got: %v", result.Errors)
	}
}

func TestParseError_EmptyPipeline(t *testing.T) {
	result, err := Load(testdataPath("error_empty_pipeline.yaml"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.HasErrors() {
		t.Fatal("expected validation errors for pipeline with neither run nor sql")
	}
	found := false
	for _, e := range result.Errors {
		if strings.Contains(e.Message, "must have either run or sql") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected 'must have either run or sql' error, got: %v", result.Errors)
	}
}

func TestParse_EnvInterpolation(t *testing.T) {
	os.Setenv("TEST_FLOWERPOT_VAR", "hello-world")
	defer os.Unsetenv("TEST_FLOWERPOT_VAR")

	result, err := Load(testdataPath("valid_with_env.yaml"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.HasErrors() {
		t.Fatalf("expected no errors, got: %v", result.Errors)
	}
	p := result.Config.Pipelines["hello"]
	if p == nil {
		t.Fatal("expected pipeline 'hello'")
	}
	if !strings.Contains(p.Run, "hello-world") {
		t.Fatalf("expected interpolated value 'hello-world' in run command, got: %s", p.Run)
	}
}

func TestParse_LineNumbers(t *testing.T) {
	result, err := Load(testdataPath("error_mutual_exclusive.yaml"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hasLineNumber := false
	for _, e := range result.Errors {
		if e.Line > 0 {
			hasLineNumber = true
		}
	}
	if !hasLineNumber {
		t.Fatalf("expected at least one error with line number, got: %v", result.Errors)
	}
}

func TestParseError_SqlFileMissing(t *testing.T) {
	result, err := Load(testdataPath("error_sql_file_missing.yaml"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.HasErrors() {
		t.Fatal("expected validation errors")
	}
	found := false
	for _, e := range result.Errors {
		if strings.Contains(e.Message, "not found") && strings.Contains(e.Message, "does_not_exist.sql") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected sql file not found error, got: %v", result.Errors)
	}
}

func TestParse_SqlFileExists(t *testing.T) {
	path := testdataPath(filepath.Join("examples", "sql-duckdb-basic", "flowerpot.yaml"))
	result, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, e := range result.Errors {
		if strings.Contains(e.Message, "sql file") && strings.Contains(e.Message, "not found") {
			t.Fatalf("SQL files should exist, got error: %v", e)
		}
	}
}
