package config

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"

	"gopkg.in/yaml.v3"
)

var supportedDrivers = map[string]bool{
	"postgres": true,
	"duckdb":   true,
}

var envVarPattern = regexp.MustCompile(`\$\{([^}]+)\}`)

// InterpolateEnv replaces ${VAR} references with environment variable values.
// Unset variables are left as-is.
func InterpolateEnv(content string) string {
	return interpolateEnv(content)
}

// Config is the top-level flowerpot.yaml schema.
type Config struct {
	Schedule       string                `yaml:"schedule"`
	Timezone       string                `yaml:"timezone"`
	Overlap        string                `yaml:"overlap"`
	Catchup        bool                  `yaml:"catchup"`
	MaxConcurrent  int                   `yaml:"max_concurrent"`
	DefaultTimeout string                `yaml:"default_timeout"`
	DefaultRetry   *RetryConfig          `yaml:"default_retry"`
	Settings       map[string]string     `yaml:"settings"`
	Warehouses     map[string]*Warehouse `yaml:"warehouses"`
	Pipelines      map[string]*Pipeline  `yaml:"pipelines"`
}

type Warehouse struct {
	Driver string `yaml:"driver"`
	DSN    string `yaml:"dsn"`
}

type Pipeline struct {
	Run         string            `yaml:"run"`
	SQL         string            `yaml:"sql"`
	After       []string          `yaml:"after"`
	Retry       *RetryConfig      `yaml:"retry"`
	Timeout     string            `yaml:"timeout"`
	Env         map[string]string `yaml:"env"`
	Cwd         string            `yaml:"cwd"`
	Python      *PythonConfig     `yaml:"python"`
	Image       string            `yaml:"image"`
	Warehouse   string            `yaml:"warehouse"`
	Transaction *bool             `yaml:"transaction"`
	Schedule    string            `yaml:"schedule"`
}

// UseTransaction returns whether this pipeline should execute SQL in a transaction.
// Defaults to true if not explicitly set.
func (p *Pipeline) UseTransaction() bool {
	if p.Transaction == nil {
		return true
	}
	return *p.Transaction
}

type RetryConfig struct {
	Attempts int    `yaml:"attempts"`
	Delay    string `yaml:"delay"`
	Strategy string `yaml:"strategy"` // "fixed" or "exponential"; defaults to "fixed"
}

type PythonConfig struct {
	Deps []string `yaml:"deps"`
}

// ValidationError holds a validation failure with an optional line number.
type ValidationError struct {
	Line    int
	Message string
}

func (e *ValidationError) Error() string {
	if e.Line > 0 {
		return fmt.Sprintf("line %d: %s", e.Line, e.Message)
	}
	return e.Message
}

// ParseResult holds the parsed config and any validation errors.
type ParseResult struct {
	Config *Config
	Errors []ValidationError
}

func (r *ParseResult) HasErrors() bool {
	return len(r.Errors) > 0
}

// Load reads a flowerpot.yaml file, interpolates env vars, parses it, and validates.
func Load(path string) (*ParseResult, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}

	interpolated := interpolateEnv(string(data))

	var rootNode yaml.Node
	if err := yaml.Unmarshal([]byte(interpolated), &rootNode); err != nil {
		return nil, fmt.Errorf("parsing YAML: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal([]byte(interpolated), &cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	applyDefaults(&cfg)

	baseDir := filepath.Dir(path)
	errors := validate(&cfg, &rootNode, baseDir)

	return &ParseResult{Config: &cfg, Errors: errors}, nil
}

func interpolateEnv(content string) string {
	return envVarPattern.ReplaceAllStringFunc(content, func(match string) string {
		varName := match[2 : len(match)-1]
		if val, ok := os.LookupEnv(varName); ok {
			return val
		}
		return match
	})
}

func applyDefaults(cfg *Config) {
	if cfg.Timezone == "" {
		cfg.Timezone = "UTC"
	}
	if cfg.Overlap == "" {
		cfg.Overlap = "skip"
	}
	if cfg.MaxConcurrent == 0 {
		cfg.MaxConcurrent = 4
	}
}

func validate(cfg *Config, rootNode *yaml.Node, baseDir string) []ValidationError {
	var errs []ValidationError

	pipelineLines := buildPipelineNodeMap(rootNode)

	pipelineNames := make([]string, 0, len(cfg.Pipelines))
	for name := range cfg.Pipelines {
		pipelineNames = append(pipelineNames, name)
	}
	sort.Strings(pipelineNames)

	for _, name := range pipelineNames {
		p := cfg.Pipelines[name]
		line := pipelineLines[name]

		if p.Schedule != "" {
			errs = append(errs, ValidationError{
				Line:    line,
				Message: fmt.Sprintf("pipeline %q: per-pipeline schedules are a v0.2 feature", name),
			})
		}

		if p.Run == "" && p.SQL == "" {
			errs = append(errs, ValidationError{
				Line:    line,
				Message: fmt.Sprintf("pipeline %q: must have either run or sql", name),
			})
		}

		if p.Run != "" && p.SQL != "" {
			errs = append(errs, ValidationError{
				Line:    line,
				Message: fmt.Sprintf("pipeline %q: run and sql are mutually exclusive", name),
			})
		}

		if p.SQL != "" && p.Warehouse == "" {
			errs = append(errs, ValidationError{
				Line:    line,
				Message: fmt.Sprintf("pipeline %q: sql requires warehouse", name),
			})
		}

		if p.Warehouse != "" {
			if cfg.Warehouses == nil {
				errs = append(errs, ValidationError{
					Line:    line,
					Message: fmt.Sprintf("pipeline %q: warehouse %q is not defined in warehouses", name, p.Warehouse),
				})
			} else if _, ok := cfg.Warehouses[p.Warehouse]; !ok {
				errs = append(errs, ValidationError{
					Line:    line,
					Message: fmt.Sprintf("pipeline %q: warehouse %q is not defined in warehouses", name, p.Warehouse),
				})
			}
		}

		for _, dep := range p.After {
			if _, ok := cfg.Pipelines[dep]; !ok {
				errs = append(errs, ValidationError{
					Line:    line,
					Message: fmt.Sprintf("pipeline %q: after references %q which is not found", name, dep),
				})
			}
		}

		if p.SQL != "" && p.Warehouse != "" {
			sqlPath := p.SQL
			if !filepath.IsAbs(sqlPath) {
				sqlPath = filepath.Join(baseDir, sqlPath)
			}
			if _, err := os.Stat(sqlPath); os.IsNotExist(err) {
				errs = append(errs, ValidationError{
					Line:    line,
					Message: fmt.Sprintf("pipeline %q: sql file %q not found", name, p.SQL),
				})
			}
		}
	}

	warehouseNames := make([]string, 0, len(cfg.Warehouses))
	for name := range cfg.Warehouses {
		warehouseNames = append(warehouseNames, name)
	}
	sort.Strings(warehouseNames)

	for _, name := range warehouseNames {
		w := cfg.Warehouses[name]
		if !supportedDrivers[w.Driver] {
			errs = append(errs, ValidationError{
				Message: fmt.Sprintf("warehouse %q: unsupported driver %q", name, w.Driver),
			})
		}
	}

	return errs
}

// buildPipelineNodeMap walks the raw YAML node tree to find line numbers for each pipeline key.
func buildPipelineNodeMap(root *yaml.Node) map[string]int {
	result := make(map[string]int)
	if root == nil || root.Kind != yaml.DocumentNode || len(root.Content) == 0 {
		return result
	}

	mapping := root.Content[0]
	if mapping.Kind != yaml.MappingNode {
		return result
	}

	for i := 0; i < len(mapping.Content)-1; i += 2 {
		key := mapping.Content[i]
		val := mapping.Content[i+1]
		if key.Value == "pipelines" && val.Kind == yaml.MappingNode {
			for j := 0; j < len(val.Content)-1; j += 2 {
				pipelineKey := val.Content[j]
				result[pipelineKey.Value] = pipelineKey.Line
			}
		}
	}

	return result
}

// DAGGraph builds a dag.Graph-compatible adjacency map from the config.
func (c *Config) DAGGraph() map[string][]string {
	g := make(map[string][]string, len(c.Pipelines))
	for name, p := range c.Pipelines {
		g[name] = p.After
	}
	return g
}

// SQLFileCount returns the total number of SQL files referenced and how many exist on disk.
func (c *Config) SQLFileCount(baseDir string) (total, found int) {
	for _, p := range c.Pipelines {
		if p.SQL == "" {
			continue
		}
		total++
		sqlPath := p.SQL
		if !filepath.IsAbs(sqlPath) {
			sqlPath = filepath.Join(baseDir, sqlPath)
		}
		if _, err := os.Stat(sqlPath); err == nil {
			found++
		}
	}
	return
}

// HasSQLPipelines returns true if any pipeline uses sql:.
func (c *Config) HasSQLPipelines() bool {
	for _, p := range c.Pipelines {
		if p.SQL != "" {
			return true
		}
	}
	return false
}

