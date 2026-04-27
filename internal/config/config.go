package config

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

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
	ShutdownGrace  string                `yaml:"shutdown_grace"`
	LogRetention   string                `yaml:"log_retention"`
	DefaultTimeout string                `yaml:"default_timeout"`
	DefaultRetry   *RetryConfig          `yaml:"default_retry"`
	Settings       map[string]string     `yaml:"settings"`
	Warehouses     map[string]*Warehouse `yaml:"warehouses"`
	Pipelines      map[string]*Pipeline  `yaml:"pipelines"`
	Groups         []PipelineGroup       `yaml:"-"`
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
	Config      string            `yaml:"config"`
	Metadata    *PipelineMetadata `yaml:"metadata"`
}

type PipelineMetadata struct {
	Name string   `yaml:"name"`
	Tags []string `yaml:"tags"`
}

type PipelineGroup struct {
	Key      string
	Metadata *PipelineMetadata
	Source   string
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
	Deps         []string `yaml:"deps"`
	Requirements string   `yaml:"requirements"`
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
	if err := expandGroups(&cfg, baseDir); err != nil {
		return nil, err
	}

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

// expandGroups resolves pipeline entries that reference sub-config files
// (config: field set, run/sql empty). Inner pipelines are auto-prefixed
// with the group key and merged into the flat Pipelines map.
func expandGroups(cfg *Config, baseDir string) error {
	groupKeys := make([]string, 0)
	for name, p := range cfg.Pipelines {
		if p.Config != "" {
			groupKeys = append(groupKeys, name)
		}
	}
	sort.Strings(groupKeys)

	for _, groupKey := range groupKeys {
		ref := cfg.Pipelines[groupKey]

		if ref.Run != "" || ref.SQL != "" {
			return fmt.Errorf("pipeline %q: config and run/sql are mutually exclusive", groupKey)
		}

		subPath := ref.Config
		if !filepath.IsAbs(subPath) {
			subPath = filepath.Join(baseDir, subPath)
		}

		data, err := os.ReadFile(subPath)
		if err != nil {
			return fmt.Errorf("pipeline %q: sub-config %q: %w", groupKey, ref.Config, err)
		}

		interpolated := interpolateEnv(string(data))

		var subCfg Config
		if err := yaml.Unmarshal([]byte(interpolated), &subCfg); err != nil {
			return fmt.Errorf("pipeline %q: parsing sub-config %q: %w", groupKey, ref.Config, err)
		}

		if len(subCfg.Pipelines) == 0 {
			return fmt.Errorf("pipeline %q: sub-config %q has no pipelines", groupKey, ref.Config)
		}

		subDir := filepath.Dir(subPath)

		innerNames := make([]string, 0, len(subCfg.Pipelines))
		for n := range subCfg.Pipelines {
			innerNames = append(innerNames, n)
		}
		sort.Strings(innerNames)

		for _, innerName := range innerNames {
			inner := subCfg.Pipelines[innerName]

			if inner.Config != "" {
				return fmt.Errorf("pipeline %q: sub-config %q: nested groups are not supported (pipeline %q has config field)", groupKey, ref.Config, innerName)
			}

			qualified := groupKey + "." + innerName

			if _, exists := cfg.Pipelines[qualified]; exists {
				return fmt.Errorf("pipeline %q: duplicate qualified name %q", groupKey, qualified)
			}

			rewritten := make([]string, len(inner.After))
			for i, dep := range inner.After {
				if strings.Contains(dep, ".") {
					rewritten[i] = dep
				} else {
					rewritten[i] = groupKey + "." + dep
				}
			}
			inner.After = rewritten

			if inner.SQL != "" && !filepath.IsAbs(inner.SQL) {
				inner.SQL = filepath.Join(subDir, inner.SQL)
			}
			if inner.Cwd == "" {
				inner.Cwd = subDir
			} else if !filepath.IsAbs(inner.Cwd) {
				inner.Cwd = filepath.Join(subDir, inner.Cwd)
			}
			if inner.Python != nil && inner.Python.Requirements != "" && !filepath.IsAbs(inner.Python.Requirements) {
				inner.Python.Requirements = filepath.Join(subDir, inner.Python.Requirements)
			}

			cfg.Pipelines[qualified] = inner
		}

		cfg.Groups = append(cfg.Groups, PipelineGroup{
			Key:      groupKey,
			Metadata: ref.Metadata,
			Source:   subPath,
		})

		delete(cfg.Pipelines, groupKey)
	}

	return nil
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

		if p.Image != "" && p.Python != nil {
			errs = append(errs, ValidationError{
				Line:    line,
				Message: fmt.Sprintf("pipeline %q: image and python are mutually exclusive", name),
			})
		}

		if p.Python != nil && p.Python.Requirements != "" {
			reqPath := p.Python.Requirements
			if !filepath.IsAbs(reqPath) {
				reqPath = filepath.Join(baseDir, reqPath)
			}
			if _, err := os.Stat(reqPath); os.IsNotExist(err) {
				errs = append(errs, ValidationError{
					Line:    line,
					Message: fmt.Sprintf("pipeline %q: python.requirements file %q not found", name, p.Python.Requirements),
				})
			}
		}
	}

	validOverlaps := map[string]bool{"skip": true, "queue": true, "kill_previous": true, "": true}
	if !validOverlaps[cfg.Overlap] {
		errs = append(errs, ValidationError{
			Message: fmt.Sprintf("overlap: unknown policy %q (valid: skip, queue, kill_previous)", cfg.Overlap),
		})
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

