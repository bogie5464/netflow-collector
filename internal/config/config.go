// Package config is the only place the collector reads its environment. Load
// finds the nearest .env by walking up from the working directory, overlays the
// process environment on top of it (an exported variable always wins), binds
// the result into Config and validates it once. Nothing else in the module
// calls os.Getenv.
package config

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/go-playground/validator/v10"
)

// Known values for NFC_SOURCES and NFC_SINKS.
const (
	SourceNetFlow = "netflow"
	SourceKafka   = "kafka"
	SinkPostgres  = "postgres"
	SinkMariaDB   = "mariadb"
)

// maxWalkUp bounds the search for .env so a misplaced working directory cannot
// pick up a file from an unrelated tree.
const maxWalkUp = 10

// Config holds every NFC_* variable. All of them are declared here, once; a
// later build step never adds a required variable. A variable is required only
// from the step that consumes it, and the conditional ones only when the
// feature that needs them is enabled.
type Config struct {
	LogLevel    string `env:"NFC_LOG_LEVEL" envDefault:"info" validate:"oneof=debug info warn error"`
	HTTPAddr    string `env:"NFC_HTTP_ADDR" validate:"omitempty,hostname_port"`
	NetFlowAddr string `env:"NFC_NETFLOW_ADDR" validate:"omitempty,hostname_port"`

	Sources []string `env:"NFC_SOURCES" validate:"dive,oneof=netflow kafka"`
	Sinks   []string `env:"NFC_SINKS" validate:"dive,oneof=postgres mariadb"`

	PostgresDSN   string `env:"NFC_POSTGRES_DSN" validate:"required_if=PostgresEnabled true"`
	MariaDBDSN    string `env:"NFC_MARIADB_DSN" validate:"required_if=MariaDBEnabled true"`
	RetentionDays int    `env:"NFC_RETENTION_DAYS" envDefault:"30" validate:"min=1"`

	KafkaBrokers []string `env:"NFC_KAFKA_BROKERS" validate:"required_if=KafkaEnabled true,dive,hostname_port"`
	KafkaTopic   string   `env:"NFC_KAFKA_TOPIC" validate:"required_if=KafkaEnabled true"`
	KafkaGroup   string   `env:"NFC_KAFKA_GROUP" validate:"required_if=KafkaEnabled true"`

	PipelineBuffer int           `env:"NFC_PIPELINE_BUFFER" envDefault:"65536" validate:"min=1"`
	BatchSize      int           `env:"NFC_BATCH_SIZE" envDefault:"2000" validate:"min=1"`
	BatchInterval  time.Duration `env:"NFC_BATCH_INTERVAL" envDefault:"1s" validate:"gt=0"`
	Workers        int           `env:"NFC_WORKERS" envDefault:"4" validate:"min=1"`

	// APIKeys are the bearer keys accepted on /v1. Never log one in full.
	APIKeys []string `env:"NFC_API_KEYS"`

	// Derived from Sources and Sinks before validation so the conditional
	// rules above can be plain required_if tags. Not read from the environment.
	PostgresEnabled bool `env:"-"`
	MariaDBEnabled  bool `env:"-"`
	KafkaEnabled    bool `env:"-"`
	NetFlowEnabled  bool `env:"-"`
}

// VarError reports one invalid variable. Var is the NFC_* name so the message
// tells the operator what to fix.
type VarError struct {
	Var string
	Msg string
}

func (e *VarError) Error() string { return e.Var + ": " + e.Msg }

// Load finds the nearest .env, overlays the process environment on it, and
// returns the validated configuration. A validation failure returns an error
// that wraps one *VarError per offending variable.
func Load() (*Config, error) {
	wd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("config: working directory: %w", err)
	}
	return load(wd, os.Environ())
}

// load is Load with its inputs injected so tests stay hermetic.
func load(wd string, environ []string) (*Config, error) {
	vars := map[string]string{}
	if path, ok := findDotEnv(wd); ok {
		f, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("config: open %s: %w", path, err)
		}
		vars, err = parseDotEnv(f)
		_ = f.Close()
		if err != nil {
			return nil, fmt.Errorf("config: parse %s: %w", path, err)
		}
	}
	// The shell always wins over the file.
	for _, kv := range environ {
		if k, v, ok := strings.Cut(kv, "="); ok {
			vars[k] = v
		}
	}
	return parse(vars)
}

// parse binds and validates one flat set of variables.
func parse(vars map[string]string) (*Config, error) {
	var cfg Config
	if err := env.ParseWithOptions(&cfg, env.Options{Environment: vars}); err != nil {
		return nil, bindError(err)
	}
	cfg.PostgresEnabled = slices.Contains(cfg.Sinks, SinkPostgres)
	cfg.MariaDBEnabled = slices.Contains(cfg.Sinks, SinkMariaDB)
	cfg.KafkaEnabled = slices.Contains(cfg.Sources, SourceKafka)
	cfg.NetFlowEnabled = slices.Contains(cfg.Sources, SourceNetFlow)

	if err := newValidator().Struct(&cfg); err != nil {
		return nil, validationError(err)
	}
	return &cfg, nil
}

// findDotEnv walks up from dir, at most maxWalkUp levels, and returns the first
// .env it finds. The upward walk is what lets `go test` inside a package
// directory find the repository's file.
func findDotEnv(dir string) (string, bool) {
	dir = filepath.Clean(dir)
	for range maxWalkUp + 1 {
		candidate := filepath.Join(dir, ".env")
		if st, err := os.Stat(candidate); err == nil && !st.IsDir() {
			return candidate, true
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", false
}

// parseDotEnv reads KEY=value lines. Blank lines and # comments are skipped, an
// optional `export ` prefix is accepted, and a value wrapped in matching single
// or double quotes has the quotes removed. Nothing is expanded.
func parseDotEnv(r io.Reader) (map[string]string, error) {
	vars := map[string]string{}
	sc := bufio.NewScanner(r)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		key, val, ok := strings.Cut(line, "=")
		key = strings.TrimSpace(key)
		if !ok || key == "" {
			return nil, fmt.Errorf("line %d: expected KEY=value", n)
		}
		val = strings.TrimSpace(val)
		if len(val) >= 2 {
			if q := val[0]; (q == '"' || q == '\'') && val[len(val)-1] == q {
				val = val[1 : len(val)-1]
			}
		}
		vars[key] = val
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return vars, nil
}

// newValidator reports field names as their NFC_* variable, so a failure names
// what the operator has to change rather than a Go identifier.
func newValidator() *validator.Validate {
	v := validator.New(validator.WithRequiredStructEnabled())
	v.RegisterTagNameFunc(func(f reflect.StructField) string {
		name := f.Tag.Get("env")
		if name == "" || name == "-" {
			return f.Name
		}
		return name
	})
	return v
}

// bindError converts an env binding failure (a non-integer NFC_WORKERS, say)
// into one VarError per variable.
func bindError(err error) error {
	var agg env.AggregateError
	if !errors.As(err, &agg) {
		return fmt.Errorf("config: %w", err)
	}
	errs := make([]error, 0, len(agg.Errors))
	for _, e := range agg.Errors {
		var pe env.ParseError
		if errors.As(e, &pe) {
			errs = append(errs, &VarError{Var: envName(pe.Name), Msg: "invalid " + pe.Type.String() + ": " + pe.Err.Error()})
			continue
		}
		errs = append(errs, &VarError{Var: "?", Msg: e.Error()})
	}
	return fmt.Errorf("config: %w", errors.Join(errs...))
}

// envName maps a Config field name back to its NFC_* variable.
func envName(field string) string {
	if f, ok := reflect.TypeFor[Config]().FieldByName(field); ok {
		if tag := f.Tag.Get("env"); tag != "" && tag != "-" {
			return tag
		}
	}
	return field
}

// validationError converts validator failures into one VarError per variable.
func validationError(err error) error {
	var ves validator.ValidationErrors
	if !errors.As(err, &ves) {
		return fmt.Errorf("config: %w", err)
	}
	errs := make([]error, 0, len(ves))
	for _, fe := range ves {
		name := fe.Field()
		if i := strings.IndexByte(name, '['); i > 0 {
			name = name[:i] // NFC_SINKS[1] -> NFC_SINKS
		}
		errs = append(errs, &VarError{Var: name, Msg: describe(fe)})
	}
	return fmt.Errorf("config: %w", errors.Join(errs...))
}

func describe(fe validator.FieldError) string {
	switch fe.Tag() {
	case "required", "required_if":
		return "must be set"
	case "oneof":
		return fmt.Sprintf("must be one of %s (got %q)", strings.Join(strings.Fields(fe.Param()), ", "), fmt.Sprint(fe.Value()))
	case "min":
		return "must be at least " + fe.Param()
	case "gt":
		return "must be greater than " + fe.Param()
	case "hostname_port":
		return "must be host:port"
	default:
		return "failed " + fe.Tag()
	}
}
