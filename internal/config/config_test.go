package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// baseline is a complete, valid environment so each subtest changes one thing.
func baseline() []string {
	return []string{
		"NFC_SOURCES=netflow",
		"NFC_SINKS=postgres",
		"NFC_POSTGRES_DSN=postgres://netflow:netflow@127.0.0.1:15432/netflow?sslmode=disable",
	}
}

func varErrors(t *testing.T, err error) map[string]string {
	t.Helper()
	require.Error(t, err)
	out := map[string]string{}
	var ve *VarError
	for _, e := range unwrapAll(err) {
		if errors.As(e, &ve) {
			out[ve.Var] = ve.Msg
		}
	}
	require.NotEmpty(t, out, "expected at least one VarError in %v", err)
	return out
}

// unwrapAll flattens an errors.Join tree into its leaves.
func unwrapAll(err error) []error {
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		var leaves []error
		for _, e := range joined.Unwrap() {
			leaves = append(leaves, unwrapAll(e)...)
		}
		return leaves
	}
	if inner := errors.Unwrap(err); inner != nil {
		return unwrapAll(inner)
	}
	return []error{err}
}

func TestParseDotEnv(t *testing.T) {
	t.Run("skips blanks and comments, strips export and quotes", func(t *testing.T) {
		in := "\n# comment\nNFC_LOG_LEVEL=debug\nexport NFC_WORKERS = 8\nNFC_KAFKA_TOPIC=\"flows\"\nNFC_KAFKA_GROUP='g'\nNFC_OTEL_ENDPOINT=\n"
		vars, err := parseDotEnv(strings.NewReader(in))
		require.NoError(t, err)
		require.Equal(t, map[string]string{
			"NFC_LOG_LEVEL":     "debug",
			"NFC_WORKERS":       "8",
			"NFC_KAFKA_TOPIC":   "flows",
			"NFC_KAFKA_GROUP":   "g",
			"NFC_OTEL_ENDPOINT": "",
		}, vars)
	})
	t.Run("rejects a line without an equals sign", func(t *testing.T) {
		_, err := parseDotEnv(strings.NewReader("NFC_LOG_LEVEL\n"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "line 1")
	})
}

func TestFindDotEnv(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, ".env"), []byte("NFC_LOG_LEVEL=warn\n"), 0o600))
	deep := filepath.Join(root, "internal", "sink", "postgres")
	require.NoError(t, os.MkdirAll(deep, 0o755))

	t.Run("walks up to the nearest .env", func(t *testing.T) {
		path, ok := findDotEnv(deep)
		require.True(t, ok)
		require.Equal(t, filepath.Join(root, ".env"), path)
	})
	t.Run("stops after ten levels", func(t *testing.T) {
		far := root
		for range maxWalkUp + 1 {
			far = filepath.Join(far, "d")
		}
		require.NoError(t, os.MkdirAll(far, 0o755))
		_, ok := findDotEnv(far)
		require.False(t, ok)
	})
}

func TestLoad(t *testing.T) {
	root := t.TempDir()
	dotenv := strings.Join(append(baseline(), "NFC_LOG_LEVEL=warn", "NFC_WORKERS=9"), "\n") + "\n"
	require.NoError(t, os.WriteFile(filepath.Join(root, ".env"), []byte(dotenv), 0o600))
	deep := filepath.Join(root, "internal", "config")
	require.NoError(t, os.MkdirAll(deep, 0o755))

	t.Run("loads .env found above the working directory", func(t *testing.T) {
		cfg, err := load(deep, nil)
		require.NoError(t, err)
		require.Equal(t, "warn", cfg.LogLevel)
		require.Equal(t, 9, cfg.Workers)
		require.True(t, cfg.PostgresEnabled)
		require.False(t, cfg.MariaDBEnabled)
	})
	t.Run("does not overwrite a variable exported in the shell", func(t *testing.T) {
		cfg, err := load(deep, []string{"NFC_LOG_LEVEL=debug"})
		require.NoError(t, err)
		require.Equal(t, "debug", cfg.LogLevel)
		require.Equal(t, 9, cfg.Workers, "keys the shell does not set still come from .env")
	})
	t.Run("works with no .env at all", func(t *testing.T) {
		cfg, err := load(t.TempDir(), baseline())
		require.NoError(t, err)
		require.Equal(t, "info", cfg.LogLevel)
	})
	t.Run("Load reads the process environment", func(t *testing.T) {
		t.Chdir(deep)
		t.Setenv("NFC_LOG_LEVEL", "error")
		cfg, err := Load()
		require.NoError(t, err)
		require.Equal(t, "error", cfg.LogLevel)
	})
}

func TestDefaults(t *testing.T) {
	cfg, err := load(t.TempDir(), baseline())
	require.NoError(t, err)
	require.Equal(t, "info", cfg.LogLevel)
	require.Equal(t, 30, cfg.RetentionDays)
	require.Equal(t, 65536, cfg.PipelineBuffer)
	require.Equal(t, 2000, cfg.BatchSize)
	require.Equal(t, time.Second, cfg.BatchInterval)
	require.Equal(t, 4, cfg.Workers)
	require.False(t, cfg.OTELEnabled)
	require.Empty(t, cfg.APIKeys)
}

func TestValidation(t *testing.T) {
	cases := []struct {
		name    string
		environ []string
		wantVar string
		wantMsg string
	}{
		{"rejects an unknown sink and names NFC_SINKS",
			append(baseline(), "NFC_SINKS=nope"), "NFC_SINKS", "must be one of postgres, mariadb"},
		{"rejects an unknown source",
			append(baseline(), "NFC_SOURCES=netflow,carrier-pigeon"), "NFC_SOURCES", "must be one of netflow, kafka"},
		{"requires the postgres DSN only when postgres is enabled",
			[]string{"NFC_SINKS=postgres"}, "NFC_POSTGRES_DSN", "must be set"},
		{"requires the mariadb DSN when mariadb is enabled",
			append(baseline(), "NFC_SINKS=postgres,mariadb"), "NFC_MARIADB_DSN", "must be set"},
		{"requires kafka brokers when kafka is enabled",
			append(baseline(), "NFC_SOURCES=kafka", "NFC_KAFKA_TOPIC=flows", "NFC_KAFKA_GROUP=g"), "NFC_KAFKA_BROKERS", "must be set"},
		{"requires the OTLP endpoint when tracing is on",
			append(baseline(), "NFC_OTEL_ENABLED=true"), "NFC_OTEL_ENDPOINT", "must be set"},
		{"rejects an unknown log level",
			append(baseline(), "NFC_LOG_LEVEL=loud"), "NFC_LOG_LEVEL", "must be one of"},
		{"rejects a zero worker count",
			append(baseline(), "NFC_WORKERS=0"), "NFC_WORKERS", "must be at least 1"},
		{"rejects a non-integer batch size",
			append(baseline(), "NFC_BATCH_SIZE=lots"), "NFC_BATCH_SIZE", "invalid"},
		{"rejects a malformed listen address",
			append(baseline(), "NFC_HTTP_ADDR=8080"), "NFC_HTTP_ADDR", "host:port"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := load(t.TempDir(), tc.environ)
			got := varErrors(t, err)
			require.Contains(t, got, tc.wantVar, "errors: %v", got)
			require.Contains(t, got[tc.wantVar], tc.wantMsg)
			require.Contains(t, err.Error(), tc.wantVar)
		})
	}

	t.Run("accepts a mariadb-only deployment without a postgres DSN", func(t *testing.T) {
		cfg, err := load(t.TempDir(), []string{"NFC_SINKS=mariadb", "NFC_MARIADB_DSN=netflow:netflow@tcp(127.0.0.1:13306)/netflow"})
		require.NoError(t, err)
		require.True(t, cfg.MariaDBEnabled)
		require.False(t, cfg.PostgresEnabled)
	})
	t.Run("accepts the committed .env.example", func(t *testing.T) {
		f, err := os.Open(filepath.Join("..", "..", ".env.example"))
		require.NoError(t, err)
		defer func() { require.NoError(t, f.Close()) }()
		vars, err := parseDotEnv(f)
		require.NoError(t, err)
		cfg, err := parse(vars)
		require.NoError(t, err)
		require.Equal(t, []string{"netflow"}, cfg.Sources)
		require.Equal(t, []string{"postgres"}, cfg.Sinks)
		require.Equal(t, []string{"dev-local-key"}, cfg.APIKeys)
	})
}
