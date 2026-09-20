package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bogie5464/netflow-collector/internal/config"
	"github.com/bogie5464/netflow-collector/internal/flow"
	"github.com/bogie5464/netflow-collector/internal/obs"
)

const (
	goodKey  = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	otherKey = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
)

// captureLogs routes slog.Default through a buffer for the test's duration.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

func authHandler(t *testing.T) (http.Handler, *bytes.Buffer) {
	t.Helper()
	logs := captureLogs(t)
	h := New(&fakeQuerier{records: records(2)}, nil, config.Config{APIKeys: []string{otherKey, goodKey}})
	return h, logs
}

func do(h http.Handler, path, authorization string) (*httptest.ResponseRecorder, envelope) {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	var env envelope
	_ = json.Unmarshal(rr.Body.Bytes(), &env)
	return rr, env
}

const flowsPath = "/v1/flows?start=2026-09-19T00:00:00Z&end=2026-09-20T00:00:00Z"

func TestAuthRejectsWithoutKey(t *testing.T) {
	h, _ := authHandler(t)
	for name, header := range map[string]string{
		"no header":        "",
		"wrong scheme":     "Basic " + goodKey,
		"empty bearer":     "Bearer ",
		"unknown key":      "Bearer " + "0000000000000000000000000000000000000000000000000000000000000000",
		"prefix of a key":  "Bearer " + goodKey[:32],
		"key plus a byte":  "Bearer " + goodKey + "0",
		"key on exporters": "",
	} {
		t.Run(name, func(t *testing.T) {
			path := flowsPath
			if name == "key on exporters" {
				path = "/v1/exporters"
			}
			rr, env := do(h, path, header)
			require.Equal(t, http.StatusUnauthorized, rr.Code, rr.Body.String())
			require.NotNil(t, env.Error)
			require.Equal(t, CodeUnauthorized, env.Error.Code)
			require.Equal(t, rr.Header().Get("X-Request-Id"), env.Error.RequestID)
			require.Equal(t, `Bearer realm="netflow-collector"`, rr.Header().Get("WWW-Authenticate"))
			require.Nil(t, env.Data, "no flow data on a 401")
			require.Equal(t, "a valid API key is required", env.Error.Message, "the message never says which half failed")
		})
	}
}

func TestAuthAcceptsAnyConfiguredKey(t *testing.T) {
	h, logs := authHandler(t)
	for _, key := range []string{goodKey, otherKey} {
		rr, env := do(h, flowsPath, "Bearer "+key)
		require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
		require.Nil(t, env.Error)
	}
	rr, _ := do(h, flowsPath, "bearer "+goodKey)
	require.Equal(t, http.StatusOK, rr.Code, "the scheme is case-insensitive")
	require.NotContains(t, logs.String(), goodKey[:6], "a successful request logs no key material")
}

func TestAuth401LogsOnlyTheKeyPrefix(t *testing.T) {
	h, logs := authHandler(t)
	presented := "deadbeefcafe0123456789abcdef0123456789abcdef0123456789abcdef0123"
	rr, _ := do(h, flowsPath, "Bearer "+presented)
	require.Equal(t, http.StatusUnauthorized, rr.Code)
	out := logs.String()
	require.Contains(t, out, "level=WARN")
	require.Contains(t, out, "key_prefix=deadbe")
	require.NotContains(t, out, presented)
	require.NotContains(t, out, presented[6:])
}

func TestAuthLeavesOpsEndpointsPublic(t *testing.T) {
	h, _ := authHandler(t)
	for _, path := range []string{"/healthz", "/readyz", "/metrics"} {
		rr, env := do(h, path, "")
		require.NotEqual(t, http.StatusUnauthorized, rr.Code, path)
		if env.Error != nil {
			require.NotEqual(t, CodeUnauthorized, env.Error.Code, path)
		}
	}
	// Auth protects the whole subtree: an unknown /v1 path is 401 before 404.
	rr, _ := do(h, "/v1/nothing", "")
	require.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestKeyMatchesIsExhaustive(t *testing.T) {
	keys := [][]byte{[]byte(otherKey), []byte(goodKey)}
	require.True(t, keyMatches(keys, []byte(goodKey)))
	require.True(t, keyMatches(keys, []byte(otherKey)))
	require.False(t, keyMatches(keys, []byte(goodKey[:63])))
	require.False(t, keyMatches(keys, nil))
	require.False(t, keyMatches(nil, []byte(goodKey)), "no keys never matches")
}

func TestCheckConfig(t *testing.T) {
	require.NoError(t, CheckConfig(config.Config{}), "API disabled: no key needed")
	require.NoError(t, CheckConfig(config.Config{HTTPAddr: ":8080", APIKeys: []string{goodKey}}))
	err := CheckConfig(config.Config{HTTPAddr: ":8080"})
	require.ErrorContains(t, err, "NFC_API_KEYS")
	err = CheckConfig(config.Config{HTTPAddr: ":8080", APIKeys: []string{goodKey, " "}})
	require.ErrorContains(t, err, "NFC_API_KEYS")
}

// --- ops endpoints (registered outside /v1, always public) ---

type pingBackend struct {
	flow.Backend
	err   error
	delay time.Duration
}

func (b pingBackend) Query(ctx context.Context, _ flow.FlowQuery) (flow.FlowResultPage, error) {
	if b.delay > 0 {
		select {
		case <-time.After(b.delay):
		case <-ctx.Done():
			return flow.FlowResultPage{}, ctx.Err()
		}
	}
	return flow.FlowResultPage{}, b.err
}

func TestOpsEndpoints(t *testing.T) {
	obs.Prime([]string{"netflow"}, []string{"postgres"})
	ok := pingBackend{}
	down := pingBackend{err: errors.New("connection refused")}

	t.Run("healthz is 200 regardless of backends", func(t *testing.T) {
		h := New(&fakeQuerier{}, map[string]flow.Backend{"postgres": down, "mariadb": down}, config.Config{APIKeys: []string{goodKey}})
		rr, env := do(h, "/healthz", "")
		require.Equal(t, http.StatusOK, rr.Code)
		require.Nil(t, env.Error)
		require.NotEmpty(t, rr.Header().Get("X-Request-Id"))
	})

	t.Run("readyz is 200 when every backend answers", func(t *testing.T) {
		h := New(&fakeQuerier{}, map[string]flow.Backend{"postgres": ok, "mariadb": ok}, config.Config{APIKeys: []string{goodKey}})
		rr, env := do(h, "/readyz", "")
		require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
		require.Nil(t, env.Error)
		require.Contains(t, string(env.Data), `"postgres":"ok"`)
	})

	t.Run("readyz is 503 when any backend fails while healthz stays 200", func(t *testing.T) {
		h := New(&fakeQuerier{}, map[string]flow.Backend{"postgres": down, "mariadb": down}, config.Config{APIKeys: []string{goodKey}})
		rr, env := do(h, "/readyz", "")
		require.Equal(t, http.StatusServiceUnavailable, rr.Code)
		require.Equal(t, CodeBackendUnavailable, env.Error.Code)
		require.Equal(t, rr.Header().Get("X-Request-Id"), env.Error.RequestID)
		require.NotContains(t, rr.Body.String(), "connection refused", "backend detail never reaches the client")
		rr, _ = do(h, "/healthz", "")
		require.Equal(t, http.StatusOK, rr.Code)
	})

	t.Run("readyz gives every backend a 2 s budget", func(t *testing.T) {
		h := New(&fakeQuerier{}, map[string]flow.Backend{"postgres": pingBackend{delay: 10 * time.Second}}, config.Config{APIKeys: []string{goodKey}})
		started := time.Now()
		rr, _ := do(h, "/readyz", "")
		require.Equal(t, http.StatusServiceUnavailable, rr.Code)
		require.Less(t, time.Since(started), 4*time.Second)
	})

	t.Run("metrics is Prometheus text with the six instruments", func(t *testing.T) {
		h := New(&fakeQuerier{}, nil, config.Config{APIKeys: []string{goodKey}})
		rr, _ := do(h, "/metrics", "")
		require.Equal(t, http.StatusOK, rr.Code)
		require.Contains(t, rr.Header().Get("Content-Type"), "text/plain")
		for _, name := range []string{
			"netflow_records_ingested_total", "netflow_records_dropped_total", "netflow_batch_write_duration_seconds",
			"netflow_batch_write_errors_total", "netflow_pipeline_buffer_length", "netflow_record_visibility_lag_seconds",
		} {
			require.Contains(t, rr.Body.String(), name)
		}
	})
}
