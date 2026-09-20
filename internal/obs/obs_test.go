package obs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/bogie5464/netflow-collector/internal/flow"
	"github.com/bogie5464/netflow-collector/internal/obs"
	"github.com/bogie5464/netflow-collector/internal/pipeline"
)

func TestLoggingIsSingleLineJSONWithRedaction(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	log := obs.SetupLogging(&buf, "debug", obs.ServiceName)

	log.Info("boot", "NFC_API_KEYS", "dev-local-key", "authorization", "Bearer abc", "nfc_postgres_dsn", "postgres://u:pw@h/db",
		"password", "hunter2", "token", "t", "key_prefix", "deadbe", "component", "app")
	log.Debug("nested", slog.Group("req", slog.String("Authorization", "Bearer xyz"), slog.String("path", "/v1/flows")))

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	require.Len(t, lines, 2, "one line per record, no pretty printing")
	var first map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &first))
	for _, k := range []string{"time", "level", "msg", "service"} {
		require.Contains(t, first, k)
	}
	require.Equal(t, "netflow-collector", first["service"])
	require.Equal(t, "INFO", first["level"])
	for _, k := range []string{"NFC_API_KEYS", "authorization", "nfc_postgres_dsn", "password", "token"} {
		require.Equal(t, obs.Redacted, first[k], k)
	}
	require.Equal(t, "deadbe", first["key_prefix"], "non-secret attributes pass through")
	require.Equal(t, "app", first["component"])
	require.NotContains(t, buf.String(), "dev-local-key")
	require.NotContains(t, buf.String(), "hunter2")
	require.NotContains(t, buf.String(), "pw@h")

	var second map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &second))
	req := second["req"].(map[string]any)
	require.Equal(t, obs.Redacted, req["Authorization"], "redaction reaches into groups")
	require.Equal(t, "/v1/flows", req["path"])
	require.NotContains(t, buf.String(), "xyz")
}

func TestLogLevelIsHonoured(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	log := obs.SetupLogging(&buf, "warn", obs.ServiceName)
	log.Info("hidden")
	log.Warn("shown")
	require.NotContains(t, buf.String(), "hidden")
	require.Contains(t, buf.String(), "shown")
	require.Equal(t, slog.LevelInfo, obs.ParseLevel("nonsense"), "unknown levels mean info")
}

func TestMetricsAreServed(t *testing.T) {
	stop, err := obs.SetupMetrics()
	require.NoError(t, err)
	t.Cleanup(func() { _ = stop(context.Background()) })

	obs.Prime([]string{"netflow"}, []string{"postgres"})
	rr := httptest.NewRecorder()
	promhttp.Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	for _, name := range []string{
		"netflow_records_ingested_total", "netflow_records_dropped_total", "netflow_batch_write_duration_seconds",
		"netflow_batch_write_errors_total", "netflow_pipeline_buffer_length", "netflow_record_visibility_lag_seconds",
	} {
		require.Contains(t, body, name)
	}
	require.Contains(t, body, `netflow_records_ingested_total{source="netflow"} 0`, "primed series are present before any record")
	require.Contains(t, body, "target_info", "the OTel meter provider is bridged onto the same registry")

	// Calling SetupMetrics again in the same process must not fail on the
	// already-registered collector.
	stop2, err := obs.SetupMetrics()
	require.NoError(t, err)
	_ = stop2(context.Background())
}

func TestRedactedKeyMatching(t *testing.T) {
	for _, k := range []string{"authorization", "Authorization", "NFC_API_KEYS", "api_key", "NFC_MARIADB_DSN", "dsn", "db.password", "refresh_token", "client_secret"} {
		require.True(t, obs.IsRedactedKey(k), k)
	}
	for _, k := range []string{"key_prefix", "request_id", "path", "tokens_per_second", "dsn_host"} {
		require.False(t, obs.IsRedactedKey(k), k)
	}
}

type instantSink struct{}

func (instantSink) WriteBatch(context.Context, []flow.FlowRecord) error { return nil }

func histogram(t *testing.T, h prometheus.Histogram) (uint64, float64) {
	t.Helper()
	var m dto.Metric
	require.NoError(t, h.Write(&m))
	return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
}

func TestVisibilityLagIsObservedAtWriteCompletion(t *testing.T) {
	p, err := pipeline.New(pipeline.Options{BufferSize: 64, BatchSize: 4, BatchInterval: time.Hour, Workers: 1}, pipeline.Sink{Name: "instant", Sink: instantSink{}})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	countBefore, sumBefore := histogram(t, obs.RecordVisibilityLag)
	receivedAt := time.Now().Add(-3 * time.Second)
	for i := range 4 {
		require.True(t, p.Offer("t", flow.FlowRecord{ReceivedAt: receivedAt, SrcPort: uint16(i)}))
	}
	require.Eventually(t, func() bool {
		c, _ := histogram(t, obs.RecordVisibilityLag)
		return c == countBefore+4
	}, 5*time.Second, 10*time.Millisecond, "one observation per record at write completion")
	_, sum := histogram(t, obs.RecordVisibilityLag)
	perRecord := (sum - sumBefore) / 4
	require.InDelta(t, 3.0, perRecord, 1.0, "lag ≈ write completion − received_at")

	cancel()
	require.NoError(t, <-done)
}
