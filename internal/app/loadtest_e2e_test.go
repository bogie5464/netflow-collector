//go:build load

package app

import (
	"context"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/bogie5464/netflow-collector/internal/flow"
	"github.com/bogie5464/netflow-collector/internal/obs"
	"github.com/bogie5464/netflow-collector/internal/pipeline"
	"github.com/bogie5464/netflow-collector/internal/sink/sinktest"
	"github.com/bogie5464/netflow-collector/internal/source/netflow"
)

const sendWindow = 10 * time.Second

// TestEndToEndUDPThroughput measures real throughput through the whole
// stack: a real UDP socket, real goflow2 decode, the real pipeline, and a
// real TimescaleDB backend — not the in-memory-only numbers in
// docs/performance.md. It blasts v5 datagrams for a fixed window and reads
// the same Prometheus counters an operator would.
func TestEndToEndUDPThroughput(t *testing.T) {
	dsn := sinktest.StartTimescale(t)
	cfg := testConfig(dsn)
	cfg.PipelineBuffer = 65536
	cfg.BatchSize = 2000
	cfg.BatchInterval = time.Second
	cfg.Workers = 4

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a, err := New(ctx, cfg)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	select {
	case <-a.Ready():
	case err := <-done:
		t.Fatalf("app stopped before listening: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("app did not start")
	}

	writeErrBefore := testutil.ToFloat64(obs.BatchWriteErrors.WithLabelValues("postgres"))
	writeSumBefore, writeCountBefore := histogram(t, obs.BatchWriteDuration.WithLabelValues("postgres"))

	c := blast(t, a.NetFlowAddr())

	writeErrs := testutil.ToFloat64(obs.BatchWriteErrors.WithLabelValues("postgres")) - writeErrBefore
	writeSumAfter, writeCountAfter := histogram(t, obs.BatchWriteDuration.WithLabelValues("postgres"))
	writeCount := writeCountAfter - writeCountBefore
	writeSum := writeSumAfter - writeSumBefore
	if writeCount > 0 {
		t.Logf("WRITES:   %d WriteBatch calls, %.3fs total => %.1f ms mean per batch, %.0f records/s per worker, %.0f batch write errors",
			writeCount, writeSum, 1000*writeSum/float64(writeCount), float64(cfg.BatchSize)*float64(writeCount)/writeSum, writeErrs)
	}
	c.report(t, "durably ingested (Postgres)")

	cancel()
	require.NoError(t, <-done)
}

// TestEndToEndUDPThroughputStubSink is the same wire path — real UDP
// socket, real decode, real pipeline — into a sink that only counts. It
// measures the collector's own ceiling on this machine, independent of any
// database, so docs/performance.md can separate "what the collector does"
// from "what your database does".
func TestEndToEndUDPThroughputStubSink(t *testing.T) {
	cfg := testConfig("")
	cfg.PipelineBuffer = 65536
	cfg.BatchSize = 2000
	cfg.BatchInterval = time.Second
	cfg.Workers = 4

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src, err := netflow.New(cfg)
	require.NoError(t, err)
	var sink countingSink
	pipe, err := pipeline.New(pipeline.Options{
		BufferSize:    cfg.PipelineBuffer,
		BatchSize:     cfg.BatchSize,
		BatchInterval: cfg.BatchInterval,
		Workers:       cfg.Workers,
	}, pipeline.Sink{Name: "stub", Sink: &sink})
	require.NoError(t, err)

	pipeCtx, stopPipe := context.WithCancel(context.Background())
	pipeDone := make(chan error, 1)
	go func() { pipeDone <- pipe.Run(pipeCtx) }()
	srcDone := make(chan error, 1)
	go func() { srcDone <- pipe.RunSource(ctx, "netflow", src) }()
	bound := src.(interface {
		Ready() <-chan struct{}
		LocalAddr() netip.AddrPort
	})
	select {
	case <-bound.Ready():
	case err := <-srcDone:
		t.Fatalf("source stopped before listening: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("source did not start")
	}

	c := blast(t, bound.LocalAddr())
	t.Logf("SINK:     %d records reached the stub sink", sink.n.Load())
	c.report(t, "ingested (stub sink)")

	cancel()
	require.NoError(t, <-srcDone)
	stopPipe()
	require.NoError(t, <-pipeDone)
}

type countingSink struct{ n atomic.Int64 }

func (s *countingSink) WriteBatch(_ context.Context, records []flow.FlowRecord) error {
	s.n.Add(int64(len(records)))
	return nil
}

type counts struct {
	sent, datagramSize            int64
	sendElapsed                   time.Duration
	ingested, dropped, decodeErrs float64
}

// blast sends one-record v5 datagrams back to back for sendWindow, waits for
// the pipeline to drain, and returns the deltas of the ingest counters.
func blast(t *testing.T, addr netip.AddrPort) counts {
	t.Helper()
	conn, err := net.Dial("udp", addr.String())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	src, dst := netip.MustParseAddr("203.0.113.10"), netip.MustParseAddr("198.51.100.200")
	datagram := v5Datagram(src, dst, 51514, 443, 6, 12, 9000) // 1 v5 record: 24 + 48 = 72 bytes

	ingestedBefore := testutil.ToFloat64(obs.RecordsIngested.WithLabelValues("netflow"))
	droppedBefore := testutil.ToFloat64(obs.RecordsDropped.WithLabelValues(obs.DropBufferFull))
	decodeErrBefore := testutil.ToFloat64(obs.DecodeErrors.WithLabelValues("netflow"))

	var c counts
	c.datagramSize = int64(len(datagram))
	start := time.Now()
	deadline := start.Add(sendWindow)
	for time.Now().Before(deadline) {
		if _, err := conn.Write(datagram); err != nil {
			t.Fatalf("write failed after %d datagrams: %v", c.sent, err)
		}
		c.sent++
	}
	c.sendElapsed = time.Since(start)

	// Let the pipeline drain and the last batch flush.
	time.Sleep(2 * time.Second)

	c.ingested = testutil.ToFloat64(obs.RecordsIngested.WithLabelValues("netflow")) - ingestedBefore
	c.dropped = testutil.ToFloat64(obs.RecordsDropped.WithLabelValues(obs.DropBufferFull)) - droppedBefore
	c.decodeErrs = testutil.ToFloat64(obs.DecodeErrors.WithLabelValues("netflow")) - decodeErrBefore
	return c
}

func (c counts) report(t *testing.T, label string) {
	t.Helper()
	bytesSent := c.sent * c.datagramSize
	t.Logf("SEND:     %d datagrams (%d bytes) in %s => %.0f datagrams/s, %.0f bytes/s",
		c.sent, bytesSent, c.sendElapsed, float64(c.sent)/c.sendElapsed.Seconds(), float64(bytesSent)/c.sendElapsed.Seconds())
	t.Logf("INGESTED: %.0f records %s, %.0f dropped (buffer full), %.0f decode errors, over %s => %.0f records/s, %.0f bytes/s",
		c.ingested, label, c.dropped, c.decodeErrs, sendWindow, c.ingested/sendWindow.Seconds(), c.ingested*float64(c.datagramSize)/sendWindow.Seconds())
	t.Logf("ACCOUNTING: sent=%d ingested=%.0f dropped=%.0f decode_errs=%.0f unaccounted=%.0f (kernel/socket loss plus any records in a batch that failed to write)",
		c.sent, c.ingested, c.dropped, c.decodeErrs, float64(c.sent)-c.ingested-c.dropped-c.decodeErrs)
}

func histogram(t *testing.T, o prometheus.Observer) (sum float64, count uint64) {
	t.Helper()
	var m dto.Metric
	require.NoError(t, o.(prometheus.Metric).Write(&m))
	return m.GetHistogram().GetSampleSum(), m.GetHistogram().GetSampleCount()
}
