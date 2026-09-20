//go:build load

package app

import (
	"context"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/bogie5464/netflow-collector/internal/obs"
	"github.com/bogie5464/netflow-collector/internal/sink/sinktest"
)

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

	addr := a.NetFlowAddr()
	conn, err := net.Dial("udp", addr.String())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	src, dst := netip.MustParseAddr("203.0.113.10"), netip.MustParseAddr("198.51.100.200")
	datagram := v5Datagram(src, dst, 51514, 443, 6, 12, 9000) // 1 v5 record: 24 + 48 = 72 bytes
	datagramSize := len(datagram)

	const sendWindow = 10 * time.Second
	ingestedBefore := testutil.ToFloat64(obs.RecordsIngested.WithLabelValues("netflow"))
	droppedBefore := testutil.ToFloat64(obs.RecordsDropped.WithLabelValues(obs.DropBufferFull))
	decodeErrBefore := testutil.ToFloat64(obs.DecodeErrors.WithLabelValues("netflow"))
	writeErrBefore := testutil.ToFloat64(obs.BatchWriteErrors.WithLabelValues("postgres"))

	var sent int64
	start := time.Now()
	deadline := start.Add(sendWindow)
	for time.Now().Before(deadline) {
		if _, err := conn.Write(datagram); err != nil {
			t.Fatalf("write failed after %d datagrams: %v", sent, err)
		}
		sent++
	}
	sendElapsed := time.Since(start)

	// Let the pipeline drain and the last batch flush.
	time.Sleep(2 * time.Second)

	ingestedAfter := testutil.ToFloat64(obs.RecordsIngested.WithLabelValues("netflow"))
	droppedAfter := testutil.ToFloat64(obs.RecordsDropped.WithLabelValues(obs.DropBufferFull))
	decodeErrAfter := testutil.ToFloat64(obs.DecodeErrors.WithLabelValues("netflow"))
	writeErrAfter := testutil.ToFloat64(obs.BatchWriteErrors.WithLabelValues("postgres"))

	ingested := ingestedAfter - ingestedBefore
	dropped := droppedAfter - droppedBefore
	decodeErrs := decodeErrAfter - decodeErrBefore
	writeErrs := writeErrAfter - writeErrBefore
	bytesSent := sent * int64(datagramSize)

	t.Logf("SEND:     %d datagrams (%d bytes) in %s => %.0f datagrams/s, %.0f bytes/s",
		sent, bytesSent, sendElapsed, float64(sent)/sendElapsed.Seconds(), float64(bytesSent)/sendElapsed.Seconds())
	t.Logf("INGESTED: %.0f records ingested, %.0f dropped (buffer full), %.0f decode errors, %.0f batch write errors, over %s send window => %.0f records/s ingested, %.0f bytes/s ingested",
		ingested, dropped, decodeErrs, writeErrs, sendWindow, ingested/sendWindow.Seconds(), ingested*float64(datagramSize)/sendWindow.Seconds())
	t.Logf("ACCOUNTING: sent=%d ingested=%.0f dropped=%.0f decode_errs=%.0f unaccounted=%.0f (kernel/socket loss plus any records in a batch that failed to write)",
		sent, ingested, dropped, decodeErrs, float64(sent)-ingested-dropped-decodeErrs)

	cancel()
	require.NoError(t, <-done)
}
