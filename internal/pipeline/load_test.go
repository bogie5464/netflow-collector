//go:build load

// Load verification. Runs without -race on purpose: the detector costs
// roughly an order of magnitude and would fail the throughput floor on
// correct code.
//
//	go test -tags=load -run TestPipelineLoad ./internal/pipeline/...
package pipeline

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/bogie5464/netflow-collector/internal/flow"
	"github.com/bogie5464/netflow-collector/internal/obs"
)

const (
	loadRecords   = 200_000
	loadFloorRate = 20_000.0 // records per second
)

// countingSink accepts instantly and counts records. A sleep per batch
// turns it into the deliberately slow sink for the overload scenario.
type countingSink struct {
	n     atomic.Int64
	delay time.Duration
}

func (s *countingSink) WriteBatch(_ context.Context, records []flow.FlowRecord) error {
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	s.n.Add(int64(len(records)))
	return nil
}

func droppedTotal() float64 {
	return testutil.ToFloat64(obs.RecordsDropped.WithLabelValues(obs.DropBufferFull)) +
		testutil.ToFloat64(obs.RecordsDropped.WithLabelValues(obs.DropShutdown))
}

// TestPipelineLoadThroughput offers 200,000 records to a pipeline with an
// in-memory sink and asserts the floor: 20,000 records/second and 0 drops.
// An in-memory Go pipeline does far more than that on any modern machine,
// so the floor catches something pathological, not the hardware.
func TestPipelineLoadThroughput(t *testing.T) {
	sink := &countingSink{}
	p, err := New(Options{BufferSize: 65536, BatchSize: 2000, BatchInterval: time.Second, Workers: 4}, Sink{Name: "memory", Sink: sink})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	dropBefore := droppedTotal()
	rec := flow.FlowRecord{ReceivedAt: time.Now().UTC().Truncate(time.Microsecond), Protocol: 6, SrcPort: 51514, DstPort: 443}
	start := time.Now()
	for i := range loadRecords {
		rec.Packets = uint64(i)
		p.Offer("load", rec)
	}
	offered := time.Since(start)
	require.Eventually(t, func() bool { return sink.n.Load() == loadRecords }, 30*time.Second, 5*time.Millisecond,
		"sink received %d of %d", sink.n.Load(), loadRecords)
	elapsed := time.Since(start)

	rate := float64(loadRecords) / elapsed.Seconds()
	t.Logf("throughput: %d records in %s (offer loop %s) = %.0f records/s; floor %.0f", loadRecords, elapsed, offered, rate, loadFloorRate)
	require.Equal(t, 0.0, droppedTotal()-dropBefore, "0 drops with an instant sink")
	require.GreaterOrEqual(t, rate, loadFloorRate)

	cancel()
	require.NoError(t, <-done)
}

// TestPipelineLoadConservation offers records far faster than a slow sink
// drains them and asserts the hardware-independent invariant: every record
// is either ingested or counted as dropped, and the pending buffer never
// exceeds NFC_PIPELINE_BUFFER.
func TestPipelineLoadConservation(t *testing.T) {
	const (
		bufferSize = 1024
		producers  = 8
		each       = 50_000
	)
	sink := &countingSink{delay: 2 * time.Millisecond}
	p, err := New(Options{BufferSize: bufferSize, BatchSize: 256, BatchInterval: 10 * time.Millisecond, Workers: 2}, Sink{Name: "slow", Sink: sink})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	// Sample the buffer occupancy while the producers run.
	var (
		maxPending atomic.Int64
		stopSample = make(chan struct{})
		sampled    sync.WaitGroup
	)
	sampled.Add(1)
	go func() {
		defer sampled.Done()
		for {
			select {
			case <-stopSample:
				return
			default:
			}
			if n := int64(len(p.buf)); n > maxPending.Load() {
				maxPending.Store(n)
			}
			time.Sleep(50 * time.Microsecond)
		}
	}()

	inBefore := testutil.ToFloat64(obs.RecordsIngested.WithLabelValues("load-slow"))
	dropBefore := droppedTotal()
	var (
		wg       sync.WaitGroup
		accepted atomic.Int64
	)
	start := time.Now()
	for range producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := flow.FlowRecord{ReceivedAt: time.Now().UTC(), Protocol: 17}
			for i := range each {
				rec.Packets = uint64(i)
				if p.Offer("load-slow", rec) {
					accepted.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	offerTime := time.Since(start)
	close(stopSample)
	sampled.Wait()

	offered := float64(producers * each)
	ingested := testutil.ToFloat64(obs.RecordsIngested.WithLabelValues("load-slow")) - inBefore
	dropped := droppedTotal() - dropBefore
	t.Logf("overload: offered %.0f in %s; ingested %.0f, dropped %.0f, max pending %d of %d", offered, offerTime, ingested, dropped, maxPending.Load(), bufferSize)
	require.Equal(t, offered, ingested+dropped, "ingested + dropped == offered, exactly")
	require.Equal(t, float64(accepted.Load()), ingested, "Offer's return value agrees with the counter")
	require.Positive(t, dropped, "the slow sink must have forced drops, or the scenario proved nothing")
	require.LessOrEqual(t, maxPending.Load(), int64(bufferSize), "the buffer never grows past NFC_PIPELINE_BUFFER")
	require.Less(t, offerTime, 10*time.Second, "producers were never blocked")

	cancel()
	require.NoError(t, <-done)
	require.Equal(t, ingested, float64(sink.n.Load()), "every ingested record reached the sink after the drain")
}
