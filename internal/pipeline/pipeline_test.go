package pipeline

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/bogie5464/netflow-collector/internal/flow"
	"github.com/bogie5464/netflow-collector/internal/obs"
)

// fakeSink records every batch. If block is non-nil, WriteBatch waits on it
// (or ctx) first; if err is non-nil, every write fails with it.
type fakeSink struct {
	mu      sync.Mutex
	batches [][]flow.FlowRecord
	block   chan struct{}
	delay   time.Duration
	err     error
}

func (s *fakeSink) WriteBatch(ctx context.Context, records []flow.FlowRecord) error {
	if s.block != nil {
		select {
		case <-s.block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	if s.err != nil {
		return s.err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches = append(s.batches, append([]flow.FlowRecord(nil), records...))
	return nil
}

func (s *fakeSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, b := range s.batches {
		n += len(b)
	}
	return n
}

func (s *fakeSink) sizes() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]int, len(s.batches))
	for i, b := range s.batches {
		out[i] = len(b)
	}
	return out
}

func rec(i int) flow.FlowRecord {
	return flow.FlowRecord{ReceivedAt: time.Now().UTC().Truncate(time.Microsecond), SrcPort: uint16(i)}
}

func ingested(source string) float64 {
	return testutil.ToFloat64(obs.RecordsIngested.WithLabelValues(source))
}

func dropped() float64 {
	return testutil.ToFloat64(obs.RecordsDropped.WithLabelValues(obs.DropBufferFull)) +
		testutil.ToFloat64(obs.RecordsDropped.WithLabelValues(obs.DropShutdown))
}

// run starts p.Run and returns a stop function that cancels and waits.
func run(t *testing.T, p *Pipeline) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("Run did not return after cancel")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func TestNewRejectsBadOptions(t *testing.T) {
	_, err := New(Options{BufferSize: 0, BatchSize: 1, BatchInterval: time.Second, Workers: 1}, Sink{"s", &fakeSink{}})
	require.Error(t, err)
	_, err = New(Options{BufferSize: 1, BatchSize: 1, BatchInterval: time.Second, Workers: 1})
	require.Error(t, err, "no sinks")
}

func TestOverflowDropsAndNeverBlocksTheSource(t *testing.T) {
	blocked := &fakeSink{block: make(chan struct{})}
	p, err := New(Options{BufferSize: 8, BatchSize: 4, BatchInterval: time.Hour, Workers: 1}, Sink{"blocked", blocked})
	require.NoError(t, err)
	run(t, p)
	t.Cleanup(func() { close(blocked.block) }) // registered after run so it releases the sink before stop

	const offered = 200
	inBefore, dropBefore := ingested("t-overflow"), dropped()
	bfBefore := testutil.ToFloat64(obs.RecordsDropped.WithLabelValues(obs.DropBufferFull))
	start := time.Now()
	accepted := 0
	for i := range offered {
		if p.Offer("t-overflow", rec(i)) {
			accepted++
		}
	}
	require.Less(t, time.Since(start), 500*time.Millisecond, "Offer must never block on a blocked sink")
	require.Less(t, accepted, offered, "the bounded buffer must overflow")
	require.Equal(t, float64(accepted), ingested("t-overflow")-inBefore)
	require.Equal(t, float64(offered-accepted), dropped()-dropBefore)
	require.Equal(t, float64(offered-accepted), testutil.ToFloat64(obs.RecordsDropped.WithLabelValues(obs.DropBufferFull))-bfBefore,
		"every drop is reason=buffer_full")
}

func TestInstantSinkWritesEverything(t *testing.T) {
	sink := &fakeSink{}
	p, err := New(Options{BufferSize: 65536, BatchSize: 2000, BatchInterval: 50 * time.Millisecond, Workers: 4}, Sink{"fast", sink})
	require.NoError(t, err)
	run(t, p)

	const offered = 10000
	dropBefore := dropped()
	for i := range offered {
		require.True(t, p.Offer("t-instant", rec(i)))
	}
	require.Eventually(t, func() bool { return sink.count() == offered }, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, 0.0, dropped()-dropBefore)
}

func TestBatchSizeAndIntervalFlush(t *testing.T) {
	sink := &fakeSink{}
	p, err := New(Options{BufferSize: 1024, BatchSize: 100, BatchInterval: 100 * time.Millisecond, Workers: 1}, Sink{"s", sink})
	require.NoError(t, err)
	run(t, p)

	for i := range 250 {
		require.True(t, p.Offer("t-batch", rec(i)))
	}
	require.Eventually(t, func() bool { return sink.count() == 250 }, 3*time.Second, 5*time.Millisecond)
	require.Equal(t, []int{100, 100, 50}, sink.sizes(), "two full batches at NFC_BATCH_SIZE, then the partial one after NFC_BATCH_INTERVAL")
}

func TestOneFailingSinkDoesNotStarveTheOther(t *testing.T) {
	good := &fakeSink{}
	bad := &fakeSink{err: errors.New("disk on fire")}
	p, err := New(Options{BufferSize: 1024, BatchSize: 10, BatchInterval: 20 * time.Millisecond, Workers: 2},
		Sink{"good", good}, Sink{"bad", bad})
	require.NoError(t, err)
	run(t, p)

	errBefore := testutil.ToFloat64(obs.BatchWriteErrors.WithLabelValues("bad"))
	goodErrBefore := testutil.ToFloat64(obs.BatchWriteErrors.WithLabelValues("good"))
	for i := range 30 {
		require.True(t, p.Offer("t-fail", rec(i)))
	}
	require.Eventually(t, func() bool { return good.count() == 30 }, 3*time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(obs.BatchWriteErrors.WithLabelValues("bad"))-errBefore == 3
	}, 3*time.Second, 5*time.Millisecond, "one error per batch for the failing sink")
	require.Equal(t, goodErrBefore, testutil.ToFloat64(obs.BatchWriteErrors.WithLabelValues("good")))

	// Still running after the failures.
	for i := range 10 {
		require.True(t, p.Offer("t-fail", rec(100+i)))
	}
	require.Eventually(t, func() bool { return good.count() == 40 }, 3*time.Second, 5*time.Millisecond)
}

func TestCancelFlushesPartialBatchToEverySink(t *testing.T) {
	a, b := &fakeSink{}, &fakeSink{}
	p, err := New(Options{BufferSize: 1024, BatchSize: 100, BatchInterval: time.Hour, Workers: 2}, Sink{"a", a}, Sink{"b", b})
	require.NoError(t, err)
	stop := run(t, p)

	for i := range 7 {
		require.True(t, p.Offer("t-cancel", rec(i)))
	}
	require.Equal(t, 0, a.count(), "nothing flushes before size or interval")
	stop()
	require.Equal(t, []int{7}, a.sizes())
	require.Equal(t, []int{7}, b.sizes())
	require.False(t, p.Offer("t-cancel", rec(99)), "offers after shutdown are dropped, not lost silently")
}

func TestIngestedPlusDroppedEqualsOffered(t *testing.T) {
	// A sink slower than the producers, so the buffer oscillates between
	// full and draining and both counters move.
	slow := &fakeSink{delay: time.Millisecond}
	p, err := New(Options{BufferSize: 64, BatchSize: 16, BatchInterval: 5 * time.Millisecond, Workers: 2}, Sink{"slow", slow})
	require.NoError(t, err)
	run(t, p)

	const producers, each = 8, 2000
	inBefore, dropBefore := ingested("t-invariant"), dropped()
	var wg sync.WaitGroup
	for range producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				p.Offer("t-invariant", rec(i))
			}
		}()
	}
	wg.Wait()

	gotIn := ingested("t-invariant") - inBefore
	gotDrop := dropped() - dropBefore
	require.Equal(t, float64(producers*each), gotIn+gotDrop, "ingested=%v dropped=%v", gotIn, gotDrop)
}
