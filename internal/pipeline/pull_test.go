package pipeline

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bogie5464/netflow-collector/internal/flow"
)

// flakySink fails the first n batches, then succeeds.
type flakySink struct {
	fakeSink
	failFirst int32
	seen      atomic.Int32
}

func (s *flakySink) WriteBatch(ctx context.Context, records []flow.FlowRecord) error {
	if s.seen.Add(1) <= s.failFirst {
		return errors.New("flaky: rejected")
	}
	return s.fakeSink.WriteBatch(ctx, records)
}

// pullSource is a flow.PullSource that offers a fixed set of records, awaits
// their writes, and reports what Written told it on result.
type pullSource struct {
	records []flow.FlowRecord
	result  chan error
	started chan struct{}
}

func newPullSource(records ...flow.FlowRecord) *pullSource {
	return &pullSource{records: records, result: make(chan error, 1), started: make(chan struct{})}
}

func (s *pullSource) Start(context.Context, chan<- flow.FlowRecord) error {
	return errors.New("pull source must be run through StartPull")
}

func (s *pullSource) StartPull(ctx context.Context, in flow.Intake) error {
	close(s.started)
	var last uint64
	for _, r := range s.records {
		seq, err := in.Offer(ctx, r)
		if err != nil {
			return err
		}
		last = seq
	}
	s.result <- in.Written(ctx, last)
	<-ctx.Done()
	return nil
}

func TestPullOfferBlocksInsteadOfDropping(t *testing.T) {
	sink := &fakeSink{block: make(chan struct{})}
	p, err := New(Options{BufferSize: 4, BatchSize: 2, BatchInterval: 10 * time.Millisecond, Workers: 1}, Sink{Name: "s", Sink: sink})
	require.NoError(t, err)
	stop := run(t, p)
	droppedBefore := dropped()

	// 4 fill the buffer; the 5th must wait — with the sink blocked, the
	// batcher takes records into pending, so the buffer drains and the
	// offer completes without a drop.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	in := intake{p: p, source: "pull"}
	var seqs []uint64
	for i := range 8 {
		seq, err := in.Offer(ctx, rec(i))
		require.NoError(t, err)
		seqs = append(seqs, seq)
	}
	for i := 1; i < len(seqs); i++ {
		require.Equal(t, seqs[i-1]+1, seqs[i], "sequence numbers are dense and increasing")
	}
	require.Equal(t, droppedBefore, dropped(), "a pull source is never dropped from")

	close(sink.block)
	require.NoError(t, in.Written(ctx, seqs[len(seqs)-1]))
	require.Equal(t, 8, sink.count())
	stop()
}

func TestWrittenAdvancesOnlyThroughContiguousBatches(t *testing.T) {
	var w watermark
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Batches [1,2] [3,4] [5,6] complete in the order 2nd, 3rd, 1st.
	w.complete(3, 4, true)
	w.complete(5, 6, true)
	waitCtx, waitCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	require.ErrorIs(t, w.await(waitCtx, 4), context.DeadlineExceeded, "4 is written but 1 and 2 are not, so nothing is durable yet")
	waitCancel()

	w.complete(1, 2, true)
	require.NoError(t, w.await(ctx, 6), "the run 1..6 is now contiguous")
	require.NoError(t, w.await(ctx, 3))
}

func TestWrittenReportsAFailureOnceThenMovesOn(t *testing.T) {
	var w watermark
	ctx := context.Background()
	w.complete(1, 2, true)
	w.complete(3, 4, false) // a sink rejected this batch
	w.complete(5, 6, true)

	require.NoError(t, w.await(ctx, 2), "everything before the failure is fine")
	require.ErrorIs(t, w.await(ctx, 6), flow.ErrWriteFailed, "waiting past the failed range reports it")
	require.NoError(t, w.await(ctx, 6), "and reports it once: the caller is redelivering")
}

func TestPullSourceHearsAboutARejectedBatch(t *testing.T) {
	sink := &flakySink{failFirst: 1}
	p, err := New(Options{BufferSize: 16, BatchSize: 4, BatchInterval: 10 * time.Millisecond, Workers: 1}, Sink{Name: "s", Sink: sink})
	require.NoError(t, err)
	run(t, p)

	src := newPullSource(rec(1), rec(2), rec(3), rec(4))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.RunSource(ctx, "pull", src) }()
	<-src.started
	select {
	case err := <-src.result:
		require.ErrorIs(t, err, flow.ErrWriteFailed)
	case <-time.After(5 * time.Second):
		t.Fatal("Written never returned")
	}
	cancel()
	require.NoError(t, <-done)
}

func TestPullSourceIsAcknowledgedOnlyAfterEverySinkWrote(t *testing.T) {
	slow := &fakeSink{block: make(chan struct{})}
	fast := &fakeSink{}
	p, err := New(Options{BufferSize: 16, BatchSize: 4, BatchInterval: 10 * time.Millisecond, Workers: 2},
		Sink{Name: "slow", Sink: slow}, Sink{Name: "fast", Sink: fast})
	require.NoError(t, err)
	run(t, p)

	src := newPullSource(rec(1), rec(2))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = p.RunSource(ctx, "pull", src) }()
	<-src.started
	require.Eventually(t, func() bool { return fast.count() == 2 }, 5*time.Second, 10*time.Millisecond)
	select {
	case err := <-src.result:
		t.Fatalf("acknowledged before the slow sink wrote: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(slow.block)
	select {
	case err := <-src.result:
		require.NoError(t, err, "every sink wrote, so the records are durable")
	case <-time.After(5 * time.Second):
		t.Fatal("Written never returned")
	}
}
