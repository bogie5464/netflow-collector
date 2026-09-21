// Package pipeline is the backpressure spine between sources and sinks: a
// bounded buffer, a batcher, and a worker pool that fans each batch out to
// every sink in parallel.
//
// Backpressure is drop-and-count for a push source. Offer never blocks and
// the buffer never grows; when it is full the record is discarded and
// counted. That default branch in Offer is the entire strategy for anything
// arriving on a wire that will not wait, and must never become a blocking
// send or a growing slice.
//
// A pull source (flow.PullSource) is the exception by design: its transport
// can wait, so it is waited on. It offers through an Intake that blocks for
// buffer space and reports, by sequence number, when a record has been
// written to every sink — which is what lets the source acknowledge its
// transport only after the write.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bogie5464/netflow-collector/internal/flow"
	"github.com/bogie5464/netflow-collector/internal/obs"
)

// Options sizes the pipeline. They map one-to-one onto NFC_PIPELINE_BUFFER,
// NFC_BATCH_SIZE, NFC_BATCH_INTERVAL and NFC_WORKERS; the caller passes the
// values so this package never reads configuration itself.
type Options struct {
	BufferSize    int
	BatchSize     int
	BatchInterval time.Duration
	Workers       int
	// WriteTimeout bounds one WriteBatch call so a hung sink cannot pin a
	// worker forever. Zero means the default of 30s.
	WriteTimeout time.Duration
}

// Sink is a flow.Sink with the name that labels its metrics.
type Sink struct {
	Name string
	flow.Sink
}

// Pipeline is created with New and driven by Run.
type Pipeline struct {
	opts  Options
	sinks []Sink
	log   *slog.Logger

	buf     chan flow.FlowRecord
	closing atomic.Bool

	// Sequence numbers are assigned to buffered records in buffer order,
	// densely from 1, under offerMu so the order of assignment is the order
	// in the channel. The batcher, the only reader, can therefore know a
	// batch's range by counting.
	offerMu sync.Mutex
	next    uint64
	space   chan struct{} // signalled when the batcher takes a record

	wm watermark
}

// New validates opts and builds the bounded buffer.
func New(opts Options, sinks ...Sink) (*Pipeline, error) {
	if opts.BufferSize < 1 || opts.BatchSize < 1 || opts.BatchInterval <= 0 || opts.Workers < 1 {
		return nil, fmt.Errorf("pipeline: invalid options %+v", opts)
	}
	if opts.WriteTimeout <= 0 {
		opts.WriteTimeout = 30 * time.Second
	}
	if len(sinks) == 0 {
		return nil, errors.New("pipeline: at least one sink is required")
	}
	return &Pipeline{
		opts:  opts,
		sinks: sinks,
		log:   slog.Default().With("component", "pipeline"),
		buf:   make(chan flow.FlowRecord, opts.BufferSize),
		space: make(chan struct{}, 1),
	}, nil
}

// Offer hands one record to the pipeline without ever blocking. It returns
// false when the record was dropped; every outcome is counted, so
// ingested + dropped == offered holds exactly.
func (p *Pipeline) Offer(source string, rec flow.FlowRecord) bool {
	if p.closing.Load() {
		obs.RecordsDropped.WithLabelValues(obs.DropShutdown).Inc()
		return false
	}
	if _, ok := p.tryOffer(rec); ok {
		obs.RecordsIngested.WithLabelValues(source).Inc()
		return true
	}
	obs.RecordsDropped.WithLabelValues(obs.DropBufferFull).Inc()
	return false
}

// tryOffer is the one non-blocking send. The lock is held only across it,
// so a blocked pull source never holds up a push source.
func (p *Pipeline) tryOffer(rec flow.FlowRecord) (uint64, bool) {
	p.offerMu.Lock()
	defer p.offerMu.Unlock()
	select {
	case p.buf <- rec:
		p.next++
		obs.PipelineBufferLength.Set(float64(len(p.buf)))
		return p.next, true
	default:
		return 0, false
	}
}

// intake is the flow.Intake handed to a pull source.
type intake struct {
	p      *Pipeline
	source string
}

// Offer waits for buffer space. It wakes on the batcher's signal rather
// than polling, and gives up only with ctx.
func (in intake) Offer(ctx context.Context, rec flow.FlowRecord) (uint64, error) {
	for {
		if in.p.closing.Load() {
			return 0, errors.New("pipeline: closing")
		}
		if seq, ok := in.p.tryOffer(rec); ok {
			obs.RecordsIngested.WithLabelValues(in.source).Inc()
			return seq, nil
		}
		select {
		case <-in.p.space:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

// Written is the watermark wait; see watermark.
func (in intake) Written(ctx context.Context, seq uint64) error {
	return in.p.wm.await(ctx, seq)
}

// RunSource runs src until ctx is cancelled, offering every record it emits.
// A flow.PullSource is run through the blocking Intake instead. For a push
// source the intake channel is unbuffered on purpose: the only queue is the
// bounded buffer, and the goroutine draining the channel never blocks, so
// the source is never held up by a slow sink.
func (p *Pipeline) RunSource(ctx context.Context, name string, src flow.Source) error {
	if pull, ok := src.(flow.PullSource); ok {
		if err := pull.StartPull(ctx, intake{p: p, source: name}); err != nil {
			return fmt.Errorf("pipeline: source %s: %w", name, err)
		}
		return nil
	}
	intake := make(chan flow.FlowRecord)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for rec := range intake {
			p.Offer(name, rec)
		}
	}()
	err := src.Start(ctx, intake)
	close(intake)
	<-done
	if err != nil {
		return fmt.Errorf("pipeline: source %s: %w", name, err)
	}
	return nil
}

// Run batches the buffer and writes until ctx is cancelled, then stops
// accepting offers, drains what is buffered, flushes the pending partial
// batch to every sink and returns.
func (p *Pipeline) Run(ctx context.Context) error {
	batches := make(chan batch)
	var workers sync.WaitGroup
	for range p.opts.Workers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for b := range batches {
				ok := p.write(context.WithoutCancel(ctx), b.records)
				p.wm.complete(b.first, b.last, ok)
			}
		}()
	}

	var (
		pending = make([]flow.FlowRecord, 0, p.opts.BatchSize)
		taken   uint64 // records taken from the buffer so far == the last one's sequence
		first   = uint64(1)
	)
	timer := time.NewTimer(p.opts.BatchInterval)
	defer timer.Stop()
	flush := func() {
		if len(pending) == 0 {
			return
		}
		batches <- batch{records: pending, first: first, last: taken}
		first = taken + 1
		pending = make([]flow.FlowRecord, 0, p.opts.BatchSize)
	}
	take := func(rec flow.FlowRecord) {
		taken++
		pending = append(pending, rec)
		select {
		case p.space <- struct{}{}:
		default:
		}
	}

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case rec := <-p.buf:
			obs.PipelineBufferLength.Set(float64(len(p.buf)))
			take(rec)
			if len(pending) >= p.opts.BatchSize {
				flush()
				resetTimer(timer, p.opts.BatchInterval)
			}
		case <-timer.C:
			flush()
			timer.Reset(p.opts.BatchInterval)
		}
	}

	// Shutdown: refuse new offers, drain the buffer, flush the remainder.
	p.closing.Store(true)
drain:
	for {
		select {
		case rec := <-p.buf:
			take(rec)
			if len(pending) >= p.opts.BatchSize {
				flush()
			}
		default:
			break drain
		}
	}
	flush()
	obs.PipelineBufferLength.Set(0)
	close(batches)
	workers.Wait()
	p.wm.close()
	return nil
}

// batch is one flush: the records and their sequence range.
type batch struct {
	records     []flow.FlowRecord
	first, last uint64
}

// write fans one batch out to every sink in parallel and collects the
// outcome per sink. A failing sink degrades only its own metrics. It reports
// true only if every sink succeeded, which is what a pull source is told.
func (p *Pipeline) write(ctx context.Context, records []flow.FlowRecord) bool {
	ctx, cancel := context.WithTimeout(ctx, p.opts.WriteTimeout)
	defer cancel()

	var (
		wg        sync.WaitGroup
		succeeded atomic.Int32
	)
	for _, s := range p.sinks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			err := s.WriteBatch(ctx, records)
			obs.BatchWriteDuration.WithLabelValues(s.Name).Observe(time.Since(start).Seconds())
			if err != nil {
				obs.BatchWriteErrors.WithLabelValues(s.Name).Inc()
				p.log.Error("batch write failed", "sink", s.Name, "records", len(records), "err", err)
				return
			}
			succeeded.Add(1)
		}()
	}
	wg.Wait()

	if succeeded.Load() == 0 {
		return false
	}
	now := time.Now()
	for i := range records {
		obs.RecordVisibilityLag.Observe(now.Sub(records[i].ReceivedAt).Seconds())
	}
	return int(succeeded.Load()) == len(p.sinks)
}

// resetTimer restarts a timer that may or may not have fired.
func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}
