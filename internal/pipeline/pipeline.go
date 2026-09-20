// Package pipeline is the backpressure spine between sources and sinks: a
// bounded buffer, a batcher, and a worker pool that fans each batch out to
// every sink in parallel.
//
// Backpressure is drop-and-count. Offer never blocks and the buffer never
// grows; when it is full the record is discarded and counted. That default
// branch in Offer is the entire strategy and must never become a blocking
// send or a growing slice.
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
	select {
	case p.buf <- rec:
		obs.RecordsIngested.WithLabelValues(source).Inc()
		obs.PipelineBufferLength.Set(float64(len(p.buf)))
		return true
	default:
		obs.RecordsDropped.WithLabelValues(obs.DropBufferFull).Inc()
		return false
	}
}

// RunSource runs src until ctx is cancelled, offering every record it emits.
// The intake channel is unbuffered on purpose: the only queue is the bounded
// buffer, and the goroutine draining intake never blocks, so the source is
// never held up by a slow sink.
func (p *Pipeline) RunSource(ctx context.Context, name string, src flow.Source) error {
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
	batches := make(chan []flow.FlowRecord)
	var workers sync.WaitGroup
	for range p.opts.Workers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for batch := range batches {
				p.write(context.WithoutCancel(ctx), batch)
			}
		}()
	}

	pending := make([]flow.FlowRecord, 0, p.opts.BatchSize)
	timer := time.NewTimer(p.opts.BatchInterval)
	defer timer.Stop()
	flush := func() {
		if len(pending) == 0 {
			return
		}
		batches <- pending
		pending = make([]flow.FlowRecord, 0, p.opts.BatchSize)
	}

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case rec := <-p.buf:
			obs.PipelineBufferLength.Set(float64(len(p.buf)))
			pending = append(pending, rec)
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
			pending = append(pending, rec)
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
	return nil
}

// write fans one batch out to every sink in parallel and collects the
// outcome per sink. A failing sink degrades only its own metrics.
func (p *Pipeline) write(ctx context.Context, batch []flow.FlowRecord) {
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
			err := s.WriteBatch(ctx, batch)
			obs.BatchWriteDuration.WithLabelValues(s.Name).Observe(time.Since(start).Seconds())
			if err != nil {
				obs.BatchWriteErrors.WithLabelValues(s.Name).Inc()
				p.log.Error("batch write failed", "sink", s.Name, "records", len(batch), "err", err)
				return
			}
			succeeded.Add(1)
		}()
	}
	wg.Wait()

	if succeeded.Load() == 0 {
		return
	}
	now := time.Now()
	for i := range batch {
		obs.RecordVisibilityLag.Observe(now.Sub(batch[i].ReceivedAt).Seconds())
	}
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
