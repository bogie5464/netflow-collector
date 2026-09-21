package pipeline

import (
	"context"
	"sync"

	"github.com/bogie5464/netflow-collector/internal/flow"
)

// watermark tracks how far the pipeline's write order has been made durable.
// Workers complete batches out of order; written advances only through a
// contiguous run of completed batches, so "written ≥ seq" means every
// record up to seq has been through every sink.
//
// A batch that any sink rejected still completes the sequence — otherwise
// nothing after it could ever be acknowledged — but is remembered as a
// failed range until a waiter has been told. A pull source that hears
// ErrWriteFailed rewinds to its last acknowledgement, so the failed records
// come back through the pipeline; the sinks that did write them will
// dedup.
type watermark struct {
	mu       sync.Mutex
	written  uint64
	done     map[uint64]uint64 // first → last of completed batches not yet contiguous
	failed   []span            // ranges a sink rejected, oldest first, not yet reported
	changed  chan struct{}     // closed and replaced whenever written or failed changes
	closed   bool
	initOnce sync.Once
}

type span struct{ first, last uint64 }

func (w *watermark) init() {
	w.initOnce.Do(func() {
		w.done = map[uint64]uint64{}
		w.changed = make(chan struct{})
	})
}

// complete records one batch's outcome and advances written as far as the
// contiguous run allows.
func (w *watermark) complete(first, last uint64, ok bool) {
	w.init()
	w.mu.Lock()
	defer w.mu.Unlock()
	if !ok {
		w.failed = append(w.failed, span{first, last})
	}
	w.done[first] = last
	for {
		last, found := w.done[w.written+1]
		if !found {
			break
		}
		delete(w.done, w.written+1)
		w.written = last
	}
	w.wake()
}

// close releases every waiter; the pipeline is stopping.
func (w *watermark) close() {
	w.init()
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	w.wake()
}

func (w *watermark) wake() {
	close(w.changed)
	w.changed = make(chan struct{})
}

// await blocks until written ≥ seq, or a failed range at or below seq is
// pending, or ctx ends. A failure is reported once: the ranges it covers
// are forgotten on return, because the caller is about to redeliver them.
func (w *watermark) await(ctx context.Context, seq uint64) error {
	w.init()
	for {
		w.mu.Lock()
		if n := len(w.failed); n > 0 && w.failed[0].first <= seq {
			kept := w.failed[:0]
			for _, f := range w.failed {
				if f.first > seq {
					kept = append(kept, f)
				}
			}
			w.failed = kept
			w.mu.Unlock()
			return flow.ErrWriteFailed
		}
		if w.written >= seq {
			w.mu.Unlock()
			return nil
		}
		if w.closed {
			w.mu.Unlock()
			return context.Canceled
		}
		changed := w.changed
		w.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
