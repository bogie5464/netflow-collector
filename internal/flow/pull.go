package flow

import (
	"context"
	"errors"
)

// ErrWriteFailed is returned by Intake.Written when a record at or below the
// awaited sequence was in a batch that a sink rejected. The source should
// rewind to its last acknowledgement and deliver again; every sink writes
// conflict-tolerantly, so the records that did land are not duplicated.
var ErrWriteFailed = errors.New("flow: a batch failed to write")

// Intake is the pipeline as a PullSource sees it. Unlike the channel a
// Source writes to, Offer blocks: a transport that can wait is waited on
// rather than dropped from, and the sequence numbers let the source learn
// when its records are durable.
type Intake interface {
	// Offer blocks until the record is buffered or ctx ends, and returns
	// the record's position in the pipeline's write order.
	Offer(ctx context.Context, rec FlowRecord) (seq uint64, err error)
	// Written blocks until every record with a sequence at or below seq has
	// been written to every sink, or returns ErrWriteFailed if one of them
	// was in a batch a sink rejected. A failure is reported once.
	Written(ctx context.Context, seq uint64) error
}

// PullSource is the optional capability of a Source whose transport can be
// paused and can deliver again from an acknowledged position — a message
// bus, a file. The pipeline prefers StartPull when a source offers it: the
// source never loses a record to a full buffer, and it acknowledges its
// transport only after Written confirms the write. Discovered by type
// assertion; Source itself never changes, and a push source (UDP) cannot
// implement this because nothing on the wire waits for it.
type PullSource interface {
	Source
	StartPull(ctx context.Context, in Intake) error
}
