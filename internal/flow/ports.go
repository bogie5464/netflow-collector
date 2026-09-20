package flow

import "context"

// Source produces flow records from somewhere — a UDP socket, a Kafka topic,
// a file replay — onto out until ctx is cancelled. It must never block on a
// slow consumer beyond what out's capacity allows; backpressure is the
// pipeline's concern and is drop-and-count.
type Source interface {
	Start(ctx context.Context, out chan<- FlowRecord) error
}

// Sink stores a batch of records. It is batch-shaped and nothing more: no SQL,
// no query builder, no dialect flag. A write must be conflict-tolerant so that
// a redelivered record with the same (ReceivedAt, DedupKey) is not an error.
type Sink interface {
	WriteBatch(ctx context.Context, records []FlowRecord) error
}

// Querier answers a bounded FlowQuery with one page of results.
type Querier interface {
	Query(ctx context.Context, q FlowQuery) (FlowResultPage, error)
}

// Backend is a storage engine: it writes, it reads, it owns its own schema
// migrations, and it can be closed.
type Backend interface {
	Sink
	Querier
	Migrate(ctx context.Context) error
	Close() error
}
