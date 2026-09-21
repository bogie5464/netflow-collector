package flow

import "context"

// Pinger is an optional capability a Sink may implement to answer /readyz
// with something cheaper or more meaningful than a query. A sink that cannot
// be queried at all — a message bus — must implement it, because it is the
// only readiness signal the collector has for it. Discovered by type
// assertion, never added to Sink or Backend.
type Pinger interface {
	Ping(ctx context.Context) error
}
