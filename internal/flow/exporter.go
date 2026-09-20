package flow

import (
	"context"
	"net/netip"
	"time"
)

// Exporter is one row of the exporters table.
type Exporter struct {
	ID          int64
	IPAddress   netip.Addr
	Label       *string
	FirstSeenAt time.Time
	LastSeenAt  time.Time
}

// ExporterLister is an optional capability a Backend may implement to serve
// GET /v1/exporters. It is discovered by type assertion, never added to
// Backend itself — the storage contract in ports.go never widens for one
// endpoint.
type ExporterLister interface {
	ListExporters(ctx context.Context) ([]Exporter, error)
}
