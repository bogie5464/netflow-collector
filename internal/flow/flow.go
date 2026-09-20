// Package flow is the domain contract of the collector: the record every source
// produces and every sink stores, the query every backend answers, and the four
// interfaces in ports.go that make sources and backends pluggable.
//
// This package imports nothing from this module. Anything it imported would
// become part of the contract every adapter has to accept.
package flow

import (
	"net/netip"
	"time"
)

// FlowType identifies the wire protocol a record was decoded from. It is stored
// as constrained text, never as a database enum.
type FlowType string

// The three flow types the collector understands.
const (
	FlowTypeNetFlow5 FlowType = "netflow5"
	FlowTypeNetFlow9 FlowType = "netflow9"
	FlowTypeIPFIX    FlowType = "ipfix"
)

// Valid reports whether t is one of the known flow types.
func (t FlowType) Valid() bool {
	switch t {
	case FlowTypeNetFlow5, FlowTypeNetFlow9, FlowTypeIPFIX:
		return true
	default:
		return false
	}
}

// FlowRecord is one flow as accepted by this collector. Every address is a
// netip.Addr and every timestamp is a time.Time; sinks truncate timestamps to
// microseconds on write because both storage engines store microseconds.
type FlowRecord struct {
	// ReceivedAt is when this collector accepted the record. On a live wire
	// protocol it is the collector's clock; on a transport that can redeliver
	// (Kafka) it comes from the message so a redelivery is the same row.
	ReceivedAt time.Time

	// ExporterAddr is the source address of the exporting device. Sinks
	// resolve it to an exporters row through their own bounded cache.
	ExporterAddr netip.Addr

	// ExporterID is the surrogate key of the exporters row. It is zero on
	// ingest (the sink resolves it) and populated by a Querier on read.
	ExporterID int64

	FlowType FlowType

	// FirstSwitched and LastSwitched are the flow's start and end as reported
	// by the exporter. They are not the same clock as ReceivedAt.
	FirstSwitched time.Time
	LastSwitched  time.Time

	SrcAddr netip.Addr
	DstAddr netip.Addr
	SrcPort uint16 // 0 for protocols without ports
	DstPort uint16
	// Protocol is the IANA protocol number: 6 TCP, 17 UDP, 1 ICMP.
	Protocol uint8
	// TCPFlags is the cumulative OR of the flags seen in the flow.
	TCPFlags uint8

	// Packets and Bytes are counts before sampling correction; multiply by
	// SamplingRate to estimate the wire count.
	Packets      uint64
	Bytes        uint64
	SamplingRate uint32 // 1 means unsampled

	InputIface  uint32 // SNMP ifIndex on the exporter, 0 when unknown
	OutputIface uint32
	SrcAS       uint32 // 0 means the exporter did not tell us
	DstAS       uint32

	// NextHop is the invalid zero Addr when the exporter omits it.
	NextHop netip.Addr

	// DedupKey is nil for append-only sources (the UDP path) and non-nil for
	// transports that can redeliver (the Kafka path). Sinks never branch on
	// it: the nullable unique index on (received_at, dedup_key) does the work.
	DedupKey []byte
}

// FlowQuery is a bounded time range plus whitelisted filters, a limit and an
// opaque cursor. It is not an expression tree and not a query language.
type FlowQuery struct {
	// Start and End bound the half-open range [Start, End) on ReceivedAt.
	// Both are mandatory: there is no unbounded scan, ever.
	Start time.Time
	End   time.Time

	// ExporterID filters on the exporters row; 0 means no filter.
	ExporterID int64
	// SrcAddr and DstAddr are exact-match filters; the invalid zero Addr
	// means no filter.
	SrcAddr netip.Addr
	DstAddr netip.Addr
	// Protocol filters on the IANA protocol number; nil means no filter,
	// because 0 is itself a valid protocol.
	Protocol *uint8

	// Limit is the maximum number of records in the page. The caller clamps
	// it; a backend treats 0 as its own default.
	Limit int
	// Cursor is the opaque NextCursor of a previous page, or empty for the
	// first page. Only the backend that produced it can decode it.
	Cursor string
}

// FlowResultPage is one page of query results.
type FlowResultPage struct {
	Records []FlowRecord
	// NextCursor is the opaque cursor for the next page; empty when HasMore
	// is false.
	NextCursor string
	HasMore    bool
}
