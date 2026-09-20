// Package sinktest is the storage contract, executable. Every backend calls
// Conformance with a factory and passes it unmodified; if the suite needs a
// branch for an engine, the interface has leaked and the fix belongs in the
// backend or the interface, never here.
package sinktest

import (
	"context"
	"fmt"
	"net/netip"
	"sort"
	"testing"
	"time"

	"github.com/moby/moby/api/types/network"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/bogie5464/netflow-collector/internal/flow"
)

// TimescaleImage is copied literally from docker-compose.yml, which owns it.
const TimescaleImage = "timescale/timescaledb:2.30.1-pg17"

// StartTimescale starts a throwaway TimescaleDB container and returns its DSN.
// It waits with an explicit SQL probe rather than scraping logs.
func StartTimescale(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	c, err := tcpostgres.Run(ctx, TimescaleImage,
		tcpostgres.WithDatabase("netflow"),
		tcpostgres.WithUsername("netflow"),
		tcpostgres.WithPassword("netflow"),
		testcontainers.WithWaitStrategy(
			wait.ForSQL("5432/tcp", "pgx", func(host string, port network.Port) string {
				return fmt.Sprintf("postgres://netflow:netflow@%s:%s/netflow?sslmode=disable", host, port.Port())
			}).WithStartupTimeout(2*time.Minute)),
	)
	require.NoError(t, err, "start %s", TimescaleImage)
	t.Cleanup(func() { require.NoError(t, testcontainers.TerminateContainer(c)) })
	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	return dsn
}

// Factory returns a freshly reset backend for one assertion. Each call must
// start from empty tables: no assertion may see another's rows.
type Factory func(t *testing.T) (flow.Sink, flow.Querier)

// Fixture builds one deterministic record. Timestamps are truncated to
// microseconds because both engines store microseconds and Go carries
// nanoseconds; comparing at nanosecond precision fails on correct code.
func Fixture(i int, exporter netip.Addr, receivedAt time.Time, dedup bool) flow.FlowRecord {
	receivedAt = receivedAt.UTC().Truncate(time.Microsecond)
	r := flow.FlowRecord{
		ReceivedAt:    receivedAt,
		ExporterAddr:  exporter,
		FlowType:      flow.FlowTypeNetFlow5,
		FirstSwitched: receivedAt.Add(-2 * time.Second),
		LastSwitched:  receivedAt.Add(-500 * time.Millisecond),
		SrcAddr:       netip.AddrFrom4([4]byte{203, 0, 113, byte(i)}),
		DstAddr:       netip.AddrFrom4([4]byte{198, 51, 100, byte(200 + i%50)}),
		SrcPort:       uint16(40000 + i),
		DstPort:       443,
		Protocol:      6,
		TCPFlags:      24,
		Packets:       uint64(10 + i),
		Bytes:         uint64(1000 * (i + 1)),
		SamplingRate:  1000,
		InputIface:    3,
		OutputIface:   7,
		SrcAS:         64500,
		DstAS:         64501,
		NextHop:       netip.MustParseAddr("198.51.100.1"),
	}
	if i%3 == 1 { // some IPv6, some without a next hop, some UDP
		r.SrcAddr = netip.MustParseAddr(fmt.Sprintf("2001:db8::%x", i+1))
		r.DstAddr = netip.MustParseAddr("2001:db8:ffff::1")
		r.NextHop = netip.Addr{}
		r.Protocol = 17
		r.TCPFlags = 0
		r.FlowType = flow.FlowTypeIPFIX
	}
	if dedup {
		key := make([]byte, 16)
		copy(key, fmt.Sprintf("dedup-%010d", i))
		r.DedupKey = key
	}
	return r
}

var (
	exporterA = netip.MustParseAddr("192.0.2.1")
	exporterB = netip.MustParseAddr("192.0.2.2")
	baseTime  = time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
)

// batch builds n fixtures one microsecond apart so ordering is total.
func batch(n int, exporter netip.Addr, dedup bool) []flow.FlowRecord {
	out := make([]flow.FlowRecord, n)
	for i := range out {
		out[i] = Fixture(i, exporter, baseTime.Add(time.Duration(i)*time.Microsecond), dedup)
	}
	return out
}

// all fetches every record in the fixture window, following cursors.
func all(t *testing.T, q flow.Querier, base flow.FlowQuery) []flow.FlowRecord {
	t.Helper()
	var out []flow.FlowRecord
	for {
		page, err := q.Query(context.Background(), base)
		require.NoError(t, err)
		out = append(out, page.Records...)
		if !page.HasMore {
			require.Empty(t, page.NextCursor, "NextCursor must be empty on the last page")
			return out
		}
		require.NotEmpty(t, page.NextCursor)
		base.Cursor = page.NextCursor
	}
}

func window() flow.FlowQuery {
	return flow.FlowQuery{Start: baseTime.Add(-time.Hour), End: baseTime.Add(time.Hour), Limit: 1000}
}

// comparable strips the read-side field a write cannot know.
func comparable(r flow.FlowRecord) flow.FlowRecord {
	r.ExporterID = 0
	return r
}

func bySrcPort(rs []flow.FlowRecord) {
	sort.Slice(rs, func(i, j int) bool { return rs[i].SrcPort < rs[j].SrcPort })
}

// Conformance runs the six assertions every backend must pass.
func Conformance(t *testing.T, factory Factory) {
	t.Helper()
	ctx := context.Background()

	t.Run("round-trips every field at microsecond precision", func(t *testing.T) {
		sink, querier := factory(t)
		want := batch(6, exporterA, false)
		require.NoError(t, sink.WriteBatch(ctx, want))
		got := all(t, querier, window())
		require.Len(t, got, len(want))
		bySrcPort(got)
		for i := range want {
			require.Positive(t, got[i].ExporterID, "the querier resolves exporter_id")
			require.Equal(t, comparable(want[i]), comparable(got[i]))
		}
	})

	t.Run("writing a batch with non-nil DedupKey twice leaves the row count unchanged", func(t *testing.T) {
		sink, querier := factory(t)
		recs := batch(10, exporterA, true)
		require.NoError(t, sink.WriteBatch(ctx, recs))
		require.NoError(t, sink.WriteBatch(ctx, recs))
		require.Len(t, all(t, querier, window()), 10)
	})

	t.Run("a batch mixing new records with redelivered ones stores only the new ones", func(t *testing.T) {
		sink, querier := factory(t)
		require.NoError(t, sink.WriteBatch(ctx, batch(10, exporterA, true)))
		// The first 10 keys of this batch are the redeliveries; the last 10 are new.
		require.NoError(t, sink.WriteBatch(ctx, batch(20, exporterA, true)))
		require.Len(t, all(t, querier, window()), 20)
	})

	t.Run("writing a batch with nil DedupKey twice doubles the row count", func(t *testing.T) {
		sink, querier := factory(t)
		recs := batch(10, exporterA, false)
		require.NoError(t, sink.WriteBatch(ctx, recs))
		require.NoError(t, sink.WriteBatch(ctx, recs))
		require.Len(t, all(t, querier, window()), 20)
	})

	t.Run("pages 25 records by 10 across 3 pages with no repeat or gap", func(t *testing.T) {
		sink, querier := factory(t)
		require.NoError(t, sink.WriteBatch(ctx, batch(25, exporterA, false)))
		q := window()
		q.Limit = 10
		var pages []flow.FlowResultPage
		for {
			page, err := querier.Query(ctx, q)
			require.NoError(t, err)
			pages = append(pages, page)
			if !page.HasMore {
				break
			}
			q.Cursor = page.NextCursor
		}
		require.Len(t, pages, 3)
		require.Len(t, pages[0].Records, 10)
		require.Len(t, pages[1].Records, 10)
		require.Len(t, pages[2].Records, 5)
		require.True(t, pages[0].HasMore)
		require.True(t, pages[1].HasMore)
		require.False(t, pages[2].HasMore)
		seen := map[uint16]bool{}
		var prev time.Time
		for i, p := range pages {
			for _, r := range p.Records {
				require.False(t, seen[r.SrcPort], "record %d repeated", r.SrcPort)
				seen[r.SrcPort] = true
				if !prev.IsZero() {
					require.False(t, r.ReceivedAt.After(prev), "page %d out of order", i)
				}
				prev = r.ReceivedAt
			}
		}
		require.Len(t, seen, 25)
	})

	t.Run("treats the time range as half-open [Start, End)", func(t *testing.T) {
		sink, querier := factory(t)
		start, end := baseTime, baseTime.Add(time.Minute)
		recs := []flow.FlowRecord{
			Fixture(0, exporterA, start.Add(-time.Microsecond), false), // before
			Fixture(1, exporterA, start, false),                        // in: at Start
			Fixture(2, exporterA, end.Add(-time.Microsecond), false),   // in: just before End
			Fixture(3, exporterA, end, false),                          // out: at End
		}
		require.NoError(t, sink.WriteBatch(ctx, recs))
		got := all(t, querier, flow.FlowQuery{Start: start, End: end, Limit: 100})
		bySrcPort(got)
		require.Len(t, got, 2)
		require.Equal(t, recs[1].SrcPort, got[0].SrcPort)
		require.Equal(t, recs[2].SrcPort, got[1].SrcPort)
	})

	t.Run("applies the exporter, address and protocol filters", func(t *testing.T) {
		sink, querier := factory(t)
		recs := append(batch(6, exporterA, false), Fixture(99, exporterB, baseTime.Add(time.Second), false))
		require.NoError(t, sink.WriteBatch(ctx, recs))
		everything := all(t, querier, window())
		require.Len(t, everything, 7)

		var idB int64
		for _, r := range everything {
			if r.ExporterAddr == exporterB {
				idB = r.ExporterID
			}
		}
		require.Positive(t, idB)

		q := window()
		q.ExporterID = idB
		got := all(t, querier, q)
		require.Len(t, got, 1)
		require.Equal(t, exporterB, got[0].ExporterAddr)

		q = window()
		q.SrcAddr = recs[2].SrcAddr
		got = all(t, querier, q)
		require.Len(t, got, 1)
		require.Equal(t, recs[2].SrcPort, got[0].SrcPort)

		q = window()
		q.DstAddr = recs[1].DstAddr // the IPv6 fixture
		got = all(t, querier, q)
		require.Len(t, got, 2, "fixtures 1 and 4 share the IPv6 destination")

		udp := uint8(17)
		q = window()
		q.Protocol = &udp
		got = all(t, querier, q)
		require.Len(t, got, 2)
		for _, r := range got {
			require.Equal(t, udp, r.Protocol)
		}
	})
}
