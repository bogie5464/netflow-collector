package clickhouse_test

import (
	"context"
	"net/netip"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"

	"github.com/bogie5464/netflow-collector/internal/flow"
	"github.com/bogie5464/netflow-collector/internal/sink/clickhouse"
	"github.com/bogie5464/netflow-collector/internal/sink/sinktest"
)

// Each top-level test gets its own container; within the conformance run the
// factory truncates the tables so no assertion sees another's rows.

func newBackend(t *testing.T, dsn string) flow.Backend {
	t.Helper()
	ctx := context.Background()
	b, err := clickhouse.New(ctx, dsn, 30)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	require.NoError(t, b.Migrate(ctx))
	return b
}

func exec(t *testing.T, dsn string, statements ...string) {
	t.Helper()
	opts, err := ch.ParseDSN(dsn)
	require.NoError(t, err)
	conn, err := ch.Open(opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()
	for _, s := range statements {
		require.NoError(t, conn.Exec(context.Background(), s), s)
	}
}

func truncate(t *testing.T, dsn string) {
	t.Helper()
	exec(t, dsn, "TRUNCATE TABLE flow_records", "TRUNCATE TABLE exporters")
}

func TestClickHouseMigrateIsIdempotent(t *testing.T) {
	dsn := sinktest.StartClickHouse(t)
	b := newBackend(t, dsn)
	require.NoError(t, b.Migrate(context.Background()), "second run changes nothing")

	opts, err := ch.ParseDSN(dsn)
	require.NoError(t, err)
	conn, err := ch.Open(opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()

	var n uint64
	require.NoError(t, conn.QueryRow(context.Background(),
		`SELECT count() FROM system.tables WHERE database = currentDatabase() AND name IN ('exporters', 'flow_records')`).Scan(&n))
	require.EqualValues(t, 2, n)

	var engine string
	require.NoError(t, conn.QueryRow(context.Background(),
		`SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name = 'flow_records'`).Scan(&engine))
	require.Contains(t, engine, "ReplacingMergeTree")
	require.Contains(t, engine, "ORDER BY (received_at, seq)")
	require.Contains(t, engine, "TTL toDateTime(received_at) + toIntervalDay(30)", "retention applied from NFC_RETENTION_DAYS")
}

func TestClickHouseConformance(t *testing.T) {
	dsn := sinktest.StartClickHouse(t)
	sinktest.Conformance(t, func(t *testing.T) (flow.Sink, flow.Querier) {
		b := newBackend(t, dsn)
		truncate(t, dsn)
		return b, b
	})
}

func TestClickHouseIPv6RoundTrip(t *testing.T) {
	b := newBackend(t, sinktest.StartClickHouse(t))
	ctx := context.Background()
	at := time.Now().UTC().Truncate(time.Microsecond)
	rec := sinktest.Fixture(1, netip.MustParseAddr("2001:db8:1::1"), at, false) // fixture 1 carries IPv6 src and dst
	require.True(t, rec.SrcAddr.Is6())
	require.NoError(t, b.WriteBatch(ctx, []flow.FlowRecord{rec}))
	page, err := b.Query(ctx, flow.FlowQuery{Start: at.Add(-time.Second), End: at.Add(time.Second), Limit: 10})
	require.NoError(t, err)
	require.Len(t, page.Records, 1)
	require.Equal(t, rec.SrcAddr, page.Records[0].SrcAddr)
	require.Equal(t, rec.DstAddr, page.Records[0].DstAddr)
	require.Equal(t, rec.ExporterAddr, page.Records[0].ExporterAddr)
}

func TestClickHouseListExporters(t *testing.T) {
	b := newBackend(t, sinktest.StartClickHouse(t))
	lister, ok := b.(flow.ExporterLister)
	require.True(t, ok, "clickhouse backend must implement flow.ExporterLister")

	ctx := context.Background()
	exporter := netip.MustParseAddr("192.0.2.60")
	first := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	require.NoError(t, b.WriteBatch(ctx, []flow.FlowRecord{sinktest.Fixture(0, exporter, first, false)}))
	// A later batch advances last_seen_at and must not disturb first_seen_at.
	require.NoError(t, b.WriteBatch(ctx, []flow.FlowRecord{sinktest.Fixture(1, exporter, first.Add(time.Hour), false)}))

	got, err := lister.ListExporters(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, exporter, got[0].IPAddress)
	require.Positive(t, got[0].ID)
	require.Equal(t, first, got[0].FirstSeenAt)
	require.Equal(t, first.Add(time.Hour), got[0].LastSeenAt)

	page, err := b.Query(ctx, flow.FlowQuery{Start: first.Add(-time.Second), End: first.Add(2 * time.Hour), Limit: 10})
	require.NoError(t, err)
	require.Len(t, page.Records, 2)
	require.Equal(t, got[0].ID, page.Records[0].ExporterID, "records carry the same exporter id the lister reports")
}

func TestClickHouseRejectsUndecodableCursor(t *testing.T) {
	b := newBackend(t, sinktest.StartClickHouse(t))
	_, err := b.Query(context.Background(), flow.FlowQuery{
		Start: time.Now().Add(-time.Hour), End: time.Now(), Limit: 10, Cursor: "not base64url!",
	})
	require.ErrorIs(t, err, clickhouse.ErrBadCursor)
}
