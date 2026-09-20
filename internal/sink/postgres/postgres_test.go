package postgres

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/bogie5464/netflow-collector/internal/flow"
	"github.com/bogie5464/netflow-collector/internal/sink/sinktest"
)

// Each top-level test gets its own container; within the conformance run the
// factory resets the tables so no assertion sees another's rows.

func newBackend(t *testing.T, dsn string) flow.Backend {
	t.Helper()
	ctx := context.Background()
	b, err := New(ctx, dsn, 30)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	require.NoError(t, b.Migrate(ctx))
	return b
}

func truncate(t *testing.T, dsn string) {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	defer pool.Close()
	_, err = pool.Exec(context.Background(), `TRUNCATE flow_records, exporters RESTART IDENTITY`)
	require.NoError(t, err)
}

func TestPostgresMigrateIsIdempotent(t *testing.T) {
	// The container starts empty, so the first Migrate here is the "empty
	// database" case; the second run must change nothing.
	dsn := sinktest.StartTimescale(t)
	b := newBackend(t, dsn)
	require.NoError(t, b.Migrate(context.Background()))

	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	defer pool.Close()
	var n int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM information_schema.tables WHERE table_name IN ('exporters','flow_records')`).Scan(&n))
	require.Equal(t, 2, n)
	var partitioned bool
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) = 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'flow_records'`).Scan(&partitioned))
	require.True(t, partitioned, "flow_records is a hypertable")
	var dim string
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT column_name FROM timescaledb_information.dimensions WHERE hypertable_name = 'flow_records'`).Scan(&dim))
	require.Equal(t, "received_at", dim)
	var policies int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM timescaledb_information.jobs WHERE proc_name = 'policy_retention' AND hypertable_name = 'flow_records'`).Scan(&policies))
	require.Equal(t, 1, policies, "exactly one retention policy after two Migrate runs")
}

func TestPostgresConformance(t *testing.T) {
	dsn := sinktest.StartTimescale(t)
	sinktest.Conformance(t, func(t *testing.T) (flow.Sink, flow.Querier) {
		b := newBackend(t, dsn)
		truncate(t, dsn)
		return b, b
	})
}

func TestPostgresRejectsBadCursor(t *testing.T) {
	b := newBackend(t, sinktest.StartTimescale(t))
	_, err := b.Query(context.Background(), flow.FlowQuery{Cursor: "not-a-cursor"})
	require.ErrorIs(t, err, ErrBadCursor)
}

func TestPostgresListExporters(t *testing.T) {
	b := newBackend(t, sinktest.StartTimescale(t))
	lister, ok := b.(flow.ExporterLister)
	require.True(t, ok, "postgres backend must implement flow.ExporterLister")

	exporter := netip.MustParseAddr("192.0.2.50")
	rec := sinktest.Fixture(0, exporter, time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC), false)
	require.NoError(t, b.WriteBatch(context.Background(), []flow.FlowRecord{rec}))

	got, err := lister.ListExporters(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, exporter, got[0].IPAddress)
	require.Nil(t, got[0].Label)
	require.False(t, got[0].FirstSeenAt.IsZero())
	require.False(t, got[0].LastSeenAt.IsZero())
}
