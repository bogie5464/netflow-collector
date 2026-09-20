package mariadb_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/moby/moby/api/types/network"

	"github.com/bogie5464/netflow-collector/internal/flow"
	"github.com/bogie5464/netflow-collector/internal/sink/mariadb"
	"github.com/bogie5464/netflow-collector/internal/sink/sinktest"
)

// mariaImage is copied literally from docker-compose.yml, which owns it.
const mariaImage = "mariadb:11.8.9"

// startMariaDB starts a throwaway MariaDB container and returns a DSN that
// already carries parseTime=true&loc=UTC. It waits with an explicit SQL probe.
func startMariaDB(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	dsnFor := func(host string, port network.Port) string {
		return fmt.Sprintf("netflow:netflow@tcp(%s:%s)/netflow?parseTime=true&loc=UTC&charset=utf8mb4", host, port.Port())
	}
	c, err := testcontainers.Run(ctx, mariaImage,
		testcontainers.WithEnv(map[string]string{
			"MARIADB_DATABASE":      "netflow",
			"MARIADB_USER":          "netflow",
			"MARIADB_PASSWORD":      "netflow",
			"MARIADB_ROOT_PASSWORD": "netflow",
		}),
		testcontainers.WithExposedPorts("3306/tcp"),
		testcontainers.WithWaitStrategy(wait.ForSQL("3306/tcp", "mysql", dsnFor).WithStartupTimeout(2*time.Minute)),
	)
	require.NoError(t, err, "start %s", mariaImage)
	t.Cleanup(func() { require.NoError(t, testcontainers.TerminateContainer(c)) })
	host, err := c.Host(ctx)
	require.NoError(t, err)
	port, err := c.MappedPort(ctx, "3306/tcp")
	require.NoError(t, err)
	return dsnFor(host, port)
}

func newBackend(t *testing.T, dsn string) flow.Backend {
	t.Helper()
	ctx := context.Background()
	b, err := mariadb.New(ctx, dsn, 30)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	require.NoError(t, b.Migrate(ctx))
	return b
}

func truncate(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	for _, stmt := range []string{
		"SET FOREIGN_KEY_CHECKS = 0",
		"TRUNCATE TABLE flow_records",
		"TRUNCATE TABLE exporters",
		"SET FOREIGN_KEY_CHECKS = 1",
	} {
		_, err := db.Exec(stmt)
		require.NoError(t, err, stmt)
	}
}

func TestMariaDBMigrateIsIdempotent(t *testing.T) {
	dsn := startMariaDB(t)
	b := newBackend(t, dsn)
	require.NoError(t, b.Migrate(context.Background()), "second run changes nothing")

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	var n int
	require.NoError(t, db.QueryRow(
		`SELECT count(*) FROM information_schema.tables WHERE table_schema = 'netflow' AND table_name IN ('exporters','flow_records')`).Scan(&n))
	require.Equal(t, 2, n)
	var cols string
	require.NoError(t, db.QueryRow(`
		SELECT group_concat(column_name ORDER BY seq_in_index)
		  FROM information_schema.statistics
		 WHERE table_schema = 'netflow' AND table_name = 'flow_records'
		   AND index_name = 'uq_flow_records_dedup' AND non_unique = 0`).Scan(&cols))
	require.Equal(t, "received_at,dedup_key", cols, "unique index on (received_at, dedup_key)")
}

func TestMariaDBConformance(t *testing.T) {
	dsn := startMariaDB(t)
	sinktest.Conformance(t, func(t *testing.T) (flow.Sink, flow.Querier) {
		b := newBackend(t, dsn)
		truncate(t, dsn)
		return b, b
	})
}

func TestMariaDBIPv6RoundTrip(t *testing.T) {
	b := newBackend(t, startMariaDB(t))
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

func TestMariaDBRejectsDSNWithoutUTCParseTime(t *testing.T) {
	_, err := mariadb.New(context.Background(), "netflow:netflow@tcp(127.0.0.1:1)/netflow", 30)
	require.ErrorContains(t, err, "parseTime=true&loc=UTC")
}

func TestMariaDBListExporters(t *testing.T) {
	b := newBackend(t, startMariaDB(t))
	lister, ok := b.(flow.ExporterLister)
	require.True(t, ok, "mariadb backend must implement flow.ExporterLister")

	exporter := netip.MustParseAddr("192.0.2.60")
	rec := sinktest.Fixture(0, exporter, time.Now().UTC(), false)
	require.NoError(t, b.WriteBatch(context.Background(), []flow.FlowRecord{rec}))

	got, err := lister.ListExporters(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, exporter, got[0].IPAddress)
	require.Nil(t, got[0].Label)
	require.False(t, got[0].FirstSeenAt.IsZero())
	require.False(t, got[0].LastSeenAt.IsZero())
}
