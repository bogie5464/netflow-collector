package mariadb_test

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"net"
	"net/netip"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/moby/moby/api/types/network"

	"github.com/bogie5464/netflow-collector/internal/app"
	"github.com/bogie5464/netflow-collector/internal/config"
	"github.com/bogie5464/netflow-collector/internal/flow"
	"github.com/bogie5464/netflow-collector/internal/obs"
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

// v5Datagram builds one NetFlow v5 datagram with a single record.
func v5Datagram(srcPort uint16) []byte {
	b := binary.BigEndian.AppendUint16(nil, 5)
	b = binary.BigEndian.AppendUint16(b, 1)
	b = binary.BigEndian.AppendUint32(b, 10_000)
	b = binary.BigEndian.AppendUint32(b, uint32(time.Now().Unix()))
	b = binary.BigEndian.AppendUint32(b, 0)
	b = binary.BigEndian.AppendUint32(b, 1)
	b = append(b, 0, 0)
	b = binary.BigEndian.AppendUint16(b, 0)
	b = append(b, 203, 0, 113, 10, 198, 51, 100, 200, 0, 0, 0, 0)
	b = binary.BigEndian.AppendUint16(b, 3)
	b = binary.BigEndian.AppendUint16(b, 7)
	b = binary.BigEndian.AppendUint32(b, 12)
	b = binary.BigEndian.AppendUint32(b, 9000)
	b = binary.BigEndian.AppendUint32(b, 1000)
	b = binary.BigEndian.AppendUint32(b, 2500)
	b = binary.BigEndian.AppendUint16(b, srcPort)
	b = binary.BigEndian.AppendUint16(b, 443)
	b = append(b, 0, 24, 6, 0)
	b = binary.BigEndian.AppendUint16(b, 64500)
	b = binary.BigEndian.AppendUint16(b, 64501)
	b = append(b, 24, 24, 0, 0)
	return b
}

func sampleCount(t *testing.T, sink string) uint64 {
	t.Helper()
	var m dto.Metric
	require.NoError(t, obs.BatchWriteDuration.WithLabelValues(sink).(interface{ Write(*dto.Metric) error }).Write(&m))
	return m.GetHistogram().GetSampleCount()
}

// TestMariaDBFanOut covers NFC_SINKS=postgres,mariadb through the real
// wiring layer: one datagram lands in both backends and each sink label
// records its own write duration.
func TestMariaDBFanOut(t *testing.T) {
	pgDSN := sinktest.StartTimescale(t)
	maDSN := startMariaDB(t)
	cfg := config.Config{
		LogLevel: "info", NetFlowAddr: "127.0.0.1:0",
		Sources: []string{config.SourceNetFlow}, Sinks: []string{config.SinkPostgres, config.SinkMariaDB},
		PostgresDSN: pgDSN, MariaDBDSN: maDSN, RetentionDays: 30,
		PipelineBuffer: 1024, BatchSize: 100, BatchInterval: 100 * time.Millisecond, Workers: 2,
		PostgresEnabled: true, MariaDBEnabled: true, NetFlowEnabled: true,
	}
	ctx, cancel := context.WithCancel(context.Background())
	a, err := app.New(ctx, cfg)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	select {
	case <-a.Ready():
	case err := <-done:
		t.Fatalf("app stopped early: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("app did not start")
	}

	pgBefore, maBefore := sampleCount(t, "postgres"), sampleCount(t, "mariadb")
	conn, err := net.Dial("udp", a.NetFlowAddr().String())
	require.NoError(t, err)
	_, err = conn.Write(v5Datagram(51514))
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	q := flow.FlowQuery{Start: time.Now().Add(-time.Minute), End: time.Now().Add(time.Minute), Limit: 10}
	for _, name := range []string{"postgres", "mariadb"} {
		require.Eventually(t, func() bool {
			page, err := a.Backend(name).Query(ctx, q)
			return err == nil && len(page.Records) == 1 && page.Records[0].SrcPort == 51514
		}, 5*time.Second, 50*time.Millisecond, "record visible in %s", name)
	}
	require.Equal(t, pgBefore+1, sampleCount(t, "postgres"), "one batch write observed for sink=postgres")
	require.Equal(t, maBefore+1, sampleCount(t, "mariadb"), "one batch write observed for sink=mariadb")

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("app did not stop")
	}
}
