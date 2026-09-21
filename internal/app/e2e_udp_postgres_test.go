package app

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/moby/moby/api/types/network"

	"github.com/bogie5464/netflow-collector/internal/api"
	"github.com/bogie5464/netflow-collector/internal/config"
	"github.com/bogie5464/netflow-collector/internal/flow"
	"github.com/bogie5464/netflow-collector/internal/obs"
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

// v5Datagram builds one NetFlow v5 datagram with a single record from the
// documented wire layout: 24-byte header, 48-byte record, big-endian.
func v5Datagram(src, dst netip.Addr, srcPort, dstPort uint16, proto uint8, pkts, octets uint32) []byte {
	b := binary.BigEndian.AppendUint16(nil, 5)
	b = binary.BigEndian.AppendUint16(b, 1)
	b = binary.BigEndian.AppendUint32(b, 10_000)                    // sys_uptime ms
	b = binary.BigEndian.AppendUint32(b, uint32(time.Now().Unix())) // unix_secs
	b = binary.BigEndian.AppendUint32(b, 0)                         // unix_nsecs
	b = binary.BigEndian.AppendUint32(b, 1)                         // flow_sequence
	b = append(b, 0, 0)                                             // engine_type, engine_id
	b = binary.BigEndian.AppendUint16(b, 0)                         // sampling_interval
	b = append(b, src.AsSlice()...)
	b = append(b, dst.AsSlice()...)
	b = append(b, 0, 0, 0, 0) // nexthop
	b = binary.BigEndian.AppendUint16(b, 3)
	b = binary.BigEndian.AppendUint16(b, 7)
	b = binary.BigEndian.AppendUint32(b, pkts)
	b = binary.BigEndian.AppendUint32(b, octets)
	b = binary.BigEndian.AppendUint32(b, 1000) // first
	b = binary.BigEndian.AppendUint32(b, 2500) // last
	b = binary.BigEndian.AppendUint16(b, srcPort)
	b = binary.BigEndian.AppendUint16(b, dstPort)
	b = append(b, 0, 24, proto, 0) // pad, tcp_flags, prot, tos
	b = binary.BigEndian.AppendUint16(b, 64500)
	b = binary.BigEndian.AppendUint16(b, 64501)
	b = append(b, 24, 24, 0, 0)
	return b
}

func testConfig(dsn string) config.Config {
	return config.Config{
		LogLevel:        "info",
		NetFlowAddr:     "127.0.0.1:0",
		Sources:         []string{config.SourceNetFlow},
		Sinks:           []string{config.SinkPostgres},
		PostgresDSN:     dsn,
		RetentionDays:   30,
		PipelineBuffer:  1024,
		BatchSize:       100,
		BatchInterval:   200 * time.Millisecond,
		Workers:         2,
		PostgresEnabled: true,
		NetFlowEnabled:  true,
	}
}

func TestVerticalSlice(t *testing.T) {
	dsn := sinktest.StartTimescale(t)
	ctx := context.Background()

	a, err := New(ctx, testConfig(dsn))
	require.NoError(t, err)

	// Package-level Run installs the signal handler; here we do the same so
	// the SIGTERM assertion exercises the real shutdown path.
	sigCtx, stop := signalContext(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- a.Run(sigCtx) }()

	select {
	case <-a.Ready():
	case err := <-done:
		t.Fatalf("app stopped before listening: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("app did not start listening")
	}
	addr := a.NetFlowAddr()
	require.True(t, addr.IsValid())
	require.NotZero(t, addr.Port(), "listener bound an ephemeral port after migrations")

	src, dst := netip.MustParseAddr("203.0.113.10"), netip.MustParseAddr("198.51.100.200")
	conn, err := net.Dial("udp", addr.String())
	require.NoError(t, err)
	_, err = conn.Write(v5Datagram(src, dst, 51514, 443, 6, 12, 9000))
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	querier := a.Backend(config.SinkPostgres)
	q := flow.FlowQuery{Start: time.Now().Add(-time.Minute), End: time.Now().Add(time.Minute), Limit: 10}
	var got flow.FlowRecord
	require.Eventually(t, func() bool {
		page, err := querier.Query(ctx, q)
		if err != nil || len(page.Records) == 0 {
			return false
		}
		got = page.Records[0]
		return true
	}, 5*time.Second, 50*time.Millisecond, "record must be queryable within 5 seconds")
	require.Equal(t, flow.FlowTypeNetFlow5, got.FlowType)
	require.Equal(t, src, got.SrcAddr)
	require.Equal(t, dst, got.DstAddr)
	require.Equal(t, uint16(51514), got.SrcPort)
	require.Equal(t, uint16(443), got.DstPort)
	require.Equal(t, uint8(6), got.Protocol)
	require.Equal(t, uint64(12), got.Packets)
	require.Equal(t, uint64(9000), got.Bytes)
	require.Equal(t, netip.MustParseAddr("127.0.0.1"), got.ExporterAddr)
	require.Nil(t, got.DedupKey)

	// A record still buffered at shutdown must be flushed, not lost.
	conn, err = net.Dial("udp", addr.String())
	require.NoError(t, err)
	_, err = conn.Write(v5Datagram(src, dst, 51515, 443, 6, 1, 64))
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	time.Sleep(50 * time.Millisecond) // let the datagram reach the buffer, shorter than the batch interval

	started := time.Now()
	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGTERM))
	select {
	case err := <-done:
		require.NoError(t, err, "SIGTERM is a clean exit")
	case <-time.After(10 * time.Second):
		t.Fatal("app did not stop within 10 seconds of SIGTERM")
	}
	require.Less(t, time.Since(started), 10*time.Second)

	// Backends are closed; read through a fresh one to see the flushed record.
	fresh, err := New(ctx, testConfig(dsn))
	require.NoError(t, err)
	q.Limit = 10
	page, err := fresh.Backend(config.SinkPostgres).Query(ctx, q)
	require.NoError(t, err)
	require.Len(t, page.Records, 2, "the batch pending at SIGTERM was flushed")
	fresh.closeSinks()

	// Port is released.
	pc, err := net.ListenPacket("udp", addr.String())
	require.NoError(t, err)
	require.NoError(t, pc.Close())
}

// TestVerticalSliceServesAPI covers the one path internal/api's own tests
// cannot: the REST API wired through the real App, over a real HTTP
// listener bound by serveHTTP, rather than a fake Querier and httptest.
func TestVerticalSliceServesAPI(t *testing.T) {
	dsn := sinktest.StartTimescale(t)
	cfg := testConfig(dsn)
	cfg.HTTPAddr = "127.0.0.1:0"
	cfg.APIKeys = []string{"test-key"}

	ctx, cancel := context.WithCancel(context.Background())
	a, err := New(ctx, cfg)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	select {
	case <-a.Ready():
	case err := <-done:
		t.Fatalf("app stopped before listening: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("app did not start listening")
	}

	addr := a.NetFlowAddr()
	require.True(t, addr.IsValid())
	src, dst := netip.MustParseAddr("203.0.113.20"), netip.MustParseAddr("198.51.100.201")
	conn, err := net.Dial("udp", addr.String())
	require.NoError(t, err)
	_, err = conn.Write(v5Datagram(src, dst, 51600, 443, 6, 5, 500))
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	base := "http://" + a.HTTPAddr().String()

	resp, err := http.Get(base + "/v1/flows?start=2026-01-01T00:00:00Z&end=2027-01-01T00:00:00Z")
	require.NoError(t, err)
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode, "no API key")
	require.NoError(t, resp.Body.Close())

	resp, err = http.Get(base + "/healthz")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "/healthz stays public")
	require.NoError(t, resp.Body.Close())

	require.Eventually(t, func() bool {
		req, err := http.NewRequest(http.MethodGet, base+"/v1/flows?start=2026-01-01T00:00:00Z&end=2027-01-01T00:00:00Z&limit=10", nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer test-key")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			return false
		}
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return strings.Contains(string(body), `"src_port":51600`)
	}, 5*time.Second, 50*time.Millisecond, "record must become queryable through the real HTTP wiring")

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("app did not stop")
	}
}

func sampleCount(t *testing.T, sink string) uint64 {
	t.Helper()
	var m dto.Metric
	require.NoError(t, obs.BatchWriteDuration.WithLabelValues(sink).(interface{ Write(*dto.Metric) error }).Write(&m))
	return m.GetHistogram().GetSampleCount()
}

// TestVerticalSliceFanOut covers NFC_SINKS=postgres,mariadb through the real
// wiring layer: one datagram lands in both backends and each sink label
// records its own write duration.
func TestVerticalSliceFanOut(t *testing.T) {
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
	a, err := New(ctx, cfg)
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
	src, dst := netip.MustParseAddr("203.0.113.10"), netip.MustParseAddr("198.51.100.200")
	conn, err := net.Dial("udp", a.NetFlowAddr().String())
	require.NoError(t, err)
	_, err = conn.Write(v5Datagram(src, dst, 51514, 443, 6, 12, 9000))
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	q := flow.FlowQuery{Start: time.Now().Add(-time.Minute), End: time.Now().Add(time.Minute), Limit: 10}
	for _, name := range []string{config.SinkPostgres, config.SinkMariaDB} {
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

func TestVerticalSliceUnknownNamesAreConfigErrors(t *testing.T) {
	// Names config's oneof would reject are used on purpose: this is the
	// wiring layer's own check, for a backend nothing implements.
	cfg := testConfig("postgres://unused")
	cfg.Sinks = []string{"nosuchdb"}
	_, err := New(context.Background(), cfg)
	require.ErrorIs(t, err, ErrConfig)
	require.ErrorContains(t, err, "nosuchdb")

	cfg = testConfig("postgres://unused")
	cfg.Sources = []string{"nosuchsource"}
	_, err = New(context.Background(), cfg)
	require.ErrorIs(t, err, ErrConfig)
	require.ErrorContains(t, err, "nosuchsource")
}

func TestVerticalSliceStorageFailureBindsNothing(t *testing.T) {
	// A closed port stands in for a database that cannot be migrated: the
	// boot fails in the storage class (exit 3) and no listener is bound.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())

	a, err := New(context.Background(), testConfig("postgres://netflow:netflow@127.0.0.1:"+itoa(port)+"/netflow?sslmode=disable&connect_timeout=2"))
	if err == nil {
		err = a.Run(context.Background())
	}
	require.ErrorIs(t, err, ErrMigration)
	require.NotErrorIs(t, err, ErrConfig)
	if a != nil {
		require.False(t, a.NetFlowAddr().IsValid(), "no listener may bind when storage boot fails")
	}
}

func itoa(n int) string { return strconv.Itoa(n) }

// TestRunFailsFastOnInvalidConfig covers the one path only the package-level
// Run exercises: config.Load() reading the real environment, before New is
// ever reached. Everything else in this file calls New directly with a
// hand-built config.Config, bypassing Load and its validation entirely.
func TestRunFailsFastOnInvalidConfig(t *testing.T) {
	t.Setenv("NFC_SOURCES", "netflow")
	t.Setenv("NFC_SINKS", "postgres")
	t.Setenv("NFC_NETFLOW_ADDR", "127.0.0.1:0")
	t.Setenv("NFC_HTTP_ADDR", "")
	t.Setenv("NFC_POSTGRES_DSN", "")

	err := Run(context.Background())
	require.ErrorIs(t, err, ErrConfig)
	require.ErrorContains(t, err, "NFC_POSTGRES_DSN")
}

// TestOpenAPI covers the package's own thin wrapper — app.OpenAPI is what
// cmd/collector's -dump-openapi flag calls, and it was never invoked from
// any test: only internal/api's own OpenAPI() was.
func TestOpenAPI(t *testing.T) {
	require.Equal(t, api.OpenAPI(), OpenAPI())
}
