package app

import (
	"context"
	"encoding/binary"
	"net"
	"net/netip"
	"os"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bogie5464/netflow-collector/internal/config"
	"github.com/bogie5464/netflow-collector/internal/flow"
	"github.com/bogie5464/netflow-collector/internal/sink/sinktest"
)

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
	fresh.closeBackends()

	// Port is released.
	pc, err := net.ListenPacket("udp", addr.String())
	require.NoError(t, err)
	require.NoError(t, pc.Close())
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
